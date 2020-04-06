/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Topic based implementation for {@link RemoteLogMetadataManager}.
 *
 * This may be moved to core module as it is part of cluster running on brokers.
 *
 * This implementation is not efficient for now. We will improve once the basic end to end usecase is working fine.
 */
public class RLMMWithTopicStorage implements RemoteLogMetadataManager, RemoteLogSegmentMetadataUpdater {

    private static final Logger log = LoggerFactory.getLogger(RLMMWithTopicStorage.class);

    public static final String REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP =
            "remote.log.metadata.topic.replication.factor";
    public static final String REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP = "remote.log.metadata.topic.partitions";
    public static final String REMOTE_LOG_METADATA_TOPIC_RETENTION_MINS_PROP = "remote.log.metadata.topic.retention.mins";
    public static final int DEFAULT_REMOTE_LOG_METADATA_TOPIC_PARTITIONS = 3;
    public static final int DEFAULT_REMOTE_LOG_METADATA_TOPIC_RETENTION_MINS = 365 * 24 * 60;
    public static final int DEFAULT_REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR = 3;

    private static final String COMMITTED_LOG_METADATA_FILE_NAME = "_rlmm_committed_metadata_log";

    private static final int PUBLISH_TIMEOUT_SECS = 120;
    public static final String REMOTE_LOG_METADATA_CLIENT_PREFIX = "__remote_log_metadata_client";

    private int noOfMetadataTopicPartitions;
    private ConcurrentSkipListMap<RemoteLogSegmentId, RemoteLogSegmentMetadata> idWithSegmentMetadata =
            new ConcurrentSkipListMap<>();
    private Map<TopicPartition, NavigableMap<Long, RemoteLogSegmentId>> partitionsWithSegmentIds =
            new ConcurrentHashMap<>();
    private KafkaProducer<String, Object> producer;
    private AdminClient adminClient;
    private KafkaConsumer<String, RemoteLogSegmentMetadata> consumer;
    private String logDir;
    private Map<String, ?> configs;

    private CommittedLogMetadataFile committedLogMetadataFile;
    private ConsumerTask consumerTask;
    private boolean initialized;

    private static class ProducerCallback implements Callback {
        private volatile RecordMetadata recordMetadata;

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
            this.recordMetadata = recordMetadata;
            // exception is ignored as this would have already been thrown as we call Future<RecordMetadata>.get()
        }

        RecordMetadata recordMetadata() {
            return recordMetadata;
        }
    }

    @Override
    public void putRemoteLogSegmentData(RemoteLogSegmentId remoteLogSegmentId,
                                        RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws IOException {
        // insert remote log metadata into the topic.
        publishMessageToPartition(remoteLogSegmentId, remoteLogSegmentMetadata);
    }

    private void publishMessageToPartition(RemoteLogSegmentId remoteLogSegmentId,
                                           RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        log.info("Publishing messages to remote log metadata topic for remote log segment metadata [{}]",
                remoteLogSegmentMetadata);

        int partitionNo = metadataPartitionFor(remoteLogSegmentId.topicPartition());
        try {
            final ProducerCallback callback = new ProducerCallback();
            producer.send(new ProducerRecord<>(Topic.REMOTE_LOG_METADATA_TOPIC_NAME, partitionNo,
                            remoteLogSegmentId.topicPartition().toString(), remoteLogSegmentMetadata),
                    callback)
                    .get(PUBLISH_TIMEOUT_SECS, TimeUnit.SECONDS);

            final RecordMetadata recordMetadata = callback.recordMetadata();
            if (!recordMetadata.hasOffset()) {
                throw new KafkaException("Received record in the callback does not have offsets.");
            }

            waitTillConsumerCatchesUp(recordMetadata);
        } catch (ExecutionException e) {
            throw new KafkaException("Exception occurred while publishing message for remote-log-segment-id"
                    + remoteLogSegmentId, e.getCause());
        } catch (KafkaException e) {
            throw e;
        } catch (Exception e) {
            throw new KafkaException("Exception occurred while publishing messagefor remote-log-segment-id"
                    + remoteLogSegmentId, e);
        }
    }

    private void waitTillConsumerCatchesUp(RecordMetadata recordMetadata) throws InterruptedException {
        final int partition = recordMetadata.partition();

        // if the current assignment does not have the subscription for this partition then return immediately.
        if (!consumerTask.assignedPartition(partition)) {
            return;
        }

        final long offset = recordMetadata.offset();
        final long sleepTimeMs = 1000L;
        while (consumerTask.committedOffset(partition) < offset) {
            log.debug("Did not receive the messages till the expected offset [{}] for partition [{}], Sleeping for [{}]",
                    offset, partition, sleepTimeMs);
            Thread.sleep(sleepTimeMs);
        }
    }

    @Override
    public RemoteLogSegmentId getRemoteLogSegmentId(TopicPartition topicPartition, long offset) throws IOException {
        NavigableMap<Long, RemoteLogSegmentId> remoteLogSegmentIdMap = partitionsWithSegmentIds.get(topicPartition);
        if (remoteLogSegmentIdMap == null) {
            return null;
        }

        // look for floor entry as the given offset may exist in this entry.
        Map.Entry<Long, RemoteLogSegmentId> entry = remoteLogSegmentIdMap.floorEntry(offset);
        if (entry == null) {
            return null;
        }

        //todo-tier double-check given offset exists in the entry or look forward.
        RemoteLogSegmentMetadata remoteLogSegmentMetadata = idWithSegmentMetadata.get(entry.getValue());
        while (remoteLogSegmentMetadata != null && remoteLogSegmentMetadata.endOffset() < offset) {
            entry = remoteLogSegmentIdMap.higherEntry(entry.getKey());
            if (entry == null) {
                break;
            }
            remoteLogSegmentMetadata = idWithSegmentMetadata.get(entry.getValue());
        }

        return remoteLogSegmentMetadata != null ? remoteLogSegmentMetadata.remoteLogSegmentId() : null;
    }

    @Override
    public RemoteLogSegmentMetadata getRemoteLogSegmentMetadata(RemoteLogSegmentId remoteLogSegmentId)
            throws IOException {
        return idWithSegmentMetadata.get(remoteLogSegmentId);
    }

    @Override
    public Optional<Long> earliestLogOffset(TopicPartition tp) throws IOException {
        NavigableMap<Long, RemoteLogSegmentId> map = partitionsWithSegmentIds.get(tp);

        return map == null || map.isEmpty() ? Optional.empty() : Optional.of(map.firstEntry().getKey());
    }

    public Optional<Long> highestLogOffset(TopicPartition tp) throws IOException {
        NavigableMap<Long, RemoteLogSegmentId> map = partitionsWithSegmentIds.get(tp);

        return map == null || map.isEmpty() ? Optional.empty() : Optional.of(map.lastEntry().getKey());
    }

    @Override
    public void deleteRemoteLogSegmentMetadata(RemoteLogSegmentId remoteLogSegmentId) throws IOException {
        RemoteLogSegmentMetadata metadata = idWithSegmentMetadata.get(remoteLogSegmentId);
        if (metadata != null) {
            publishMessageToPartition(remoteLogSegmentId, RemoteLogSegmentMetadata.markForDeletion(metadata));
        }
    }

    @Override
    public List<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicPartition topicPartition, long minOffset) {
        NavigableMap<Long, RemoteLogSegmentId> map = partitionsWithSegmentIds.get(topicPartition);
        if (map == null) {
            return Collections.emptyList();
        }

        return map.tailMap(minOffset, true).values().stream()
                .filter(id -> idWithSegmentMetadata.get(id) != null)
                .map(remoteLogSegmentId -> idWithSegmentMetadata.get(remoteLogSegmentId))
                .collect(Collectors.toList());
    }

    @Override
    public void onPartitionLeadershipChanges(Set<TopicPartition> leaderPartitions,
                                             Set<TopicPartition> followerPartitions) {
        Objects.requireNonNull(leaderPartitions, "leaderPartitions can not be null");
        Objects.requireNonNull(followerPartitions, "followerPartitions can not be null");

        log.info("Received leadership notifications with leader partitions {} and follower partitions {}",
                leaderPartitions, followerPartitions);

        initialize();

        final HashSet<TopicPartition> allPartitions = new HashSet<>(leaderPartitions);
        allPartitions.addAll(followerPartitions);
        consumerTask.reassignForPartitions(allPartitions);
    }

    @Override
    public void onStopPartitions(Set<TopicPartition> partitions) {
        // remove these partitions from the currently assigned topic partitions.
        consumerTask.removeAssignmentsForPartitions(partitions);
    }

    @Override
    public void onServerStarted() {
        initialize();
    }

    private synchronized void initialize() {
        if (!initialized) {
            log.info("Initializing all the clients and resources.");
            //create clients
            createAdminClient();
            createProducer();
            createConsumer();

            // todo-tier use rocksdb
            //load the stored data
            loadMetadataStore();

            initConsumerThread();

            initialized = true;
        }
    }

    @Override
    public void close() throws IOException {
        // closeClients
        Utils.closeQuietly(producer, "Metadata Producer");
        Utils.closeQuietly(consumer, "Metadata Consumer");
        Utils.closeQuietly(adminClient, "Admin Client");
        Utils.closeQuietly(consumerTask, "ConsumerTask");
        idWithSegmentMetadata = new ConcurrentSkipListMap<>();
        partitionsWithSegmentIds = new ConcurrentHashMap<>();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.configs = configs;

        Object propVal = configs.get(REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP);
        noOfMetadataTopicPartitions =
                (propVal == null) ? DEFAULT_REMOTE_LOG_METADATA_TOPIC_PARTITIONS : Integer.parseInt(propVal.toString());

        logDir = (String) configs.get("log.dir");
        if (logDir == null || logDir.trim().isEmpty()) {
            throw new IllegalArgumentException("log.dir can not be null or empty");
        }

        File metadataLogFile = new File(logDir, COMMITTED_LOG_METADATA_FILE_NAME);
        committedLogMetadataFile = new CommittedLogMetadataFile(metadataLogFile);

        log.info("RLMMWithTopicStorage is initialized: {}", this);
    }

    private void loadMetadataStore() {
        try {
            final CommittedLogMetadataFile.Data data = committedLogMetadataFile.read();
            idWithSegmentMetadata.putAll(data.idWithSegmentMetadata);
            for (Map.Entry<TopicPartition, Map<Long, RemoteLogSegmentId>> entry : data.partitionsWithSegmentIds
                    .entrySet()) {
                partitionsWithSegmentIds.computeIfAbsent(entry.getKey(), k -> new ConcurrentSkipListMap<>())
                        .putAll(entry.getValue());
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new KafkaException("Error occurred while loading remote log metadata file.", e);
        }
    }

    public void syncLogMetadataDataFile() throws IOException {
        // idWithSegmentMetadata and partitionsWithSegmentIds are not going to be modified while this is being done.
        committedLogMetadataFile.write(idWithSegmentMetadata, partitionsWithSegmentIds);
    }

    private void initConsumerThread() {
        try {
            // start a thread to continuously consume records from topic partitions.
            consumerTask = new ConsumerTask(consumer, logDir, this);
            KafkaThread.daemon("RLMM-Consumer-Task", consumerTask).start();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error encountered while getting no of partitions of a remote log metadata " +
                    "topic with name : " + Topic.REMOTE_LOG_METADATA_TOPIC_NAME);
        }
    }

    public void updateRemoteLogSegmentMetadata(TopicPartition tp, RemoteLogSegmentMetadata metadata) {
        final NavigableMap<Long, RemoteLogSegmentId> map = partitionsWithSegmentIds
                .computeIfAbsent(tp, topicPartition -> new ConcurrentSkipListMap<>());
        if (metadata.markedForDeletion()) {
            idWithSegmentMetadata.remove(metadata.remoteLogSegmentId());
            // todo-tier check for concurrent updates when leader/follower switches occur
            map.remove(metadata.startOffset());
        } else {
            map.put(metadata.startOffset(), metadata.remoteLogSegmentId());
            idWithSegmentMetadata.put(metadata.remoteLogSegmentId(), metadata);
        }
    }

    private static class CommittedLogMetadataFile {
        private final File metadataStoreFile;

        CommittedLogMetadataFile(File metadataStoreFile) {
            this.metadataStoreFile = metadataStoreFile;

            if (!metadataStoreFile.exists()) {
                try {
                    metadataStoreFile.createNewFile();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public synchronized void write(ConcurrentSkipListMap<RemoteLogSegmentId,
                RemoteLogSegmentMetadata> idWithSegmentMetadata,
                Map<TopicPartition, NavigableMap<Long, RemoteLogSegmentId>> partitionsWithSegmentIds)
                throws IOException {
            File newMetadataStoreFile = new File(metadataStoreFile.getAbsolutePath() + ".new");
            try (FileOutputStream fos = new FileOutputStream(newMetadataStoreFile);
                 ObjectOutputStream oos = new ObjectOutputStream(fos)) {
                oos.writeObject(partitionsWithSegmentIds);
                oos.writeObject(idWithSegmentMetadata);
                oos.flush();

                fos.getFD().sync();
            }

            Utils.atomicMoveWithFallback(newMetadataStoreFile.toPath(), metadataStoreFile.toPath());
        }

        @SuppressWarnings("unchecked")
        public synchronized Data read() throws IOException, ClassNotFoundException {
            // checking for empty files.
            if (metadataStoreFile.length() == 0) {
                return new Data(Collections.emptyMap(), Collections.emptyMap());
            }

            try (FileInputStream fis = new FileInputStream(metadataStoreFile);
                 ObjectInputStream ois = new ObjectInputStream(fis)) {

                Object readObject = ois.readObject();
                if (!(readObject instanceof Map)) {
                    throw new RuntimeException("Illegal format of the read object.");
                }
                Map<TopicPartition, Map<Long, RemoteLogSegmentId>> partitionsWithSegmentIds =
                        (Map<TopicPartition, Map<Long, RemoteLogSegmentId>>) readObject;

                readObject = ois.readObject();
                if (!(readObject instanceof Map)) {
                    throw new RuntimeException("Illegal format of the read object.");
                }
                Map<RemoteLogSegmentId, RemoteLogSegmentMetadata> idWithSegmentMetadata =
                        (Map<RemoteLogSegmentId, RemoteLogSegmentMetadata>) readObject;

                return new Data(idWithSegmentMetadata, partitionsWithSegmentIds);
            }
        }

        static class Data {
            final Map<RemoteLogSegmentId, RemoteLogSegmentMetadata> idWithSegmentMetadata;
            final Map<TopicPartition, Map<Long, RemoteLogSegmentId>> partitionsWithSegmentIds;

            public Data(
                    Map<RemoteLogSegmentId, RemoteLogSegmentMetadata> idWithSegmentMetadata,
                    Map<TopicPartition, Map<Long, RemoteLogSegmentId>> partitionsWithSegmentIds) {
                this.idWithSegmentMetadata = idWithSegmentMetadata;
                this.partitionsWithSegmentIds = partitionsWithSegmentIds;
            }
        }
    }

    // There are similar implementations in core/streams about offset checkpoint file.
    // We can not reuse them as they exist in core and streams modules and we do not need to store topic name as it is
    // same. We do not want this class to be moved into core, better to keep this out in clients or a new module.
    private static class CommittedOffsetsFile {
        private final File offsetsFile;

        private static final Pattern MINIMUM_ONE_WHITESPACE = Pattern.compile("\\s+");

        CommittedOffsetsFile(File offsetsFile) {
            this.offsetsFile = offsetsFile;
        }

        public synchronized void write(Map<Integer, Long> committedOffsets) throws IOException {
            File newOffsetsFile = new File(offsetsFile.getAbsolutePath() + ".new");

            FileOutputStream fos = new FileOutputStream(newOffsetsFile);
            try (final BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(fos, StandardCharsets.UTF_8))) {
                for (Map.Entry<Integer, Long> entry : committedOffsets.entrySet()) {
                    writer.write(entry.getKey() + " " + entry.getValue());
                    writer.newLine();
                }

                writer.flush();
                fos.getFD().sync();
            }

            Utils.atomicMoveWithFallback(newOffsetsFile.toPath(), offsetsFile.toPath());
        }

        public synchronized Map<Integer, Long> read() throws IOException {
            Map<Integer, Long> partitionOffsets = new HashMap<>();
            try (BufferedReader bufferedReader = Files.newBufferedReader(offsetsFile.toPath(),
                    StandardCharsets.UTF_8)) {
                String line = null;
                while ((line = bufferedReader.readLine()) != null) {
                    String[] strings = MINIMUM_ONE_WHITESPACE.split(line);
                    if (strings.length != 2) {
                        throw new IOException("Invalid format in line: []" + line);
                    }
                    int partition = Integer.parseInt(strings[0]);
                    long offset = Long.parseLong(strings[1]);
                    partitionOffsets.put(partition, offset);
                }
            }
            return partitionOffsets;
        }
    }

    public int metadataPartitionFor(TopicPartition tp) {
        Objects.requireNonNull(tp, "TopicPartition can not be null");

        return Math.abs(tp.toString().hashCode()) % noOfMetadataTopicPartitions;
    }

    private void createAdminClient() {
        Map<String, Object> props = new HashMap<>(configs);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, createClientId("admin"));
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 5000);

        this.adminClient = AdminClient.create(props);
    }

    private void createProducer() {
        Map<String, Object> props = new HashMap<>(configs);

        props.put(ProducerConfig.CLIENT_ID_CONFIG, createClientId("producer"));

        props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, RLMMSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE);

        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 5000);

        this.producer = new KafkaProducer<>(props);
    }

    private String createClientId(String suffix) {
        // Added hasCode as part of client-id here to differentiate between multiple runs of broker.
        // Broker epoch could not be used as it is created only after RemoteLogManager and ReplicaManager are
        // created.
        return REMOTE_LOG_METADATA_CLIENT_PREFIX + "_" + suffix + configs.get("broker.id") + "_" + hashCode();
    }

    private void createConsumer() {
        Map<String, Object> props = new HashMap<>(configs);

        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, createClientId("consumer"));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 30 * 1000);
        props.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, false);
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, RLMMDeserializer.class.getName());

        this.consumer = new KafkaConsumer<>(props);
    }

    private static class ConsumerTask implements Runnable, Closeable {
        private static final Logger log = LoggerFactory.getLogger(ConsumerTask.class);
        private final KafkaConsumer<String, RemoteLogSegmentMetadata> consumer;
        private final RemoteLogSegmentMetadataUpdater remoteLogSegmentMetadataUpdater;
        private final CommittedOffsetsFile committedOffsetsFile;

        private final Object lock = new Object();
        private volatile Set<Integer> assignedMetaPartitions = Collections.emptySet();
        private Set<TopicPartition> reassignedTopicPartitions;

        private volatile boolean closed = false;
        private volatile boolean reassign = false;

        private Map<Integer, Long> targetEndOffsets = new ConcurrentHashMap<>();

        // map of topic-partition vs committed offsets
        private Map<Integer, Long> committedOffsets = new ConcurrentHashMap<>();

        private static final String COMMITTED_OFFSETS_FILE_NAME = "_rlmm_committed_offsets";
        private long lastSyncedTs = System.currentTimeMillis();

        public ConsumerTask(KafkaConsumer<String, RemoteLogSegmentMetadata> consumer,
                            String logDirStr,
                            RemoteLogSegmentMetadataUpdater remoteLogSegmentMetadataUpdater) throws IOException {
            this.consumer = consumer;
            final File logDir = new File(logDirStr);
            this.remoteLogSegmentMetadataUpdater = remoteLogSegmentMetadataUpdater;

            if (!logDir.exists()) {
                throw new IllegalArgumentException("log.dir [" + logDirStr + "] does not exist.");
            }

            // look whether the committed file exists or not.
            File file = new File(logDir, COMMITTED_OFFSETS_FILE_NAME);
            committedOffsetsFile = new CommittedOffsetsFile(file);
            if (file.createNewFile()) {
                log.info("Created file: [{}] successfully", file);
            } else {
                // load committed offset and assign them in the consumer
                final Map<Integer, Long> committedOffsets = committedOffsetsFile.read();
                final Set<Map.Entry<Integer, Long>> entries = committedOffsets.entrySet();

                if (!entries.isEmpty()) {
                    // assign topic partitions from the earlier committed offsets file.
                    assignedMetaPartitions = committedOffsets.keySet();
                    Set<TopicPartition> assignedTopicPartitions = assignedMetaPartitions.stream()
                            .map(x -> new TopicPartition(Topic.REMOTE_LOG_METADATA_TOPIC_NAME, x))
                            .collect(Collectors.toSet());
                    consumer.assign(assignedTopicPartitions);

                    // seek to the committed offset
                    for (Map.Entry<Integer, Long> entry : entries) {
                        this.committedOffsets.put(entry.getKey(), entry.getValue());
                        consumer.seek(new TopicPartition(Topic.REMOTE_LOG_METADATA_TOPIC_NAME, entry.getKey()),
                                entry.getValue());
                    }
                }
            }
        }

        @Override
        public void run() {
            log.info("Started Consumer task thread.");
            try {
                while (!closed) {
                    synchronized (lock) {
                        while (assignedMetaPartitions.isEmpty()) {
                            // if no partitions are assigned, wait till they are assigned.
                            log.info("Waiting for assigned meta partitions");
                            try {
                                lock.wait();
                            } catch (InterruptedException e) {
                                throw new KafkaException(e);
                            }
                        }

                        if (reassign) {
                            // whenever it is assigned, we should wait to process any get metadata requests until we
                            //poll all the messages till the endoffsets captured now.
                            Set<TopicPartition> assignedTopicPartitions = assignedMetaPartitions.stream()
                                    .map(x -> new TopicPartition(Topic.REMOTE_LOG_METADATA_TOPIC_NAME, x))
                                    .collect(Collectors.toSet());
                            log.info("Reassigning partitions to consumer task [{}]", assignedTopicPartitions);
                            consumer.assign(assignedTopicPartitions);
                            log.info("Reassigned partitions to consumer task [{}]", assignedTopicPartitions);

                            log.info("Fetching end offsets to consumer task [{}]", assignedTopicPartitions);
                            Map<TopicPartition, Long> endOffsets;
                            while (true) {
                                try {
                                    endOffsets = consumer.endOffsets(assignedTopicPartitions, Duration.ofSeconds(30));
                                    break;
                                } catch (Exception e) {
                                    // ignore exception
                                    log.info("#### Error encountered in fetching end offsets");
                                }
                            }
                            log.info("Fetched end offsets to consumer task [{}]", endOffsets);

                            for (Map.Entry<TopicPartition, Long> entry
                                    : endOffsets.entrySet()) {
                                if (entry.getValue() > 0) {
                                    targetEndOffsets.put(entry.getKey().partition(), entry.getValue());
                                }
                            }

                            reassign = false;
                        }
                    }

                    log.info("Polling consumer to receive remote log metadata topic records");
                    ConsumerRecords<String, RemoteLogSegmentMetadata> consumerRecords
                            = consumer.poll(Duration.ofSeconds(30L));
                    for (ConsumerRecord<String, RemoteLogSegmentMetadata> record : consumerRecords) {
                        try {
                            String key = record.key();
                            TopicPartition tp = buildTopicPartition(key);
                            remoteLogSegmentMetadataUpdater.updateRemoteLogSegmentMetadata(tp, record.value());
                            committedOffsets.put(record.partition(), record.offset());
                        } catch (WakeupException e) {
                            throw e;
                        } catch (Exception e) {
                            log.error(String.format("Error encountered while consuming record: {%s}", record), e);
                        }
                    }

                    // check whether messages are received till end offsets or not for the assigned metadata partitions.
                    if (!targetEndOffsets.isEmpty()) {
                        for (Map.Entry<Integer, Long> entry : targetEndOffsets.entrySet()) {
                            final Long offset = committedOffsets.getOrDefault(entry.getKey(), 0L);
                            if (offset >= entry.getValue()) {
                                targetEndOffsets.remove(entry.getKey());
                            }
                        }
                    }

                    // write data and sync offsets.
                    syncCommittedDataAndOffsets(false);
                }
            } catch (Exception e) {
                if (closed) {
                    log.info("ConsumerTask is closed");
                } else {
                    //todo-tier add a metric that the consumer task is failed. This will allow users can take an action
                    // based on the error.
                    log.error("Error occurred in consumer task", e);
                }
            } finally {
                log.info("Exiting from consumer task thread");
                if (!closed) {
                    // sync this only if it is not closed as it comes here in a non-graceful error.
                    syncCommittedDataAndOffsets(true);
                }
            }
        }

        public void syncCommittedDataAndOffsets(boolean forceSync) {
            if (!forceSync && System.currentTimeMillis() - lastSyncedTs < 60_000) {
                return;
            }

            try {
                remoteLogSegmentMetadataUpdater.syncLogMetadataDataFile();
                committedOffsetsFile.write(committedOffsets);
                lastSyncedTs = System.currentTimeMillis();
            } catch (IOException e) {
                log.error("Error encountered while writing committed offsets to a local file", e);
            }
        }

        public void reassignForPartitions(Set<TopicPartition> partitions) {
            Objects.requireNonNull(partitions, "partitions can not be null");

            log.info("Reassigning for user partitions {}", partitions);
            synchronized (lock) {
                // check for the corresponding partitions.
                Set<Integer> newlyAssignedPartitions = new HashSet<>();
                for (TopicPartition tp : partitions) {
                    newlyAssignedPartitions.add(remoteLogSegmentMetadataUpdater.metadataPartitionFor(tp));
                }
                reassignedTopicPartitions = new HashSet<>(partitions);
                assignedMetaPartitions = Collections.unmodifiableSet(newlyAssignedPartitions);
                log.info("Newly assigned metadata partitions {}", assignedMetaPartitions);
                reassign = true;

                lock.notifyAll();
            }
        }

        public void removeAssignmentsForPartitions(Set<TopicPartition> partitions) {
            synchronized (lock) {
                Set<TopicPartition> updatedReassignedPartitions = new HashSet<>(reassignedTopicPartitions);
                updatedReassignedPartitions.removeAll(partitions);
                Set<Integer> updatedAssignedMetaPartitions = new HashSet<>();
                for (TopicPartition tp : updatedReassignedPartitions) {
                    updatedAssignedMetaPartitions.add(remoteLogSegmentMetadataUpdater.metadataPartitionFor(tp));
                }

                if (!updatedAssignedMetaPartitions.equals(assignedMetaPartitions)) {
                    reassignedTopicPartitions = Collections.unmodifiableSet(updatedReassignedPartitions);
                    assignedMetaPartitions = Collections.unmodifiableSet(updatedAssignedMetaPartitions);
                    reassign = true;
                    lock.notifyAll();
                }
            }
        }

        public Long committedOffset(int partition) {
            return committedOffsets.getOrDefault(partition, -1L);
        }

        public void close() {
            closed = true;
            consumer.wakeup();
            syncCommittedDataAndOffsets(true);
        }

        private TopicPartition buildTopicPartition(String key) {
            int index = key.lastIndexOf("-");
            if (index < 0) {
                throw new IllegalArgumentException(
                        "Given key is not of topic partition format like <topic>-<partition>.");
            }
            String topic = key.substring(0, index);
            int partition = Integer.parseInt(key.substring(index + 1));
            return new TopicPartition(topic, partition);
        }

        public boolean assignedPartition(int partition) {
            return assignedMetaPartitions.contains(partition);
        }

    }


    public static class RLMMSerializer implements Serializer<RemoteLogSegmentMetadata> {

        @Override
        public byte[] serialize(String topic, RemoteLogSegmentMetadata data) {
            try {
                return RemoteLogSegmentMetadata.asBytes(data);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    public static class RLMMDeserializer implements Deserializer<RemoteLogSegmentMetadata> {

        @Override
        public RemoteLogSegmentMetadata deserialize(String topic, byte[] data) {
            try {
                return RemoteLogSegmentMetadata.fromBytes(data);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }
}

interface RemoteLogSegmentMetadataUpdater {
    void updateRemoteLogSegmentMetadata(TopicPartition tp, RemoteLogSegmentMetadata remoteLogSegmentMetadata);

    void syncLogMetadataDataFile() throws IOException;

    int metadataPartitionFor(TopicPartition tp);
}
