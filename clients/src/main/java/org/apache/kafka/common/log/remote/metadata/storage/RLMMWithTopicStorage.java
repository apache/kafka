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
package org.apache.kafka.common.log.remote.metadata.storage;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.log.remote.storage.RemoteLogMetadataManager;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.common.log.remote.storage.RemoteStorageException;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
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
import java.util.stream.Collectors;

/**
 * This is an implementation of {@link RemoteLogMetadataManager} based on internal topic storage.
 *
 * This may be moved to core module as it is part of cluster running on brokers.
 */
public class RLMMWithTopicStorage implements RemoteLogMetadataManager, RemoteLogSegmentMetadataUpdater {

    private static final Logger log = LoggerFactory.getLogger(RLMMWithTopicStorage.class);

    public static final String REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP =
            "remote.log.metadata.topic.replication.factor";
    public static final String REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP = "remote.log.metadata.topic.partitions";
    public static final String REMOTE_LOG_METADATA_TOPIC_RETENTION_MINS_PROP =
            "remote.log.metadata.topic.retention.mins";
    public static final int DEFAULT_REMOTE_LOG_METADATA_TOPIC_PARTITIONS = 3;
    public static final int DEFAULT_REMOTE_LOG_METADATA_TOPIC_RETENTION_MINS = 365 * 24 * 60;
    public static final int DEFAULT_REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR = 3;

    private static final String COMMITTED_LOG_METADATA_FILE_NAME = "_rlmm_committed_metadata_log";

    private static final int PUBLISH_TIMEOUT_SECS = 120;
    public static final String REMOTE_LOG_METADATA_CLIENT_PREFIX = "__remote_log_metadata_client";

    protected int noOfMetadataTopicPartitions;
    private ConcurrentSkipListMap<RemoteLogSegmentId, RemoteLogSegmentMetadata> idWithSegmentMetadata =
            new ConcurrentSkipListMap<>();
    private Map<TopicPartition, NavigableMap<Long, RemoteLogSegmentId>> partitionsWithSegmentIds =
            new ConcurrentHashMap<>();
    private KafkaProducer<String, Object> producer;
    private AdminClient adminClient;
    private KafkaConsumer<String, RemoteLogSegmentMetadata> consumer;
    private String logDir;
    private volatile Map<String, ?> configs;

    private CommittedLogMetadataStore committedLogMetadataStore;
    private ConsumerTask consumerTask;
    private volatile boolean initialized;
    private boolean configured;

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
    public void putRemoteLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
        // insert remote log metadata into the topic.
        publishMessageToPartition(remoteLogSegmentMetadata);
    }

    private void publishMessageToPartition(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        ensureInitialized();

        log.info("Publishing messages to remote log metadata topic for remote log segment metadata [{}]",
                remoteLogSegmentMetadata);

        RemoteLogSegmentId remoteLogSegmentId = remoteLogSegmentMetadata.remoteLogSegmentId();
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
            throw new KafkaException("Exception occurred while publishing message for remote-log-segment-id"
                    + remoteLogSegmentId, e);
        }
    }

    private void waitTillConsumerCatchesUp(RecordMetadata recordMetadata) throws InterruptedException {
        final int partition = recordMetadata.partition();

        // if the current assignment does not have the subscription for this partition then return immediately.
        if (!consumerTask.assignedPartition(partition)) {
            log.warn("This consumer is not subscribed to the target partition [{}] on which message is produced.",
                    partition);
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
    public RemoteLogSegmentMetadata remoteLogSegmentMetadata(TopicPartition topicPartition, long offset) throws RemoteStorageException {
        ensureInitialized();

        NavigableMap<Long, RemoteLogSegmentId> remoteLogSegmentIdMap = partitionsWithSegmentIds.get(topicPartition);
        if (remoteLogSegmentIdMap == null) {
            return null;
        }

        // look for floor entry as the given offset may exist in this entry.
        Map.Entry<Long, RemoteLogSegmentId> entry = remoteLogSegmentIdMap.floorEntry(offset);
        if (entry == null) {
            // if the offset is lower than the minimum offset available in metadata then return null.
            return null;
        }

        RemoteLogSegmentMetadata remoteLogSegmentMetadata = idWithSegmentMetadata.get(entry.getValue());
        // look forward for the segment which has the target offset, if it does not exist then return the highest
        // offset segment available.
        while (remoteLogSegmentMetadata != null && remoteLogSegmentMetadata.endOffset() < offset) {
            entry = remoteLogSegmentIdMap.higherEntry(entry.getKey());
            if (entry == null) {
                break;
            }
            remoteLogSegmentMetadata = idWithSegmentMetadata.get(entry.getValue());
        }

        return remoteLogSegmentMetadata;
    }

    @Override
    public Optional<Long> earliestLogOffset(TopicPartition tp) throws RemoteStorageException {
        ensureInitialized();

        NavigableMap<Long, RemoteLogSegmentId> map = partitionsWithSegmentIds.get(tp);

        return map == null || map.isEmpty() ? Optional.empty() : Optional.of(map.firstEntry().getKey());
    }

    public Optional<Long> highestLogOffset(TopicPartition tp) throws RemoteStorageException {
        ensureInitialized();

        NavigableMap<Long, RemoteLogSegmentId> map = partitionsWithSegmentIds.get(tp);

        return map == null || map.isEmpty() ? Optional.empty() : Optional.of(map.lastEntry().getKey());
    }

    @Override
    public void deleteRemoteLogSegmentMetadata(RemoteLogSegmentId remoteLogSegmentId) throws RemoteStorageException {
        ensureInitialized();

        RemoteLogSegmentMetadata metadata = idWithSegmentMetadata.get(remoteLogSegmentId);
        if (metadata != null) {
            publishMessageToPartition(RemoteLogSegmentMetadata.markForDeletion(metadata));
        }
    }

    @Override
    public List<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicPartition topicPartition, long minOffset) {
        ensureInitialized();

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
        consumerTask.addAssignmentsForPartitions(allPartitions);
    }

    @Override
    public void onStopPartitions(Set<TopicPartition> partitions) {
        ensureInitialized();

        initialize();

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

            if (configs.get(BOOTSTRAP_SERVERS_CONFIG) == null) {
                throw new InvalidConfigurationException("Broker endpoint must be configured for the remote log " +
                        "metadata manager.");
            }

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

    private void ensureInitialized() {
        if (!initialized)
            throw new KafkaException("Resources required are not yet initialized by invoking initialize()");
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
    public synchronized void configure(Map<String, ?> configs) {
        if (configured) {
            log.info("configure is already invoked earlier.");
            return;
        }
        log.info("RLMMWithTopicStorage is initializing with configs: {}", configs);

        this.configs = Collections.unmodifiableMap(configs);

        Object propVal = configs.get(REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP);
        noOfMetadataTopicPartitions =
                (propVal == null) ? DEFAULT_REMOTE_LOG_METADATA_TOPIC_PARTITIONS : Integer.parseInt(propVal.toString());

        logDir = (String) configs.get("log.dir");
        if (logDir == null || logDir.trim().isEmpty()) {
            throw new IllegalArgumentException("log.dir can not be null or empty");
        }

        File metadataLogFile = new File(logDir, COMMITTED_LOG_METADATA_FILE_NAME);
        committedLogMetadataStore = new CommittedLogMetadataStore(metadataLogFile);

        configured = true;
        log.info("RLMMWithTopicStorage is initialized: {}", this);
    }

    private void loadMetadataStore() {
        try {
            final Collection<RemoteLogSegmentMetadata> remoteLogSegmentMetadatas = committedLogMetadataStore.read();
            for (RemoteLogSegmentMetadata entry : remoteLogSegmentMetadatas) {
                partitionsWithSegmentIds.computeIfAbsent(entry.remoteLogSegmentId().topicPartition(),
                    k -> new ConcurrentSkipListMap<>()).put(entry.startOffset(), entry.remoteLogSegmentId());
                idWithSegmentMetadata.put(entry.remoteLogSegmentId(), entry);
            }
        } catch (IOException e) {
            throw new KafkaException("Error occurred while loading remote log metadata file.", e);
        }
    }

    public void syncLogMetadataDataFile() throws IOException {
        ensureInitialized();

        // idWithSegmentMetadata and partitionsWithSegmentIds are not going to be modified while this is being done.
        committedLogMetadataStore.write(idWithSegmentMetadata.values());
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

    public int metadataPartitionFor(TopicPartition tp) {
        ensureInitialized();
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
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, new RLSMSerDe().serializer().getClass().getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE);

        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 5000);

        this.producer = new KafkaProducer<>(props);
    }

    private String createClientId(String suffix) {
        // Added hashCode as part of client-id here to differentiate between multiple runs of broker.
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
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, new RLSMSerDe().deserializer().getClass().getName());

        this.consumer = new KafkaConsumer<>(props);
    }
}
