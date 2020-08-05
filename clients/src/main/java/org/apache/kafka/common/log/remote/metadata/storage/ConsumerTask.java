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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * This class is responsible for consuming messages from remote log metadata topic ({@link Topic#REMOTE_LOG_METADATA_TOPIC_NAME})
 * partitions and maintain the state of the remote log segment metadata. It gives an API to add or remove
 * for what topic partition's metadata should be consumed by this instance using
 * {{@link #addAssignmentsForPartitions(Set)}} and {@link #removeAssignmentsForPartitions(Set)} respectively.
 *
 * When a broker is started, controller sends topic partitions that this broker is leader or follower for and the
 * partitions to be deleted. This class receives those notifications with
 * {@link #addAssignmentsForPartitions(Set)} and {@link #removeAssignmentsForPartitions(Set)} assigns consumer for the
 * respective remote log metadata partitions by using {@link RemoteLogSegmentMetadataUpdater#metadataPartitionFor(TopicPartition)}.
 * Any leadership changes later are called through the same API. We will remove the partitions that ard deleted from
 * this broker which are received through {@link #removeAssignmentsForPartitions(Set)}. 
 *
 * After receiving these events it invokes {@link RemoteLogSegmentMetadataUpdater#updateRemoteLogSegmentMetadata(TopicPartition, RemoteLogSegmentMetadata)},
 * which maintains inmemory representation of the state of {@link RemoteLogSegmentMetadata}.
 */
class ConsumerTask implements Runnable, Closeable {
    private static final Logger log = LoggerFactory.getLogger(ConsumerTask.class);
    private final KafkaConsumer<String, RemoteLogSegmentMetadata> consumer;
    private final RemoteLogSegmentMetadataUpdater remoteLogSegmentMetadataUpdater;
    private final CommittedOffsetsFile committedOffsetsFile;

    private final Object lock = new Object();
    private volatile Set<Integer> assignedMetaPartitions = Collections.emptySet();
    private Set<TopicPartition> reassignedTopicPartitions = Collections.emptySet();

    private volatile boolean closed = false;
    private volatile boolean reassign = false;

    private Map<Integer, Long> targetEndOffsets = new ConcurrentHashMap<>();

    // map of topic-partition vs committed offsets
    private volatile Map<Integer, Long> committedOffsets = new ConcurrentHashMap<>();

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
                Set<Integer> assignedMetaPartitionsSnapshot = Collections.emptySet();
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
                        assignedMetaPartitionsSnapshot = new HashSet<>(assignedMetaPartitions);
                        reassign = false;
                    }
                }

                if (!assignedMetaPartitionsSnapshot.isEmpty())
                    executeReassignment(assignedMetaPartitionsSnapshot);

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

    private void executeReassignment(Set<Integer> assignedMetaPartitionsSnapshot) {
        // whenever it is assigned, we should wait to process any get metadata requests until we
        //poll all the messages till the endoffsets captured now.
        Set<TopicPartition> assignedTopicPartitions = assignedMetaPartitionsSnapshot.stream()
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
                log.debug("Error encountered in fetching end offsets");
            }
        }
        log.info("Fetched end offsets to consumer task [{}]", endOffsets);

        for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
            if (entry.getValue() > 0) {
                targetEndOffsets.put(entry.getKey().partition(), entry.getValue());
            }
        }
    }

    private void syncCommittedDataAndOffsets(boolean forceSync) {
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

    public void addAssignmentsForPartitions(Set<TopicPartition> updatedPartitions) {
        Objects.requireNonNull(updatedPartitions, "partitions can not be null");

        log.info("Reassigning for user partitions {}", updatedPartitions);
        updateAssignmentsForPartitions(updatedPartitions, Collections.emptySet());
    }

    public void removeAssignmentsForPartitions(Set<TopicPartition> partitions) {
        updateAssignmentsForPartitions(Collections.emptySet(), partitions);
    }

    private void updateAssignmentsForPartitions(Set<TopicPartition> addedPartitions,
                                                Set<TopicPartition> removedPartitions) {
        Objects.requireNonNull(addedPartitions, "addedPartitions must not be null");
        Objects.requireNonNull(removedPartitions, "removedPartitions must not be null");

        if (addedPartitions.isEmpty() && removedPartitions.isEmpty()) {
            return;
        }

        synchronized (lock) {
            Set<TopicPartition> updatedReassignedPartitions = new HashSet<>(reassignedTopicPartitions);
            updatedReassignedPartitions.addAll(addedPartitions);
            updatedReassignedPartitions.removeAll(removedPartitions);
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
            throw new IllegalArgumentException("Given key is not of topic partition format like <topic>-<partition>.");
        }
        String topic = key.substring(0, index);
        int partition = Integer.parseInt(key.substring(index + 1));
        return new TopicPartition(topic, partition);
    }

    public boolean assignedPartition(int partition) {
        synchronized (lock) {
            return assignedMetaPartitions.contains(partition);
        }
    }

}
