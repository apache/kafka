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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_NAME;

/**
 * This class is responsible for consuming messages from remote log metadata topic ({@link TopicBasedRemoteLogMetadataManagerConfig#REMOTE_LOG_METADATA_TOPIC_NAME})
 * partitions and maintain the state of the remote log segment metadata. It gives an API to add or remove
 * for what topic partition's metadata should be consumed by this instance using
 * {{@link #addAssignmentsForPartitions(Set)}} and {@link #removeAssignmentsForPartitions(Set)} respectively.
 * <p>
 * When a broker is started, controller sends topic partitions that this broker is leader or follower for and the
 * partitions to be deleted. This class receives those notifications with
 * {@link #addAssignmentsForPartitions(Set)} and {@link #removeAssignmentsForPartitions(Set)} assigns consumer for the
 * respective remote log metadata partitions by using {@link RemoteLogMetadataTopicPartitioner#metadataPartition(TopicIdPartition)}.
 * Any leadership changes later are called through the same API. We will remove the partitions that are deleted from
 * this broker which are received through {@link #removeAssignmentsForPartitions(Set)}.
 * <p>
 * After receiving these events it invokes {@link RemotePartitionMetadataEventHandler#handleRemoteLogSegmentMetadata(RemoteLogSegmentMetadata)},
 * which maintains in-memory representation of the state of {@link RemoteLogSegmentMetadata}.
 */
class ConsumerTask implements Runnable, Closeable {
    private static final Logger log = LoggerFactory.getLogger(ConsumerTask.class);

    private static final long POLL_INTERVAL_MS = 100L;

    private final RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final RemotePartitionMetadataEventHandler remotePartitionMetadataEventHandler;
    private final RemoteLogMetadataTopicPartitioner topicPartitioner;
    private final Time time;

    // It indicates whether the closing process has been started or not. If it is set as true,
    // consumer will stop consuming messages and it will not allow partition assignments to be updated.
    private volatile boolean closing = false;

    // It indicates whether the consumer needs to assign the partitions or not. This is set when it is
    // determined that the consumer needs to be assigned with the updated partitions.
    private volatile boolean assignPartitions = false;

    // It represents a lock for any operations related to the assignedTopicPartitions.
    private final Object assignPartitionsLock = new Object();

    // Remote log metadata topic partitions that consumer is assigned to.
    private volatile Set<Integer> assignedMetaPartitions = Collections.emptySet();

    // User topic partitions that this broker is a leader/follower for.
    private Set<TopicIdPartition> assignedTopicPartitions = Collections.emptySet();

    // Map of remote log metadata topic partition to consumed offsets. Received consumer records
    // may or may not have been processed based on the assigned topic partitions.
    private final Map<Integer, Long> partitionToConsumedOffsets = new ConcurrentHashMap<>();

    // Map of remote log metadata topic partition to processed offsets that were synced in committedOffsetsFile.
    private Map<Integer, Long> lastSyncedPartitionToConsumedOffsets = Collections.emptyMap();

    private final long committedOffsetSyncIntervalMs;
    private CommittedOffsetsFile committedOffsetsFile;
    private long lastSyncedTimeMs;

    public ConsumerTask(KafkaConsumer<byte[], byte[]> consumer,
                        RemotePartitionMetadataEventHandler remotePartitionMetadataEventHandler,
                        RemoteLogMetadataTopicPartitioner topicPartitioner,
                        Path committedOffsetsPath,
                        Time time,
                        long committedOffsetSyncIntervalMs) {
        this.consumer = Objects.requireNonNull(consumer);
        this.remotePartitionMetadataEventHandler = Objects.requireNonNull(remotePartitionMetadataEventHandler);
        this.topicPartitioner = Objects.requireNonNull(topicPartitioner);
        this.time = Objects.requireNonNull(time);
        this.committedOffsetSyncIntervalMs = committedOffsetSyncIntervalMs;

        initializeConsumerAssignment(committedOffsetsPath);
    }

    private void initializeConsumerAssignment(Path committedOffsetsPath) {
        try {
            committedOffsetsFile = new CommittedOffsetsFile(committedOffsetsPath.toFile());
        } catch (IOException e) {
            throw new KafkaException(e);
        }

        Map<Integer, Long> committedOffsets = Collections.emptyMap();
        try {
            // Load committed offset and assign them in the consumer.
            committedOffsets = committedOffsetsFile.readEntries();
        } catch (IOException e) {
            // Ignore the error and consumer consumes from the earliest offset.
            log.error("Encountered error while building committed offsets from the file. " +
                              "Consumer will consume from the earliest offset for the assigned partitions.", e);
        }

        if (!committedOffsets.isEmpty()) {
            // Assign topic partitions from the earlier committed offsets file.
            Set<Integer> earlierAssignedPartitions = committedOffsets.keySet();
            assignedMetaPartitions = Collections.unmodifiableSet(earlierAssignedPartitions);
            Set<TopicPartition> metadataTopicPartitions = earlierAssignedPartitions.stream()
                                                                                   .map(x -> new TopicPartition(REMOTE_LOG_METADATA_TOPIC_NAME, x))
                                                                                   .collect(Collectors.toSet());
            consumer.assign(metadataTopicPartitions);

            // Seek to the committed offsets
            for (Map.Entry<Integer, Long> entry : committedOffsets.entrySet()) {
                partitionToConsumedOffsets.put(entry.getKey(), entry.getValue());
                consumer.seek(new TopicPartition(REMOTE_LOG_METADATA_TOPIC_NAME, entry.getKey()), entry.getValue());
            }

            lastSyncedPartitionToConsumedOffsets = Collections.unmodifiableMap(committedOffsets);
        }
    }

    @Override
    public void run() {
        log.info("Started Consumer task thread.");
        lastSyncedTimeMs = time.milliseconds();
        try {
            while (!closing) {
                maybeWaitForPartitionsAssignment();

                log.info("Polling consumer to receive remote log metadata topic records");
                ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofMillis(POLL_INTERVAL_MS));
                for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                    processConsumerRecord(record);
                }

                maybeSyncCommittedDataAndOffsets(false);
            }
        } catch (Exception e) {
            log.error("Error occurred in consumer task, close:[{}]", closing, e);
        } finally {
            maybeSyncCommittedDataAndOffsets(true);
            closeConsumer();
            log.info("Exiting from consumer task thread");
        }
    }

    private void processConsumerRecord(ConsumerRecord<byte[], byte[]> record) {
        // Taking assignPartitionsLock here as updateAssignmentsForPartitions changes assignedTopicPartitions
        // and also calls remotePartitionMetadataEventHandler.clearTopicPartition(removedPartition) for the removed
        // partitions.
        RemoteLogMetadata remoteLogMetadata = serde.deserialize(record.value());
        synchronized (assignPartitionsLock) {
            if (assignedTopicPartitions.contains(remoteLogMetadata.topicIdPartition())) {
                remotePartitionMetadataEventHandler.handleRemoteLogMetadata(remoteLogMetadata);
            } else {
                log.debug("This event {} is skipped as the topic partition is not assigned for this instance.", remoteLogMetadata);
            }
            partitionToConsumedOffsets.put(record.partition(), record.offset());
        }
    }

    private void maybeSyncCommittedDataAndOffsets(boolean forceSync) {
        // Return immediately if there is no consumption from last time.
        boolean noConsumedOffsetUpdates = partitionToConsumedOffsets.equals(lastSyncedPartitionToConsumedOffsets);
        if (noConsumedOffsetUpdates || !forceSync && time.milliseconds() - lastSyncedTimeMs < committedOffsetSyncIntervalMs) {
            log.debug("Skip syncing committed offsets, noConsumedOffsetUpdates: {}, forceSync: {}", noConsumedOffsetUpdates, forceSync);
            return;
        }

        try {
            // Need to take lock on assignPartitionsLock as assignedTopicPartitions might
            // get updated by other threads.
            synchronized (assignPartitionsLock) {
                for (TopicIdPartition topicIdPartition : assignedTopicPartitions) {
                    int metadataPartition = topicPartitioner.metadataPartition(topicIdPartition);
                    Long offset = partitionToConsumedOffsets.get(metadataPartition);
                    if (offset != null) {
                        remotePartitionMetadataEventHandler.syncLogMetadataSnapshot(topicIdPartition, metadataPartition, offset);
                    } else {
                        log.debug("Skipping syncup of the remote-log-metadata-file for partition:{} , with remote log metadata partition{}, and no offset",
                                topicIdPartition, metadataPartition);
                    }
                }

                // Write partitionToConsumedOffsets into committed offsets file as we do not want to process them again
                // in case of restarts.
                committedOffsetsFile.writeEntries(partitionToConsumedOffsets);
                lastSyncedPartitionToConsumedOffsets = new HashMap<>(partitionToConsumedOffsets);
            }

            lastSyncedTimeMs = time.milliseconds();
        } catch (IOException e) {
            throw new KafkaException("Error encountered while writing committed offsets to a local file", e);
        }
    }

    private void closeConsumer() {
        log.info("Closing the consumer instance");
        try {
            consumer.close(Duration.ofSeconds(30));
        } catch (Exception e) {
            log.error("Error encountered while closing the consumer", e);
        }
    }

    private void maybeWaitForPartitionsAssignment() {
        Set<Integer> assignedMetaPartitionsSnapshot = Collections.emptySet();
        synchronized (assignPartitionsLock) {
            // If it is closing, return immediately. This should be inside the assignPartitionsLock as the closing is updated
            // in close() method with in the same lock to avoid any race conditions.
            if (closing) {
                return;
            }

            while (assignedMetaPartitions.isEmpty()) {
                // If no partitions are assigned, wait until they are assigned.
                log.debug("Waiting for assigned remote log metadata partitions..");
                try {
                    // No timeout is set here, as it is always notified. Even when it is closed, the race can happen
                    // between the thread calling this method and the thread calling close(). We should have a check
                    // for closing as that might have been set and notified with assignPartitionsLock by `close`
                    // method.
                    assignPartitionsLock.wait();

                    if (closing) {
                        return;
                    }
                } catch (InterruptedException e) {
                    throw new KafkaException(e);
                }
            }

            if (assignPartitions) {
                assignedMetaPartitionsSnapshot = new HashSet<>(assignedMetaPartitions);
                // Removing unassigned meta partitions from partitionToConsumedOffsets and partitionToCommittedOffsets
                partitionToConsumedOffsets.entrySet().removeIf(entry -> !assignedMetaPartitions.contains(entry.getKey()));

                assignPartitions = false;
            }
        }

        if (!assignedMetaPartitionsSnapshot.isEmpty()) {
            executeReassignment(assignedMetaPartitionsSnapshot);
        }
    }

    private void executeReassignment(Set<Integer> assignedMetaPartitionsSnapshot) {
        Set<TopicPartition> assignedMetaTopicPartitions =
                assignedMetaPartitionsSnapshot.stream()
                                              .map(partitionNum -> new TopicPartition(REMOTE_LOG_METADATA_TOPIC_NAME, partitionNum))
                                              .collect(Collectors.toSet());
        log.info("Reassigning partitions to consumer task [{}]", assignedMetaTopicPartitions);
        consumer.assign(assignedMetaTopicPartitions);
    }

    public void addAssignmentsForPartitions(Set<TopicIdPartition> partitions) {
        updateAssignmentsForPartitions(partitions, Collections.emptySet());
    }

    public void removeAssignmentsForPartitions(Set<TopicIdPartition> partitions) {
        updateAssignmentsForPartitions(Collections.emptySet(), partitions);
    }

    private void updateAssignmentsForPartitions(Set<TopicIdPartition> addedPartitions,
                                                Set<TopicIdPartition> removedPartitions) {
        log.info("Updating assignments for addedPartitions: {} and removedPartition: {}", addedPartitions, removedPartitions);

        Objects.requireNonNull(addedPartitions, "addedPartitions must not be null");
        Objects.requireNonNull(removedPartitions, "removedPartitions must not be null");

        if (addedPartitions.isEmpty() && removedPartitions.isEmpty()) {
            return;
        }

        synchronized (assignPartitionsLock) {
            Set<TopicIdPartition> updatedReassignedPartitions = new HashSet<>(assignedTopicPartitions);
            updatedReassignedPartitions.addAll(addedPartitions);
            updatedReassignedPartitions.removeAll(removedPartitions);
            Set<Integer> updatedAssignedMetaPartitions = new HashSet<>();
            for (TopicIdPartition tp : updatedReassignedPartitions) {
                updatedAssignedMetaPartitions.add(topicPartitioner.metadataPartition(tp));
            }

            // Clear removed topic partitions from inmemory cache.
            for (TopicIdPartition removedPartition : removedPartitions) {
                remotePartitionMetadataEventHandler.clearTopicPartition(removedPartition);
            }

            assignedTopicPartitions = Collections.unmodifiableSet(updatedReassignedPartitions);
            log.debug("Assigned topic partitions: {}", assignedTopicPartitions);

            if (!updatedAssignedMetaPartitions.equals(assignedMetaPartitions)) {
                assignedMetaPartitions = Collections.unmodifiableSet(updatedAssignedMetaPartitions);
                log.debug("Assigned metadata topic partitions: {}", assignedMetaPartitions);

                assignPartitions = true;
                assignPartitionsLock.notifyAll();
            } else {
                log.debug("No change in assigned metadata topic partitions: {}", assignedMetaPartitions);
            }
        }
    }

    public Optional<Long> receivedOffsetForPartition(int partition) {
        return Optional.ofNullable(partitionToConsumedOffsets.get(partition));
    }

    public boolean isPartitionAssigned(int partition) {
        return assignedMetaPartitions.contains(partition);
    }

    public void close() {
        if (!closing) {
            synchronized (assignPartitionsLock) {
                // Closing should be updated only after acquiring the lock to avoid race in
                // maybeWaitForPartitionsAssignment() where it waits on assignPartitionsLock. It should not wait
                // if the closing is already set.
                closing = true;
                consumer.wakeup();
                assignPartitionsLock.notifyAll();
            }
        }
    }
}
