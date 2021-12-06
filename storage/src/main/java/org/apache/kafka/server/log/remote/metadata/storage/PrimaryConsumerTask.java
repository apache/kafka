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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
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
import java.util.concurrent.ConcurrentMap;
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
class PrimaryConsumerTask implements Runnable, Closeable {
    private static final Logger log = LoggerFactory.getLogger(PrimaryConsumerTask.class);
    private static final long POLL_INTERVAL_MS = 100L;

    private final RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final RemotePartitionMetadataEventHandler handler;
    private final RemoteLogMetadataTopicPartitioner partitioner;
    private final Time time;
    private final SecondaryConsumerTask secondaryConsumerTask;

    // It indicates whether the closing process has been started or not. If it is set as true,
    // consumer will stop consuming messages and it will not allow partition assignments to be updated.
    private volatile boolean closing = false;
    // It indicates whether the consumer needs to assign the partitions or not. This is set when it is
    // determined that the consumer needs to be assigned with the updated partitions.
    private volatile boolean assignmentChanged = false;

    private final Object assignmentLock = new Object();

    // Remote log metadata topic partitions that primary consumer is assigned to.
    private volatile Set<Integer> assignedMetaPartitions = Collections.emptySet();

    // User topic partitions that this broker is a leader/follower for.
    private Set<TopicIdPartition> assignedUserTopicPartitions = Collections.emptySet();

    // Map of remote log metadata topic partition to consumed offsets.
    private final ConcurrentMap<Integer, Long> readOffsetsByPartition = new ConcurrentHashMap<>();
    private Map<Integer, Long> committedOffsetsByPartition = Collections.emptyMap();

    private final long offsetSyncIntervalMs;
    private CommittedOffsetsFile offsetsFile;
    private volatile long lastSyncedTimeMs;
    private final Object syncCommittedDataLock = new Object();

    public PrimaryConsumerTask(Map<String, Object> consumerProperties,
                               RemotePartitionMetadataEventHandler handler,
                               RemoteLogMetadataTopicPartitioner partitioner,
                               Path committedOffsetsPath,
                               long secondaryConsumerSubscriptionIntervalMs,
                               Time time,
                               long offsetSyncIntervalMs) {
        this.handler = Objects.requireNonNull(handler);
        this.partitioner = Objects.requireNonNull(partitioner);
        this.time = Objects.requireNonNull(time);
        this.offsetSyncIntervalMs = offsetSyncIntervalMs;

        consumer = createPrimaryConsumer(consumerProperties);
        secondaryConsumerTask = new SecondaryConsumerTask(consumerProperties, secondaryConsumerSubscriptionIntervalMs, time, partitioner, serde, handler,
                POLL_INTERVAL_MS);
        initialize(committedOffsetsPath);
    }

    private KafkaConsumer<byte[], byte[]> createPrimaryConsumer(Map<String, Object> consumerProperties) {
        Map<String, Object> props = new HashMap<>(consumerProperties);
        props.put(CommonClientConfigs.CLIENT_ID_CONFIG,
                props.getOrDefault(CommonClientConfigs.CLIENT_ID_CONFIG, "rlmm_consumer_") + " _primary");
        return new KafkaConsumer<>(props);
    }

    private void initialize(Path committedOffsetsPath) {
        // look whether the committed file exists or not.
        File file = committedOffsetsPath.toFile();
        offsetsFile = new CommittedOffsetsFile(file);
        try {
            if (file.createNewFile()) {
                log.info("Created file: [{}] successfully", file);
            } else {
                // load committed offset and assign them in the consumer
                final Map<Integer, Long> committedOffsets = offsetsFile.read();
                if (!committedOffsets.isEmpty()) {
                    // assign topic partitions from the earlier committed offsets file.
                    assignedMetaPartitions = Collections.unmodifiableSet(committedOffsets.keySet());
                    Set<TopicPartition> metadataTopicPartitions = assignedMetaPartitions.stream()
                            .map(x -> new TopicPartition(REMOTE_LOG_METADATA_TOPIC_NAME, x))
                            .collect(Collectors.toSet());
                    consumer.assign(metadataTopicPartitions);
                    committedOffsets.forEach((partition, offset) -> {
                        readOffsetsByPartition.put(partition, offset);
                        consumer.seek(new TopicPartition(REMOTE_LOG_METADATA_TOPIC_NAME, partition), offset + 1);
                    });
                }
            }
        } catch (IOException e) {
            // Ignore the error and consumer consumes from the earliest offset.
            log.error("Encountered error while building committed offsets from the file", e);
        }
    }

    @Override
    public void run() {
        log.info("Started Consumer task thread.");
        lastSyncedTimeMs = time.milliseconds();
        try {
            while (!closing) {
                // Wait for primary partition assignments.
                maybeWaitForPartitionsAssignmentToPrimaryConsumer();

                // Check and consume from secondary if needed.
                boolean continuePrimaryConsumption =
                        secondaryConsumerTask.maybeConsumeFromSecondaryConsumer(
                                Collections.unmodifiableMap(readOffsetsByPartition),
                                this::resumePartitionsForPrimaryConsumption);

                // Consume from primary as the catchup is already done.
                if (continuePrimaryConsumption) {
                    consumeFromPrimaryConsumer();
                }
            }
        } catch (WakeupException ex) {
            // ignore
        } catch (Exception e) {
            log.error("Error occurred in consumer task, close:[{}]", closing, e);
        } finally {
            closeConsumers();
        }

        log.info("Exiting from consumer task thread");
    }

    private void consumeFromPrimaryConsumer() {
        ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofMillis(POLL_INTERVAL_MS));
        log.debug("Processing {} records received from remote log metadata topic", consumerRecords.count());
        for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
            handleRemoteLogMetadata(serde.deserialize(record.value()));
            readOffsetsByPartition.put(record.partition(), record.offset());
        }
        syncCommittedDataAndOffsets(false);
    }

    private void resumePartitionsForPrimaryConsumption(Set<TopicIdPartition> newPartitions,
                                                       Map<Integer, Long> consumedPartitionToOffsets) {
        // assign these partitions to primary consumer and also set the offsets to consume from.
        assignPartitionsForPrimaryConsumption(newPartitions, Collections.emptySet());
        executeReassignmentAndSeek(assignedMetaPartitions, consumedPartitionToOffsets);
    }

    private void syncCommittedDataAndOffsets(boolean forceSync) {
        synchronized (syncCommittedDataLock) {
            boolean notChanged = committedOffsetsByPartition.equals(readOffsetsByPartition);
            if (notChanged || !forceSync && time.milliseconds() - lastSyncedTimeMs < offsetSyncIntervalMs) {
                log.debug("Skip syncing committed offsets, notChanged: {}, forceSync: {}", notChanged, forceSync);
                return;
            }
            try {
                for (TopicIdPartition topicIdPartition: assignedUserTopicPartitions) {
                    int metadataPartition = partitioner.metadataPartition(topicIdPartition);
                    Long consumedOffset = readOffsetsByPartition.get(metadataPartition);
                    if (consumedOffset != null) {
                        handler.syncLogMetadataDataFile(topicIdPartition, metadataPartition, consumedOffset);
                    }
                }
                offsetsFile.write(readOffsetsByPartition);
                committedOffsetsByPartition = new HashMap<>(readOffsetsByPartition);
                lastSyncedTimeMs = time.milliseconds();
            } catch (IOException e) {
                log.error("Error encountered while writing committed offsets to a local file", e);
            }
        }
    }

    private void closeConsumers() {
        log.info("Closing the consumer instances");
        try {
            secondaryConsumerTask.closeConsumer();
        } catch (Exception e) {
            log.error("Error encountered while closing the secondary consumer", e);
        }
        try {
            consumer.close(Duration.ofSeconds(30));
        } catch (Exception e) {
            log.error("Error encountered while closing the primary consumer", e);
        }
    }

    private void maybeWaitForPartitionsAssignmentToPrimaryConsumer() {
        Set<Integer> assignedMetaPartitionsSnapshot = Collections.emptySet();
        synchronized (assignmentLock) {
            // If it is closing, return immediately. This should be inside the assignPartitionsLock as the closing is updated
            // in close() method with in the same lock to avoid any race conditions.
            if (closing) {
                return;
            }
            while (!closing && assignedMetaPartitions.isEmpty()) {
                // If no partitions are assigned, wait until they are assigned.
                log.debug("Waiting for assigned remote log metadata partitions..");
                try {
                    // No timeout is set here, as it is always notified. Even when it is closed, the race can happen
                    // between the thread calling this method and the thread calling close() but closing check earlier
                    // will guard against not coming here after closing is set and notify is invoked.
                    assignmentLock.wait();
                } catch (InterruptedException e) {
                    throw new KafkaException(e);
                }
            }
            if (assignmentChanged) {
                assignedMetaPartitionsSnapshot = new HashSet<>(assignedMetaPartitions);
                assignmentChanged = false;
            }
        }
        if (!assignedMetaPartitionsSnapshot.isEmpty()) {
            executeReassignmentAndSeek(assignedMetaPartitionsSnapshot, Collections.emptyMap());
        }
    }

    private void handleRemoteLogMetadata(RemoteLogMetadata metadata) {
        if (assignedUserTopicPartitions.contains(metadata.topicIdPartition())) {
            handler.handleRemoteLogMetadata(metadata);
        } else {
            log.debug("This event {} is skipped as the topic partition is not assigned for this instance.", metadata);
        }
    }

    private void executeReassignmentAndSeek(Set<Integer> metadataPartitions,
                                            Map<Integer, Long> offsetsByPartition) {
        Set<TopicPartition> assignedMetaTopicPartitions = metadataPartitions.stream()
                .map(partitionNum -> new TopicPartition(REMOTE_LOG_METADATA_TOPIC_NAME, partitionNum))
                .collect(Collectors.toSet());
        log.info("Reassigning partitions to consumer task [{}]", assignedMetaTopicPartitions);
        consumer.assign(assignedMetaTopicPartitions);
        offsetsByPartition.forEach((partition, offset) ->
                consumer.seek(new TopicPartition(REMOTE_LOG_METADATA_TOPIC_NAME, partition), offset + 1));
    }

    public void addAssignmentsForPartitions(Set<TopicIdPartition> partitions) {
        updateAssignmentsForPartitions(partitions, Collections.emptySet());
    }

    public void removeAssignmentsForPartitions(Set<TopicIdPartition> partitions) {
        updateAssignmentsForPartitions(Collections.emptySet(), partitions);
    }

    private void updateAssignmentsForPartitions(Set<TopicIdPartition> addedPartitions,
                                                Set<TopicIdPartition> removedPartitions) {
        Objects.requireNonNull(addedPartitions, "addedPartitions must not be null");
        Objects.requireNonNull(removedPartitions, "removedPartitions must not be null");
        log.info("Updating assignments for addedPartitions: {} and removedPartition: {}", addedPartitions,
                removedPartitions);
        if (addedPartitions.isEmpty() && removedPartitions.isEmpty()) {
            return;
        }
        synchronized (assignmentLock) {
            if (assignedUserTopicPartitions.isEmpty()) {
                assignPartitionsForPrimaryConsumption(addedPartitions, removedPartitions);
                return;
            }
            // Find out the new assigned user partitions.
            // Start them from the earliest offset in the secondary consumer.
            // Once it catches up, move them to the primary consumer and clear from secondary consumer.
            Set<TopicIdPartition> userPartitionsToCatchup = new HashSet<>();
            for (TopicIdPartition addedPartition: addedPartitions) {
                if (!assignedUserTopicPartitions.contains(addedPartition)) {
                    userPartitionsToCatchup.add(addedPartition);
                }
            }
            if (!userPartitionsToCatchup.isEmpty()) {
                // Add the new user partitions to catchup to the existing partitions.
                log.debug("New user partitions to catchup: [{}]", userPartitionsToCatchup);
                secondaryConsumerTask.addPartitions(userPartitionsToCatchup);
            }
            assignPartitionsForPrimaryConsumption(Collections.emptySet(), removedPartitions);
        }
    }

    private void assignPartitionsForPrimaryConsumption(Set<TopicIdPartition> addedPartitions,
                                                       Set<TopicIdPartition> removedPartitions) {
        Set<TopicIdPartition> idealUserPartitions = new HashSet<>(assignedUserTopicPartitions);
        idealUserPartitions.addAll(addedPartitions);
        idealUserPartitions.removeAll(removedPartitions);

        Set<Integer> idealMetaPartitions = new HashSet<>();
        for (TopicIdPartition tp : idealUserPartitions) {
            idealMetaPartitions.add(partitioner.metadataPartition(tp));
        }
        synchronized (assignmentLock) {
            assignedUserTopicPartitions = Collections.unmodifiableSet(idealUserPartitions);
            log.debug("Assigned topic partitions: {}", assignedUserTopicPartitions);
            if (!idealMetaPartitions.equals(assignedMetaPartitions)) {
                assignedMetaPartitions = Collections.unmodifiableSet(idealMetaPartitions);
                log.debug("Assigned metadata topic partitions: {}", assignedMetaPartitions);
                assignmentChanged = true;
                assignmentLock.notifyAll();
            } else {
                log.debug("No change in assigned metadata topic partitions: {}", assignedMetaPartitions);
            }
        }
    }

    public Optional<Long> receivedOffsetForPartition(int partition) {
        return Optional.ofNullable(readOffsetsByPartition.get(partition));
    }

    public boolean isMetadataPartitionAssigned(int partition) {
        return assignedMetaPartitions.contains(partition);
    }

    public boolean isUserPartitionAssignedToPrimary(TopicIdPartition partition) {
        return assignedUserTopicPartitions.contains(partition);
    }

    public void close() {
        if (!closing) {
            synchronized (assignmentLock) {
                // Closing should be updated only after acquiring the lock to avoid race in
                // maybeWaitForPartitionsAssignment() where it waits on assignPartitionsLock. It should not wait
                // if the closing is already set.
                closing = true;
                try {
                    secondaryConsumerTask.close();
                } catch (Exception e) {
                    // ignore error;
                }
                try {
                    consumer.wakeup();
                } catch (Exception e) {
                    // ignore error.
                }
                // Resources are closed through closeConsumers() when the thread is completed in #run() method.
                assignmentLock.notifyAll();
                syncCommittedDataAndOffsets(true);
            }
        }
    }

}
