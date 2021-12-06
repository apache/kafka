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
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_NAME;

public class SecondaryConsumerTask {
    private static final Logger log = LoggerFactory.getLogger(SecondaryConsumerTask.class);

    private final KafkaConsumer<byte[], byte[]> consumer;
    private long lastProcessedTimestamp;
    private final long subscriptionIntervalMs;
    private final Time time;
    private final RemoteLogMetadataTopicPartitioner partitioner;
    private final RemoteLogMetadataSerde serde;
    private final RemotePartitionMetadataEventHandler handler;
    private final long pollIntervalMs;
    private volatile boolean closing = false;

    // User topic partitions that need to be catch up with secondary consumer.
    private final UserPartitions userPartitions = new UserPartitions();

    public SecondaryConsumerTask(Map<String, Object> consumerProperties,
                                 long subscriptionIntervalMs,
                                 Time time,
                                 RemoteLogMetadataTopicPartitioner partitioner,
                                 RemoteLogMetadataSerde serde,
                                 RemotePartitionMetadataEventHandler handler,
                                 long pollIntervalMs) {
        this.consumer = createSecondaryConsumer(consumerProperties);
        this.subscriptionIntervalMs = subscriptionIntervalMs;
        this.time = time;
        this.partitioner = partitioner;
        this.serde = serde;
        this.handler = handler;
        this.pollIntervalMs = pollIntervalMs;
        lastProcessedTimestamp = time.milliseconds();
    }

    private KafkaConsumer<byte[], byte[]> createSecondaryConsumer(Map<String, Object> consumerProperties) {
        Map<String, Object> props = new HashMap<>(consumerProperties);
        props.put(CommonClientConfigs.CLIENT_ID_CONFIG,
                  props.getOrDefault(CommonClientConfigs.CLIENT_ID_CONFIG, "rlmm_consumer_") + " _secondary");
        return new KafkaConsumer<>(props);
    }

    public boolean maybeConsumeFromSecondaryConsumer(Map<Integer, Long> metadataPartitionToConsumedOffsets,
                                                     BiConsumer<Set<TopicIdPartition>, Map<Integer, Long>> resumePartitionsForPrimaryConsumption) {
        // If there are no partitions remaining for secondary consumption, then return false. So that the primary
        // consumer can start consuming.
        if (userPartitions.isEmpty() ||
                (time.milliseconds() - lastProcessedTimestamp) < subscriptionIntervalMs) {
            // Nothing to consume with the secondary consumer for now, return true indicating to consume from
            // the primary consumer.
            return true;
        }
        try {
            // Compute metadata partitions for the respective user partitions.
            Set<TopicIdPartition> assignedUserPartitions = userPartitions.removeAll();
            Set<Integer> metadataPartitions = new HashSet<>();
            Set<TopicPartition> metadataTopicPartitions = new HashSet<>();
            for (TopicIdPartition topicIdPartition: assignedUserPartitions) {
                int metadataPartition = partitioner.metadataPartition(topicIdPartition);
                metadataPartitions.add(metadataPartition);
                metadataTopicPartitions.add(new TopicPartition(REMOTE_LOG_METADATA_TOPIC_NAME, metadataPartition));
            }
            log.info("Assigning secondary consumer partitions with [{}]", metadataTopicPartitions);
            // Assign userPartitions with the secondary consumer.
            consumer.assign(metadataTopicPartitions);
            // Start consuming from the beginning.
            consumer.seekToBeginning(metadataTopicPartitions);

            // Compute end-offsets, if the primary consumer is already consuming then consume until the processed offset and
            // start processing from that offset when these partitions are moved to the primary consumer.
            Map<TopicPartition, Long> targetOffsets = new HashMap<>();
            consumer.endOffsets(metadataTopicPartitions).forEach(
                (topicPartition, endOffset) -> targetOffsets.put(topicPartition, endOffset - 1));

            metadataPartitionToConsumedOffsets.forEach((partition, offset) -> {
                if (metadataPartitions.contains(partition)) {
                    targetOffsets.merge(new TopicPartition(REMOTE_LOG_METADATA_TOPIC_NAME, partition), offset, Math::min);
                }
            });

            Set<Integer> remainingPartitions = new HashSet<>(metadataPartitions);
            Map<Integer, Long> consumedOffsets = new HashMap<>();
            // Check whether the existing consumption is finished or not. If not, continue processing from the existing
            // assignment. Continuously process them until it reaches the target end-offsets.
            while (!closing && !remainingPartitions.isEmpty()) {
                ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofMillis(pollIntervalMs));
                log.debug("Processing {} records received from remote log metadata topic", consumerRecords.count());
                for (ConsumerRecord<byte[], byte[]> record: consumerRecords) {
                    // Store the received metadata. Respective store is already created and loaded when a user partition is
                    // assigned in TopicBasedRemoteLogMetadataManager.
                    RemoteLogMetadata remoteLogMetadata = serde.deserialize(record.value());
                    if (assignedUserPartitions.contains(remoteLogMetadata.topicIdPartition())) {
                        handler.handleRemoteLogMetadata(remoteLogMetadata);
                    }
                    consumedOffsets.put(record.partition(), record.offset());
                }
                List<TopicPartition> completedPartitions = new ArrayList<>();
                targetOffsets.forEach((tp, endOffset) -> {
                    if (consumedOffsets.getOrDefault(tp.partition(), -1L) >= endOffset) {
                        remainingPartitions.remove(tp.partition());
                        completedPartitions.add(tp);
                    }
                });
                consumer.pause(completedPartitions);
            }

            // Move these user partitions to the primary consumer. In primary consumer, start consuming from the
            // offsets until the primary and secondary consumers have processed.
            Map<Integer, Long> consumedPartitionToOffsets = new HashMap<>(metadataPartitionToConsumedOffsets);
            consumedPartitionToOffsets.putAll(consumedOffsets);

            // Finished consuming from secondary assignments, returning true indicates to consume from primary assignments.
            consumer.unsubscribe();
            resumePartitionsForPrimaryConsumption.accept(assignedUserPartitions, consumedPartitionToOffsets);
            return true;
        } finally {
            lastProcessedTimestamp = time.milliseconds();
            if (closing) {
                closeConsumer();
            }
        }
    }

    public void closeConsumer() {
        consumer.close(Duration.ofSeconds(30));
    }

    public void addPartitions(Set<TopicIdPartition> userPartitionsToCatchup) {
        userPartitions.addAll(userPartitionsToCatchup);
    }

    public void close() {
        if (!closing) {
            closing = true;
            consumer.wakeup();
        }
    }

    private static class UserPartitions {
        private final Set<TopicIdPartition> topicIdPartitions = new HashSet<>();

        public synchronized boolean isEmpty() {
            return topicIdPartitions.isEmpty();
        }

        public synchronized Set<TopicIdPartition> removeAll() {
            if (topicIdPartitions.isEmpty()) {
                return Collections.emptySet();
            } else {
                Set<TopicIdPartition> result = new HashSet<>(topicIdPartitions);
                topicIdPartitions.clear();
                return result;
            }
        }

        public synchronized boolean contains(TopicIdPartition topicIdPartition) {
            return topicIdPartitions.contains(topicIdPartition);
        }

        public synchronized void addAll(Set<TopicIdPartition> partitions) {
            topicIdPartitions.addAll(partitions);
        }
    }
}
