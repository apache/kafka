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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.StreamsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StoreChangelogReader implements ChangelogReader {
    private static final Logger log = LoggerFactory.getLogger(StoreChangelogReader.class);

    private final Consumer<byte[], byte[]> consumer;
    private final Time time;
    private final long partitionValidationTimeoutMs;
    private final Map<String, List<PartitionInfo>> partitionInfo = new HashMap<>();
    private final Map<TopicPartition, StateRestorer> stateRestorers = new HashMap<>();


    public StoreChangelogReader(final Consumer<byte[], byte[]> consumer, final Time time, final long partitionValidationTimeoutMs) {
        this.consumer = consumer;
        this.time = time;
        this.partitionValidationTimeoutMs = partitionValidationTimeoutMs;
    }


    @Override
    public void validatePartitionExists(final TopicPartition topicPartition, final String storeName) {
        final long start = time.milliseconds();
        // fetch all info on all topics to avoid multiple remote calls
        if (partitionInfo.isEmpty()) {
            try {
                partitionInfo.putAll(consumer.listTopics());
            } catch (final TimeoutException e) {
                log.warn("Could not list topics so will fall back to partition by partition fetching");
            }
        }

        final long endTime = time.milliseconds() + partitionValidationTimeoutMs;
        while (!hasPartition(topicPartition) && time.milliseconds() < endTime) {
            try {
                final List<PartitionInfo> partitions = consumer.partitionsFor(topicPartition.topic());
                if (partitions != null) {
                    partitionInfo.put(topicPartition.topic(), partitions);
                }
            } catch (final TimeoutException e) {
                throw new StreamsException(String.format("Could not fetch partition info for topic: %s before expiration of the configured request timeout",
                                                         topicPartition.topic()));
            }
        }

        if (!hasPartition(topicPartition)) {
            throw new StreamsException(String.format("Store %s's change log (%s) does not contain partition %s",
                                                     storeName, topicPartition.topic(), topicPartition.partition()));
        }
        log.debug("Took {} ms to validate that partition {} exists", time.milliseconds() - start, topicPartition);
    }

    @Override
    public void register(final StateRestorer restorer) {
        if (restorer.offsetLimit() > 0) {
            stateRestorers.put(restorer.partition(), restorer);
        }
    }

    public void restore() {
        final long start = time.milliseconds();
        try {
            if (!consumer.subscription().isEmpty()) {
                throw new IllegalStateException(String.format("Restore consumer should have not subscribed to any partitions (%s) beforehand", consumer.subscription()));
            }
            final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(stateRestorers.keySet());


            // remove any partitions where we already have all of the data
            final Map<TopicPartition, StateRestorer> needsRestoring = new HashMap<>();
            for (final TopicPartition topicPartition : endOffsets.keySet()) {
                final StateRestorer restorer = stateRestorers.get(topicPartition);
                if (restorer.checkpoint() >= endOffsets.get(topicPartition)) {
                    restorer.setRestoredOffset(restorer.checkpoint());
                } else {
                    needsRestoring.put(topicPartition, restorer);
                }
            }

            consumer.assign(needsRestoring.keySet());

            for (final StateRestorer restorer : needsRestoring.values()) {
                if (restorer.checkpoint() != StateRestorer.NO_CHECKPOINT) {
                    consumer.seek(restorer.partition(), restorer.checkpoint());
                } else {
                    consumer.seekToBeginning(Collections.singletonList(restorer.partition()));
                }
            }

            final Set<TopicPartition> partitions = new HashSet<>(needsRestoring.keySet());
            while (!partitions.isEmpty()) {
                final ConsumerRecords<byte[], byte[]> allRecords = consumer.poll(10);
                final Iterator<TopicPartition> partitionIterator = partitions.iterator();
                while (partitionIterator.hasNext()) {
                    restorePartition(endOffsets, allRecords, partitionIterator);
                }
            }
        } finally {
            consumer.assign(Collections.<TopicPartition>emptyList());
            log.debug("Took {} ms to restore active state", time.milliseconds() - start);
        }
    }

    @Override
    public Map<TopicPartition, Long> restoredOffsets() {
        final Map<TopicPartition, Long> restoredOffsets = new HashMap<>();
        for (final Map.Entry<TopicPartition, StateRestorer> entry : stateRestorers.entrySet()) {
            final StateRestorer restorer = entry.getValue();
            if (restorer.isPersistent()) {
                restoredOffsets.put(entry.getKey(), restorer.restoredOffset());
            }
        }
        return restoredOffsets;
    }

    private void restorePartition(final Map<TopicPartition, Long> endOffsets,
                                  final ConsumerRecords<byte[], byte[]> allRecords,
                                  final Iterator<TopicPartition> partitionIterator) {
        final TopicPartition topicPartition = partitionIterator.next();
        final StateRestorer restorer = stateRestorers.get(topicPartition);
        final Long endOffset = endOffsets.get(topicPartition);
        final long pos = processNext(allRecords.records(topicPartition), restorer, endOffset);
        if (restorer.hasCompleted(pos, endOffset)) {
            if (pos > endOffset + 1) {
                throw new IllegalStateException(
                        String.format("Log end offset of %s should not change while restoring: old end offset %d, current offset %d",
                                      topicPartition,
                                      endOffset,
                                      pos));
            }
            restorer.setRestoredOffset(pos);
            partitionIterator.remove();
        }
    }

    private long processNext(final List<ConsumerRecord<byte[], byte[]>> records, final StateRestorer restorer, final Long endOffset) {
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            final long offset = record.offset();
            if (restorer.hasCompleted(offset, endOffset)) {
                return offset;
            }
            restorer.restore(record.key(), record.value());
        }
        return consumer.position(restorer.partition());
    }

    private boolean hasPartition(final TopicPartition topicPartition) {
        final List<PartitionInfo> partitions = partitionInfo.get(topicPartition.topic());

        if (partitions == null) {
            return false;
        }

        for (final PartitionInfo partition : partitions) {
            if (partition.partition() == topicPartition.partition()) {
                return true;
            }
        }

        return false;

    }


}
