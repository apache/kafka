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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StoreChangelogReader implements ChangelogReader {
    private static final Logger log = LoggerFactory.getLogger(StoreChangelogReader.class);

    private final Consumer<byte[], byte[]> consumer;
    private final String logPrefix;
    private final Time time;
    private final long partitionValidationTimeoutMs;
    private final Map<String, List<PartitionInfo>> partitionInfo = new HashMap<>();
    private final StateRestoreListener stateRestoreListener;
    private final Map<TopicPartition, StateRestorer> stateRestorers = new HashMap<>();
    private final Map<TopicPartition, StateRestorer> needsRestoring = new HashMap<>();
    private final Map<TopicPartition, StateRestorer> needsInitializing = new HashMap<>();
    private final Map<TopicPartition, Long> endOffsets = new HashMap<>();

    public StoreChangelogReader(final String threadId, final Consumer<byte[], byte[]> consumer, final Time time,
                                final long partitionValidationTimeoutMs, final StateRestoreListener stateRestoreListener) {
        this.time = time;
        this.consumer = consumer;
        this.partitionValidationTimeoutMs = partitionValidationTimeoutMs;

        this.logPrefix = String.format("stream-thread [%s]", threadId);
        this.stateRestoreListener = stateRestoreListener;
    }

    public StoreChangelogReader(final Consumer<byte[], byte[]> consumer, final Time time,
                                long partitionValidationTimeoutMs, final StateRestoreListener stateRestoreListener) {
        this("", consumer, time, partitionValidationTimeoutMs, stateRestoreListener);
    }

    @Override
    public void validatePartitionExists(final TopicPartition topicPartition, final String storeName) {
        final long start = time.milliseconds();
        // fetch all info on all topics to avoid multiple remote calls
        if (partitionInfo.isEmpty()) {
            try {
                partitionInfo.putAll(consumer.listTopics());
            } catch (final TimeoutException e) {
                log.warn("{} Could not list topics so will fall back to partition by partition fetching", logPrefix);
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
        log.debug("{} Took {}ms to validate that partition {} exists", logPrefix, time.milliseconds() - start, topicPartition);
    }

    @Override
    public void register(final StateRestorer restorer) {
        restorer.setGlobalRestoreListener(stateRestoreListener);
        stateRestorers.put(restorer.partition(), restorer);
        needsInitializing.put(restorer.partition(), restorer);
    }


    public Collection<TopicPartition> restore() {
        if (!needsInitializing.isEmpty()) {
            initialize();
        }

        if (needsRestoring.isEmpty()) {
            consumer.assign(Collections.<TopicPartition>emptyList());
            return completed();
        }

        final Set<TopicPartition> partitions = new HashSet<>(needsRestoring.keySet());
        final ConsumerRecords<byte[], byte[]> allRecords = consumer.poll(10);
        for (final TopicPartition partition : partitions) {
            restorePartition(allRecords, partition);
        }

        if (needsRestoring.isEmpty()) {
            consumer.assign(Collections.<TopicPartition>emptyList());
        }

        return completed();
    }

    private void initialize() {
        final Map<TopicPartition, StateRestorer> newTasksNeedingRestoration = new HashMap<>();

        if (!consumer.subscription().isEmpty()) {
            throw new IllegalStateException(String.format("Restore consumer should have not subscribed to any partitions (%s) beforehand", consumer.subscription()));
        }
        endOffsets.putAll(consumer.endOffsets(needsInitializing.keySet()));

        // remove any partitions where we already have all of the data
        for (final Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            Long offset = entry.getValue();
            final StateRestorer restorer = needsInitializing.get(topicPartition);
            // might be null as has was initialized in a previous invocation.
            if (restorer != null) {
                if (restorer.checkpoint() >= offset) {
                    restorer.setRestoredOffset(restorer.checkpoint());
                } else if (restorer.offsetLimit() == 0 || endOffsets.get(topicPartition) == 0) {
                    restorer.setRestoredOffset(0);
                } else {
                    newTasksNeedingRestoration.put(topicPartition, restorer);
                    final Long endOffset = endOffsets.get(topicPartition);
                    restorer.restoreStarted(restorer.startingOffset(), endOffset);
                }
            }
        }

        log.debug("{} Starting restoring state stores from changelog topics {}", logPrefix, newTasksNeedingRestoration.keySet());

        final Set<TopicPartition> assignment = new HashSet<>(consumer.assignment());
        assignment.addAll(newTasksNeedingRestoration.keySet());
        consumer.assign(assignment);
        final List<StateRestorer> needsPositionUpdate = new ArrayList<>();
        for (final StateRestorer restorer : newTasksNeedingRestoration.values()) {
            if (restorer.checkpoint() != StateRestorer.NO_CHECKPOINT) {
                consumer.seek(restorer.partition(), restorer.checkpoint());
                logRestoreOffsets(restorer.partition(),
                                  restorer.checkpoint(),
                                  endOffsets.get(restorer.partition()));
                restorer.setStartingOffset(consumer.position(restorer.partition()));
            } else {
                consumer.seekToBeginning(Collections.singletonList(restorer.partition()));
                needsPositionUpdate.add(restorer);
            }
        }

        for (final StateRestorer restorer : needsPositionUpdate) {
            final long position = consumer.position(restorer.partition());
            restorer.setStartingOffset(position);
            logRestoreOffsets(restorer.partition(),
                              position,
                              endOffsets.get(restorer.partition()));
        }

        needsRestoring.putAll(newTasksNeedingRestoration);
        needsInitializing.clear();
    }

    private void logRestoreOffsets(final TopicPartition partition, final long startingOffset, final Long endOffset) {
        log.debug("{} Restoring partition {} from offset {} to endOffset {}",
                  logPrefix,
                  partition,
                  startingOffset,
                  endOffset);
    }

    private Collection<TopicPartition> completed() {
        final Set<TopicPartition> completed = new HashSet<>(stateRestorers.keySet());
        completed.removeAll(needsRestoring.keySet());
        log.debug("{} completed partitions {}", logPrefix, completed);
        return completed;
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

    @Override
    public void reset() {
        partitionInfo.clear();
        stateRestorers.clear();
        needsRestoring.clear();
        endOffsets.clear();
        needsInitializing.clear();
    }

    private void restorePartition(final ConsumerRecords<byte[], byte[]> allRecords,
                                    final TopicPartition topicPartition) {
        final StateRestorer restorer = stateRestorers.get(topicPartition);
        final Long endOffset = endOffsets.get(topicPartition);
        final long pos = processNext(allRecords.records(topicPartition), restorer, endOffset);
        restorer.setRestoredOffset(pos);
        if (restorer.hasCompleted(pos, endOffset)) {
            if (pos > endOffset + 1) {
                throw new IllegalStateException(
                        String.format("Log end offset of %s should not change while restoring: old end offset %d, current offset %d",
                                      topicPartition,
                                      endOffset,
                                      pos));
            }

            log.debug("{} Completed restoring state from changelog {} with {} records ranging from offset {} to {}",
                      logPrefix,
                      topicPartition,
                      restorer.restoredNumRecords(),
                      restorer.startingOffset(),
                      restorer.restoredOffset());

            restorer.restoreDone();
            needsRestoring.remove(topicPartition);
        }
    }

    private long processNext(final List<ConsumerRecord<byte[], byte[]>> records,
                             final StateRestorer restorer, final Long endOffset) {
        final List<KeyValue<byte[], byte[]>> restoreRecords = new ArrayList<>();
        long nextPosition = -1;

        for (final ConsumerRecord<byte[], byte[]> record : records) {
            final long offset = record.offset();
            if (restorer.hasCompleted(offset, endOffset)) {
                nextPosition = record.offset();
                break;
            }
            if (record.key() != null) {
                restoreRecords.add(KeyValue.pair(record.key(), record.value()));
            }
        }

        if (nextPosition == -1) {
            nextPosition = consumer.position(restorer.partition());
        }

        if (!restoreRecords.isEmpty()) {
            restorer.restore(restoreRecords);
            restorer.restoreBatchCompleted(nextPosition, records.size());

        }

        return nextPosition;
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
