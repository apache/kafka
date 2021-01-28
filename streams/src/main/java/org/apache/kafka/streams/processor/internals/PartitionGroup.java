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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Function;

/**
 * PartitionGroup is used to buffer all co-partitioned records for processing.
 *
 * In other words, it represents the "same" partition over multiple co-partitioned topics, and it is used
 * to buffer records from that partition in each of the contained topic-partitions.
 * Each StreamTask has exactly one PartitionGroup.
 *
 * PartitionGroup implements the algorithm that determines in what order buffered records are selected for processing.
 *
 * Specifically, when polled, it returns the record from the topic-partition with the lowest stream-time.
 * Stream-time for a topic-partition is defined as the highest timestamp
 * yet observed at the head of that topic-partition.
 *
 * PartitionGroup also maintains a stream-time for the group as a whole.
 * This is defined as the highest timestamp of any record yet polled from the PartitionGroup.
 * Note however that any computation that depends on stream-time should track it on a per-operator basis to obtain an
 * accurate view of the local time as seen by that processor.
 *
 * The PartitionGroups's stream-time is initially UNKNOWN (-1), and it set to a known value upon first poll.
 * As a consequence of the definition, the PartitionGroup's stream-time is non-decreasing
 * (i.e., it increases or stays the same over time).
 */
public class PartitionGroup {

    private  final Logger logger;
    private final Map<TopicPartition, RecordQueue> partitionQueues;
    private final Sensor enforcedProcessingSensor;
    private final long maxTaskIdleMs;
    private final Sensor recordLatenessSensor;
    private final PriorityQueue<RecordQueue> nonEmptyQueuesByTime;

    private long streamTime;
    private int totalBuffered;
    private boolean allBuffered;
    private final Map<TopicPartition, Long> fetchedLags = new HashMap<>();
    private final Map<TopicPartition, Long> idlePartitionDeadlines = new HashMap<>();

    static class RecordInfo {
        RecordQueue queue;

        ProcessorNode<?, ?, ?, ?> node() {
            return queue.source();
        }

        TopicPartition partition() {
            return queue.partition();
        }

        RecordQueue queue() {
            return queue;
        }
    }

    PartitionGroup(final LogContext logContext,
                   final Map<TopicPartition, RecordQueue> partitionQueues,
                   final Sensor recordLatenessSensor,
                   final Sensor enforcedProcessingSensor,
                   final long maxTaskIdleMs) {
        this.logger = logContext.logger(PartitionGroup.class);
        nonEmptyQueuesByTime = new PriorityQueue<>(partitionQueues.size(), Comparator.comparingLong(RecordQueue::headRecordTimestamp));
        this.partitionQueues = partitionQueues;
        this.enforcedProcessingSensor = enforcedProcessingSensor;
        this.maxTaskIdleMs = maxTaskIdleMs;
        this.recordLatenessSensor = recordLatenessSensor;
        totalBuffered = 0;
        allBuffered = false;
        streamTime = RecordQueue.UNKNOWN;
    }

    public void addFetchedMetadata(final TopicPartition partition, final ConsumerRecords.Metadata metadata) {
        final Long lag = metadata.lag();
        if (lag != null) {
            logger.trace("added fetched lag {}: {}", partition, lag);
            fetchedLags.put(partition, lag);
        }
    }

    public boolean readyToProcess(final long wallClockTime) {
        if (logger.isTraceEnabled()) {
            for (final Map.Entry<TopicPartition, RecordQueue> entry : partitionQueues.entrySet()) {
                logger.trace(
                    "buffered/lag {}: {}/{}",
                    entry.getKey(),
                    entry.getValue().size(),
                    fetchedLags.get(entry.getKey())
                );
            }
        }
        // Log-level strategy:
        //  TRACE for messages that don't wait for fetches
        //  TRACE when we waited for a fetch and decided to wait some more, as configured
        //  TRACE when we are ready for processing and didn't have to enforce processing
        //  INFO  when we enforce processing, since this has to wait for fetches AND may result in disorder

        if (maxTaskIdleMs == StreamsConfig.MAX_TASK_IDLE_MS_DISABLED) {
            if (logger.isTraceEnabled() && !allBuffered && totalBuffered > 0) {
                final Set<TopicPartition> bufferedPartitions = new HashSet<>();
                final Set<TopicPartition> emptyPartitions = new HashSet<>();
                for (final Map.Entry<TopicPartition, RecordQueue> entry : partitionQueues.entrySet()) {
                    if (entry.getValue().isEmpty()) {
                        emptyPartitions.add(entry.getKey());
                    } else {
                        bufferedPartitions.add(entry.getKey());
                    }
                }
                logger.trace("Ready for processing because max.task.idle.ms is disabled." +
                              "\n\tThere may be out-of-order processing for this task as a result." +
                              "\n\tBuffered partitions: {}" +
                              "\n\tNon-buffered partitions: {}",
                          bufferedPartitions,
                          emptyPartitions);
            }
            return true;
        }

        final Set<TopicPartition> queued = new HashSet<>();
        Map<TopicPartition, Long> enforced = null;

        for (final Map.Entry<TopicPartition, RecordQueue> entry : partitionQueues.entrySet()) {
            final TopicPartition partition = entry.getKey();
            final RecordQueue queue = entry.getValue();

            final Long nullableFetchedLag = fetchedLags.get(partition);

            if (!queue.isEmpty()) {
                // this partition is ready for processing
                idlePartitionDeadlines.remove(partition);
                queued.add(partition);
            } else if (nullableFetchedLag == null) {
                // must wait to fetch metadata for the partition
                idlePartitionDeadlines.remove(partition);
                logger.trace("Waiting to fetch data for {}", partition);
                return false;
            } else if (nullableFetchedLag > 0L) {
                // must wait to poll the data we know to be on the broker
                idlePartitionDeadlines.remove(partition);
                logger.trace(
                    "Lag for {} is currently {}, but no data is buffered locally. Waiting to buffer some records.",
                    partition,
                    nullableFetchedLag
                );
                return false;
            } else {
                // p is known to have zero lag. wait for maxTaskIdleMs to see if more data shows up.
                // One alternative would be to set the deadline to nullableMetadata.receivedTimestamp + maxTaskIdleMs
                // instead. That way, we would start the idling timer as of the freshness of our knowledge about the zero
                // lag instead of when we happen to run this method, but realistically it's probably a small difference
                // and using wall clock time seems more intuitive for users,
                // since the log message will be as of wallClockTime.
                idlePartitionDeadlines.putIfAbsent(partition, wallClockTime + maxTaskIdleMs);
                final long deadline = idlePartitionDeadlines.get(partition);
                if (wallClockTime < deadline) {
                    logger.trace(
                        "Lag for {} is currently 0 and current time is {}. Waiting for new data to be produced for configured idle time {} (deadline is {}).",
                        partition,
                        wallClockTime,
                        maxTaskIdleMs,
                        deadline
                    );
                    return false;
                } else {
                    // this partition is ready for processing due to the task idling deadline passing
                    if (enforced == null) {
                        enforced = new HashMap<>();
                    }
                    enforced.put(partition, deadline);
                }
            }
        }
        if (enforced == null) {
            logger.trace("All partitions were buffered locally, so this task is ready for processing.");
            return true;
        } else if (queued.isEmpty()) {
            logger.trace("No partitions were buffered locally, so this task is not ready for processing.");
            return false;
        } else {
            enforcedProcessingSensor.record(1.0d, wallClockTime);
            logger.info("Continuing to process although some partition timestamps were not buffered locally." +
                         "\n\tThere may be out-of-order processing for this task as a result." +
                         "\n\tPartitions with local data: {}." +
                         "\n\tPartitions we gave up waiting for, with their corresponding deadlines: {}." +
                         "\n\tConfigured max.task.idle.ms: {}." +
                         "\n\tCurrent wall-clock time: {}.",
                     queued,
                     enforced,
                     maxTaskIdleMs,
                     wallClockTime);
            return true;
        }
    }

    // visible for testing
    long partitionTimestamp(final TopicPartition partition) {
        final RecordQueue queue = partitionQueues.get(partition);
        if (queue == null) {
            throw new IllegalStateException("Partition " + partition + " not found.");
        }
        return queue.partitionTime();
    }

    // creates queues for new partitions, removes old queues, saves cached records for previously assigned partitions
    void updatePartitions(final Set<TopicPartition> newInputPartitions, final Function<TopicPartition, RecordQueue> recordQueueCreator) {
        final Set<TopicPartition> removedPartitions = new HashSet<>();
        final Iterator<Map.Entry<TopicPartition, RecordQueue>> queuesIterator = partitionQueues.entrySet().iterator();
        while (queuesIterator.hasNext()) {
            final Map.Entry<TopicPartition, RecordQueue> queueEntry = queuesIterator.next();
            final TopicPartition topicPartition = queueEntry.getKey();
            if (!newInputPartitions.contains(topicPartition)) {
                // if partition is removed should delete its queue
                totalBuffered -= queueEntry.getValue().size();
                queuesIterator.remove();
                removedPartitions.add(topicPartition);
            }
            newInputPartitions.remove(topicPartition);
        }
        for (final TopicPartition newInputPartition : newInputPartitions) {
            partitionQueues.put(newInputPartition, recordQueueCreator.apply(newInputPartition));
        }
        nonEmptyQueuesByTime.removeIf(q -> removedPartitions.contains(q.partition()));
        allBuffered = allBuffered && newInputPartitions.isEmpty();
    }

    void setPartitionTime(final TopicPartition partition, final long partitionTime) {
        final RecordQueue queue = partitionQueues.get(partition);
        if (queue == null) {
            throw new IllegalStateException("Partition " + partition + " not found.");
        }
        if (streamTime < partitionTime) {
            streamTime = partitionTime;
        }
        queue.setPartitionTime(partitionTime);
    }

    /**
     * Get the next record and queue
     *
     * @return StampedRecord
     */
    StampedRecord nextRecord(final RecordInfo info, final long wallClockTime) {
        StampedRecord record = null;

        final RecordQueue queue = nonEmptyQueuesByTime.poll();
        info.queue = queue;

        if (queue != null) {
            // get the first record from this queue.
            record = queue.poll();

            if (record != null) {
                --totalBuffered;

                if (queue.isEmpty()) {
                    // if a certain queue has been drained, reset the flag
                    allBuffered = false;
                } else {
                    nonEmptyQueuesByTime.offer(queue);
                }

                // always update the stream-time to the record's timestamp yet to be processed if it is larger
                if (record.timestamp > streamTime) {
                    streamTime = record.timestamp;
                    recordLatenessSensor.record(0, wallClockTime);
                } else {
                    recordLatenessSensor.record(streamTime - record.timestamp, wallClockTime);
                }
            }
        }

        return record;
    }

    /**
     * Adds raw records to this partition group
     *
     * @param partition the partition
     * @param rawRecords  the raw records
     * @return the queue size for the partition
     */
    int addRawRecords(final TopicPartition partition, final Iterable<ConsumerRecord<byte[], byte[]>> rawRecords) {
        final RecordQueue recordQueue = partitionQueues.get(partition);

        if (recordQueue == null) {
            throw new IllegalStateException("Partition " + partition + " not found.");
        }

        final int oldSize = recordQueue.size();
        final int newSize = recordQueue.addRawRecords(rawRecords);

        // add this record queue to be considered for processing in the future if it was empty before
        if (oldSize == 0 && newSize > 0) {
            nonEmptyQueuesByTime.offer(recordQueue);

            // if all partitions now are non-empty, set the flag
            // we do not need to update the stream-time here since this task will definitely be
            // processed next, and hence the stream-time will be updated when we retrieved records by then
            if (nonEmptyQueuesByTime.size() == this.partitionQueues.size()) {
                allBuffered = true;
            }
        }

        totalBuffered += newSize - oldSize;

        return newSize;
    }

    Set<TopicPartition> partitions() {
        return Collections.unmodifiableSet(partitionQueues.keySet());
    }

    /**
     * Return the stream-time of this partition group defined as the largest timestamp seen across all partitions
     */
    long streamTime() {
        return streamTime;
    }

    Long headRecordOffset(final TopicPartition partition) {
        final RecordQueue recordQueue = partitionQueues.get(partition);

        if (recordQueue == null) {
            throw new IllegalStateException("Partition " + partition + " not found.");
        }

        return recordQueue.headRecordOffset();
    }

    /**
     * @throws IllegalStateException if the record's partition does not belong to this partition group
     */
    int numBuffered(final TopicPartition partition) {
        final RecordQueue recordQueue = partitionQueues.get(partition);

        if (recordQueue == null) {
            throw new IllegalStateException("Partition " + partition + " not found.");
        }

        return recordQueue.size();
    }

    int numBuffered() {
        return totalBuffered;
    }

    boolean allPartitionsBufferedLocally() {
        return allBuffered;
    }

    void clear() {
        for (final RecordQueue queue : partitionQueues.values()) {
            queue.clear();
        }
        nonEmptyQueuesByTime.clear();
        totalBuffered = 0;
        streamTime = RecordQueue.UNKNOWN;
    }
}
