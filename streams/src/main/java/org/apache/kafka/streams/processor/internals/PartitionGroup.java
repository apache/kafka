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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Iterator;
import java.util.HashSet;
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

    private final Map<TopicPartition, RecordQueue> partitionQueues;
    private final Sensor recordLatenessSensor;
    private final PriorityQueue<RecordQueue> nonEmptyQueuesByTime;

    private long streamTime;
    private int totalBuffered;
    private boolean allBuffered;


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

    PartitionGroup(final Map<TopicPartition, RecordQueue> partitionQueues, final Sensor recordLatenessSensor) {
        nonEmptyQueuesByTime = new PriorityQueue<>(partitionQueues.size(), Comparator.comparingLong(RecordQueue::headRecordTimestamp));
        this.partitionQueues = partitionQueues;
        this.recordLatenessSensor = recordLatenessSensor;
        totalBuffered = 0;
        allBuffered = false;
        streamTime = RecordQueue.UNKNOWN;
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

    boolean allPartitionsBuffered() {
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
