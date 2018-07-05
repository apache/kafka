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

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * A PartitionGroup is composed from a set of partitions. It also maintains the timestamp of this
 * group, hence the associated task as the min timestamp across all partitions in the group.
 */
public class PartitionGroup {

    private final Map<TopicPartition, RecordQueue> partitionQueues;

    private final PriorityQueue<RecordQueue> nonEmptyQueuesByTime;
    private long streamTime;
    private int totalBuffered;

    public static class RecordInfo {
        RecordQueue queue;

        public ProcessorNode node() {
            return queue.source();
        }

        public TopicPartition partition() {
            return queue.partition();
        }

        RecordQueue queue() {
            return queue;
        }
    }


    PartitionGroup(final Map<TopicPartition, RecordQueue> partitionQueues) {
        nonEmptyQueuesByTime = new PriorityQueue<>(partitionQueues.size(), Comparator.comparingLong(RecordQueue::timestamp));
        this.partitionQueues = partitionQueues;
        totalBuffered = 0;
        streamTime = -1;
    }

    /**
     * Get the next record and queue
     *
     * @return StampedRecord
     */
    StampedRecord nextRecord(final RecordInfo info) {
        StampedRecord record = null;

        final RecordQueue queue = nonEmptyQueuesByTime.poll();
        info.queue = queue;

        if (queue != null) {
            // get the first record from this queue.
            record = queue.poll();

            if (record != null) {
                --totalBuffered;

                if (!queue.isEmpty()) {
                    nonEmptyQueuesByTime.offer(queue);
                }

                // Since this was previously a queue with min timestamp,
                // streamTime could only advance if this queue's time did.
                if (queue.timestamp() > streamTime) {
                    computeStreamTime();
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

        final int oldSize = recordQueue.size();
        final long oldTimestamp = recordQueue.timestamp();
        final int newSize = recordQueue.addRawRecords(rawRecords);

        // add this record queue to be considered for processing in the future if it was empty before
        if (oldSize == 0 && newSize > 0) {
            nonEmptyQueuesByTime.offer(recordQueue);
        }

        // Adding to this queue could only advance streamTime if it was previously the queue with min timestamp (= streamTime)
        if (oldTimestamp <= streamTime && recordQueue.timestamp() > streamTime) {
            computeStreamTime();
        }

        totalBuffered += newSize - oldSize;

        return newSize;
    }

    public Set<TopicPartition> partitions() {
        return Collections.unmodifiableSet(partitionQueues.keySet());
    }

    /**
     * Return the timestamp of this partition group as the smallest
     * partition timestamp among all its partitions
     */
    public long timestamp() {
        return streamTime;
    }

    private void computeStreamTime() {
        // we should always return the smallest timestamp of all partitions
        // to avoid group partition time goes backward
        long timestamp = Long.MAX_VALUE;
        for (final RecordQueue queue : partitionQueues.values()) {
            if (queue.timestamp() < timestamp) {
                timestamp = queue.timestamp();
            }
        }
        this.streamTime = timestamp;
    }

    /**
     * @throws IllegalStateException if the record's partition does not belong to this partition group
     */
    int numBuffered(final TopicPartition partition) {
        final RecordQueue recordQueue = partitionQueues.get(partition);

        if (recordQueue == null) {
            throw new IllegalStateException(String.format("Record's partition %s does not belong to this partition-group.", partition));
        }

        return recordQueue.size();
    }

    int numBuffered() {
        return totalBuffered;
    }

    public void close() {
        partitionQueues.clear();
    }

    public void clear() {
        nonEmptyQueuesByTime.clear();
        for (final RecordQueue queue : partitionQueues.values()) {
            queue.clear();
        }
    }
}
