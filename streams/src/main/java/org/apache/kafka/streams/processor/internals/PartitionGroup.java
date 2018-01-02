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

    private final PriorityQueue<RecordQueue> queuesByTime;

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

    // since task is thread-safe, we do not need to synchronize on local variables
    private int totalBuffered;

    PartitionGroup(final Map<TopicPartition, RecordQueue> partitionQueues) {
        queuesByTime = new PriorityQueue<>(partitionQueues.size(), new Comparator<RecordQueue>() {

            @Override
            public int compare(final RecordQueue queue1, final RecordQueue queue2) {
                final long time1 = queue1.timestamp();
                final long time2 = queue2.timestamp();

                if (time1 < time2) {
                    return -1;
                }
                if (time1 > time2) {
                    return 1;
                }
                return 0;
            }
        });

        this.partitionQueues = partitionQueues;

        totalBuffered = 0;
    }

    /**
     * Get the next record and queue
     *
     * @return StampedRecord
     */
    StampedRecord nextRecord(final RecordInfo info) {
        StampedRecord record = null;

        final RecordQueue queue = queuesByTime.poll();
        if (queue != null) {
            // get the first record from this queue.
            record = queue.poll();

            if (!queue.isEmpty()) {
                queuesByTime.offer(queue);
            }
        }
        info.queue = queue;

        if (record != null) {
            --totalBuffered;
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
        final int newSize = recordQueue.addRawRecords(rawRecords);

        // add this record queue to be considered for processing in the future if it was empty before
        if (oldSize == 0 && newSize > 0) {
            queuesByTime.offer(recordQueue);
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
        // we should always return the smallest timestamp of all partitions
        // to avoid group partition time goes backward
        long timestamp = Long.MAX_VALUE;
        for (final RecordQueue queue : partitionQueues.values()) {
            if (timestamp > queue.timestamp()) {
                timestamp = queue.timestamp();
            }
        }
        return timestamp;
    }

    /**
     * @throws IllegalStateException if the record's partition does not belong to this partition group
     */
    int numBuffered(final TopicPartition partition) {
        final RecordQueue recordQueue = partitionQueues.get(partition);

        if (recordQueue == null) {
            throw new IllegalStateException("Record's partition does not belong to this partition-group.");
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
        queuesByTime.clear();
        for (final RecordQueue queue : partitionQueues.values()) {
            queue.clear();
        }
    }
}
