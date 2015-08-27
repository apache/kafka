/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streaming.processor.internals;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

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

    // since task is thread-safe, we do not need to synchronize on local variables
    private int totalBuffered;

    public PartitionGroup(Map<TopicPartition, RecordQueue> partitionQueues) {
        this.queuesByTime = new PriorityQueue<>(new Comparator<RecordQueue>() {

            @Override
            public int compare(RecordQueue queue1, RecordQueue queue2) {
                long time1 = queue1.timestamp();
                long time2 = queue2.timestamp();

                if (time1 < time2) return -1;
                if (time1 > time2) return 1;
                return 0;
            }
        });

        this.partitionQueues = partitionQueues;

        this.totalBuffered = 0;
    }

    /**
     * Get one record from the specified partition queue
     *
     * @return StampedRecord
     */
    public StampedRecord getRecord(RecordQueue queue) {
        // get the first record from this queue.
        StampedRecord record = queue.poll();

        // update the partition's timestamp and re-order it against other partitions.
        queuesByTime.remove(queue);

        if (queue.size() > 0) {
            queuesByTime.offer(queue);
        }

        totalBuffered--;

        return record;
    }

    /**
     * Get the next partition queue that has the lowest timestamp to process
     *
     * @return RecordQueue
     */
    public RecordQueue nextQueue() {
        // get the partition with the lowest timestamp
        return queuesByTime.peek();
    }

    /**
     * Put a timestamped record associated into its corresponding partition's queues
     */
    public void putRecord(StampedRecord record, TopicPartition partition) {
        if (record.partition() != partition.partition() || !record.topic().equals(partition.topic()))
            throw new KafkaException("The specified partition is different from the record's associated partition.");

        RecordQueue recordQueue = partitionQueues.get(partition);

        if (recordQueue == null)
            throw new KafkaException("Record's partition does not belong to this partition-group.");

        boolean wasEmpty = recordQueue.isEmpty();

        recordQueue.add(record);

        totalBuffered++;

        // add this record queue to be considered for processing in the future if it was empty before
        if (wasEmpty) {
            queuesByTime.offer(recordQueue);
        }
    }

    public Deserializer<?> keyDeserializer(TopicPartition partition) {
        RecordQueue recordQueue = partitionQueues.get(partition);

        if (recordQueue == null)
            throw new KafkaException("Record's partition does not belong to this partition-group.");

        return recordQueue.source().keyDeserializer;
    }

    public Deserializer<?> valDeserializer(TopicPartition partition) {
        RecordQueue recordQueue = partitionQueues.get(partition);

        if (recordQueue == null)
            throw new KafkaException("Record's partition does not belong to this partition-group.");

        return recordQueue.source().valDeserializer;
    }

    public Set<TopicPartition> partitions() {
        return partitionQueues.keySet();
    }

    /**
     * Return the timestamp of this partition group as the smallest
     * partition timestamp among all its partitions
     */
    public long timestamp() {
        if (queuesByTime.isEmpty()) {
            return TimestampTracker.NOT_KNOWN;
        } else {
            return queuesByTime.peek().timestamp();
        }
    }

    public int numbuffered(TopicPartition partition) {
        RecordQueue recordQueue = partitionQueues.get(partition);

        if (recordQueue == null)
            throw new KafkaException("Record's partition does not belong to this partition-group.");

        return recordQueue.size();
    }

    public int numbuffered() {
        return totalBuffered;
    }

    public void close() {
        queuesByTime.clear();
        partitionQueues.clear();
    }
}
