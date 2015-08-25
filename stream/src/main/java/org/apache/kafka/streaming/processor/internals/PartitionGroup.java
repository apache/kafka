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
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * A PartitionGroup is composed from a set of partitions; it also keeps track
 * of the consumed offsets for each of its partitions.
 */
public class PartitionGroup {

    private final Map<TopicPartition, RecordQueue> partitionQueues;

    private final Map<TopicPartition, Long> consumedOffsets;

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

        this.consumedOffsets = new HashMap<>();

        this.totalBuffered = 0;
    }

    /**
     * Process one record from this partition group's queues
     * as the first record from the lowest stamped partition,
     * return its partition when completed
     */
    @SuppressWarnings("unchecked")
    public TopicPartition processRecord() {
        // get the partition with the lowest timestamp.
        RecordQueue recordQueue = queuesByTime.poll();

        // get the first record from this partition's queue.
        StampedRecord record = recordQueue.get();

        totalBuffered--;

        // process the record by passing it to the source node of the topology
        recordQueue.source().process(record.key(), record.value());

        // update the partition's timestamp and re-order it against other partitions.
        if (recordQueue.size() > 0) {
            queuesByTime.offer(recordQueue);
        }

        // update the consumed offset map.
        consumedOffsets.put(recordQueue.partition(), record.offset());

        // return the processed record's partition
        return recordQueue.partition();
    }

    /**
     * Put a timestamped record associated into its corresponding partition's queues.
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
            return -1L;
        } else {
            return queuesByTime.peek().timestamp();
        }
    }

    public long offset(TopicPartition partition) {
        Long offset = consumedOffsets.get(partition);

        if (offset == null)
            throw new KafkaException("No record has ever been consumed for this partition.");

        return offset;
    }

    public Map<TopicPartition, Long> offsets() {
        return consumedOffsets;
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
        consumedOffsets.clear();
        partitionQueues.clear();
    }
}
