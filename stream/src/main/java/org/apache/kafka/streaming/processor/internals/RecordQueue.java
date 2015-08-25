/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streaming.processor.internals;

import org.apache.kafka.common.TopicPartition;

import java.util.ArrayDeque;
import java.util.PriorityQueue;

/**
 * RecordQueue is a FIFO queue of {@link StampedRecord} (ConsumerRecord + timestamp). It also keeps track of the
 * partition timestamp defined as the minimum timestamp of records in its queue; in addition, its partition
 * timestamp is monotonically increasing such that once it is advanced, it will not be decremented.
 */
public class RecordQueue {

    private final SourceNode source;
    private final TopicPartition partition;
    private final ArrayDeque<StampedRecord> fifoQueue = new ArrayDeque<>();
    private final PriorityQueue<StampedRecord> timeQueue = new PriorityQueue<>();

    private long partitionTime = -1L;

    /**
     * Creates a new instance of RecordQueue
     *
     * @param partition        partition
     * @param source           source node
     */
    public RecordQueue(TopicPartition partition, SourceNode source) {
        this.partition = partition;
        this.source = source;
    }

    public SourceNode source() {
        return source;
    }

    /**
     * Returns the partition with which this queue is associated
     *
     * @return TopicPartition
     */
    public TopicPartition partition() {
        return partition;
    }

    /**
     * Adds a StampedRecord to the queue
     *
     * @param record StampedRecord
     */
    public void add(StampedRecord record) {
        fifoQueue.addLast(record);

        // only add it to the timestamp tracker queue if its timestamp
        // is no smaller than the current partition timestamp
        if (record.timestamp >= partitionTime) {
            timeQueue.offer(record);
        }
    }

    /**
     * Returns the get record fro the queue
     *
     * @return StampedRecord
     */
    public StampedRecord get() {
        StampedRecord elem = fifoQueue.pollFirst();

        if (elem == null) return null;

        // try to advance the partition timestamp if necessary
        timeQueue.remove(elem);

        if (!timeQueue.isEmpty()) {
            partitionTime = timeQueue.peek().timestamp;
        }

        return elem;
    }

    /**
     * Returns the number of records in the queue
     *
     * @return the number of records
     */
    public int size() {
        return fifoQueue.size();
    }

    /**
     * Tests if the queue is empty
     *
     * @return true if the queue is empty, otherwise false
     */
    public boolean isEmpty() {
        return fifoQueue.isEmpty();
    }

    /**
     * Returns the tracked partition timestamp
     *
     * @return timestamp
     */
    public long timestamp() {
        return partitionTime;
    }
}
