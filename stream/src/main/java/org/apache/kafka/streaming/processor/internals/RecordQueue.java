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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayDeque;

/**
 * RecordQueue is a queue of {@link StampedRecord} (ConsumerRecord + timestamp).
 */
public class RecordQueue {

    private final SourceNode source;
    private final TopicPartition partition;
    private final ArrayDeque<StampedRecord> queue = new ArrayDeque<>();
    private final TimestampTracker<ConsumerRecord<Object, Object>> timestampTracker = new MinTimestampTracker<>();

    private long offset;

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
        queue.addLast(record);

        offset = record.offset();
        timestampTracker.addStampedElement(record);
    }

    /**
     * Returns the next record fro the queue
     *
     * @return StampedRecord
     */
    public StampedRecord next() {
        StampedRecord elem = queue.pollFirst();

        if (elem == null) return null;

        timestampTracker.removeStampedElement(elem);

        return elem;
    }

    /**
     * Returns the highest offset in the queue
     *
     * @return offset
     */
    public long offset() {
        return offset;
    }

    /**
     * Returns the number of records in the queue
     *
     * @return the number of records
     */
    public int size() {
        return queue.size();
    }

    /**
     * Tests if the queue is empty
     *
     * @return true if the queue is empty, otherwise false
     */
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    /**
     * Returns a timestamp tracked by the TimestampTracker
     *
     * @return timestamp
     */
    public long timestamp() {
        return timestampTracker.get();
    }
}
