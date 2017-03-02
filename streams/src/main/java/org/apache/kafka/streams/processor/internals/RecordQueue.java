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
//import org.apache.kafka.common.utils.SystemTime;
//import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;


/**
 * RecordQueue is a FIFO queue of {@link StampedRecord} (ConsumerRecord + timestamp). It also keeps track of the
 * partition timestamp defined as the minimum timestamp of records in its queue; in addition, its partition
 * timestamp is monotonically increasing such that once it is advanced, it will not be decremented.
 */
public class RecordQueue {

    private static final Logger log = LoggerFactory.getLogger(RecordQueue.class);

    private final SourceNode source;
    private final TimestampExtractor timestampExtractor;
    private final TopicPartition partition;
    private final ArrayDeque<StampedRecord> fifoQueue;
    //private final TimestampTracker<ConsumerRecord<Object, Object>> timeTracker;
    private long prevTimestamp = TimestampTracker.NOT_KNOWN;
    private final RecordDeserializer recordDeserializer;
    //private final Time time;

    private long partitionTime = TimestampTracker.NOT_KNOWN;

    RecordQueue(final TopicPartition partition,
                final SourceNode source,
                final TimestampExtractor timestampExtractor) {
        this.partition = partition;
        this.source = source;
        this.timestampExtractor = timestampExtractor;
        this.fifoQueue = new ArrayDeque<>();
        //this.timeTracker = new MinTimestampTracker<>();
        this.recordDeserializer = new SourceNodeRecordDeserializer(source);
        //this.time = new SystemTime();
    }


    /**
     * Returns the corresponding source node in the topology
     *
     * @return SourceNode
     */
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
     * Add a batch of {@link ConsumerRecord} into the queue
     *
     * @param rawRecords the raw records
     * @return the size of this queue
     */
    public int addRawRecords(Iterable<ConsumerRecord<byte[], byte[]>> rawRecords) {
        //long start1 = 0, end1 = 0, end2 = 0, end3 = 0;
        //long sum1 = 0, sum2 = 0, sum3 = 0;
        for (ConsumerRecord<byte[], byte[]> rawRecord : rawRecords) {
            //start1 = time.nanoseconds();
            ConsumerRecord<Object, Object> record = recordDeserializer.deserialize(rawRecord);
            //end1 = time.nanoseconds();
            long timestamp = timestampExtractor.extract(record, prevTimestamp);
            //log.trace("Source node {} extracted timestamp {} for record {}", source.name(), timestamp, record);

            // drop message if TS is invalid, i.e., negative
            if (timestamp < 0) {
                continue;
            }
            //end2 = time.nanoseconds();

            StampedRecord stampedRecord = new StampedRecord(record, timestamp);
            fifoQueue.addLast(stampedRecord);
            //timeTracker.addElement(stampedRecord);
            //end3 = time.nanoseconds();

            //sum1 += (end1 - start1);
            //sum2 += (end2 - end1);
            //sum3 += (end3 - end2);
        }
        //System.out.println(sum1 + "\t" +
        //    sum2 + "\t" +
        //    sum3);

        // update the partition timestamp if its currently
        // tracked min timestamp has exceed its value; this will
        // usually only take effect for the first added batch
        advanceTime();

        return size();
    }

    private void advanceTime() {
        StampedRecord elem = fifoQueue.peekFirst();
        if (elem != null) {
            prevTimestamp = elem.timestamp;
        }
        if (prevTimestamp > partitionTime)
            partitionTime = prevTimestamp;
    }

    /**
     * Get the next {@link StampedRecord} from the queue
     *
     * @return StampedRecord
     */
    public StampedRecord poll() {
        StampedRecord elem = fifoQueue.pollFirst();

        if (elem == null)
            return null;

        //timeTracker.removeElement(elem);

        // only advance the partition timestamp if its currently
        // tracked min timestamp has exceeded its value
        advanceTime();

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

    /**
     * Clear the fifo queue of its elements
     */
    public void clear() {
        fifoQueue.clear();
    }
}
