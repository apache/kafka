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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streaming.processor.ProcessorContext;
import org.apache.kafka.streaming.processor.Punctuator;
import org.apache.kafka.streaming.processor.TimestampExtractor;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A StreamTask is associated with a {@link PartitionGroup} which is assigned to a StreamThread for processing.
 */
public class StreamTask {

    private final int id;
    private final int desiredUnprocessed;

    private final Consumer consumer;
    private final PartitionGroup partitionGroup;
    private final TimestampExtractor timestampExtractor;

    private final Map<TopicPartition, Long> consumedOffsets;
    private final PunctuationQueue punctuationQueue = new PunctuationQueue();
    private final ArrayDeque<NewRecords> newRecordBuffer = new ArrayDeque<>();

    private long streamTime = -1;
    private boolean commitRequested = false;
    private StampedRecord currRecord = null;
    private ProcessorNode currNode = null;

    /**
     * Creates StreamGroup
     *
     * @param consumer                       the instance of {@link Consumer}
     * @param partitions                     the instance of {@link TimestampExtractor}
     * @param desiredUnprocessedPerPartition the target number of records kept in a queue for each topic
     */
    public StreamTask(int id,
                      Consumer consumer,
                      ProcessorTopology topology,
                      Collection<TopicPartition> partitions,
                      int desiredUnprocessedPerPartition) {
        this.id = id;
        this.consumer = consumer;
        this.desiredUnprocessed = desiredUnprocessedPerPartition;
        this.consumedOffsets = new HashMap<>();

        // create partition queues and pipe them to corresponding source nodes in the topology
        Map<TopicPartition, RecordQueue> partitionQueues = new HashMap<>();
        for (TopicPartition partition : partitions) {
            RecordQueue queue = createRecordQueue(partition, topology.source(partition.topic()));
            partitionQueues.put(partition, queue);
        }
        this.partitionGroup = new PartitionGroup(partitionQueues);
    }

    public int id() {
        return id;
    }

    public StampedRecord record() {
        return currRecord;
    }

    public ProcessorNode node() {
        return currNode;
    }

    public void setNode(ProcessorNode node) {
        currNode = node;
    }

    public Set<TopicPartition> partitions() {
        return queuesPerPartition.keySet();
    }

    /**
     * Adds records to queues
     *
     * @param partition the partition
     * @param iterator  the iterator of records
     */
    @SuppressWarnings("unchecked")
    public void addRecords(TopicPartition partition, Iterator<ConsumerRecord<byte[], byte[]>> iterator) {

        // get deserializers for this partition
        Deserializer<?> keyDeserializer = partitionGroup.keyDeserializer(partition);
        Deserializer<?> valDeserializer = partitionGroup.valDeserializer(partition);

        while (iterator.hasNext()) {

            ConsumerRecord<byte[], byte[]> rawRecord = iterator.next();

            // deserialize the raw record, extract the timestamp and put into the queue
            Object key = keyDeserializer.deserialize(rawRecord.topic(), rawRecord.key());
            Object value = valDeserializer.deserialize(rawRecord.topic(), rawRecord.value());
            long timestamp = timestampExtractor.extract(rawRecord.topic(), key, value);

            StampedRecord stampedRecord = new StampedRecord(new ConsumerRecord<>(rawRecord.topic(), rawRecord.partition(), rawRecord.offset(), key, value), timestamp);

            partitionGroup.putRecord(stampedRecord, partition);
        }
    }

    /**
     * Schedules a punctuation for the processor
     *
     * @param punctuator the punctuator requesting scheduler
     * @param interval  the interval in milliseconds
     */
    public void schedule(Punctuator punctuator, long interval) {
        punctuationQueue.schedule(new PunctuationSchedule(punctuator, interval));
    }

    /**
     * Processes one record
     */
    @SuppressWarnings("unchecked")
    public boolean process() {
        synchronized (this) {
            boolean readyForNextExecution = false;

            // take the next record queue with the smallest estimated timestamp to process
            long timestamp = partitionGroup.timestamp();
            StampedRecord record = partitionGroup.nextRecord();

            currRecord = recordQueue.next();
            currNode = recordQueue.source();

            if (streamTime < timestamp) streamTime = timestamp;

            currNode.process(currRecord.key(), currRecord.value());
            consumedOffsets.put(recordQueue.partition(), currRecord.offset());

            // TODO: local state flush and downstream producer flush
            // need to be done altogether with offset commit atomically
            if (commitRequested) {
                // flush local state
                context.flush();

                // flush produced records in the downstream
                context.recordCollector().flush();

                // commit consumed offsets
                ingestor.commit(consumedOffsets());
            }

            if (commitRequested) ingestor.commit(Collections.singletonMap(
                new TopicPartition(currRecord.topic(), currRecord.partition()),
                currRecord.offset()));

            // update this record queue's estimated timestamp
            if (recordQueue.size() > 0) {
                readyForNextExecution = true;
            }

            if (recordQueue.size() == this.desiredUnprocessed) {
                ingestor.unpause(recordQueue.partition(), recordQueue.offset());
            }

            buffered--;
            currRecord = null;

            punctuationQueue.mayPunctuate(streamTime);

            return readyForNextExecution;
        }
    }

    /**
     * Returns consumed offsets
     *
     * @return the map of partition to consumed offset
     */
    public Map<TopicPartition, Long> consumedOffsets() {
        return this.consumedOffsets;
    }

    /**
     * Request committing the current record's offset
     */
    public void commitOffset() {
        this.commitRequested = true;
    }

    public void close() {
        queuesByTime.clear();
        queuesPerPartition.clear();
    }

    protected RecordQueue createRecordQueue(TopicPartition partition, SourceNode source) {
        return new RecordQueue(partition, source);
    }

    private static class NewRecords {
        final TopicPartition partition;
        final Iterator<ConsumerRecord<byte[], byte[]>> iterator;

        NewRecords(TopicPartition partition, Iterator<ConsumerRecord<byte[], byte[]>> iterator) {
            this.partition = partition;
            this.iterator = iterator;
        }
    }
}
