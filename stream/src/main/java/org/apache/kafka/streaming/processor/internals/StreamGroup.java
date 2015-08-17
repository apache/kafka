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
import org.apache.kafka.streaming.processor.Chooser;
import org.apache.kafka.streaming.processor.KafkaSource;
import org.apache.kafka.streaming.processor.Punctuator;
import org.apache.kafka.streaming.processor.ProcessorContext;
import org.apache.kafka.streaming.processor.RecordQueue;
import org.apache.kafka.streaming.processor.StampedRecord;
import org.apache.kafka.streaming.processor.TimestampExtractor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A StreamGroup is composed of multiple streams from different topics that need to be synchronized.
 */
public class StreamGroup {

    private final ProcessorContext context;
    private final Ingestor ingestor;
    private final Chooser chooser;
    private final TimestampExtractor timestampExtractor;
    private final Map<TopicPartition, RecordQueue> stash = new HashMap<>();

    private final int desiredUnprocessed;

    // TODO: merge stash, consumedOffset, and newRecordBuffer into sth. like partition metadata
    private final Map<TopicPartition, Long> consumedOffsets;
    private final PunctuationQueue punctuationQueue = new PunctuationQueue();
    private final ArrayDeque<NewRecords> newRecordBuffer = new ArrayDeque<>();

    private long streamTime = -1;
    private boolean commitRequested = false;
    private StampedRecord currRecord = null;
    private volatile int buffered = 0;

    /**
     * Creates StreamGroup
     *
     * @param context                        the task context
     * @param ingestor                       the instance of {@link Ingestor}
     * @param chooser                        the instance of {@link Chooser}
     * @param timestampExtractor             the instance of {@link TimestampExtractor}
     * @param desiredUnprocessedPerPartition the target number of records kept in a queue for each topic
     */
    public StreamGroup(ProcessorContext context,
                       Ingestor ingestor,
                       Chooser chooser,
                       TimestampExtractor timestampExtractor,
                       int desiredUnprocessedPerPartition) {
        this.context = context;
        this.ingestor = ingestor;
        this.chooser = chooser;
        this.timestampExtractor = timestampExtractor;
        this.desiredUnprocessed = desiredUnprocessedPerPartition;
        this.consumedOffsets = new HashMap<>();
    }

    public StampedRecord record() {
        return currRecord;
    }

    public Set<TopicPartition> partitions() {
        return stash.keySet();
    }

    /**
     * Adds a partition and its receiver to this stream synchronizer
     *
     * @param partition the partition
     * @param source    the instance of KStreamImpl
     */
    @SuppressWarnings("unchecked")
    public void addPartition(TopicPartition partition, KafkaSource source) {
        synchronized (this) {
            RecordQueue recordQueue = stash.get(partition);

            if (recordQueue == null) {
                stash.put(partition, createRecordQueue(partition, source));
            } else {
                throw new IllegalStateException("duplicate partition");
            }
        }
    }

    /**
     * Adds records
     *
     * @param partition the partition
     * @param iterator  the iterator of records
     */
    @SuppressWarnings("unchecked")
    public void addRecords(TopicPartition partition, Iterator<ConsumerRecord<byte[], byte[]>> iterator) {
        synchronized (this) {
            newRecordBuffer.addLast(new NewRecords(partition, iterator));
        }
    }

    @SuppressWarnings("unchecked")
    private void ingestNewRecords() {
        for (NewRecords newRecords : newRecordBuffer) {
            TopicPartition partition = newRecords.partition;
            Iterator<ConsumerRecord<byte[], byte[]>> iterator = newRecords.iterator;

            RecordQueue recordQueue = stash.get(partition);
            if (recordQueue != null) {
                boolean wasEmpty = recordQueue.isEmpty();

                while (iterator.hasNext()) {
                    ConsumerRecord<byte[], byte[]> record = iterator.next();

                    // deserialize the raw record, extract the timestamp and put into the queue
                    Deserializer<?> keyDeserializer = ((KafkaSource) recordQueue.source()).keyDeserializer;
                    Deserializer<?> valDeserializer = ((KafkaSource) recordQueue.source()).valDeserializer;

                    Object key = keyDeserializer.deserialize(record.topic(), record.key());
                    Object value = valDeserializer.deserialize(record.topic(), record.value());
                    ConsumerRecord deserializedRecord = new ConsumerRecord<>(record.topic(), record.partition(), record.offset(), key, value);

                    long timestamp = timestampExtractor.extract(record.topic(), key, value);
                    recordQueue.add(new StampedRecord(deserializedRecord, timestamp));
                    buffered++;
                }

                int queueSize = recordQueue.size();
                if (wasEmpty && queueSize > 0) chooser.add(recordQueue);

                // if we have buffered enough for this partition, pause
                if (queueSize >= this.desiredUnprocessed) {
                    ingestor.pause(partition);
                }
            }
        }
        newRecordBuffer.clear();
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
            ingestNewRecords();

            RecordQueue recordQueue = chooser.next();
            if (recordQueue == null) {
                return false;
            }

            if (recordQueue.size() == 0) throw new IllegalStateException("empty record queue");

            if (recordQueue.size() == this.desiredUnprocessed) {
                ingestor.unpause(recordQueue.partition(), recordQueue.offset());
            }

            long trackedTimestamp = recordQueue.trackedTimestamp();
            currRecord = recordQueue.next();

            if (streamTime < trackedTimestamp) streamTime = trackedTimestamp;

            recordQueue.source().process(currRecord.key(), currRecord.value());
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

            if (recordQueue.size() > 0) {
                readyForNextExecution = true;
                chooser.add(recordQueue);
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
        chooser.close();
        stash.clear();
    }

    protected RecordQueue createRecordQueue(TopicPartition partition, KafkaSource source) {
        return new RecordQueueImpl(partition, source, new MinTimestampTracker<ConsumerRecord<Object, Object>>());
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
