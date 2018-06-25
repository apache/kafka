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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.slf4j.Logger;

import java.util.ArrayDeque;


/**
 * RecordQueue is a FIFO queue of {@link StampedRecord} (ConsumerRecord + timestamp). It also keeps track of the
 * partition timestamp defined as the minimum timestamp of records in its queue; in addition, its partition
 * timestamp is monotonically increasing such that once it is advanced, it will not be decremented.
 */
public class RecordQueue {
    private final SourceNode source;
    private final TimestampExtractor timestampExtractor;
    private final TopicPartition partition;
    private final ArrayDeque<StampedRecord> fifoQueue;
    private final TimestampTracker<ConsumerRecord<Object, Object>> timeTracker;
    private final RecordDeserializer recordDeserializer;
    private final ProcessorContext processorContext;
    private final Logger log;

    private long partitionTime = TimestampTracker.NOT_KNOWN;

    RecordQueue(final TopicPartition partition,
                final SourceNode source,
                final TimestampExtractor timestampExtractor,
                final DeserializationExceptionHandler deserializationExceptionHandler,
                final InternalProcessorContext processorContext,
                final LogContext logContext) {
        this.partition = partition;
        this.source = source;
        this.timestampExtractor = timestampExtractor;
        this.fifoQueue = new ArrayDeque<>();
        this.timeTracker = new MinTimestampTracker<>();
        this.recordDeserializer = new RecordDeserializer(
            source,
            deserializationExceptionHandler,
            logContext,
            processorContext.metrics().skippedRecordsSensor()
        );
        this.processorContext = processorContext;
        this.log = logContext.logger(RecordQueue.class);
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
    int addRawRecords(final Iterable<ConsumerRecord<byte[], byte[]>> rawRecords) {
        for (final ConsumerRecord<byte[], byte[]> rawRecord : rawRecords) {

            final ConsumerRecord<Object, Object> record = recordDeserializer.deserialize(processorContext, rawRecord);
            if (record == null) {
                // this only happens if the deserializer decides to skip. It has already logged the reason.
                continue;
            }

            final long timestamp;
            try {
                timestamp = timestampExtractor.extract(record, timeTracker.get());
            } catch (final StreamsException internalFatalExtractorException) {
                throw internalFatalExtractorException;
            } catch (final Exception fatalUserException) {
                throw new StreamsException(
                    String.format("Fatal user code error in TimestampExtractor callback for record %s.", record),
                    fatalUserException);
            }
            log.trace("Source node {} extracted timestamp {} for record {}", source.name(), timestamp, record);

            // drop message if TS is invalid, i.e., negative
            if (timestamp < 0) {
                log.warn(
                    "Skipping record due to negative extracted timestamp. topic=[{}] partition=[{}] offset=[{}] extractedTimestamp=[{}] extractor=[{}]",
                    record.topic(), record.partition(), record.offset(), timestamp, timestampExtractor.getClass().getCanonicalName()
                );
                ((StreamsMetricsImpl) processorContext.metrics()).skippedRecordsSensor().record();
                continue;
            }

            final StampedRecord stampedRecord = new StampedRecord(record, timestamp);
            fifoQueue.addLast(stampedRecord);
            timeTracker.addElement(stampedRecord);
        }

        // update the partition timestamp if its currently
        // tracked min timestamp has exceed its value; this will
        // usually only take effect for the first added batch
        final long timestamp = timeTracker.get();

        if (timestamp > partitionTime) {
            partitionTime = timestamp;
        }

        return size();
    }

    /**
     * Get the next {@link StampedRecord} from the queue
     *
     * @return StampedRecord
     */
    public StampedRecord poll() {
        final StampedRecord elem = fifoQueue.pollFirst();

        if (elem == null) {
            return null;
        }

        timeTracker.removeElement(elem);

        // only advance the partition timestamp if its currently
        // tracked min timestamp has exceeded its value
        final long timestamp = timeTracker.get();

        if (timestamp > partitionTime) {
            partitionTime = timestamp;
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

    /**
     * Clear the fifo queue of its elements, also clear the time tracker's kept stamped elements
     */
    public void clear() {
        fifoQueue.clear();
        timeTracker.clear();
        partitionTime = TimestampTracker.NOT_KNOWN;
    }

    /*
     * Returns the timestamp tracker of the record queue
     *
     * This is only used for testing
     */
    TimestampTracker<ConsumerRecord<Object, Object>> timeTracker() {
        return timeTracker;
    }
}
