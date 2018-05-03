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
package org.apache.kafka.common.record;

import org.apache.kafka.common.RecordsProcessingStats;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionRecordsStats;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Encapsulation for holding down-converted records with implementation for being able to "lazily" down-convert records.
 * Records are down-converted in batches and on-demand when {@link #writeTo} method is called. The implementation ensures
 * that we are able to send out at least one full batch of messages after down-conversion if {@link #writeTo} is used in
 * conjunction with {@link #sizeInBytes} - see {@link org.apache.kafka.common.requests.RecordsSend#writeTo} for an example.
 * <p> Because we do not have a full view of the underlying down-converted records, lot of methods typically associated
 * with {@link Records} are unsupported and not implemented. Specifically, this class provides implementations for
 * {@link #sizeInBytes() sizeInBytes} and {@link #writeTo(GatheringByteChannel, long, int) writeTo} methods only. </p>
 */
public class LazyDownConversionRecords implements SerializableRecords {
    private static final Logger log = LoggerFactory.getLogger(LazyDownConversionRecords.class);
    private final TopicPartition topicPartition;
    private final Records records;
    private final byte toMagic;
    private final long firstOffset;
    private final int minimumSize;
    private RecordsWriter convertedRecordsWriter = null;
    private LazyDownConversionRecordsIterator convertedRecordsIterator = null;
    private RecordsProcessingStats processingStats = null;
    private static final long MAX_READ_SIZE = 16L * 1024L;

    /**
     * @param records Records to lazily down-convert
     * @param toMagic Magic version to down-convert to
     * @param firstOffset The starting offset for down-converted records. This only impacts some cases. See
     *                    {@link RecordsUtil#downConvert(Iterable, byte, long, Time)} for an explanation.
     */
    public LazyDownConversionRecords(TopicPartition topicPartition, Records records, byte toMagic, long firstOffset) {
        this.topicPartition = topicPartition;
        this.records = records;
        this.toMagic = toMagic;
        this.firstOffset = firstOffset;
        this.minimumSize = RecordsUtil.downConvert(
                Arrays.asList(records.batchIterator().peek()), toMagic, firstOffset, new SystemTime())
                .records()
                .sizeInBytes();
    }

    /**
     * {@inheritDoc}
     * Note that we do not have a way to return the exact size of down-converted messages, so we return the size of the
     * pre-down-converted messages. The consumer however expects at least one full batch of messages to be sent out so
     * we also factor in the down-converted size of the first batch.
     */
    @Override
    public int sizeInBytes() {
        return Math.max(records.sizeInBytes(), minimumSize);
    }

    /**
     * {@inheritDoc}
     * Note that this is a stateful method. The expected use case is to start writing to the underlying channel starting
     * with (position = 0, length = sizeInBytes()), with position being advanced by the bytes written and the length being
     * reduced by the same amount. Example usage is the following:
     * <pre>
     *     LazyDownConversionRecords records = ...
     *     int length = records.sizeInBytes();
     *     int remaining = length;
     *
     *     while (remaining > 0)
     *         remaining -= records.writeTo(channel, length - remaining, remaining);
     * </pre>
     * Also see {@link org.apache.kafka.common.requests.RecordsSend#writeTo} for example usage.
     */
    @Override
    public long writeTo(GatheringByteChannel channel, long position, int length) throws IOException {
        if (position == 0) {
            log.info("Initializing lazy down-conversion for {" + topicPartition + "} with length=" + length);
            convertedRecordsIterator = lazyDownConversionRecordsIterator(MAX_READ_SIZE);
            processingStats = new RecordsProcessingStats(0, 0, 0);
        }

        if ((convertedRecordsWriter == null) || (convertedRecordsWriter.remaining() == 0)) {
            Records convertedRecords;

            if (convertedRecordsIterator.hasNext()) {
                // Get next set of down-converted records
                ConvertedRecords recordsAndStats = convertedRecordsIterator.next();
                convertedRecords = recordsAndStats.records();

                if ((position == 0) && (convertedRecords.batchIterator().peek().sizeInBytes() > length))
                    throw new EOFException("Unable to send first batch completely." +
                            " batch_size: " + convertedRecords.batchIterator().peek().sizeInBytes() +
                            " required_size: " + length +
                            " minimum_size: " + minimumSize +
                            " records_size: " + records.sizeInBytes());

                processingStats.addToProcessingStats(recordsAndStats.recordsProcessingStats());
                log.info("Got lazy converted records for {" + topicPartition + "} with length=" + convertedRecords.sizeInBytes());
            } else {
                if (position == 0)
                    throw new EOFException("Unable to get the first batch of down-converted records");

                // We do not have any records left to down-convert. Construct a "fake" message for the length remaining.
                // This message will be ignored by the consumer because its length will be past the length of maximum
                // possible response size.
                // DefaultRecordBatch =>
                //      BaseOffset => Int64
                //      Length => Int32
                //      ...
                // TODO: check if there is a better way to encapsulate this logic, perhaps in DefaultRecordBatch
                log.info("Constructing fake message batch for topic-partition {" + topicPartition + "} for remaining length " + length);
                int minLength = (Long.SIZE / Byte.SIZE) + (Integer.SIZE / Byte.SIZE);
                ByteBuffer fakeMessageBatch = ByteBuffer.allocate(Math.max(minLength, length + 1));
                fakeMessageBatch.putLong(-1L);
                fakeMessageBatch.putInt(length + 1);
                convertedRecords = MemoryRecords.readableRecords(fakeMessageBatch);
            }
            convertedRecordsWriter = new RecordsWriter(convertedRecords);
        }

        return convertedRecordsWriter.writeTo(channel, length);
    }

    @Override
    public TopicPartitionRecordsStats recordsProcessingStats() {
        return new TopicPartitionRecordsStats(topicPartition, processingStats);
    }

    // Protected for unit tests
    protected LazyDownConversionRecordsIterator lazyDownConversionRecordsIterator(long maximumReadSize) {
        return new LazyDownConversionRecordsIterator(records, maximumReadSize);
    }

    /**
     * Implementation for writing {@link Records} to a particular channel. Internally tracks the progress of writes.
     */
    private static class RecordsWriter {
        private final Records records;
        private int position;

        RecordsWriter(Records records) {
            if (records == null)
                throw new IllegalArgumentException();
            this.records = records;
            position = 0;
        }

        private int position() {
            return position;
        }

        public int remaining() {
            return records.sizeInBytes() - position();
        }

        private void advancePosition(long numBytes) {
            position += numBytes;
        }

        public long writeTo(GatheringByteChannel channel, int length) throws IOException {
            int maxLength = Math.min(remaining(), length);
            long written = records.writeTo(channel, position(), maxLength);
            advancePosition(written);
            return written;
        }
    }

    /**
     * Implementation for being able to iterate over down-converted records. Goal of this implementation is to keep
     * it as memory-efficient as possible by not having to maintain all down-converted records in-memory. Maintains
     * a view into batches of down-converted records.
     */
    protected class LazyDownConversionRecordsIterator extends AbstractIterator<ConvertedRecords> {
        /**
         * Iterator over records that require down-conversion.
         */
        private final AbstractIterator<? extends RecordBatch> batchIterator;

        /**
         * Maximum possible size of messages that will be read. This limit does not apply for the first message batch.
         */
        private final long maximumReadSize;

        LazyDownConversionRecordsIterator(Records recordsToDownConvert, long maximumReadSize) {
            this.batchIterator = recordsToDownConvert.batchIterator();
            this.maximumReadSize = maximumReadSize;
        }

        /**
         * Make next set of down-converted records
         * @return Down-converted records
         */
        @Override
        protected ConvertedRecords makeNext() {
            List<RecordBatch> batches = new ArrayList<>();
            boolean isFirstBatch = true;
            long sizeSoFar = 0;

            if (!batchIterator.hasNext())
                return allDone();

            // Figure out batches we should down-convert based on the size constraints
            while (batchIterator.hasNext() &&
                    (isFirstBatch || (batchIterator.peek().sizeInBytes() + sizeSoFar) <= maximumReadSize)) {
                RecordBatch currentBatch = batchIterator.next();
                batches.add(currentBatch);
                sizeSoFar += currentBatch.sizeInBytes();
                isFirstBatch = false;
            }
            return RecordsUtil.downConvert(batches, toMagic, firstOffset, new SystemTime());
        }
    }
}
