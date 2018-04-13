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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Encapsulation for holding down-converted records with implementation for being able to "lazily" down-convert records.
 * Records are down-converted in batches and on-demand when {@link #writeTo} method is called.
 * <p>
 * Because we do not have a full view of the underlying down-converted records, lot of methods typically associated with
 * {@link Records} are unsupported and not implemented. Specifically, this class provides implementations for
 * {@link #sizeInBytes() sizeInBytes} and {@link #writeTo(GatheringByteChannel, long, int) writeTo} methods only.
 * </p>
 */
public class LazyDownConversionRecords implements Records {
    private final TopicPartition topicPartition;
    private final Records records;
    private final byte toMagic;
    private final long firstOffset;
    private RecordsWriter convertedRecordsWriter = null;
    private LazyDownConversionRecordsIterator recordsIterator = null;
    private RecordsProcessingStats processingStats = null;

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
    }

    /**
     * {@inheritDoc}
     * Note that this method returns the size of records before down-conversion, because we do not have a full view
     * of the down-converted records.
     */
    @Override
    public int sizeInBytes() {
        return records.sizeInBytes();
    }

    @Override
    public long writeTo(GatheringByteChannel channel, long position, int length) throws IOException {
        if (position == 0) {
            recordsIterator = new LazyDownConversionRecordsIterator(records);
            processingStats = new RecordsProcessingStats(0, 0, 0);
        }

        if ((convertedRecordsWriter == null) || (convertedRecordsWriter.remaining() == 0)) {
            Records convertedRecords;

            if (recordsIterator.hasNext()) {
                // Get next set of down-converted records
                ConvertedRecords recordsAndStats = recordsIterator.next();
                convertedRecords = recordsAndStats.records();
                processingStats.addToProcessingStats(recordsAndStats.recordsProcessingStats());
            } else {
                // We do not have any records left to down-convert. Construct a "fake" message for the length remaining.
                // This message will be ignored by the consumer because its length will be past the length of maximum
                // possible response size.
                // TODO: check if this implementation is correct.
                // TODO: there should be a better way to encapsulate this logic.
                byte[] fakeMessage = new byte[length];
                Arrays.fill(fakeMessage, Byte.MAX_VALUE);
                ByteBuffer buffer = ByteBuffer.wrap(fakeMessage);
                convertedRecords = MemoryRecords.readableRecords(buffer);
            }
            convertedRecordsWriter = new RecordsWriter(convertedRecords);
        }
        return convertedRecordsWriter.writeTo(channel, length);
    }

    @Override
    public Iterable<? extends RecordBatch> batches() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasMatchingMagic(byte magic) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasCompatibleMagic(byte magic) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConvertedRecords<? extends Records> downConvert(byte toMagic, long firstOffset, Time time) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Record> records() {
        throw new UnsupportedOperationException();
    }

    @Override
    public AbstractIterator<? extends RecordBatch> batchIterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TopicPartitionRecordsStats recordsProcessingStats() {
        return new TopicPartitionRecordsStats(topicPartition, processingStats);
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
    private class LazyDownConversionRecordsIterator extends AbstractIterator<ConvertedRecords> {
        /**
         * Iterator over records that require down-conversion.
         */
        private final AbstractIterator<? extends RecordBatch> batchIterator;

        /**
         * Maximum possible size of messages that will be read. This limit does not apply for the first message batch.
         */
        private static final long MAX_READ_SIZE = 16L * 1024L;

        LazyDownConversionRecordsIterator(Records recordsToDownConvert) {
            batchIterator = recordsToDownConvert.batchIterator();
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
                    (isFirstBatch || (batchIterator.peek().sizeInBytes() + sizeSoFar) <= MAX_READ_SIZE)) {
                RecordBatch currentBatch = batchIterator.next();
                batches.add(currentBatch);
                sizeSoFar += currentBatch.sizeInBytes();
                isFirstBatch = false;
            }
            return RecordsUtil.downConvert(batches, toMagic, firstOffset, new SystemTime());
        }
    }
}
