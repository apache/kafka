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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Time;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Encapsulation for holding records that require down-conversion in a lazy, chunked manner (KIP-283). See
 * {@link LazyDownConversionRecordsSend} for the actual chunked send implementation.
 */
public class LazyDownConversionRecords implements BaseRecords {
    private final TopicPartition topicPartition;
    private final Records records;
    private final byte toMagic;
    private final long firstOffset;
    private ConvertedRecords firstConvertedBatch;
    private final int sizeInBytes;
    private final Time time;

    /**
     * @param topicPartition The topic-partition to which records belong
     * @param records Records to lazily down-convert
     * @param toMagic Magic version to down-convert to
     * @param firstOffset The starting offset for down-converted records. This only impacts some cases. See
     *                    {@link RecordsUtil#downConvert(Iterable, byte, long, Time)} for an explanation.
     * @param time The time instance to use
     *
     * @throws org.apache.kafka.common.errors.UnsupportedCompressionTypeException If the first batch to down-convert
     *    has a compression type which we do not support down-conversion for.
     */
    public LazyDownConversionRecords(TopicPartition topicPartition, Records records, byte toMagic, long firstOffset, Time time) {
        this.topicPartition = Objects.requireNonNull(topicPartition);
        this.records = Objects.requireNonNull(records);
        this.toMagic = toMagic;
        this.firstOffset = firstOffset;
        this.time = Objects.requireNonNull(time);

        // Kafka consumers expect at least one full batch of messages for every topic-partition. To guarantee this, we
        // need to make sure that we are able to accommodate one full batch of down-converted messages. The way we achieve
        // this is by having sizeInBytes method factor in the size of the first down-converted batch and return at least
        // its size.
        java.util.Iterator<ConvertedRecords<?>> it = iterator(0);
        if (it.hasNext()) {
            firstConvertedBatch = it.next();
            sizeInBytes = Math.max(records.sizeInBytes(), firstConvertedBatch.records().sizeInBytes());
        } else {
            // If there are no messages we got after down-conversion, make sure we are able to send at least an overflow
            // message to the consumer. Typically, the consumer would need to increase the fetch size in such cases.
            firstConvertedBatch = null;
            sizeInBytes = LazyDownConversionRecordsSend.MIN_OVERFLOW_MESSAGE_LENGTH;
        }
    }

    @Override
    public int sizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public LazyDownConversionRecordsSend toSend(String destination) {
        return new LazyDownConversionRecordsSend(destination, this);
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof LazyDownConversionRecords) {
            LazyDownConversionRecords that = (LazyDownConversionRecords) o;
            return toMagic == that.toMagic &&
                    firstOffset == that.firstOffset &&
                    topicPartition.equals(that.topicPartition) &&
                    records.equals(that.records);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = toMagic;
        result = 31 * result + (int) (firstOffset ^ (firstOffset >>> 32));
        result = 31 * result + topicPartition.hashCode();
        result = 31 * result + records.hashCode();
        return result;
    }

    public java.util.Iterator<ConvertedRecords<?>> iterator(long maximumReadSize) {
        // We typically expect only one iterator instance to be created, so null out the first converted batch after
        // first use to make it available for GC.
        ConvertedRecords firstBatch = firstConvertedBatch;
        firstConvertedBatch = null;
        return new Iterator(records, maximumReadSize, firstBatch);
    }

    /**
     * Implementation for being able to iterate over down-converted records. Goal of this implementation is to keep
     * it as memory-efficient as possible by not having to maintain all down-converted records in-memory. Maintains
     * a view into batches of down-converted records.
     */
    private class Iterator extends AbstractIterator<ConvertedRecords<?>> {
        private final AbstractIterator<? extends RecordBatch> batchIterator;
        private final long maximumReadSize;
        private ConvertedRecords firstConvertedBatch;

        /**
         * @param recordsToDownConvert Records that require down-conversion
         * @param maximumReadSize Maximum possible size of underlying records that will be down-converted in each call to
         *                        {@link #makeNext()}. This is a soft limit as {@link #makeNext()} will always convert
         *                        and return at least one full message batch.
         */
        private Iterator(Records recordsToDownConvert, long maximumReadSize, ConvertedRecords<?> firstConvertedBatch) {
            this.batchIterator = recordsToDownConvert.batchIterator();
            this.maximumReadSize = maximumReadSize;
            this.firstConvertedBatch = firstConvertedBatch;
            // If we already have the first down-converted batch, advance the underlying records iterator to next batch
            if (firstConvertedBatch != null)
                this.batchIterator.next();
        }

        /**
         * Make next set of down-converted records
         * @return Down-converted records
         */
        @Override
        protected ConvertedRecords makeNext() {
            // If we have cached the first down-converted batch, return that now
            if (firstConvertedBatch != null) {
                ConvertedRecords convertedBatch = firstConvertedBatch;
                firstConvertedBatch = null;
                return convertedBatch;
            }

            while (batchIterator.hasNext()) {
                final List<RecordBatch> batches = new ArrayList<>();
                boolean isFirstBatch = true;
                long sizeSoFar = 0;

                // Figure out batches we should down-convert based on the size constraints
                while (batchIterator.hasNext() &&
                        (isFirstBatch || (batchIterator.peek().sizeInBytes() + sizeSoFar) <= maximumReadSize)) {
                    RecordBatch currentBatch = batchIterator.next();
                    batches.add(currentBatch);
                    sizeSoFar += currentBatch.sizeInBytes();
                    isFirstBatch = false;
                }

                ConvertedRecords convertedRecords = RecordsUtil.downConvert(batches, toMagic, firstOffset, time);
                // During conversion, it is possible that we drop certain batches because they do not have an equivalent
                // representation in the message format we want to convert to. For example, V0 and V1 message formats
                // have no notion of transaction markers which were introduced in V2 so they get dropped during conversion.
                // We return converted records only when we have at least one valid batch of messages after conversion.
                if (convertedRecords.records().sizeInBytes() > 0)
                    return convertedRecords;
            }
            return allDone();
        }
    }
}
