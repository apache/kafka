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
import org.apache.kafka.common.requests.LazyDownConversionRecordsSend;
import org.apache.kafka.common.requests.RecordsSend;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;

import java.nio.channels.GatheringByteChannel;
import java.util.Arrays;

/**
 * Encapsulation for holding down-converted records with implementation for being able to "lazily" down-convert records.
 * Records are down-converted in batches and on-demand when {@link #writeTo} method is called. The implementation ensures
 * that we are able to send out at least one full batch of messages after down-conversion if {@link #writeTo} is used in
 * conjunction with {@link #sizeInBytes} - see {@link org.apache.kafka.common.requests.RecordsSend#writeTo} for an example.
 * <p> Because we do not have a full view of the underlying down-converted records, lot of methods typically associated
 * with {@link Records} are unsupported and not implemented. Specifically, this class provides implementations for
 * {@link #sizeInBytes() sizeInBytes} and {@link #writeTo(GatheringByteChannel, long, int) writeTo} methods only. </p>
 */
public class LazyDownConversionRecords implements BaseRecords {
    private final TopicPartition topicPartition;
    private final Records records;
    private final byte toMagic;
    private final long firstOffset;
    private final int minimumSize;
    private static final long MAX_READ_SIZE = 128L * 1024L;

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

        AbstractIterator<? extends RecordBatch> it = records.batchIterator();
        if (it.hasNext())
            minimumSize = RecordsUtil.downConvert(
                    Arrays.asList(it.peek()), toMagic, firstOffset, new SystemTime()).records().sizeInBytes();
        else
            minimumSize = 0;
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

    @Override
    public RecordsSend toSend(String destination) {
        return new LazyDownConversionRecordsSend(destination, this);
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof LazyDownConversionRecords) {
            LazyDownConversionRecords that = (LazyDownConversionRecords) o;
            return (toMagic == that.toMagic) && records.equals(that.records);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return records.hashCode();
    }

    // Protected for unit tests
    protected LazyDownConversionRecordsIterator lazyDownConversionRecordsIterator(long maximumReadSize) {
        return new LazyDownConversionRecordsIterator(records, toMagic, firstOffset, maximumReadSize);
    }

    public LazyDownConversionRecordsIterator lazyDownConversionRecordsIterator() {
        return lazyDownConversionRecordsIterator(MAX_READ_SIZE);
    }
}
