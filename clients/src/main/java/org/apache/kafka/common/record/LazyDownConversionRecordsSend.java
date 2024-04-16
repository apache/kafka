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
import org.apache.kafka.common.errors.UnsupportedCompressionTypeException;
import org.apache.kafka.common.network.TransferableChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Encapsulation for {@link RecordsSend} for {@link LazyDownConversionRecords}. Records are down-converted in batches and
 * on-demand when {@link #writeTo} method is called.
 */
public final class LazyDownConversionRecordsSend extends RecordsSend<LazyDownConversionRecords> {
    private static final Logger log = LoggerFactory.getLogger(LazyDownConversionRecordsSend.class);
    private static final int MAX_READ_SIZE = 128 * 1024;
    static final int MIN_OVERFLOW_MESSAGE_LENGTH = Records.LOG_OVERHEAD;

    private final RecordValidationStats recordValidationStats;
    private final Iterator<ConvertedRecords<?>> convertedRecordsIterator;

    private RecordsSend<MemoryRecords> convertedRecordsWriter;

    public LazyDownConversionRecordsSend(LazyDownConversionRecords records) {
        super(records, records.sizeInBytes());
        convertedRecordsWriter = null;
        recordValidationStats = new RecordValidationStats();
        convertedRecordsIterator = records().iterator(MAX_READ_SIZE);
    }

    private MemoryRecords buildOverflowBatch(int remaining) {
        // We do not have any records left to down-convert. Construct an overflow message for the length remaining.
        // This message will be ignored by the consumer because its length will be past the length of maximum
        // possible response size.
        // DefaultRecordBatch =>
        //      BaseOffset => Int64
        //      Length => Int32
        //      ...
        ByteBuffer overflowMessageBatch = ByteBuffer.allocate(
                Math.max(MIN_OVERFLOW_MESSAGE_LENGTH, Math.min(remaining + 1, MAX_READ_SIZE)));
        overflowMessageBatch.putLong(-1L);

        // Fill in the length of the overflow batch. A valid batch must be at least as long as the minimum batch
        // overhead.
        overflowMessageBatch.putInt(Math.max(remaining + 1, DefaultRecordBatch.RECORD_BATCH_OVERHEAD));
        log.debug("Constructed overflow message batch for partition {} with length={}", topicPartition(), remaining);
        return MemoryRecords.readableRecords(overflowMessageBatch);
    }

    @Override
    public int writeTo(TransferableChannel channel, int previouslyWritten, int remaining) throws IOException {
        if (convertedRecordsWriter == null || convertedRecordsWriter.completed()) {
            MemoryRecords convertedRecords;

            try {
                // Check if we have more chunks left to down-convert
                if (convertedRecordsIterator.hasNext()) {
                    // Get next chunk of down-converted messages
                    ConvertedRecords<?> recordsAndStats = convertedRecordsIterator.next();
                    convertedRecords = (MemoryRecords) recordsAndStats.records();
                    recordValidationStats.add(recordsAndStats.recordConversionStats());
                    log.debug("Down-converted records for partition {} with length={}", topicPartition(), convertedRecords.sizeInBytes());
                } else {
                    convertedRecords = buildOverflowBatch(remaining);
                }
            } catch (UnsupportedCompressionTypeException e) {
                // We have encountered a compression type which does not support down-conversion (e.g. zstd).
                // Since we have already sent at least one batch and we have committed to the fetch size, we
                // send an overflow batch. The consumer will read the first few records and then fetch from the
                // offset of the batch which has the unsupported compression type. At that time, we will
                // send back the UNSUPPORTED_COMPRESSION_TYPE error which will allow the consumer to fail gracefully.
                convertedRecords = buildOverflowBatch(remaining);
            }

            convertedRecordsWriter = new DefaultRecordsSend<>(convertedRecords, Math.min(convertedRecords.sizeInBytes(), remaining));
        }
        // safe to cast to int since `remaining` is an int
        return (int) convertedRecordsWriter.writeTo(channel);
    }

    public RecordValidationStats recordConversionStats() {
        return recordValidationStats;
    }

    public TopicPartition topicPartition() {
        return records().topicPartition();
    }
}
