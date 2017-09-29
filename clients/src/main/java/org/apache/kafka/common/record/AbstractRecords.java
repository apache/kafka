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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class AbstractRecords implements Records {

    private final Iterable<Record> records = new Iterable<Record>() {
        @Override
        public Iterator<Record> iterator() {
            return recordsIterator();
        }
    };

    @Override
    public boolean hasMatchingMagic(byte magic) {
        for (RecordBatch batch : batches())
            if (batch.magic() != magic)
                return false;
        return true;
    }

    @Override
    public boolean hasCompatibleMagic(byte magic) {
        for (RecordBatch batch : batches())
            if (batch.magic() > magic)
                return false;
        return true;
    }

    /**
     * Down convert batches to the provided message format version. The first offset parameter is only relevant in the
     * conversion from uncompressed v2 or higher to v1 or lower. The reason is that uncompressed records in v0 and v1
     * are not batched (put another way, each batch always has 1 record).
     *
     * If a client requests records in v1 format starting from the middle of an uncompressed batch in v2 format, we
     * need to drop records from the batch during the conversion. Some versions of librdkafka rely on this for
     * correctness.
     *
     * The temporaryMemoryBytes computation assumes that the batches are not loaded into the heap
     * (via classes like FileChannelRecordBatch) before this method is called. This is the case in the broker (we
     * only load records into the heap when down converting), but it's not for the producer. However, down converting
     * in the producer is very uncommon and the extra complexity to handle that case is not worth it.
     */
    protected ConvertedRecords<MemoryRecords> downConvert(Iterable<? extends RecordBatch> batches, byte toMagic,
            long firstOffset, Time time) {
        // maintain the batch along with the decompressed records to avoid the need to decompress again
        List<RecordBatchAndRecords> recordBatchAndRecordsList = new ArrayList<>();
        int totalSizeEstimate = 0;
        long startNanos = time.nanoseconds();

        for (RecordBatch batch : batches) {
            if (toMagic < RecordBatch.MAGIC_VALUE_V2 && batch.isControlBatch())
                continue;

            if (batch.magic() <= toMagic) {
                totalSizeEstimate += batch.sizeInBytes();
                recordBatchAndRecordsList.add(new RecordBatchAndRecords(batch, null, null));
            } else {
                List<Record> records = new ArrayList<>();
                for (Record record : batch) {
                    // See the method javadoc for an explanation
                    if (toMagic > RecordBatch.MAGIC_VALUE_V1 || batch.isCompressed() || record.offset() >= firstOffset)
                        records.add(record);
                }
                if (records.isEmpty())
                    continue;
                final long baseOffset;
                if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 && toMagic >= RecordBatch.MAGIC_VALUE_V2)
                    baseOffset = batch.baseOffset();
                else
                    baseOffset = records.get(0).offset();
                totalSizeEstimate += estimateSizeInBytes(toMagic, baseOffset, batch.compressionType(), records);
                recordBatchAndRecordsList.add(new RecordBatchAndRecords(batch, records, baseOffset));
            }
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalSizeEstimate);
        long temporaryMemoryBytes = 0;
        int numRecordsConverted = 0;
        for (RecordBatchAndRecords recordBatchAndRecords : recordBatchAndRecordsList) {
            temporaryMemoryBytes += recordBatchAndRecords.batch.sizeInBytes();
            if (recordBatchAndRecords.batch.magic() <= toMagic) {
                recordBatchAndRecords.batch.writeTo(buffer);
            } else {
                MemoryRecordsBuilder builder = convertRecordBatch(toMagic, buffer, recordBatchAndRecords);
                buffer = builder.buffer();
                temporaryMemoryBytes += builder.uncompressedBytesWritten();
                numRecordsConverted += builder.numRecords();
            }
        }

        buffer.flip();
        RecordsProcessingStats stats = new RecordsProcessingStats(temporaryMemoryBytes, numRecordsConverted,
                time.nanoseconds() - startNanos);
        return new ConvertedRecords<>(MemoryRecords.readableRecords(buffer), stats);
    }

    /**
     * Return a buffer containing the converted record batches. The returned buffer may not be the same as the received
     * one (e.g. it may require expansion).
     */
    private MemoryRecordsBuilder convertRecordBatch(byte magic, ByteBuffer buffer, RecordBatchAndRecords recordBatchAndRecords) {
        RecordBatch batch = recordBatchAndRecords.batch;
        final TimestampType timestampType = batch.timestampType();
        long logAppendTime = timestampType == TimestampType.LOG_APPEND_TIME ? batch.maxTimestamp() : RecordBatch.NO_TIMESTAMP;

        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic, batch.compressionType(),
                timestampType, recordBatchAndRecords.baseOffset, logAppendTime);
        for (Record record : recordBatchAndRecords.records)
            builder.append(record);

        builder.close();
        return builder;
    }

    /**
     * Get an iterator over the deep records.
     * @return An iterator over the records
     */
    @Override
    public Iterable<Record> records() {
        return records;
    }

    private Iterator<Record> recordsIterator() {
        return new AbstractIterator<Record>() {
            private final Iterator<? extends RecordBatch> batches = batches().iterator();
            private Iterator<Record> records;

            @Override
            protected Record makeNext() {
                if (records != null && records.hasNext())
                    return records.next();

                if (batches.hasNext()) {
                    records = batches.next().iterator();
                    return makeNext();
                }

                return allDone();
            }
        };
    }

    public static int estimateSizeInBytes(byte magic,
                                          long baseOffset,
                                          CompressionType compressionType,
                                          Iterable<Record> records) {
        int size = 0;
        if (magic <= RecordBatch.MAGIC_VALUE_V1) {
            for (Record record : records)
                size += Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, record.key(), record.value());
        } else {
            size = DefaultRecordBatch.sizeInBytes(baseOffset, records);
        }
        return estimateCompressedSizeInBytes(size, compressionType);
    }

    public static int estimateSizeInBytes(byte magic,
                                          CompressionType compressionType,
                                          Iterable<SimpleRecord> records) {
        int size = 0;
        if (magic <= RecordBatch.MAGIC_VALUE_V1) {
            for (SimpleRecord record : records)
                size += Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, record.key(), record.value());
        } else {
            size = DefaultRecordBatch.sizeInBytes(records);
        }
        return estimateCompressedSizeInBytes(size, compressionType);
    }

    private static int estimateCompressedSizeInBytes(int size, CompressionType compressionType) {
        return compressionType == CompressionType.NONE ? size : Math.min(Math.max(size / 2, 1024), 1 << 16);
    }

    /**
     * Get an upper bound estimate on the batch size needed to hold a record with the given fields. This is only
     * an estimate because it does not take into account overhead from the compression algorithm.
     */
    public static int estimateSizeInBytesUpperBound(byte magic, CompressionType compressionType, byte[] key, byte[] value, Header[] headers) {
        return estimateSizeInBytesUpperBound(magic, compressionType, Utils.wrapNullable(key), Utils.wrapNullable(value), headers);
    }

    /**
     * Get an upper bound estimate on the batch size needed to hold a record with the given fields. This is only
     * an estimate because it does not take into account overhead from the compression algorithm.
     */
    public static int estimateSizeInBytesUpperBound(byte magic, CompressionType compressionType, ByteBuffer key,
                                                    ByteBuffer value, Header[] headers) {
        if (magic >= RecordBatch.MAGIC_VALUE_V2)
            return DefaultRecordBatch.estimateBatchSizeUpperBound(key, value, headers);
        else if (compressionType != CompressionType.NONE)
            return Records.LOG_OVERHEAD + LegacyRecord.recordOverhead(magic) + LegacyRecord.recordSize(magic, key, value);
        else
            return Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, key, value);
    }

    /**
     * Return the size of the record batch header.
     *
     * For V0 and V1 with no compression, it's unclear if Records.LOG_OVERHEAD or 0 should be chosen. There is no header
     * per batch, but a sequence of batches is preceded by the offset and size. This method returns `0` as it's what
     * `MemoryRecordsBuilder` requires.
     */
    public static int recordBatchHeaderSizeInBytes(byte magic, CompressionType compressionType) {
        if (magic > RecordBatch.MAGIC_VALUE_V1) {
            return DefaultRecordBatch.RECORD_BATCH_OVERHEAD;
        } else if (compressionType != CompressionType.NONE) {
            return Records.LOG_OVERHEAD + LegacyRecord.recordOverhead(magic);
        } else {
            return 0;
        }
    }

    private static class RecordBatchAndRecords {
        private final RecordBatch batch;
        private final List<Record> records;
        private final Long baseOffset;

        private RecordBatchAndRecords(RecordBatch batch, List<Record> records, Long baseOffset) {
            this.batch = batch;
            this.records = records;
            this.baseOffset = baseOffset;
        }
    }

}
