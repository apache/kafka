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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(value = Parameterized.class)
public class MemoryRecordsBuilderTest {

    private final CompressionType compressionType;
    private final int bufferOffset;
    private final Time time;

    public MemoryRecordsBuilderTest(int bufferOffset, CompressionType compressionType) {
        this.bufferOffset = bufferOffset;
        this.compressionType = compressionType;
        this.time = Time.SYSTEM;
    }

    @Test
    public void testWriteEmptyRecordSet() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.position(bufferOffset);

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.MAGIC_VALUE_V0, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        MemoryRecords records = builder.build();
        assertEquals(0, records.sizeInBytes());
        assertEquals(bufferOffset, buffer.position());
    }

    @Test
    public void testWriteTransactionalRecordSet() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.position(bufferOffset);

        long pid = 9809;
        short epoch = 15;
        int sequence = 2342;

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, pid, epoch, sequence, true, false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.append(System.currentTimeMillis(), "foo".getBytes(), "bar".getBytes());
        MemoryRecords records = builder.build();

        List<MutableRecordBatch> batches = Utils.toList(records.batches().iterator());
        assertEquals(1, batches.size());
        assertTrue(batches.get(0).isTransactional());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteTransactionalNotAllowedMagicV0() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.position(bufferOffset);

        long pid = 9809;
        short epoch = 15;
        int sequence = 2342;

        new MemoryRecordsBuilder(buffer, RecordBatch.MAGIC_VALUE_V0, compressionType, TimestampType.CREATE_TIME,
                0L, 0L, pid, epoch, sequence, true, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteTransactionalNotAllowedMagicV1() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.position(bufferOffset);

        long pid = 9809;
        short epoch = 15;
        int sequence = 2342;

        new MemoryRecordsBuilder(buffer, RecordBatch.MAGIC_VALUE_V1, compressionType, TimestampType.CREATE_TIME,
                0L, 0L, pid, epoch, sequence, true, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteControlBatchNotAllowedMagicV0() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.position(bufferOffset);

        long pid = 9809;
        short epoch = 15;
        int sequence = 2342;

        new MemoryRecordsBuilder(buffer, RecordBatch.MAGIC_VALUE_V0, compressionType, TimestampType.CREATE_TIME,
                0L, 0L, pid, epoch, sequence, false, true, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteControlBatchNotAllowedMagicV1() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.position(bufferOffset);

        long pid = 9809;
        short epoch = 15;
        int sequence = 2342;

        new MemoryRecordsBuilder(buffer, RecordBatch.MAGIC_VALUE_V1, compressionType, TimestampType.CREATE_TIME,
                0L, 0L, pid, epoch, sequence, false, true, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteTransactionalWithInvalidPID() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.position(bufferOffset);

        long pid = RecordBatch.NO_PRODUCER_ID;
        short epoch = 15;
        int sequence = 2342;

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compressionType, TimestampType.CREATE_TIME,
                0L, 0L, pid, epoch, sequence, true, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteIdempotentWithInvalidEpoch() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.position(bufferOffset);

        long pid = 9809;
        short epoch = RecordBatch.NO_PRODUCER_EPOCH;
        int sequence = 2342;

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compressionType, TimestampType.CREATE_TIME,
                0L, 0L, pid, epoch, sequence, true, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteIdempotentWithInvalidBaseSequence() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.position(bufferOffset);

        long pid = 9809;
        short epoch = 15;
        int sequence = RecordBatch.NO_SEQUENCE;

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compressionType, TimestampType.CREATE_TIME,
                0L, 0L, pid, epoch, sequence, true, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteEndTxnMarkerNonTransactionalBatch() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.position(bufferOffset);

        long pid = 9809;
        short epoch = 15;
        int sequence = RecordBatch.NO_SEQUENCE;

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compressionType, TimestampType.CREATE_TIME,
                0L, 0L, pid, epoch, sequence, false, true, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.appendEndTxnMarker(RecordBatch.NO_TIMESTAMP, new EndTransactionMarker(ControlRecordType.ABORT, 0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteEndTxnMarkerNonControlBatch() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.position(bufferOffset);

        long pid = 9809;
        short epoch = 15;
        int sequence = RecordBatch.NO_SEQUENCE;

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compressionType, TimestampType.CREATE_TIME,
                0L, 0L, pid, epoch, sequence, true, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.appendEndTxnMarker(RecordBatch.NO_TIMESTAMP, new EndTransactionMarker(ControlRecordType.ABORT, 0));
    }

    @Test
    public void testCompressionRateV0() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.position(bufferOffset);

        LegacyRecord[] records = new LegacyRecord[] {
                LegacyRecord.create(RecordBatch.MAGIC_VALUE_V0, 0L, "a".getBytes(), "1".getBytes()),
                LegacyRecord.create(RecordBatch.MAGIC_VALUE_V0, 1L, "b".getBytes(), "2".getBytes()),
                LegacyRecord.create(RecordBatch.MAGIC_VALUE_V0, 2L, "c".getBytes(), "3".getBytes()),
        };

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.MAGIC_VALUE_V0, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());

        int uncompressedSize = 0;
        for (LegacyRecord record : records) {
            uncompressedSize += record.sizeInBytes() + Records.LOG_OVERHEAD;
            builder.append(record);
        }

        MemoryRecords built = builder.build();
        if (compressionType == CompressionType.NONE) {
            assertEquals(1.0, builder.compressionRatio(), 0.00001);
        } else {
            int compressedSize = built.sizeInBytes() - Records.LOG_OVERHEAD - LegacyRecord.RECORD_OVERHEAD_V0;
            double computedCompressionRate = (double) compressedSize / uncompressedSize;
            assertEquals(computedCompressionRate, builder.compressionRatio(), 0.00001);
        }
    }

    @Test
    public void testEstimatedSizeInBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.position(bufferOffset);

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());

        int previousEstimate = 0;
        for (int i = 0; i < 10; i++) {
            builder.append(new SimpleRecord(i, ("" + i).getBytes()));
            int currentEstimate = builder.estimatedSizeInBytes();
            assertTrue(currentEstimate > previousEstimate);
            previousEstimate = currentEstimate;
        }

        int bytesWrittenBeforeClose = builder.estimatedSizeInBytes();
        MemoryRecords records = builder.build();
        assertEquals(records.sizeInBytes(), builder.estimatedSizeInBytes());
        if (compressionType == CompressionType.NONE)
            assertEquals(records.sizeInBytes(), bytesWrittenBeforeClose);
    }

    @Test
    public void testCompressionRateV1() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.position(bufferOffset);

        LegacyRecord[] records = new LegacyRecord[] {
                LegacyRecord.create(RecordBatch.MAGIC_VALUE_V1, 0L, "a".getBytes(), "1".getBytes()),
                LegacyRecord.create(RecordBatch.MAGIC_VALUE_V1, 1L, "b".getBytes(), "2".getBytes()),
                LegacyRecord.create(RecordBatch.MAGIC_VALUE_V1, 2L, "c".getBytes(), "3".getBytes()),
        };

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.MAGIC_VALUE_V1, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());

        int uncompressedSize = 0;
        for (LegacyRecord record : records) {
            uncompressedSize += record.sizeInBytes() + Records.LOG_OVERHEAD;
            builder.append(record);
        }

        MemoryRecords built = builder.build();
        if (compressionType == CompressionType.NONE) {
            assertEquals(1.0, builder.compressionRatio(), 0.00001);
        } else {
            int compressedSize = built.sizeInBytes() - Records.LOG_OVERHEAD - LegacyRecord.RECORD_OVERHEAD_V1;
            double computedCompressionRate = (double) compressedSize / uncompressedSize;
            assertEquals(computedCompressionRate, builder.compressionRatio(), 0.00001);
        }
    }

    @Test
    public void buildUsingLogAppendTime() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.position(bufferOffset);

        long logAppendTime = System.currentTimeMillis();
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.MAGIC_VALUE_V1, compressionType,
                TimestampType.LOG_APPEND_TIME, 0L, logAppendTime, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE, false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.append(0L, "a".getBytes(), "1".getBytes());
        builder.append(0L, "b".getBytes(), "2".getBytes());
        builder.append(0L, "c".getBytes(), "3".getBytes());
        MemoryRecords records = builder.build();

        MemoryRecordsBuilder.RecordsInfo info = builder.info();
        assertEquals(logAppendTime, info.maxTimestamp);

        if (compressionType != CompressionType.NONE)
            assertEquals(2L, info.shallowOffsetOfMaxTimestamp);
        else
            assertEquals(0L, info.shallowOffsetOfMaxTimestamp);

        for (RecordBatch batch : records.batches()) {
            assertEquals(TimestampType.LOG_APPEND_TIME, batch.timestampType());
            for (Record record : batch)
                assertEquals(logAppendTime, record.timestamp());
        }
    }

    @Test
    public void buildUsingCreateTime() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.position(bufferOffset);

        long logAppendTime = System.currentTimeMillis();
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.MAGIC_VALUE_V1, compressionType,
                TimestampType.CREATE_TIME, 0L, logAppendTime, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.append(0L, "a".getBytes(), "1".getBytes());
        builder.append(2L, "b".getBytes(), "2".getBytes());
        builder.append(1L, "c".getBytes(), "3".getBytes());
        MemoryRecords records = builder.build();

        MemoryRecordsBuilder.RecordsInfo info = builder.info();
        assertEquals(2L, info.maxTimestamp);

        if (compressionType == CompressionType.NONE)
            assertEquals(1L, info.shallowOffsetOfMaxTimestamp);
        else
            assertEquals(2L, info.shallowOffsetOfMaxTimestamp);

        int i = 0;
        long[] expectedTimestamps = new long[] {0L, 2L, 1L};
        for (RecordBatch batch : records.batches()) {
            assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
            for (Record record : batch)
                assertEquals(expectedTimestamps[i++], record.timestamp());
        }
    }

    @Test
    public void testAppendedChecksumConsistency() {
        ByteBuffer buffer = ByteBuffer.allocate(512);
        for (byte magic : Arrays.asList(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1, RecordBatch.MAGIC_VALUE_V2)) {
            MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, magic, compressionType,
                    TimestampType.CREATE_TIME, 0L, LegacyRecord.NO_TIMESTAMP, RecordBatch.NO_PRODUCER_ID,
                    RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, false,
                    RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
            Long checksumOrNull = builder.append(1L, "key".getBytes(), "value".getBytes());
            MemoryRecords memoryRecords = builder.build();
            List<Record> records = TestUtils.toList(memoryRecords.records());
            assertEquals(1, records.size());
            assertEquals(checksumOrNull, records.get(0).checksumOrNull());
        }
    }

    @Test
    public void testSmallWriteLimit() {
        // with a small write limit, we always allow at least one record to be added

        byte[] key = "foo".getBytes();
        byte[] value = "bar".getBytes();
        int writeLimit = 0;
        ByteBuffer buffer = ByteBuffer.allocate(512);
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compressionType,
                TimestampType.CREATE_TIME, 0L, LegacyRecord.NO_TIMESTAMP, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE, false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, writeLimit);

        assertFalse(builder.isFull());
        assertTrue(builder.hasRoomFor(0L, key, value, Record.EMPTY_HEADERS));
        builder.append(0L, key, value);

        assertTrue(builder.isFull());
        assertFalse(builder.hasRoomFor(0L, key, value, Record.EMPTY_HEADERS));

        MemoryRecords memRecords = builder.build();
        List<Record> records = TestUtils.toList(memRecords.records());
        assertEquals(1, records.size());

        Record record = records.get(0);
        assertEquals(ByteBuffer.wrap(key), record.key());
        assertEquals(ByteBuffer.wrap(value), record.value());
    }

    @Test
    public void writePastLimit() {
        ByteBuffer buffer = ByteBuffer.allocate(64);
        buffer.position(bufferOffset);

        long logAppendTime = System.currentTimeMillis();
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.MAGIC_VALUE_V1, compressionType,
                TimestampType.CREATE_TIME, 0L, logAppendTime, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.setEstimatedCompressionRatio(0.5f);
        builder.append(0L, "a".getBytes(), "1".getBytes());
        builder.append(1L, "b".getBytes(), "2".getBytes());

        assertFalse(builder.hasRoomFor(2L, "c".getBytes(), "3".getBytes(), Record.EMPTY_HEADERS));
        builder.append(2L, "c".getBytes(), "3".getBytes());
        MemoryRecords records = builder.build();

        MemoryRecordsBuilder.RecordsInfo info = builder.info();
        assertEquals(2L, info.maxTimestamp);
        assertEquals(2L, info.shallowOffsetOfMaxTimestamp);

        long i = 0L;
        for (RecordBatch batch : records.batches()) {
            assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
            for (Record record : batch)
                assertEquals(i++, record.timestamp());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAppendAtInvalidOffset() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.position(bufferOffset);

        long logAppendTime = System.currentTimeMillis();
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.MAGIC_VALUE_V1, compressionType,
                TimestampType.CREATE_TIME, 0L, logAppendTime, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());

        builder.appendWithOffset(0L, System.currentTimeMillis(), "a".getBytes(), null);

        // offsets must increase monotonically
        builder.appendWithOffset(0L, System.currentTimeMillis(), "b".getBytes(), null);
    }

    @Test
    public void convertV2ToV1UsingMixedCreateAndLogAppendTime() {
        ByteBuffer buffer = ByteBuffer.allocate(512);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2,
                compressionType, TimestampType.LOG_APPEND_TIME, 0L);
        builder.append(10L, "1".getBytes(), "a".getBytes());
        builder.close();

        int sizeExcludingTxnMarkers = buffer.position();

        MemoryRecords.writeEndTransactionalMarker(buffer, 1L, System.currentTimeMillis(), 0, 15L, (short) 0,
                new EndTransactionMarker(ControlRecordType.ABORT, 0));

        int position = buffer.position();

        builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, compressionType,
                TimestampType.CREATE_TIME, 1L);
        builder.append(12L, "2".getBytes(), "b".getBytes());
        builder.append(13L, "3".getBytes(), "c".getBytes());
        builder.close();

        sizeExcludingTxnMarkers += buffer.position() - position;

        MemoryRecords.writeEndTransactionalMarker(buffer, 14L, System.currentTimeMillis(), 0, 1L, (short) 0,
                new EndTransactionMarker(ControlRecordType.COMMIT, 0));

        buffer.flip();

        ConvertedRecords<MemoryRecords> convertedRecords = MemoryRecords.readableRecords(buffer)
                .downConvert(RecordBatch.MAGIC_VALUE_V1, 0, time);
        MemoryRecords records = convertedRecords.records();

        // Transactional markers are skipped when down converting to V1, so exclude them from size
        verifyRecordsProcessingStats(convertedRecords.recordsProcessingStats(),
                3, 3, records.sizeInBytes(), sizeExcludingTxnMarkers);

        List<? extends RecordBatch> batches = Utils.toList(records.batches().iterator());
        if (compressionType != CompressionType.NONE) {
            assertEquals(2, batches.size());
            assertEquals(TimestampType.LOG_APPEND_TIME, batches.get(0).timestampType());
            assertEquals(TimestampType.CREATE_TIME, batches.get(1).timestampType());
        } else {
            assertEquals(3, batches.size());
            assertEquals(TimestampType.LOG_APPEND_TIME, batches.get(0).timestampType());
            assertEquals(TimestampType.CREATE_TIME, batches.get(1).timestampType());
            assertEquals(TimestampType.CREATE_TIME, batches.get(2).timestampType());
        }

        List<Record> logRecords = Utils.toList(records.records().iterator());
        assertEquals(3, logRecords.size());
        assertEquals(ByteBuffer.wrap("1".getBytes()), logRecords.get(0).key());
        assertEquals(ByteBuffer.wrap("2".getBytes()), logRecords.get(1).key());
        assertEquals(ByteBuffer.wrap("3".getBytes()), logRecords.get(2).key());
    }

    @Test
    public void convertToV1WithMixedV0AndV2Data() {
        ByteBuffer buffer = ByteBuffer.allocate(512);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V0,
                compressionType, TimestampType.NO_TIMESTAMP_TYPE, 0L);
        builder.append(RecordBatch.NO_TIMESTAMP, "1".getBytes(), "a".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, compressionType,
                TimestampType.CREATE_TIME, 1L);
        builder.append(11L, "2".getBytes(), "b".getBytes());
        builder.append(12L, "3".getBytes(), "c".getBytes());
        builder.close();

        buffer.flip();

        ConvertedRecords<MemoryRecords> convertedRecords = MemoryRecords.readableRecords(buffer)
                .downConvert(RecordBatch.MAGIC_VALUE_V1, 0, time);
        MemoryRecords records = convertedRecords.records();
        verifyRecordsProcessingStats(convertedRecords.recordsProcessingStats(), 3, 2,
                records.sizeInBytes(), buffer.limit());

        List<? extends RecordBatch> batches = Utils.toList(records.batches().iterator());
        if (compressionType != CompressionType.NONE) {
            assertEquals(2, batches.size());
            assertEquals(RecordBatch.MAGIC_VALUE_V0, batches.get(0).magic());
            assertEquals(0, batches.get(0).baseOffset());
            assertEquals(RecordBatch.MAGIC_VALUE_V1, batches.get(1).magic());
            assertEquals(1, batches.get(1).baseOffset());
        } else {
            assertEquals(3, batches.size());
            assertEquals(RecordBatch.MAGIC_VALUE_V0, batches.get(0).magic());
            assertEquals(0, batches.get(0).baseOffset());
            assertEquals(RecordBatch.MAGIC_VALUE_V1, batches.get(1).magic());
            assertEquals(1, batches.get(1).baseOffset());
            assertEquals(RecordBatch.MAGIC_VALUE_V1, batches.get(2).magic());
            assertEquals(2, batches.get(2).baseOffset());
        }

        List<Record> logRecords = Utils.toList(records.records().iterator());
        assertEquals("1", utf8(logRecords.get(0).key()));
        assertEquals("2", utf8(logRecords.get(1).key()));
        assertEquals("3", utf8(logRecords.get(2).key()));

        convertedRecords = MemoryRecords.readableRecords(buffer).downConvert(RecordBatch.MAGIC_VALUE_V1, 2L, time);
        records = convertedRecords.records();

        batches = Utils.toList(records.batches().iterator());
        logRecords = Utils.toList(records.records().iterator());

        if (compressionType != CompressionType.NONE) {
            assertEquals(2, batches.size());
            assertEquals(RecordBatch.MAGIC_VALUE_V0, batches.get(0).magic());
            assertEquals(0, batches.get(0).baseOffset());
            assertEquals(RecordBatch.MAGIC_VALUE_V1, batches.get(1).magic());
            assertEquals(1, batches.get(1).baseOffset());
            assertEquals("1", utf8(logRecords.get(0).key()));
            assertEquals("2", utf8(logRecords.get(1).key()));
            assertEquals("3", utf8(logRecords.get(2).key()));
            verifyRecordsProcessingStats(convertedRecords.recordsProcessingStats(), 3, 2,
                    records.sizeInBytes(), buffer.limit());
        } else {
            assertEquals(2, batches.size());
            assertEquals(RecordBatch.MAGIC_VALUE_V0, batches.get(0).magic());
            assertEquals(0, batches.get(0).baseOffset());
            assertEquals(RecordBatch.MAGIC_VALUE_V1, batches.get(1).magic());
            assertEquals(2, batches.get(1).baseOffset());
            assertEquals("1", utf8(logRecords.get(0).key()));
            assertEquals("3", utf8(logRecords.get(1).key()));
            verifyRecordsProcessingStats(convertedRecords.recordsProcessingStats(), 3, 1,
                    records.sizeInBytes(), buffer.limit());
        }
    }

    private String utf8(ByteBuffer buffer) {
        return Utils.utf8(buffer, buffer.remaining());
    }

    @Test
    public void shouldThrowIllegalStateExceptionOnBuildWhenAborted() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.position(bufferOffset);

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.MAGIC_VALUE_V0, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE, false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.abort();
        try {
            builder.build();
            fail("Should have thrown KafkaException");
        } catch (IllegalStateException e) {
            // ok
        }
    }

    @Test
    public void shouldResetBufferToInitialPositionOnAbort() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.position(bufferOffset);

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.MAGIC_VALUE_V0, compressionType,
                                                                TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                                                                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.append(0L, "a".getBytes(), "1".getBytes());
        builder.abort();
        assertEquals(bufferOffset, builder.buffer().position());
    }

    @Test
    public void shouldThrowIllegalStateExceptionOnCloseWhenAborted() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.position(bufferOffset);

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.MAGIC_VALUE_V0, compressionType,
                                                                TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                                                                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.abort();
        try {
            builder.close();
            fail("Should have thrown IllegalStateException");
        } catch (IllegalStateException e) {
            // ok
        }
    }

    @Test
    public void shouldThrowIllegalStateExceptionOnAppendWhenAborted() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.position(bufferOffset);

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.MAGIC_VALUE_V0, compressionType,
                                                                TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                                                                false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.abort();
        try {
            builder.append(0L, "a".getBytes(), "1".getBytes());
            fail("Should have thrown IllegalStateException");
        } catch (IllegalStateException e) {
            // ok
        }
    }

    @Parameterized.Parameters(name = "bufferOffset={0}, compression={1}")
    public static Collection<Object[]> data() {
        List<Object[]> values = new ArrayList<>();
        for (int bufferOffset : Arrays.asList(0, 15))
            for (CompressionType compressionType : CompressionType.values())
                values.add(new Object[] {bufferOffset, compressionType});
        return values;
    }

    private void verifyRecordsProcessingStats(RecordsProcessingStats processingStats, int numRecords,
            int numRecordsConverted, long finalBytes, long preConvertedBytes) {
        assertNotNull("Records processing info is null", processingStats);
        assertEquals(numRecordsConverted, processingStats.numRecordsConverted());
        // Since nanoTime accuracy on build machines may not be sufficient to measure small conversion times,
        // only check if the value >= 0. Default is -1, so this checks if time has been recorded.
        assertTrue("Processing time not recorded: " + processingStats, processingStats.conversionTimeNanos() >= 0);
        long tempBytes = processingStats.temporaryMemoryBytes();
        if (compressionType == CompressionType.NONE) {
            if (numRecordsConverted == 0)
                assertEquals(finalBytes, tempBytes);
            else if (numRecordsConverted == numRecords)
                assertEquals(preConvertedBytes + finalBytes, tempBytes);
            else {
                assertTrue(String.format("Unexpected temp bytes %d final %d pre %d", tempBytes, finalBytes, preConvertedBytes),
                        tempBytes > finalBytes && tempBytes < finalBytes + preConvertedBytes);
            }
        } else {
            long compressedBytes = finalBytes - Records.LOG_OVERHEAD - LegacyRecord.RECORD_OVERHEAD_V0;
            assertTrue(String.format("Uncompressed size expected temp=%d, compressed=%d", tempBytes, compressedBytes),
                    tempBytes > compressedBytes);
        }
    }

}
