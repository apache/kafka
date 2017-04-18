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
import static org.junit.Assert.assertTrue;

@RunWith(value = Parameterized.class)
public class MemoryRecordsBuilderTest {

    private final CompressionType compressionType;
    private final int bufferOffset;

    public MemoryRecordsBuilderTest(int bufferOffset, CompressionType compressionType) {
        this.bufferOffset = bufferOffset;
        this.compressionType = compressionType;
    }

    @Test
    public void testWriteEmptyRecordSet() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.position(bufferOffset);

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.MAGIC_VALUE_V0, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
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
                TimestampType.CREATE_TIME, 0L, 0L, pid, epoch, sequence, true, RecordBatch.NO_PARTITION_LEADER_EPOCH,
                buffer.capacity());
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
                0L, 0L, pid, epoch, sequence, true, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteTransactionalNotAllowedMagicV1() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.position(bufferOffset);

        long pid = 9809;
        short epoch = 15;
        int sequence = 2342;

        new MemoryRecordsBuilder(buffer, RecordBatch.MAGIC_VALUE_V1, compressionType, TimestampType.CREATE_TIME,
                0L, 0L, pid, epoch, sequence, true, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteTransactionalWithInvalidPID() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.position(bufferOffset);

        long pid = RecordBatch.NO_PRODUCER_ID;
        short epoch = 15;
        int sequence = 2342;

        new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compressionType, TimestampType.CREATE_TIME,
                0L, 0L, pid, epoch, sequence, true, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteIdempotentWithInvalidEpoch() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.position(bufferOffset);

        long pid = 9809;
        short epoch = RecordBatch.NO_PRODUCER_EPOCH;
        int sequence = 2342;

        new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compressionType, TimestampType.CREATE_TIME,
                0L, 0L, pid, epoch, sequence, true, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteIdempotentWithInvalidBaseSequence() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.position(bufferOffset);

        long pid = 9809;
        short epoch = 15;
        int sequence = RecordBatch.NO_SEQUENCE;

        new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compressionType, TimestampType.CREATE_TIME,
                0L, 0L, pid, epoch, sequence, true, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
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
                false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());

        int uncompressedSize = 0;
        for (LegacyRecord record : records) {
            uncompressedSize += record.sizeInBytes() + Records.LOG_OVERHEAD;
            builder.append(record);
        }

        MemoryRecords built = builder.build();
        if (compressionType == CompressionType.NONE) {
            assertEquals(1.0, builder.compressionRate(), 0.00001);
        } else {
            int compressedSize = built.sizeInBytes() - Records.LOG_OVERHEAD - LegacyRecord.RECORD_OVERHEAD_V0;
            double computedCompressionRate = (double) compressedSize / uncompressedSize;
            assertEquals(computedCompressionRate, builder.compressionRate(), 0.00001);
        }
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
                false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());

        int uncompressedSize = 0;
        for (LegacyRecord record : records) {
            uncompressedSize += record.sizeInBytes() + Records.LOG_OVERHEAD;
            builder.append(record);
        }

        MemoryRecords built = builder.build();
        if (compressionType == CompressionType.NONE) {
            assertEquals(1.0, builder.compressionRate(), 0.00001);
        } else {
            int compressedSize = built.sizeInBytes() - Records.LOG_OVERHEAD - LegacyRecord.RECORD_OVERHEAD_V1;
            double computedCompressionRate = (double) compressedSize / uncompressedSize;
            assertEquals(computedCompressionRate, builder.compressionRate(), 0.00001);
        }
    }

    @Test
    public void buildUsingLogAppendTime() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.position(bufferOffset);

        long logAppendTime = System.currentTimeMillis();
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.MAGIC_VALUE_V1, compressionType,
                TimestampType.LOG_APPEND_TIME, 0L, logAppendTime, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
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
                false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
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
    public void testSmallWriteLimit() {
        // with a small write limit, we always allow at least one record to be added

        byte[] key = "foo".getBytes();
        byte[] value = "bar".getBytes();
        int writeLimit = 0;
        ByteBuffer buffer = ByteBuffer.allocate(512);
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compressionType,
                TimestampType.CREATE_TIME, 0L, LegacyRecord.NO_TIMESTAMP, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, writeLimit);

        assertFalse(builder.isFull());
        assertTrue(builder.hasRoomFor(0L, key, value));
        builder.append(0L, key, value);

        assertTrue(builder.isFull());
        assertFalse(builder.hasRoomFor(0L, key, value));

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
                false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());
        builder.append(0L, "a".getBytes(), "1".getBytes());
        builder.append(1L, "b".getBytes(), "2".getBytes());

        assertFalse(builder.hasRoomFor(2L, "c".getBytes(), "3".getBytes()));
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
                false, RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.capacity());

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

        builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, compressionType,
                TimestampType.CREATE_TIME, 1L);
        builder.append(11L, "2".getBytes(), "b".getBytes());
        builder.appendControlRecord(12L, ControlRecordType.COMMIT, null);
        builder.append(13L, "3".getBytes(), "c".getBytes());
        builder.close();

        buffer.flip();

        Records records = MemoryRecords.readableRecords(buffer).downConvert(RecordBatch.MAGIC_VALUE_V1);

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

        Records records = MemoryRecords.readableRecords(buffer).downConvert(RecordBatch.MAGIC_VALUE_V1);

        List<? extends RecordBatch> batches = Utils.toList(records.batches().iterator());
        if (compressionType != CompressionType.NONE) {
            assertEquals(2, batches.size());
            assertEquals(RecordBatch.MAGIC_VALUE_V0, batches.get(0).magic());
            assertEquals(RecordBatch.MAGIC_VALUE_V1, batches.get(1).magic());
        } else {
            assertEquals(3, batches.size());
            assertEquals(RecordBatch.MAGIC_VALUE_V0, batches.get(0).magic());
            assertEquals(RecordBatch.MAGIC_VALUE_V1, batches.get(1).magic());
            assertEquals(RecordBatch.MAGIC_VALUE_V1, batches.get(2).magic());
        }

        List<Record> logRecords = Utils.toList(records.records().iterator());
        assertEquals(ByteBuffer.wrap("1".getBytes()), logRecords.get(0).key());
        assertEquals(ByteBuffer.wrap("2".getBytes()), logRecords.get(1).key());
        assertEquals(ByteBuffer.wrap("3".getBytes()), logRecords.get(2).key());
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        List<Object[]> values = new ArrayList<>();
        for (int bufferOffset : Arrays.asList(0, 15))
            for (CompressionType compressionType : CompressionType.values())
                values.add(new Object[] {bufferOffset, compressionType});
        return values;
    }

}
