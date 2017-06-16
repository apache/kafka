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

import org.apache.kafka.common.record.AbstractLegacyRecordBatch.ByteBufferLegacyRecordBatch;
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AbstractLegacyRecordBatchTest {

    @Test
    public void testSetLastOffsetCompressed() {
        SimpleRecord[] simpleRecords = new SimpleRecord[] {
            new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
            new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
            new SimpleRecord(3L, "c".getBytes(), "3".getBytes())
        };

        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, 0L,
                CompressionType.GZIP, TimestampType.CREATE_TIME, simpleRecords);

        long lastOffset = 500L;
        long firstOffset = lastOffset - simpleRecords.length + 1;

        ByteBufferLegacyRecordBatch batch = new ByteBufferLegacyRecordBatch(records.buffer());
        batch.setLastOffset(lastOffset);
        assertEquals(lastOffset, batch.lastOffset());
        assertEquals(firstOffset, batch.baseOffset());
        assertTrue(batch.isValid());

        List<MutableRecordBatch> recordBatches = Utils.toList(records.batches().iterator());
        assertEquals(1, recordBatches.size());
        assertEquals(lastOffset, recordBatches.get(0).lastOffset());

        long offset = firstOffset;
        for (Record record : records.records())
            assertEquals(offset++, record.offset());
    }

    /**
     * The wrapper offset should be 0 in v0, but not in v1. However, the latter worked by accident and some versions of
     * librdkafka now depend on it. So we support 0 for compatibility reasons, but the recommendation is to set the
     * wrapper offset to the relative offset of the last record in the batch.
     */
    @Test
    public void testIterateCompressedRecordWithWrapperOffsetZero() {
        for (byte magic : Arrays.asList(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1)) {
            SimpleRecord[] simpleRecords = new SimpleRecord[] {
                new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
                new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(3L, "c".getBytes(), "3".getBytes())
            };

            MemoryRecords records = MemoryRecords.withRecords(magic, 0L,
                    CompressionType.GZIP, TimestampType.CREATE_TIME, simpleRecords);

            ByteBufferLegacyRecordBatch batch = new ByteBufferLegacyRecordBatch(records.buffer());
            batch.setLastOffset(0L);

            long offset = 0L;
            for (Record record : batch)
                assertEquals(offset++, record.offset());
        }
    }

    @Test(expected = InvalidRecordException.class)
    public void testInvalidWrapperOffsetV1() {
        SimpleRecord[] simpleRecords = new SimpleRecord[] {
            new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
            new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
            new SimpleRecord(3L, "c".getBytes(), "3".getBytes())
        };

        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, 0L,
                CompressionType.GZIP, TimestampType.CREATE_TIME, simpleRecords);

        ByteBufferLegacyRecordBatch batch = new ByteBufferLegacyRecordBatch(records.buffer());
        batch.setLastOffset(1L);

        batch.iterator();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetNoTimestampTypeNotAllowed() {
        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, 0L,
                CompressionType.GZIP, TimestampType.CREATE_TIME,
                new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
                new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(3L, "c".getBytes(), "3".getBytes()));
        ByteBufferLegacyRecordBatch batch = new ByteBufferLegacyRecordBatch(records.buffer());
        batch.setMaxTimestamp(TimestampType.NO_TIMESTAMP_TYPE, RecordBatch.NO_TIMESTAMP);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetLogAppendTimeNotAllowedV0() {
        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V0, 0L,
                CompressionType.GZIP, TimestampType.CREATE_TIME,
                new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
                new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(3L, "c".getBytes(), "3".getBytes()));
        long logAppendTime = 15L;
        ByteBufferLegacyRecordBatch batch = new ByteBufferLegacyRecordBatch(records.buffer());
        batch.setMaxTimestamp(TimestampType.LOG_APPEND_TIME, logAppendTime);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetCreateTimeNotAllowedV0() {
        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V0, 0L,
                CompressionType.GZIP, TimestampType.CREATE_TIME,
                new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
                new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(3L, "c".getBytes(), "3".getBytes()));
        long createTime = 15L;
        ByteBufferLegacyRecordBatch batch = new ByteBufferLegacyRecordBatch(records.buffer());
        batch.setMaxTimestamp(TimestampType.CREATE_TIME, createTime);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetPartitionLeaderEpochNotAllowedV0() {
        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V0, 0L,
                CompressionType.GZIP, TimestampType.CREATE_TIME,
                new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
                new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(3L, "c".getBytes(), "3".getBytes()));
        ByteBufferLegacyRecordBatch batch = new ByteBufferLegacyRecordBatch(records.buffer());
        batch.setPartitionLeaderEpoch(15);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetPartitionLeaderEpochNotAllowedV1() {
        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, 0L,
                CompressionType.GZIP, TimestampType.CREATE_TIME,
                new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
                new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(3L, "c".getBytes(), "3".getBytes()));
        ByteBufferLegacyRecordBatch batch = new ByteBufferLegacyRecordBatch(records.buffer());
        batch.setPartitionLeaderEpoch(15);
    }

    @Test
    public void testSetLogAppendTimeV1() {
        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, 0L,
                CompressionType.GZIP, TimestampType.CREATE_TIME,
                new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
                new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(3L, "c".getBytes(), "3".getBytes()));

        long logAppendTime = 15L;

        ByteBufferLegacyRecordBatch batch = new ByteBufferLegacyRecordBatch(records.buffer());
        batch.setMaxTimestamp(TimestampType.LOG_APPEND_TIME, logAppendTime);
        assertEquals(TimestampType.LOG_APPEND_TIME, batch.timestampType());
        assertEquals(logAppendTime, batch.maxTimestamp());
        assertTrue(batch.isValid());

        List<MutableRecordBatch> recordBatches = Utils.toList(records.batches().iterator());
        assertEquals(1, recordBatches.size());
        assertEquals(TimestampType.LOG_APPEND_TIME, recordBatches.get(0).timestampType());
        assertEquals(logAppendTime, recordBatches.get(0).maxTimestamp());

        for (Record record : records.records())
            assertEquals(logAppendTime, record.timestamp());
    }

    @Test
    public void testSetCreateTimeV1() {
        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, 0L,
                CompressionType.GZIP, TimestampType.CREATE_TIME,
                new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
                new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(3L, "c".getBytes(), "3".getBytes()));

        long createTime = 15L;

        ByteBufferLegacyRecordBatch batch = new ByteBufferLegacyRecordBatch(records.buffer());
        batch.setMaxTimestamp(TimestampType.CREATE_TIME, createTime);
        assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
        assertEquals(createTime, batch.maxTimestamp());
        assertTrue(batch.isValid());

        List<MutableRecordBatch> recordBatches = Utils.toList(records.batches().iterator());
        assertEquals(1, recordBatches.size());
        assertEquals(TimestampType.CREATE_TIME, recordBatches.get(0).timestampType());
        assertEquals(createTime, recordBatches.get(0).maxTimestamp());

        long expectedTimestamp = 1L;
        for (Record record : records.records())
            assertEquals(expectedTimestamp++, record.timestamp());
    }

}
