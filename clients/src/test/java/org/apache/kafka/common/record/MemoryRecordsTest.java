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

import org.apache.kafka.test.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.wrapNullable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(value = Parameterized.class)
public class MemoryRecordsTest {

    private CompressionType compression;
    private byte magic;
    private long firstOffset;
    private long pid;
    private short epoch;
    private int firstSequence;
    private long logAppendTime = System.currentTimeMillis();
    private int partitionLeaderEpoch = 998;

    public MemoryRecordsTest(byte magic, long firstOffset, CompressionType compression) {
        this.magic = magic;
        this.compression = compression;
        this.firstOffset = firstOffset;
        if (magic >= RecordBatch.MAGIC_VALUE_V2) {
            pid = 134234L;
            epoch = 28;
            firstSequence = 777;
        } else {
            pid = RecordBatch.NO_PID;
            epoch = RecordBatch.NO_EPOCH;
            firstSequence = RecordBatch.NO_SEQUENCE;
        }
    }

    @Test
    public void testIterator() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, magic, compression,
                TimestampType.CREATE_TIME, firstOffset, logAppendTime, pid, epoch, firstSequence, false,
                partitionLeaderEpoch, buffer.limit());

        byte[][] keys = new byte[][] {"a".getBytes(), "b".getBytes(), "c".getBytes(), null, "d".getBytes(), null};
        byte[][] values = new byte[][] {"1".getBytes(), "2".getBytes(), "3".getBytes(), "4".getBytes(), null, null};
        long[] timestamps = new long[] {1, 2, 3, 4, 5, 6};

        for (int i = 0; i < keys.length; i++)
            builder.append(timestamps[i], keys[i], values[i]);


        MemoryRecords memoryRecords = builder.build();
        for (int iteration = 0; iteration < 2; iteration++) {
            int total = 0;
            for (RecordBatch batch : memoryRecords.batches()) {
                assertTrue(batch.isValid());
                assertEquals(compression, batch.compressionType());
                assertEquals(firstOffset + total, batch.baseOffset());

                if (magic >= RecordBatch.MAGIC_VALUE_V2) {
                    assertEquals(pid, batch.pid());
                    assertEquals(epoch, batch.epoch());
                    assertEquals(firstSequence + total, batch.baseSequence());
                    assertEquals(partitionLeaderEpoch, batch.partitionLeaderEpoch());
                } else {
                    assertEquals(RecordBatch.NO_PID, batch.pid());
                    assertEquals(RecordBatch.NO_EPOCH, batch.epoch());
                    assertEquals(RecordBatch.NO_SEQUENCE, batch.baseSequence());
                    assertEquals(RecordBatch.UNKNOWN_PARTITION_LEADER_EPOCH, batch.partitionLeaderEpoch());
                }

                int records = 0;
                for (Record record : batch) {
                    assertTrue(record.isValid());
                    assertTrue(record.hasMagic(batch.magic()));
                    assertFalse(record.isCompressed());
                    assertEquals(firstOffset + total, record.offset());
                    assertEquals(wrapNullable(keys[total]), record.key());
                    assertEquals(wrapNullable(values[total]), record.value());

                    if (magic >= RecordBatch.MAGIC_VALUE_V2)
                        assertEquals(firstSequence + total, record.sequence());

                    if (magic > RecordBatch.MAGIC_VALUE_V0) {
                        assertEquals(timestamps[total], record.timestamp());
                        if (magic < RecordBatch.MAGIC_VALUE_V2)
                            assertTrue(record.hasTimestampType(batch.timestampType()));
                    }

                    total++;
                    records++;
                }

                assertEquals(batch.baseOffset() + records - 1, batch.lastOffset());
            }
        }
    }

    @Test
    public void testHasRoomForMethod() {
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), magic, compression,
                TimestampType.CREATE_TIME, 0L);
        builder.append(0L, "a".getBytes(), "1".getBytes());
        assertTrue(builder.hasRoomFor(1L, "b".getBytes(), "2".getBytes()));
        builder.close();
        assertFalse(builder.hasRoomFor(1L, "b".getBytes(), "2".getBytes()));
    }

    @Test
    public void testFilterTo() {
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic, compression, TimestampType.CREATE_TIME, 0L);
        builder.append(10L, null, "a".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, magic, compression, TimestampType.CREATE_TIME, 1L);
        builder.append(11L, "1".getBytes(), "b".getBytes());
        builder.append(12L, null, "c".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, magic, compression, TimestampType.CREATE_TIME, 3L);
        builder.append(13L, null, "d".getBytes());
        builder.append(20L, "4".getBytes(), "e".getBytes());
        builder.append(15L, "5".getBytes(), "f".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, magic, compression, TimestampType.CREATE_TIME, 6L);
        builder.append(16L, "6".getBytes(), "g".getBytes());
        builder.close();

        buffer.flip();

        ByteBuffer filtered = ByteBuffer.allocate(2048);
        MemoryRecords.FilterResult result = MemoryRecords.readableRecords(buffer).filterTo(new RetainNonNullKeysFilter(), filtered);

        filtered.flip();

        assertEquals(7, result.messagesRead);
        assertEquals(4, result.messagesRetained);
        assertEquals(buffer.limit(), result.bytesRead);
        assertEquals(filtered.limit(), result.bytesRetained);
        if (magic > 0) {
            assertEquals(20L, result.maxTimestamp);
            if (compression == CompressionType.NONE)
                assertEquals(4L, result.shallowOffsetOfMaxTimestamp);
            else
                assertEquals(5L, result.shallowOffsetOfMaxTimestamp);
        }

        MemoryRecords filteredRecords = MemoryRecords.readableRecords(filtered);

        List<RecordBatch.MutableRecordBatch> shallowBatches = TestUtils.toList(filteredRecords.batches());
        final List<Long> expectedEndOffsets;
        final List<Long> expectedStartOffsets;

        if (magic < RecordBatch.MAGIC_VALUE_V2 && compression == CompressionType.NONE) {
            expectedEndOffsets = asList(1L, 4L, 5L, 6L);
            expectedStartOffsets = asList(1L, 4L, 5L, 6L);
        } else if (magic < RecordBatch.MAGIC_VALUE_V2) {
            expectedEndOffsets = asList(1L, 5L, 6L);
            expectedStartOffsets = asList(1L, 4L, 6L);
        } else {
            expectedEndOffsets = asList(1L, 5L, 6L);
            expectedStartOffsets = asList(1L, 3L, 6L);
        }

        assertEquals(expectedEndOffsets.size(), shallowBatches.size());

        for (int i = 0; i < expectedEndOffsets.size(); i++) {
            RecordBatch shallowEntry = shallowBatches.get(i);
            assertEquals(expectedStartOffsets.get(i).longValue(), shallowEntry.baseOffset());
            assertEquals(expectedEndOffsets.get(i).longValue(), shallowEntry.lastOffset());
            assertEquals(magic, shallowEntry.magic());
            assertEquals(compression, shallowEntry.compressionType());
            assertEquals(magic == RecordBatch.MAGIC_VALUE_V0 ? TimestampType.NO_TIMESTAMP_TYPE : TimestampType.CREATE_TIME,
                    shallowEntry.timestampType());
        }

        List<Record> records = TestUtils.toList(filteredRecords.records());
        assertEquals(4, records.size());

        Record first = records.get(0);
        assertEquals(1L, first.offset());
        if (magic > RecordBatch.MAGIC_VALUE_V0)
            assertEquals(11L, first.timestamp());
        assertEquals(ByteBuffer.wrap("1".getBytes()), first.key());
        assertEquals(ByteBuffer.wrap("b".getBytes()), first.value());

        Record second = records.get(1);
        assertEquals(4L, second.offset());
        if (magic > RecordBatch.MAGIC_VALUE_V0)
            assertEquals(20L, second.timestamp());
        assertEquals(ByteBuffer.wrap("4".getBytes()), second.key());
        assertEquals(ByteBuffer.wrap("e".getBytes()), second.value());

        Record third = records.get(2);
        assertEquals(5L, third.offset());
        if (magic > RecordBatch.MAGIC_VALUE_V0)
            assertEquals(15L, third.timestamp());
        assertEquals(ByteBuffer.wrap("5".getBytes()), third.key());
        assertEquals(ByteBuffer.wrap("f".getBytes()), third.value());

        Record fourth = records.get(3);
        assertEquals(6L, fourth.offset());
        if (magic > RecordBatch.MAGIC_VALUE_V0)
            assertEquals(16L, fourth.timestamp());
        assertEquals(ByteBuffer.wrap("6".getBytes()), fourth.key());
        assertEquals(ByteBuffer.wrap("g".getBytes()), fourth.value());
    }

    @Test
    public void testFilterToPreservesLogAppendTime() {
        long logAppendTime = System.currentTimeMillis();

        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic, compression,
                TimestampType.LOG_APPEND_TIME, 0L, logAppendTime, pid, epoch, firstSequence);
        builder.append(10L, null, "a".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, magic, compression, TimestampType.LOG_APPEND_TIME, 1L, logAppendTime,
                pid, epoch, firstSequence);
        builder.append(11L, "1".getBytes(), "b".getBytes());
        builder.append(12L, null, "c".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, magic, compression, TimestampType.LOG_APPEND_TIME, 3L, logAppendTime,
                pid, epoch, firstSequence);
        builder.append(13L, null, "d".getBytes());
        builder.append(14L, "4".getBytes(), "e".getBytes());
        builder.append(15L, "5".getBytes(), "f".getBytes());
        builder.close();

        buffer.flip();

        ByteBuffer filtered = ByteBuffer.allocate(2048);
        MemoryRecords.readableRecords(buffer).filterTo(new RetainNonNullKeysFilter(), filtered);

        filtered.flip();
        MemoryRecords filteredRecords = MemoryRecords.readableRecords(filtered);

        List<RecordBatch.MutableRecordBatch> shallowBatches = TestUtils.toList(filteredRecords.batches());
        assertEquals(magic < RecordBatch.MAGIC_VALUE_V2 && compression == CompressionType.NONE ? 3 : 2, shallowBatches.size());

        for (RecordBatch shallowEntry : shallowBatches) {
            assertEquals(compression, shallowEntry.compressionType());
            if (magic > RecordBatch.MAGIC_VALUE_V0) {
                assertEquals(TimestampType.LOG_APPEND_TIME, shallowEntry.timestampType());
                assertEquals(logAppendTime, shallowEntry.maxTimestamp());
            }
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        List<Object[]> values = new ArrayList<>();
        for (long firstOffset : asList(0L, 57L))
            for (byte magic : asList(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1, RecordBatch.MAGIC_VALUE_V2))
                for (CompressionType type: CompressionType.values())
                    values.add(new Object[] {magic, firstOffset, type});
        return values;
    }

    private static class RetainNonNullKeysFilter implements MemoryRecords.RecordFilter {
        @Override
        public boolean shouldRetain(Record record) {
            return record.hasKey();
        }
    }
}
