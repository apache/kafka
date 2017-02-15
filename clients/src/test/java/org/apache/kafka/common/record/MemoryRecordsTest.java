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
    private long pid = 134234L;
    private short epoch = 28;
    private int firstSequence = 777;
    private long logAppendTime = System.currentTimeMillis();

    public MemoryRecordsTest(byte magic, long firstOffset, CompressionType compression) {
        this.magic = magic;
        this.compression = compression;
        this.firstOffset = firstOffset;
    }

    @Test
    public void testIterator() {
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), magic, compression,
                TimestampType.CREATE_TIME, firstOffset, logAppendTime, pid, epoch, firstSequence);

        byte[][] keys = new byte[][] {"a".getBytes(), "b".getBytes(), "c".getBytes(), null, "d".getBytes(), null};
        byte[][] values = new byte[][] {"1".getBytes(), "2".getBytes(), "3".getBytes(), "4".getBytes(), null, null};
        long[] timestamps = new long[] {1, 2, 3, 4, 5, 6};

        for (int i = 0; i < keys.length; i++)
            builder.append(timestamps[i], keys[i], values[i]);


        MemoryRecords memoryRecords = builder.build();
        for (int iteration = 0; iteration < 2; iteration++) {
            int total = 0;
            for (LogEntry entry : memoryRecords.entries()) {
                assertTrue(entry.isValid());
                assertEquals(compression, entry.compressionType());
                assertEquals(firstOffset + total, entry.baseOffset());

                if (magic >= LogEntry.MAGIC_VALUE_V2) {
                    assertEquals(pid, entry.pid());
                    assertEquals(epoch, entry.epoch());
                    assertEquals(firstSequence + total, entry.firstSequence());
                }

                int records = 0;
                for (LogRecord record : entry) {
                    assertTrue(record.isValid());
                    assertTrue(record.hasMagic(entry.magic()));
                    assertFalse(record.isCompressed());
                    assertEquals(firstOffset + total, record.offset());
                    assertEquals(wrapNullable(keys[total]), record.key());
                    assertEquals(wrapNullable(values[total]), record.value());

                    if (magic >= LogEntry.MAGIC_VALUE_V2)
                        assertEquals(firstSequence + total, record.sequence());

                    if (magic > LogEntry.MAGIC_VALUE_V0) {
                        assertEquals(timestamps[total], record.timestamp());
                        if (magic < LogEntry.MAGIC_VALUE_V2)
                            assertTrue(record.hasTimestampType(entry.timestampType()));
                    }

                    total++;
                    records++;
                }

                assertEquals(entry.baseOffset() + records - 1, entry.lastOffset());
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

        List<LogEntry.MutableLogEntry> shallowEntries = TestUtils.toList(filteredRecords.entries());
        final List<Long> expectedEndOffsets;
        final List<Long> expectedStartOffsets;

        if (magic < LogEntry.MAGIC_VALUE_V2 && compression == CompressionType.NONE) {
            expectedEndOffsets = asList(1L, 4L, 5L, 6L);
            expectedStartOffsets = asList(1L, 4L, 5L, 6L);
        } else if (magic < LogEntry.MAGIC_VALUE_V2) {
            expectedEndOffsets = asList(1L, 5L, 6L);
            expectedStartOffsets = asList(1L, 4L, 6L);
        } else {
            expectedEndOffsets = asList(1L, 5L, 6L);
            expectedStartOffsets = asList(1L, 3L, 6L);
        }

        assertEquals(expectedEndOffsets.size(), shallowEntries.size());

        for (int i = 0; i < expectedEndOffsets.size(); i++) {
            LogEntry shallowEntry = shallowEntries.get(i);
            assertEquals(expectedStartOffsets.get(i).longValue(), shallowEntry.baseOffset());
            assertEquals(expectedEndOffsets.get(i).longValue(), shallowEntry.lastOffset());
            assertEquals(magic, shallowEntry.magic());
            assertEquals(compression, shallowEntry.compressionType());
            assertEquals(magic == LogEntry.MAGIC_VALUE_V0 ? TimestampType.NO_TIMESTAMP_TYPE : TimestampType.CREATE_TIME,
                    shallowEntry.timestampType());
        }

        List<LogRecord> records = TestUtils.toList(filteredRecords.records());
        assertEquals(4, records.size());

        LogRecord first = records.get(0);
        assertEquals(1L, first.offset());
        if (magic > LogEntry.MAGIC_VALUE_V0)
            assertEquals(11L, first.timestamp());
        assertEquals(ByteBuffer.wrap("1".getBytes()), first.key());
        assertEquals(ByteBuffer.wrap("b".getBytes()), first.value());

        LogRecord second = records.get(1);
        assertEquals(4L, second.offset());
        if (magic > LogEntry.MAGIC_VALUE_V0)
            assertEquals(20L, second.timestamp());
        assertEquals(ByteBuffer.wrap("4".getBytes()), second.key());
        assertEquals(ByteBuffer.wrap("e".getBytes()), second.value());

        LogRecord third = records.get(2);
        assertEquals(5L, third.offset());
        if (magic > LogEntry.MAGIC_VALUE_V0)
            assertEquals(15L, third.timestamp());
        assertEquals(ByteBuffer.wrap("5".getBytes()), third.key());
        assertEquals(ByteBuffer.wrap("f".getBytes()), third.value());

        LogRecord fourth = records.get(3);
        assertEquals(6L, fourth.offset());
        if (magic > LogEntry.MAGIC_VALUE_V0)
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

        List<LogEntry.MutableLogEntry> shallowEntries = TestUtils.toList(filteredRecords.entries());
        assertEquals(magic < LogEntry.MAGIC_VALUE_V2 && compression == CompressionType.NONE ? 3 : 2, shallowEntries.size());

        for (LogEntry shallowEntry : shallowEntries) {
            assertEquals(compression, shallowEntry.compressionType());
            if (magic > LogEntry.MAGIC_VALUE_V0) {
                assertEquals(TimestampType.LOG_APPEND_TIME, shallowEntry.timestampType());
                assertEquals(logAppendTime, shallowEntry.maxTimestamp());
            }
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        List<Object[]> values = new ArrayList<>();
        for (long firstOffset : asList(0L, 57L))
            for (byte magic : asList(LogEntry.MAGIC_VALUE_V0, LogEntry.MAGIC_VALUE_V1, LogEntry.MAGIC_VALUE_V2))
                for (CompressionType type: CompressionType.values())
                    values.add(new Object[] {magic, firstOffset, type});
        return values;
    }

    private static class RetainNonNullKeysFilter implements MemoryRecords.LogRecordFilter {
        @Override
        public boolean shouldRetain(LogRecord record) {
            return record.hasKey();
        }
    }
}
