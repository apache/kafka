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
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
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
            pid = RecordBatch.NO_PRODUCER_ID;
            epoch = RecordBatch.NO_PRODUCER_EPOCH;
            firstSequence = RecordBatch.NO_SEQUENCE;
        }
    }

    @Test
    public void testIterator() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, magic, compression,
                TimestampType.CREATE_TIME, firstOffset, logAppendTime, pid, epoch, firstSequence, false,
                partitionLeaderEpoch, buffer.limit());

        SimpleRecord[] records = new SimpleRecord[] {
            new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
            new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
            new SimpleRecord(3L, "c".getBytes(), "3".getBytes()),
            new SimpleRecord(4L, null, "4".getBytes()),
            new SimpleRecord(5L, "d".getBytes(), null),
            new SimpleRecord(6L, (byte[]) null, null)
        };

        for (SimpleRecord record : records)
            builder.append(record);

        MemoryRecords memoryRecords = builder.build();
        for (int iteration = 0; iteration < 2; iteration++) {
            int total = 0;
            for (RecordBatch batch : memoryRecords.batches()) {
                assertTrue(batch.isValid());
                assertEquals(compression, batch.compressionType());
                assertEquals(firstOffset + total, batch.baseOffset());

                if (magic >= RecordBatch.MAGIC_VALUE_V2) {
                    assertEquals(pid, batch.producerId());
                    assertEquals(epoch, batch.producerEpoch());
                    assertEquals(firstSequence + total, batch.baseSequence());
                    assertEquals(partitionLeaderEpoch, batch.partitionLeaderEpoch());
                    assertEquals(records.length, batch.countOrNull().intValue());
                    assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
                    assertEquals(records[records.length - 1].timestamp(), batch.maxTimestamp());
                } else {
                    assertEquals(RecordBatch.NO_PRODUCER_ID, batch.producerId());
                    assertEquals(RecordBatch.NO_PRODUCER_EPOCH, batch.producerEpoch());
                    assertEquals(RecordBatch.NO_SEQUENCE, batch.baseSequence());
                    assertEquals(RecordBatch.UNKNOWN_PARTITION_LEADER_EPOCH, batch.partitionLeaderEpoch());
                    assertNull(batch.countOrNull());
                    if (magic == RecordBatch.MAGIC_VALUE_V0)
                        assertEquals(TimestampType.NO_TIMESTAMP_TYPE, batch.timestampType());
                    else
                        assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
                }

                int recordCount = 0;
                for (Record record : batch) {
                    assertTrue(record.isValid());
                    assertTrue(record.hasMagic(batch.magic()));
                    assertFalse(record.isCompressed());
                    assertEquals(firstOffset + total, record.offset());
                    assertEquals(records[total].key(), record.key());
                    assertEquals(records[total].value(), record.value());

                    if (magic >= RecordBatch.MAGIC_VALUE_V2)
                        assertEquals(firstSequence + total, record.sequence());

                    assertFalse(record.hasTimestampType(TimestampType.LOG_APPEND_TIME));
                    if (magic == RecordBatch.MAGIC_VALUE_V0) {
                        assertEquals(RecordBatch.NO_TIMESTAMP, record.timestamp());
                        assertFalse(record.hasTimestampType(TimestampType.CREATE_TIME));
                        assertTrue(record.hasTimestampType(TimestampType.NO_TIMESTAMP_TYPE));
                    } else {
                        assertEquals(records[total].timestamp(), record.timestamp());
                        assertFalse(record.hasTimestampType(TimestampType.NO_TIMESTAMP_TYPE));
                        if (magic < RecordBatch.MAGIC_VALUE_V2)
                            assertTrue(record.hasTimestampType(TimestampType.CREATE_TIME));
                        else
                            assertFalse(record.hasTimestampType(TimestampType.CREATE_TIME));
                    }

                    total++;
                    recordCount++;
                }

                assertEquals(batch.baseOffset() + recordCount - 1, batch.lastOffset());
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

    /**
     * This test verifies that the checksum returned for various versions matches hardcoded values to catch unintentional
     * changes to how the checksum is computed.
     */
    @Test
    public void testChecksum() {
        // we get reasonable coverage with uncompressed and one compression type
        if (compression != CompressionType.NONE && compression != CompressionType.LZ4)
            return;

        SimpleRecord[] records = {
            new SimpleRecord(283843L, "key1".getBytes(), "value1".getBytes()),
            new SimpleRecord(1234L, "key2".getBytes(), "value2".getBytes())
        };
        RecordBatch batch = MemoryRecords.withRecords(magic, compression, records).batches().iterator().next();
        long expectedChecksum;
        if (magic == RecordBatch.MAGIC_VALUE_V0) {
            if (compression == CompressionType.NONE)
                expectedChecksum = 1978725405L;
            else
                expectedChecksum = 66944826L;
        } else if (magic == RecordBatch.MAGIC_VALUE_V1) {
            if (compression == CompressionType.NONE)
                expectedChecksum = 109425508L;
            else
                expectedChecksum = 1407303399L;
        } else {
            if (compression == CompressionType.NONE)
                expectedChecksum = 3851219455L;
            else
                expectedChecksum = 2745969314L;
        }
        assertEquals("Unexpected checksum for magic " + magic + " and compression type " + compression,
                expectedChecksum, batch.checksum());
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

        List<MutableRecordBatch> batches = TestUtils.toList(filteredRecords.batches());
        final List<Long> expectedEndOffsets;
        final List<Long> expectedStartOffsets;
        final List<Long> expectedMaxTimestamps;

        if (magic < RecordBatch.MAGIC_VALUE_V2 && compression == CompressionType.NONE) {
            expectedEndOffsets = asList(1L, 4L, 5L, 6L);
            expectedStartOffsets = asList(1L, 4L, 5L, 6L);
            expectedMaxTimestamps = asList(11L, 20L, 15L, 16L);
        } else if (magic < RecordBatch.MAGIC_VALUE_V2) {
            expectedEndOffsets = asList(1L, 5L, 6L);
            expectedStartOffsets = asList(1L, 4L, 6L);
            expectedMaxTimestamps = asList(11L, 20L, 16L);
        } else {
            expectedEndOffsets = asList(1L, 5L, 6L);
            expectedStartOffsets = asList(1L, 3L, 6L);
            expectedMaxTimestamps = asList(11L, 20L, 16L);
        }

        assertEquals(expectedEndOffsets.size(), batches.size());

        for (int i = 0; i < expectedEndOffsets.size(); i++) {
            RecordBatch batch = batches.get(i);
            assertEquals(expectedStartOffsets.get(i).longValue(), batch.baseOffset());
            assertEquals(expectedEndOffsets.get(i).longValue(), batch.lastOffset());
            assertEquals(magic, batch.magic());
            assertEquals(compression, batch.compressionType());
            if (magic >= RecordBatch.MAGIC_VALUE_V1) {
                assertEquals(expectedMaxTimestamps.get(i).longValue(), batch.maxTimestamp());
                assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
            } else {
                assertEquals(RecordBatch.NO_TIMESTAMP, batch.maxTimestamp());
                assertEquals(TimestampType.NO_TIMESTAMP_TYPE, batch.timestampType());
            }
        }

        List<Record> records = TestUtils.toList(filteredRecords.records());
        assertEquals(4, records.size());

        Record first = records.get(0);
        assertEquals(1L, first.offset());
        if (magic > RecordBatch.MAGIC_VALUE_V0)
            assertEquals(11L, first.timestamp());
        assertEquals("1", Utils.utf8(first.key(), first.keySize()));
        assertEquals("b", Utils.utf8(first.value(), first.valueSize()));

        Record second = records.get(1);
        assertEquals(4L, second.offset());
        if (magic > RecordBatch.MAGIC_VALUE_V0)
            assertEquals(20L, second.timestamp());
        assertEquals("4", Utils.utf8(second.key(), second.keySize()));
        assertEquals("e", Utils.utf8(second.value(), second.valueSize()));

        Record third = records.get(2);
        assertEquals(5L, third.offset());
        if (magic > RecordBatch.MAGIC_VALUE_V0)
            assertEquals(15L, third.timestamp());
        assertEquals("5", Utils.utf8(third.key(), third.keySize()));
        assertEquals("f", Utils.utf8(third.value(), third.valueSize()));

        Record fourth = records.get(3);
        assertEquals(6L, fourth.offset());
        if (magic > RecordBatch.MAGIC_VALUE_V0)
            assertEquals(16L, fourth.timestamp());
        assertEquals("6", Utils.utf8(fourth.key(), fourth.keySize()));
        assertEquals("g", Utils.utf8(fourth.value(), fourth.valueSize()));
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

        List<MutableRecordBatch> batches = TestUtils.toList(filteredRecords.batches());
        assertEquals(magic < RecordBatch.MAGIC_VALUE_V2 && compression == CompressionType.NONE ? 3 : 2, batches.size());

        for (RecordBatch batch : batches) {
            assertEquals(compression, batch.compressionType());
            if (magic > RecordBatch.MAGIC_VALUE_V0) {
                assertEquals(TimestampType.LOG_APPEND_TIME, batch.timestampType());
                assertEquals(logAppendTime, batch.maxTimestamp());
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
