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

import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SimpleRecordTest {

    @Test(expected = InvalidRecordException.class)
    public void testCompressedIterationWithNullValue() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        DataOutputStream out = new DataOutputStream(new ByteBufferOutputStream(buffer));
        LegacyLogEntry.writeHeader(out, 0L, LegacyRecord.RECORD_OVERHEAD_V1);
        LegacyRecord.write(out, LogEntry.MAGIC_VALUE_V1, 1L, (byte[]) null, null,
                CompressionType.GZIP, TimestampType.CREATE_TIME);

        buffer.flip();

        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        for (Record record : records.records())
            fail("Iteration should have caused invalid record error");
    }

    @Test(expected = InvalidRecordException.class)
    public void testCompressedIterationWithEmptyRecords() throws Exception {
        ByteBuffer emptyCompressedValue = ByteBuffer.allocate(64);
        OutputStream gzipOutput = CompressionType.GZIP.wrapForOutput(new ByteBufferOutputStream(emptyCompressedValue),
                LogEntry.MAGIC_VALUE_V1, 64);
        gzipOutput.close();
        emptyCompressedValue.flip();

        ByteBuffer buffer = ByteBuffer.allocate(128);
        DataOutputStream out = new DataOutputStream(new ByteBufferOutputStream(buffer));
        LegacyLogEntry.writeHeader(out, 0L, LegacyRecord.RECORD_OVERHEAD_V1 + emptyCompressedValue.remaining());
        LegacyRecord.write(out, LogEntry.MAGIC_VALUE_V1, 1L, null, Utils.toArray(emptyCompressedValue),
                CompressionType.GZIP, TimestampType.CREATE_TIME);

        buffer.flip();

        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        for (Record record : records.records())
            fail("Iteration should have caused invalid record error");
    }

    /* This scenario can happen if the record size field is corrupt and we end up allocating a buffer that is too small */
    @Test(expected = InvalidRecordException.class)
    public void testIsValidWithTooSmallBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(2);
        LegacyRecord record = new LegacyRecord(buffer);
        assertFalse(record.isValid());
        record.ensureValid();
    }

    @Test(expected = InvalidRecordException.class)
    public void testIsValidWithChecksumMismatch() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        // set checksum
        buffer.putInt(2);
        LegacyRecord record = new LegacyRecord(buffer);
        assertFalse(record.isValid());
        record.ensureValid();
    }

    @Test
    public void testIsValidWithFourBytesBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        LegacyRecord record = new LegacyRecord(buffer);
        // it is a bit weird that we return `true` in this case, we could extend the definition of `isValid` to
        // something like the following to detect a clearly corrupt record:
        // return size() >= recordSize(0, 0) && checksum() == computeChecksum();
        assertTrue(record.isValid());
        // no exception should be thrown
        record.ensureValid();
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotUpconvertWithNoTimestampType() {
        LegacyRecord record = LegacyRecord.create(LogEntry.MAGIC_VALUE_V0, LogEntry.NO_TIMESTAMP, "foo".getBytes(), "bar".getBytes());
        record.convert(LogEntry.MAGIC_VALUE_V1, TimestampType.NO_TIMESTAMP_TYPE);
    }

    @Test
    public void testConvertFromV0ToV1() {
        byte[][] keys = new byte[][] {"a".getBytes(), "".getBytes(), null, "b".getBytes()};
        byte[][] values = new byte[][] {"1".getBytes(), "".getBytes(), "2".getBytes(), null};

        for (int i = 0; i < keys.length; i++) {
            LegacyRecord record = LegacyRecord.create(LogEntry.MAGIC_VALUE_V0, LogEntry.NO_TIMESTAMP, keys[i], values[i]);
            LegacyRecord converted = record.convert(LogEntry.MAGIC_VALUE_V1, TimestampType.CREATE_TIME);

            assertEquals(LogEntry.MAGIC_VALUE_V1, converted.magic());
            assertEquals(LogEntry.NO_TIMESTAMP, converted.timestamp());
            assertEquals(TimestampType.CREATE_TIME, converted.timestampType());
            assertEquals(record.key(), converted.key());
            assertEquals(record.value(), converted.value());
            assertTrue(record.isValid());
            assertEquals(record.convertedSize(LogEntry.MAGIC_VALUE_V1), converted.sizeInBytes());
        }
    }

    @Test
    public void testConvertFromV1ToV0() {
        byte[][] keys = new byte[][] {"a".getBytes(), "".getBytes(), null, "b".getBytes()};
        byte[][] values = new byte[][] {"1".getBytes(), "".getBytes(), "2".getBytes(), null};

        for (int i = 0; i < keys.length; i++) {
            LegacyRecord record = LegacyRecord.create(LogEntry.MAGIC_VALUE_V1, System.currentTimeMillis(), keys[i], values[i]);
            LegacyRecord converted = record.convert(LogEntry.MAGIC_VALUE_V0, TimestampType.NO_TIMESTAMP_TYPE);

            assertEquals(LogEntry.MAGIC_VALUE_V0, converted.magic());
            assertEquals(LogEntry.NO_TIMESTAMP, converted.timestamp());
            assertEquals(TimestampType.NO_TIMESTAMP_TYPE, converted.timestampType());
            assertEquals(record.key(), converted.key());
            assertEquals(record.value(), converted.value());
            assertTrue(record.isValid());
            assertEquals(record.convertedSize(LogEntry.MAGIC_VALUE_V0), converted.sizeInBytes());
        }
    }

    @Test
    public void buildEosRecord() {
        ByteBuffer buffer = ByteBuffer.allocate(2048);

        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, LogEntry.MAGIC_VALUE_V2, CompressionType.NONE,
                TimestampType.CREATE_TIME, 1234567L);
        builder.appendWithOffset(1234567, System.currentTimeMillis(), "a".getBytes(), "v".getBytes());
        builder.appendWithOffset(1234568, System.currentTimeMillis(), "b".getBytes(), "v".getBytes());

        MemoryRecords records = builder.build();
        for (LogEntry.MutableLogEntry entry : records.entries()) {
            assertEquals(1234567, entry.baseOffset());
            assertEquals(1234568, entry.lastOffset());
            assertTrue(entry.isValid());

            for (Record record : entry) {
                assertTrue(record.isValid());
            }
        }
    }

    @Test
    public void appendControlRecord() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, LogEntry.MAGIC_VALUE_V2, CompressionType.NONE,
                TimestampType.CREATE_TIME, 0L);

        builder.appendControlRecord(System.currentTimeMillis(), ControlRecordType.COMMIT, null);
        builder.appendControlRecord(System.currentTimeMillis(), ControlRecordType.ABORT, null);
        MemoryRecords records = builder.build();

        List<Record> logRecords = TestUtils.toList(records.records());
        assertEquals(2, logRecords.size());

        Record commitRecord = logRecords.get(0);
        assertTrue(commitRecord.isControlRecord());
        assertEquals(ControlRecordType.COMMIT, ControlRecordType.parse(commitRecord.key()));

        Record abortRecord = logRecords.get(1);
        assertTrue(abortRecord.isControlRecord());
        assertEquals(ControlRecordType.ABORT, ControlRecordType.parse(abortRecord.key()));
    }

}
