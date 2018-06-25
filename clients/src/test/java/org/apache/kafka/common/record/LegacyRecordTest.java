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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(value = Parameterized.class)
public class LegacyRecordTest {

    private final byte magic;
    private final long timestamp;
    private final ByteBuffer key;
    private final ByteBuffer value;
    private final CompressionType compression;
    private final TimestampType timestampType;
    private final LegacyRecord record;

    public LegacyRecordTest(byte magic, long timestamp, byte[] key, byte[] value, CompressionType compression) {
        this.magic = magic;
        this.timestamp = timestamp;
        this.timestampType = TimestampType.CREATE_TIME;
        this.key = key == null ? null : ByteBuffer.wrap(key);
        this.value = value == null ? null : ByteBuffer.wrap(value);
        this.compression = compression;
        this.record = LegacyRecord.create(magic, timestamp, key, value, compression, timestampType);
    }

    @Test
    public void testFields() {
        assertEquals(compression, record.compressionType());
        assertEquals(key != null, record.hasKey());
        assertEquals(key, record.key());
        if (key != null)
            assertEquals(key.limit(), record.keySize());
        assertEquals(magic, record.magic());
        assertEquals(value, record.value());
        if (value != null)
            assertEquals(value.limit(), record.valueSize());
        if (magic > 0) {
            assertEquals(timestamp, record.timestamp());
            assertEquals(timestampType, record.timestampType());
        } else {
            assertEquals(RecordBatch.NO_TIMESTAMP, record.timestamp());
            assertEquals(TimestampType.NO_TIMESTAMP_TYPE, record.timestampType());
        }
    }

    @Test
    public void testChecksum() {
        assertEquals(record.checksum(), record.computeChecksum());

        byte attributes = LegacyRecord.computeAttributes(magic, this.compression, TimestampType.CREATE_TIME);
        assertEquals(record.checksum(), LegacyRecord.computeChecksum(
                magic,
                attributes,
                this.timestamp,
                this.key == null ? null : this.key.array(),
                this.value == null ? null : this.value.array()
        ));
        assertTrue(record.isValid());
        for (int i = LegacyRecord.CRC_OFFSET + LegacyRecord.CRC_LENGTH; i < record.sizeInBytes(); i++) {
            LegacyRecord copy = copyOf(record);
            copy.buffer().put(i, (byte) 69);
            assertFalse(copy.isValid());
            try {
                copy.ensureValid();
                fail("Should fail the above test.");
            } catch (InvalidRecordException e) {
                // this is good
            }
        }
    }

    private LegacyRecord copyOf(LegacyRecord record) {
        ByteBuffer buffer = ByteBuffer.allocate(record.sizeInBytes());
        record.buffer().put(buffer);
        buffer.rewind();
        record.buffer().rewind();
        return new LegacyRecord(buffer);
    }

    @Test
    public void testEquality() {
        assertEquals(record, copyOf(record));
    }

    @Parameters
    public static Collection<Object[]> data() {
        byte[] payload = new byte[1000];
        Arrays.fill(payload, (byte) 1);
        List<Object[]> values = new ArrayList<>();
        for (byte magic : Arrays.asList(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1))
            for (long timestamp : Arrays.asList(RecordBatch.NO_TIMESTAMP, 0L, 1L))
                for (byte[] key : Arrays.asList(null, "".getBytes(), "key".getBytes(), payload))
                    for (byte[] value : Arrays.asList(null, "".getBytes(), "value".getBytes(), payload))
                        for (CompressionType compression : CompressionType.values())
                            values.add(new Object[] {magic, timestamp, key, value, compression});
        return values;
    }

}
