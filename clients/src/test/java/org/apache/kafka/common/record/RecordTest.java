/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class RecordTest {

    private long timestamp;
    private ByteBuffer key;
    private ByteBuffer value;
    private CompressionType compression;
    private Record record;

    public RecordTest(long timestamp, byte[] key, byte[] value, CompressionType compression) {
        this.timestamp = timestamp;
        this.key = key == null ? null : ByteBuffer.wrap(key);
        this.value = value == null ? null : ByteBuffer.wrap(value);
        this.compression = compression;
        this.record = new Record(timestamp, key, value, compression);
    }

    @Test
    public void testFields() {
        assertEquals(compression, record.compressionType());
        assertEquals(key != null, record.hasKey());
        assertEquals(key, record.key());
        if (key != null)
            assertEquals(key.limit(), record.keySize());
        assertEquals(Record.CURRENT_MAGIC_VALUE, record.magic());
        assertEquals(value, record.value());
        if (value != null)
            assertEquals(value.limit(), record.valueSize());
    }

    @Test
    public void testChecksum() {
        assertEquals(record.checksum(), record.computeChecksum());
        assertEquals(record.checksum(), Record.computeChecksum(
            this.timestamp,
            this.key == null ? null : this.key.array(),
            this.value == null ? null : this.value.array(),
            this.compression, 0, -1));
        assertTrue(record.isValid());
        for (int i = Record.CRC_OFFSET + Record.CRC_LENGTH; i < record.size(); i++) {
            Record copy = copyOf(record);
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

    private Record copyOf(Record record) {
        ByteBuffer buffer = ByteBuffer.allocate(record.size());
        record.buffer().put(buffer);
        buffer.rewind();
        record.buffer().rewind();
        return new Record(buffer);
    }

    @Test
    public void testEquality() {
        assertEquals(record, copyOf(record));
    }

    @Parameters
    public static Collection<Object[]> data() {
        byte[] payload = new byte[1000];
        Arrays.fill(payload, (byte) 1);
        List<Object[]> values = new ArrayList<Object[]>();
        for (long timestamp : Arrays.asList(Record.NO_TIMESTAMP, 0L, 1L))
            for (byte[] key : Arrays.asList(null, "".getBytes(), "key".getBytes(), payload))
                for (byte[] value : Arrays.asList(null, "".getBytes(), "value".getBytes(), payload))
                    for (CompressionType compression : CompressionType.values())
                        values.add(new Object[] {timestamp, key, value, compression});
        return values;
    }

}
