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

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SimpleRecordTest {

    /* This scenario can happen if the record size field is corrupt and we end up allocating a buffer that is too small */
    @Test(expected = InvalidRecordException.class)
    public void testIsValidWithTooSmallBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(2);
        Record record = new Record(buffer);
        assertFalse(record.isValid());
        record.ensureValid();
    }

    @Test(expected = InvalidRecordException.class)
    public void testIsValidWithChecksumMismatch() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        // set checksum
        buffer.putInt(2);
        Record record = new Record(buffer);
        assertFalse(record.isValid());
        record.ensureValid();
    }

    @Test
    public void testIsValidWithFourBytesBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        Record record = new Record(buffer);
        // it is a bit weird that we return `true` in this case, we could extend the definition of `isValid` to
        // something like the following to detect a clearly corrupt record:
        // return size() >= recordSize(0, 0) && checksum() == computeChecksum();
        assertTrue(record.isValid());
        // no exception should be thrown
        record.ensureValid();
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotUpconvertWithNoTimestampType() {
        Record record = Record.create(Record.MAGIC_VALUE_V0, Record.NO_TIMESTAMP, "foo".getBytes(), "bar".getBytes());
        record.convert(Record.MAGIC_VALUE_V1, TimestampType.NO_TIMESTAMP_TYPE);
    }

    @Test
    public void testConvertFromV0ToV1() {
        byte[][] keys = new byte[][] {"a".getBytes(), "".getBytes(), null, "b".getBytes()};
        byte[][] values = new byte[][] {"1".getBytes(), "".getBytes(), "2".getBytes(), null};

        for (int i = 0; i < keys.length; i++) {
            Record record = Record.create(Record.MAGIC_VALUE_V0, Record.NO_TIMESTAMP, keys[i], values[i]);
            Record converted = record.convert(Record.MAGIC_VALUE_V1, TimestampType.CREATE_TIME);

            assertEquals(Record.MAGIC_VALUE_V1, converted.magic());
            assertEquals(Record.NO_TIMESTAMP, converted.timestamp());
            assertEquals(TimestampType.CREATE_TIME, converted.timestampType());
            assertEquals(record.key(), converted.key());
            assertEquals(record.value(), converted.value());
            assertTrue(record.isValid());
            assertEquals(record.convertedSize(Record.MAGIC_VALUE_V1), converted.sizeInBytes());
        }
    }

    @Test
    public void testConvertFromV1ToV0() {
        byte[][] keys = new byte[][] {"a".getBytes(), "".getBytes(), null, "b".getBytes()};
        byte[][] values = new byte[][] {"1".getBytes(), "".getBytes(), "2".getBytes(), null};

        for (int i = 0; i < keys.length; i++) {
            Record record = Record.create(Record.MAGIC_VALUE_V1, System.currentTimeMillis(), keys[i], values[i]);
            Record converted = record.convert(Record.MAGIC_VALUE_V0, TimestampType.NO_TIMESTAMP_TYPE);

            assertEquals(Record.MAGIC_VALUE_V0, converted.magic());
            assertEquals(Record.NO_TIMESTAMP, converted.timestamp());
            assertEquals(TimestampType.NO_TIMESTAMP_TYPE, converted.timestampType());
            assertEquals(record.key(), converted.key());
            assertEquals(record.value(), converted.value());
            assertTrue(record.isValid());
            assertEquals(record.convertedSize(Record.MAGIC_VALUE_V0), converted.sizeInBytes());
        }
    }

}
