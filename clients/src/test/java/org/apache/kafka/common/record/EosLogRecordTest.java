/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.common.record;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EosLogRecordTest {

    @Test
    public void testBasicSerde() {
        ByteBuffer key = ByteBuffer.wrap("hi".getBytes());
        ByteBuffer value = ByteBuffer.wrap("there".getBytes());
        int delta = 57;
        long timestamp = System.currentTimeMillis();

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        EosLogRecord.writeTo(buffer, delta, timestamp, key, value);
        buffer.flip();

        EosLogRecord record = EosLogRecord.readFrom(buffer, 1L, 0, null);
        assertNotNull(record);
        assertEquals(0, record.attributes());
        assertEquals(58L, record.offset());
        assertEquals(57, record.sequence());
        assertEquals(timestamp, record.timestamp());
        assertEquals(key, record.key());
        assertEquals(value, record.value());
    }

    @Test
    public void testSerdeNullKey() {
        ByteBuffer key = null;
        ByteBuffer value = ByteBuffer.wrap("there".getBytes());
        int delta = 57;
        long timestamp = System.currentTimeMillis();

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        EosLogRecord.writeTo(buffer, delta, timestamp, key, value);
        buffer.flip();

        EosLogRecord record = EosLogRecord.readFrom(buffer, 1L, 0, null);
        assertNotNull(record);
        assertEquals(58L, record.offset());
        assertEquals(timestamp, record.timestamp());
        assertEquals(key, record.key());
        assertEquals(value, record.value());
    }

    @Test
    public void testSerdeNullValue() {
        ByteBuffer key = ByteBuffer.wrap("hi".getBytes());
        ByteBuffer value = null;
        int delta = 57;
        long timestamp = System.currentTimeMillis();

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        EosLogRecord.writeTo(buffer, delta, timestamp, key, value);
        buffer.flip();

        EosLogRecord record = EosLogRecord.readFrom(buffer, 1L, 0, null);
        assertNotNull(record);
        assertEquals(58L, record.offset());
        assertEquals(timestamp, record.timestamp());
        assertEquals(key, record.key());
        assertEquals(value, record.value());
    }

    @Test
    public void testSerdeNullKeyAndValue() {
        ByteBuffer key = null;
        ByteBuffer value = null;
        int delta = 57;
        long timestamp = System.currentTimeMillis();

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        EosLogRecord.writeTo(buffer, delta, timestamp, key, value);
        buffer.flip();

        EosLogRecord record = EosLogRecord.readFrom(buffer, 1L, 0, null);
        assertNotNull(record);
        assertEquals(58L, record.offset());
        assertEquals(timestamp, record.timestamp());
        assertEquals(key, record.key());
        assertEquals(value, record.value());
    }

    @Test
    public void testSerdeNoSequence() {
        ByteBuffer key = ByteBuffer.wrap("hi".getBytes());
        ByteBuffer value = ByteBuffer.wrap("there".getBytes());
        int delta = 57;
        long timestamp = System.currentTimeMillis();

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        EosLogRecord.writeTo(buffer, delta, timestamp, key, value);
        buffer.flip();

        EosLogRecord record = EosLogRecord.readFrom(buffer, 1L, LogEntry.NO_SEQUENCE, null);
        assertNotNull(record);
        assertEquals(LogEntry.NO_SEQUENCE, record.sequence());
    }

}
