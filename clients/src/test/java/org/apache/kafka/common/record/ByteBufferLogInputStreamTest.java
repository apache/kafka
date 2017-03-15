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
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ByteBufferLogInputStreamTest {

    @Test
    public void iteratorIgnoresIncompleteEntries() {
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, Record.MAGIC_VALUE_V1, CompressionType.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(15L, "a".getBytes(), "1".getBytes());
        builder.append(20L, "b".getBytes(), "2".getBytes());

        ByteBuffer recordsBuffer = builder.build().buffer();
        recordsBuffer.limit(recordsBuffer.limit() - 5);

        Iterator<ByteBufferLogInputStream.ByteBufferLogEntry> iterator = MemoryRecords.readableRecords(recordsBuffer).shallowEntries().iterator();
        assertTrue(iterator.hasNext());
        ByteBufferLogInputStream.ByteBufferLogEntry first = iterator.next();
        assertEquals(0L, first.offset());

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testSetCreateTimeV1() {
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, Record.MAGIC_VALUE_V1, CompressionType.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(15L, "a".getBytes(), "1".getBytes());
        Iterator<ByteBufferLogInputStream.ByteBufferLogEntry> iterator = builder.build().shallowEntries().iterator();

        assertTrue(iterator.hasNext());
        ByteBufferLogInputStream.ByteBufferLogEntry entry = iterator.next();

        long createTimeMs = 20L;
        entry.setCreateTime(createTimeMs);

        assertEquals(TimestampType.CREATE_TIME, entry.record().timestampType());
        assertEquals(createTimeMs, entry.record().timestamp());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetCreateTimeNotAllowedV0() {
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, Record.MAGIC_VALUE_V0, CompressionType.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(15L, "a".getBytes(), "1".getBytes());
        Iterator<ByteBufferLogInputStream.ByteBufferLogEntry> iterator = builder.build().shallowEntries().iterator();

        assertTrue(iterator.hasNext());
        ByteBufferLogInputStream.ByteBufferLogEntry entry = iterator.next();

        long createTimeMs = 20L;
        entry.setCreateTime(createTimeMs);
    }

    @Test
    public void testSetLogAppendTimeV1() {
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, Record.MAGIC_VALUE_V1, CompressionType.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(15L, "a".getBytes(), "1".getBytes());
        Iterator<ByteBufferLogInputStream.ByteBufferLogEntry> iterator = builder.build().shallowEntries().iterator();

        assertTrue(iterator.hasNext());
        ByteBufferLogInputStream.ByteBufferLogEntry entry = iterator.next();

        long logAppendTime = 20L;
        entry.setLogAppendTime(logAppendTime);

        assertEquals(TimestampType.LOG_APPEND_TIME, entry.record().timestampType());
        assertEquals(logAppendTime, entry.record().timestamp());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetLogAppendTimeNotAllowedV0() {
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, Record.MAGIC_VALUE_V0, CompressionType.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(15L, "a".getBytes(), "1".getBytes());
        Iterator<ByteBufferLogInputStream.ByteBufferLogEntry> iterator = builder.build().shallowEntries().iterator();

        assertTrue(iterator.hasNext());
        ByteBufferLogInputStream.ByteBufferLogEntry entry = iterator.next();

        long logAppendTime = 20L;
        entry.setLogAppendTime(logAppendTime);
    }

}
