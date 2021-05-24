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
package org.apache.kafka.common.protocol.types;

import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TypeTest {

    @Test
    public void testEmptyRecordsSerde() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        Type.RECORDS.write(buffer, MemoryRecords.EMPTY);
        buffer.flip();
        assertEquals(4, Type.RECORDS.sizeOf(MemoryRecords.EMPTY));
        assertEquals(4, buffer.limit());
        assertEquals(MemoryRecords.EMPTY, Type.RECORDS.read(buffer));
    }

    @Test
    public void testNullRecordsSerde() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        Type.RECORDS.write(buffer, null);
        buffer.flip();
        assertEquals(4, Type.RECORDS.sizeOf(MemoryRecords.EMPTY));
        assertEquals(4, buffer.limit());
        assertNull(Type.RECORDS.read(buffer));
    }

    @Test
    public void testRecordsSerde() {
        MemoryRecords records = MemoryRecords.withRecords(CompressionType.NONE,
            new SimpleRecord("foo".getBytes()),
            new SimpleRecord("bar".getBytes()));
        ByteBuffer buffer = ByteBuffer.allocate(Type.RECORDS.sizeOf(records));
        Type.RECORDS.write(buffer, records);
        buffer.flip();
        assertEquals(records, Type.RECORDS.read(buffer));
    }

    @Test
    public void testEmptyCompactRecordsSerde() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        Type.COMPACT_RECORDS.write(buffer, MemoryRecords.EMPTY);
        buffer.flip();
        assertEquals(1, Type.COMPACT_RECORDS.sizeOf(MemoryRecords.EMPTY));
        assertEquals(1, buffer.limit());
        assertEquals(MemoryRecords.EMPTY, Type.COMPACT_RECORDS.read(buffer));
    }

    @Test
    public void testNullCompactRecordsSerde() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        Type.COMPACT_RECORDS.write(buffer, null);
        buffer.flip();
        assertEquals(1, Type.COMPACT_RECORDS.sizeOf(MemoryRecords.EMPTY));
        assertEquals(1, buffer.limit());
        assertNull(Type.COMPACT_RECORDS.read(buffer));
    }

    @Test
    public void testCompactRecordsSerde() {
        MemoryRecords records = MemoryRecords.withRecords(CompressionType.NONE,
            new SimpleRecord("foo".getBytes()),
            new SimpleRecord("bar".getBytes()));
        ByteBuffer buffer = ByteBuffer.allocate(Type.COMPACT_RECORDS.sizeOf(records));
        Type.COMPACT_RECORDS.write(buffer, records);
        buffer.flip();
        assertEquals(records, Type.COMPACT_RECORDS.read(buffer));
    }

}
