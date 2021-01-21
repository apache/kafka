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

import org.apache.kafka.common.errors.CorruptRecordException;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ByteBufferLogInputStreamTest {

    @Test
    public void iteratorIgnoresIncompleteEntries() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, CompressionType.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(15L, "a".getBytes(), "1".getBytes());
        builder.append(20L, "b".getBytes(), "2".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, CompressionType.NONE, TimestampType.CREATE_TIME, 2L);
        builder.append(30L, "c".getBytes(), "3".getBytes());
        builder.append(40L, "d".getBytes(), "4".getBytes());
        builder.close();

        buffer.flip();
        buffer.limit(buffer.limit() - 5);

        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        Iterator<MutableRecordBatch> iterator = records.batches().iterator();
        assertTrue(iterator.hasNext());
        MutableRecordBatch first = iterator.next();
        assertEquals(1L, first.lastOffset());

        assertFalse(iterator.hasNext());
    }

    @Test
    public void iteratorRaisesOnTooSmallRecords() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, CompressionType.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(15L, "a".getBytes(), "1".getBytes());
        builder.append(20L, "b".getBytes(), "2".getBytes());
        builder.close();

        int position = buffer.position();

        builder = MemoryRecords.builder(buffer, CompressionType.NONE, TimestampType.CREATE_TIME, 2L);
        builder.append(30L, "c".getBytes(), "3".getBytes());
        builder.append(40L, "d".getBytes(), "4".getBytes());
        builder.close();

        buffer.flip();
        buffer.putInt(position + DefaultRecordBatch.LENGTH_OFFSET, 9);

        ByteBufferLogInputStream logInputStream = new ByteBufferLogInputStream(buffer, Integer.MAX_VALUE);
        assertNotNull(logInputStream.nextBatch());
        assertThrows(CorruptRecordException.class, logInputStream::nextBatch);
    }

    @Test
    public void iteratorRaisesOnInvalidMagic() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, CompressionType.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(15L, "a".getBytes(), "1".getBytes());
        builder.append(20L, "b".getBytes(), "2".getBytes());
        builder.close();

        int position = buffer.position();

        builder = MemoryRecords.builder(buffer, CompressionType.NONE, TimestampType.CREATE_TIME, 2L);
        builder.append(30L, "c".getBytes(), "3".getBytes());
        builder.append(40L, "d".getBytes(), "4".getBytes());
        builder.close();

        buffer.flip();
        buffer.put(position + DefaultRecordBatch.MAGIC_OFFSET, (byte) 37);

        ByteBufferLogInputStream logInputStream = new ByteBufferLogInputStream(buffer, Integer.MAX_VALUE);
        assertNotNull(logInputStream.nextBatch());
        assertThrows(CorruptRecordException.class, logInputStream::nextBatch);
    }

    @Test
    public void iteratorRaisesOnTooLargeRecords() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, CompressionType.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(15L, "a".getBytes(), "1".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, CompressionType.NONE, TimestampType.CREATE_TIME, 2L);
        builder.append(30L, "c".getBytes(), "3".getBytes());
        builder.append(40L, "d".getBytes(), "4".getBytes());
        builder.close();
        buffer.flip();

        ByteBufferLogInputStream logInputStream = new ByteBufferLogInputStream(buffer, 60);
        assertNotNull(logInputStream.nextBatch());
        assertThrows(CorruptRecordException.class, logInputStream::nextBatch);
    }

}
