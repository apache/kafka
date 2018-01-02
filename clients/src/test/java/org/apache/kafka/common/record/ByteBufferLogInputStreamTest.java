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
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ByteBufferLogInputStreamTest {

    @Test
    public void iteratorIgnoresIncompleteEntries() {
        RecordsBuilder builder = new RecordsBuilder();
        RecordsBuilder.BatchBuilder firstBatch = builder.newBatch();
        firstBatch.append(new SimpleRecord(15L, "a".getBytes(), "1".getBytes()));
        firstBatch.append(new SimpleRecord(20L, "b".getBytes(), "2".getBytes()));
        firstBatch.closeBatch();

        RecordsBuilder.BatchBuilder secondBatch = builder.newBatch();
        secondBatch.append(new SimpleRecord(30L, "c".getBytes(), "3".getBytes()));
        secondBatch.append(new SimpleRecord(40L, "d".getBytes(), "4".getBytes()));
        secondBatch.closeBatch();

        ByteBuffer buffer = builder.build().buffer().duplicate();
        buffer.limit(buffer.limit() - 5);

        MemoryRecords records = new MemoryRecords(buffer);
        Iterator<MutableRecordBatch> iterator = records.batches().iterator();
        assertTrue(iterator.hasNext());
        MutableRecordBatch first = iterator.next();
        assertEquals(1L, first.lastOffset());

        assertFalse(iterator.hasNext());
    }

    @Test(expected = CorruptRecordException.class)
    public void iteratorRaisesOnTooSmallRecords() throws IOException {
        RecordsBuilder builder = new RecordsBuilder();
        RecordsBuilder.BatchBuilder firstBatch = builder.newBatch();
        firstBatch.append(new SimpleRecord(15L, "a".getBytes(), "1".getBytes()));
        firstBatch.append(new SimpleRecord(20L, "b".getBytes(), "2".getBytes()));
        firstBatch.closeBatch();

        int position = builder.bufferPosition();

        RecordsBuilder.BatchBuilder secondBatch = builder.newBatch();
        secondBatch.append(new SimpleRecord(30L, "c".getBytes(), "3".getBytes()));
        secondBatch.append(new SimpleRecord(40L, "d".getBytes(), "4".getBytes()));
        secondBatch.closeBatch();

        MemoryRecords records = builder.build();
        ByteBuffer buffer = records.buffer();
        buffer.putInt(position + DefaultRecordBatch.LENGTH_OFFSET, 9);

        ByteBufferLogInputStream logInputStream = new ByteBufferLogInputStream(buffer, Integer.MAX_VALUE);
        assertNotNull(logInputStream.nextBatch());
        logInputStream.nextBatch();
    }

    @Test(expected = CorruptRecordException.class)
    public void iteratorRaisesOnInvalidMagic() throws IOException {
        RecordsBuilder builder = new RecordsBuilder();
        RecordsBuilder.BatchBuilder firstBatch = builder.newBatch();
        firstBatch.append(new SimpleRecord(15L, "a".getBytes(), "1".getBytes()));
        firstBatch.append(new SimpleRecord(20L, "b".getBytes(), "2".getBytes()));
        firstBatch.closeBatch();

        int position = builder.bufferPosition();

        RecordsBuilder.BatchBuilder secondBatch = builder.newBatch();
        secondBatch.append(new SimpleRecord(30L, "c".getBytes(), "3".getBytes()));
        secondBatch.append(new SimpleRecord(40L, "d".getBytes(), "4".getBytes()));
        secondBatch.closeBatch();

        MemoryRecords records = builder.build();
        ByteBuffer buffer = records.buffer();
        buffer.put(position + DefaultRecordBatch.MAGIC_OFFSET, (byte) 37);

        ByteBufferLogInputStream logInputStream = new ByteBufferLogInputStream(buffer, Integer.MAX_VALUE);
        assertNotNull(logInputStream.nextBatch());
        logInputStream.nextBatch();
    }

    @Test(expected = CorruptRecordException.class)
    public void iteratorRaisesOnTooLargeRecords() throws IOException {
        RecordsBuilder builder = new RecordsBuilder();
        RecordsBuilder.BatchBuilder firstBatch = builder.newBatch();
        firstBatch.append(new SimpleRecord(15L, "a".getBytes(), "1".getBytes()));
        firstBatch.append(new SimpleRecord(20L, "b".getBytes(), "2".getBytes()));
        firstBatch.closeBatch();

        MemoryRecords records = builder.build();
        ByteBufferLogInputStream logInputStream = new ByteBufferLogInputStream(records.buffer(), 25);
        assertNotNull(logInputStream.nextBatch());
        logInputStream.nextBatch();
    }

}
