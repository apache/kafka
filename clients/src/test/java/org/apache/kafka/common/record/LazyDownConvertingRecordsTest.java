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

import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.network.ByteBufferChannel;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class LazyDownConvertingRecordsTest {

    private final Time time = new MockTime();

    @Test
    public void testConversionInSend() throws IOException {
        byte toMagic = 1;
        MemoryRecords original = recordSet(RecordBatch.CURRENT_MAGIC_VALUE);

        LazyDownConvertingRecords converting = new LazyDownConvertingRecords(original, toMagic, 0L, time);
        RecordsSend send = converting.toSend("1");
        Records converted = recordsFromSend(send);
        for (RecordBatch batch : converted.batches())
            assertEquals(toMagic, batch.magic());

        assertEquals(converting.sizeInBytes(), send.size());
        assertEquals(7, send.stats().numRecordsConverted());
    }

    @Test
    public void testBatchIteration() {
        byte toMagic = 1;
        MemoryRecords original = recordSet(RecordBatch.CURRENT_MAGIC_VALUE);
        LazyDownConvertingRecords converting = new LazyDownConvertingRecords(original, toMagic, 0L, time);

        for (RecordBatch batch : converting.batches())
            assertEquals(toMagic, batch.magic());
    }

    @Test
    public void testRecordIteration() {
        byte toMagic = 0;
        MemoryRecords original = recordSet(RecordBatch.CURRENT_MAGIC_VALUE);
        LazyDownConvertingRecords converting = new LazyDownConvertingRecords(original, toMagic, 0L, time);
        for (Record record : converting.records())
            assertEquals(RecordBatch.NO_TIMESTAMP, record.timestamp());
    }

    private MemoryRecords recordSet(byte magic) {
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic, CompressionType.NONE,
                TimestampType.CREATE_TIME, 0L);
        builder.append(10L, "1".getBytes(), "a".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, magic, CompressionType.NONE, TimestampType.CREATE_TIME, 1L);
        builder.append(11L, "2".getBytes(), "b".getBytes());
        builder.append(12L, "3".getBytes(), "c".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, magic, CompressionType.NONE, TimestampType.CREATE_TIME, 3L);
        builder.append(13L, "4".getBytes(), "d".getBytes());
        builder.append(20L, "5".getBytes(), "e".getBytes());
        builder.append(15L, "6".getBytes(), "f".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, magic, CompressionType.NONE, TimestampType.CREATE_TIME, 6L);
        builder.append(16L, "7".getBytes(), "g".getBytes());
        builder.close();

        buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }

    private Records recordsFromSend(Send send) throws IOException {
        ByteBufferChannel channel = new ByteBufferChannel(send.size() * 2);
        while (!send.completed())
            send.writeTo(channel);
        channel.close();
        return MemoryRecords.readableRecords(channel.buffer());
    }

}