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
package org.apache.kafka.common.message;

import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.RecordsReadable;
import org.apache.kafka.common.protocol.RecordsWritable;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MultiRecordsSend;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ByteBufferChannel;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RecordsSerdeTest {

    @Test
    public void testSerde() throws Exception {
        MemoryRecords records = MemoryRecords.withRecords(CompressionType.NONE,
            new SimpleRecord("foo".getBytes()),
            new SimpleRecord("bar".getBytes()));

        SimpleRecordsMessageData message = new SimpleRecordsMessageData()
            .setTopic("foo")
            .setRecordSet(records);

        testSerdeAllVerions(message);
    }

    @Test
    public void testSerdeNullRecords() throws Exception {
        SimpleRecordsMessageData message = new SimpleRecordsMessageData()
            .setTopic("foo");
        assertNull(message.recordSet());

        testSerdeAllVerions(message);
    }

    private void testSerdeAllVerions(SimpleRecordsMessageData message) throws Exception {
        for (short version = SimpleRecordsMessageData.LOWEST_SUPPORTED_VERSION;
             version <= SimpleRecordsMessageData.HIGHEST_SUPPORTED_VERSION;
             version++) {
            ByteBuffer buffer = serialize(message, version);
            assertEquals(buffer, serializeAsStruct(message, version));
            assertEquals(message, deserialize(buffer.duplicate(), version));
            assertEquals(message, deserializeFromStruct(buffer.duplicate(), version));
        }
    }

    private SimpleRecordsMessageData deserializeFromStruct(ByteBuffer buffer, short version) {
        Schema schema = SimpleRecordsMessageData.SCHEMAS[version];
        Struct struct = schema.read(buffer);
        return new SimpleRecordsMessageData(struct, version);
    }

    private SimpleRecordsMessageData deserialize(ByteBuffer buffer, short version) {
        RecordsReadable readable = new RecordsReadable(buffer);
        return new SimpleRecordsMessageData(readable, version);
    }

    private ByteBuffer serializeAsStruct(SimpleRecordsMessageData message, short version) {
        Struct struct = message.toStruct(version);
        ByteBuffer buffer = ByteBuffer.allocate(struct.sizeOf());
        struct.writeTo(buffer);
        buffer.flip();
        return buffer;
    }

    private ByteBuffer serialize(SimpleRecordsMessageData message, short version) throws IOException {
        ArrayDeque<Send> sends = new ArrayDeque<>();
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int totalMessageSize = message.size(cache, version);

        int recordsSize = message.recordSet() == null ? 0 : message.recordSet().sizeInBytes();
        RecordsWritable writer = new RecordsWritable("0",
            totalMessageSize - recordsSize, sends::add);
        message.write(writer, cache, version);
        writer.flush();

        MultiRecordsSend send = new MultiRecordsSend("0", sends);
        ByteBufferChannel channel = new ByteBufferChannel(send.size());
        send.writeTo(channel);
        channel.close();
        return channel.buffer();
    }

}
