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

import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RecordsSerdeTest {

    @Test
    public void testSerdeRecords() throws Exception {
        MemoryRecords records = MemoryRecords.withRecords(CompressionType.NONE,
            new SimpleRecord("foo".getBytes()),
            new SimpleRecord("bar".getBytes()));

        SimpleRecordsMessageData message = new SimpleRecordsMessageData()
            .setTopic("foo")
            .setRecordSet(records);

        testAllRoundTrips(message);
    }

    @Test
    public void testSerdeNullRecords() throws Exception {
        SimpleRecordsMessageData message = new SimpleRecordsMessageData()
            .setTopic("foo");
        assertNull(message.recordSet());

        testAllRoundTrips(message);
    }

    @Test
    public void testSerdeEmptyRecords() throws Exception {
        SimpleRecordsMessageData message = new SimpleRecordsMessageData()
            .setTopic("foo")
            .setRecordSet(MemoryRecords.EMPTY);
        testAllRoundTrips(message);
    }

    private void testAllRoundTrips(SimpleRecordsMessageData message) throws Exception {
        for (short version = SimpleRecordsMessageData.LOWEST_SUPPORTED_VERSION;
             version <= SimpleRecordsMessageData.HIGHEST_SUPPORTED_VERSION;
             version++) {
            testRoundTrip(message, version);
        }
    }

    private void testRoundTrip(SimpleRecordsMessageData message, short version) throws IOException {
        ByteBuffer buf = serialize(message, version);

        SimpleRecordsMessageData message2 = deserialize(buf.duplicate(), version);
        assertEquals(message, message2);
        assertEquals(message.hashCode(), message2.hashCode());

        // Check struct serialization as well
        assertEquals(buf, serializeThroughStruct(message, version));
        SimpleRecordsMessageData messageFromStruct = deserializeThroughStruct(buf.duplicate(), version);
        assertEquals(message, messageFromStruct);
        assertEquals(message.hashCode(), messageFromStruct.hashCode());
    }

    private SimpleRecordsMessageData deserializeThroughStruct(ByteBuffer buffer, short version) {
        Schema schema = SimpleRecordsMessageData.SCHEMAS[version];
        Struct struct = schema.read(buffer);
        return new SimpleRecordsMessageData(struct, version);
    }

    private SimpleRecordsMessageData deserialize(ByteBuffer buffer, short version) {
        ByteBufferAccessor readable = new ByteBufferAccessor(buffer);
        return new SimpleRecordsMessageData(readable, version);
    }

    private ByteBuffer serializeThroughStruct(SimpleRecordsMessageData message, short version) {
        Struct struct = message.toStruct(version);
        ByteBuffer buffer = ByteBuffer.allocate(struct.sizeOf());
        struct.writeTo(buffer);
        buffer.flip();
        return buffer;
    }

    private ByteBuffer serialize(SimpleRecordsMessageData message, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int totalMessageSize = message.size(cache, version);
        ByteBuffer buffer = ByteBuffer.allocate(totalMessageSize);
        ByteBufferAccessor writer = new ByteBufferAccessor(buffer);
        message.write(writer, cache, version);
        buffer.flip();
        return buffer;
    }

}
