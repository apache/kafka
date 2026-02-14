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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.MemoryRecords;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ProtocolRoundTripConsistencyTest {

    private Struct nonFlexibleStruct;

    private Struct flexibleStruct;

    private AllTypeMessageData messageData;

    @BeforeEach
    public void setup() {
        nonFlexibleStruct = new Struct(AllTypeMessageData.SCHEMA_0)
            .set("my_boolean", false)
            .set("my_int8", (byte) 12)
            .set("my_int16", (short) 123)
            .set("my_uint16", 33000)
            .set("my_int32", 1234)
            .set("my_uint32", 1234567L)
            .set("my_uint64", 0xcafcacafcacafcaL)
            .set("my_uuid", Uuid.fromString("H3KKO4NTRPaCWtEmm3vW7A"))
            .set("my_float64", 12.34D)
            .set("my_string", "string")
            .set("my_nullable_string", null)
            .set("my_bytes", ByteBuffer.wrap("bytes".getBytes()))
            .set("my_nullable_bytes", null)
            .set("my_records", MemoryRecords.EMPTY)
            .set("my_nullable_records", null)
            .set("my_int_array", new Object[] {})
            .set("my_nullable_int_array", null);
        nonFlexibleStruct.set("my_common_struct", nonFlexibleStruct.instance("my_common_struct")
            .set("foo", 123)
            .set("bar", 123));

        flexibleStruct = new Struct(AllTypeMessageData.SCHEMA_1)
            .set("my_boolean", false)
            .set("my_int8", (byte) 12)
            .set("my_int16", (short) 123)
            .set("my_uint16", 33000)
            .set("my_int32", 1234)
            .set("my_uint32", 1234567L)
            .set("my_uint64", 0xcafcacafcacafcaL)
            .set("my_uuid", Uuid.fromString("H3KKO4NTRPaCWtEmm3vW7A"))
            .set("my_float64", 12.34D)
            .set("my_compact_string", "compact string")
            .set("my_compact_nullable_string", null)
            .set("my_compact_bytes", ByteBuffer.wrap("compact bytes".getBytes()))
            .set("my_compact_nullable_bytes", null)
            .set("my_compact_records", MemoryRecords.EMPTY)
            .set("my_compact_nullable_records", null)
            .set("my_int_array", new Object[] {})
            .set("my_nullable_int_array", null)
            .set("_tagged_fields", new TreeMap<Integer, Field>());
        flexibleStruct.set("my_common_struct", flexibleStruct.instance("my_common_struct")
            .set("foo", 123)
            .set("bar", 123)
            .set("_tagged_fields", new TreeMap<Integer, Field>()));

        messageData = new AllTypeMessageData();
    }

    @Test
    public void testNonFlexibleWithNullDefault() {
        messageData.setMyBytes("bytes".getBytes());
        messageData.setMyRecords(MemoryRecords.EMPTY);

        checkSchemaAndMessageRoundTripConsistency((short) 0, messageData, nonFlexibleStruct);
    }

    @Test
    public void testNonFlexibleWithNonNullValue() {
        messageData.setMyBytes("bytes".getBytes());
        messageData.setMyRecords(MemoryRecords.EMPTY);
        messageData.setMyNullableString("nullable string");
        messageData.setMyNullableBytes("nullable bytes".getBytes());
        messageData.setMyNullableRecords(MemoryRecords.EMPTY);
        messageData.setMyNullableIntArray(List.of(1, 2, 3));

        nonFlexibleStruct.set("my_nullable_string", "nullable string")
            .set("my_nullable_bytes", ByteBuffer.wrap("nullable bytes".getBytes()))
            .set("my_nullable_records", MemoryRecords.EMPTY)
            .set("my_nullable_int_array", new Object[] {1, 2, 3});

        checkSchemaAndMessageRoundTripConsistency((short) 0, messageData, nonFlexibleStruct);
    }

    @Test
    public void testFlexibleWithNullDefault() {
        messageData.setMyCompactBytes("compact bytes".getBytes());
        messageData.setMyCompactRecords(MemoryRecords.EMPTY);
        messageData.setMyCommonStruct(null);

        flexibleStruct.set("my_common_struct", null);

        checkSchemaAndMessageRoundTripConsistency((short) 1, messageData, flexibleStruct);
    }

    @Test
    public void testFlexibleWithNonNullValue() {
        messageData.setMyCompactBytes("compact bytes".getBytes());
        messageData.setMyCompactRecords(MemoryRecords.EMPTY);
        messageData.setMyCompactNullableString("compact nullable string");
        messageData.setMyCompactNullableBytes("compact nullable bytes".getBytes());
        messageData.setMyCompactNullableRecords(MemoryRecords.EMPTY);
        messageData.setMyNullableIntArray(List.of(1, 2, 3));

        flexibleStruct.set("my_compact_nullable_string", "compact nullable string")
            .set("my_compact_nullable_bytes", ByteBuffer.wrap("compact nullable bytes".getBytes()))
            .set("my_compact_nullable_records", MemoryRecords.EMPTY)
            .set("my_nullable_int_array", new Object[] {1, 2, 3});

        checkSchemaAndMessageRoundTripConsistency((short) 1, messageData, flexibleStruct);
    }

    private void checkSchemaAndMessageRoundTripConsistency(short version, AllTypeMessageData message, Struct struct) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        ByteBuffer buf = ByteBuffer.allocate(message.size(cache, version));
        ByteBufferAccessor serializedMessageAccessor = new ByteBufferAccessor(buf);
        // Serialize message
        message.write(serializedMessageAccessor, cache, version);

        ByteBuffer serializedSchemaBuffer = ByteBuffer.allocate(struct.sizeOf());
        // Serialize schema
        struct.writeTo(serializedSchemaBuffer);

        assertEquals(message.size(cache, version), serializedMessageAccessor.buffer().position(),
            "Buffer should be completely filled to message size.");
        assertEquals(struct.sizeOf(), serializedSchemaBuffer.position(),
            "Buffer should be completely filled to struct size.");
        assertEquals(serializedSchemaBuffer.position(), serializedMessageAccessor.buffer().position(),
            "Generated and non-generated schema serializer should serialize to the same length.");
        assertEquals(serializedSchemaBuffer, serializedMessageAccessor.buffer(),
            "Generated and non-generated schema serializer should serialize to the same content.");

        serializedMessageAccessor.flip();
        // Deserialize message
        Schema schema = version == 0 ? AllTypeMessageData.SCHEMA_0 : AllTypeMessageData.SCHEMA_1;
        Struct deserializedStruct = schema.read(serializedMessageAccessor.buffer());
        assertEquals(struct, deserializedStruct, "Deserialized struct should match original struct after round trip");

        serializedSchemaBuffer.flip();
        // Deserialize schema
        AllTypeMessageData deserializedMessage = new AllTypeMessageData(new ByteBufferAccessor(serializedSchemaBuffer), version);
        assertEquals(message, deserializedMessage, "Deserialized message should match original message after round trip");
    }
}
