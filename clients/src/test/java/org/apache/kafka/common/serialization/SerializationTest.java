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
package org.apache.kafka.common.serialization;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SerializationTest {

    final private String topic = "testTopic";
    final private Map<Class<?>, List<Object>> testData = new HashMap<Class<?>, List<Object>>() {
        {
            put(String.class, Arrays.asList("my string"));
            put(Short.class, Arrays.asList((short) 32767, (short) -32768));
            put(Integer.class, Arrays.asList(423412424, -41243432));
            put(Long.class, Arrays.asList(922337203685477580L, -922337203685477581L));
            put(Float.class, Arrays.asList(5678567.12312f, -5678567.12341f));
            put(Double.class, Arrays.asList(5678567.12312d, -5678567.12341d));
            put(byte[].class, Arrays.asList("my string".getBytes()));
            put(ByteBuffer.class, Arrays.asList(ByteBuffer.allocate(10).put("my string".getBytes())));
            put(Bytes.class, Arrays.asList(new Bytes("my string".getBytes())));
            put(UUID.class, Arrays.asList(UUID.randomUUID()));
        }
    };

    private class DummyClass {
    }

    @SuppressWarnings("unchecked")
    @Test
    public void allSerdesShouldRoundtripInput() {
        for (Map.Entry<Class<?>, List<Object>> test : testData.entrySet()) {
            try (Serde<Object> serde = Serdes.serdeFrom((Class<Object>) test.getKey())) {
                for (Object value : test.getValue()) {
                    assertEquals(value, serde.deserializer().deserialize(topic, serde.serializer().serialize(topic, value)),
                        "Should get the original " + test.getKey().getSimpleName() + " after serialization and deserialization");
                }
            }
        }
    }

    @Test
    public void allSerdesShouldSupportNull() {
        for (Class<?> cls : testData.keySet()) {
            try (Serde<?> serde = Serdes.serdeFrom(cls)) {
                assertNull(serde.serializer().serialize(topic, null),
                    "Should support null in " + cls.getSimpleName() + " serialization");
                assertNull(serde.deserializer().deserialize(topic, null),
                    "Should support null in " + cls.getSimpleName() + " deserialization");
            }
        }
    }

    @Test
    public void testSerdeFromUnknown() {
        assertThrows(IllegalArgumentException.class, () -> Serdes.serdeFrom(DummyClass.class));
    }

    @Test
    public void testSerdeFromNotNull() {
        try (Serde<Long> serde = Serdes.Long()) {
            assertThrows(IllegalArgumentException.class, () -> Serdes.serdeFrom(null, serde.deserializer()));
        }
    }

    @Test
    public void stringSerdeShouldSupportDifferentEncodings() {
        String str = "my string";
        List<String> encodings = Arrays.asList("UTF8", "UTF-16");

        for (String encoding : encodings) {
            try (Serde<String> serDeser = getStringSerde(encoding)) {

                Serializer<String> serializer = serDeser.serializer();
                Deserializer<String> deserializer = serDeser.deserializer();
                assertEquals(str, deserializer.deserialize(topic, serializer.serialize(topic, str)),
                    "Should get the original string after serialization and deserialization with encoding " + encoding);
            }
        }
    }

    @Test
    public void floatDeserializerShouldThrowSerializationExceptionOnZeroBytes() {
        try (Serde<Float> serde = Serdes.Float()) {
            assertThrows(SerializationException.class, () -> serde.deserializer().deserialize(topic, new byte[0]));
        }
    }

    @Test
    public void floatDeserializerShouldThrowSerializationExceptionOnTooFewBytes() {
        try (Serde<Float> serde = Serdes.Float()) {
            assertThrows(SerializationException.class, () -> serde.deserializer().deserialize(topic, new byte[3]));
        }
    }


    @Test
    public void floatDeserializerShouldThrowSerializationExceptionOnTooManyBytes() {
        try (Serde<Float> serde = Serdes.Float()) {
            assertThrows(SerializationException.class, () -> serde.deserializer().deserialize(topic, new byte[5]));
        }
    }

    @Test
    public void floatSerdeShouldPreserveNaNValues() {
        int someNaNAsIntBits = 0x7f800001;
        float someNaN = Float.intBitsToFloat(someNaNAsIntBits);
        int anotherNaNAsIntBits = 0x7f800002;
        float anotherNaN = Float.intBitsToFloat(anotherNaNAsIntBits);

        try (Serde<Float> serde = Serdes.Float()) {
            // Because of NaN semantics we must assert based on the raw int bits.
            Float roundtrip = serde.deserializer().deserialize(topic,
                    serde.serializer().serialize(topic, someNaN));
            assertEquals(someNaNAsIntBits, Float.floatToRawIntBits(roundtrip));
            Float otherRoundtrip = serde.deserializer().deserialize(topic,
                    serde.serializer().serialize(topic, anotherNaN));
            assertEquals(anotherNaNAsIntBits, Float.floatToRawIntBits(otherRoundtrip));
        }
    }

    @Test
    public void testSerializeVoid() {
        try (Serde<Void> serde = Serdes.Void()) {
            serde.serializer().serialize(topic, null);
        }
    }

    @Test
    public void testDeserializeVoid() {
        try (Serde<Void> serde = Serdes.Void()) {
            serde.deserializer().deserialize(topic, null);
        }
    }

    @Test
    public void voidDeserializerShouldThrowOnNotNullValues() {
        try (Serde<Void> serde = Serdes.Void()) {
            assertThrows(IllegalArgumentException.class, () -> serde.deserializer().deserialize(topic, new byte[5]));
        }
    }

    private Serde<String> getStringSerde(String encoder) {
        Map<String, Object> serializerConfigs = new HashMap<String, Object>();
        serializerConfigs.put("key.serializer.encoding", encoder);
        Serializer<String> serializer = Serdes.String().serializer();
        serializer.configure(serializerConfigs, true);

        Map<String, Object> deserializerConfigs = new HashMap<String, Object>();
        deserializerConfigs.put("key.deserializer.encoding", encoder);
        Deserializer<String> deserializer = Serdes.String().deserializer();
        deserializer.configure(deserializerConfigs, true);

        return Serdes.serdeFrom(serializer, deserializer);
    }
}
