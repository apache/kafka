/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.serialization;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SerializationTest {

    private static class SerDeser<T> {
        final Serializer<T> serializer;
        final Deserializer<T> deserializer;

        public SerDeser(Serializer<T> serializer, Deserializer<T> deserializer) {
            this.serializer = serializer;
            this.deserializer = deserializer;
        }
    }

    @Test
    public void testStringSerializer() {
        String str = "my string";
        String mytopic = "testTopic";
        List<String> encodings = new ArrayList<String>();
        encodings.add("UTF8");
        encodings.add("UTF-16");

        for (String encoding : encodings) {
            SerDeser<String> serDeser = getStringSerDeser(encoding);
            Serializer<String> serializer = serDeser.serializer;
            Deserializer<String> deserializer = serDeser.deserializer;

            assertEquals("Should get the original string after serialization and deserialization with encoding " + encoding,
                    str, deserializer.deserialize(mytopic, serializer.serialize(mytopic, str)));

            assertEquals("Should support null in serialization and deserialization with encoding " + encoding,
                    null, deserializer.deserialize(mytopic, serializer.serialize(mytopic, null)));
        }
    }

    @Test
    public void testIntegerSerializer() {
        Integer[] integers = new Integer[]{
            423412424,
            -41243432
        };
        String mytopic = "testTopic";

        Serializer<Integer> serializer = new IntegerSerializer();
        Deserializer<Integer> deserializer = new IntegerDeserializer();

        for (Integer integer : integers) {
            assertEquals("Should get the original integer after serialization and deserialization",
                    integer, deserializer.deserialize(mytopic, serializer.serialize(mytopic, integer)));
        }

        assertEquals("Should support null in serialization and deserialization",
                null, deserializer.deserialize(mytopic, serializer.serialize(mytopic, null)));

        serializer.close();
        deserializer.close();
    }

    @Test
    public void testByteBufferSerializer() {
        String mytopic = "testTopic";
        ByteBuffer buf = ByteBuffer.allocate(10);
        buf.put("my string".getBytes());

        Serializer<ByteBuffer> serializer = new ByteBufferSerializer();
        Deserializer<ByteBuffer> deserializer = new ByteBufferDeserializer();

        assertEquals("Should get the original ByteBuffer after serialization and deserialization",
              buf, deserializer.deserialize(mytopic, serializer.serialize(mytopic, buf)));
        System.out.println(buf);
        assertEquals("Should support null in serialization and deserialization",
                null, deserializer.deserialize(mytopic, serializer.serialize(mytopic, null)));

        serializer.close();
        deserializer.close();
    }

    private SerDeser<String> getStringSerDeser(String encoder) {
        Map<String, Object> serializerConfigs = new HashMap<String, Object>();
        serializerConfigs.put("key.serializer.encoding", encoder);
        Serializer<String> serializer = new StringSerializer();
        serializer.configure(serializerConfigs, true);

        Map<String, Object> deserializerConfigs = new HashMap<String, Object>();
        deserializerConfigs.put("key.deserializer.encoding", encoder);
        Deserializer<String> deserializer = new StringDeserializer();
        deserializer.configure(deserializerConfigs, true);

        return new SerDeser<String>(serializer, deserializer);
    }
}
