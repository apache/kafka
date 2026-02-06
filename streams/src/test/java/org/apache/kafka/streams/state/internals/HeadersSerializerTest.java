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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HeadersSerializerTest {

    private final HeadersSerializer serializer = new HeadersSerializer();
    private final HeadersDeserializer deserializer = new HeadersDeserializer();

    @Test
    public void shouldSerializeNullHeaders() {
        final byte[] serialized = serializer.serialize("", null);

        assertNotNull(serialized);
        assertEquals(0, serialized.length, "Null headers should serialize to empty byte array (0 bytes)");
    }

    @Test
    public void shouldSerializeEmptyHeaders() {
        final Headers headers = new RecordHeaders();
        final byte[] serialized = serializer.serialize("", headers);

        assertNotNull(serialized);
        assertEquals(0, serialized.length, "Empty headers should serialize to empty byte array (0 bytes)");
    }

    @Test
    public void shouldSerializeSingleHeader() {
        final Headers headers = new RecordHeaders()
            .add("key1", "value1".getBytes());
        final byte[] serialized = serializer.serialize("", headers);

        assertNotNull(serialized);
        assertTrue(serialized.length > 0);

        final Headers deserialized = deserializer.deserialize("", serialized);
        assertNotNull(deserialized);
        assertEquals(1, deserialized.toArray().length);

        final Header header = deserialized.lastHeader("key1");
        assertNotNull(header);
        assertEquals("key1", header.key());
        assertArrayEquals("value1".getBytes(), header.value());
    }

    @Test
    public void shouldSerializeMultipleHeaders() {
        final Headers headers = new RecordHeaders()
            .add("key0", "value0".getBytes())
            .add("key1", "value1".getBytes())
            .add("key2", "value2".getBytes());
        final byte[] serialized = serializer.serialize("", headers);

        assertNotNull(serialized);
        assertTrue(serialized.length > 0);

        final Headers deserialized = deserializer.deserialize("", serialized);
        assertNotNull(deserialized);
        assertEquals(3, deserialized.toArray().length);

        final Header[] headerArray = deserialized.toArray();
        for (int i = 0; i < headerArray.length; i++) {
            final Header header = headerArray[i];
            assertEquals("key" + i, header.key());
            assertArrayEquals(("value" + i).getBytes(), header.value());
        }
    }

    @Test
    public void shouldSerializeHeaderWithNullValue() {
        final Headers headers = new RecordHeaders()
            .add("key1", null);
        final byte[] serialized = serializer.serialize("", headers);

        assertNotNull(serialized);
        assertTrue(serialized.length > 0);

        final Headers deserialized = deserializer.deserialize("", serialized);
        assertNotNull(deserialized);
        assertEquals(1, deserialized.toArray().length);

        final Header header = deserialized.lastHeader("key1");
        assertNotNull(header);
        assertEquals("key1", header.key());
        assertNull(header.value());
    }

    @Test
    public void shouldSerializeHeadersWithEmptyValue() {
        final Headers headers = new RecordHeaders()
            .add("key1", new byte[0]);
        final byte[] serialized = serializer.serialize("", headers);

        assertNotNull(serialized);
        assertTrue(serialized.length > 0);

        final Headers deserialized = deserializer.deserialize("", serialized);
        assertNotNull(deserialized);
        assertEquals(1, deserialized.toArray().length);

        final Header header = deserialized.lastHeader("key1");
        assertNotNull(header);
        assertEquals("key1", header.key());
        assertArrayEquals(new byte[0], header.value());
    }

    @Test
    public void shouldSerializeHeadersWithSpecialCharacters() {
        final Headers headers = new RecordHeaders()
            .add("key-with-dash", "value".getBytes())
            .add("key.with.dots", "value".getBytes())
            .add("key_with_underscores", "value".getBytes());
        final byte[] serialized = serializer.serialize("", headers);

        assertNotNull(serialized);
        assertTrue(serialized.length > 0);

        final Headers deserialized = deserializer.deserialize("", serialized);
        assertNotNull(deserialized);
        assertEquals(3, deserialized.toArray().length);

        assertNotNull(deserialized.lastHeader("key-with-dash"));
        assertArrayEquals("value".getBytes(), deserialized.lastHeader("key-with-dash").value());

        assertNotNull(deserialized.lastHeader("key.with.dots"));
        assertArrayEquals("value".getBytes(), deserialized.lastHeader("key.with.dots").value());

        assertNotNull(deserialized.lastHeader("key_with_underscores"));
        assertArrayEquals("value".getBytes(), deserialized.lastHeader("key_with_underscores").value());
    }
}
