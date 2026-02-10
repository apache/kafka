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

public class HeadersDeserializerTest {

    private final HeadersSerializer serializer = new HeadersSerializer();
    private final HeadersDeserializer deserializer = new HeadersDeserializer();

    @Test
    public void shouldDeserializeNullData() {
        final Headers headers = deserializer.deserialize("", null);

        assertNotNull(headers);
        assertEquals(0, headers.toArray().length);
    }

    @Test
    public void shouldDeserializeEmptyData() {
        final Headers headers = deserializer.deserialize("", new byte[0]);

        assertNotNull(headers);
        assertEquals(0, headers.toArray().length);
    }

    @Test
    public void shouldRoundTripEmptyHeaders() {
        final Headers original = new RecordHeaders();
        final byte[] serialized = serializer.serialize("", original);
        final Headers deserialized = deserializer.deserialize("", serialized);

        assertNotNull(deserialized);
        assertEquals(0, deserialized.toArray().length);
    }

    @Test
    public void shouldRoundTripSingleHeader() {
        final Headers original = new RecordHeaders()
            .add("key1", "value1".getBytes());
        final byte[] serialized = serializer.serialize("", original);
        final Headers deserialized = deserializer.deserialize("", serialized);

        assertNotNull(deserialized);
        assertEquals(1, deserialized.toArray().length);

        final Header header = deserialized.lastHeader("key1");
        assertNotNull(header);
        assertEquals("key1", header.key());
        assertArrayEquals("value1".getBytes(), header.value());
    }

    @Test
    public void shouldRoundTripMultipleHeaders() {
        final Headers original = new RecordHeaders()
            .add("key0", "value0".getBytes())
            .add("key1", "value1".getBytes())
            .add("key2", "value2".getBytes());
        final byte[] serialized = serializer.serialize("", original);
        final Headers deserialized = deserializer.deserialize("", serialized);
        assertNotNull(deserialized);

        final Header[] headerArray = deserialized.toArray();
        assertEquals(3, headerArray.length);
        for (int i = 0; i < headerArray.length; i++) {
            Header next = headerArray[i];
            assertEquals("key" + i, next.key());
            assertArrayEquals(("value" + i).getBytes(), next.value());
        }
    }

    @Test
    public void shouldRoundTripHeaderWithNullValue() {
        final Headers original = new RecordHeaders()
            .add("key1", null);
        final byte[] serialized = serializer.serialize("", original);
        final Headers deserialized = deserializer.deserialize("", serialized);

        assertNotNull(deserialized);
        assertEquals(1, deserialized.toArray().length);

        final Header header = deserialized.lastHeader("key1");
        assertNotNull(header);
        assertEquals("key1", header.key());
        assertNull(header.value());
    }

    @Test
    public void shouldRoundTripHeaderWithEmptyValue() {
        final Headers original = new RecordHeaders()
            .add("key1", new byte[0]);
        final byte[] serialized = serializer.serialize("", original);
        final Headers deserialized = deserializer.deserialize("", serialized);

        assertNotNull(deserialized);
        assertEquals(1, deserialized.toArray().length);

        final Header header = deserialized.lastHeader("key1");
        assertNotNull(header);
        assertEquals("key1", header.key());
        assertArrayEquals(new byte[0], header.value());
    }
}
