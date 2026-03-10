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

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class HeadersDeserializerTest {

    @Test
    public void shouldDeserializeNullData() {
        final Headers headers = HeadersDeserializer.deserialize(null);

        assertNotNull(headers);
        assertEquals(0, headers.toArray().length);
    }

    @Test
    public void shouldDeserializeEmptyData() {
        final Headers headers = HeadersDeserializer.deserialize(new byte[0]);

        assertNotNull(headers);
        assertEquals(0, headers.toArray().length);
    }

    @Test
    public void shouldRoundTripEmptyHeaders() {
        final Headers original = new RecordHeaders();
        final byte[] serialized = HeadersSerializer.serialize(original);
        final Headers deserialized = HeadersDeserializer.deserialize(serialized);

        assertNotNull(deserialized);
        assertEquals(0, deserialized.toArray().length);
    }

    @Test
    public void shouldRoundTripSingleHeader() {
        final Headers original = new RecordHeaders()
            .add("key1", "value1".getBytes());
        final byte[] serialized = HeadersSerializer.serialize(original);
        final Headers deserialized = HeadersDeserializer.deserialize(serialized);

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
        final byte[] serialized = HeadersSerializer.serialize(original);
        final Headers deserialized = HeadersDeserializer.deserialize(serialized);
        assertNotNull(deserialized);

        final Header[] headerArray = deserialized.toArray();
        assertEquals(3, headerArray.length);
        for (int i = 0; i < headerArray.length; i++) {
            final Header next = headerArray[i];
            assertEquals("key" + i, next.key());
            assertArrayEquals(("value" + i).getBytes(), next.value());
        }
    }

    @Test
    public void shouldRoundTripHeaderWithNullValue() {
        final Headers original = new RecordHeaders()
            .add("key1", null);
        final byte[] serialized = HeadersSerializer.serialize(original);
        final Headers deserialized = HeadersDeserializer.deserialize(serialized);

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
        final byte[] serialized = HeadersSerializer.serialize(original);
        final Headers deserialized = HeadersDeserializer.deserialize(serialized);

        assertNotNull(deserialized);
        assertEquals(1, deserialized.toArray().length);

        final Header header = deserialized.lastHeader("key1");
        assertNotNull(header);
        assertEquals("key1", header.key());
        assertArrayEquals(new byte[0], header.value());
    }

    @Test
    public void shouldAllowDuplicateKeys() {
        final Headers original = new RecordHeaders()
            .add("key0", "value0".getBytes())
            .add("key0", "value0".getBytes())
            .add("key1", "value1".getBytes())
            .add("key2", "value2".getBytes())
            .add("key2", "value3".getBytes());
        final byte[] serialized = HeadersSerializer.serialize(original);
        final Headers deserialized = HeadersDeserializer.deserialize(serialized);
        assertNotNull(deserialized);

        final Header[] headerArray = deserialized.toArray();
        assertEquals(5, headerArray.length);
        final Iterator<Header> iterator = deserialized.iterator();
        Header next = iterator.next();
        assertEquals("key0", next.key());
        assertArrayEquals("value0".getBytes(), next.value());
        next = iterator.next();
        assertEquals("key0", next.key());
        assertArrayEquals("value0".getBytes(), next.value());
        next = iterator.next();
        assertEquals("key1", next.key());
        assertArrayEquals("value1".getBytes(), next.value());
        next = iterator.next();
        assertEquals("key2", next.key());
        assertArrayEquals("value2".getBytes(), next.value());
        next = iterator.next();
        assertEquals("key2", next.key());
        assertArrayEquals("value3".getBytes(), next.value());
    }
}
