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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HeadersSerializerTest {

    private final HeadersSerializer serializer = new HeadersSerializer();

    @Test
    public void shouldSerializeNullHeaders() {
        final byte[] serialized = serializer.serialize(null);

        assertNotNull(serialized);
        assertTrue(serialized.length > 0);
        assertEquals(1, serialized.length, "Empty header should have 1 byte to indicate headers count is 0");
    }

    @Test
    public void shouldSerializeEmptyHeaders() {
        final Headers headers = new RecordHeaders();
        final byte[] serialized = serializer.serialize(headers);

        assertNotNull(serialized);
        assertTrue(serialized.length > 0);
        assertEquals(1, serialized.length, "Empty header should have 1 byte to indicate headers count is 0");
    }

    @Test
    public void shouldSerializeSingleHeader() {
        final Headers headers = new RecordHeaders()
            .add("key1", "value1".getBytes());
        final byte[] serialized = serializer.serialize(headers);

        assertNotNull(serialized);
        assertTrue(serialized.length > 0);
    }

    @Test
    public void shouldSerializeMultipleHeaders() {
        final Headers headers = new RecordHeaders()
            .add("key1", "value1".getBytes())
            .add("key2", "value2".getBytes())
            .add("key3", "value3".getBytes());
        final byte[] serialized = serializer.serialize(headers);

        assertNotNull(serialized);
        assertTrue(serialized.length > 0);
    }

    @Test
    public void shouldSerializeHeaderWithNullValue() {
        final Headers headers = new RecordHeaders()
            .add("key1", null);
        final byte[] serialized = serializer.serialize(headers);

        assertNotNull(serialized);
        assertTrue(serialized.length > 0);
    }

    @Test
    public void shouldSerializeHeadersWithEmptyValue() {
        final Headers headers = new RecordHeaders()
            .add("key1", new byte[0]);
        final byte[] serialized = serializer.serialize(headers);

        assertNotNull(serialized);
        assertTrue(serialized.length > 0);
    }

    @Test
    public void shouldSerializeHeadersWithSpecialCharacters() {
        final Headers headers = new RecordHeaders()
            .add("key-with-dash", "value".getBytes())
            .add("key.with.dots", "value".getBytes())
            .add("key_with_underscores", "value".getBytes());
        final byte[] serialized = serializer.serialize(headers);

        assertNotNull(serialized);
        assertTrue(serialized.length > 0);
    }
}
