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

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ValueTimestampHeadersDeserializerTest {

    private static final String TOPIC = "test-topic";
    private ValueTimestampHeadersSerializer<String> serializer;
    private ValueTimestampHeadersDeserializer<String> deserializer;

    @BeforeEach
    void setup() {
        serializer = new ValueTimestampHeadersSerializer<>(Serdes.String().serializer());
        deserializer = new ValueTimestampHeadersDeserializer<>(Serdes.String().deserializer());
    }

    @AfterEach
    void cleanup() {
        if (serializer != null) {
            serializer.close();
        }
        if (deserializer != null) {
            deserializer.close();
        }
    }

    @Test
    public void shouldDeserializeNullToNull() {
        final ValueTimestampHeaders<String> result = deserializer.deserialize(TOPIC, null);
        assertNull(result);
    }

    @Test
    public void shouldDeserializeWithEmptyHeaders() {
        final Headers headers = new RecordHeaders();
        final ValueTimestampHeaders<String> original =
            ValueTimestampHeaders.make("test-value", 123456789L, headers);

        final byte[] serialized = serializer.serialize(TOPIC, original);
        final ValueTimestampHeaders<String> deserialized = deserializer.deserialize(TOPIC, serialized);

        assertNotNull(deserialized);
        assertEquals("test-value", deserialized.value());
        assertEquals(123456789L, deserialized.timestamp());
        assertNotNull(deserialized.headers());
        assertEquals(0, deserialized.headers().toArray().length);
    }

    @Test
    public void shouldDeserializeWithSingleHeader() {
        final Headers headers = new RecordHeaders()
            .add("key1", "value1".getBytes());
        final ValueTimestampHeaders<String> original =
            ValueTimestampHeaders.make("test-value", 123456789L, headers);

        final byte[] serialized = serializer.serialize(TOPIC, original);
        final ValueTimestampHeaders<String> deserialized = deserializer.deserialize(TOPIC, serialized);

        assertNotNull(deserialized);
        assertEquals("test-value", deserialized.value());
        assertEquals(123456789L, deserialized.timestamp());

        final Headers deserializedHeaders = deserialized.headers();
        assertNotNull(deserializedHeaders);
        assertEquals(1, deserializedHeaders.toArray().length);

        final Header header = deserializedHeaders.lastHeader("key1");
        assertNotNull(header);
        assertEquals("key1", header.key());
        assertArrayEquals("value1".getBytes(), header.value());
    }

    @Test
    public void shouldDeserializeWithMultipleHeaders() {
        final Headers headers = new RecordHeaders()
            .add("key0", "value0".getBytes())
            .add("key0", "value1".getBytes())
            .add("key2", "value2".getBytes());
        final ValueTimestampHeaders<String> original =
            ValueTimestampHeaders.make("test-value", 123456789L, headers);

        final byte[] serialized = serializer.serialize(TOPIC, original);
        final ValueTimestampHeaders<String> deserialized = deserializer.deserialize(TOPIC, serialized);

        assertNotNull(deserialized);
        assertEquals("test-value", deserialized.value());
        assertEquals(123456789L, deserialized.timestamp());

        final Headers deserializedHeaders = deserialized.headers();
        assertNotNull(deserializedHeaders);
        final Header[] headersArray = deserializedHeaders.toArray();
        assertEquals(3, headersArray.length);
        final Iterator<Header> iterator = headers.iterator();
        Header next = iterator.next();
        assertEquals("key0", next.key());
        assertArrayEquals(("value0").getBytes(), next.value());
        next = iterator.next();
        assertEquals("key0", next.key());
        assertArrayEquals(("value1").getBytes(), next.value());
        next = iterator.next();
        assertEquals("key2", next.key());
        assertArrayEquals(("value2").getBytes(), next.value());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldReturnNullWhenSerializingNullValue() {
        final Headers headers = new RecordHeaders()
            .add("key1", "value1".getBytes());
        final ValueTimestampHeaders<String> original =
            ValueTimestampHeaders.makeAllowNullable(null, 123456789L, headers);

        final byte[] serialized = serializer.serialize(TOPIC, original);
        assertNull(serialized, "Serializer should return null when value is null");

        final ValueTimestampHeaders<String> deserialized = deserializer.deserialize(TOPIC, serialized);
        assertNull(deserialized, "Deserializer should return null when input is null");
    }

    @Test
    public void shouldDeserializeWithHeaderContainingNullValue() {
        final Headers headers = new RecordHeaders()
            .add("key1", null);
        final ValueTimestampHeaders<String> original =
            ValueTimestampHeaders.make("test-value", 123456789L, headers);

        final byte[] serialized = serializer.serialize(TOPIC, original);
        final ValueTimestampHeaders<String> deserialized = deserializer.deserialize(TOPIC, serialized);

        assertNotNull(deserialized);
        assertEquals("test-value", deserialized.value());
        assertEquals(123456789L, deserialized.timestamp());

        final Header header = deserialized.headers().lastHeader("key1");
        assertNotNull(header);
        assertEquals("key1", header.key());
        assertNull(header.value());
    }

    @Test
    public void shouldExtractValue() {
        final Headers headers = new RecordHeaders()
            .add("key1", "value1".getBytes());
        final ValueTimestampHeaders<String> original =
            ValueTimestampHeaders.make("test-value", 123456789L, headers);

        final byte[] serialized = serializer.serialize(TOPIC, original);

        try (final Serde<String> stringSerde = Serdes.String()) {
            final String value = ValueTimestampHeadersDeserializer.value(serialized, stringSerde.deserializer());
            assertNotNull(value);
            assertEquals("test-value", value);
        }
    }

    @Test
    public void shouldReturnNullForRawValueWhenInputIsNull() {
        final ValueTimestampHeaders<String> value = ValueTimestampHeadersDeserializer.value(null, deserializer);
        assertNull(value);
    }

    @Test
    public void shouldExtractTimestamp() {
        final Headers headers = new RecordHeaders()
            .add("key1", "value1".getBytes());
        final ValueTimestampHeaders<String> original =
            ValueTimestampHeaders.make("test-value", 123456789L, headers);

        final byte[] serialized = serializer.serialize(TOPIC, original);
        final long timestamp = ValueTimestampHeadersDeserializer.timestamp(serialized);

        assertEquals(123456789L, timestamp);
    }

    @Test
    public void shouldThrowExceptionWhenExtractingTimestampFromNull() {
        // ByteBuffer.wrap() throws NullPointerException for null input
        assertThrows(NullPointerException.class, () ->
            ValueTimestampHeadersDeserializer.timestamp(null)
        );
    }

    @Test
    public void shouldExtractHeaders() {
        final Headers headers = new RecordHeaders()
            .add("key1", "value1".getBytes())
            .add("key2", "value2".getBytes());
        final ValueTimestampHeaders<String> original =
            ValueTimestampHeaders.make("test-value", 123456789L, headers);

        final byte[] serialized = serializer.serialize(TOPIC, original);
        final Headers extractedHeaders = ValueTimestampHeadersDeserializer.headers(serialized);

        assertNotNull(extractedHeaders);
        assertEquals(2, extractedHeaders.toArray().length);
        assertArrayEquals("value1".getBytes(), extractedHeaders.lastHeader("key1").value());
        assertArrayEquals("value2".getBytes(), extractedHeaders.lastHeader("key2").value());
    }

    @Test
    public void shouldExtractEmptyHeaders() {
        final Headers headers = new RecordHeaders();
        final ValueTimestampHeaders<String> original =
            ValueTimestampHeaders.make("test-value", 123456789L, headers);

        final byte[] serialized = serializer.serialize(TOPIC, original);
        final Headers extractedHeaders = ValueTimestampHeadersDeserializer.headers(serialized);

        assertNotNull(extractedHeaders);
        assertEquals(0, extractedHeaders.toArray().length);
    }

    @Test
    public void shouldReturnNullWhenExtractingHeadersFromNull() {
        final Headers headers = ValueTimestampHeadersDeserializer.headers(null);
        assertNull(headers);
    }

    @Test
    public void shouldThrowExceptionWhenDataIsTooShort() {
        // Create malformed data: only headersSize varint, no actual headers or timestamp
        final byte[] malformedData = new byte[] {0x02};  // headersSize = 1 but no data follows

        assertThrows(SerializationException.class, () ->
            deserializer.deserialize(TOPIC, malformedData),
            "Should throw SerializationException for malformed data"
        );
    }

    @Test
    public void shouldThrowExceptionWhenHeadersSizeIsInconsistent() {
        // Create data with headersSize = 10 but not enough actual data
        final byte[] malformedData = new byte[] {
            0x14,  // headersSize = 10 (ZigZag encoding)
            0x00, 0x00  // Only 2 bytes when 10 + 8 (timestamp) are expected
        };

        assertThrows(SerializationException.class, () ->
            deserializer.deserialize(TOPIC, malformedData),
            "Should throw SerializationException when buffer doesn't have enough data"
        );
    }
}
