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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ValueTimestampHeadersSerializerTest {

    private static final String TOPIC = "test-topic";
    private static final long TIMESTAMP = 123456789L;
    private static final String VALUE = "test-value";

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
    public void shouldSerializeAndDeserializeNonNullData() {
        final Headers headers = new RecordHeaders()
            .add("key1", "value1".getBytes());
        final ValueTimestampHeaders<String> original =
            ValueTimestampHeaders.make(VALUE, TIMESTAMP, headers);

        final byte[] serialized = serializer.serialize(TOPIC, original);
        assertNotNull(serialized);

        final ValueTimestampHeaders<String> deserialized =
            deserializer.deserialize(TOPIC, serialized);

        assertNotNull(deserialized);
        assertEquals(original.value(), deserialized.value());
        assertEquals(original.timestamp(), deserialized.timestamp());
        assertArrayEquals(original.headers().toArray(), deserialized.headers().toArray());
    }

    @Test
    public void shouldSerializeNullDataAsNull() {
        final byte[] serialized = serializer.serialize(TOPIC, null);
        assertNull(serialized);
    }

    @Test
    public void shouldSerializeValueWithEmptyHeaders() {
        final Headers emptyHeaders = new RecordHeaders();
        final ValueTimestampHeaders<String> valueTimestampHeaders =
            ValueTimestampHeaders.make(VALUE, TIMESTAMP, emptyHeaders);

        final byte[] serialized = serializer.serialize(TOPIC, valueTimestampHeaders);
        assertNotNull(serialized);

        final ValueTimestampHeaders<String> deserialized =
            deserializer.deserialize(TOPIC, serialized);

        assertEquals(VALUE, deserialized.value());
        assertEquals(TIMESTAMP, deserialized.timestamp());
        assertEquals(0, deserialized.headers().toArray().length);
    }

    @Test
    public void shouldSerializeValueWithMultipleHeaders() {
        final Headers headers = new RecordHeaders()
            .add("key1", "value1".getBytes())
            .add("key2", "value2".getBytes())
            .add("key3", "value3".getBytes());
        final ValueTimestampHeaders<String> valueTimestampHeaders =
            ValueTimestampHeaders.make(VALUE, TIMESTAMP, headers);

        final byte[] serialized = serializer.serialize(TOPIC, valueTimestampHeaders);
        assertNotNull(serialized);

        final ValueTimestampHeaders<String> deserialized =
            deserializer.deserialize(TOPIC, serialized);

        assertEquals(VALUE, deserialized.value());
        assertEquals(TIMESTAMP, deserialized.timestamp());
        assertEquals(3, deserialized.headers().toArray().length);
    }

    @Test
    public void shouldSerializeValueWithNullHeaders() {
        final ValueTimestampHeaders<String> valueTimestampHeaders =
            ValueTimestampHeaders.make(VALUE, TIMESTAMP, null);

        final byte[] serialized = serializer.serialize(TOPIC, valueTimestampHeaders);
        assertNotNull(serialized);

        final ValueTimestampHeaders<String> deserialized =
            deserializer.deserialize(TOPIC, serialized);

        assertEquals(VALUE, deserialized.value());
        assertEquals(TIMESTAMP, deserialized.timestamp());
        assertEquals(0, deserialized.headers().toArray().length);
    }

    @Test
    public void shouldDropSerializedValueIfEqualWithGreaterTimestamp() {
        final Headers headers = new RecordHeaders()
            .add("key1", "value1".getBytes());

        final byte[] oldRecord = serializer.serialize(TOPIC, VALUE, TIMESTAMP, headers);
        final byte[] newRecord = serializer.serialize(TOPIC, VALUE, TIMESTAMP + 1, headers);

        assertTrue(ValueTimestampHeadersSerializer.valuesAndHeadersAreSameAndTimeIsIncreasing(
            oldRecord, newRecord));
    }

    @Test
    public void shouldKeepSerializedValueIfOutOfOrder() {
        final Headers headers = new RecordHeaders()
            .add("key1", "value1".getBytes());

        final byte[] oldRecord = serializer.serialize(TOPIC, VALUE, TIMESTAMP, headers);
        final byte[] outOfOrderRecord = serializer.serialize(TOPIC, VALUE, TIMESTAMP - 1, headers);

        assertFalse(ValueTimestampHeadersSerializer.valuesAndHeadersAreSameAndTimeIsIncreasing(
            oldRecord, outOfOrderRecord));
    }

    @Test
    public void shouldKeepSerializedValueIfDifferentValue() {
        final Headers headers = new RecordHeaders()
            .add("key1", "value1".getBytes());

        final byte[] oldRecord = serializer.serialize(TOPIC, VALUE, TIMESTAMP, headers);
        final byte[] newRecord = serializer.serialize(TOPIC, "different-value", TIMESTAMP + 1, headers);

        assertFalse(ValueTimestampHeadersSerializer.valuesAndHeadersAreSameAndTimeIsIncreasing(
            oldRecord, newRecord));
    }

    @Test
    public void shouldKeepSerializedValueIfDifferentHeaders() {
        final Headers headers1 = new RecordHeaders()
            .add("key1", "value1".getBytes());
        final Headers headers2 = new RecordHeaders()
            .add("key2", "value2".getBytes());

        final byte[] oldRecord = serializer.serialize(TOPIC, VALUE, TIMESTAMP, headers1);
        final byte[] newRecord = serializer.serialize(TOPIC, VALUE, TIMESTAMP + 1, headers2);

        assertFalse(ValueTimestampHeadersSerializer.valuesAndHeadersAreSameAndTimeIsIncreasing(
            oldRecord, newRecord));
    }

    @Test
    public void shouldHandleSameReferenceComparison() {
        final Headers headers = new RecordHeaders()
            .add("key1", "value1".getBytes());
        final byte[] record = serializer.serialize(TOPIC, VALUE, TIMESTAMP, headers);

        assertTrue(ValueTimestampHeadersSerializer.valuesAndHeadersAreSameAndTimeIsIncreasing(
            record, record));
    }

    @Test
    public void shouldHandleNullComparison() {
        final Headers headers = new RecordHeaders()
            .add("key1", "value1".getBytes());
        final byte[] record = serializer.serialize(TOPIC, VALUE, TIMESTAMP, headers);

        assertFalse(ValueTimestampHeadersSerializer.valuesAndHeadersAreSameAndTimeIsIncreasing(
            null, record));
        assertFalse(ValueTimestampHeadersSerializer.valuesAndHeadersAreSameAndTimeIsIncreasing(
            record, null));
    }

    @Test
    public void shouldHandleDifferentLengthComparison() {
        final Headers headers1 = new RecordHeaders()
            .add("key1", "value1".getBytes());
        final Headers headers2 = new RecordHeaders()
            .add("key1", "value1".getBytes())
            .add("key2", "value2".getBytes());

        final byte[] record1 = serializer.serialize(TOPIC, VALUE, TIMESTAMP, headers1);
        final byte[] record2 = serializer.serialize(TOPIC, VALUE, TIMESTAMP, headers2);

        assertFalse(ValueTimestampHeadersSerializer.valuesAndHeadersAreSameAndTimeIsIncreasing(
            record1, record2));
    }

    @Test
    public void shouldExtractRawValue() {
        final Headers headers = new RecordHeaders()
            .add("key1", "value1".getBytes());
        final byte[] serialized = serializer.serialize(TOPIC, VALUE, TIMESTAMP, headers);

        final byte[] rawValue = ValueTimestampHeadersDeserializer.rawValue(serialized);
        assertNotNull(rawValue);

        final String deserializedValue = Serdes.String().deserializer().deserialize(TOPIC, rawValue);
        assertEquals(VALUE, deserializedValue);
    }

    @Test
    public void shouldExtractTimestamp() {
        final Headers headers = new RecordHeaders()
            .add("key1", "value1".getBytes());
        final byte[] serialized = serializer.serialize(TOPIC, VALUE, TIMESTAMP, headers);

        final long extractedTimestamp = ValueTimestampHeadersDeserializer.timestamp(serialized);
        assertEquals(TIMESTAMP, extractedTimestamp);
    }

    @Test
    public void shouldExtractHeaders() {
        final Headers headers = new RecordHeaders()
            .add("key1", "value1".getBytes())
            .add("key2", "value2".getBytes());
        final byte[] serialized = serializer.serialize(TOPIC, VALUE, TIMESTAMP, headers);

        final Headers extractedHeaders = ValueTimestampHeadersDeserializer.headers(serialized);
        assertNotNull(extractedHeaders);
        assertEquals(2, extractedHeaders.toArray().length);
        assertArrayEquals("value1".getBytes(), extractedHeaders.lastHeader("key1").value());
        assertArrayEquals("value2".getBytes(), extractedHeaders.lastHeader("key2").value());
    }

    @Test
    public void shouldDeserializeNull() {
        final ValueTimestampHeaders<String> deserialized = deserializer.deserialize(TOPIC, null);
        assertNull(deserialized);
    }

    @Test
    public void shouldReturnNullForRawValueOfNull() {
        final byte[] rawValue = ValueTimestampHeadersDeserializer.rawValue(null);
        assertNull(rawValue);
    }
}
