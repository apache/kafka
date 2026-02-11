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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

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
            .add("key1", "value2".getBytes())
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
    public void shouldReturnNullWhenSerializingNullValue() {
        final ValueTimestampHeaders<String> valueTimestampHeaders =
            ValueTimestampHeaders.makeAllowNullable(null, TIMESTAMP, new RecordHeaders());
        final byte[] serialized = serializer.serialize(TOPIC, valueTimestampHeaders);
        assertNull(serialized);
    }
}
