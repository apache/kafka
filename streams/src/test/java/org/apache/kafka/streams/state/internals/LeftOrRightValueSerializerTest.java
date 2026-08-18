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
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class LeftOrRightValueSerializerTest {
    private static final String TOPIC = "some-topic";
    private static final Headers HEADERS = new RecordHeaders();

    private static final LeftOrRightValueSerde<String, Integer> STRING_OR_INTEGER_SERDE = new LeftOrRightValueSerde<>(Serdes.String(), Serdes.Integer());

    @Test
    public void shouldSerializeStringValue() {
        final String value = "some-string";

        final LeftOrRightValue<String, Integer> leftOrRightValue = LeftOrRightValue.makeLeftValue(value);

        final byte[] serialized = STRING_OR_INTEGER_SERDE.serializer().serialize(TOPIC, HEADERS, leftOrRightValue);

        assertThat(serialized, is(notNullValue()));

        final LeftOrRightValue<String, Integer> deserialized = STRING_OR_INTEGER_SERDE.deserializer().deserialize(TOPIC, HEADERS, serialized);

        assertThat(deserialized, is(leftOrRightValue));
    }

    @Test
    public void shouldSerializeIntegerValue() {
        final int value = 5;

        final LeftOrRightValue<String, Integer> leftOrRightValue = LeftOrRightValue.makeRightValue(value);

        final byte[] serialized = STRING_OR_INTEGER_SERDE.serializer().serialize(TOPIC, HEADERS, leftOrRightValue);

        assertThat(serialized, is(notNullValue()));

        final LeftOrRightValue<String, Integer> deserialized = STRING_OR_INTEGER_SERDE.deserializer().deserialize(TOPIC, HEADERS, serialized);

        assertThat(deserialized, is(leftOrRightValue));
    }

    @Test
    public void shouldThrowIfSerializeValueAsNull() {
        assertThrows(NullPointerException.class,
            () -> STRING_OR_INTEGER_SERDE.serializer().serialize(TOPIC, HEADERS, LeftOrRightValue.makeLeftValue(null)));
    }

    @Test
    public void shouldThrowIfSerializeOtherValueAsNull() {
        assertThrows(NullPointerException.class,
            () -> STRING_OR_INTEGER_SERDE.serializer().serialize(TOPIC, HEADERS, LeftOrRightValue.makeRightValue(null)));
    }

    @Test
    public void shouldPassHeadersToUnderlyingSerializer() {
        final Serializer<String> mockSerializer = mock(StringSerializer.class);

        final String topic = "dummy";
        final String value = "some-string";
        final Headers headers = new RecordHeaders().add("key", "value".getBytes());
        final LeftOrRightValue<String, String> data = LeftOrRightValue.makeLeftValue(value);

        final LeftOrRightValueSerializer<String, String> testSerializer = new LeftOrRightValueSerializer<>(mockSerializer, null);

        testSerializer.serialize(topic, headers, data);

        verify(mockSerializer).serialize(topic, headers, value);
        verify(mockSerializer, never()).serialize(topic, value);
    }
}
