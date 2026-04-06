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
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TimestampedKeyAndJoinSideSerializerTest {
    private static final String TOPIC = "some-topic";
    private static final Headers HEADERS = new RecordHeaders();

    private static final TimestampedKeyAndJoinSideSerde<String> STRING_SERDE = new TimestampedKeyAndJoinSideSerde<>(Serdes.String());

    @Test
    public void shouldSerializeKeyWithJoinSideAsTrue() {
        final String value = "some-string";

        final TimestampedKeyAndJoinSide<String> timestampedKeyAndJoinSide = TimestampedKeyAndJoinSide.makeLeft(value, 10);

        final byte[] serialized = STRING_SERDE.serializer().serialize(TOPIC, HEADERS, timestampedKeyAndJoinSide);

        assertThat(serialized, is(notNullValue()));

        final TimestampedKeyAndJoinSide<String> deserialized = STRING_SERDE.deserializer().deserialize(TOPIC, HEADERS, serialized);

        assertThat(deserialized, is(timestampedKeyAndJoinSide));
    }

    @Test
    public void shouldSerializeKeyWithJoinSideAsFalse() {
        final String value = "some-string";

        final TimestampedKeyAndJoinSide<String> timestampedKeyAndJoinSide = TimestampedKeyAndJoinSide.makeRight(value, 20);

        final byte[] serialized = STRING_SERDE.serializer().serialize(TOPIC, HEADERS, timestampedKeyAndJoinSide);

        assertThat(serialized, is(notNullValue()));

        final TimestampedKeyAndJoinSide<String> deserialized = STRING_SERDE.deserializer().deserialize(TOPIC, HEADERS, serialized);

        assertThat(deserialized, is(timestampedKeyAndJoinSide));
    }

    @Test
    public void shouldThrowIfSerializeNullData() {
        assertThrows(NullPointerException.class,
            () -> STRING_SERDE.serializer().serialize(TOPIC, HEADERS, TimestampedKeyAndJoinSide.makeLeft(null, 0)));
    }

    @Test
    public void shouldPassHeadersToUnderlyingSerializer() {
        try (MockedConstruction<LongSerializer> timestampSerializer = mockConstruction(LongSerializer.class)) {
            final Serializer<String> mockSerializer = mock(StringSerializer.class);
            final TimestampedKeyAndJoinSideSerializer<String> testSerializer = new TimestampedKeyAndJoinSideSerializer<>(mockSerializer);
            final Serializer<Long> innerTimestampSerializer = timestampSerializer.constructed().get(0);

            final String topic = "dummy";
            final String key = "some-key";
            final long timestamp = 10;
            final Headers headers = new RecordHeaders().add("key", "value".getBytes());
            final TimestampedKeyAndJoinSide<String> data = TimestampedKeyAndJoinSide.makeLeft(key, timestamp);

            when(mockSerializer.serialize(topic, headers, data.key())).thenReturn(key.getBytes());
            when(innerTimestampSerializer.serialize(topic, headers, data.timestamp())).thenReturn(new byte[]{Byte.MAX_VALUE});

            testSerializer.serialize(topic, headers, data);

            verify(mockSerializer).serialize(topic, headers, key);
            verify(mockSerializer, never()).serialize(topic, key);

            verify(innerTimestampSerializer).serialize(topic, headers, timestamp);
            verify(innerTimestampSerializer, never()).serialize(topic, timestamp);
        }
    }
}
