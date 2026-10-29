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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TimestampedKeyAndJoinSideDeserializerTest {

    @Test
    public void shouldPassHeadersToUnderlyingDeserializer() {
        try (MockedConstruction<LongDeserializer> timestampSerializer = mockConstruction(LongDeserializer.class)) {
            final Deserializer<String> mockDeserializer = mock(StringDeserializer.class);
            final TimestampedKeyAndJoinSideDeserializer<String> testDeserializer = new TimestampedKeyAndJoinSideDeserializer<>(mockDeserializer);
            final Deserializer<Long> innerTimestampDeserializer = timestampSerializer.constructed().get(0);

            final String topic = "dummy";
            final String key = "some-key";
            final long timestamp = 10;
            final Headers headers = new RecordHeaders().add("key", "value".getBytes());
            final TimestampedKeyAndJoinSide<String> data = TimestampedKeyAndJoinSide.makeLeft(key, timestamp);
            final byte[] serializedValue = new TimestampedKeyAndJoinSideSerializer<>(Serdes.String().serializer()).serialize(topic, headers, data);

            when(mockDeserializer.deserialize(topic, headers, key.getBytes())).thenReturn(key);
            when(innerTimestampDeserializer.deserialize(eq(topic), eq(headers), any(byte[].class))).thenReturn(timestamp);

            testDeserializer.deserialize(topic, headers, serializedValue);

            verify(mockDeserializer).deserialize(topic, headers, key.getBytes());
            verify(mockDeserializer, never()).deserialize(topic, key.getBytes());

            verify(innerTimestampDeserializer).deserialize(eq(topic), eq(headers), any(byte[].class));
            verify(innerTimestampDeserializer, never()).deserialize(eq(topic), any(byte[].class));
        }
    }
}
