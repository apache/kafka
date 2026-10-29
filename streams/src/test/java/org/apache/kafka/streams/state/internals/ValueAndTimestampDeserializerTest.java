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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import org.junit.jupiter.api.Test;

import static org.apache.kafka.streams.state.internals.ValueAndTimestampDeserializer.rawValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class ValueAndTimestampDeserializerTest {
    private static final String TOPIC = "some-topic";
    private static final long TIMESTAMP = 23;
    private static final Headers HEADERS = new RecordHeaders().add("key", "value".getBytes());

    private static final ValueAndTimestampSerde<String> STRING_SERDE = new ValueAndTimestampSerde<>(Serdes.String());

    @Test
    public void shouldPassHeadersToUnderlyingDeserializer() {
        final Deserializer<String> mockDeserializer = mock(StringDeserializer.class);
        final ValueAndTimestampDeserializer<String> deserializer = new ValueAndTimestampDeserializer<>(mockDeserializer);

        final String value = "value";
        final ValueAndTimestamp<String> valueAndTimestamp = ValueAndTimestamp.make(value, TIMESTAMP);
        final byte[] serialized = STRING_SERDE.serializer().serialize(TOPIC, HEADERS, valueAndTimestamp);

        deserializer.deserialize(TOPIC, HEADERS, serialized);

        verify(mockDeserializer).deserialize(TOPIC, HEADERS, rawValue(serialized));
        verify(mockDeserializer, never()).deserialize(TOPIC, rawValue(serialized));
    }
}
