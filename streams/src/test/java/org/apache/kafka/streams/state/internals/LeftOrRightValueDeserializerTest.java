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

import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LeftOrRightValueDeserializerTest {

    @Test
    public void shouldPassHeadersToUnderlyingDeserializer() {
        final Deserializer<String> mockDeserializer = mock(StringDeserializer.class);

        final String topic = "dummy";
        final String value = "some-string";
        final Headers headers = new RecordHeaders().add("key", "value".getBytes());
        final LeftOrRightValue<String, Object> data = LeftOrRightValue.makeLeftValue(value);
        final byte[] serializedBytes = new LeftOrRightValueSerializer<>(Serdes.String().serializer(), null).serialize(topic, headers, data);

        when(mockDeserializer.deserialize(topic, headers, value.getBytes())).thenReturn("dummy-value");

        final LeftOrRightValueDeserializer<String, String> testDeserializer = new LeftOrRightValueDeserializer<>(mockDeserializer, null);

        testDeserializer.deserialize(topic, headers, serializedBytes);

        verify(mockDeserializer).deserialize(topic, headers, value.getBytes());
        verify(mockDeserializer, never()).deserialize(topic, value.getBytes());
    }
}
