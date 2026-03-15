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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FullChangeSerdeTest {
    private final FullChangeSerde<String> serde = FullChangeSerde.wrap(Serdes.String());

    /**
     * We used to serialize a Change into a single byte[]. Now, we don't anymore, but we still keep this logic here
     * so that we can produce the legacy format to test that we can still deserialize it.
     */
    private static byte[] mergeChangeArraysIntoSingleLegacyFormattedArray(final Change<byte[]> serialChange) {
        if (serialChange == null) {
            return null;
        }

        final int oldSize = serialChange.oldValue == null ? -1 : serialChange.oldValue.length;
        final int newSize = serialChange.newValue == null ? -1 : serialChange.newValue.length;

        final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES * 2 + Math.max(0, oldSize) + Math.max(0, newSize));


        buffer.putInt(oldSize);
        if (serialChange.oldValue != null) {
            buffer.put(serialChange.oldValue);
        }

        buffer.putInt(newSize);
        if (serialChange.newValue != null) {
            buffer.put(serialChange.newValue);
        }
        return buffer.array();
    }

    @Test
    public void shouldRoundTripNull() {
        assertThat(serde.serializeParts(null, null, null), nullValue());
        assertThat(mergeChangeArraysIntoSingleLegacyFormattedArray(null), nullValue());
        assertThat(FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(null), nullValue());
        assertThat(serde.deserializeParts(null, null, null), nullValue());
    }


    @Test
    public void shouldRoundTripNullChange() {
        assertThat(
            serde.serializeParts(null, null, new Change<>(null, null)),
            is(new Change<byte[]>(null, null))
        );

        assertThat(
            serde.deserializeParts(null, null, new Change<>(null, null)),
            is(new Change<String>(null, null))
        );

        final byte[] legacyFormat = mergeChangeArraysIntoSingleLegacyFormattedArray(new Change<>(null, null));
        assertThat(
            FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(legacyFormat),
            is(new Change<byte[]>(null, null))
        );
    }

    @Test
    public void shouldRoundTripOldNull() {
        final Change<byte[]> serialized = serde.serializeParts(null, new RecordHeaders(), new Change<>("new", null));
        final byte[] legacyFormat = mergeChangeArraysIntoSingleLegacyFormattedArray(serialized);
        final Change<byte[]> decomposedLegacyFormat = FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(legacyFormat);
        assertThat(
            serde.deserializeParts(null, new RecordHeaders(), decomposedLegacyFormat),
            is(new Change<>("new", null))
        );
    }

    @Test
    public void shouldRoundTripNewNull() {
        final Change<byte[]> serialized = serde.serializeParts(null, new RecordHeaders(), new Change<>(null, "old"));
        final byte[] legacyFormat = mergeChangeArraysIntoSingleLegacyFormattedArray(serialized);
        final Change<byte[]> decomposedLegacyFormat = FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(legacyFormat);
        assertThat(
            serde.deserializeParts(null, new RecordHeaders(), decomposedLegacyFormat),
            is(new Change<>(null, "old"))
        );
    }

    @Test
    public void shouldRoundTripChange() {
        final Change<byte[]> serialized = serde.serializeParts(null, new RecordHeaders(), new Change<>("new", "old"));
        final byte[] legacyFormat = mergeChangeArraysIntoSingleLegacyFormattedArray(serialized);
        final Change<byte[]> decomposedLegacyFormat = FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(legacyFormat);
        assertThat(
            serde.deserializeParts(null, new RecordHeaders(), decomposedLegacyFormat),
            is(new Change<>("new", "old"))
        );
    }

    @Test
    public void shouldPassHeadersToUnderlyingSerializer() {
        final Serializer<String> mockSerializer = mock(StringSerializer.class);
        final Serde<String> mockSerde = mock(Serdes.StringSerde.class);
        when(mockSerde.serializer()).thenReturn(mockSerializer);

        final String topic = "dummy";
        final String newValue = "new";
        final String oldValue = "old";
        final Headers headers = new RecordHeaders().add("key", "value".getBytes());
        final Change<String> data = new Change<>(newValue, oldValue);

        final FullChangeSerde<String> testSerde = FullChangeSerde.wrap(mockSerde);

        testSerde.serializeParts(topic, headers, data);

        verify(mockSerializer).serialize(topic, headers, newValue);
        verify(mockSerializer).serialize(topic, headers, oldValue);
        verify(mockSerializer, never()).serialize(topic, newValue);
        verify(mockSerializer, never()).serialize(topic, oldValue);
    }

    @Test
    public void shouldPassHeadersToUnderlyingDeserializer() {
        final Deserializer<String> mockDeserializer = mock(StringDeserializer.class);
        final Serde<String> mockSerde = mock(Serdes.StringSerde.class);
        when(mockSerde.deserializer()).thenReturn(mockDeserializer);

        final String topic = "dummy";
        final byte[] newValueBytes = "new".getBytes();
        final byte[] oldValueBytes = "old".getBytes();
        final Headers headers = new RecordHeaders().add("key", "value".getBytes());
        final Change<byte[]> serialChange = new Change<>(newValueBytes, oldValueBytes);

        final FullChangeSerde<String> testSerde = FullChangeSerde.wrap(mockSerde);

        testSerde.deserializeParts(topic, headers, serialChange);

        verify(mockDeserializer).deserialize(topic, headers, newValueBytes);
        verify(mockDeserializer).deserialize(topic, headers, oldValueBytes);
        verify(mockDeserializer, never()).deserialize(topic, newValueBytes);
        verify(mockDeserializer, never()).deserialize(topic, oldValueBytes);
    }
}
