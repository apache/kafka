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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.easymock.EasyMock;
import org.junit.Test;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class FullChangeSerdeTest {
    private final FullChangeSerde<String> serde = FullChangeSerde.castOrWrap(Serdes.String());

    @Test
    public void shouldRoundTripNull() {
        final byte[] serialized = serde.serializer().serialize(null, null);
        assertThat(
            serde.deserializer().deserialize(null, serialized),
            nullValue()
        );
    }


    @Test
    public void shouldRoundTripNullChange() {
        final byte[] serialized = serde.serializer().serialize(null, new Change<>(null, null));
        assertThat(
            serde.deserializer().deserialize(null, serialized),
            is(new Change<>(null, null))
        );
    }

    @Test
    public void shouldRoundTripOldNull() {
        final byte[] serialized = serde.serializer().serialize(null, new Change<>("new", null));
        assertThat(
            serde.deserializer().deserialize(null, serialized),
            is(new Change<>("new", null))
        );
    }

    @Test
    public void shouldRoundTripNewNull() {
        final byte[] serialized = serde.serializer().serialize(null, new Change<>(null, "old"));
        assertThat(
            serde.deserializer().deserialize(null, serialized),
            is(new Change<>(null, "old"))
        );
    }

    @Test
    public void shouldRoundTripChange() {
        final byte[] serialized = serde.serializer().serialize(null, new Change<>("new", "old"));
        assertThat(
            serde.deserializer().deserialize(null, serialized),
            is(new Change<>("new", "old"))
        );
    }

    @Test
    public void shouldConfigureSerde() {
        final Serde<Void> mock = EasyMock.mock(Serde.class);
        mock.configure(emptyMap(), false);
        EasyMock.expectLastCall();
        EasyMock.replay(mock);
        final FullChangeSerde<Void> serde = FullChangeSerde.castOrWrap(mock);
        serde.configure(emptyMap(), false);
        EasyMock.verify(mock);
    }

    @Test
    public void shouldCloseSerde() {
        final Serde<Void> mock = EasyMock.mock(Serde.class);
        mock.close();
        EasyMock.expectLastCall();
        EasyMock.replay(mock);
        final FullChangeSerde<Void> serde = FullChangeSerde.castOrWrap(mock);
        serde.close();
        EasyMock.verify(mock);
    }

    @Test
    public void shouldConfigureSerializer() {
        final Serde<Void> mockSerde = EasyMock.mock(Serde.class);
        final Serializer<Void> mockSerializer = EasyMock.mock(Serializer.class);
        EasyMock.expect(mockSerde.serializer()).andReturn(mockSerializer);
        EasyMock.replay(mockSerde);
        mockSerializer.configure(emptyMap(), false);
        EasyMock.expectLastCall();
        EasyMock.replay(mockSerializer);
        final Serializer<Change<Void>> serializer = FullChangeSerde.castOrWrap(mockSerde).serializer();
        serializer.configure(emptyMap(), false);
        EasyMock.verify(mockSerde);
        EasyMock.verify(mockSerializer);
    }

    @Test
    public void shouldCloseSerializer() {
        final Serde<Void> mockSerde = EasyMock.mock(Serde.class);
        final Serializer<Void> mockSerializer = EasyMock.mock(Serializer.class);
        EasyMock.expect(mockSerde.serializer()).andReturn(mockSerializer);
        EasyMock.replay(mockSerde);
        mockSerializer.close();
        EasyMock.expectLastCall();
        EasyMock.replay(mockSerializer);
        final Serializer<Change<Void>> serializer = FullChangeSerde.castOrWrap(mockSerde).serializer();
        serializer.close();
        EasyMock.verify(mockSerde);
        EasyMock.verify(mockSerializer);
    }

    @Test
    public void shouldConfigureDeserializer() {
        final Serde<Void> mockSerde = EasyMock.mock(Serde.class);
        final Deserializer<Void> mockDeserializer = EasyMock.mock(Deserializer.class);
        EasyMock.expect(mockSerde.deserializer()).andReturn(mockDeserializer);
        EasyMock.replay(mockSerde);
        mockDeserializer.configure(emptyMap(), false);
        EasyMock.expectLastCall();
        EasyMock.replay(mockDeserializer);
        final Deserializer<Change<Void>> serializer = FullChangeSerde.castOrWrap(mockSerde).deserializer();
        serializer.configure(emptyMap(), false);
        EasyMock.verify(mockSerde);
        EasyMock.verify(mockDeserializer);
    }

    @Test
    public void shouldCloseDeserializer() {
        final Serde<Void> mockSerde = EasyMock.mock(Serde.class);
        final Deserializer<Void> mockDeserializer = EasyMock.mock(Deserializer.class);
        EasyMock.expect(mockSerde.deserializer()).andReturn(mockDeserializer);
        EasyMock.replay(mockSerde);
        mockDeserializer.close();
        EasyMock.expectLastCall();
        EasyMock.replay(mockDeserializer);
        final Deserializer<Change<Void>> serializer = FullChangeSerde.castOrWrap(mockSerde).deserializer();
        serializer.close();
        EasyMock.verify(mockSerde);
        EasyMock.verify(mockDeserializer);
    }
}
