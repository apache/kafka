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
package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.api.ProcessorContext;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CombinedKeySchemaTest {
    private static final String FK_TOPIC = "fkTopic";
    private static final String PK_TOPIC = "pkTopic";
    private static final Headers HEADERS = new RecordHeaders().add("key", "value".getBytes());

    @Test
    public void nonNullPrimaryKeySerdeTest() {
        final CombinedKeySchema<String, Integer> cks = new CombinedKeySchema<>(
            () -> FK_TOPIC, Serdes.String(),
            () -> PK_TOPIC, Serdes.Integer()
        );
        final Integer primary = -999;
        final Bytes result = cks.toBytes("foreignKey", primary, HEADERS);

        final CombinedKey<String, Integer> deserializedKey = cks.fromBytes(result, new RecordHeaders());
        assertEquals("foreignKey", deserializedKey.foreignKey());
        assertEquals(primary, deserializedKey.primaryKey());
    }

    @Test
    public void nullPrimaryKeySerdeTest() {
        final CombinedKeySchema<String, Integer> cks = new CombinedKeySchema<>(
            () -> FK_TOPIC, Serdes.String(),
            () -> PK_TOPIC, Serdes.Integer()
        );
        assertThrows(NullPointerException.class, () -> cks.toBytes("foreignKey", null, HEADERS));
    }

    @Test
    public void nullForeignKeySerdeTest() {
        final CombinedKeySchema<String, Integer> cks = new CombinedKeySchema<>(
            () -> FK_TOPIC, Serdes.String(),
            () -> PK_TOPIC, Serdes.Integer()
        );
        assertThrows(NullPointerException.class, () -> cks.toBytes(null, 10, HEADERS));
    }

    @Test
    public void prefixKeySerdeTest() {
        final CombinedKeySchema<String, Integer> cks = new CombinedKeySchema<>(
            () -> FK_TOPIC, Serdes.String(),
            () -> PK_TOPIC, Serdes.Integer()
        );
        final String foreignKey = "someForeignKey";
        final byte[] foreignKeySerializedData =
            Serdes.String().serializer().serialize(FK_TOPIC, foreignKey);
        final Bytes prefix = cks.prefixBytes(foreignKey, HEADERS);

        final ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + foreignKeySerializedData.length);
        buf.putInt(foreignKeySerializedData.length);
        buf.put(foreignKeySerializedData);
        final Bytes expectedPrefixBytes = Bytes.wrap(buf.array());

        assertEquals(expectedPrefixBytes, prefix);
    }

    @Test
    public void nullPrefixKeySerdeTest() {
        final CombinedKeySchema<String, Integer> cks = new CombinedKeySchema<>(
            () -> FK_TOPIC, Serdes.String(),
            () -> PK_TOPIC, Serdes.Integer()
        );
        final String foreignKey = null;
        assertThrows(NullPointerException.class, () -> cks.prefixBytes(foreignKey, HEADERS));
    }

    @Test
    public void shouldPassHeadersToUnderlyingSerializerOnPrefixBytes() {
        final Serializer<String> mockSerializer = mock(StringSerializer.class);
        final Serde<String> mockSerde = mock(Serdes.StringSerde.class);
        when(mockSerde.serializer()).thenReturn(mockSerializer);
        when(mockSerde.deserializer()).thenReturn(Serdes.String().deserializer());

        final String foreignKey = "foreignKey";
        when(mockSerializer.serialize(FK_TOPIC, HEADERS, foreignKey)).thenReturn(foreignKey.getBytes());

        final CombinedKeySchema<String, String> cks = new CombinedKeySchema<>(
            () -> FK_TOPIC, mockSerde,
            () -> PK_TOPIC, mockSerde
        );
        cks.init(mock(ProcessorContext.class));
        cks.prefixBytes(foreignKey, HEADERS);

        verify(mockSerializer).serialize(FK_TOPIC, HEADERS, foreignKey);
        verify(mockSerializer, never()).serialize(FK_TOPIC, foreignKey);
    }

    @Test
    public void shouldPassHeadersToUnderlyingSerializersOnToBytes() {
        final Serializer<String> mockSerializer = mock(StringSerializer.class);
        final Serde<String> mockSerde = mock(Serdes.StringSerde.class);
        when(mockSerde.serializer()).thenReturn(mockSerializer);
        when(mockSerde.deserializer()).thenReturn(Serdes.String().deserializer());


        final String foreignKey = "foreignKey";
        final String primaryKey = "primaryKey";
        when(mockSerializer.serialize(FK_TOPIC, HEADERS, foreignKey)).thenReturn(foreignKey.getBytes());
        when(mockSerializer.serialize(PK_TOPIC, HEADERS, primaryKey)).thenReturn(primaryKey.getBytes());

        final CombinedKeySchema<String, String> cks = new CombinedKeySchema<>(
            () -> FK_TOPIC, mockSerde,
            () -> PK_TOPIC, mockSerde
        );
        cks.init(mock(ProcessorContext.class));
        cks.toBytes(foreignKey, primaryKey, HEADERS);

        verify(mockSerializer).serialize(FK_TOPIC, HEADERS, foreignKey);
        verify(mockSerializer).serialize(PK_TOPIC, HEADERS, primaryKey);
        verify(mockSerializer, never()).serialize(FK_TOPIC, foreignKey);
        verify(mockSerializer, never()).serialize(PK_TOPIC, primaryKey);
    }

    @Test
    public void shouldPassHeadersToUnderlyingDeserializersOnFromBytes() {
        final Deserializer<String> mockDeserializer = mock(StringDeserializer.class);
        final Serde<String> mockSerde = mock(Serdes.StringSerde.class);
        when(mockSerde.serializer()).thenReturn(Serdes.String().serializer());
        when(mockSerde.deserializer()).thenReturn(mockDeserializer);

        final String foreignKey = "foreignKey";
        final String primaryKey = "primaryKey";
        when(mockDeserializer.deserialize(FK_TOPIC, HEADERS, foreignKey.getBytes())).thenReturn(foreignKey);
        when(mockDeserializer.deserialize(PK_TOPIC, HEADERS, primaryKey.getBytes())).thenReturn(primaryKey);

        final CombinedKeySchema<String, String> serializerCks = new CombinedKeySchema<>(
            () -> FK_TOPIC, Serdes.String(),
            () -> PK_TOPIC, Serdes.String()
        );
        final Bytes serialized = serializerCks.toBytes(foreignKey, primaryKey, HEADERS);

        final CombinedKeySchema<String, String> cks = new CombinedKeySchema<>(
            () -> FK_TOPIC, mockSerde,
            () -> PK_TOPIC, mockSerde
        );
        cks.init(mock(ProcessorContext.class));
        cks.fromBytes(serialized, HEADERS);

        final byte[] foreignKeyRaw = Serdes.String().serializer().serialize(null, HEADERS, foreignKey);
        final byte[] primaryKeyRaw = Serdes.String().serializer().serialize(null, HEADERS, primaryKey);

        verify(mockDeserializer).deserialize(FK_TOPIC, HEADERS, foreignKeyRaw);
        verify(mockDeserializer, never()).deserialize(FK_TOPIC, foreignKeyRaw);
        verify(mockDeserializer).deserialize(PK_TOPIC, HEADERS, primaryKeyRaw);
        verify(mockDeserializer, never()).deserialize(PK_TOPIC, primaryKeyRaw);
    }
}
