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

import java.util.Collections;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.internals.Murmur3;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

@SuppressWarnings({"unchecked", "rawtypes"})
public class SubscriptionWrapperSerdeTest {

    @Test
    @SuppressWarnings("unchecked")
    public void shouldSerdeV0Test() {
        final byte version = SubscriptionWrapper.VERSION_0;
        final String originalKey = "originalKey";
        final SubscriptionWrapperSerde swSerde = new SubscriptionWrapperSerde<>(() -> "pkTopic", Serdes.String());
        final long[] hashedValue = Murmur3.hash128(new byte[] {(byte) 0xFF, (byte) 0xAA, (byte) 0x00, (byte) 0x19});
        final Integer primaryPartition = null;
        final SubscriptionWrapper wrapper = new SubscriptionWrapper<>(
            hashedValue,
            SubscriptionWrapper.Instruction.DELETE_KEY_AND_PROPAGATE,
            originalKey,
            version,
            primaryPartition);
        final byte[] serialized = swSerde.serializer().serialize(null, wrapper);
        final SubscriptionWrapper deserialized = (SubscriptionWrapper) swSerde.deserializer()
            .deserialize(null, serialized);

        assertEquals(SubscriptionWrapper.Instruction.DELETE_KEY_AND_PROPAGATE, deserialized.getInstruction());
        assertArrayEquals(hashedValue, deserialized.getHash());
        assertEquals(originalKey, deserialized.getPrimaryKey());
        assertEquals(primaryPartition, deserialized.getPrimaryPartition());
        assertEquals(version, deserialized.getVersion());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldSerdeV1Test() {
        final byte version = SubscriptionWrapper.VERSION_1;
        final String originalKey = "originalKey";
        final SubscriptionWrapperSerde swSerde = new SubscriptionWrapperSerde<>(() -> "pkTopic", Serdes.String());
        final long[] hashedValue = Murmur3.hash128(new byte[] {(byte) 0xFF, (byte) 0xAA, (byte) 0x00, (byte) 0x19});
        final Integer primaryPartition = 10;
        final SubscriptionWrapper wrapper = new SubscriptionWrapper<>(
            hashedValue,
            SubscriptionWrapper.Instruction.DELETE_KEY_AND_PROPAGATE,
            originalKey,
            version,
            primaryPartition);
        final byte[] serialized = swSerde.serializer().serialize(null, wrapper);
        final SubscriptionWrapper deserialized = (SubscriptionWrapper) swSerde.deserializer()
            .deserialize(null, serialized);

        assertEquals(SubscriptionWrapper.Instruction.DELETE_KEY_AND_PROPAGATE, deserialized.getInstruction());
        assertArrayEquals(hashedValue, deserialized.getHash());
        assertEquals(originalKey, deserialized.getPrimaryKey());
        assertEquals(primaryPartition, deserialized.getPrimaryPartition());
        assertEquals(version, deserialized.getVersion());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldSerdeWithV0IfUpgradeTest() {
        final byte version = SubscriptionWrapper.VERSION_1;
        final String originalKey = "originalKey";
        final SubscriptionWrapperSerde swSerde = new SubscriptionWrapperSerde<>(() -> "pkTopic", Serdes.String());
        swSerde.configure(
            Collections.singletonMap(StreamsConfig.UPGRADE_FROM_CONFIG, StreamsConfig.UPGRADE_FROM_32),
            true);
        final long[] hashedValue = Murmur3.hash128(new byte[] {(byte) 0xFF, (byte) 0xAA, (byte) 0x00, (byte) 0x19});
        final Integer primaryPartition = 10;
        final SubscriptionWrapper wrapper = new SubscriptionWrapper<>(
            hashedValue,
            SubscriptionWrapper.Instruction.DELETE_KEY_AND_PROPAGATE,
            originalKey,
            version,
            primaryPartition);
        final byte[] serialized = swSerde.serializer().serialize(null, wrapper);
        final SubscriptionWrapper deserialized = (SubscriptionWrapper) swSerde.deserializer()
            .deserialize(null, serialized);

        assertEquals(SubscriptionWrapper.Instruction.DELETE_KEY_AND_PROPAGATE, deserialized.getInstruction());
        assertArrayEquals(hashedValue, deserialized.getHash());
        assertEquals(originalKey, deserialized.getPrimaryKey());
        assertEquals(0, deserialized.getVersion());
        assertNull(deserialized.getPrimaryPartition());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldSerdeNullHashV0Test() {
        final byte version = SubscriptionWrapper.VERSION_0;
        final String originalKey = "originalKey";
        final SubscriptionWrapperSerde swSerde = new SubscriptionWrapperSerde<>(() -> "pkTopic", Serdes.String());
        final long[] hashedValue = null;
        final Integer primaryPartition = null;
        final SubscriptionWrapper wrapper = new SubscriptionWrapper<>(
            hashedValue,
            SubscriptionWrapper.Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE,
            originalKey,
            version,
            primaryPartition);
        final byte[] serialized = swSerde.serializer().serialize(null, wrapper);
        final SubscriptionWrapper deserialized = (SubscriptionWrapper) swSerde.deserializer().deserialize(null, serialized);

        assertEquals(SubscriptionWrapper.Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE, deserialized.getInstruction());
        assertArrayEquals(hashedValue, deserialized.getHash());
        assertEquals(originalKey, deserialized.getPrimaryKey());
        assertEquals(primaryPartition, deserialized.getPrimaryPartition());
        assertEquals(version, deserialized.getVersion());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldSerdeNullHashV1Test() {
        final byte version = SubscriptionWrapper.VERSION_1;
        final String originalKey = "originalKey";
        final SubscriptionWrapperSerde swSerde = new SubscriptionWrapperSerde<>(() -> "pkTopic", Serdes.String());
        final long[] hashedValue = null;
        final Integer primaryPartition = 10;
        final SubscriptionWrapper wrapper = new SubscriptionWrapper<>(
            hashedValue,
            SubscriptionWrapper.Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE,
            originalKey,
            version,
            primaryPartition);
        final byte[] serialized = swSerde.serializer().serialize(null, wrapper);
        final SubscriptionWrapper deserialized = (SubscriptionWrapper) swSerde.deserializer()
            .deserialize(null, serialized);

        assertEquals(SubscriptionWrapper.Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE, deserialized.getInstruction());
        assertArrayEquals(hashedValue, deserialized.getHash());
        assertEquals(originalKey, deserialized.getPrimaryKey());
        assertEquals(primaryPartition, deserialized.getPrimaryPartition());
        assertEquals(version, deserialized.getVersion());
    }

    @Test
    public void shouldSerdeNullPrimaryPartitionOnV0Test() {
        final String originalKey = "originalKey";
        final SubscriptionWrapperSerde swSerde = new SubscriptionWrapperSerde<>(() -> "pkTopic", Serdes.String());
        final long[] hashedValue = Murmur3.hash128(new byte[] {(byte) 0xFF, (byte) 0xAA, (byte) 0x00, (byte) 0x19});
        final Integer primaryPartition = null;
        final byte version = SubscriptionWrapper.VERSION_0;
        final SubscriptionWrapper wrapper = new SubscriptionWrapper<>(
            hashedValue,
            SubscriptionWrapper.Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE,
            originalKey,
            version,
            primaryPartition);
        final byte[] serialized = swSerde.serializer().serialize(null, wrapper);
        final SubscriptionWrapper deserialized = (SubscriptionWrapper) swSerde.deserializer().deserialize(null, serialized);

        assertEquals(SubscriptionWrapper.Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE, deserialized.getInstruction());
        assertArrayEquals(hashedValue, deserialized.getHash());
        assertEquals(originalKey, deserialized.getPrimaryKey());
        assertEquals(primaryPartition, deserialized.getPrimaryPartition());
        assertEquals(version, deserialized.getVersion());
    }

    @Test
    public void shouldThrowExceptionOnNullKeyV0Test() {
        final String originalKey = null;
        final long[] hashedValue = Murmur3.hash128(new byte[] {(byte) 0xFF, (byte) 0xAA, (byte) 0x00, (byte) 0x19});
        final Integer primaryPartition = 10;
        assertThrows(NullPointerException.class, () -> new SubscriptionWrapper<>(hashedValue,
            SubscriptionWrapper.Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE,
            originalKey,
            SubscriptionWrapper.VERSION_0,
            primaryPartition));
    }

    @Test
    public void shouldThrowExceptionOnNullKeyV1Test() {
        final String originalKey = null;
        final long[] hashedValue = Murmur3.hash128(new byte[] {(byte) 0xFF, (byte) 0xAA, (byte) 0x00, (byte) 0x19});
        final Integer primaryPartition = 10;
        assertThrows(NullPointerException.class, () -> new SubscriptionWrapper<>(hashedValue,
            SubscriptionWrapper.Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE,
            originalKey,
            SubscriptionWrapper.VERSION_1,
            primaryPartition));
    }

    @Test
    public void shouldThrowExceptionOnNullInstructionV0Test() {
        final String originalKey = "originalKey";
        final long[] hashedValue = Murmur3.hash128(new byte[] {(byte) 0xFF, (byte) 0xAA, (byte) 0x00, (byte) 0x19});
        final Integer primaryPartition = 10;
        assertThrows(NullPointerException.class, () -> new SubscriptionWrapper<>(
            hashedValue,
            null,
            originalKey,
            SubscriptionWrapper.VERSION_0,
            primaryPartition));
    }

    @Test
    public void shouldThrowExceptionOnNullInstructionV1Test() {
        final String originalKey = "originalKey";
        final long[] hashedValue = Murmur3.hash128(new byte[] {(byte) 0xFF, (byte) 0xAA, (byte) 0x00, (byte) 0x19});
        final Integer primaryPartition = 10;
        assertThrows(NullPointerException.class, () -> new SubscriptionWrapper<>(
            hashedValue,
            null,
            originalKey,
            SubscriptionWrapper.VERSION_0,
            primaryPartition));
    }

    @Test
    public void shouldThrowExceptionOnNullPrimaryPartitionV1Test() {
        final SubscriptionWrapperSerde swSerde = new SubscriptionWrapperSerde<>(() -> "pkTopic", Serdes.String());
        final String originalKey = "originalKey";
        final long[] hashedValue = Murmur3.hash128(new byte[] {(byte) 0xFF, (byte) 0xAA, (byte) 0x00, (byte) 0x19});
        final Integer primaryPartition = null;
        final SubscriptionWrapper wrapper = new SubscriptionWrapper<>(
            hashedValue,
            SubscriptionWrapper.Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE,
            originalKey,
            SubscriptionWrapper.VERSION_1,
            primaryPartition);
        assertThrows(NullPointerException.class, () -> swSerde.serializer().serialize(null, wrapper));
    }

    @Test (expected = UnsupportedVersionException.class)
    public void shouldThrowExceptionOnUnsupportedVersionTest() {
        final String originalKey = "originalKey";
        final long[] hashedValue = null;
        final Integer primaryPartition = 10;
        new SubscriptionWrapper<>(
            hashedValue,
            SubscriptionWrapper.Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE,
            originalKey,
            (byte) 0x80,
            primaryPartition);
    }
}
