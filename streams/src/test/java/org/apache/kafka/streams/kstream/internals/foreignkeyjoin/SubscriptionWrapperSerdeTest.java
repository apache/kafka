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

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.internals.Murmur3;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@SuppressWarnings({"unchecked", "rawtypes"})
public class SubscriptionWrapperSerdeTest {

    @Test
    @SuppressWarnings("unchecked")
    public void shouldSerdeTest() {
        final String originalKey = "originalKey";
        final SubscriptionWrapperSerde swSerde = new SubscriptionWrapperSerde<>(() -> "pkTopic", Serdes.String());
        final long[] hashedValue = Murmur3.hash128(new byte[] {(byte) 0xFF, (byte) 0xAA, (byte) 0x00, (byte) 0x19});
        final SubscriptionWrapper wrapper = new SubscriptionWrapper<>(hashedValue, SubscriptionWrapper.Instruction.DELETE_KEY_AND_PROPAGATE, originalKey);
        final byte[] serialized = swSerde.serializer().serialize(null, wrapper);
        final SubscriptionWrapper deserialized = (SubscriptionWrapper) swSerde.deserializer().deserialize(null, serialized);

        assertEquals(SubscriptionWrapper.Instruction.DELETE_KEY_AND_PROPAGATE, deserialized.getInstruction());
        assertArrayEquals(hashedValue, deserialized.getHash());
        assertEquals(originalKey, deserialized.getPrimaryKey());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldSerdeNullHashTest() {
        final String originalKey = "originalKey";
        final SubscriptionWrapperSerde swSerde = new SubscriptionWrapperSerde<>(() -> "pkTopic", Serdes.String());
        final long[] hashedValue = null;
        final SubscriptionWrapper wrapper = new SubscriptionWrapper<>(hashedValue, SubscriptionWrapper.Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE, originalKey);
        final byte[] serialized = swSerde.serializer().serialize(null, wrapper);
        final SubscriptionWrapper deserialized = (SubscriptionWrapper) swSerde.deserializer().deserialize(null, serialized);

        assertEquals(SubscriptionWrapper.Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE, deserialized.getInstruction());
        assertArrayEquals(hashedValue, deserialized.getHash());
        assertEquals(originalKey, deserialized.getPrimaryKey());
    }

    @Test (expected = NullPointerException.class)
    @SuppressWarnings("unchecked")
    public void shouldThrowExceptionOnNullKeyTest() {
        final String originalKey = null;
        final SubscriptionWrapperSerde swSerde = new SubscriptionWrapperSerde<>(() -> "pkTopic", Serdes.String());
        final long[] hashedValue = Murmur3.hash128(new byte[] {(byte) 0xFF, (byte) 0xAA, (byte) 0x00, (byte) 0x19});
        final SubscriptionWrapper wrapper = new SubscriptionWrapper<>(hashedValue, SubscriptionWrapper.Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE, originalKey);
        swSerde.serializer().serialize(null, wrapper);
    }

    @Test (expected = NullPointerException.class)
    @SuppressWarnings("unchecked")
    public void shouldThrowExceptionOnNullInstructionTest() {
        final String originalKey = "originalKey";
        final SubscriptionWrapperSerde swSerde = new SubscriptionWrapperSerde<>(() -> "pkTopic", Serdes.String());
        final long[] hashedValue = Murmur3.hash128(new byte[] {(byte) 0xFF, (byte) 0xAA, (byte) 0x00, (byte) 0x19});
        final SubscriptionWrapper wrapper = new SubscriptionWrapper<>(hashedValue, null, originalKey);
        swSerde.serializer().serialize(null, wrapper);
    }

    @Test (expected = UnsupportedVersionException.class)
    public void shouldThrowExceptionOnUnsupportedVersionTest() {
        final String originalKey = "originalKey";
        final long[] hashedValue = null;
        new SubscriptionWrapper<>(hashedValue, SubscriptionWrapper.Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE, originalKey, (byte) 0x80);
    }
}
