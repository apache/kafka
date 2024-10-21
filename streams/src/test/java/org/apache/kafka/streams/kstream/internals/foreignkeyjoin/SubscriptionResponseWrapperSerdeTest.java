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
import org.apache.kafka.common.hash.Murmur3;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SubscriptionResponseWrapperSerdeTest {
    private static final class NonNullableSerde<T> implements Serde<T>, Serializer<T>, Deserializer<T> {
        private final Serde<T> delegate;

        NonNullableSerde(final Serde<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) { }

        @Override
        public void close() { }

        @Override
        public Serializer<T> serializer() {
            return this;
        }

        @Override
        public Deserializer<T> deserializer() {
            return this;
        }

        @Override
        public byte[] serialize(final String topic, final T data) {
            return delegate.serializer().serialize(topic, requireNonNull(data));
        }

        @Override
        public T deserialize(final String topic, final byte[] data) {
            return delegate.deserializer().deserialize(topic, requireNonNull(data));
        }
    }

    @Test
    public void ShouldSerdeWithNonNullsTest() {
        final long[] hashedValue = Murmur3.hash128(new byte[] {(byte) 0x01, (byte) 0x9A, (byte) 0xFF, (byte) 0x00});
        final String foreignValue = "foreignValue";
        final SubscriptionResponseWrapper<String> srw = new SubscriptionResponseWrapper<>(hashedValue, foreignValue, 1);
        try (final SubscriptionResponseWrapperSerde<String> srwSerde = new SubscriptionResponseWrapperSerde<>(new NonNullableSerde<>(Serdes.String()))) {
            final byte[] serResponse = srwSerde.serializer().serialize(null, srw);
            final SubscriptionResponseWrapper<String> result = srwSerde.deserializer().deserialize(null, serResponse);

            assertArrayEquals(hashedValue, result.originalValueHash());
            assertEquals(foreignValue, result.foreignValue());
            assertNull(result.primaryPartition());
        }
    }

    @Test
    public void shouldSerdeWithNullForeignValueTest() {
        final long[] hashedValue = Murmur3.hash128(new byte[] {(byte) 0x01, (byte) 0x9A, (byte) 0xFF, (byte) 0x00});
        final SubscriptionResponseWrapper<String> srw = new SubscriptionResponseWrapper<>(hashedValue, null, 1);
        try (final SubscriptionResponseWrapperSerde<String> srwSerde = new SubscriptionResponseWrapperSerde<>(new NonNullableSerde<>(Serdes.String()))) {
            final byte[] serResponse = srwSerde.serializer().serialize(null, srw);
            final SubscriptionResponseWrapper<String> result = srwSerde.deserializer().deserialize(null, serResponse);

            assertArrayEquals(hashedValue, result.originalValueHash());
            assertNull(result.foreignValue());
            assertNull(result.primaryPartition());
        }
    }

    @Test
    public void shouldSerdeWithNullHashTest() {
        final long[] hashedValue = null;
        final String foreignValue = "foreignValue";
        final SubscriptionResponseWrapper<String> srw = new SubscriptionResponseWrapper<>(hashedValue, foreignValue, 1);
        try (final SubscriptionResponseWrapperSerde<String> srwSerde = new SubscriptionResponseWrapperSerde<>(new NonNullableSerde<>(Serdes.String()))) {
            final byte[] serResponse = srwSerde.serializer().serialize(null, srw);
            final SubscriptionResponseWrapper<String> result = srwSerde.deserializer().deserialize(null, serResponse);

            assertArrayEquals(hashedValue, result.originalValueHash());
            assertEquals(foreignValue, result.foreignValue());
            assertNull(result.primaryPartition());
        }
    }

    @Test
    public void shouldSerdeWithNullsTest() {
        final long[] hashedValue = null;
        final String foreignValue = null;
        final SubscriptionResponseWrapper<String> srw = new SubscriptionResponseWrapper<>(hashedValue, foreignValue, 1);
        try (final SubscriptionResponseWrapperSerde<String> srwSerde = new SubscriptionResponseWrapperSerde<>(new NonNullableSerde<>(Serdes.String()))) {
            final byte[] serResponse = srwSerde.serializer().serialize(null, srw);
            final SubscriptionResponseWrapper<String> result = srwSerde.deserializer().deserialize(null, serResponse);

            assertArrayEquals(hashedValue, result.originalValueHash());
            assertEquals(foreignValue, result.foreignValue());
            assertNull(result.primaryPartition());
        }
    }

    @Test
    public void shouldThrowExceptionWithBadVersionTest() {
        final long[] hashedValue = null;
        assertThrows(
            UnsupportedVersionException.class,
            () -> new SubscriptionResponseWrapper<>(hashedValue, "foreignValue", (byte) -1, 1)
        );
    }

    @Test
    public void shouldThrowExceptionOnSerializeWhenDataVersionUnknown() {
        final SubscriptionResponseWrapper<String> srw = new InvalidSubscriptionResponseWrapper(null, null, 1);
        try (final SubscriptionResponseWrapperSerde<String> srwSerde = new SubscriptionResponseWrapperSerde<>(null)) {
            assertThrows(
                UnsupportedVersionException.class,
                () -> srwSerde.serializer().serialize(null, srw)
            );
        }
    }

    public static class InvalidSubscriptionResponseWrapper extends SubscriptionResponseWrapper<String> {

        public InvalidSubscriptionResponseWrapper(final long[] originalValueHash,
                                                  final String foreignValue,
                                                  final Integer primaryPartition) {
            super(originalValueHash, foreignValue, primaryPartition);
        }

        @Override
        public byte version() {
            return -1;
        }
    }
}