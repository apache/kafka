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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.state.internals.Murmur3;
import org.junit.Test;

import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class SubscriptionResponseWrapperSerdeTest {
    private static final class NonNullableSerde<T> implements Serde<T>, Serializer<T>, Deserializer<T> {
        private final Serde<T> delegate;

        NonNullableSerde(final Serde<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {

        }

        @Override
        public void close() {

        }

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
    @SuppressWarnings("unchecked")
    public void ShouldSerdeWithNonNullsTest() {
        final long[] hashedValue = Murmur3.hash128(new byte[] {(byte) 0x01, (byte) 0x9A, (byte) 0xFF, (byte) 0x00});
        final String foreignValue = "foreignValue";
        final SubscriptionResponseWrapper<String> srw = new SubscriptionResponseWrapper<>(hashedValue, foreignValue);
        final SubscriptionResponseWrapperSerde<String> srwSerde = new SubscriptionResponseWrapperSerde(new NonNullableSerde(Serdes.String()));
        final byte[] serResponse = srwSerde.serializer().serialize(null, srw);
        final SubscriptionResponseWrapper<String> result = srwSerde.deserializer().deserialize(null, serResponse);

        assertArrayEquals(hashedValue, result.getOriginalValueHash());
        assertEquals(foreignValue, result.getForeignValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldSerdeWithNullForeignValueTest() {
        final long[] hashedValue = Murmur3.hash128(new byte[] {(byte) 0x01, (byte) 0x9A, (byte) 0xFF, (byte) 0x00});
        final SubscriptionResponseWrapper<String> srw = new SubscriptionResponseWrapper<>(hashedValue, null);
        final SubscriptionResponseWrapperSerde<String> srwSerde = new SubscriptionResponseWrapperSerde(new NonNullableSerde(Serdes.String()));
        final byte[] serResponse = srwSerde.serializer().serialize(null, srw);
        final SubscriptionResponseWrapper<String> result = srwSerde.deserializer().deserialize(null, serResponse);

        assertArrayEquals(hashedValue, result.getOriginalValueHash());
        assertNull(result.getForeignValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldSerdeWithNullHashTest() {
        final long[] hashedValue = null;
        final String foreignValue = "foreignValue";
        final SubscriptionResponseWrapper<String> srw = new SubscriptionResponseWrapper<>(hashedValue, foreignValue);
        final SubscriptionResponseWrapperSerde<String> srwSerde = new SubscriptionResponseWrapperSerde(new NonNullableSerde(Serdes.String()));
        final byte[] serResponse = srwSerde.serializer().serialize(null, srw);
        final SubscriptionResponseWrapper<String> result = srwSerde.deserializer().deserialize(null, serResponse);

        assertArrayEquals(hashedValue, result.getOriginalValueHash());
        assertEquals(foreignValue, result.getForeignValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldSerdeWithNullsTest() {
        final long[] hashedValue = null;
        final String foreignValue = null;
        final SubscriptionResponseWrapper<String> srw = new SubscriptionResponseWrapper<>(hashedValue, foreignValue);
        final SubscriptionResponseWrapperSerde<String> srwSerde = new SubscriptionResponseWrapperSerde(new NonNullableSerde(Serdes.String()));
        final byte[] serResponse = srwSerde.serializer().serialize(null, srw);
        final SubscriptionResponseWrapper<String> result = srwSerde.deserializer().deserialize(null, serResponse);

        assertArrayEquals(hashedValue, result.getOriginalValueHash());
        assertEquals(foreignValue, result.getForeignValue());
    }

    @Test
    public void shouldThrowExceptionWithBadVersionTest() {
        final long[] hashedValue = null;
        assertThrows(UnsupportedVersionException.class,
            () -> new SubscriptionResponseWrapper<>(hashedValue, "foreignValue", (byte) 0xFF));
    }
}
