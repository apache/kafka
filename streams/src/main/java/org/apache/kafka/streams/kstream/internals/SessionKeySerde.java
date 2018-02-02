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
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Serde for a {@link Windowed} key when working with {@link org.apache.kafka.streams.kstream.SessionWindows}
 *
 * @param <K> sessionId type
 */
public class SessionKeySerde<K> implements Serde<Windowed<K>> {
    private static final int TIMESTAMP_SIZE = 8;

    private final Serde<K> keySerde;

    public SessionKeySerde(final Serde<K> keySerde) {
        this.keySerde = keySerde;
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<Windowed<K>> serializer() {
        return new SessionKeySerializer(keySerde.serializer());
    }

    @Override
    public Deserializer<Windowed<K>> deserializer() {
        return new SessionKeyDeserializer(keySerde.deserializer());
    }

    private class SessionKeySerializer implements Serializer<Windowed<K>> {

        private final Serializer<K> keySerializer;

        SessionKeySerializer(final Serializer<K> keySerializer) {
            this.keySerializer = keySerializer;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {

        }

        @Override
        public byte[] serialize(final String topic, final Windowed<K> data) {
            if (data == null) {
                return null;
            }
            return toBinary(data, keySerializer, topic).get();
        }

        @Override
        public void close() {

        }
    }

    private class SessionKeyDeserializer implements Deserializer<Windowed<K>> {
        private final Deserializer<K> deserializer;

        SessionKeyDeserializer(final Deserializer<K> deserializer) {
            this.deserializer = deserializer;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
        }

        @Override
        public Windowed<K> deserialize(final String topic, final byte[] data) {
            if (data == null || data.length == 0) {
                return null;
            }
            return from(data, deserializer, topic);
        }


        @Override
        public void close() {

        }
    }

    public static long extractEnd(final byte[] binaryKey) {
        return ByteBuffer.wrap(binaryKey).getLong(binaryKey.length - 2 * TIMESTAMP_SIZE);
    }

    public static long extractStart(final byte[] binaryKey) {
        return ByteBuffer.wrap(binaryKey).getLong(binaryKey.length - TIMESTAMP_SIZE);
    }

    public static Window extractWindow(final byte[] binaryKey) {
        final ByteBuffer buffer = ByteBuffer.wrap(binaryKey);
        final long start = buffer.getLong(binaryKey.length - TIMESTAMP_SIZE);
        final long end = buffer.getLong(binaryKey.length - 2 * TIMESTAMP_SIZE);
        return new SessionWindow(start, end);
    }

    public static byte[] extractKeyBytes(final byte[] binaryKey) {
        final byte[] bytes = new byte[binaryKey.length - 2 * TIMESTAMP_SIZE];
        System.arraycopy(binaryKey, 0, bytes, 0, bytes.length);
        return bytes;
    }

    public static <K> Windowed<K> from(final byte[] binaryKey, final Deserializer<K> keyDeserializer, final String topic) {
        final K key = extractKey(binaryKey, keyDeserializer, topic);
        final Window window = extractWindow(binaryKey);
        return new Windowed<>(key, window);
    }

    public static Windowed<Bytes> fromBytes(Bytes bytesKey) {
        final byte[] binaryKey = bytesKey.get();
        final ByteBuffer buffer = ByteBuffer.wrap(binaryKey);
        final long start = buffer.getLong(binaryKey.length - TIMESTAMP_SIZE);
        final long end = buffer.getLong(binaryKey.length - 2 * TIMESTAMP_SIZE);
        return new Windowed<>(Bytes.wrap(extractKeyBytes(binaryKey)), new SessionWindow(start, end));
    }

    private static <K> K extractKey(final byte[] binaryKey, final Deserializer<K> deserializer, final String topic) {
        return deserializer.deserialize(topic, extractKeyBytes(binaryKey));
    }

    public static <K> Bytes toBinary(final Windowed<K> sessionKey, final Serializer<K> serializer, final String topic) {
        final byte[] bytes = serializer.serialize(topic, sessionKey.key());
        ByteBuffer buf = ByteBuffer.allocate(bytes.length + 2 * TIMESTAMP_SIZE);
        buf.put(bytes);
        buf.putLong(sessionKey.window().end());
        buf.putLong(sessionKey.window().start());
        return new Bytes(buf.array());
    }

    public static Bytes bytesToBinary(final Windowed<Bytes> sessionKey) {
        final byte[] bytes = sessionKey.key().get();
        ByteBuffer buf = ByteBuffer.allocate(bytes.length + 2 * TIMESTAMP_SIZE);
        buf.put(bytes);
        buf.putLong(sessionKey.window().end());
        buf.putLong(sessionKey.window().start());
        return new Bytes(buf.array());
    }
}
