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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.StateSerdes;

import java.nio.ByteBuffer;


public class WindowedSerdes {

    static public class TimeWindowedSerde<T> extends Serdes.WrapperSerde<Windowed<T>> {
        public static final int SEQNUM_SIZE = 4;
        public static final int TIMESTAMP_SIZE = 8;

        // Default constructor needed by Kafka
        public TimeWindowedSerde() {
            super(new TimeWindowedSerializer<T>(), new TimeWindowedDeserializer<T>());
        }

        public TimeWindowedSerde(final Serde<T> inner) {
            super(new TimeWindowedSerializer<>(inner.serializer()), new TimeWindowedDeserializer<>(inner.deserializer()));
        }

        /**
         * Safely construct a time window of the given size,
         * taking care of bounding endMs to Long.MAX_VALUE if necessary
         */
        public static TimeWindow timeWindowForSize(final long startMs, final long windowSize) {
            final long endMs = startMs + windowSize;
            return new TimeWindow(startMs, endMs < 0 ? Long.MAX_VALUE : endMs);
        }

        // for pipe serdes
        public static  byte[] toBinary(final Windowed<Bytes> timeKey) {
            final byte[] bytes = timeKey.key().get();
            ByteBuffer buf = ByteBuffer.allocate(bytes.length + TIMESTAMP_SIZE);
            buf.put(bytes);
            buf.putLong(timeKey.window().startMs);

            return buf.array();
        }

        public static <K> byte[] toBinary(final Windowed<K> timeKey, final Serializer<K> serializer, final String topic) {
            final byte[] bytes = serializer.serialize(topic, timeKey.key());
            ByteBuffer buf = ByteBuffer.allocate(bytes.length + TIMESTAMP_SIZE);
            buf.put(bytes);
            buf.putLong(timeKey.window().startMs);

            return buf.array();
        }

        public static <K> Windowed<K> from(final byte[] binaryKey, final long windowSize, final Deserializer<K> deserializer, final String topic) {
            final byte[] bytes = new byte[binaryKey.length - TIMESTAMP_SIZE];
            System.arraycopy(binaryKey, 0, bytes, 0, bytes.length);
            final K key = deserializer.deserialize(topic, bytes);
            final Window window = extractWindow(binaryKey, windowSize);
            return new Windowed<>(key, window);
        }

        private static Window extractWindow(final byte[] binaryKey, final long windowSize) {
            final ByteBuffer buffer = ByteBuffer.wrap(binaryKey);
            final long start = buffer.getLong(binaryKey.length - TIMESTAMP_SIZE);
            return timeWindowForSize(start, windowSize);
        }

        // for store serdes

        public static <K> Bytes toStoreKeyBinary(final Windowed<K> timeKey, final int seqnum, final Serializer<K> serializer, final String topic) {
            return toStoreKeyBinary(timeKey.key(), timeKey.window().start(), seqnum, serializer, topic);
        }

        public static <K> Bytes toStoreKeyBinary(final K key, final long timestamp, final int seqnum, final Serializer<K> serializer, final String topic) {
            final byte[] bytes = serializer.serialize(topic, key);
            return toStoreKeyBinary(bytes, timestamp, seqnum);
        }

        public static Bytes toStoreKeyBinary(Bytes key, final long timestamp, final int seqnum) {
            byte[] serializedKey = key.get();
            return toStoreKeyBinary(serializedKey, timestamp, seqnum);
        }

        public static <K> Bytes toStoreKeyBinary(K key, final long timestamp, final int seqnum, final StateSerdes<K, ?> serdes) {
            byte[] serializedKey = serdes.rawKey(key);
            return toStoreKeyBinary(serializedKey, timestamp, seqnum);
        }

        public static Bytes toStoreKeyBinary(byte[] serializedKey, final long timestamp, final int seqnum) {
            ByteBuffer buf = ByteBuffer.allocate(serializedKey.length + TIMESTAMP_SIZE + SEQNUM_SIZE);
            buf.put(serializedKey);
            buf.putLong(timestamp);
            buf.putInt(seqnum);

            return Bytes.wrap(buf.array());
        }

        public static byte[] extractStoreKeyBytes(final byte[] binaryKey) {
            final byte[] bytes = new byte[binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE];
            System.arraycopy(binaryKey, 0, bytes, 0, bytes.length);
            return bytes;
        }

        public static <K> K extractStoreKey(final byte[] binaryKey, final StateSerdes<K, ?> serdes) {
            final byte[] bytes = new byte[binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE];
            System.arraycopy(binaryKey, 0, bytes, 0, bytes.length);
            return serdes.keyFrom(bytes);
        }

        public static long extractStoreTimestamp(byte[] binaryKey) {
            return ByteBuffer.wrap(binaryKey).getLong(binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE);
        }

        public static int extractStoreSequence(byte[] binaryKey) {
            return ByteBuffer.wrap(binaryKey).getInt(binaryKey.length - SEQNUM_SIZE);
        }

        public static <K> Windowed<K> fromStoreKey(final byte[] binaryKey, final long windowSize, final StateSerdes<K, ?> serdes) {
            final K key = serdes.keyDeserializer().deserialize(serdes.topic(), extractStoreKeyBytes(binaryKey));
            final Window window = extractStoreWindow(binaryKey, windowSize);
            return new Windowed<>(key, window);
        }

        public static Windowed<Bytes> fromStoreKey(final byte[] binaryKey, final long windowSize) {
            final Bytes key = Bytes.wrap(extractStoreKeyBytes(binaryKey));
            final Window window = extractStoreWindow(binaryKey, windowSize);
            return new Windowed<>(key, window);
        }

        private static Window extractStoreWindow(final byte[] binaryKey, final long windowSize) {
            final ByteBuffer buffer = ByteBuffer.wrap(binaryKey);
            final long start = buffer.getLong(binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE);
            return timeWindowForSize(start, windowSize);
        }
    }

    static public class SessionWindowedSerde<T> extends Serdes.WrapperSerde<Windowed<T>> {
        public static final int TIMESTAMP_SIZE = 8;

        // Default constructor needed by Kafka
        public SessionWindowedSerde() {
            super(new SessionWindowedSerializer<T>(), new SessionWindowedDeserializer<T>());
        }

        public SessionWindowedSerde(final Serde<T> inner) {
            super(new SessionWindowedSerializer<>(inner.serializer()), new SessionWindowedDeserializer<>(inner.deserializer()));
        }

        private static <K> K extractKey(final byte[] binaryKey, final Deserializer<K> deserializer, final String topic) {
            return deserializer.deserialize(topic, extractKeyBytes(binaryKey));
        }

        public static byte[] extractKeyBytes(final byte[] binaryKey) {
            final byte[] bytes = new byte[binaryKey.length - 2 * TIMESTAMP_SIZE];
            System.arraycopy(binaryKey, 0, bytes, 0, bytes.length);
            return bytes;
        }

        public static long extractEndTimestamp(final byte[] binaryKey) {
            return ByteBuffer.wrap(binaryKey).getLong(binaryKey.length - 2 * TIMESTAMP_SIZE);
        }

        public static long extractStartTimestamp(final byte[] binaryKey) {
            return ByteBuffer.wrap(binaryKey).getLong(binaryKey.length - TIMESTAMP_SIZE);
        }

        public static Window extractWindow(final byte[] binaryKey) {
            final ByteBuffer buffer = ByteBuffer.wrap(binaryKey);
            final long start = buffer.getLong(binaryKey.length - TIMESTAMP_SIZE);
            final long end = buffer.getLong(binaryKey.length - 2 * TIMESTAMP_SIZE);
            return new SessionWindow(start, end);
        }

        public static <K> Windowed<K> from(final byte[] binaryKey, final Deserializer<K> keyDeserializer, final String topic) {
            final K key = extractKey(binaryKey, keyDeserializer, topic);
            final Window window = extractWindow(binaryKey);
            return new Windowed<>(key, window);
        }

        public static Windowed<Bytes> from(final Bytes bytesKey) {
            final byte[] binaryKey = bytesKey.get();
            final Window window = extractWindow(binaryKey);
            return new Windowed<>(Bytes.wrap(extractKeyBytes(binaryKey)), window);
        }

        public static <K> byte[] toBinary(final Windowed<K> sessionKey, final Serializer<K> serializer, final String topic) {
            final byte[] bytes = serializer.serialize(topic, sessionKey.key());
            ByteBuffer buf = ByteBuffer.allocate(bytes.length + 2 * TIMESTAMP_SIZE);
            buf.put(bytes);
            buf.putLong(sessionKey.window().end());
            buf.putLong(sessionKey.window().start());
            return buf.array();
        }

        public static byte[] toBinary(final Windowed<Bytes> sessionKey) {
            final byte[] bytes = sessionKey.key().get();
            ByteBuffer buf = ByteBuffer.allocate(bytes.length + 2 * TIMESTAMP_SIZE);
            buf.put(bytes);
            buf.putLong(sessionKey.window().end());
            buf.putLong(sessionKey.window().start());
            return buf.array();
        }
    }

    static public <T> Serde<Windowed<T>> timeWindowedSerdeFrom(final Class<T> type) {
        return new TimeWindowedSerde<>(Serdes.serdeFrom(type));
    }

    static public <T> Serde<Windowed<T>> sessionWindowedSerdeFrom(final Class<T> type) {
        return new TimeWindowedSerde<>(Serdes.serdeFrom(type));
    }
}