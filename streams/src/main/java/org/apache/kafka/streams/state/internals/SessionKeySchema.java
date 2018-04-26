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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.state.KeyValueIterator;

import org.rocksdb.NativeComparatorWrapper;

import java.nio.ByteBuffer;
import java.util.List;


public class SessionKeySchema implements SegmentedBytesStore.KeySchema {

    private static final int TIMESTAMP_SIZE = 8;
    private static final int SUFFIX_SIZE = 2 * TIMESTAMP_SIZE;

    private static final SessionKeyBytesComparator COMPARATOR = new SessionKeyBytesComparator();

    private String topic;
    private final Serde<Bytes> bytesSerdes = Serdes.Bytes();

    @Override
    public void init(final String topic) {
        this.topic = topic;
    }

    @Override
    public Bytes upperRange(final Bytes key, final long latestSessionStartTime) {
        final Windowed<Bytes> sessionKey = new Windowed<>(key, new SessionWindow(latestSessionStartTime, Long.MAX_VALUE));
        return Bytes.wrap(SessionKeySchema.toBinary(sessionKey, bytesSerdes.serializer(), topic));
    }

    @Override
    public Bytes lowerRange(final Bytes key, final long earliestSessionEndTime) {
        final Windowed<Bytes> sessionKey = new Windowed<>(key, new SessionWindow(0, Math.max(0, earliestSessionEndTime)));
        return Bytes.wrap(SessionKeySchema.toBinary(sessionKey, bytesSerdes.serializer(), topic));
    }

    @Override
    public long segmentTimestamp(final Bytes key) {
        return SessionKeySchema.extractEndTimestamp(key.get());
    }

    @Override
    public HasNextCondition hasNextCondition(final Bytes binaryKeyFrom, final Bytes binaryKeyTo, final long from, final long to) {
        return new HasNextCondition() {
            @Override
            public boolean hasNext(final KeyValueIterator<Bytes, ?> iterator) {
                while (iterator.hasNext()) {
                    final Bytes bytes = iterator.peekNextKey();
                    final Windowed<Bytes> windowedKey = SessionKeySchema.from(bytes);
                    if ((binaryKeyFrom == null || windowedKey.key().compareTo(binaryKeyFrom) >= 0)
                        && (binaryKeyTo == null || windowedKey.key().compareTo(binaryKeyTo) <= 0)
                        && windowedKey.window().end() >= from
                        && windowedKey.window().start() <= to) {
                        return true;
                    }
                    iterator.next();
                }
                return false;
            }
        };
    }

    @Override
    public List<Segment> segmentsToSearch(final Segments segments,
                                          final long from,
                                          final long to) {
        return segments.segments(from, Long.MAX_VALUE);
    }

    @Override
    public Bytes.ByteArrayComparator bytesComparator() {
        return COMPARATOR;
    }

    private static <K> K extractKey(final byte[] binaryKey,
                                    final Deserializer<K> deserializer,
                                    final String topic) {
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

    public static <K> Windowed<K> from(final byte[] binaryKey,
                                       final Deserializer<K> keyDeserializer,
                                       final String topic) {
        final K key = extractKey(binaryKey, keyDeserializer, topic);
        final Window window = extractWindow(binaryKey);
        return new Windowed<>(key, window);
    }

    public static Windowed<Bytes> from(final Bytes bytesKey) {
        final byte[] binaryKey = bytesKey.get();
        final Window window = extractWindow(binaryKey);
        return new Windowed<>(Bytes.wrap(extractKeyBytes(binaryKey)), window);
    }

    public static <K> byte[] toBinary(final Windowed<K> sessionKey,
                                      final Serializer<K> serializer,
                                      final String topic) {
        final byte[] bytes = serializer.serialize(topic, sessionKey.key());
        final ByteBuffer buf = ByteBuffer.allocate(bytes.length + 2 * TIMESTAMP_SIZE);
        buf.put(bytes);
        buf.putLong(sessionKey.window().end());
        buf.putLong(sessionKey.window().start());
        return buf.array();
    }

    public static byte[] toBinary(final Windowed<Bytes> sessionKey) {
        final byte[] bytes = sessionKey.key().get();
        final ByteBuffer buf = ByteBuffer.allocate(bytes.length + 2 * TIMESTAMP_SIZE);
        buf.put(bytes);
        buf.putLong(sessionKey.window().end());
        buf.putLong(sessionKey.window().start());
        return buf.array();
    }

    private static class SessionKeyBytesComparator extends Bytes.LexicographicByteArrayComparator {

        @Override
        public int compare(byte[] buffer1, byte[] buffer2) {
            final int retOnKey = compare(buffer1, 0, buffer1.length - SUFFIX_SIZE,
                                         buffer2, 0, buffer2.length - SUFFIX_SIZE);

            if (retOnKey == 0) {
                // if the key is the same, compare the suffix
                return compare(buffer1, buffer1.length - SUFFIX_SIZE, SUFFIX_SIZE,
                               buffer2, buffer2.length - SUFFIX_SIZE, SUFFIX_SIZE);
            } else {
                return retOnKey;
            }
        }
    }

    public static class NativeSessionKeyBytesComparatorWrapper extends NativeComparatorWrapper {

        @Override
        protected long initializeNative(final long... nativeParameterHandles) {
            return newComparator();
        }

        private native long newComparator();
    }
}
