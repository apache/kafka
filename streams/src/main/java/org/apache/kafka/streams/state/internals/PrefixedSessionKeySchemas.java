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
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.state.internals.SegmentedBytesStore.KeySchema;

import static org.apache.kafka.streams.state.StateSerdes.TIMESTAMP_SIZE;

public class PrefixedSessionKeySchemas {

    private static final int PREFIX_SIZE = 1;
    private static final byte TIME_FIRST_PREFIX = 0;
    private static final byte KEY_FIRST_PREFIX = 1;

    private static byte extractPrefix(final byte[] binaryBytes) {
        return binaryBytes[0];
    }

    public static class TimeFirstSessionKeySchema implements KeySchema {

        @Override
        public Bytes upperRange(final Bytes key, final long to) {
            if (key == null) {
                // Put next prefix instead of null so that we can start from right prefix
                // when scanning backwards
                final byte nextPrefix = TIME_FIRST_PREFIX + 1;
                return Bytes.wrap(ByteBuffer.allocate(PREFIX_SIZE).put(nextPrefix).array());
            }
            return Bytes.wrap(ByteBuffer.allocate(PREFIX_SIZE + 2 * TIMESTAMP_SIZE + key.get().length)
                .put(TIME_FIRST_PREFIX)
                // the end timestamp can be as large as possible as long as it's larger than start time
                .putLong(Long.MAX_VALUE)
                // this is the start timestamp
                .putLong(to)
                .put(key.get())
                .array());
        }

        @Override
        public Bytes lowerRange(final Bytes key, final long from) {
            if (key == null) {
                return Bytes.wrap(ByteBuffer.allocate(PREFIX_SIZE + TIMESTAMP_SIZE)
                    .put(TIME_FIRST_PREFIX)
                    .putLong(from)
                    .array());
            }

            return Bytes.wrap(ByteBuffer.allocate(PREFIX_SIZE + 2 * TIMESTAMP_SIZE + key.get().length)
                .put(TIME_FIRST_PREFIX)
                .putLong(from)
                .putLong(0L)
                .put(key.get())
                .array());
        }

        /**
         * @param key the key in the range
         * @param to the latest start time
         */
        @Override
        public Bytes upperRangeFixedSize(final Bytes key, final long to) {
            return toBinary(key, to, Long.MAX_VALUE);
        }

        /**
         * @param key the key in the range
         * @param from the earliest end timestamp in the range
         */
        @Override
        public Bytes lowerRangeFixedSize(final Bytes key, final long from) {
            return toBinary(key, 0, Math.max(0, from));
        }

        @Override
        public long segmentTimestamp(final Bytes key) {
            return extractEndTimestamp(key.get());
        }

        @Override
        public HasNextCondition hasNextCondition(final Bytes binaryKeyFrom,
                                                 final Bytes binaryKeyTo,
                                                 final long earliestWindowEndTime,
                                                 final long latestWindowStartTime,
                                                 final boolean forward) {
            return iterator -> {
                while (iterator.hasNext()) {
                    final Bytes bytes = iterator.peekNextKey();
                    final byte prefix = extractPrefix(bytes.get());

                    if (prefix != TIME_FIRST_PREFIX) {
                        return false;
                    }

                    final Windowed<Bytes> windowedKey = from(bytes);
                    final long endTime = windowedKey.window().end();
                    final long startTime = windowedKey.window().start();

                    // We can return false directly here since keys are sorted by end time and if
                    // we get time smaller than `from`, there won't be time within range.
                    if (!forward && endTime < earliestWindowEndTime) {
                        return false;
                    }

                    if ((binaryKeyFrom == null || windowedKey.key().compareTo(binaryKeyFrom) >= 0)
                        && (binaryKeyTo == null || windowedKey.key().compareTo(binaryKeyTo) <= 0)
                        && endTime >= earliestWindowEndTime && startTime <= latestWindowStartTime) {
                        return true;
                    }
                    iterator.next();
                }
                return false;
            };
        }

        @Override
        public <S extends Segment> List<S> segmentsToSearch(final Segments<S> segments,
                                                            final long earliestWindowEndTime,
                                                            final long latestWindowStartTime,
                                                            final boolean forward) {
            return segments.segments(earliestWindowEndTime, Long.MAX_VALUE, forward);
        }

        static long extractStartTimestamp(final byte[] binaryKey) {
            return ByteBuffer.wrap(binaryKey).getLong(PREFIX_SIZE + TIMESTAMP_SIZE);
        }

        static long extractEndTimestamp(final byte[] binaryKey) {
            return ByteBuffer.wrap(binaryKey).getLong(PREFIX_SIZE);
        }

        private static <K> K extractKey(final byte[] binaryKey,
                                        final Deserializer<K> deserializer,
                                        final String topic) {
            return deserializer.deserialize(topic, extractKeyBytes(binaryKey));
        }

        static byte[] extractKeyBytes(final byte[] binaryKey) {
            final byte[] bytes = new byte[binaryKey.length - 2 * TIMESTAMP_SIZE - PREFIX_SIZE];
            System.arraycopy(binaryKey, PREFIX_SIZE + 2 * TIMESTAMP_SIZE, bytes, 0, bytes.length);
            return bytes;
        }

        static Window extractWindow(final byte[] binaryKey) {
            final ByteBuffer buffer = ByteBuffer.wrap(binaryKey);
            final long start = buffer.getLong(PREFIX_SIZE + TIMESTAMP_SIZE);
            final long end = buffer.getLong(PREFIX_SIZE);
            return new SessionWindow(start, end);
        }

        public static Windowed<Bytes> from(final Bytes bytesKey) {
            final byte[] binaryKey = bytesKey.get();
            final Window window = extractWindow(binaryKey);
            return new Windowed<>(Bytes.wrap(extractKeyBytes(binaryKey)), window);
        }

        public static <K> Windowed<K> from(final byte[] binaryKey,
                                           final Deserializer<K> keyDeserializer,
                                           final String topic) {
            final K key = extractKey(binaryKey, keyDeserializer, topic);
            final Window window = extractWindow(binaryKey);
            return new Windowed<>(key, window);
        }

        public static <K> byte[] toBinary(final Windowed<K> sessionKey,
                                          final Serializer<K> serializer,
                                          final String topic) {
            final byte[] bytes = serializer.serialize(topic, sessionKey.key());
            return toBinary(Bytes.wrap(bytes), sessionKey.window().start(), sessionKey.window().end()).get();
        }

        public static Bytes toBinary(final Windowed<Bytes> sessionKey) {
            return toBinary(sessionKey.key(), sessionKey.window().start(), sessionKey.window().end());
        }

        // for time prefixed schema, like the session key schema we need to put time stamps first, then the key
        // and hence we need to override the write binary function with the write reordering
        public static void writeBinary(final ByteBuffer buf,
                                       final Bytes key,
                                       final long startTime,
                                       final long endTime) {
            buf.putLong(endTime);
            buf.putLong(startTime);
            if (key != null) {
                buf.put(key.get());
            }
        }

        public static Bytes toBinary(final Bytes key,
                                     final long startTime,
                                     final long endTime) {
            final ByteBuffer buf = ByteBuffer.allocate(PREFIX_SIZE + SessionKeySchema.keyByteLength(key));
            buf.put(TIME_FIRST_PREFIX);
            writeBinary(buf, key, startTime, endTime);
            return Bytes.wrap(buf.array());
        }

        public static byte[] extractWindowBytesFromNonPrefixSessionKey(final byte[] binaryKey) {
            final ByteBuffer buffer = ByteBuffer.allocate(PREFIX_SIZE + binaryKey.length).put(TIME_FIRST_PREFIX);
            // Put timestamp
            buffer.put(binaryKey, binaryKey.length - 2 * TIMESTAMP_SIZE, 2 * TIMESTAMP_SIZE);
            buffer.put(binaryKey, 0, binaryKey.length - 2 * TIMESTAMP_SIZE);

            return buffer.array();
        }
    }

    public static class KeyFirstSessionKeySchema implements KeySchema {

        @Override
        public Bytes upperRange(final Bytes key, final long to) {
            final Bytes noPrefixBytes = new SessionKeySchema().upperRange(key, to);
            return wrapPrefix(noPrefixBytes, true);
        }

        @Override
        public Bytes lowerRange(final Bytes key, final long from) {
            final Bytes noPrefixBytes = new SessionKeySchema().lowerRange(key, from);
            // Wrap at least prefix even key is null
            return wrapPrefix(noPrefixBytes, false);
        }

        @Override
        public Bytes upperRangeFixedSize(final Bytes key, final long to) {
            final ByteBuffer buffer = ByteBuffer.allocate(PREFIX_SIZE + SessionKeySchema.keyByteLength(key));
            buffer.put(KEY_FIRST_PREFIX);
            SessionKeySchema.writeBinary(buffer, SessionKeySchema.upperRangeFixedWindow(key, to));
            return Bytes.wrap(buffer.array());
        }

        @Override
        public Bytes lowerRangeFixedSize(final Bytes key, final long from) {
            final ByteBuffer buffer = ByteBuffer.allocate(PREFIX_SIZE + SessionKeySchema.keyByteLength(key));
            buffer.put(KEY_FIRST_PREFIX);
            SessionKeySchema.writeBinary(buffer, SessionKeySchema.lowerRangeFixedWindow(key, from));
            return Bytes.wrap(buffer.array());
        }

        @Override
        public long segmentTimestamp(final Bytes key) {
            return extractEndTimestamp(key.get());
        }

        @Override
        public HasNextCondition hasNextCondition(final Bytes binaryKeyFrom,
                                                 final Bytes binaryKeyTo,
                                                 final long from,
                                                 final long to,
                                                 final boolean forward) {
            return iterator -> {
                while (iterator.hasNext()) {
                    final Bytes bytes = iterator.peekNextKey();
                    final byte prefix = extractPrefix(bytes.get());

                    if (prefix != KEY_FIRST_PREFIX) {
                        return false;
                    }

                    final Windowed<Bytes> windowedKey = from(bytes);
                    final long endTime = windowedKey.window().end();
                    final long startTime = windowedKey.window().start();

                    if ((binaryKeyFrom == null || windowedKey.key().compareTo(binaryKeyFrom) >= 0)
                        && (binaryKeyTo == null || windowedKey.key().compareTo(binaryKeyTo) <= 0)
                        && endTime >= from
                        && startTime <= to) {
                        return true;
                    }
                    iterator.next();
                }
                return false;
            };
        }

        @Override
        public <S extends Segment> List<S> segmentsToSearch(final Segments<S> segments,
                                                            final long from,
                                                            final long to,
                                                            final boolean forward) {
            return segments.segments(from, Long.MAX_VALUE, forward);
        }

        static Window extractWindow(final byte[] binaryKey) {
            final ByteBuffer buffer = ByteBuffer.wrap(binaryKey);
            final long start = buffer.getLong(binaryKey.length - TIMESTAMP_SIZE);
            final long end = buffer.getLong(binaryKey.length - 2 * TIMESTAMP_SIZE);
            return new SessionWindow(start, end);
        }

        static byte[] extractKeyBytes(final byte[] binaryKey) {
            final byte[] bytes = new byte[binaryKey.length - 2 * TIMESTAMP_SIZE - PREFIX_SIZE];
            System.arraycopy(binaryKey, PREFIX_SIZE, bytes, 0, bytes.length);
            return bytes;
        }

        public static Windowed<Bytes> from(final Bytes bytesKey) {
            final byte[] binaryKey = bytesKey.get();
            final Window window = extractWindow(binaryKey);
            return new Windowed<>(Bytes.wrap(extractKeyBytes(binaryKey)), window);
        }

        private static <K> K extractKey(final byte[] binaryKey,
                                        final Deserializer<K> deserializer,
                                        final String topic) {
            return deserializer.deserialize(topic, extractKeyBytes(binaryKey));
        }

        public static <K> Windowed<K> from(final byte[] binaryKey,
                                           final Deserializer<K> keyDeserializer,
                                           final String topic) {
            final K key = extractKey(binaryKey, keyDeserializer, topic);
            final Window window = extractWindow(binaryKey);
            return new Windowed<>(key, window);
        }

        static long extractStartTimestamp(final byte[] binaryKey) {
            return ByteBuffer.wrap(binaryKey).getLong(binaryKey.length - TIMESTAMP_SIZE);
        }

        static long extractEndTimestamp(final byte[] binaryKey) {
            return ByteBuffer.wrap(binaryKey).getLong(binaryKey.length - 2 * TIMESTAMP_SIZE);
        }

        public static Bytes toBinary(final Windowed<Bytes> sessionKey) {
            return toBinary(sessionKey.key(), sessionKey.window().start(), sessionKey.window().end());
        }

        public static <K> byte[] toBinary(final Windowed<K> sessionKey,
                                          final Serializer<K> serializer,
                                          final String topic) {
            final byte[] bytes = serializer.serialize(topic, sessionKey.key());
            return toBinary(Bytes.wrap(bytes), sessionKey.window().start(), sessionKey.window().end()).get();
        }

        public static Bytes toBinary(final Bytes key,
                                     final long startTime,
                                     final long endTime) {
            final ByteBuffer buf = ByteBuffer.allocate(PREFIX_SIZE + SessionKeySchema.keyByteLength(key));
            buf.put(KEY_FIRST_PREFIX);
            SessionKeySchema.writeBinary(buf, key, startTime, endTime);
            return Bytes.wrap(buf.array());
        }

        private static Bytes wrapPrefix(final Bytes noPrefixKey, final boolean upperRange) {
            // Need to scan from prefix even key is null
            if (noPrefixKey == null) {
                final byte prefix = upperRange ? KEY_FIRST_PREFIX + 1 : KEY_FIRST_PREFIX;
                final byte[] ret = ByteBuffer.allocate(PREFIX_SIZE)
                    .put(prefix)
                    .array();
                return Bytes.wrap(ret);
            }
            final byte[] ret = ByteBuffer.allocate(PREFIX_SIZE + noPrefixKey.get().length)
                .put(KEY_FIRST_PREFIX)
                .put(noPrefixKey.get())
                .array();
            return Bytes.wrap(ret);
        }

        public static byte[] prefixNonPrefixSessionKey(final byte[] binaryKey) {
            assert binaryKey != null;

            return wrapPrefix(Bytes.wrap(binaryKey), false).get();
        }
    }
}
