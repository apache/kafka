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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;

import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.kafka.streams.state.StateSerdes.TIMESTAMP_SIZE;
import static org.apache.kafka.streams.state.internals.WindowKeySchema.timeWindowForSize;

public class PrefixedWindowKeySchemas {

    private static final int PREFIX_SIZE = 1;
    private static final byte TIME_FIRST_PREFIX = 0;
    private static final byte KEY_FIRST_PREFIX = 1;
    private static final int SEQNUM_SIZE = 4;
    private static final int SUFFIX_SIZE = TIMESTAMP_SIZE + SEQNUM_SIZE;
    private static final byte[] MIN_SUFFIX = new byte[SUFFIX_SIZE];

    private static byte extractPrefix(final byte[] binaryBytes) {
        return binaryBytes[0];
    }

    public static class TimeFirstWindowKeySchema implements RocksDBSegmentedBytesStore.KeySchema {

        @Override
        public Bytes upperRange(final Bytes key, final long to) {
            if (to == Long.MAX_VALUE) {
                return null;
            }

            return Bytes.wrap(ByteBuffer.allocate(PREFIX_SIZE + TIMESTAMP_SIZE)
                .put(TIME_FIRST_PREFIX)
                .putLong(to + 1)
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

            /*
             * Larger timestamp or key's byte order can't be smaller than this lower range. Reason:
             *     1. Timestamp is fixed length (with big endian byte order). Since we put timestamp
             *        first, larger timestamp will have larger byte order.
             *     2. If timestamp is the same but key (k1) is larger than this lower range key (k2):
             *         a. If k2 is not a prefix of k1, then k1 will always have larger byte order no
             *            matter what seqnum k2 has
             *         b. If k2 is a prefix of k1, since k2's seqnum is 0, after k1 appends seqnum,
             *            it will always be larger than (k1 + seqnum).
             */
            return Bytes.wrap(ByteBuffer.allocate(PREFIX_SIZE + TIMESTAMP_SIZE + key.get().length + SEQNUM_SIZE)
                .put(TIME_FIRST_PREFIX)
                .putLong(from)
                .put(key.get())
                .array());
        }

        @Override
        public Bytes lowerRangeFixedSize(final Bytes key, final long from) {
            return TimeFirstWindowKeySchema.toTimeOrderedStoreKeyBinary(key, Math.max(0, from),
                0);
        }

        @Override
        public Bytes upperRangeFixedSize(final Bytes key, final long to) {
            return TimeFirstWindowKeySchema.toTimeOrderedStoreKeyBinary(key, to, Integer.MAX_VALUE);
        }

        @Override
        public long segmentTimestamp(final Bytes key) {
            return WindowKeySchema.extractStoreTimestamp(key.get());
        }

        @Override
        public HasNextCondition hasNextCondition(final Bytes binaryKeyFrom,
            final Bytes binaryKeyTo,
            final long from,
            final long to) {
            return iterator -> {
                while (iterator.hasNext()) {
                    final Bytes bytes = iterator.peekNextKey();
                    final byte prefix = extractPrefix(bytes.get());

                    if (prefix != TIME_FIRST_PREFIX) {
                        return false;
                    }

                    final long time = TimeFirstWindowKeySchema.extractStoreTimestamp(bytes.get());

                    // We can return false directly here since keys are sorted by time and if
                    // we get time larger than `to`, there won't be time within range.
                    if (time > to) {
                        return false;
                    }

                     final Bytes keyBytes = Bytes.wrap(
                        TimeFirstWindowKeySchema.extractStoreKeyBytes(bytes.get()));
                    if ((binaryKeyFrom == null || keyBytes.compareTo(binaryKeyFrom) >= 0)
                        && (binaryKeyTo == null || keyBytes.compareTo(binaryKeyTo) <= 0)
                        && time >= from) {
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
            return segments.segments(from, to, forward);
        }

        static byte[] extractStoreKeyBytes(final byte[] binaryKey) {
            final byte[] bytes = new byte[binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE - PREFIX_SIZE];
            System.arraycopy(binaryKey, TIMESTAMP_SIZE + PREFIX_SIZE, bytes, 0, bytes.length);
            return bytes;
        }

        static long extractStoreTimestamp(final byte[] binaryKey) {
            return ByteBuffer.wrap(binaryKey).getLong(PREFIX_SIZE);
        }

        // for store serdes
        public static Bytes toTimeOrderedStoreKeyBinary(final Bytes key,
                                                        final long timestamp,
                                                        final int seqnum) {
            final byte[] serializedKey = key.get();
            return toTimeOrderedStoreKeyBinary(serializedKey, timestamp, seqnum);
        }

        static Bytes toTimeOrderedStoreKeyBinary(final byte[] serializedKey,
                                                 final long timestamp,
                                                 final int seqnum) {
            final ByteBuffer buf = ByteBuffer.allocate(
                PREFIX_SIZE + TIMESTAMP_SIZE + serializedKey.length + SEQNUM_SIZE);
            buf.put(TIME_FIRST_PREFIX);
            buf.putLong(timestamp);
            buf.put(serializedKey);
            buf.putInt(seqnum);

            return Bytes.wrap(buf.array());
        }

        public static Windowed<Bytes> fromStoreBytesKey(final byte[] binaryKey,
                                                        final long windowSize) {
            final Bytes key = Bytes.wrap(extractStoreKeyBytes(binaryKey));
            final Window window = extractStoreWindow(binaryKey, windowSize);
            return new Windowed<>(key, window);
        }

        static Window extractStoreWindow(final byte[] binaryKey,
                                         final long windowSize) {
            final long start = WindowKeySchema.extractStoreTimestamp(binaryKey);
            return timeWindowForSize(start, windowSize);
        }
    }

    public static class KeyFirstWindowKeySchema extends WindowKeySchema {

        private Bytes wrapPrefix(final Bytes noPrefixKey) {
             final byte[] ret = ByteBuffer.allocate(PREFIX_SIZE + noPrefixKey.get().length)
                .put(KEY_FIRST_PREFIX)
                .put(noPrefixKey.get())
                .array();
            return Bytes.wrap(ret);
        }

        @Override
        public Bytes upperRange(final Bytes key, final long to) {
            final Bytes noPrefixBytes = super.upperRange(key, to);
            if (noPrefixBytes == null) {
                return null;
            }
            return wrapPrefix(noPrefixBytes);
        }

        @Override
        public Bytes lowerRange(final Bytes key, final long from) {
            final Bytes noPrefixBytes = super.lowerRange(key, from);
            if (noPrefixBytes == null) {
                return null;
            }
            return wrapPrefix(noPrefixBytes);
        }

        @Override
        public Bytes lowerRangeFixedSize(final Bytes key, final long from) {
            final Bytes noPrefixBytes = WindowKeySchema.toStoreKeyBinary(key, Math.max(0, from), 0);
            return wrapPrefix(noPrefixBytes);
        }

        @Override
        public Bytes upperRangeFixedSize(final Bytes key, final long to) {
            final Bytes noPrefixBytes = WindowKeySchema.toStoreKeyBinary(key, to, Integer.MAX_VALUE);
            return wrapPrefix(noPrefixBytes);
        }

        @Override
        public long segmentTimestamp(final Bytes key) {
            return KeyFirstWindowKeySchema.extractStoreTimestamp(key.get());
        }

        @Override
        public HasNextCondition hasNextCondition(final Bytes binaryKeyFrom,
                                                 final Bytes binaryKeyTo,
                                                 final long from,
                                                 final long to) {
            return iterator -> {
                while (iterator.hasNext()) {
                    final Bytes bytes = iterator.peekNextKey();
                    final byte prefix = extractPrefix(bytes.get());

                    if (prefix != KEY_FIRST_PREFIX) {
                        return false;
                    }

                    final Bytes keyBytes = Bytes.wrap(KeyFirstWindowKeySchema.extractStoreKeyBytes(bytes.get()));
                    final long time = KeyFirstWindowKeySchema.extractStoreTimestamp(bytes.get());
                    if ((binaryKeyFrom == null || keyBytes.compareTo(binaryKeyFrom) >= 0)
                        && (binaryKeyTo == null || keyBytes.compareTo(binaryKeyTo) <= 0)
                        && time >= from
                        && time <= to) {
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
            return segments.segments(from, to, forward);
        }

        public static Bytes toStoreKeyBinary(final Bytes key,
                                             final long timestamp,
                                             final int seqnum) {
            final byte[] serializedKey = key.get();
            return toStoreKeyBinary(serializedKey, timestamp, seqnum);
        }

        // package private for testing
        static Bytes toStoreKeyBinary(final byte[] serializedKey,
                                      final long timestamp,
                                      final int seqnum) {
            final ByteBuffer buf = ByteBuffer.allocate(PREFIX_SIZE + serializedKey.length + TIMESTAMP_SIZE + SEQNUM_SIZE);
            buf.put(KEY_FIRST_PREFIX);
            buf.put(serializedKey);
            buf.putLong(timestamp);
            buf.putInt(seqnum);

            return Bytes.wrap(buf.array());
        }

        static byte[] extractStoreKeyBytes(final byte[] binaryKey) {
            final byte[] bytes = new byte[binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE - PREFIX_SIZE];
            System.arraycopy(binaryKey, PREFIX_SIZE, bytes, 0, bytes.length);
            return bytes;
        }

        static long extractStoreTimestamp(final byte[] binaryKey) {
            return ByteBuffer.wrap(binaryKey).getLong(binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE);
        }

        static int extractStoreSeqnum(final byte[] binaryKey) {
            return ByteBuffer.wrap(binaryKey).getInt(binaryKey.length - SEQNUM_SIZE);
        }

        public static Windowed<Bytes> fromStoreBytesKey(final byte[] binaryKey,
                                                    final long windowSize) {
            final Bytes key = Bytes.wrap(extractStoreKeyBytes(binaryKey));
            final Window window = extractStoreWindow(binaryKey, windowSize);
            return new Windowed<>(key, window);
        }

        static Window extractStoreWindow(final byte[] binaryKey,
                                     final long windowSize) {
            final long start = KeyFirstWindowKeySchema.extractStoreTimestamp(binaryKey);
            return timeWindowForSize(start, windowSize);
        }
    }
}
