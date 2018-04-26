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
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;

import org.rocksdb.NativeComparatorWrapper;

import java.nio.ByteBuffer;
import java.util.List;

public class WindowKeySchema implements RocksDBSegmentedBytesStore.KeySchema {

    private static final int SEQNUM_SIZE = 4;
    private static final int TIMESTAMP_SIZE = 8;
    private static final int SUFFIX_SIZE = TIMESTAMP_SIZE + SEQNUM_SIZE;

    private static final WindowKeyBytesComparator COMPARATOR = new WindowKeyBytesComparator();

    @Override
    public void init(final String topic) {
        // nothing to do
    }

    @Override
    public Bytes lowerRange(final Bytes key, final long from) {
        return WindowKeySchema.toStoreKeyBinary(key, Math.max(0, from), 0);
    }

    @Override
    public Bytes upperRange(final Bytes key, final long to) {
        return WindowKeySchema.toStoreKeyBinary(key, to, Integer.MAX_VALUE);
    }

    @Override
    public long segmentTimestamp(final Bytes key) {
        return WindowKeySchema.extractStoreTimestamp(key.get());
    }

    @Override
    public HasNextCondition hasNextCondition(final Bytes binaryKeyFrom, final Bytes binaryKeyTo, final long from, final long to) {
        return new HasNextCondition() {
            @Override
            public boolean hasNext(final KeyValueIterator<Bytes, ?> iterator) {
                while (iterator.hasNext()) {
                    final Bytes bytes = iterator.peekNextKey();
                    final Bytes keyBytes = Bytes.wrap(WindowKeySchema.extractStoreKeyBytes(bytes.get()));
                    final long time = WindowKeySchema.extractStoreTimestamp(bytes.get());
                    if ((binaryKeyFrom == null || keyBytes.compareTo(binaryKeyFrom) >= 0)
                        && (binaryKeyTo == null || keyBytes.compareTo(binaryKeyTo) <= 0)
                        && time >= from
                        && time <= to) {
                        return true;
                    }
                    iterator.next();
                }
                return false;
            }
        };
    }

    @Override
    public List<Segment> segmentsToSearch(final Segments segments, final long from, final long to) {
        return segments.segments(from, to);
    }

    @Override
    public Bytes.ByteArrayComparator bytesComparator() {
        return COMPARATOR;
    }

    /**
     * Safely construct a time window of the given size,
     * taking care of bounding endMs to Long.MAX_VALUE if necessary
     */
    public static TimeWindow timeWindowForSize(final long startMs,
                                               final long windowSize) {
        final long endMs = startMs + windowSize;
        return new TimeWindow(startMs, endMs < 0 ? Long.MAX_VALUE : endMs);
    }

    // for pipe serdes

    public static <K> byte[] toBinary(final Windowed<K> timeKey,
                                      final Serializer<K> serializer,
                                      final String topic) {
        final byte[] bytes = serializer.serialize(topic, timeKey.key());
        final ByteBuffer buf = ByteBuffer.allocate(bytes.length + TIMESTAMP_SIZE);
        buf.put(bytes);
        buf.putLong(timeKey.window().start());

        return buf.array();
    }

    public static <K> Windowed<K> from(final byte[] binaryKey,
                                       final long windowSize,
                                       final Deserializer<K> deserializer,
                                       final String topic) {
        final byte[] bytes = new byte[binaryKey.length - TIMESTAMP_SIZE];
        System.arraycopy(binaryKey, 0, bytes, 0, bytes.length);
        final K key = deserializer.deserialize(topic, bytes);
        final Window window = extractWindow(binaryKey, windowSize);
        return new Windowed<>(key, window);
    }

    private static Window extractWindow(final byte[] binaryKey,
                                        final long windowSize) {
        final ByteBuffer buffer = ByteBuffer.wrap(binaryKey);
        final long start = buffer.getLong(binaryKey.length - TIMESTAMP_SIZE);
        return timeWindowForSize(start, windowSize);
    }

    // for store serdes

    public static Bytes toStoreKeyBinary(final Bytes key,
                                         final long timestamp,
                                         final int seqnum) {
        final byte[] serializedKey = key.get();
        return toStoreKeyBinary(serializedKey, timestamp, seqnum);
    }

    public static <K> Bytes toStoreKeyBinary(final K key,
                                             final long timestamp,
                                             final int seqnum,
                                             final StateSerdes<K, ?> serdes) {
        final byte[] serializedKey = serdes.rawKey(key);
        return toStoreKeyBinary(serializedKey, timestamp, seqnum);
    }

    public static Bytes toStoreKeyBinary(final Windowed<Bytes> timeKey,
                                         final int seqnum) {
        final byte[] bytes = timeKey.key().get();
        return toStoreKeyBinary(bytes, timeKey.window().start(), seqnum);
    }

    public static <K> Bytes toStoreKeyBinary(final Windowed<K> timeKey,
                                             final int seqnum,
                                             final StateSerdes<K, ?> serdes) {
        final byte[] serializedKey = serdes.rawKey(timeKey.key());
        return toStoreKeyBinary(serializedKey, timeKey.window().start(), seqnum);
    }

    // package private for testing
    static Bytes toStoreKeyBinary(final byte[] serializedKey,
                                  final long timestamp,
                                  final int seqnum) {
        final ByteBuffer buf = ByteBuffer.allocate(serializedKey.length + TIMESTAMP_SIZE + SEQNUM_SIZE);
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

    public static <K> K extractStoreKey(final byte[] binaryKey,
                                        final StateSerdes<K, ?> serdes) {
        final byte[] bytes = new byte[binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE];
        System.arraycopy(binaryKey, 0, bytes, 0, bytes.length);
        return serdes.keyFrom(bytes);
    }

    public static long extractStoreTimestamp(final byte[] binaryKey) {
        return ByteBuffer.wrap(binaryKey).getLong(binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE);
    }

    public static int extractStoreSequence(final byte[] binaryKey) {
        return ByteBuffer.wrap(binaryKey).getInt(binaryKey.length - SEQNUM_SIZE);
    }

    public static <K> Windowed<K> fromStoreKey(final byte[] binaryKey,
                                               final long windowSize,
                                               final StateSerdes<K, ?> serdes) {
        final K key = serdes.keyDeserializer().deserialize(serdes.topic(), extractStoreKeyBytes(binaryKey));
        final Window window = extractStoreWindow(binaryKey, windowSize);
        return new Windowed<>(key, window);
    }

    public static Windowed<Bytes> fromStoreKey(final byte[] binaryKey,
                                               final long windowSize) {
        final Bytes key = Bytes.wrap(extractStoreKeyBytes(binaryKey));
        final Window window = extractStoreWindow(binaryKey, windowSize);
        return new Windowed<>(key, window);
    }

    public static Window extractStoreWindow(final byte[] binaryKey,
                                            final long windowSize) {
        final ByteBuffer buffer = ByteBuffer.wrap(binaryKey);
        final long start = buffer.getLong(binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE);
        return timeWindowForSize(start, windowSize);
    }

    private static class WindowKeyBytesComparator extends Bytes.LexicographicByteArrayComparator {

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

    public static class NativeWindowKeyBytesComparatorWrapper extends NativeComparatorWrapper {

        @Override
        protected long initializeNative(final long... nativeParameterHandles) {
            return newComparator();
        }

        private native long newComparator();
    }
}
