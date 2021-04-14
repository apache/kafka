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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.StateSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * A {@link RocksDBSegmentedBytesStore.KeySchema} to serialize/deserialize a RocksDB store
 * key into a schema combined of (time,seq,key). This key schema is more efficient when doing
 * range queries between a time interval. For key range queries better use {@link WindowKeySchema}.
 */
public class TimeOrderedKeySchema implements RocksDBSegmentedBytesStore.KeySchema {
    private static final Logger LOG = LoggerFactory.getLogger(TimeOrderedKeySchema.class);

    private static final int TIMESTAMP_SIZE = 8;
    private static final int SEQNUM_SIZE = 4;
    private static final int PREFIX_SIZE = TIMESTAMP_SIZE + SEQNUM_SIZE;

    /**
     * {@inheritdoc}
     *
     * Queries using the {@link TimeOrderedKeySchema} are optimized for time range queries only. Key
     * range queries may be slower. If better performance on key range queries are necessary, then
     * use the {@link WindowKeySchema}.
     */
    @Override
    public Bytes upperRange(final Bytes key, final long to) {
        return toStoreKeyBinary(key.get(), to, Integer.MAX_VALUE);
    }

    /**
     * {@inheritdoc}
     *
     * Queries using the {@link TimeOrderedKeySchema} are optimized for time range queries only. Key
     * range queries may be slower. If better performance on key range queries are necessary, then
     * use the {@link WindowKeySchema}.
     */
    @Override
    public Bytes lowerRange(final Bytes key, final long from) {
        return toStoreKeyBinary(key.get(), from, 0);
    }

    /**
     * {@inheritdoc}
     *
     * Queries using the {@link TimeOrderedKeySchema} are optimized for time range queries only. Key
     * range queries may be slower. If better performance on key range queries are necessary, then
     * use the {@link WindowKeySchema}.
     */
    @Override
    public Bytes upperRangeFixedSize(final Bytes key, final long to) {
        return toStoreKeyBinary(key, to, Integer.MAX_VALUE);
    }

    /**
     * {@inheritdoc}
     *
     * Queries using the {@link TimeOrderedKeySchema} are optimized for time range queries only. Key
     * range queries may be slower. If better performance on key range queries are necessary, then
     * use the {@link WindowKeySchema}.
     */
    @Override
    public Bytes lowerRangeFixedSize(final Bytes key, final long from) {
        return toStoreKeyBinary(key, Math.max(0, from), 0);
    }

    @Override
    public long segmentTimestamp(final Bytes key) {
        return extractStoreTimestamp(key.get());
    }

    /**
     * {@inheritdoc}
     *
     * This method is not optimized for {@link TimeOrderedKeySchema}. The method may do unnecessary
     * checks to find the next record.
     */
    @Override
    public HasNextCondition hasNextCondition(final Bytes binaryKeyFrom, final Bytes binaryKeyTo, final long from, final long to) {
        return iterator -> {
            while (iterator.hasNext()) {
                final Bytes bytes = iterator.peekNextKey();
                final Bytes keyBytes = Bytes.wrap(extractStoreKeyBytes(bytes.get()));
                final long time = extractStoreTimestamp(bytes.get());
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
    public <S extends Segment> List<S> segmentsToSearch(final Segments<S> segments, final long from, final long to, final boolean forward) {
        return segments.segments(from, to, forward);
    }

    public static Bytes toStoreKeyBinary(final Bytes key,
                                         final long timestamp,
                                         final int seqnum) {
        final byte[] serializedKey = key.get();
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
        final ByteBuffer buf = ByteBuffer.allocate(TIMESTAMP_SIZE + serializedKey.length + SEQNUM_SIZE);
        buf.putLong(timestamp);
        buf.putInt(seqnum);
        buf.put(serializedKey);
        return Bytes.wrap(buf.array());
    }

    static byte[] extractStoreKeyBytes(final byte[] binaryKey) {
        final byte[] bytes = new byte[binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE];
        System.arraycopy(binaryKey, PREFIX_SIZE, bytes, 0, bytes.length);
        return bytes;
    }

    static <K> K extractStoreKey(final byte[] binaryKey,
                                 final StateSerdes<K, ?> serdes) {
        final byte[] bytes = new byte[binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE];
        System.arraycopy(binaryKey, PREFIX_SIZE, bytes, 0, bytes.length);
        return serdes.keyFrom(bytes);
    }

    static long extractStoreTimestamp(final byte[] binaryKey) {
        return ByteBuffer.wrap(binaryKey).getLong(0);
    }

    static int extractStoreSequence(final byte[] binaryKey) {
        return ByteBuffer.wrap(binaryKey).getInt(TIMESTAMP_SIZE);
    }

    static <K> Windowed<K> fromStoreKey(final byte[] binaryKey,
                                        final long windowSize,
                                        final Deserializer<K> deserializer,
                                        final String topic) {
        final K key = deserializer.deserialize(topic, extractStoreKeyBytes(binaryKey));
        final Window window = extractStoreWindow(binaryKey, windowSize);
        return new Windowed<>(key, window);
    }

    static Windowed<Bytes> fromStoreBytesKey(final byte[] binaryKey,
                                             final long windowSize) {
        final Bytes key = Bytes.wrap(extractStoreKeyBytes(binaryKey));
        final Window window = extractStoreWindow(binaryKey, windowSize);
        return new Windowed<>(key, window);
    }

    static Window extractStoreWindow(final byte[] binaryKey,
                                     final long windowSize) {
        final ByteBuffer buffer = ByteBuffer.wrap(binaryKey);
        final long start = buffer.getLong(0);
        return timeWindowForSize(start, windowSize);
    }

    /**
     * Safely construct a time window of the given size,
     * taking care of bounding endMs to Long.MAX_VALUE if necessary
     */
    static TimeWindow timeWindowForSize(final long startMs,
                                        final long windowSize) {
        long endMs = startMs + windowSize;

        if (endMs < 0) {
            LOG.warn("Warning: window end time was truncated to Long.MAX");
            endMs = Long.MAX_VALUE;
        }
        return new TimeWindow(startMs, endMs);
    }
}
