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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.HasNextCondition;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;

import java.nio.ByteBuffer;
import java.util.List;

public class WindowKeySchema implements SegmentedBytesStore.SegmentedKeySchema {

    private static final int SUFFIX_SIZE = WindowStoreUtils.TIMESTAMP_SIZE + WindowStoreUtils.SEQNUM_SIZE;
    private static final byte[] MIN_SUFFIX = new byte[SUFFIX_SIZE];
    private final SegmentedCacheFunction segmentedCacheFunction;
    private final long windowSize;

    private StateSerdes<Bytes, byte[]> serdes;

    WindowKeySchema(final long segmentInterval, final long windowSize) {
        segmentedCacheFunction = new SegmentedCacheFunction(this, segmentInterval);
        this.windowSize = windowSize;
    }

    @Override
    public void init(final String topic) {
        serdes = new StateSerdes<>(topic, Serdes.Bytes(), Serdes.ByteArray());
    }

    @Override
    public Bytes upperRange(final Bytes key, final long to) {
        final byte[] maxSuffix = ByteBuffer.allocate(SUFFIX_SIZE)
            .putLong(to)
            .putInt(Integer.MAX_VALUE)
            .array();

        return OrderedBytes.upperRange(key, maxSuffix);
    }

    @Override
    public Bytes lowerRange(final Bytes key, final long from) {
        return OrderedBytes.lowerRange(key, MIN_SUFFIX);
    }

    @Override
    public Bytes lowerRangeFixedSize(final Bytes key, final long from) {
        return WindowStoreUtils.toBinaryKey(key, Math.max(0, from), 0, serdes);
    }

    @Override
    public Bytes upperRangeFixedSize(final Bytes key, final long to) {
        return WindowStoreUtils.toBinaryKey(key, to, Integer.MAX_VALUE, serdes);
    }

    @Override
    public long segmentTimestamp(final Bytes key) {
        return WindowStoreUtils.timestampFromBinaryKey(key.get());
    }

    @Override
    public HasNextCondition hasNextCondition(final Bytes binaryKeyFrom, final Bytes binaryKeyTo, final long from, final long to) {
        return new HasNextCondition() {
            @Override
            public boolean hasNext(final KeyValueIterator<Bytes, ?> iterator) {
                while (iterator.hasNext()) {
                    final Bytes bytes = iterator.peekNextKey();
                    final Bytes keyBytes = WindowStoreUtils.bytesKeyFromBinaryKey(bytes.get());
                    final long time = WindowStoreUtils.timestampFromBinaryKey(bytes.get());
                    if (keyBytes.compareTo(binaryKeyFrom) >= 0
                        && keyBytes.compareTo(binaryKeyTo) <= 0
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
    public Bytes toCacheKey(final Bytes storeKey) {
        return segmentedCacheFunction.cacheKey(storeKey);
    }

    @Override
    public Bytes toStoreKey(final Bytes cacheKey) {
        return segmentedCacheFunction.key(cacheKey);
    }

    @Override
    public Bytes extractRecordKey(final Bytes binaryKey) {
        return WindowStoreUtils.keyFromBinaryKey(binaryKey.get(), serdes);
    }

    @Override
    public Bytes toBinaryKey(final Windowed<Bytes> storeKey) {
        return WindowStoreUtils.toBinaryKey(storeKey.key().get(), storeKey.window().start(), 0);
    }

    @Override
    public Window extractWindow(final Bytes binaryKey) {
        return WindowStoreUtils.timeWindowForSize(WindowStoreUtils.timestampFromBinaryKey(binaryKey.get()), windowSize);
    }

    @Override
    public int compareKeys(final Bytes cacheKey, final Bytes storeKey) {
        return segmentedCacheFunction.compareSegmentedKeys(cacheKey, storeKey);
    }

    @Override
    public Bytes toBinaryKey(final Bytes key, final long timestamp, final int sequenceNumber) {
        return WindowStoreUtils.toBinaryKey(key.get(), timestamp, sequenceNumber);
    }

    @Override
    public List<Segment> segmentsToSearch(final Segments segments, final long from, final long to) {
        return segments.segments(from, to);
    }
}
