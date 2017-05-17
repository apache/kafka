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
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;

import java.nio.ByteBuffer;
import java.util.List;

class WindowKeySchema implements RocksDBSegmentedBytesStore.KeySchema {

    private static final int SUFFIX_SIZE = WindowStoreUtils.TIMESTAMP_SIZE + WindowStoreUtils.SEQNUM_SIZE;
    private static final byte[] MIN_SUFFIX = new byte[SUFFIX_SIZE];
    private static final int MIN_KEY_LENGTH = 1;

    private StateSerdes<Bytes, byte[]> serdes;

    @Override
    public void init(final String topic) {
        serdes = new StateSerdes<>(topic, Serdes.Bytes(), Serdes.ByteArray());
    }

    @Override
    public Bytes upperRange(final Bytes key, final long to) {
        final byte[] bytes = key.get();
        ByteBuffer rangeEnd = ByteBuffer.allocate(bytes.length + SUFFIX_SIZE);

        final byte[] maxSuffix = ByteBuffer.allocate(SUFFIX_SIZE)
            .putLong(to)
            .putInt(Integer.MAX_VALUE)
            .array();

        int i = 0;
        while (i < bytes.length && (
            i < MIN_KEY_LENGTH // assumes keys are at least one byte long
            || bytes[i] >= maxSuffix[0]
            )) {
            rangeEnd.put(bytes[i++]);
        }

        rangeEnd.put(maxSuffix);
        rangeEnd.flip();

        byte[] res = new byte[rangeEnd.remaining()];
        ByteBuffer.wrap(res).put(rangeEnd);
        return Bytes.wrap(res);
    }

    @Override
    public Bytes lowerRange(final Bytes key, final long from) {
        final byte[] bytes = key.get();
        ByteBuffer rangeStart = ByteBuffer.allocate(bytes.length + SUFFIX_SIZE);
        // any key in the range would start at least with the given prefix to be
        // in the range, and have at least SUFFIX_SIZE number of trailing zero bytes.

        // unless there is a maximum key length, you can keep appending more zero bytes
        // to keyFrom to create a key that will match the range, yet that would precede
        // WindowStoreUtils.toBinaryKey(keyFrom, from, 0) in byte order
        return Bytes.wrap(
            rangeStart
                .put(bytes)
                .put(MIN_SUFFIX)
                .array()
        );
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
    public List<Segment> segmentsToSearch(final Segments segments, final long from, final long to) {
        return segments.segments(from, to);
    }
}
