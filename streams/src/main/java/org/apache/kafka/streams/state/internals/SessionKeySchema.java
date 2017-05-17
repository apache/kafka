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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionKeySerde;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.nio.ByteBuffer;
import java.util.List;


class SessionKeySchema implements SegmentedBytesStore.KeySchema {

    private static final int SUFFIX_SIZE = 2 * WindowStoreUtils.TIMESTAMP_SIZE;
    private static final byte[] MIN_SUFFIX = new byte[SUFFIX_SIZE];
    private static final int MIN_KEY_LENGTH = 1;

    private String topic;

    @Override
    public void init(final String topic) {
        this.topic = topic;
    }

    @Override
    public Bytes upperRangeFixedSize(final Bytes key, final long to) {
        final Windowed<Bytes> sessionKey = new Windowed<>(key, new SessionWindow(to, Long.MAX_VALUE));
        return SessionKeySerde.toBinary(sessionKey, Serdes.Bytes().serializer(), topic);
    }

    @Override
    public Bytes lowerRangeFixedSize(final Bytes key, final long from) {
        final Windowed<Bytes> sessionKey = new Windowed<>(key, new SessionWindow(0, Math.max(0, from)));
        return SessionKeySerde.toBinary(sessionKey, Serdes.Bytes().serializer(), topic);
    }

    @Override
    public Bytes upperRange(Bytes key, long to) {
        final byte[] bytes = key.get();
        ByteBuffer rangeEnd = ByteBuffer.allocate(bytes.length + SUFFIX_SIZE);

        final byte[] maxSuffix = ByteBuffer.allocate(SUFFIX_SIZE)
            .putLong(to)
            .putLong(Long.MAX_VALUE)
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
    public Bytes lowerRange(Bytes key, long from) {
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
    public long segmentTimestamp(final Bytes key) {
        return SessionKeySerde.extractEnd(key.get());
    }

    @Override
    public HasNextCondition hasNextCondition(final Bytes binaryKey, final long from, final long to) {
        return new HasNextCondition() {
            @Override
            public boolean hasNext(final KeyValueIterator<Bytes, ?> iterator) {
                while (iterator.hasNext()) {
                    final Bytes bytes = iterator.peekNextKey();
                    final Windowed<Bytes> windowedKey = SessionKeySerde.fromBytes(bytes);
                    if (windowedKey.key().equals(binaryKey)
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
    public HasNextCondition hasNextCondition(final Bytes binaryKeyFrom, final Bytes binaryKeyTo, final long from, final long to) {
        return new HasNextCondition() {
            @Override
            public boolean hasNext(final KeyValueIterator<Bytes, ?> iterator) {
                while (iterator.hasNext()) {
                    final Bytes bytes = iterator.peekNextKey();
                    final Windowed<Bytes> windowedKey = SessionKeySerde.fromBytes(bytes);
                    if (windowedKey.key().compareTo(binaryKeyFrom) >= 0
                        && windowedKey.key().compareTo(binaryKeyTo) <= 0
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
    public List<Segment> segmentsToSearch(final Segments segments, final long from, final long to) {
        return segments.segments(from, Long.MAX_VALUE);
    }

}
