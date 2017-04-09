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

import java.util.List;


class SessionKeySchema implements SegmentedBytesStore.KeySchema {
    private String topic;

    @Override
    public void init(final String topic) {
        this.topic = topic;
    }

    @Override
    public Bytes upperRange(final Bytes key, final long to) {
        final Windowed<Bytes> sessionKey = new Windowed<>(key, new SessionWindow(to, Long.MAX_VALUE));
        return SessionKeySerde.toBinary(sessionKey, Serdes.Bytes().serializer(), topic);
    }

    @Override
    public Bytes lowerRange(final Bytes key, final long from) {
        final Windowed<Bytes> sessionKey = new Windowed<>(key, new SessionWindow(0, Math.max(0, from)));
        return SessionKeySerde.toBinary(sessionKey, Serdes.Bytes().serializer(), topic);
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
    public List<Segment> segmentsToSearch(final Segments segments, final long from, final long to) {
        return segments.segments(from, Long.MAX_VALUE);
    }

}
