/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionKeyBinaryConverter;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.List;

class RocksDBSessionStore<K, AGG> implements SessionStore<K, AGG> {

    private final Serde<K> keySerde;
    private final Serde<AGG> aggSerde;
    private final SegmentedBytesStore bytesStore;
    private StateSerdes<K, AGG> serdes;


    RocksDBSessionStore(final SegmentedBytesStore bytesStore,
                        final Serde<K> keySerde,
                        final Serde<AGG> aggSerde) {
        this.keySerde = keySerde;
        this.bytesStore = bytesStore;
        this.aggSerde = aggSerde;
    }


    @SuppressWarnings("unchecked")
    @Override
    public KeyValueIterator<Windowed<K>, AGG> findSessionsToMerge(final K key, final long earliestSessionEndTime, final long latestSessionStartTime) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = bytesStore.fetch(Bytes.wrap(serdes.rawKey(key)), earliestSessionEndTime, latestSessionStartTime);
        return new SessionStoreIterator(bytesIterator, serdes);
    }


    @Override
    public void remove(final Windowed<K> key) {
        bytesStore.remove(SessionKeyBinaryConverter.toBinary(key, serdes.keySerializer()));
    }

    @Override
    public void put(final Windowed<K> sessionKey, final AGG aggregate) {
        bytesStore.put(SessionKeyBinaryConverter.toBinary(sessionKey, serdes.keySerializer()), aggSerde.serializer().serialize(bytesStore.name(), aggregate));
    }

    @Override
    public String name() {
        return bytesStore.name();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context, final StateStore root) {
        this.serdes = new StateSerdes<>(bytesStore.name(),
                                        keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                                        aggSerde == null ? (Serde<AGG>) context.valueSerde() : aggSerde);

        bytesStore.init(context, root);
    }

    @Override
    public void flush() {
        bytesStore.flush();
    }

    @Override
    public void close() {
        bytesStore.close();
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return bytesStore.isOpen();
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> fetch(final K key) {
        return findSessionsToMerge(key, 0, Long.MAX_VALUE);
    }

    static class SessionKeySchema implements RocksDBSegmentedBytesStore.KeySchema {

        @Override
        public Bytes upperRange(final Bytes key, final long to) {
            final Windowed<Bytes> sessionKey = new Windowed<>(key, new TimeWindow(to, Long.MAX_VALUE));
            return SessionKeyBinaryConverter.toBinary(sessionKey, Serdes.Bytes().serializer());
        }

        @Override
        public Bytes lowerRange(final Bytes key, final long from) {
            final Windowed<Bytes> sessionKey = new Windowed<>(key, new TimeWindow(0, Math.max(0, from)));
            return SessionKeyBinaryConverter.toBinary(sessionKey, Serdes.Bytes().serializer());
        }

        @Override
        public long segmentTimestamp(final Bytes key) {
            return SessionKeyBinaryConverter.extractEnd(key.get());
        }

        @Override
        public HasNextCondition<Bytes> hasNextCondition(final Bytes binaryKey, final long from, final long to) {
            return hasNextMatchingSession(from, to, binaryKey);
        }

        @Override
        public List<Segment> segmentsToSearch(final Segments segments, final long from, final long to) {
            return segments.segments(from, Long.MAX_VALUE);
        }

    }

    static HasNextCondition<Bytes> hasNextMatchingSession(final long earliestEndTime, final long latestStartTime, final Bytes binaryKey) {
        return new HasNextCondition<Bytes>() {
            @Override
            public boolean hasNext(final KeyValueIterator<Bytes, ?> iterator) {
                if (iterator.hasNext()) {
                    final Bytes bytes = iterator.peekNextKey();
                    final Bytes keyBytes = Bytes.wrap(SessionKeyBinaryConverter.extractKeyBytes(bytes.get()));
                    if (!keyBytes.equals(binaryKey)) {
                        return false;
                    }
                    final long start = SessionKeyBinaryConverter.extractStart(bytes.get());
                    final long end = SessionKeyBinaryConverter.extractEnd(bytes.get());
                    return end >= earliestEndTime && start <= latestStartTime;
                }
                return false;
            }
        };
    }

    static class SessionStoreIterator<K, AGG> implements KeyValueIterator<Windowed<K>, AGG> {

        private final KeyValueIterator<Bytes, byte[]> bytesIterator;
        private final StateSerdes<K, AGG> serdes;

        SessionStoreIterator(final KeyValueIterator<Bytes, byte[]> bytesIterator, final StateSerdes<K, AGG> serdes) {
            this.bytesIterator = bytesIterator;
            this.serdes = serdes;
        }

        @Override
        public void close() {
            bytesIterator.close();
        }

        @Override
        public Windowed<K> peekNextKey() {
            final Bytes bytes = bytesIterator.peekNextKey();
            return SessionKeyBinaryConverter.from(bytes.get(), serdes.keyDeserializer());
        }

        @Override
        public boolean hasNext() {
            return bytesIterator.hasNext();
        }

        @Override
        public KeyValue<Windowed<K>, AGG> next() {
            final KeyValue<Bytes, byte[]> next = bytesIterator.next();
            return KeyValue.pair(SessionKeyBinaryConverter.from(next.key.get(), serdes.keyDeserializer()), serdes.valueFrom(next.value));
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove not supported by SessionStoreIterator");
        }
    }
}
