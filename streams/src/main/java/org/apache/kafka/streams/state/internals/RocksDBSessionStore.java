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
import org.apache.kafka.streams.kstream.internals.SessionKeySerde;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StateSerdes;


class RocksDBSessionStore<K, AGG> implements SessionStore<K, AGG> {

    private final Serde<K> keySerde;
    private final Serde<AGG> aggSerde;
    protected final SegmentedBytesStore bytesStore;

    protected StateSerdes<K, AGG> serdes;

    RocksDBSessionStore(final SegmentedBytesStore bytesStore,
                        final Serde<K> keySerde,
                        final Serde<AGG> aggSerde) {
        this.keySerde = keySerde;
        this.bytesStore = bytesStore;
        this.aggSerde = aggSerde;
    }

    // this is optimizing the case when this store is already a bytes store, in which we can avoid Bytes.wrap() costs
    private static class RocksDBSessionBytesStore extends RocksDBSessionStore<Bytes, byte[]> {
        RocksDBSessionBytesStore(final SegmentedBytesStore inner) {
            super(inner, Serdes.Bytes(), Serdes.ByteArray());
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes key, final long earliestSessionEndTime, final long latestSessionStartTime) {
            final KeyValueIterator<Bytes, byte[]> bytesIterator = bytesStore.fetch(key, earliestSessionEndTime, latestSessionStartTime);
            return SessionStoreIterator.bytesIterator(bytesIterator);
        }

        @Override
        public void remove(final Windowed<Bytes> key) {
            bytesStore.remove(SessionKeySerde.bytesToBinary(key));
        }

        @Override
        public void put(final Windowed<Bytes> sessionKey, final byte[] aggregate) {
            bytesStore.put(SessionKeySerde.bytesToBinary(sessionKey), aggregate);
        }
    }

    static RocksDBSessionStore<Bytes, byte[]> bytesStore(final SegmentedBytesStore inner) {
        return new RocksDBSessionBytesStore(inner);
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> findSessions(final K key, final long earliestSessionEndTime, final long latestSessionStartTime) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = bytesStore.fetch(Bytes.wrap(serdes.rawKey(key)), earliestSessionEndTime, latestSessionStartTime);
        return new SessionStoreIterator<>(bytesIterator, serdes);
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> fetch(final K key) {
        return findSessions(key, 0, Long.MAX_VALUE);
    }

    @Override
    public void remove(final Windowed<K> key) {
        bytesStore.remove(SessionKeySerde.toBinary(key, serdes.keySerializer()));
    }

    @Override
    public void put(final Windowed<K> sessionKey, final AGG aggregate) {
        bytesStore.put(SessionKeySerde.toBinary(sessionKey, serdes.keySerializer()), aggSerde.serializer().serialize(bytesStore.name(), aggregate));
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

    private static class SessionStoreIterator<K, AGG> implements KeyValueIterator<Windowed<K>, AGG> {

        protected final KeyValueIterator<Bytes, byte[]> bytesIterator;
        private final StateSerdes<K, AGG> serdes;

        // this is optimizing the case when underlying is already a bytes store iterator, in which we can avoid Bytes.wrap() costs
        private static class SessionStoreBytesIterator extends SessionStoreIterator<Bytes, byte[]> {
            SessionStoreBytesIterator(final KeyValueIterator<Bytes, byte[]> underlying) {
                super(underlying, null);
            }

            @Override
            public Windowed<Bytes> peekNextKey() {
                final Bytes key = bytesIterator.peekNextKey();

                return SessionKeySerde.fromBytes(key);
            }

            @Override
            public KeyValue<Windowed<Bytes>, byte[]> next() {
                final KeyValue<Bytes, byte[]> next = bytesIterator.next();
                return KeyValue.pair(SessionKeySerde.fromBytes(next.key), next.value);
            }
        }

        static SessionStoreIterator<Bytes, byte[]> bytesIterator(final KeyValueIterator<Bytes, byte[]> underlying) {
            return new SessionStoreBytesIterator(underlying);
        }

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
            return SessionKeySerde.from(bytes.get(), serdes.keyDeserializer());
        }

        @Override
        public boolean hasNext() {
            return bytesIterator.hasNext();
        }

        @Override
        public KeyValue<Windowed<K>, AGG> next() {
            final KeyValue<Bytes, byte[]> next = bytesIterator.next();
            return KeyValue.pair(SessionKeySerde.from(next.key.get(), serdes.keyDeserializer()), serdes.valueFrom(next.value));
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove() not supported by SessionStoreIterator.");
        }
    }
}
