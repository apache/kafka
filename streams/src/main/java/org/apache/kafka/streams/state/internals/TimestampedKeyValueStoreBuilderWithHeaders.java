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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.HeadersBytesStore;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

import java.util.List;
import java.util.Objects;

/**
 * Builder for {@link TimestampedKeyValueStoreWithHeaders} instances.
 *
 * This is analogous to {@link TimestampedKeyValueStoreBuilder}, but uses
 * {@link ValueTimestampHeaders} as the value wrapper and wires up the
 * header-aware store stack (change-logging, caching, metering).
 */
public class TimestampedKeyValueStoreBuilderWithHeaders<K, V>
    extends AbstractStoreBuilder<K, ValueTimestampHeaders<V>, TimestampedKeyValueStoreWithHeaders<K, V>> {

    private final KeyValueBytesStoreSupplier storeSupplier;

    public TimestampedKeyValueStoreBuilderWithHeaders(final KeyValueBytesStoreSupplier storeSupplier,
                                                      final Serde<K> keySerde,
                                                      final Serde<V> valueSerde,
                                                      final Time time) {
        super(
            storeSupplier.name(),
            keySerde,
            valueSerde == null ? null : new ValueTimestampHeadersSerde<>(valueSerde),
            time
        );
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        Objects.requireNonNull(storeSupplier.metricsScope(), "storeSupplier's metricsScope can't be null");
        this.storeSupplier = storeSupplier;
    }

    @Override
    public TimestampedKeyValueStoreWithHeaders<K, V> build() {
        KeyValueStore<Bytes, byte[]> store = storeSupplier.get();

        if (!(store instanceof HeadersBytesStore)) {
            if (store.persistent()) {
                store = new TimestampedToHeadersStoreAdapter(store);
            } else {
                store = new InMemoryTimestampedKeyValueStoreWithHeadersMarker(store);
            }
        }

        return new MeteredTimestampedKeyValueStoreWithHeaders<>(
            maybeWrapCaching(maybeWrapLogging(store)),
            storeSupplier.metricsScope(),
            time,
            keySerde,
            valueSerde
        );
    }

    private KeyValueStore<Bytes, byte[]> maybeWrapCaching(final KeyValueStore<Bytes, byte[]> inner) {
        if (!enableCaching) {
            return inner;
        }
        return new CachingKeyValueStore(inner, CachingKeyValueStore.CacheType.TIMESTAMPED_KEY_VALUE_STORE_WITH_HEADERS);
    }

    private KeyValueStore<Bytes, byte[]> maybeWrapLogging(final KeyValueStore<Bytes, byte[]> inner) {
        if (!enableLogging) {
            return inner;
        }
        return new ChangeLoggingTimestampedKeyValueBytesStoreWithHeaders(inner);
    }

    private static final class InMemoryTimestampedKeyValueStoreWithHeadersMarker
        extends WrappedStateStore<KeyValueStore<Bytes, byte[]>, Bytes, byte[]>
        implements KeyValueStore<Bytes, byte[]>, HeadersBytesStore {

        private InMemoryTimestampedKeyValueStoreWithHeadersMarker(final KeyValueStore<Bytes, byte[]> wrapped) {
            super(wrapped);
            if (wrapped.persistent()) {
                throw new IllegalArgumentException("Provided store must not be a persistent store, but it is.");
            }
        }

        @Override
        public void put(final Bytes key,
                        final byte[] value) {
            wrapped().put(key, value);
        }

        @Override
        public byte[] putIfAbsent(final Bytes key,
                                  final byte[] value) {
            return wrapped().putIfAbsent(key, value);
        }

        @Override
        public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
            wrapped().putAll(entries);
        }

        @Override
        public byte[] delete(final Bytes key) {
            return wrapped().delete(key);
        }

        @Override
        public byte[] get(final Bytes key) {
            return wrapped().get(key);
        }

        @Override
        public KeyValueIterator<Bytes, byte[]> range(final Bytes from,
                                                     final Bytes to) {
            return wrapped().range(from, to);
        }

        @Override
        public KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from,
                                                            final Bytes to) {
            return wrapped().reverseRange(from, to);
        }

        @Override
        public KeyValueIterator<Bytes, byte[]> all() {
            return wrapped().all();
        }

        @Override
        public KeyValueIterator<Bytes, byte[]> reverseAll() {
            return wrapped().reverseAll();
        }

        @Override
        public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(final P prefix,
                                                                                        final PS prefixKeySerializer) {
            return wrapped().prefixScan(prefix, prefixKeySerializer);
        }

        @Override
        public long approximateNumEntries() {
            return wrapped().approximateNumEntries();
        }

        @Override
        public <R> QueryResult<R> query(final Query<R> query,
                                        final PositionBound positionBound,
                                        final QueryConfig config) {

            throw new UnsupportedOperationException("Queries (IQv2) are not supported by timestamped key-value stores with headers yet.");
        }

        @Override
        public Position getPosition() {
            return wrapped().getPosition();
        }

        @Override
        public boolean persistent() {
            return false;
        }
    }
}