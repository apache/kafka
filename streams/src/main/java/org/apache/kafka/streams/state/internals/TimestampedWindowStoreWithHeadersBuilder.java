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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.HeadersBytesStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Store builder for {@link TimestampedWindowStoreWithHeaders}.
 * <p>
 * This builder creates header-aware timestamped window stores that preserve record headers
 * alongside values and timestamps. It wraps the underlying bytes store with the necessary
 * layers (logging, caching, metering) to provide a fully-functional store.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class TimestampedWindowStoreWithHeadersBuilder<K, V>
    extends AbstractStoreBuilder<K, ValueTimestampHeaders<V>, TimestampedWindowStoreWithHeaders<K, V>> {

    private static final Logger LOG = LoggerFactory.getLogger(TimestampedWindowStoreWithHeadersBuilder.class);

    private final WindowBytesStoreSupplier storeSupplier;

    public TimestampedWindowStoreWithHeadersBuilder(final WindowBytesStoreSupplier storeSupplier,
                                                    final Serde<K> keySerde,
                                                    final Serde<V> valueSerde,
                                                    final Time time) {
        super(storeSupplier.name(), keySerde, valueSerde == null ? null : new ValueTimestampHeadersSerde<>(valueSerde), time);
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        Objects.requireNonNull(storeSupplier.metricsScope(), "storeSupplier's metricsScope can't be null");
        this.storeSupplier = storeSupplier;
    }

    @Override
    public TimestampedWindowStoreWithHeaders<K, V> build() {
        WindowStore<Bytes, byte[]> store = storeSupplier.get();

        if (!(store instanceof HeadersBytesStore)) {
            if (store.persistent()) {
                store = new TimestampedToHeadersWindowStoreAdapter(store);
            } else {
                store = new InMemoryTimestampedWindowStoreWithHeadersMarker(store);
            }
        }

        if (storeSupplier.retainDuplicates() && enableCaching) {
            LOG.warn("Disabling caching for {} since store was configured to retain duplicates", storeSupplier.name());
            enableCaching = false;
        }

        return new MeteredTimestampedWindowStoreWithHeaders<>(
            maybeWrapCaching(maybeWrapLogging(store)),
            storeSupplier.windowSize(),
            storeSupplier.metricsScope(),
            time,
            keySerde,
            valueSerde);
    }

    private WindowStore<Bytes, byte[]> maybeWrapCaching(final WindowStore<Bytes, byte[]> inner) {
        if (!enableCaching) {
            return inner;
        }

        final boolean isTimeOrdered = isTimeOrderedStore(inner);
        if (isTimeOrdered) {
            return new TimeOrderedCachingWindowStore(
                inner,
                storeSupplier.windowSize(),
                storeSupplier.segmentIntervalMs());
        }

        return new CachingWindowStore(
            inner,
            storeSupplier.windowSize(),
            storeSupplier.segmentIntervalMs());
    }

    private boolean isTimeOrderedStore(final StateStore stateStore) {
        if (stateStore instanceof RocksDBTimeOrderedWindowStore) {
            return true;
        }
        if (stateStore instanceof WrappedStateStore) {
            return isTimeOrderedStore(((WrappedStateStore<?, ?, ?>) stateStore).wrapped());
        }
        return false;
    }

    private WindowStore<Bytes, byte[]> maybeWrapLogging(final WindowStore<Bytes, byte[]> inner) {
        if (!enableLogging) {
            return inner;
        }
        return new ChangeLoggingTimestampedWindowBytesStoreWithHeaders(inner, storeSupplier.retainDuplicates());
    }

    public long retentionPeriod() {
        return storeSupplier.retentionPeriod();
    }

    /**
     * Marker wrapper for in-memory window stores that support both timestamps and headers.
     * <p>
     * This wrapper indicates that the underlying store understands the value-with-headers format.
     * The actual in-memory store doesn't need to change since it operates on raw bytes.
     */
    private static final class InMemoryTimestampedWindowStoreWithHeadersMarker
        extends WrappedStateStore<WindowStore<Bytes, byte[]>, Bytes, byte[]>
        implements WindowStore<Bytes, byte[]>, TimestampedBytesStore, HeadersBytesStore {

        private InMemoryTimestampedWindowStoreWithHeadersMarker(final WindowStore<Bytes, byte[]> wrapped) {
            super(wrapped);
            if (wrapped.persistent()) {
                throw new IllegalArgumentException("Provided store must not be a persistent store, but it is.");
            }
        }

        @Override
        public void put(final Bytes key,
                        final byte[] value,
                        final long windowStartTimestamp) {
            wrapped().put(key, value, windowStartTimestamp);
        }

        @Override
        public byte[] fetch(final Bytes key,
                            final long time) {
            return wrapped().fetch(key, time);
        }

        @Override
        public WindowStoreIterator<byte[]> fetch(final Bytes key,
                                                 final long timeFrom,
                                                 final long timeTo) {
            return wrapped().fetch(key, timeFrom, timeTo);
        }

        @Override
        public WindowStoreIterator<byte[]> backwardFetch(final Bytes key,
                                                         final long timeFrom,
                                                         final long timeTo) {
            return wrapped().backwardFetch(key, timeFrom, timeTo);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom,
                                                               final Bytes keyTo,
                                                               final long timeFrom,
                                                               final long timeTo) {
            return wrapped().fetch(keyFrom, keyTo, timeFrom, timeTo);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom,
                                                                       final Bytes keyTo,
                                                                       final long timeFrom,
                                                                       final long timeTo) {
            return wrapped().backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom,
                                                                  final long timeTo) {
            return wrapped().fetchAll(timeFrom, timeTo);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final long timeFrom,
                                                                          final long timeTo) {
            return wrapped().backwardFetchAll(timeFrom, timeTo);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
            return wrapped().all();
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
            return wrapped().backwardAll();
        }

        @Override
        public <R> QueryResult<R> query(final Query<R> query,
                                        final PositionBound positionBound,
                                        final QueryConfig config) {
            throw new UnsupportedOperationException("Queries (IQv2) are not supported for timestamped window stores with headers yet.");
        }

        @Override
        public boolean persistent() {
            return false;
        }
    }
}
