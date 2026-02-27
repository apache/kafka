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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;

import java.util.List;
import java.util.Map;

/**
 * A wrapper class for non-windowed key-value stores used within the DSL. All such stores are
 * instances of either {@link TimestampedKeyValueStore}, {@link TimestampedKeyValueStoreWithHeaders},
 * or {@link VersionedKeyValueStore}.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public class KeyValueStoreWrapper<K, V> implements StateStore {

    public static final long PUT_RETURN_CODE_IS_LATEST
        = VersionedKeyValueStore.PUT_RETURN_CODE_VALID_TO_UNDEFINED;

    private TimestampedKeyValueStoreWithHeaders<K, V> headersStore = null;
    private VersionedKeyValueStore<K, V> versionedStore = null;

    // same as either timestampedStore or versionedStore above. kept merely as a convenience
    // to simplify implementation for methods which do not depend on store type.
    private StateStore store;

    public KeyValueStoreWrapper(final ProcessorContext<?, ?> context, final String storeName) {
        // Try headers-aware store first, then regular timestamped store, then versioned store
        try {
            // first try headers-aware timestamped store
            headersStore = context.getStateStore(storeName);
            store = headersStore;
            return;
        } catch (final ClassCastException e) {
            // ignore since could be regular timestamped or versioned store instead
        }

        try {
            // next try regular timestamped store
            headersStore = new TimestampedKeyValueStoreHeadersAdapter<>(context.getStateStore(storeName));
            store = headersStore;
            return;
        } catch (final ClassCastException e) {
            // ignore since could be versioned store instead
        }

        try {
            // finally try versioned store
            versionedStore = context.getStateStore(storeName);
            store = versionedStore;
        } catch (final ClassCastException e) {
            store = context.getStateStore(storeName);
            final String storeType = store == null ? "null" : store.getClass().getName();
            throw new InvalidStateStoreException("KTable source state store must implement either "
                + "TimestampedKeyValueStore, TimestampedKeyValueStoreWithHeaders, or VersionedKeyValueStore. Got: " + storeType);
        }
    }

    public ValueTimestampHeaders<V> get(final K key) {
        if (headersStore != null) {
            return headersStore.get(key);
        }
        if (versionedStore != null) {
            final VersionedRecord<V> versionedRecord = versionedStore.get(key);
            return versionedRecord == null
                ? null
                : ValueTimestampHeaders.make(versionedRecord.value(), versionedRecord.timestamp(), null);
        }
        throw new IllegalStateException("KeyValueStoreWrapper must be initialized with either timestamped, headers, or versioned store");
    }

    public ValueTimestampHeaders<V> get(final K key, final long asOfTimestamp) {
        if (!isVersionedStore()) {
            throw new UnsupportedOperationException("get(key, timestamp) is only supported for versioned stores");
        }
        final VersionedRecord<V> versionedRecord = versionedStore.get(key, asOfTimestamp);
        return versionedRecord == null ? null : ValueTimestampHeaders.make(versionedRecord.value(), versionedRecord.timestamp(), null);
    }

    /**
     * @return {@code -1} if the put record is the latest for its key, and {@code Long.MIN_VALUE}
     *         if the put was rejected (i.e., due to grace period having elapsed for a versioned
     *         store). If neither, any other long value may be returned.
     */
    public long put(final K key, final V value, final long timestamp, final Headers headers) {
        if (headersStore != null) {
            headersStore.put(key, ValueTimestampHeaders.make(value, timestamp, headers));
            return PUT_RETURN_CODE_IS_LATEST;
        }
        if (versionedStore != null) {
            return versionedStore.put(key, value, timestamp);
        }
        throw new IllegalStateException("KeyValueStoreWrapper must be initialized with either timestamped, headers, or versioned store");
    }

    public StateStore store() {
        return store;
    }

    public boolean isVersionedStore() {
        return versionedStore != null;
    }

    @Override
    public String name() {
        return store.name();
    }

    @Override
    public void init(final StateStoreContext stateStoreContext, final StateStore root) {
        store.init(stateStoreContext, root);
    }

    @Override
    public void commit(final Map<TopicPartition, Long> changelogOffsets) {
        store.commit(changelogOffsets);
    }

    @Override
    public void close() {
        store.close();
    }

    @Override
    public boolean persistent() {
        return store.persistent();
    }

    @Override
    public boolean isOpen() {
        return store.isOpen();
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query, final PositionBound positionBound, final QueryConfig config) {
        return store.query(query, positionBound, config);
    }

    @Override
    public Position getPosition() {
        return store.getPosition();
    }

    /**
     * Adapter that makes a TimestampedKeyValueStore appear as a
     * TimestampedKeyValueStoreWithHeaders by adding null headers support.
     */
    private static final class TimestampedKeyValueStoreHeadersAdapter<K, V>
        extends WrappedStateStore<TimestampedKeyValueStore<K, V>, K, ValueTimestampHeaders<V>>
        implements TimestampedKeyValueStoreWithHeaders<K, V> {

        public TimestampedKeyValueStoreHeadersAdapter(final TimestampedKeyValueStore<K, V> store) {
            super(store);
        }

        @Override
        public void put(final K key, final ValueTimestampHeaders<V> value) {
            if (value == null) {
                wrapped().put(key, null);
            } else {
                wrapped().put(key, ValueAndTimestamp.make(value.value(), value.timestamp()));
            }
        }

        @Override
        public ValueTimestampHeaders<V> putIfAbsent(final K key, final ValueTimestampHeaders<V> value) {
            final ValueAndTimestamp<V> valueAndTimestamp = value == null
                ? wrapped().putIfAbsent(key, null)
                : wrapped().putIfAbsent(key, ValueAndTimestamp.make(value.value(), value.timestamp()));
            return valueAndTimestamp == null
                ? null
                : ValueTimestampHeaders.make(valueAndTimestamp.value(), valueAndTimestamp.timestamp(), null);
        }

        @Override
        public void putAll(final List<KeyValue<K, ValueTimestampHeaders<V>>> entries) {
            final List<KeyValue<K, ValueAndTimestamp<V>>> convertedEntries = new java.util.ArrayList<>(entries.size());
            for (final KeyValue<K, ValueTimestampHeaders<V>> entry : entries) {
                if (entry.value == null) {
                    convertedEntries.add(KeyValue.pair(entry.key, null));
                } else {
                    convertedEntries.add(KeyValue.pair(
                        entry.key,
                        ValueAndTimestamp.make(entry.value.value(), entry.value.timestamp())
                    ));
                }
            }
            wrapped().putAll(convertedEntries);
        }

        @Override
        public ValueTimestampHeaders<V> delete(final K key) {
            final ValueAndTimestamp<V> deleted = wrapped().delete(key);
            return deleted == null
                ? null
                : ValueTimestampHeaders.make(deleted.value(), deleted.timestamp(), null);
        }

        @Override
        public ValueTimestampHeaders<V> get(final K key) {
            final ValueAndTimestamp<V> result = wrapped().get(key);
            return result == null
                ? null
                : ValueTimestampHeaders.make(result.value(), result.timestamp(), null);
        }

        @Override
        public KeyValueIterator<K, ValueTimestampHeaders<V>> range(final K from, final K to) {
            return new HeadersAdapterIterator<>(wrapped().range(from, to));
        }

        @Override
        public KeyValueIterator<K, ValueTimestampHeaders<V>> all() {
            return new HeadersAdapterIterator<>(wrapped().all());
        }

        @Override
        public long approximateNumEntries() {
            return wrapped().approximateNumEntries();
        }

        @Override
        @SuppressWarnings("deprecation")
        public void flush() {
            wrapped().flush();
        }

        /**
         * Intercepts the cache flush listener to convert between ValueTimestampHeaders and ValueAndTimestamp.
         * This allows the DSL to always use headers-aware listeners, while the adapter handles conversion.
         */
        @Override
        @SuppressWarnings("unchecked")
        public boolean setFlushListener(final CacheFlushListener<K, ValueTimestampHeaders<V>> listener,
                                        final boolean sendOldValues) {
            // Create an adapter that converts ValueAndTimestamp cache records to ValueTimestampHeaders
            final CacheFlushListener<K, ValueAndTimestamp<V>> convertingListener = record -> {
                // Convert Change<ValueAndTimestamp<V>> to Change<ValueTimestampHeaders<V>>
                final Change<ValueAndTimestamp<V>> originalChange = record.value();
                final ValueTimestampHeaders<V> newValueWithHeaders = originalChange.newValue == null
                    ? null
                    : ValueTimestampHeaders.make(originalChange.newValue.value(), originalChange.newValue.timestamp(), null);
                final ValueTimestampHeaders<V> oldValueWithHeaders = originalChange.oldValue == null
                    ? null
                    : ValueTimestampHeaders.make(originalChange.oldValue.value(), originalChange.oldValue.timestamp(), null);

                final Change<ValueTimestampHeaders<V>> convertedChange =
                    new Change<>(newValueWithHeaders, oldValueWithHeaders, originalChange.isLatest);

                // Forward to the headers-aware listener with converted data
                listener.apply(record.withValue(convertedChange));
            };

            // Cast and delegate to the wrapped store's setFlushListener
            if (wrapped() instanceof CachedStateStore) {
                return ((CachedStateStore<K, ValueAndTimestamp<V>>) wrapped()).setFlushListener(convertingListener, sendOldValues);
            }
            return false;
        }

        /**
         * Iterator adapter that wraps ValueAndTimestamp with empty headers.
         */
        private static class HeadersAdapterIterator<K, V> implements KeyValueIterator<K, ValueTimestampHeaders<V>> {
            private final KeyValueIterator<K, ValueAndTimestamp<V>> delegate;

            HeadersAdapterIterator(final KeyValueIterator<K, ValueAndTimestamp<V>> delegate) {
                this.delegate = delegate;
            }

            @Override
            public void close() {
                delegate.close();
            }

            @Override
            public K peekNextKey() {
                return delegate.peekNextKey();
            }

            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public KeyValue<K, ValueTimestampHeaders<V>> next() {
                final KeyValue<K, ValueAndTimestamp<V>> next = delegate.next();
                return KeyValue.pair(
                    next.key,
                    ValueTimestampHeaders.make(next.value.value(), next.value.timestamp(), null)
                );
            }
        }
    }
}
