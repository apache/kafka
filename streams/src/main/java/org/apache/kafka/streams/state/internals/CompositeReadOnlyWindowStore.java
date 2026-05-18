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

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Wrapper over the underlying {@link ReadOnlyWindowStore}s found in a {@link
 * org.apache.kafka.streams.processor.internals.ProcessorTopology}
 */
public class CompositeReadOnlyWindowStore<K, V> implements ReadOnlyWindowStore<K, V> {

    private final QueryableStoreType<ReadOnlyWindowStore<K, V>> windowStoreType;
    private final String storeName;
    private final StateStoreProvider provider;
    private final IsolationLevel isolationOverride;

    public CompositeReadOnlyWindowStore(final StateStoreProvider provider,
                                        final QueryableStoreType<ReadOnlyWindowStore<K, V>> windowStoreType,
                                        final String storeName) {
        this(provider, windowStoreType, storeName, null);
    }

    private CompositeReadOnlyWindowStore(final StateStoreProvider provider,
                                         final QueryableStoreType<ReadOnlyWindowStore<K, V>> windowStoreType,
                                         final String storeName,
                                         final IsolationLevel isolationOverride) {
        this.provider = provider;
        this.windowStoreType = windowStoreType;
        this.storeName = storeName;
        this.isolationOverride = isolationOverride;
    }

    @Override
    public ReadOnlyWindowStore<K, V> readOnly(final IsolationLevel isolationLevel) {
        Objects.requireNonNull(isolationLevel, "isolationLevel");
        return new CompositeReadOnlyWindowStore<>(provider, windowStoreType, storeName, isolationLevel);
    }

    private List<ReadOnlyWindowStore<K, V>> readOnlyStores() {
        final IsolationLevel level =
            isolationOverride != null ? isolationOverride : provider.defaultIsolationLevel();
        return provider.stores(storeName, windowStoreType).stream()
            .map(s -> s.readOnly(level))
            .collect(Collectors.toList());
    }

    @Override
    public V fetch(final K key, final long time) {
        Objects.requireNonNull(key, "key can't be null");
        final List<ReadOnlyWindowStore<K, V>> stores = readOnlyStores();
        for (final ReadOnlyWindowStore<K, V> windowStore : stores) {
            try {
                final V result = windowStore.fetch(key, time);
                if (result != null) {
                    return result;
                }
            } catch (final InvalidStateStoreException e) {
                throw new InvalidStateStoreException(
                    "State store is not available anymore and may have been migrated to another instance; " +
                        "please re-discover its location from the state metadata.");
            }
        }
        return null;
    }

    @Override
    public WindowStoreIterator<V> fetch(final K key,
                                        final Instant timeFrom,
                                        final Instant timeTo) {
        Objects.requireNonNull(key, "key can't be null");
        final List<ReadOnlyWindowStore<K, V>> stores = readOnlyStores();
        for (final ReadOnlyWindowStore<K, V> windowStore : stores) {
            try {
                final WindowStoreIterator<V> result = windowStore.fetch(key, timeFrom, timeTo);
                if (!result.hasNext()) {
                    result.close();
                } else {
                    return result;
                }
            } catch (final InvalidStateStoreException e) {
                throw new InvalidStateStoreException(
                    "State store is not available anymore and may have been migrated to another instance; " +
                        "please re-discover its location from the state metadata.");
            }
        }
        return KeyValueIterators.emptyWindowStoreIterator();
    }

    @Override
    public WindowStoreIterator<V> backwardFetch(final K key,
                                                final Instant timeFrom,
                                                final Instant timeTo) throws IllegalArgumentException {
        Objects.requireNonNull(key, "key can't be null");
        final List<ReadOnlyWindowStore<K, V>> stores = readOnlyStores();
        for (final ReadOnlyWindowStore<K, V> windowStore : stores) {
            try {
                final WindowStoreIterator<V> result = windowStore.backwardFetch(key, timeFrom, timeTo);
                if (!result.hasNext()) {
                    result.close();
                } else {
                    return result;
                }
            } catch (final InvalidStateStoreException e) {
                throw new InvalidStateStoreException(
                    "State store is not available anymore and may have been migrated to another instance; " +
                        "please re-discover its location from the state metadata.");
            }
        }
        return KeyValueIterators.emptyWindowStoreIterator();
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K keyFrom,
                                                  final K keyTo,
                                                  final Instant timeFrom,
                                                  final Instant timeTo) {
        final NextIteratorFunction<Windowed<K>, V, ReadOnlyWindowStore<K, V>> nextIteratorFunction =
            store -> store.fetch(keyFrom, keyTo, timeFrom, timeTo);
        return new DelegatingPeekingKeyValueIterator<>(
            storeName,
            new CompositeKeyValueIterator<>(
                readOnlyStores().iterator(),
                nextIteratorFunction));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetch(final K keyFrom,
                                                          final K keyTo,
                                                          final Instant timeFrom,
                                                          final Instant timeTo) throws IllegalArgumentException {
        final NextIteratorFunction<Windowed<K>, V, ReadOnlyWindowStore<K, V>> nextIteratorFunction =
            store -> store.backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
        return new DelegatingPeekingKeyValueIterator<>(
            storeName,
            new CompositeKeyValueIterator<>(
                readOnlyStores().iterator(),
                nextIteratorFunction));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> all() {
        final NextIteratorFunction<Windowed<K>, V, ReadOnlyWindowStore<K, V>> nextIteratorFunction =
            ReadOnlyWindowStore::all;
        return new DelegatingPeekingKeyValueIterator<>(
            storeName,
            new CompositeKeyValueIterator<>(
                readOnlyStores().iterator(),
                nextIteratorFunction));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardAll() {
        final NextIteratorFunction<Windowed<K>, V, ReadOnlyWindowStore<K, V>> nextIteratorFunction =
            ReadOnlyWindowStore::backwardAll;
        return new DelegatingPeekingKeyValueIterator<>(
            storeName,
            new CompositeKeyValueIterator<>(
                readOnlyStores().iterator(),
                nextIteratorFunction));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetchAll(final Instant timeFrom,
                                                     final Instant timeTo) {
        final NextIteratorFunction<Windowed<K>, V, ReadOnlyWindowStore<K, V>> nextIteratorFunction =
            store -> store.fetchAll(timeFrom, timeTo);
        return new DelegatingPeekingKeyValueIterator<>(
            storeName,
            new CompositeKeyValueIterator<>(
                readOnlyStores().iterator(),
                nextIteratorFunction));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetchAll(final Instant timeFrom,
                                                             final Instant timeTo) throws IllegalArgumentException {
        final NextIteratorFunction<Windowed<K>, V, ReadOnlyWindowStore<K, V>> nextIteratorFunction =
            store -> store.backwardFetchAll(timeFrom, timeTo);
        return new DelegatingPeekingKeyValueIterator<>(
            storeName,
            new CompositeKeyValueIterator<>(
                readOnlyStores().iterator(),
                nextIteratorFunction));
    }
}
