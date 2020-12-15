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

import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.internals.ApiUtils;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;

/**
 * Wrapper over the underlying {@link ReadOnlyWindowStore}s found in a {@link
 * org.apache.kafka.streams.processor.internals.ProcessorTopology}
 */
public class CompositeReadOnlyWindowStore<K, V> implements ReadOnlyWindowStore<K, V> {

    private final QueryableStoreType<ReadOnlyWindowStore<K, V>> windowStoreType;
    private final String storeName;
    private final StateStoreProvider provider;

    public CompositeReadOnlyWindowStore(final StateStoreProvider provider,
                                        final QueryableStoreType<ReadOnlyWindowStore<K, V>> windowStoreType,
                                        final String storeName) {
        this.provider = provider;
        this.windowStoreType = windowStoreType;
        this.storeName = storeName;
    }

    @Override
    public V fetch(final K key, final long time) {
        Objects.requireNonNull(key, "key can't be null");
        final List<ReadOnlyWindowStore<K, V>> stores = provider.stores(storeName, windowStoreType);
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
    @Deprecated
    public WindowStoreIterator<V> fetch(final K key,
                                        final long timeFrom,
                                        final long timeTo) {
        Objects.requireNonNull(key, "key can't be null");
        final List<ReadOnlyWindowStore<K, V>> stores = provider.stores(storeName, windowStoreType);
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

    @SuppressWarnings("deprecation") // removing fetch(K from, long from, long to) will fix this
    @Override
    public WindowStoreIterator<V> fetch(final K key,
                                        final Instant timeFrom,
                                        final Instant timeTo) throws IllegalArgumentException {
        return fetch(
            key,
            ApiUtils.validateMillisecondInstant(timeFrom, prepareMillisCheckFailMsgPrefix(timeFrom, "from")),
            ApiUtils.validateMillisecondInstant(timeTo, prepareMillisCheckFailMsgPrefix(timeTo, "to")));
    }

    @Override
    public WindowStoreIterator<V> backwardFetch(final K key,
                                                final Instant timeFrom,
                                                final Instant timeTo) throws IllegalArgumentException {
        Objects.requireNonNull(key, "key can't be null");
        final List<ReadOnlyWindowStore<K, V>> stores = provider.stores(storeName, windowStoreType);
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

    @SuppressWarnings("deprecation") // removing fetch(K from, K to, long from, long to) will fix this
    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K keyFrom,
                                                  final K keyTo,
                                                  final long timeFrom,
                                                  final long timeTo) {
        Objects.requireNonNull(keyFrom, "from can't be null");
        Objects.requireNonNull(keyTo, "to can't be null");
        final NextIteratorFunction<Windowed<K>, V, ReadOnlyWindowStore<K, V>> nextIteratorFunction =
            store -> store.fetch(keyFrom, keyTo, timeFrom, timeTo);
        return new DelegatingPeekingKeyValueIterator<>(
            storeName,
            new CompositeKeyValueIterator<>(
                provider.stores(storeName, windowStoreType).iterator(),
                nextIteratorFunction));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K keyFrom,
                                                  final K keyTo,
                                                  final Instant timeFrom,
                                                  final Instant timeTo) throws IllegalArgumentException {
        return fetch(
            keyFrom,
            keyTo,
            ApiUtils.validateMillisecondInstant(timeFrom, prepareMillisCheckFailMsgPrefix(timeFrom, "timeFrom")),
            ApiUtils.validateMillisecondInstant(timeTo, prepareMillisCheckFailMsgPrefix(timeTo, "timeTo")));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetch(final K keyFrom,
                                                          final K keyTo,
                                                          final Instant timeFrom,
                                                          final Instant timeTo) throws IllegalArgumentException {
        Objects.requireNonNull(keyFrom, "keyFrom can't be null");
        Objects.requireNonNull(keyTo, "keyTo can't be null");
        final NextIteratorFunction<Windowed<K>, V, ReadOnlyWindowStore<K, V>> nextIteratorFunction =
            store -> store.backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
        return new DelegatingPeekingKeyValueIterator<>(
            storeName,
            new CompositeKeyValueIterator<>(
                provider.stores(storeName, windowStoreType).iterator(),
                nextIteratorFunction));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> all() {
        final NextIteratorFunction<Windowed<K>, V, ReadOnlyWindowStore<K, V>> nextIteratorFunction =
            ReadOnlyWindowStore::all;
        return new DelegatingPeekingKeyValueIterator<>(
            storeName,
            new CompositeKeyValueIterator<>(
                provider.stores(storeName, windowStoreType).iterator(),
                nextIteratorFunction));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardAll() {
        final NextIteratorFunction<Windowed<K>, V, ReadOnlyWindowStore<K, V>> nextIteratorFunction =
            ReadOnlyWindowStore::backwardAll;
        return new DelegatingPeekingKeyValueIterator<>(
            storeName,
            new CompositeKeyValueIterator<>(
                provider.stores(storeName, windowStoreType).iterator(),
                nextIteratorFunction));
    }

    @Override
    @Deprecated
    public KeyValueIterator<Windowed<K>, V> fetchAll(final long timeFrom,
                                                     final long timeTo) {
        final NextIteratorFunction<Windowed<K>, V, ReadOnlyWindowStore<K, V>> nextIteratorFunction =
            store -> store.fetchAll(timeFrom, timeTo);
        return new DelegatingPeekingKeyValueIterator<>(
            storeName,
            new CompositeKeyValueIterator<>(
                provider.stores(storeName, windowStoreType).iterator(),
                nextIteratorFunction));
    }

    @SuppressWarnings("deprecation") // removing fetchAll(long from, long to) will fix this
    @Override
    public KeyValueIterator<Windowed<K>, V> fetchAll(final Instant timeFrom,
                                                     final Instant timeTo) throws IllegalArgumentException {
        return fetchAll(
            ApiUtils.validateMillisecondInstant(timeFrom, prepareMillisCheckFailMsgPrefix(timeFrom, "from")),
            ApiUtils.validateMillisecondInstant(timeTo, prepareMillisCheckFailMsgPrefix(timeTo, "to")));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetchAll(final Instant timeFrom,
                                                             final Instant timeTo) throws IllegalArgumentException {
        final NextIteratorFunction<Windowed<K>, V, ReadOnlyWindowStore<K, V>> nextIteratorFunction =
            store -> store.backwardFetchAll(timeFrom, timeTo);
        return new DelegatingPeekingKeyValueIterator<>(
            storeName,
            new CompositeKeyValueIterator<>(
                provider.stores(storeName, windowStoreType).iterator(),
                nextIteratorFunction));
    }
}
