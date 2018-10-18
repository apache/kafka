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

import java.time.Instant;
import org.apache.kafka.streams.internals.ApiUtils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.internals.StateStoreClosedException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.util.List;
import java.util.Objects;

/**
 * Wrapper over the underlying {@link ReadOnlyWindowStore}s found in a {@link
 * org.apache.kafka.streams.processor.internals.ProcessorTopology}
 */
public class CompositeReadOnlyWindowStore<K, V> implements ReadOnlyWindowStore<K, V> {

    private final QueryableStoreType<ReadOnlyWindowStore<K, V>> storeType;
    private final String storeName;
    private final StateStoreProvider provider;
    private final KafkaStreams streams;

    public CompositeReadOnlyWindowStore(final KafkaStreams streams,
                                        final StateStoreProvider provider,
                                        final QueryableStoreType<ReadOnlyWindowStore<K, V>> windowStoreType,
                                        final String storeName) {
        this.streams = streams;
        this.provider = provider;
        this.storeType = windowStoreType;
        this.storeName = storeName;
    }

    private List<ReadOnlyWindowStore<K, V>> getStores() {
        try {
            return provider.stores(storeName, storeType);
        } catch (final InvalidStateStoreException e) {
            StateStoreUtils.handleInvalidStateStoreException(streams, e);
        }
        return null;
    }

    @Override
    public V fetch(final K key, final long time) {
        Objects.requireNonNull(key, "key can't be null");
        final List<ReadOnlyWindowStore<K, V>> stores = getStores();
        for (final ReadOnlyWindowStore<K, V> windowStore : stores) {
            try {
                final V result = windowStore.fetch(key, time);
                if (result != null) {
                    return result;
                }
            } catch (final StateStoreClosedException e) {
                StateStoreUtils.handleStateStoreClosedException(streams, storeName, e);
            }
        }
        return null;
    }

    @Override
    @Deprecated
    public WindowStoreIterator<V> fetch(final K key, final long timeFrom, final long timeTo) {
        Objects.requireNonNull(key, "key can't be null");
        final List<ReadOnlyWindowStore<K, V>> stores = getStores();
        for (final ReadOnlyWindowStore<K, V> windowStore : stores) {
            try {
                final WindowStoreIterator<V> result = windowStore.fetch(key, timeFrom, timeTo);
                if (!result.hasNext()) {
                    result.close();
                } else {
                    return result;
                }
            } catch (final StateStoreClosedException e) {
                StateStoreUtils.handleStateStoreClosedException(streams, storeName, e);
            }
        }
        return KeyValueIterators.emptyWindowStoreIterator();
    }

    @Override
    public WindowStoreIterator<V> fetch(final K key, final Instant from, final Instant to) throws IllegalArgumentException {
        ApiUtils.validateMillisecondInstant(from, "from");
        ApiUtils.validateMillisecondInstant(to, "to");
        return fetch(key, from.toEpochMilli(), to.toEpochMilli());
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K from, final K to, final long timeFrom, final long timeTo) {
        Objects.requireNonNull(from, "from can't be null");
        Objects.requireNonNull(to, "to can't be null");
        final NextIteratorFunction<Windowed<K>, V, ReadOnlyWindowStore<K, V>> nextIteratorFunction = new NextIteratorFunction<Windowed<K>, V, ReadOnlyWindowStore<K, V>>() {
            @Override
            public KeyValueIterator<Windowed<K>, V> apply(final ReadOnlyWindowStore<K, V> store) {
                return store.fetch(from, to, timeFrom, timeTo);
            }
        };
        return new DelegatingPeekingKeyValueIterator<>(streams, storeName,
                                                       new CompositeKeyValueIterator<>(
                                                               getStores().iterator(),
                                                               nextIteratorFunction));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K from, final K to, final Instant fromTime, final Instant toTime) throws IllegalArgumentException {
        ApiUtils.validateMillisecondInstant(fromTime, "fromTime");
        ApiUtils.validateMillisecondInstant(toTime, "toTime");
        return fetch(from, to, fromTime.toEpochMilli(), toTime.toEpochMilli());
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> all() {
        final NextIteratorFunction<Windowed<K>, V, ReadOnlyWindowStore<K, V>> nextIteratorFunction = new NextIteratorFunction<Windowed<K>, V, ReadOnlyWindowStore<K, V>>() {
            @Override
            public KeyValueIterator<Windowed<K>, V> apply(final ReadOnlyWindowStore<K, V> store) {
                return store.all();
            }
        };
        return new DelegatingPeekingKeyValueIterator<>(streams, storeName,
                new CompositeKeyValueIterator<>(
                        getStores().iterator(),
                        nextIteratorFunction));
    }
    
    @Override
    @Deprecated
    public KeyValueIterator<Windowed<K>, V> fetchAll(final long timeFrom, final long timeTo) {
        final NextIteratorFunction<Windowed<K>, V, ReadOnlyWindowStore<K, V>> nextIteratorFunction = new NextIteratorFunction<Windowed<K>, V, ReadOnlyWindowStore<K, V>>() {
            @Override
            public KeyValueIterator<Windowed<K>, V> apply(final ReadOnlyWindowStore<K, V> store) {
                return store.fetchAll(timeFrom, timeTo);
            }
        };
        return new DelegatingPeekingKeyValueIterator<>(streams, storeName,
                new CompositeKeyValueIterator<>(
                        getStores().iterator(),
                        nextIteratorFunction));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetchAll(final Instant from, final Instant to) throws IllegalArgumentException {
        ApiUtils.validateMillisecondInstant(from, "from");
        ApiUtils.validateMillisecondInstant(to, "to");
        return fetchAll(from.toEpochMilli(), to.toEpochMilli());
    }
}
