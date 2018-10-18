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

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.internals.StateStoreClosedException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.List;
import java.util.Objects;

/**
 * A wrapper over the underlying {@link ReadOnlyKeyValueStore}s found in a {@link
 * org.apache.kafka.streams.processor.internals.ProcessorTopology}
 *
 * @param <K> key type
 * @param <V> value type
 */
public class CompositeReadOnlyKeyValueStore<K, V> implements ReadOnlyKeyValueStore<K, V> {

    private final StateStoreProvider storeProvider;
    private final QueryableStoreType<ReadOnlyKeyValueStore<K, V>> storeType;
    private final String storeName;
    private final KafkaStreams streams;

    public CompositeReadOnlyKeyValueStore(final KafkaStreams streams,
                                          final StateStoreProvider storeProvider,
                                          final QueryableStoreType<ReadOnlyKeyValueStore<K, V>> keyValueStoreType,
                                          final String storeName) {
        this.streams = streams;
        this.storeProvider = storeProvider;
        this.storeType = keyValueStoreType;
        this.storeName = storeName;
    }

    private List<ReadOnlyKeyValueStore<K, V>> getStores() {
        try {
            return storeProvider.stores(storeName, storeType);
        } catch (final InvalidStateStoreException e) {
            StateStoreUtils.handleInvalidStateStoreException(streams, e);
        }
        return null;
    }


    @Override
    public V get(final K key) {
        Objects.requireNonNull(key);
        final List<ReadOnlyKeyValueStore<K, V>> stores = getStores();
        for (final ReadOnlyKeyValueStore<K, V> store : stores) {
            try {
                final V result = store.get(key);
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
    public KeyValueIterator<K, V> range(final K from, final K to) {
        Objects.requireNonNull(from);
        Objects.requireNonNull(to);
        final NextIteratorFunction<K, V, ReadOnlyKeyValueStore<K, V>> nextIteratorFunction = new NextIteratorFunction<K, V, ReadOnlyKeyValueStore<K, V>>() {
            @Override
            public KeyValueIterator<K, V> apply(final ReadOnlyKeyValueStore<K, V> store) {
                return store.range(from, to);
            }
        };
        final List<ReadOnlyKeyValueStore<K, V>> stores = getStores();
        return new DelegatingPeekingKeyValueIterator<>(streams, storeName,
                                                       new CompositeKeyValueIterator<>(stores.iterator(), nextIteratorFunction));
    }

    @Override
    public KeyValueIterator<K, V> all() {
        final NextIteratorFunction<K, V, ReadOnlyKeyValueStore<K, V>> nextIteratorFunction = new NextIteratorFunction<K, V, ReadOnlyKeyValueStore<K, V>>() {
            @Override
            public KeyValueIterator<K, V> apply(final ReadOnlyKeyValueStore<K, V> store) {
                try {
                    return store.all();
                } catch (final StateStoreClosedException e) {
                    StateStoreUtils.handleStateStoreClosedException(streams, storeName, e);
                }
                return null;
            }
        };
        final List<ReadOnlyKeyValueStore<K, V>> stores = getStores();
        return new DelegatingPeekingKeyValueIterator<>(streams, storeName,
                                                       new CompositeKeyValueIterator<>(stores.iterator(), nextIteratorFunction));
    }

    @Override
    public long approximateNumEntries() {
        final List<ReadOnlyKeyValueStore<K, V>> stores = getStores();
        long total = 0;
        for (final ReadOnlyKeyValueStore<K, V> store : stores) {
            try {
                total += store.approximateNumEntries();
                if (total < 0) {
                    return Long.MAX_VALUE;
                }
            } catch (final StateStoreClosedException e) {
                StateStoreUtils.handleStateStoreClosedException(streams, storeName, e);
            }
        }
        return total;
    }


}

