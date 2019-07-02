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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlySessionStore;

import java.util.List;
import java.util.Objects;

/**
 * Wrapper over the underlying {@link ReadOnlySessionStore}s found in a {@link
 * org.apache.kafka.streams.processor.internals.ProcessorTopology}
 */
public class CompositeReadOnlySessionStore<K, V> implements ReadOnlySessionStore<K, V> {
    private final StateStoreProvider storeProvider;
    private final QueryableStoreType<ReadOnlySessionStore<K, V>> queryableStoreType;
    private final String storeName;
    private final KafkaStreams streams;

    public CompositeReadOnlySessionStore(final KafkaStreams streams, final StateStoreProvider storeProvider,
                                         final QueryableStoreType<ReadOnlySessionStore<K, V>> queryableStoreType,
                                         final String storeName) {
        this.streams = streams;
        this.storeProvider = storeProvider;
        this.queryableStoreType = queryableStoreType;
        this.storeName = storeName;
    }

    private List<ReadOnlySessionStore<K, V>> getStores() {
        try {
            return storeProvider.stores(storeName, queryableStoreType);
        } catch (final InvalidStateStoreException e) {
            throw StateStoreUtils.wrapExceptionFromStateStoreProvider(streams, e);
        }
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K key) {
        Objects.requireNonNull(key, "key can't be null");
        final List<ReadOnlySessionStore<K, V>> stores = getStores();
        for (final ReadOnlySessionStore<K, V> store : stores) {
            try {
                final KeyValueIterator<Windowed<K>, V> result = store.fetch(key);
                if (!result.hasNext()) {
                    result.close();
                } else {
                    return result;
                }
            } catch (final StateStoreClosedException e) {
                throw StateStoreUtils.wrapStateStoreClosedException(streams, e, storeName);
            }
        }
        return KeyValueIterators.emptyIterator();
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K from, final K to) {
        Objects.requireNonNull(from, "from can't be null");
        Objects.requireNonNull(to, "to can't be null");
        final NextIteratorFunction<Windowed<K>, V, ReadOnlySessionStore<K, V>> nextIteratorFunction = store -> {
            try {
                return store.fetch(from, to);
            } catch (final StateStoreClosedException e) {
                throw StateStoreUtils.wrapStateStoreClosedException(streams, e, storeName);
            }
        };
        final List<ReadOnlySessionStore<K, V>> stores = getStores();
        final KeyValueIterator<Windowed<K>, V> iterator = new DelegatingPeekingKeyValueIterator<>(storeName,
                new CompositeKeyValueIterator<>(stores.iterator(), nextIteratorFunction));
        ((ConsumeKafkaStreams) iterator).accept(streams);
        return iterator;
    }
}
