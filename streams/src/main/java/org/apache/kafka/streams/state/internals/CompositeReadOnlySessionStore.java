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

    public CompositeReadOnlySessionStore(final StateStoreProvider storeProvider,
                                         final QueryableStoreType<ReadOnlySessionStore<K, V>> queryableStoreType,
                                         final String storeName) {
        this.storeProvider = storeProvider;
        this.queryableStoreType = queryableStoreType;
        this.storeName = storeName;
    }


    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K key) {
        Objects.requireNonNull(key, "key can't be null");
        final List<ReadOnlySessionStore<K, V>> stores = storeProvider.stores(storeName, queryableStoreType);
        for (final ReadOnlySessionStore<K, V> store : stores) {
            try {
                final KeyValueIterator<Windowed<K>, V> result = store.fetch(key);
                if (!result.hasNext()) {
                    result.close();
                } else {
                    return result;
                }
            } catch (final InvalidStateStoreException ise) {
                throw new InvalidStateStoreException("State store  [" + storeName + "] is not available anymore" +
                                                             " and may have been migrated to another instance; " +
                                                             "please re-discover its location from the state metadata.");
            }
        }
        return KeyValueIterators.emptyIterator();
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K from, final K to) {
        Objects.requireNonNull(from, "from can't be null");
        Objects.requireNonNull(to, "to can't be null");
        final NextIteratorFunction<Windowed<K>, V, ReadOnlySessionStore<K, V>> nextIteratorFunction = new NextIteratorFunction<Windowed<K>, V, ReadOnlySessionStore<K, V>>() {
            @Override
            public KeyValueIterator<Windowed<K>, V> apply(final ReadOnlySessionStore<K, V> store) {
                return store.fetch(from, to);
            }
        };
        return new DelegatingPeekingKeyValueIterator<>(storeName,
                                                       new CompositeKeyValueIterator<>(
                                                               storeProvider.stores(storeName, queryableStoreType).iterator(),
                                                               nextIteratorFunction));
    }
}
