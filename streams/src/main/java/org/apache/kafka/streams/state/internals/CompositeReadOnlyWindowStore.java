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
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.util.List;

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

    private interface Fetcher<K, V, IteratorType extends KeyValueIterator<?, V>> {
        IteratorType fetch(ReadOnlyWindowStore<K, V> store);
        IteratorType empty();
    }

    public <IteratorType extends KeyValueIterator<?, V>> IteratorType fetch(Fetcher<K, V, IteratorType> fetcher) {
        final List<ReadOnlyWindowStore<K, V>> stores = provider.stores(storeName, windowStoreType);
        for (ReadOnlyWindowStore<K, V> windowStore : stores) {
            try {
                final IteratorType result = fetcher.fetch(windowStore);
                if (!result.hasNext()) {
                    result.close();
                } else {
                    return result;
                }
            } catch (InvalidStateStoreException e) {
                throw new InvalidStateStoreException(
                    "State store is not available anymore and may have been migrated to another instance; " +
                    "please re-discover its location from the state metadata.");
            }
        }

        return fetcher.empty();
    }

    @Override
    public WindowStoreIterator<V> fetch(final K key, final long timeFrom, final long timeTo) {
        return fetch(new Fetcher<K, V, WindowStoreIterator<V>>() {
            @Override
            public WindowStoreIterator<V> fetch(ReadOnlyWindowStore<K, V> store) {
                return store.fetch(key, timeFrom, timeTo);
            }

            @Override
            public WindowStoreIterator<V> empty() {
                return KeyValueIterators.emptyWindowStoreIterator();
            }
        });
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K from, final K to, final long timeFrom, final long timeTo) {
        return fetch(new Fetcher<K, V, KeyValueIterator<Windowed<K>, V>>() {
            @Override
            public KeyValueIterator<Windowed<K>, V> fetch(ReadOnlyWindowStore<K, V> store) {
                return store.fetch(from, to, timeFrom, timeTo);
            }

            @Override
            public KeyValueIterator<Windowed<K>, V> empty() {
                return KeyValueIterators.emptyIterator();
            }
        });
    }
}
