/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class CompositeReadOnlyStore<K, V> implements ReadOnlyKeyValueStore<K, V> {

    private final List<ReadOnlyStoreProvider> storeProviders;
    private final String storeName;

    public CompositeReadOnlyStore(final List<ReadOnlyStoreProvider> storeProviders,
                                  final String storeName) {
        this.storeProviders = storeProviders;
        this.storeName = storeName;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V get(final K key) {
        for (ReadOnlyKeyValueStore<K, V> store : allStores()) {
            V result = store.get(key);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    @Override
    public KeyValueIterator<K, V> range(final K from, final K to) {
        final NextIteratorFunction<K, V> nextIteratorFunction = new NextIteratorFunction<K, V>() {
            @Override
            public KeyValueIterator<K, V> apply(final ReadOnlyKeyValueStore<K, V> store) {
                return store.range(from, to);
            }
        };
        return new CompositeKeyValueIterator(allStores().iterator(), nextIteratorFunction);
    }

    private List<ReadOnlyKeyValueStore<K, V>> allStores() {
        final List<ReadOnlyKeyValueStore<K, V>> allStores = new ArrayList<>();
        for (ReadOnlyStoreProvider provider : storeProviders) {
            final List<ReadOnlyKeyValueStore<K, V>> stores = provider.getStores(storeName);
            allStores.addAll(stores);
        }
        if (allStores.isEmpty()) {
            throw new InvalidStateStoreException("Store " + storeName + " is currently "
                                                 + "unavailable");
        }
        return allStores;
    }

    @Override
    public KeyValueIterator<K, V> all() {
        final NextIteratorFunction<K, V> nextIteratorFunction = new NextIteratorFunction<K, V>() {
            @Override
            public KeyValueIterator<K, V> apply(final ReadOnlyKeyValueStore<K, V> store) {
                return store.all();
            }
        };
        return new CompositeKeyValueIterator(allStores().iterator(), nextIteratorFunction);
    }

    interface NextIteratorFunction<K, V> {

        KeyValueIterator<K, V> apply(final ReadOnlyKeyValueStore<K, V> store);
    }


    private class CompositeKeyValueIterator implements KeyValueIterator<K, V> {

        private final Iterator<ReadOnlyKeyValueStore<K, V>> storeIterator;
        private final NextIteratorFunction<K, V> nextIteratorFunction;

        private KeyValueIterator<K, V> current;

        CompositeKeyValueIterator(final Iterator<ReadOnlyKeyValueStore<K, V>> underlying,
                                  final NextIteratorFunction<K, V> nextIteratorFunction) {
            this.storeIterator = underlying;
            this.nextIteratorFunction = nextIteratorFunction;
        }

        @Override
        public void close() {
            if (current != null) {
                current.close();
                current = null;
            }
        }

        @Override
        public boolean hasNext() {
            while ((current == null || !current.hasNext())
                   && storeIterator.hasNext()) {
                close();
                current = nextIteratorFunction.apply(storeIterator.next());
            }
            return current != null && current.hasNext();
        }


        @Override
        public KeyValue<K, V> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return current.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Remove not supported");
        }
    }
}

