/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.List;

public class InMemoryKeyValueLoggedStore<K, V> extends WrappedStateStore.AbstractStateStore implements KeyValueStore<K, V> {

    private final KeyValueStore<K, V> inner;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    private StoreChangeLogger<K, V> changeLogger;
    private ProcessorContext context;

    InMemoryKeyValueLoggedStore(final KeyValueStore<K, V> inner, Serde<K> keySerde, Serde<V> valueSerde) {
        super(inner);
        this.inner = inner;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context, StateStore root) {
        this.context = context;
        inner.init(context, root);

        // construct the serde
        StateSerdes<K, V>  serdes = new StateSerdes<>(inner.name(),
                keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);

        this.changeLogger = new StoreChangeLogger<>(inner.name(), context, serdes);


        // if the inner store is an LRU cache, add the eviction listener to log removed record
        if (inner instanceof MemoryLRUCache) {
            ((MemoryLRUCache<K, V>) inner).whenEldestRemoved(new MemoryNavigableLRUCache.EldestEntryRemovalListener<K, V>() {
                @Override
                public void apply(K key, V value) {
                    removed(key);
                }
            });
        }
    }

    @Override
    public long approximateNumEntries() {
        return inner.approximateNumEntries();
    }

    @Override
    public V get(K key) {
        return this.inner.get(key);
    }

    @Override
    public void put(K key, V value) {
        this.inner.put(key, value);

        changeLogger.logChange(key, value);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        V originalValue = this.inner.putIfAbsent(key, value);
        if (originalValue == null) {
            changeLogger.logChange(key, value);
        }
        return originalValue;
    }

    @Override
    public void putAll(List<KeyValue<K, V>> entries) {
        this.inner.putAll(entries);

        for (KeyValue<K, V> entry : entries) {
            K key = entry.key;
            changeLogger.logChange(key, entry.value);
        }
    }

    @Override
    public V delete(K key) {
        V value = this.inner.delete(key);

        removed(key);

        return value;
    }

    /**
     * Called when the underlying {@link #inner} {@link KeyValueStore} removes an entry in response to a call from this
     * store.
     *
     * @param key the key for the entry that the inner store removed
     */
    protected void removed(K key) {
        changeLogger.logChange(key, null);
    }

    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        return this.inner.range(from, to);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return this.inner.all();
    }
}
