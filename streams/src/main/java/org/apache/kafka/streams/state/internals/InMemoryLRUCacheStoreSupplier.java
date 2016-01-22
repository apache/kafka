/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Serdes;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * An in-memory key-value store that is limited in size and retains a maximum number of most recently used entries.
 *
 * @param <K> The key type
 * @param <V> The value type
 *
 */
public class InMemoryLRUCacheStoreSupplier<K, V> implements StateStoreSupplier {

    private final String name;
    private final int capacity;
    private final Serdes serdes;
    private final Time time;

    public InMemoryLRUCacheStoreSupplier(String name, int capacity, Serdes<K, V> serdes, Time time) {
        this.name = name;
        this.capacity = capacity;
        this.serdes = serdes;
        this.time = time;
    }

    public String name() {
        return name;
    }

    public StateStore get() {
        MemoryLRUCache<K, V> cache = new MemoryLRUCache<K, V>(name, capacity);
        final MeteredKeyValueStore<K, V> store = new MeteredKeyValueStore<>(cache, serdes, "in-memory-lru-state", time);
        cache.whenEldestRemoved(new EldestEntryRemovalListener<K, V>() {
            @Override
            public void apply(K key, V value) {
                store.removed(key);
            }
        });
        return store;
    }

    public static interface EldestEntryRemovalListener<K, V> {
        public void apply(K key, V value);
    }

    protected static final class MemoryLRUCache<K, V> implements KeyValueStore<K, V> {

        private final String name;
        private final Map<K, V> map;
        private final NavigableSet<K> keys;
        private EldestEntryRemovalListener<K, V> listener;

        public MemoryLRUCache(String name, final int maxCacheSize) {
            this.name = name;
            this.keys = new TreeSet<>();
            // leave room for one extra entry to handle adding an entry before the oldest can be removed
            this.map = new LinkedHashMap<K, V>(maxCacheSize + 1, 1.01f, true) {
                private static final long serialVersionUID = 1L;

                @Override
                protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                    if (size() > maxCacheSize) {
                        K key = eldest.getKey();
                        keys.remove(key);
                        if (listener != null) listener.apply(key, eldest.getValue());
                        return true;
                    }
                    return false;
                }
            };
        }

        public MemoryLRUCache<K, V> whenEldestRemoved(EldestEntryRemovalListener<K, V> listener) {
            this.listener = listener;

            return this;
        }

        @Override
        public String name() {
            return this.name;
        }

        @Override
        public void init(ProcessorContext context) {
            // do-nothing since it is in-memory
        }

        @Override
        public boolean persistent() {
            return false;
        }

        @Override
        public V get(K key) {
            return this.map.get(key);
        }

        @Override
        public void put(K key, V value) {
            this.map.put(key, value);
            this.keys.add(key);
        }

        @Override
        public void putAll(List<KeyValue<K, V>> entries) {
            for (KeyValue<K, V> entry : entries)
                put(entry.key, entry.value);
        }

        @Override
        public V delete(K key) {
            V value = this.map.remove(key);
            this.keys.remove(key);
            return value;
        }

        @Override
        public KeyValueIterator<K, V> range(K from, K to) {
            return new MemoryLRUCache.CacheIterator<K, V>(this.keys.subSet(from, true, to, false).iterator(), this.map);
        }

        @Override
        public KeyValueIterator<K, V> all() {
            return new MemoryLRUCache.CacheIterator<K, V>(this.keys.iterator(), this.map);
        }

        @Override
        public void flush() {
            // do-nothing since it is in-memory
        }

        @Override
        public void close() {
            // do-nothing
        }

        public void clearKeys() {
            keys.clear();
        }

        private static class CacheIterator<K, V> implements KeyValueIterator<K, V> {
            private final Iterator<K> keys;
            private final Map<K, V> entries;
            private K lastKey;

            public CacheIterator(Iterator<K> keys, Map<K, V> entries) {
                this.keys = keys;
                this.entries = entries;
            }

            @Override
            public boolean hasNext() {
                return keys.hasNext();
            }

            @Override
            public KeyValue<K, V> next() {
                lastKey = keys.next();
                return new KeyValue<>(lastKey, entries.get(lastKey));
            }

            @Override
            public void remove() {
                keys.remove();
                entries.remove(lastKey);
            }

            @Override
            public void close() {
                // do nothing
            }
        }
    }
}
