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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An in-memory LRU cache store based on HashSet and HashMap.
 *
 *  * Note that the use of array-typed keys is discouraged because they result in incorrect ordering behavior.
 * If you intend to work on byte arrays as key, for example, you may want to wrap them with the {@code Bytes} class,
 * i.e. use {@code RocksDBStore<Bytes, ...>} rather than {@code RocksDBStore<byte[], ...>}.

 *
 * @param <K> The key type
 * @param <V> The value type
 *
 * @see org.apache.kafka.streams.state.Stores#create(String)
 */
public class MemoryLRUCache<K, V> implements KeyValueStore<K, V> {

    public interface EldestEntryRemovalListener<K, V> {
        void apply(K key, V value);
    }

    protected String name;
    protected Map<K, V> map;
    protected Set<K> keys;

    protected EldestEntryRemovalListener<K, V> listener;

    // this is used for extended MemoryNavigableLRUCache only
    public MemoryLRUCache() {}

    public MemoryLRUCache(String name, final int maxCacheSize) {
        this.name = name;
        this.keys = new HashSet<>();

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

    public KeyValueStore<K, V> enableLogging(Serde<K> keySerde, Serde<V> valueSerde) {
        return new InMemoryKeyValueLoggedStore<>(this.name, this, keySerde, valueSerde);
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
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context, StateStore root) {
        // do nothing
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
    public V putIfAbsent(K key, V value) {
        V originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
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

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        throw new UnsupportedOperationException("MemoryLRUCache does not support range() function.");
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public KeyValueIterator<K, V> all() {
        throw new UnsupportedOperationException("MemoryLRUCache does not support all() function.");
    }

    @Override
    public void flush() {
        // do-nothing since it is in-memory
    }

    @Override
    public void close() {
        // do-nothing since it is in-memory
    }
}
