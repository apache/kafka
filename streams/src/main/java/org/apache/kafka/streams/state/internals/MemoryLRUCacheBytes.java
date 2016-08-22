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
import org.apache.kafka.streams.state.StateSerdes;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An in-memory LRU cache store based on HashSet and HashMap. XXX: the same as MemoryLRUCache for now.
 *
 * Note that the use of array-typed keys is discouraged because they result in incorrect ordering behavior.
 * If you intend to work on byte arrays as key, for example, you may want to wrap them with the {@code Bytes} class,
 * i.e. use {@code RocksDBStore<Bytes, ...>} rather than {@code RocksDBStore<byte[], ...>}.

 *
 * @param <K> The key type
 * @param <V> The value type
 *
 * @see org.apache.kafka.streams.state.Stores#create(String)
 */
public class MemoryLRUCacheBytes<K, V>  {
    public interface EldestEntryRemovalListener<K, V> {

        void apply(K key, V value);
    }
    private final Serde<K> keySerde;

    private final Serde<V> valueSerde;
    private String name;
    protected Map<K, V> map;
    private StateSerdes<K, V> serdes;
    private volatile boolean open = true;

    protected List<MemoryLRUCacheBytes.EldestEntryRemovalListener<K, V>> listeners;

    // this is used for extended MemoryNavigableLRUCache only
    public MemoryLRUCacheBytes(Serde<K> keySerde, Serde<V> valueSerde) {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        listeners = new ArrayList<>();
    }

    public MemoryLRUCacheBytes(long maxCacheSizeBytes) {
        // TODO: implement, for now equivalent to entry-based cache
        this("tmpName", 1000, null, null);
    }

    public MemoryLRUCacheBytes(String name, final int maxCacheSize, Serde<K> keySerde, Serde<V> valueSerde) {
        this(keySerde, valueSerde);
        this.name = name;

        // leave room for one extra entry to handle adding an entry before the oldest can be removed
        this.map = new LinkedHashMap<K, V>(maxCacheSize + 1, 1.01f, true) {
            private static final long serialVersionUID = 1L;

            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                if (size() > maxCacheSize) {
                    K key = eldest.getKey();
                    if (listeners != null) {
                        for (MemoryLRUCacheBytes.EldestEntryRemovalListener<K, V> listener: listeners) {
                            listener.apply(key, eldest.getValue());
                        }
                    }
                    return true;
                }
                return false;
            }
        };
    }

    public MemoryLRUCacheBytes<K, V> whenEldestRemoved(MemoryLRUCacheBytes.EldestEntryRemovalListener<K, V> listener) {
        addEldestRemovedListener(listener);

        return this;
    }

    public void addEldestRemovedListener(MemoryLRUCacheBytes.EldestEntryRemovalListener<K, V> listener) {
        this.listeners.add(listener);
    }

    public synchronized V get(K key) {
        return this.map.get(key);
    }

    public synchronized void put(K key, V value) {
        this.map.put(key, value);
    }

    public synchronized V putIfAbsent(K key, V value) {
        V originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    public void putAll(List<KeyValue<K, V>> entries) {
        for (KeyValue<K, V> entry : entries)
            put(entry.key, entry.value);
    }

    public synchronized V delete(K key) {
        V value = this.map.remove(key);
        return value;
    }

    public int size() {
        return this.map.size();
    }
}
