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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreSupplier;

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
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final Time time;

    public InMemoryLRUCacheStoreSupplier(String name, int capacity, Serde<K> keySerde, Serde<V> valueSerde) {
        this(name, capacity, keySerde, valueSerde, null);
    }

    public InMemoryLRUCacheStoreSupplier(String name, int capacity, Serde<K> keySerde, Serde<V> valueSerde, Time time) {
        this.name = name;
        this.capacity = capacity;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.time = time;
    }

    public String name() {
        return name;
    }

    @SuppressWarnings("unchecked")
    public StateStore get() {
        final MemoryNavigableLRUCache<K, V> cache = new MemoryNavigableLRUCache<K, V>(name, capacity);
        final InMemoryKeyValueLoggedStore<K, V> loggedCache = (InMemoryKeyValueLoggedStore) cache.enableLogging(keySerde, valueSerde);
        final MeteredKeyValueStore<K, V> store = new MeteredKeyValueStore<>(loggedCache, "in-memory-lru-state", time);
        cache.whenEldestRemoved(new MemoryNavigableLRUCache.EldestEntryRemovalListener<K, V>() {
            @Override
            public void apply(K key, V value) {
                loggedCache.removed(key);
            }
        });
        return store;
    }
}
