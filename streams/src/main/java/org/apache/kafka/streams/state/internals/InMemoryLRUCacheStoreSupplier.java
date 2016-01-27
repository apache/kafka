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
        cache.whenEldestRemoved(new MemoryLRUCache.EldestEntryRemovalListener<K, V>() {
            @Override
            public void apply(K key, V value) {
                store.removed(key);
            }
        });
        return store;
    }
}
