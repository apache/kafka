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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Map;

/**
 * An in-memory key-value store that is limited in size and retains a maximum number of most recently used entries.
 *
 * @param <K> The key type
 * @param <V> The value type
 *
 */
@Deprecated
public class InMemoryLRUCacheStoreSupplier<K, V> extends AbstractStoreSupplier<K, V, KeyValueStore> {

    private final int capacity;

    public InMemoryLRUCacheStoreSupplier(String name, int capacity, Serde<K> keySerde, Serde<V> valueSerde, boolean logged, Map<String, String> logConfig) {
        this(name, capacity, keySerde, valueSerde, null, logged, logConfig);
    }

    private InMemoryLRUCacheStoreSupplier(String name, int capacity, Serde<K> keySerde, Serde<V> valueSerde, Time time, boolean logged, Map<String, String> logConfig) {
        super(name, keySerde, valueSerde, time, logged, logConfig);
        this.capacity = capacity;
    }

    public KeyValueStore get() {
        MemoryNavigableLRUCache<K, V> cache = new MemoryNavigableLRUCache<>(name, capacity, keySerde, valueSerde);
        return new MeteredKeyValueStore<>(logged ? cache.enableLogging() : cache, "in-memory-lru-state", time);
    }

}
