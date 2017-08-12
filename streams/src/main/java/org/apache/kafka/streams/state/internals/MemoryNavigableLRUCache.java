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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class MemoryNavigableLRUCache<K, V> extends MemoryLRUCache<K, V> {


    public MemoryNavigableLRUCache(String name, final int maxCacheSize, Serde<K> keySerde, Serde<V> valueSerde) {
        super(name, maxCacheSize, keySerde, valueSerde);
    }

    @Override
    public MemoryNavigableLRUCache<K, V> whenEldestRemoved(EldestEntryRemovalListener<K, V> listener) {
        this.listener = listener;

        return this;
    }

    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        final TreeMap<K, V> treeMap = toTreeMap();
        return new DelegatingPeekingKeyValueIterator<>(name(), new MemoryNavigableLRUCache.CacheIterator<>(treeMap.navigableKeySet().subSet(from, true, to, true).iterator(), treeMap));
    }

    @Override
    public  KeyValueIterator<K, V> all() {
        final TreeMap<K, V> treeMap = toTreeMap();
        return new MemoryNavigableLRUCache.CacheIterator<>(treeMap.navigableKeySet().iterator(), treeMap);
    }

    private synchronized TreeMap<K, V> toTreeMap() {
        return new TreeMap<>(this.map);
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
            // do nothing
        }

        @Override
        public void close() {
            // do nothing
        }

        @Override
        public K peekNextKey() {
            throw new UnsupportedOperationException("peekNextKey not supported");
        }
    }
}
