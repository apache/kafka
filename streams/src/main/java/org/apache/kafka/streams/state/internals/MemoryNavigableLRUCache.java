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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

public class MemoryNavigableLRUCache<K, V> extends MemoryLRUCache<K, V> {

    public MemoryNavigableLRUCache(String name, final int maxCacheSize) {
        super();

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

    @Override
    public MemoryNavigableLRUCache<K, V> whenEldestRemoved(EldestEntryRemovalListener<K, V> listener) {
        this.listener = listener;

        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        return new MemoryNavigableLRUCache.CacheIterator<K, V>(((NavigableSet) this.keys).subSet(from, true, to, false).iterator(), this.map);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return new MemoryNavigableLRUCache.CacheIterator<K, V>(this.keys.iterator(), this.map);
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
