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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Serdes;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * An in-memory key-value store based on a TreeMap.
 *
 * @param <K> The key type
 * @param <V> The value type
 *
 * @see org.apache.kafka.streams.state.Stores#create(String)
 */
public class InMemoryKeyValueStoreSupplier<K, V> implements StateStoreSupplier {

    private final String name;
    private final Time time;
    private Serdes<K, V> serdes;

    public InMemoryKeyValueStoreSupplier(String name, Serdes<K, V> serdes, Time time) {
        this.name = name;
        this.time = time;
        this.serdes = serdes;
    }

    public String name() {
        return name;
    }

    public StateStore get() {
        return new MeteredKeyValueStore<>(new MemoryStore<K, V>(name).enableLogging(serdes), "in-memory-state", time);
    }

    private static class MemoryStore<K, V> implements KeyValueStore<K, V> {

        private final String name;
        private final NavigableMap<K, V> map;

        public MemoryStore(String name) {
            super();
            this.name = name;
            this.map = new TreeMap<>();
        }

        public KeyValueStore<K, V> enableLogging(Serdes<K, V> serdes) {
            return new InMemoryKeyValueLoggedStore<>(this.name, this, serdes);
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
        }

        @Override
        public void putAll(List<KeyValue<K, V>> entries) {
            for (KeyValue<K, V> entry : entries)
                put(entry.key, entry.value);
        }

        @Override
        public V delete(K key) {
            return this.map.remove(key);
        }

        @Override
        public KeyValueIterator<K, V> range(K from, K to) {
            return new MemoryStoreIterator<K, V>(this.map.subMap(from, true, to, false).entrySet().iterator());
        }

        @Override
        public KeyValueIterator<K, V> all() {
            return new MemoryStoreIterator<K, V>(this.map.entrySet().iterator());
        }

        @Override
        public void flush() {
            // do-nothing since it is in-memory
        }

        @Override
        public void close() {
            // do-nothing
        }

        private static class MemoryStoreIterator<K, V> implements KeyValueIterator<K, V> {
            private final Iterator<Map.Entry<K, V>> iter;

            public MemoryStoreIterator(Iterator<Map.Entry<K, V>> iter) {
                this.iter = iter;
            }

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public KeyValue<K, V> next() {
                Map.Entry<K, V> entry = iter.next();
                return new KeyValue<>(entry.getKey(), entry.getValue());
            }

            @Override
            public void remove() {
                iter.remove();
            }

            @Override
            public void close() {
            }

        }
    }
}
