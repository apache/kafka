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

package org.apache.kafka.streams.state;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * An in-memory key-value store based on a TreeMap
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public class InMemoryKeyValueStore<K, V> extends MeteredKeyValueStore<K, V> {

    public InMemoryKeyValueStore(String name, ProcessorContext context) {
        this(name, context, new SystemTime());
    }

    public InMemoryKeyValueStore(String name, ProcessorContext context, Time time) {
        super(name, new MemoryStore<K, V>(name, context), context, "kafka-streams", time);
    }

    private static class MemoryStore<K, V> implements KeyValueStore<K, V> {

        private final String name;
        private final NavigableMap<K, V> map;
        private final ProcessorContext context;

        @SuppressWarnings("unchecked")
        public MemoryStore(String name, ProcessorContext context) {
            super();
            this.name = name;
            this.map = new TreeMap<>();
            this.context = context;
        }

        @Override
        public String name() {
            return this.name;
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
        public void putAll(List<Entry<K, V>> entries) {
            for (Entry<K, V> entry : entries)
                put(entry.key(), entry.value());
        }

        @Override
        public void delete(K key) {
            put(key, null);
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

        public void restore() {
            // this should not happen since it is in-memory, hence no state to load from disk
            throw new IllegalStateException("This should not happen");
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
            public Entry<K, V> next() {
                Map.Entry<K, V> entry = iter.next();
                return new Entry<>(entry.getKey(), entry.getValue());
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
