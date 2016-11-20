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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * An in-memory key-value store based on a TreeMap.
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
public class InMemoryKeyValueStoreSupplier<K, V> extends AbstractStoreSupplier<K, V> {


    public InMemoryKeyValueStoreSupplier(String name, Serde<K> keySerde, Serde<V> valueSerde, boolean logged, Map<String, String> logConfig) {
        this(name, keySerde, valueSerde, null, logged, logConfig);
    }

    public InMemoryKeyValueStoreSupplier(String name, Serde<K> keySerde, Serde<V> valueSerde, Time time, boolean logged, Map<String, String> logConfig) {
        super(name, keySerde, valueSerde, time, logged, logConfig);
    }

    public StateStore get() {
        MemoryStore<K, V> store = new MemoryStore<>(name, keySerde, valueSerde);

        return new MeteredKeyValueStore<>(logged ? store.enableLogging() : store, "in-memory-state", time);
    }

    private static class MemoryStore<K, V> implements KeyValueStore<K, V> {
        private final String name;
        private final Serde<K> keySerde;
        private final Serde<V> valueSerde;
        private final NavigableMap<K, V> map;
        private volatile boolean open = false;

        private StateSerdes<K, V> serdes;

        public MemoryStore(String name, Serde<K> keySerde, Serde<V> valueSerde) {
            this.name = name;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;

            // TODO: when we have serde associated with class types, we can
            // improve this situation by passing the comparator here.
            this.map = new TreeMap<>();
        }

        public KeyValueStore<K, V> enableLogging() {
            return new InMemoryKeyValueLoggedStore<>(this.name, this, keySerde, valueSerde);
        }

        @Override
        public String name() {
            return this.name;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context, StateStore root) {
            // construct the serde
            this.serdes = new StateSerdes<>(name,
                    keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                    valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);

            // register the store
            context.register(root, true, new StateRestoreCallback() {
                @Override
                public void restore(byte[] key, byte[] value) {
                    // check value for null, to avoid  deserialization error.
                    if (value == null) {
                        put(serdes.keyFrom(key), null);
                    } else {
                        put(serdes.keyFrom(key), serdes.valueFrom(value));
                    }
                }
            });
            this.open = true;
        }

        @Override
        public boolean persistent() {
            return false;
        }

        @Override
        public boolean isOpen() {
            return this.open;
        }

        @Override
        public synchronized V get(K key) {
            return this.map.get(key);
        }

        @Override
        public synchronized void put(K key, V value) {
            this.map.put(key, value);
        }

        @Override
        public synchronized V putIfAbsent(K key, V value) {
            V originalValue = get(key);
            if (originalValue == null) {
                put(key, value);
            }
            return originalValue;
        }

        @Override
        public synchronized void putAll(List<KeyValue<K, V>> entries) {
            for (KeyValue<K, V> entry : entries)
                put(entry.key, entry.value);
        }

        @Override
        public synchronized V delete(K key) {
            return this.map.remove(key);
        }

        @Override
        public synchronized KeyValueIterator<K, V> range(K from, K to) {
            return new MemoryStoreIterator<>(this.map.subMap(from, true, to, false).entrySet().iterator());
        }

        @Override
        public synchronized KeyValueIterator<K, V> all() {
            final TreeMap<K, V> copy = new TreeMap<>(this.map);
            return new MemoryStoreIterator<>(copy.entrySet().iterator());
        }

        @Override
        public long approximateNumEntries() {
            return this.map.size();
        }

        @Override
        public void flush() {
            // do-nothing since it is in-memory
        }

        @Override
        public void close() {
            this.open = false;
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
