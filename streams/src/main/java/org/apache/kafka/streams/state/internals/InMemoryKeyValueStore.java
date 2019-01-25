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
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class InMemoryKeyValueStore<K, V> implements KeyValueStore<K, V> {
    private final NavigableMap<K, V> map;
    private volatile boolean open = false;

    final String name;
    final Serde<K> keySerde;
    final Serde<V> valueSerde;
    StateSerdes<K, V> serdes;


    public InMemoryKeyValueStore(final String name,
                                 final Serde<K> keySerde,
                                 final Serde<V> valueSerde) {
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;

        this.map = new TreeMap<>();
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        initStateSerdes(context);
        if (root != null) {
            // register the store
            context.register(root, (key, value) -> {
                // this is a delete
                if (value == null) {
                    delete(serdes.keyFrom(key));
                } else {
                    put(serdes.keyFrom(key), serdes.valueFrom(value));
                }
            });
        }

        this.open = true;
    }

    @SuppressWarnings("unchecked")
    void initStateSerdes(final ProcessorContext context) {
        this.serdes = new StateSerdes<>(
            ProcessorStateManager.storeChangelogTopic(context.applicationId(), name),
            keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
            valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);
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
    public synchronized V get(final K key) {
        return this.map.get(key);
    }

    @Override
    public synchronized void put(final K key,
                                 final V value) {
        if (value == null) {
            this.map.remove(key);
        } else {
            this.map.put(key, value);
        }
    }

    @Override
    public synchronized V putIfAbsent(final K key,
                                      final V value) {
        final V originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    @Override
    public synchronized void putAll(final List<KeyValue<K, V>> entries) {
        for (final KeyValue<K, V> entry : entries) {
            put(entry.key, entry.value);
        }
    }

    @Override
    public synchronized V delete(final K key) {
        return this.map.remove(key);
    }

    @Override
    public synchronized KeyValueIterator<K, V> range(final K from,
                                                     final K to) {
        return new DelegatingPeekingKeyValueIterator<>(
            name,
            new InMemoryKeyValueIterator<>(this.map.subMap(from, true, to, true).entrySet().iterator()));
    }

    @Override
    public synchronized KeyValueIterator<K, V> all() {
        final TreeMap<K, V> copy = new TreeMap<>(this.map);
        return new DelegatingPeekingKeyValueIterator<>(name, new InMemoryKeyValueIterator<>(copy.entrySet().iterator()));
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
        this.map.clear();
        this.open = false;
    }

    static class InMemoryKeyValueIterator<K, V> implements KeyValueIterator<K, V> {
        private final Iterator<Map.Entry<K, V>> iter;

        InMemoryKeyValueIterator(final Iterator<Map.Entry<K, V>> iter) {
            this.iter = iter;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public KeyValue<K, V> next() {
            final Map.Entry<K, V> entry = iter.next();
            return new KeyValue<>(entry.getKey(), entry.getValue());
        }

        @Override
        public void remove() {
            iter.remove();
        }

        @Override
        public void close() {
            // do nothing
        }

        @Override
        public K peekNextKey() {
            throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
        }
    }
}
