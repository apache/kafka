/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

class InMemoryKeyValueStore<K, V> implements KeyValueStore<K, V> {
    private final TreeMap<K, V> map = new TreeMap<>();
    private final String name;
    private boolean open = true;

    InMemoryKeyValueStore(final String name) {
        this.name = name;
    }

    @Override
    public void put(final K key, final V value) {
        map.put(key, value);
    }

    @Override
    public V putIfAbsent(final K key, final V value) {
        V orig = map.get(key);
        if (orig == null) {
            map.put(key, value);
        }
        return orig;
    }

    @Override
    public void putAll(final List<KeyValue<K, V>> entries) {
        for (KeyValue<K, V> entry : entries) {
            map.put(entry.key, entry.value);
        }
    }

    @Override
    public V delete(final K key) {
        return map.remove(key);
    }

    @Override
    public long approximateNumEntries() {
        return map.size();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        // no-op
    }

    @Override
    public void flush() {
        //no-op
    }

    @Override
    public void close() {
        open = false;
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public V get(final K key) {
        return map.get(key);
    }

    @Override
    public KeyValueIterator<K, V> range(final K from, final K to) {
        return new TheIterator(this.map.subMap(from, true, to, false).entrySet().iterator());
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return new TheIterator(map.entrySet().iterator());
    }

    private class TheIterator implements KeyValueIterator<K, V> {

        private final Iterator<Map.Entry<K, V>> underlying;

        public TheIterator(final Iterator<Map.Entry<K, V>> iterator) {
            this.underlying = iterator;
        }

        @Override
        public void close() {

        }

        @Override
        public boolean hasNext() {
            return underlying.hasNext();
        }

        @Override
        public KeyValue<K, V> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Map.Entry<K, V> next = underlying.next();
            return new KeyValue<>(next.getKey(), next.getValue());
        }

        @Override
        public void remove() {

        }
    }
}
