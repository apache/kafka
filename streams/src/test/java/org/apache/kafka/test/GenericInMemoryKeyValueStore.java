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
package org.apache.kafka.test;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.CacheFlushListener;
import org.apache.kafka.streams.state.internals.DelegatingPeekingKeyValueIterator;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * This class is a generic version of the in-memory key-value store that is useful for testing when you
 *  need a basic KeyValueStore for arbitrary types and don't have/want to write a serde
 */
public class GenericInMemoryKeyValueStore<K extends Comparable, V>
    extends WrappedStateStore<StateStore, K, V>
    implements KeyValueStore<K, V> {

    private final String name;
    private final NavigableMap<K, V> map;
    private volatile boolean open = false;

    public GenericInMemoryKeyValueStore(final String name) {
        // it's not really a `WrappedStateStore` so we pass `null`
        // however, we need to implement `WrappedStateStore` to make the store usable
        super(null);
        this.name = name;

        this.map = new TreeMap<>();
    }

    @Override
    public String name() {
        return this.name;
    }

    @SuppressWarnings("deprecation")
    @Deprecated
    @Override
    /* This is a "dummy" store used for testing;
       it does not support restoring from changelog since we allow it to be serde-ignorant */
    public void init(final ProcessorContext context, final StateStore root) {
        if (root != null) {
            context.register(root, null);
        }

        this.open = true;
    }

    @Override
    public boolean setFlushListener(final CacheFlushListener<K, V> listener,
                                    final boolean sendOldValues) {
        return false;
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
            new GenericInMemoryKeyValueIterator<>(this.map.subMap(from, true, to, true).entrySet().iterator()));
    }

    @Override
    public synchronized KeyValueIterator<K, V> all() {
        final TreeMap<K, V> copy = new TreeMap<>(this.map);
        return new DelegatingPeekingKeyValueIterator<>(name, new GenericInMemoryKeyValueIterator<>(copy.entrySet().iterator()));
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

    private static class GenericInMemoryKeyValueIterator<K, V> implements KeyValueIterator<K, V> {
        private final Iterator<Entry<K, V>> iter;

        private GenericInMemoryKeyValueIterator(final Iterator<Map.Entry<K, V>> iter) {
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
        public void close() {
            // do nothing
        }

        @Override
        public K peekNextKey() {
            throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
        }
    }
}