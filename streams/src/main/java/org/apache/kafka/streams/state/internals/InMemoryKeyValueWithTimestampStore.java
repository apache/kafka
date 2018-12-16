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
import org.apache.kafka.streams.state.KeyValueWithTimestampStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.apache.kafka.streams.state.internals.KeyValueWithTimestampStoreBuilder.ValueAndTimestampSerde;

public class InMemoryKeyValueWithTimestampStore<K, V> implements KeyValueWithTimestampStore<K, V> {
    private final String name;
    private final Serde<K> keySerde;
    private final ValueAndTimestampSerde<V> valueAndTimestampSerde;
    private final NavigableMap<K, ValueAndTimestamp<V>> map;
    private volatile boolean open = false;

    private StateSerdes<K, ValueAndTimestamp<V>> serdes;

    public InMemoryKeyValueWithTimestampStore(final String name,
                                              final Serde<K> keySerde,
                                              final ValueAndTimestampSerde<V> valueAndTimestampSerde) {
        this.name = name;
        this.keySerde = keySerde;
        this.valueAndTimestampSerde = valueAndTimestampSerde;

        this.map = new TreeMap<>();
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context,
                     final StateStore root) {
        // construct the serde
        this.serdes = new StateSerdes<>(
            ProcessorStateManager.storeChangelogTopic(context.applicationId(), name),
            keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
            valueAndTimestampSerde == null ? new ValueAndTimestampSerde<>((Serde<V>) context.valueSerde()) : valueAndTimestampSerde);

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

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return this.open;
    }

    @Override
    public synchronized ValueAndTimestamp<V> get(final K key) {
        return this.map.get(key);
    }

    @Override
    public synchronized void put(final K key,
                                 final ValueAndTimestamp<V> valueAndTimestamp) {
        if (valueAndTimestamp == null || valueAndTimestamp.value() == null) {
            this.map.remove(key);
        } else {
            this.map.put(key, valueAndTimestamp);
        }
    }

    @Override
    public synchronized void put(final K key,
                                 final V value,
                                 final long timestamp) {
        if (value == null) {
            this.map.remove(key);
        } else {
            this.map.put(key, new ValueAndTimestampImpl<>(value, timestamp));
        }
    }

    @Override
    public synchronized ValueAndTimestamp<V> putIfAbsent(final K key,
                                                         final ValueAndTimestamp<V> valueAndTimestamp) {
        final ValueAndTimestamp<V> originalValueAndTimestamp = get(key);
        if (originalValueAndTimestamp == null) {
            put(key, valueAndTimestamp);
        }
        return originalValueAndTimestamp;
    }

    @Override
    public synchronized ValueAndTimestamp<V> putIfAbsent(final K key,
                                                         final V value,
                                                         final long timestamp) {
        return putIfAbsent(key, new ValueAndTimestampImpl<>(value, timestamp));
    }

    @Override
    public synchronized void putAll(final List<KeyValue<K, ValueAndTimestamp<V>>> entries) {
        for (final KeyValue<K, ValueAndTimestamp<V>> entry : entries) {
            put(entry.key, entry.value);
        }
    }

    @Override
    public synchronized ValueAndTimestamp<V> delete(final K key) {
        return this.map.remove(key);
    }

    @Override
    public synchronized KeyValueIterator<K, ValueAndTimestamp<V>> range(final K from,
                                                                        final K to) {
        return new DelegatingPeekingKeyValueIterator<>(
            name,
            new InMemoryKeyValueIterator<>(this.map.subMap(from, true, to, true).entrySet().iterator()));
    }

    @Override
    public synchronized KeyValueIterator<K, ValueAndTimestamp<V>> all() {
        final TreeMap<K, ValueAndTimestamp<V>> copy = new TreeMap<>(this.map);
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
