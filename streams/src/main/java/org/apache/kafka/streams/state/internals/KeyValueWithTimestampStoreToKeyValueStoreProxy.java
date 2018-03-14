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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.KeyValueWithTimestampStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.List;

class KeyValueWithTimestampStoreToKeyValueStoreProxy<K, V> implements KeyValueWithTimestampStore<K, V> {
    private final KeyValueStore<K, V> oldStore;
    private ProcessorContext context;

    KeyValueWithTimestampStoreToKeyValueStoreProxy(final KeyValueStore<K, V> oldStore) {
        this.oldStore = oldStore;
    }

    @Override
    public void put(final K key,
                    final V value,
                    final long timestamp) {
        oldStore.put(key, value);
    }

    @Override
    public void put(final K key,
                    final ValueAndTimestamp<V> valueWithTimestamp) {
        oldStore.put(key, valueWithTimestamp.value());
    }

    @Override
    public ValueAndTimestamp<V> putIfAbsent(final K key,
                                            final V value,
                                            final long timestamp) {
        final V oldValue = oldStore.putIfAbsent(key, value);
        return new ValueAndTimestampImpl<>(oldValue, context.timestamp());
    }

    @Override
    public ValueAndTimestamp<V> putIfAbsent(final K key,
                                            final ValueAndTimestamp<V> valueWithTimestamp) {
        return putIfAbsent(key, valueWithTimestamp.value(), valueWithTimestamp.timestamp());
    }

    @Override
    public void putAll(final List<KeyValue<K, ValueAndTimestamp<V>>> entries) {
        for (final KeyValue<K, ValueAndTimestamp<V>> entry : entries) {
            put(entry.key, entry.value.value(), entry.value.timestamp());
        }
    }

    @Override
    public ValueAndTimestamp<V> delete(final K key) {
        final V oldValue = oldStore.delete(key);
        return new ValueAndTimestampImpl<>(oldValue, context.timestamp());
    }

    @Override
    public String name() {
        return oldStore.name();
    }

    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        this.context = context;
        oldStore.init(context, root);
    }

    @Override
    public void flush() {
        oldStore.flush();
    }

    @Override
    public void close() {
        oldStore.close();
    }

    @Override
    public boolean persistent() {
        return oldStore.persistent();
    }

    @Override
    public boolean isOpen() {
        return oldStore.isOpen();
    }

    @Override
    public ValueAndTimestamp<V> get(final K key) {
        final V value = oldStore.get(key);
        return new ValueAndTimestampImpl<>(value, context.timestamp());
    }

    @Override
    public KeyValueIterator<K, ValueAndTimestamp<V>> range(final K from,
                                                           final K to) {
        return convertIterator(oldStore.range(from, to));
    }

    @Override
    public KeyValueIterator<K, ValueAndTimestamp<V>> all() {
        return convertIterator(oldStore.all());
    }

    @Override
    public long approximateNumEntries() {
        return oldStore.approximateNumEntries();
    }

    private KeyValueIterator<K, ValueAndTimestamp<V>> convertIterator(final KeyValueIterator<K, V> innerIterator) {
        return new KeyValueIterator<K, ValueAndTimestamp<V>>() {
            @Override
            public boolean hasNext() {
                return innerIterator.hasNext();
            }

            @Override
            public KeyValue<K, ValueAndTimestamp<V>> next() {
                final KeyValue<K, V> next = innerIterator.next();
                return new KeyValue<>(
                    next.key,
                    new ValueAndTimestampImpl<>(next.value, context.timestamp()));
            }

            @Override
            public void remove() {
                innerIterator.remove();
            }

            @Override
            public void close() {
                innerIterator.close();
            }

            @Override
            public K peekNextKey() {
                return innerIterator.peekNextKey();
            }
        };
    }
}
