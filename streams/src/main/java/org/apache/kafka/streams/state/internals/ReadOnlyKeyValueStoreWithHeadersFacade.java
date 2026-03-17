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

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

/**
 * A facade that wraps {@link TimestampedKeyValueStoreWithHeaders} to provide a
 * {@link ReadOnlyKeyValueStore} interface. This facade extracts the plain value
 * from {@link ValueTimestampHeaders}, discarding timestamp and headers.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ReadOnlyKeyValueStoreWithHeadersFacade<K, V> implements ReadOnlyKeyValueStore<K, V> {
    protected final TimestampedKeyValueStoreWithHeaders<K, V> inner;

    protected ReadOnlyKeyValueStoreWithHeadersFacade(final TimestampedKeyValueStoreWithHeaders<K, V> store) {
        inner = store;
    }

    @Override
    public V get(final K key) {
        final ValueTimestampHeaders<V> valueTimestampHeaders = inner.get(key);
        return valueTimestampHeaders == null ? null : valueTimestampHeaders.value();
    }

    @Override
    public KeyValueIterator<K, V> range(final K from, final K to) {
        return new KeyValueIteratorWithHeadersFacade<>(inner.range(from, to));
    }

    @Override
    public KeyValueIterator<K, V> reverseRange(final K from, final K to) {
        return new KeyValueIteratorWithHeadersFacade<>(inner.reverseRange(from, to));
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<K, V> prefixScan(final P prefix,
                                                                           final PS prefixKeySerializer) {
        return new KeyValueIteratorWithHeadersFacade<>(inner.prefixScan(prefix, prefixKeySerializer));
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return new KeyValueIteratorWithHeadersFacade<>(inner.all());
    }

    @Override
    public KeyValueIterator<K, V> reverseAll() {
        return new KeyValueIteratorWithHeadersFacade<>(inner.reverseAll());
    }

    @Override
    public long approximateNumEntries() {
        return inner.approximateNumEntries();
    }

    /**
     * Iterator facade that extracts plain values from ValueTimestampHeaders.
     */
    private static class KeyValueIteratorWithHeadersFacade<K, V> implements KeyValueIterator<K, V> {
        private final KeyValueIterator<K, ValueTimestampHeaders<V>> innerIterator;

        KeyValueIteratorWithHeadersFacade(final KeyValueIterator<K, ValueTimestampHeaders<V>> innerIterator) {
            this.innerIterator = innerIterator;
        }

        @Override
        public void close() {
            innerIterator.close();
        }

        @Override
        public K peekNextKey() {
            return innerIterator.peekNextKey();
        }

        @Override
        public boolean hasNext() {
            return innerIterator.hasNext();
        }

        @Override
        public KeyValue<K, V> next() {
            final KeyValue<K, ValueTimestampHeaders<V>> next = innerIterator.next();
            final V value = next.value == null ? null : next.value.value();
            return KeyValue.pair(next.key, value);
        }
    }
}