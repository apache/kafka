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
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

/**
 * A facade that wraps {@link TimestampedKeyValueStoreWithHeaders} to provide a
 * {@link ReadOnlyKeyValueStore} interface with {@link ValueAndTimestamp} values.
 * This facade converts {@link ValueTimestampHeaders} to {@link ValueAndTimestamp}, discarding headers.
 *
 * Similar to {@link ReadOnlyKeyValueStoreWithHeadersFacade} but for timestamped queries.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ReadOnlyTimestampedKeyValueStoreWithHeadersFacade<K, V> implements ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>> {
    protected final TimestampedKeyValueStoreWithHeaders<K, V> inner;

    public ReadOnlyTimestampedKeyValueStoreWithHeadersFacade(final TimestampedKeyValueStoreWithHeaders<K, V> store) {
        inner = store;
    }

    @Override
    public ValueAndTimestamp<V> get(final K key) {
        final ValueTimestampHeaders<V> valueTimestampHeaders = inner.get(key);
        if (valueTimestampHeaders == null) {
            return null;
        }
        return ValueAndTimestamp.make(valueTimestampHeaders.value(), valueTimestampHeaders.timestamp());
    }

    @Override
    public KeyValueIterator<K, ValueAndTimestamp<V>> range(final K from, final K to) {
        return new TimestampedKeyValueIteratorFacade<>(inner.range(from, to));
    }

    @Override
    public KeyValueIterator<K, ValueAndTimestamp<V>> reverseRange(final K from, final K to) {
        return new TimestampedKeyValueIteratorFacade<>(inner.reverseRange(from, to));
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<K, ValueAndTimestamp<V>> prefixScan(final P prefix,
                                                                                               final PS prefixKeySerializer) {
        return new TimestampedKeyValueIteratorFacade<>(inner.prefixScan(prefix, prefixKeySerializer));
    }

    @Override
    public KeyValueIterator<K, ValueAndTimestamp<V>> all() {
        return new TimestampedKeyValueIteratorFacade<>(inner.all());
    }

    @Override
    public KeyValueIterator<K, ValueAndTimestamp<V>> reverseAll() {
        return new TimestampedKeyValueIteratorFacade<>(inner.reverseAll());
    }

    @Override
    public long approximateNumEntries() {
        return inner.approximateNumEntries();
    }

    /**
     * Iterator facade that converts ValueTimestampHeaders to ValueAndTimestamp.
     */
    private static class TimestampedKeyValueIteratorFacade<K, V> implements KeyValueIterator<K, ValueAndTimestamp<V>> {
        private final KeyValueIterator<K, ValueTimestampHeaders<V>> innerIterator;

        TimestampedKeyValueIteratorFacade(final KeyValueIterator<K, ValueTimestampHeaders<V>> innerIterator) {
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
        public KeyValue<K, ValueAndTimestamp<V>> next() {
            final KeyValue<K, ValueTimestampHeaders<V>> next = innerIterator.next();
            final ValueAndTimestamp<V> valueAndTimestamp;
            if (next.value == null) {
                valueAndTimestamp = null;
            } else {
                valueAndTimestamp = ValueAndTimestamp.make(next.value.value(), next.value.timestamp());
            }
            return KeyValue.pair(next.key, valueAndTimestamp);
        }
    }
}