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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

import java.util.List;

/**
 * Adapter that wraps an old {@link TimestampedKeyValueStore} and adapts it to the
 * {@link TimestampedKeyValueStoreWithHeaders} interface for backward compatibility.
 * <p>
 * This allows old stores created with {@code Stores.timestampedKeyValueStoreBuilder()}
 * to work with DSL operators that expect the new headers-aware interface.
 * <p>
 * Values are converted between {@link ValueAndTimestamp} (old format) and
 * {@link ValueTimestampHeaders} (new format with empty headers).
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public class TimestampedKeyValueStoreToHeadersAdapter<K, V>
    extends WrappedStateStore<TimestampedKeyValueStore<K, V>, K, ValueTimestampHeaders<V>>
    implements TimestampedKeyValueStoreWithHeaders<K, V> {

    private static final Headers EMPTY_HEADERS = new RecordHeaders();

    public TimestampedKeyValueStoreToHeadersAdapter(final TimestampedKeyValueStore<K, V> inner) {
        super(inner);
    }

    @Override
    public void put(final K key, final ValueTimestampHeaders<V> value) {
        if (value == null) {
            wrapped().put(key, null);
        } else {
            wrapped().put(key, ValueAndTimestamp.make(value.value(), value.timestamp()));
        }
    }

    @Override
    public ValueTimestampHeaders<V> putIfAbsent(final K key, final ValueTimestampHeaders<V> value) {
        final ValueAndTimestamp<V> oldValue = wrapped().putIfAbsent(
            key,
            value == null ? null : ValueAndTimestamp.make(value.value(), value.timestamp())
        );
        return convertToHeaders(oldValue);
    }

    @Override
    public void putAll(final List<KeyValue<K, ValueTimestampHeaders<V>>> entries) {
        for (final KeyValue<K, ValueTimestampHeaders<V>> entry : entries) {
            put(entry.key, entry.value);
        }
    }

    @Override
    public ValueTimestampHeaders<V> delete(final K key) {
        final ValueAndTimestamp<V> oldValue = wrapped().delete(key);
        return convertToHeaders(oldValue);
    }

    @Override
    public ValueTimestampHeaders<V> get(final K key) {
        final ValueAndTimestamp<V> valueAndTimestamp = wrapped().get(key);
        return convertToHeaders(valueAndTimestamp);
    }

    @Override
    public KeyValueIterator<K, ValueTimestampHeaders<V>> range(final K from, final K to) {
        return new KeyValueIteratorAdapter<>(wrapped().range(from, to));
    }

    @Override
    public KeyValueIterator<K, ValueTimestampHeaders<V>> reverseRange(final K from, final K to) {
        return new KeyValueIteratorAdapter<>(wrapped().reverseRange(from, to));
    }

    @Override
    public KeyValueIterator<K, ValueTimestampHeaders<V>> all() {
        return new KeyValueIteratorAdapter<>(wrapped().all());
    }

    @Override
    public KeyValueIterator<K, ValueTimestampHeaders<V>> reverseAll() {
        return new KeyValueIteratorAdapter<>(wrapped().reverseAll());
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<K, ValueTimestampHeaders<V>> prefixScan(
        final P prefix,
        final PS prefixKeySerializer) {
        return new KeyValueIteratorAdapter<>(wrapped().prefixScan(prefix, prefixKeySerializer));
    }

    @Override
    public long approximateNumEntries() {
        return wrapped().approximateNumEntries();
    }

    private ValueTimestampHeaders<V> convertToHeaders(final ValueAndTimestamp<V> valueAndTimestamp) {
        if (valueAndTimestamp == null) {
            return null;
        }
        return ValueTimestampHeaders.make(
            valueAndTimestamp.value(),
            valueAndTimestamp.timestamp(),
            EMPTY_HEADERS
        );
    }

    private class KeyValueIteratorAdapter<K1, V1> implements KeyValueIterator<K1, ValueTimestampHeaders<V1>> {
        private final KeyValueIterator<K1, ValueAndTimestamp<V1>> inner;

        KeyValueIteratorAdapter(final KeyValueIterator<K1, ValueAndTimestamp<V1>> inner) {
            this.inner = inner;
        }

        @Override
        public void close() {
            inner.close();
        }

        @Override
        public K1 peekNextKey() {
            return inner.peekNextKey();
        }

        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        @Override
        public KeyValue<K1, ValueTimestampHeaders<V1>> next() {
            final KeyValue<K1, ValueAndTimestamp<V1>> next = inner.next();
            return KeyValue.pair(
                next.key,
                next.value == null ? null : ValueTimestampHeaders.make(
                    next.value.value(),
                    next.value.timestamp(),
                    EMPTY_HEADERS
                )
            );
        }
    }
}