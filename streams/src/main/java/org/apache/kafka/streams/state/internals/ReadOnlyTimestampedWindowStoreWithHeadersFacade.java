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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;

/**
 * A facade that wraps {@link TimestampedWindowStoreWithHeaders} to provide a
 * {@link ReadOnlyWindowStore} interface with {@link ValueAndTimestamp} values.
 * This facade converts {@link ValueTimestampHeaders} to {@link ValueAndTimestamp}, discarding headers.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ReadOnlyTimestampedWindowStoreWithHeadersFacade<K, V> implements ReadOnlyWindowStore<K, ValueAndTimestamp<V>> {
    protected final TimestampedWindowStoreWithHeaders<K, V> inner;

    protected ReadOnlyTimestampedWindowStoreWithHeadersFacade(final TimestampedWindowStoreWithHeaders<K, V> store) {
        inner = store;
    }

    @Override
    public ValueAndTimestamp<V> fetch(final K key, final long time) {
        final ValueTimestampHeaders<V> valueTimestampHeaders = inner.fetch(key, time);
        return valueTimestampHeaders == null ? null :
            ValueAndTimestamp.make(valueTimestampHeaders.value(), valueTimestampHeaders.timestamp());
    }

    @Override
    public WindowStoreIterator<ValueAndTimestamp<V>> fetch(final K key,
                                                            final Instant timeFrom,
                                                            final Instant timeTo) throws IllegalArgumentException {
        return new WindowStoreIteratorFacade<>(inner.fetch(key, timeFrom, timeTo));
    }

    @Override
    public WindowStoreIterator<ValueAndTimestamp<V>> backwardFetch(final K key,
                                                                    final Instant timeFrom,
                                                                    final Instant timeTo) throws IllegalArgumentException {
        return new WindowStoreIteratorFacade<>(inner.backwardFetch(key, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> fetch(final K keyFrom,
                                                                      final K keyTo,
                                                                      final Instant timeFrom,
                                                                      final Instant timeTo) throws IllegalArgumentException {
        return new KeyValueIteratorFacade<>(inner.fetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> backwardFetch(final K keyFrom,
                                                                              final K keyTo,
                                                                              final Instant timeFrom,
                                                                              final Instant timeTo) throws IllegalArgumentException {
        return new KeyValueIteratorFacade<>(inner.backwardFetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> fetchAll(final Instant timeFrom,
                                                                         final Instant timeTo) throws IllegalArgumentException {
        return new KeyValueIteratorFacade<>(inner.fetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> backwardFetchAll(final Instant timeFrom,
                                                                                 final Instant timeTo) throws IllegalArgumentException {
        return new KeyValueIteratorFacade<>(inner.backwardFetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> all() {
        return new KeyValueIteratorFacade<>(inner.all());
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> backwardAll() {
        return new KeyValueIteratorFacade<>(inner.backwardAll());
    }

    private static class WindowStoreIteratorFacade<V> implements WindowStoreIterator<ValueAndTimestamp<V>> {
        final KeyValueIterator<Long, ValueTimestampHeaders<V>> innerIterator;

        WindowStoreIteratorFacade(final KeyValueIterator<Long, ValueTimestampHeaders<V>> iterator) {
            innerIterator = iterator;
        }

        @Override
        public void close() {
            innerIterator.close();
        }

        @Override
        public Long peekNextKey() {
            return innerIterator.peekNextKey();
        }

        @Override
        public boolean hasNext() {
            return innerIterator.hasNext();
        }

        @Override
        public KeyValue<Long, ValueAndTimestamp<V>> next() {
            final KeyValue<Long, ValueTimestampHeaders<V>> innerKeyValue = innerIterator.next();
            final ValueAndTimestamp<V> valueAndTimestamp = innerKeyValue.value == null ? null :
                ValueAndTimestamp.make(innerKeyValue.value.value(), innerKeyValue.value.timestamp());
            return KeyValue.pair(innerKeyValue.key, valueAndTimestamp);
        }
    }

    private static class KeyValueIteratorFacade<K, V> implements KeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> {
        private final KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> innerIterator;

        KeyValueIteratorFacade(final KeyValueIterator<Windowed<K>, ValueTimestampHeaders<V>> iterator) {
            innerIterator = iterator;
        }

        @Override
        public void close() {
            innerIterator.close();
        }

        @Override
        public Windowed<K> peekNextKey() {
            return innerIterator.peekNextKey();
        }

        @Override
        public boolean hasNext() {
            return innerIterator.hasNext();
        }

        @Override
        public KeyValue<Windowed<K>, ValueAndTimestamp<V>> next() {
            final KeyValue<Windowed<K>, ValueTimestampHeaders<V>> innerKeyValue = innerIterator.next();
            final ValueAndTimestamp<V> valueAndTimestamp = innerKeyValue.value == null ? null :
                ValueAndTimestamp.make(innerKeyValue.value.value(), innerKeyValue.value.timestamp());
            return KeyValue.pair(innerKeyValue.key, valueAndTimestamp);
        }
    }
}