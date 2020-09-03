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
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;

import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

public class ReadOnlyWindowStoreFacade<K, V> implements ReadOnlyWindowStore<K, V> {
    protected final TimestampedWindowStore<K, V> inner;

    protected ReadOnlyWindowStoreFacade(final TimestampedWindowStore<K, V> store) {
        inner = store;
    }

    @Override
    public V fetch(final K key,
                   final long time) {
        return getValueOrNull(inner.fetch(key, time));
    }

    @Override
    @SuppressWarnings("deprecation")
    public WindowStoreIterator<V> fetch(final K key,
                                        final long timeFrom,
                                        final long timeTo) {
        return new WindowStoreIteratorFacade<>(inner.fetch(key, timeFrom, timeTo));
    }

    @Override
    public WindowStoreIterator<V> fetch(final K key,
                                        final Instant timeFrom,
                                        final Instant timeTo) throws IllegalArgumentException {
        return new WindowStoreIteratorFacade<>(inner.fetch(key, timeFrom, timeTo));
    }

    @Override
    public WindowStoreIterator<V> backwardFetch(final K key,
                                                final Instant timeFrom,
                                                final Instant timeTo) throws IllegalArgumentException {
        return new WindowStoreIteratorFacade<>(inner.backwardFetch(key, timeFrom, timeTo));
    }

    @Override
    @SuppressWarnings("deprecation")
    public KeyValueIterator<Windowed<K>, V> fetch(final K keyFrom,
                                                  final K keyTo,
                                                  final long timeFrom,
                                                  final long timeTo) {
        return new KeyValueIteratorFacade<>(inner.fetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K keyFrom,
                                                  final K keyTo,
                                                  final Instant timeFrom,
                                                  final Instant timeTo) throws IllegalArgumentException {
        return new KeyValueIteratorFacade<>(inner.fetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetch(final K keyFrom,
                                                          final K keyTo,
                                                          final Instant timeFrom,
                                                          final Instant timeTo) throws IllegalArgumentException {
        return new KeyValueIteratorFacade<>(inner.backwardFetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    @SuppressWarnings("deprecation")
    public KeyValueIterator<Windowed<K>, V> fetchAll(final long timeFrom,
                                                     final long timeTo) {
        return new KeyValueIteratorFacade<>(inner.fetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetchAll(final Instant timeFrom,
                                                     final Instant timeTo) throws IllegalArgumentException {
        return new KeyValueIteratorFacade<>(inner.fetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetchAll(final Instant timeFrom,
                                                             final Instant timeTo) throws IllegalArgumentException {
        return new KeyValueIteratorFacade<>(inner.backwardFetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> all() {
        return new KeyValueIteratorFacade<>(inner.all());
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardAll() {
        return new KeyValueIteratorFacade<>(inner.backwardAll());
    }

    private static class WindowStoreIteratorFacade<V> implements WindowStoreIterator<V> {
        final KeyValueIterator<Long, ValueAndTimestamp<V>> innerIterator;

        WindowStoreIteratorFacade(final KeyValueIterator<Long, ValueAndTimestamp<V>> iterator) {
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
        public KeyValue<Long, V> next() {
            final KeyValue<Long, ValueAndTimestamp<V>> innerKeyValue = innerIterator.next();
            return KeyValue.pair(innerKeyValue.key, getValueOrNull(innerKeyValue.value));
        }
    }
}