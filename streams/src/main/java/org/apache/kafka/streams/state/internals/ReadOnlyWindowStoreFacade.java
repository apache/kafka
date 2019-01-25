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
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;

public class ReadOnlyWindowStoreFacade<K, V> implements ReadOnlyWindowStore<K, V> {
    protected final WindowStore<K, ValueAndTimestamp<V>> inner;

    protected ReadOnlyWindowStoreFacade(final WindowStore<K, ValueAndTimestamp<V>> store) {
        inner = store;
    }

    @Override
    public V fetch(final K key,
                   final long time) {
        final ValueAndTimestamp<V> valueAndTimestamp = inner.fetch(key, time);
        return valueAndTimestamp == null ? null : valueAndTimestamp.value();
    }

    @Override
    @SuppressWarnings("deprecation")
    public WindowStoreIterator<V> fetch(final K key,
                                        final long timeFrom,
                                        final long timeTo) {
        final KeyValueIterator<Long, ValueAndTimestamp<V>> innerIterator = inner.fetch(key, timeFrom, timeTo);
        return new WindowStoreIteratorFacade<>(innerIterator);
    }

    @Override
    public WindowStoreIterator<V> fetch(final K key,
                                        final Instant from,
                                        final Instant to) throws IllegalArgumentException {
        final KeyValueIterator<Long, ValueAndTimestamp<V>> innerIterator = inner.fetch(key, from, to);
        return new WindowStoreIteratorFacade<>(innerIterator);
    }

    @Override
    @SuppressWarnings("deprecation")
    public KeyValueIterator<Windowed<K>, V> fetch(final K from,
                                                  final K to,
                                                  final long timeFrom,
                                                  final long timeTo) {
        final KeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> innerIterator = inner.fetch(from, to, timeFrom, timeTo);
        return new KeyValueIteratorFacade<>(innerIterator);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K from,
                                                  final K to,
                                                  final Instant fromTime,
                                                  final Instant toTime) throws IllegalArgumentException {
        final KeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> innerIterator = inner.fetch(from, to, fromTime, toTime);
        return new KeyValueIteratorFacade<>(innerIterator);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> all() {
        final KeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> innerIterator = inner.all();
        return new KeyValueIteratorFacade<>(innerIterator);
    }

    @Override
    @SuppressWarnings("deprecation")
    public KeyValueIterator<Windowed<K>, V> fetchAll(final long timeFrom,
                                                     final long timeTo) {
        final KeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> innerIterator = inner.fetchAll(timeFrom, timeTo);
        return new KeyValueIteratorFacade<>(innerIterator);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetchAll(final Instant from,
                                                     final Instant to) throws IllegalArgumentException {
        final KeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> innerIterator = inner.fetchAll(from, to);
        return new KeyValueIteratorFacade<>(innerIterator);
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
            return KeyValue.pair(innerKeyValue.key, innerKeyValue.value.value());
        }
    }
}
