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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

import java.util.List;

abstract class AbstractReadWriteDecorator<T extends StateStore, K, V> extends WrappedStateStore<T, K, V> {
    static final String ERROR_MESSAGE = "This method may only be called by Kafka Streams";

    private AbstractReadWriteDecorator(final T inner) {
        super(inner);
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        throw new UnsupportedOperationException(ERROR_MESSAGE);
    }

    @Override
    public void init(final StateStoreContext context,
                     final StateStore root) {
        throw new UnsupportedOperationException(ERROR_MESSAGE);
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException(ERROR_MESSAGE);
    }

    static StateStore getReadWriteStore(final StateStore store) {
        if (store instanceof TimestampedKeyValueStore) {
            return new TimestampedKeyValueStoreReadWriteDecorator<>((TimestampedKeyValueStore<?, ?>) store);
        } else if (store instanceof KeyValueStore) {
            return new KeyValueStoreReadWriteDecorator<>((KeyValueStore<?, ?>) store);
        } else if (store instanceof TimestampedWindowStore) {
            return new TimestampedWindowStoreReadWriteDecorator<>((TimestampedWindowStore<?, ?>) store);
        } else if (store instanceof WindowStore) {
            return new WindowStoreReadWriteDecorator<>((WindowStore<?, ?>) store);
        } else if (store instanceof SessionStore) {
            return new SessionStoreReadWriteDecorator<>((SessionStore<?, ?>) store);
        } else {
            return store;
        }
    }

    static class KeyValueStoreReadWriteDecorator<K, V>
        extends AbstractReadWriteDecorator<KeyValueStore<K, V>, K, V>
        implements KeyValueStore<K, V> {

        KeyValueStoreReadWriteDecorator(final KeyValueStore<K, V> inner) {
            super(inner);
        }

        @Override
        public V get(final K key) {
            return wrapped().get(key);
        }

        @Override
        public KeyValueIterator<K, V> range(final K from,
                                            final K to) {
            return wrapped().range(from, to);
        }

        @Override
        public KeyValueIterator<K, V> reverseRange(final K from,
                                                   final K to) {
            return wrapped().reverseRange(from, to);
        }

        @Override
        public KeyValueIterator<K, V> all() {
            return wrapped().all();
        }

        @Override
        public KeyValueIterator<K, V> reverseAll() {
            return wrapped().reverseAll();
        }

        @Override
        public long approximateNumEntries() {
            return wrapped().approximateNumEntries();
        }

        @Override
        public void put(final K key,
                        final V value) {
            wrapped().put(key, value);
        }

        @Override
        public V putIfAbsent(final K key,
                             final V value) {
            return wrapped().putIfAbsent(key, value);
        }

        @Override
        public void putAll(final List<KeyValue<K, V>> entries) {
            wrapped().putAll(entries);
        }

        @Override
        public V delete(final K key) {
            return wrapped().delete(key);
        }
    }

    static class TimestampedKeyValueStoreReadWriteDecorator<K, V>
        extends KeyValueStoreReadWriteDecorator<K, ValueAndTimestamp<V>>
        implements TimestampedKeyValueStore<K, V> {

        TimestampedKeyValueStoreReadWriteDecorator(final TimestampedKeyValueStore<K, V> inner) {
            super(inner);
        }
    }

    static class WindowStoreReadWriteDecorator<K, V>
        extends AbstractReadWriteDecorator<WindowStore<K, V>, K, V>
        implements WindowStore<K, V> {

        WindowStoreReadWriteDecorator(final WindowStore<K, V> inner) {
            super(inner);
        }

        @Override
        public void put(final K key,
                        final V value,
                        final long windowStartTimestamp) {
            wrapped().put(key, value, windowStartTimestamp);
        }

        @Override
        public V fetch(final K key,
                       final long time) {
            return wrapped().fetch(key, time);
        }

        @SuppressWarnings("deprecation") // note, this method must be kept if super#fetch(...) is removed
        @Override
        public WindowStoreIterator<V> fetch(final K key,
                                            final long timeFrom,
                                            final long timeTo) {
            return wrapped().fetch(key, timeFrom, timeTo);
        }

        @Override
        public WindowStoreIterator<V> backwardFetch(final K key,
                                                    final long timeFrom,
                                                    final long timeTo) {
            return wrapped().backwardFetch(key, timeFrom, timeTo);
        }

        @SuppressWarnings("deprecation") // note, this method must be kept if super#fetch(...) is removed
        @Override
        public KeyValueIterator<Windowed<K>, V> fetch(final K keyFrom,
                                                      final K keyTo,
                                                      final long timeFrom,
                                                      final long timeTo) {
            return wrapped().fetch(keyFrom, keyTo, timeFrom, timeTo);
        }

        @Override
        public KeyValueIterator<Windowed<K>, V> backwardFetch(final K keyFrom,
                                                              final K keyTo,
                                                              final long timeFrom,
                                                              final long timeTo) {
            return wrapped().backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
        }

        @SuppressWarnings("deprecation") // note, this method must be kept if super#fetch(...) is removed
        @Override
        public KeyValueIterator<Windowed<K>, V> fetchAll(final long timeFrom,
                                                         final long timeTo) {
            return wrapped().fetchAll(timeFrom, timeTo);
        }

        @Override
        public KeyValueIterator<Windowed<K>, V> backwardFetchAll(final long timeFrom,
                                                                 final long timeTo) {
            return wrapped().backwardFetchAll(timeFrom, timeTo);
        }

        @Override
        public KeyValueIterator<Windowed<K>, V> all() {
            return wrapped().all();
        }

        @Override
        public KeyValueIterator<Windowed<K>, V> backwardAll() {
            return wrapped().backwardAll();
        }
    }

    static class TimestampedWindowStoreReadWriteDecorator<K, V>
        extends WindowStoreReadWriteDecorator<K, ValueAndTimestamp<V>>
        implements TimestampedWindowStore<K, V> {

        TimestampedWindowStoreReadWriteDecorator(final TimestampedWindowStore<K, V> inner) {
            super(inner);
        }
    }

    static class SessionStoreReadWriteDecorator<K, AGG>
        extends AbstractReadWriteDecorator<SessionStore<K, AGG>, K, AGG>
        implements SessionStore<K, AGG> {

        SessionStoreReadWriteDecorator(final SessionStore<K, AGG> inner) {
            super(inner);
        }

        @Override
        public KeyValueIterator<Windowed<K>, AGG> findSessions(final K key,
                                                               final long earliestSessionEndTime,
                                                               final long latestSessionStartTime) {
            return wrapped().findSessions(key, earliestSessionEndTime, latestSessionStartTime);
        }

        @Override
        public KeyValueIterator<Windowed<K>, AGG> findSessions(final K keyFrom,
                                                               final K keyTo,
                                                               final long earliestSessionEndTime,
                                                               final long latestSessionStartTime) {
            return wrapped().findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
        }

        @Override
        public void remove(final Windowed<K> sessionKey) {
            wrapped().remove(sessionKey);
        }

        @Override
        public void put(final Windowed<K> sessionKey,
                        final AGG aggregate) {
            wrapped().put(sessionKey, aggregate);
        }

        @Override
        public AGG fetchSession(final K key,
                                final long startTime,
                                final long endTime) {
            return wrapped().fetchSession(key, startTime, endTime);
        }

        @Override
        public KeyValueIterator<Windowed<K>, AGG> fetch(final K key) {
            return wrapped().fetch(key);
        }

        @Override
        public KeyValueIterator<Windowed<K>, AGG> fetch(final K from,
                                                        final K to) {
            return wrapped().fetch(from, to);
        }
    }
}
