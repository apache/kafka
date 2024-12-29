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

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

import java.util.List;

abstract class AbstractReadOnlyDecorator<T extends StateStore, K, V> extends WrappedStateStore<T, K, V> {

    static final String ERROR_MESSAGE = "Global store is read only";

    private AbstractReadOnlyDecorator(final T inner) {
        super(inner);
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException(ERROR_MESSAGE);
    }

    @Override
    public void init(final StateStoreContext stateStoreContext,
                     final StateStore root) {
        throw new UnsupportedOperationException(ERROR_MESSAGE);
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException(ERROR_MESSAGE);
    }

    static StateStore getReadOnlyStore(final StateStore global) {
        if (global instanceof TimestampedKeyValueStore) {
            return new TimestampedKeyValueStoreReadOnlyDecorator<>((TimestampedKeyValueStore<?, ?>) global);
        } else if (global instanceof VersionedKeyValueStore) {
            return new VersionedKeyValueStoreReadOnlyDecorator<>((VersionedKeyValueStore<?, ?>) global);
        } else if (global instanceof KeyValueStore) {
            return new KeyValueStoreReadOnlyDecorator<>((KeyValueStore<?, ?>) global);
        } else if (global instanceof TimestampedWindowStore) {
            return new TimestampedWindowStoreReadOnlyDecorator<>((TimestampedWindowStore<?, ?>) global);
        } else if (global instanceof WindowStore) {
            return new WindowStoreReadOnlyDecorator<>((WindowStore<?, ?>) global);
        } else if (global instanceof SessionStore) {
            return new SessionStoreReadOnlyDecorator<>((SessionStore<?, ?>) global);
        } else {
            return global;
        }
    }

    static class KeyValueStoreReadOnlyDecorator<K, V>
        extends AbstractReadOnlyDecorator<KeyValueStore<K, V>, K, V>
        implements KeyValueStore<K, V> {

        private KeyValueStoreReadOnlyDecorator(final KeyValueStore<K, V> inner) {
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
        public <PS extends Serializer<P>, P> KeyValueIterator<K, V> prefixScan(final P prefix,
                                                                               final PS prefixKeySerializer) {
            return wrapped().prefixScan(prefix, prefixKeySerializer);
        }

        @Override
        public long approximateNumEntries() {
            return wrapped().approximateNumEntries();
        }

        @Override
        public void put(final K key,
                        final V value) {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @Override
        public V putIfAbsent(final K key,
                             final V value) {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @Override
        public void putAll(final List<KeyValue<K, V>> entries) {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @Override
        public V delete(final K key) {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    static class TimestampedKeyValueStoreReadOnlyDecorator<K, V>
        extends KeyValueStoreReadOnlyDecorator<K, ValueAndTimestamp<V>>
        implements TimestampedKeyValueStore<K, V> {

        private TimestampedKeyValueStoreReadOnlyDecorator(final TimestampedKeyValueStore<K, V> inner) {
            super(inner);
        }
    }

    static class VersionedKeyValueStoreReadOnlyDecorator<K, V>
        extends AbstractReadOnlyDecorator<VersionedKeyValueStore<K, V>, K, V>
        implements VersionedKeyValueStore<K, V> {

        private VersionedKeyValueStoreReadOnlyDecorator(final VersionedKeyValueStore<K, V> inner) {
            super(inner);
        }

        @Override
        public long put(final K key, final V value, final long timestamp) {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @Override
        public VersionedRecord<V> delete(final K key, final long timestamp) {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @Override
        public VersionedRecord<V> get(final K key) {
            return wrapped().get(key);
        }

        @Override
        public VersionedRecord<V> get(final K key, final long asOfTimestamp) {
            return wrapped().get(key, asOfTimestamp);
        }
    }

    static class WindowStoreReadOnlyDecorator<K, V>
        extends AbstractReadOnlyDecorator<WindowStore<K, V>, K, V>
        implements WindowStore<K, V> {

        private WindowStoreReadOnlyDecorator(final WindowStore<K, V> inner) {
            super(inner);
        }

        @Override
        public void put(final K key,
                        final V value,
                        final long windowStartTimestamp) {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @Override
        public V fetch(final K key,
                       final long time) {
            return wrapped().fetch(key, time);
        }

        @Override
        @Deprecated
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

        @Override
        @Deprecated
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

        @Override
        public KeyValueIterator<Windowed<K>, V> all() {
            return wrapped().all();
        }

        @Override
        public KeyValueIterator<Windowed<K>, V> backwardAll() {
            return wrapped().backwardAll();
        }

        @Override
        @Deprecated
        public KeyValueIterator<Windowed<K>, V> fetchAll(final long timeFrom,
                                                         final long timeTo) {
            return wrapped().fetchAll(timeFrom, timeTo);
        }

        @Override
        public KeyValueIterator<Windowed<K>, V> backwardFetchAll(final long timeFrom,
                                                                 final long timeTo) {
            return wrapped().backwardFetchAll(timeFrom, timeTo);
        }
    }

    static class TimestampedWindowStoreReadOnlyDecorator<K, V>
        extends WindowStoreReadOnlyDecorator<K, ValueAndTimestamp<V>>
        implements TimestampedWindowStore<K, V> {

        private TimestampedWindowStoreReadOnlyDecorator(final TimestampedWindowStore<K, V> inner) {
            super(inner);
        }
    }

    static class SessionStoreReadOnlyDecorator<K, AGG>
        extends AbstractReadOnlyDecorator<SessionStore<K, AGG>, K, AGG>
        implements SessionStore<K, AGG> {

        private SessionStoreReadOnlyDecorator(final SessionStore<K, AGG> inner) {
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
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @Override
        public void put(final Windowed<K> sessionKey,
                        final AGG aggregate) {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @Override
        public AGG fetchSession(final K key, final long earliestSessionEndTime, final long latestSessionStartTime) {
            return wrapped().fetchSession(key, earliestSessionEndTime, latestSessionStartTime);
        }

        @Override
        public KeyValueIterator<Windowed<K>, AGG> fetch(final K key) {
            return wrapped().fetch(key);
        }

        @Override
        public KeyValueIterator<Windowed<K>, AGG> fetch(final K keyFrom,
                                                        final K keyTo) {
            return wrapped().fetch(keyFrom, keyTo);
        }
    }
}
