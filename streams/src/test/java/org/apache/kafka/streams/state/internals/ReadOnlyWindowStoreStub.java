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
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.internals.ApiUtils;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;

/**
 * A very simple window store stub for testing purposes.
 */
public class ReadOnlyWindowStoreStub<K, V> implements ReadOnlyWindowStore<K, V>, StateStore {

    private final long windowSize;
    private final NavigableMap<Long, NavigableMap<K, V>> data = new TreeMap<>();
    private boolean open = true;

    ReadOnlyWindowStoreStub(final long windowSize) {
        this.windowSize = windowSize;
    }

    @Override
    public V fetch(final K key, final long time) {
        final Map<K, V> kvMap = data.get(time);
        if (kvMap != null) {
            return kvMap.get(key);
        } else {
            return null;
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public WindowStoreIterator<V> fetch(final K key, final long timeFrom, final long timeTo) {
        if (!open) {
            throw new InvalidStateStoreException("Store is not open");
        }
        final List<KeyValue<Long, V>> results = new ArrayList<>();
        for (long now = timeFrom; now <= timeTo; now++) {
            final Map<K, V> kvMap = data.get(now);
            if (kvMap != null && kvMap.containsKey(key)) {
                results.add(new KeyValue<>(now, kvMap.get(key)));
            }
        }
        return new TheWindowStoreIterator<>(results.iterator());
    }

    @Override
    public WindowStoreIterator<V> fetch(final K key, final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return fetch(
            key,
            ApiUtils.validateMillisecondInstant(timeFrom, prepareMillisCheckFailMsgPrefix(timeFrom, "from")),
            ApiUtils.validateMillisecondInstant(timeTo, prepareMillisCheckFailMsgPrefix(timeTo, "to")));
    }

    @Override
    public WindowStoreIterator<V> backwardFetch(final K key, final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        final long timeFromTs = ApiUtils.validateMillisecondInstant(timeFrom, prepareMillisCheckFailMsgPrefix(timeFrom, "timeFrom"));
        final long timeToTs = ApiUtils.validateMillisecondInstant(timeTo, prepareMillisCheckFailMsgPrefix(timeTo, "timeTo"));
        if (!open) {
            throw new InvalidStateStoreException("Store is not open");
        }
        final List<KeyValue<Long, V>> results = new ArrayList<>();
        for (long now = timeToTs; now >= timeFromTs; now--) {
            final Map<K, V> kvMap = data.get(now);
            if (kvMap != null && kvMap.containsKey(key)) {
                results.add(new KeyValue<>(now, kvMap.get(key)));
            }
        }
        return new TheWindowStoreIterator<>(results.iterator());
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> all() {
        if (!open) {
            throw new InvalidStateStoreException("Store is not open");
        }
        final List<KeyValue<Windowed<K>, V>> results = new ArrayList<>();
        for (final long now : data.keySet()) {
            final NavigableMap<K, V> kvMap = data.get(now);
            if (kvMap != null) {
                for (final Entry<K, V> entry : kvMap.entrySet()) {
                    results.add(new KeyValue<>(new Windowed<>(entry.getKey(), new TimeWindow(now, now + windowSize)), entry.getValue()));
                }
            }
        }
        final Iterator<KeyValue<Windowed<K>, V>> iterator = results.iterator();

        return new KeyValueIterator<Windowed<K>, V>() {
            @Override
            public void close() {
            }

            @Override
            public Windowed<K> peekNextKey() {
                throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public KeyValue<Windowed<K>, V> next() {
                return iterator.next();
            }

        };
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardAll() {
        if (!open) {
            throw new InvalidStateStoreException("Store is not open");
        }
        final List<KeyValue<Windowed<K>, V>> results = new ArrayList<>();
        for (final long now : data.descendingKeySet()) {
            final NavigableMap<K, V> kvMap = data.get(now);
            if (kvMap != null) {
                for (final Entry<K, V> entry : kvMap.descendingMap().entrySet()) {
                    results.add(new KeyValue<>(new Windowed<>(entry.getKey(), new TimeWindow(now, now + windowSize)), entry.getValue()));
                }
            }
        }
        final Iterator<KeyValue<Windowed<K>, V>> iterator = results.iterator();

        return new KeyValueIterator<Windowed<K>, V>() {
            @Override
            public void close() {
            }

            @Override
            public Windowed<K> peekNextKey() {
                throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public KeyValue<Windowed<K>, V> next() {
                return iterator.next();
            }

        };
    }

    @SuppressWarnings("deprecation")
    @Override
    public KeyValueIterator<Windowed<K>, V> fetchAll(final long timeFrom, final long timeTo) {
        if (!open) {
            throw new InvalidStateStoreException("Store is not open");
        }
        final List<KeyValue<Windowed<K>, V>> results = new ArrayList<>();
        for (final long now : data.keySet()) {
            if (!(now >= timeFrom && now <= timeTo)) {
                continue;
            }
            final NavigableMap<K, V> kvMap = data.get(now);
            if (kvMap != null) {
                for (final Entry<K, V> entry : kvMap.entrySet()) {
                    results.add(new KeyValue<>(new Windowed<>(entry.getKey(), new TimeWindow(now, now + windowSize)), entry.getValue()));
                }
            }
        }
        final Iterator<KeyValue<Windowed<K>, V>> iterator = results.iterator();

        return new KeyValueIterator<Windowed<K>, V>() {
            @Override
            public void close() {
            }

            @Override
            public Windowed<K> peekNextKey() {
                throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public KeyValue<Windowed<K>, V> next() {
                return iterator.next();
            }

        };
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetchAll(final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return fetchAll(
            ApiUtils.validateMillisecondInstant(timeFrom, prepareMillisCheckFailMsgPrefix(timeFrom, "from")),
            ApiUtils.validateMillisecondInstant(timeTo, prepareMillisCheckFailMsgPrefix(timeTo, "to")));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetchAll(final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        final long timeFromTs = ApiUtils.validateMillisecondInstant(timeFrom, prepareMillisCheckFailMsgPrefix(timeFrom, "timeFrom"));
        final long timeToTs = ApiUtils.validateMillisecondInstant(timeTo, prepareMillisCheckFailMsgPrefix(timeTo, "timeTo"));
        if (!open) {
            throw new InvalidStateStoreException("Store is not open");
        }
        final List<KeyValue<Windowed<K>, V>> results = new ArrayList<>();
        for (final long now : data.descendingKeySet()) {
            if (!(now >= timeFromTs && now <= timeToTs)) {
                continue;
            }
            final NavigableMap<K, V> kvMap = data.get(now);
            if (kvMap != null) {
                for (final Entry<K, V> entry : kvMap.descendingMap().entrySet()) {
                    results.add(new KeyValue<>(new Windowed<>(entry.getKey(), new TimeWindow(now, now + windowSize)), entry.getValue()));
                }
            }
        }
        final Iterator<KeyValue<Windowed<K>, V>> iterator = results.iterator();

        return new KeyValueIterator<Windowed<K>, V>() {
            @Override
            public void close() {
            }

            @Override
            public Windowed<K> peekNextKey() {
                throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public KeyValue<Windowed<K>, V> next() {
                return iterator.next();
            }

        };
    }

    @SuppressWarnings("deprecation")
    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K keyFrom, final K keyTo, final long timeFrom, final long timeTo) {
        if (!open) {
            throw new InvalidStateStoreException("Store is not open");
        }
        final List<KeyValue<Windowed<K>, V>> results = new ArrayList<>();
        for (long now = timeFrom; now <= timeTo; now++) {
            final NavigableMap<K, V> kvMap = data.get(now);
            if (kvMap != null) {
                for (final Entry<K, V> entry : kvMap.subMap(keyFrom, true, keyTo, true).entrySet()) {
                    results.add(new KeyValue<>(new Windowed<>(entry.getKey(), new TimeWindow(now, now + windowSize)), entry.getValue()));
                }
            }
        }
        final Iterator<KeyValue<Windowed<K>, V>> iterator = results.iterator();

        return new KeyValueIterator<Windowed<K>, V>() {
            @Override
            public void close() {
            }

            @Override
            public Windowed<K> peekNextKey() {
                throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public KeyValue<Windowed<K>, V> next() {
                return iterator.next();
            }

        };
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K keyFrom,
                                                  final K keyTo,
                                                  final Instant timeFrom,
                                                  final Instant timeTo) throws IllegalArgumentException {
        return fetch(
            keyFrom,
            keyTo,
            ApiUtils.validateMillisecondInstant(timeFrom, prepareMillisCheckFailMsgPrefix(timeFrom, "fromTime")),
            ApiUtils.validateMillisecondInstant(timeTo, prepareMillisCheckFailMsgPrefix(timeTo, "toTime")));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetch(final K from,
                                                          final K to,
                                                          final Instant timeFrom,
                                                          final Instant timeTo) throws IllegalArgumentException {
        final long timeFromTs = ApiUtils.validateMillisecondInstant(timeFrom, prepareMillisCheckFailMsgPrefix(timeFrom, "timeFrom"));
        final long timeToTs = ApiUtils.validateMillisecondInstant(timeTo, prepareMillisCheckFailMsgPrefix(timeTo, "timeTo"));
        if (!open) {
            throw new InvalidStateStoreException("Store is not open");
        }
        final List<KeyValue<Windowed<K>, V>> results = new ArrayList<>();
        for (long now = timeToTs; now >= timeFromTs; now--) {
            final NavigableMap<K, V> kvMap = data.get(now);
            if (kvMap != null) {
                for (final Entry<K, V> entry : kvMap.subMap(from, true, to, true).descendingMap().entrySet()) {
                    results.add(new KeyValue<>(new Windowed<>(entry.getKey(), new TimeWindow(now, now + windowSize)), entry.getValue()));
                }
            }
        }
        final Iterator<KeyValue<Windowed<K>, V>> iterator = results.iterator();

        return new KeyValueIterator<Windowed<K>, V>() {
            @Override
            public void close() {
            }

            @Override
            public Windowed<K> peekNextKey() {
                throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public KeyValue<Windowed<K>, V> next() {
                return iterator.next();
            }

        };
    }

    public void put(final K key, final V value, final long timestamp) {
        if (!data.containsKey(timestamp)) {
            data.put(timestamp, new TreeMap<>());
        }
        data.get(timestamp).put(key, value);
    }

    @Override
    public String name() {
        return null;
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    void setOpen(final boolean open) {
        this.open = open;
    }

    private static class TheWindowStoreIterator<E> implements WindowStoreIterator<E> {

        private final Iterator<KeyValue<Long, E>> underlying;

        TheWindowStoreIterator(final Iterator<KeyValue<Long, E>> underlying) {
            this.underlying = underlying;
        }

        @Override
        public void close() {
        }

        @Override
        public Long peekNextKey() {
            throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
        }

        @Override
        public boolean hasNext() {
            return underlying.hasNext();
        }

        @Override
        public KeyValue<Long, E> next() {
            return underlying.next();
        }
    }
}
