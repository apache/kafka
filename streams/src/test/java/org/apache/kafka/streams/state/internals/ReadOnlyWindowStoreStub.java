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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * A very simple window store stub for testing purposes.
 */
public class ReadOnlyWindowStoreStub<K, V> implements ReadOnlyWindowStore<K, V>, StateStore {

    private final long windowSize;
    private final Map<Long, NavigableMap<K, V>> data = new HashMap<>();
    private boolean open  = true;

    public ReadOnlyWindowStoreStub(long windowSize) {
        this.windowSize = windowSize;
    }

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
    public KeyValueIterator<Windowed<K>, V> fetch(K from, K to, long timeFrom, long timeTo) {
        if (!open) {
            throw new InvalidStateStoreException("Store is not open");
        }
        final List<KeyValue<Windowed<K>, V>> results = new ArrayList<>();
        for (long now = timeFrom; now <= timeTo; now++) {
            final NavigableMap<K, V> kvMap = data.get(now);
            if (kvMap != null) {
                for (Entry<K, V> entry : kvMap.subMap(from, true, to, true).entrySet()) {
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


            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove() not supported in " + getClass().getName());
            }
        };
    }

    public void put(final K key, final V value, final long timestamp) {
        if (!data.containsKey(timestamp)) {
            data.put(timestamp, new TreeMap<K, V>());
        }
        data.get(timestamp).put(key, value);
    }

    @Override
    public String name() {
        return null;
    }

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

    public void setOpen(final boolean open) {
        this.open = open;
    }

    private class TheWindowStoreIterator<E> implements WindowStoreIterator<E> {

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

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove() not supported in " + getClass().getName());
        }
    }
}
