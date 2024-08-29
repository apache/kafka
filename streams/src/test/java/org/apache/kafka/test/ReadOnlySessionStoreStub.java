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
package org.apache.kafka.test;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlySessionStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

public class ReadOnlySessionStoreStub<K, V> implements ReadOnlySessionStore<K, V>, StateStore {
    private final NavigableMap<K, List<KeyValue<Windowed<K>, V>>> sessions = new TreeMap<>();
    private boolean open = true;

    public void put(final Windowed<K> sessionKey, final V value) {
        if (!sessions.containsKey(sessionKey.key())) {
            sessions.put(sessionKey.key(), new ArrayList<>());
        }
        sessions.get(sessionKey.key()).add(KeyValue.pair(sessionKey, value));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> findSessions(K key, long earliestSessionEndTime, long latestSessionStartTime) {
        throw new UnsupportedOperationException("Moved from Session Store. Implement if needed");
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFindSessions(K key, long earliestSessionEndTime, long latestSessionStartTime) {
        throw new UnsupportedOperationException("Moved from Session Store. Implement if needed");
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> findSessions(K keyFrom, K keyTo, long earliestSessionEndTime, long latestSessionStartTime) {
        throw new UnsupportedOperationException("Moved from Session Store. Implement if needed");
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFindSessions(K keyFrom, K keyTo, long earliestSessionEndTime, long latestSessionStartTime) {
        throw new UnsupportedOperationException("Moved from Session Store. Implement if needed");
    }

    @Override
    public V fetchSession(K key, long earliestSessionEndTime, long latestSessionStartTime) {
        throw new UnsupportedOperationException("Moved from Session Store. Implement if needed");
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K key) {
        if (!open) {
            throw new InvalidStateStoreException("not open");
        }
        if (!sessions.containsKey(key)) {
            return new KeyValueIteratorStub<>(Collections.emptyIterator());
        }
        return new KeyValueIteratorStub<>(sessions.get(key).iterator());
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetch(K key) {
        if (!open) {
            throw new InvalidStateStoreException("not open");
        }
        if (!sessions.containsKey(key)) {
            return new KeyValueIteratorStub<>(Collections.emptyIterator());
        }
        return new KeyValueIteratorStub<>(sessions.descendingMap().get(key).iterator());
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K keyFrom, final K keyTo) {
        if (!open) {
            throw new InvalidStateStoreException("not open");
        }

        NavigableMap<K, List<KeyValue<Windowed<K>, V>>> subSessionsMap = getSubSessionsMap(keyFrom, keyTo);

        if (subSessionsMap.isEmpty()) {
            return new KeyValueIteratorStub<>(Collections.emptyIterator());
        }
        final Iterator<List<KeyValue<Windowed<K>, V>>> keysIterator = subSessionsMap.values().iterator();
        return new KeyValueIteratorStub<>(
            new Iterator<KeyValue<Windowed<K>, V>>() {

                Iterator<KeyValue<Windowed<K>, V>> it;

                @Override
                public boolean hasNext() {
                    while (it == null || !it.hasNext()) {
                        if (!keysIterator.hasNext()) {
                            return false;
                        }
                        it = keysIterator.next().iterator();
                    }
                    return true;
                }

                @Override
                public KeyValue<Windowed<K>, V> next() {
                    return it.next();
                }

            }
        );
    }

    private NavigableMap<K, List<KeyValue<Windowed<K>, V>>> getSubSessionsMap(final K keyFrom, final K keyTo) {
        final NavigableMap<K, List<KeyValue<Windowed<K>, V>>> subSessionsMap;
        if (keyFrom == null && keyTo == null) { // fetch all
            subSessionsMap = sessions;
        } else if (keyFrom == null) {
            subSessionsMap = sessions.headMap(keyTo, true);
        } else if (keyTo == null) {
            subSessionsMap = sessions.tailMap(keyFrom, true);
        } else {
            subSessionsMap = sessions.subMap(keyFrom, true, keyTo, true);
        }
        return subSessionsMap;
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetch(K keyFrom, K keyTo) {
        if (!open) {
            throw new InvalidStateStoreException("not open");
        }

        NavigableMap<K, List<KeyValue<Windowed<K>, V>>> subSessionsMap = getSubSessionsMap(keyFrom, keyTo);

        if (subSessionsMap.isEmpty()) {
            return new KeyValueIteratorStub<>(Collections.emptyIterator());
        }

        final Iterator<List<KeyValue<Windowed<K>, V>>> keysIterator = subSessionsMap.descendingMap().values().iterator();
        return new KeyValueIteratorStub<>(
            new Iterator<KeyValue<Windowed<K>, V>>() {

                Iterator<KeyValue<Windowed<K>, V>> it;

                @Override
                public boolean hasNext() {
                    while (it == null || !it.hasNext()) {
                        if (!keysIterator.hasNext()) {
                            return false;
                        }
                        it = keysIterator.next().iterator();
                    }
                    return true;
                }

                @Override
                public KeyValue<Windowed<K>, V> next() {
                    return it.next();
                }
            }
        );
    }

    @Override
    public String name() {
        return "";
    }

    @Override
    public void init(StateStoreContext context, StateStore root) {}

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

    @Override
    public Position getPosition() {
        throw new UnsupportedOperationException("Position handling not implemented");
    }


    public void setOpen(final boolean open) {
        this.open = open;
    }
}
