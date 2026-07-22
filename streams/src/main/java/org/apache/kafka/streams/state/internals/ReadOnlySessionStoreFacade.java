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

import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;

public class ReadOnlySessionStoreFacade<K, V> implements ReadOnlySessionStore<K, V> {
    protected final SessionStoreWithHeaders<K, V> inner;

    protected ReadOnlySessionStoreFacade(final SessionStoreWithHeaders<K, V> store) {
        inner = store;
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> findSessions(final K key,
                                                          final long earliestSessionEndTime,
                                                          final long latestSessionStartTime) {
        return new SessionStoreIteratorFacade<>(inner.findSessions(key, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFindSessions(final K key,
                                                                  final long earliestSessionEndTime,
                                                                  final long latestSessionStartTime) {
        return new SessionStoreIteratorFacade<>(inner.backwardFindSessions(key, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> findSessions(final K keyFrom,
                                                          final K keyTo,
                                                          final long earliestSessionEndTime,
                                                          final long latestSessionStartTime) {
        return new SessionStoreIteratorFacade<>(inner.findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFindSessions(final K keyFrom,
                                                                  final K keyTo,
                                                                  final long earliestSessionEndTime,
                                                                  final long latestSessionStartTime) {
        return new SessionStoreIteratorFacade<>(inner.backwardFindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public V fetchSession(final K key,
                          final long sessionStartTime,
                          final long sessionEndTime) {
        return AggregationWithHeaders.getAggregationOrNull(inner.fetchSession(key, sessionStartTime, sessionEndTime));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K key) {
        return new SessionStoreIteratorFacade<>(inner.fetch(key));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetch(final K key) {
        return new SessionStoreIteratorFacade<>(inner.backwardFetch(key));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K keyFrom, final K keyTo) {
        return new SessionStoreIteratorFacade<>(inner.fetch(keyFrom, keyTo));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetch(final K keyFrom, final K keyTo) {
        return new SessionStoreIteratorFacade<>(inner.backwardFetch(keyFrom, keyTo));
    }
}
