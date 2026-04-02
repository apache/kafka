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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

import java.time.Instant;

public class SessionHeadersStoreToSessionStoreAdapter<K, V>
    extends WrappedStateStore<SessionStore<K, V>, K, V>
    implements SessionStoreWithHeaders<K, V> {

    public SessionHeadersStoreToSessionStoreAdapter(final SessionStore<K, V> sessionStore) {
        super(sessionStore);
        if (sessionStore instanceof SessionStoreWithHeaders) {
            throw new ClassCastException();
        }
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<V>> findSessions(final long earliestSessionEndTime, final long latestSessionEndTime) {
        return new KeyValueIteratorAdapter(wrapped().findSessions(earliestSessionEndTime, latestSessionEndTime));
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<V>> findSessions(final K key, final long earliestSessionEndTime, final long latestSessionStartTime) {
        return new KeyValueIteratorAdapter(wrapped().findSessions(key, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<V>> findSessions(final K key, final Instant earliestSessionEndTime, final Instant latestSessionStartTime) {
        return new KeyValueIteratorAdapter(wrapped().findSessions(key, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<V>> backwardFindSessions(final K key, final long earliestSessionEndTime, final long latestSessionStartTime) {
        return new KeyValueIteratorAdapter(wrapped().backwardFindSessions(key, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<V>> backwardFindSessions(final K key, final Instant earliestSessionEndTime, final Instant latestSessionStartTime) {
        return new KeyValueIteratorAdapter(wrapped().backwardFindSessions(key, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<V>> findSessions(final K keyFrom, final K keyTo, final long earliestSessionEndTime, final long latestSessionStartTime) {
        return new KeyValueIteratorAdapter(wrapped().findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<V>> findSessions(final K keyFrom, final K keyTo, final Instant earliestSessionEndTime, final Instant latestSessionStartTime) {
        return new KeyValueIteratorAdapter(wrapped().findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<V>> backwardFindSessions(final K keyFrom, final K keyTo, final long earliestSessionEndTime, final long latestSessionStartTime) {
        return new KeyValueIteratorAdapter(wrapped().backwardFindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<V>> backwardFindSessions(final K keyFrom, final K keyTo, final Instant earliestSessionEndTime, final Instant latestSessionStartTime) {
        return new KeyValueIteratorAdapter(wrapped().backwardFindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public AggregationWithHeaders<V> fetchSession(final K key, final long sessionStartTime, final long sessionEndTime) {
        final V aggregate = wrapped().fetchSession(key, sessionStartTime, sessionEndTime);
        return aggregate == null ? null : AggregationWithHeaders.make(aggregate, new RecordHeaders());
    }

    @Override
    public AggregationWithHeaders<V> fetchSession(final K key, final Instant sessionStartTime, final Instant sessionEndTime) {
        final V aggregate = wrapped().fetchSession(key, sessionStartTime, sessionEndTime);
        return aggregate == null ? null : AggregationWithHeaders.make(aggregate, new RecordHeaders());
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<V>> fetch(final K key) {
        return null;
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<V>> backwardFetch(final K key) {
        return new KeyValueIteratorAdapter(wrapped().backwardFetch(key));
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<V>> fetch(final K keyFrom, final K keyTo) {
        return new KeyValueIteratorAdapter(wrapped().fetch(keyFrom, keyTo));
    }

    @Override
    public KeyValueIterator<Windowed<K>, AggregationWithHeaders<V>> backwardFetch(final K keyFrom, final K keyTo) {
        return new KeyValueIteratorAdapter(wrapped().backwardFetch(keyFrom, keyTo));
    }

    @Override
    public void remove(final Windowed<K> sessionKey) {
        wrapped().remove(sessionKey);
    }

    @Override
    public void put(final Windowed<K> sessionKey, final AggregationWithHeaders<V> aggregate) {
        wrapped().put(sessionKey, aggregate == null ? null : aggregate.aggregation());
    }

    @SuppressWarnings("deprecation")
    @Override
    public void flush() {
        wrapped().flush();
    }

    private final class KeyValueIteratorAdapter implements KeyValueIterator<Windowed<K>, AggregationWithHeaders<V>> {
        private final KeyValueIterator<Windowed<K>, V> innerIterator;

        private KeyValueIteratorAdapter(final KeyValueIterator<Windowed<K>, V> innerIterator) {
            this.innerIterator = innerIterator;
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
        public KeyValue<Windowed<K>, AggregationWithHeaders<V>> next() {
            final KeyValue<Windowed<K>, V> next = innerIterator.next();
            return KeyValue.pair(next.key, AggregationWithHeaders.make(next.value, new RecordHeaders()));
        }
    }
}
