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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.HeadersBytesStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;

import java.time.Instant;
import java.util.Objects;

/**
 * Builder for {@link SessionStoreWithHeaders} instances.
 *
 * This is analogous to {@link SessionStoreBuilder}, but uses
 * {@link AggregationWithHeaders} as the value wrapper and wires up the
 * header-aware store stack (change-logging, caching, metering).
 */
public class SessionStoreBuilderWithHeaders<K, V>
    extends AbstractStoreBuilder<K, AggregationWithHeaders<V>, SessionStoreWithHeaders<K, V>> {

    private final SessionBytesStoreSupplier storeSupplier;

    public SessionStoreBuilderWithHeaders(final SessionBytesStoreSupplier storeSupplier,
                                          final Serde<K> keySerde,
                                          final Serde<V> valueSerde,
                                          final Time time) {
        super(
            Objects.requireNonNull(storeSupplier, "storeSupplier cannot be null").name(),
            keySerde,
            valueSerde == null ? null : new AggregationWithHeadersSerde<>(valueSerde),
            time
        );
        Objects.requireNonNull(storeSupplier.metricsScope(), "storeSupplier's metricsScope can't be null");
        this.storeSupplier = storeSupplier;
    }

    @Override
    public SessionStoreWithHeaders<K, V> build() {
        SessionStore<Bytes, byte[]> sessionStore = storeSupplier.get();

        if (!(sessionStore instanceof HeadersBytesStore)) {
            if (sessionStore.persistent()) {
                sessionStore = new SessionToHeadersStoreAdapter(sessionStore);
            } else {
                sessionStore = new InMemorySessionStoreWithHeadersMarker(sessionStore);
            }
        }
        return new MeteredSessionStoreWithHeaders<>(
            maybeWrapCaching(maybeWrapLogging(sessionStore)),
            storeSupplier.metricsScope(),
            keySerde,
            valueSerde,
            time
        );
    }

    private SessionStore<Bytes, byte[]> maybeWrapCaching(final SessionStore<Bytes, byte[]> inner) {
        if (!enableCaching) {
            return inner;
        }
        return new CachingSessionStore(inner, storeSupplier.segmentIntervalMs());
    }

    private SessionStore<Bytes, byte[]> maybeWrapLogging(final SessionStore<Bytes, byte[]> inner) {
        if (!enableLogging) {
            return inner;
        }
        return new ChangeLoggingSessionBytesStoreWithHeaders(inner);
    }

    public long retentionPeriod() {
        return storeSupplier.retentionPeriod();
    }

    private static final class InMemorySessionStoreWithHeadersMarker
        extends WrappedStateStore<SessionStore<Bytes, byte[]>, Bytes, byte[]>
        implements SessionStore<Bytes, byte[]>, HeadersBytesStore {
        private InMemorySessionStoreWithHeadersMarker(final SessionStore<Bytes, byte[]> wrapped) {
            super(wrapped);
            if (wrapped.persistent()) {
                throw new IllegalArgumentException("Provided store must not be a persistent store, but it is.");
            }
        }

        @Override
        public void remove(final Windowed<Bytes> sessionKey) {
            wrapped().remove(sessionKey);
        }

        @Override
        public void put(final Windowed<Bytes> sessionKey, final byte[] aggregate) {
            wrapped().put(sessionKey, aggregate);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes key) {
            return wrapped().fetch(key);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom, final Bytes keyTo) {
            return wrapped().fetch(keyFrom, keyTo);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes key,
                                                                      final long earliestSessionEndTime,
                                                                      final long latestSessionStartTime) {
            return wrapped().findSessions(key, earliestSessionEndTime, latestSessionStartTime);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(final Bytes key,
                                                                              final long earliestSessionEndTime,
                                                                              final long latestSessionStartTime) {
            return wrapped().backwardFindSessions(key, earliestSessionEndTime, latestSessionStartTime);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes keyFrom,
                                                                      final Bytes keyTo,
                                                                      final long earliestSessionEndTime,
                                                                      final long latestSessionStartTime) {
            return wrapped().findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(final Bytes keyFrom,
                                                                              final Bytes keyTo,
                                                                              final long earliestSessionEndTime,
                                                                              final long latestSessionStartTime) {
            return wrapped().backwardFindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
        }

        @Override
        public byte[] fetchSession(final Bytes key,
                                   final long earliestSessionEndTime,
                                   final long latestSessionStartTime) {
            return wrapped().fetchSession(key, earliestSessionEndTime, latestSessionStartTime);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final long earliestSessionEndTime, final long latestSessionEndTime) {
            return wrapped().findSessions(earliestSessionEndTime, latestSessionEndTime);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes key,
                                                                      final Instant earliestSessionEndTime,
                                                                      final Instant latestSessionStartTime) {
            return wrapped().findSessions(key, earliestSessionEndTime, latestSessionStartTime);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(final Bytes key,
                                                                              final Instant earliestSessionEndTime,
                                                                              final Instant latestSessionStartTime) {
            return wrapped().backwardFindSessions(key, earliestSessionEndTime, latestSessionStartTime);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes keyFrom,
                                                                      final Bytes keyTo,
                                                                      final Instant earliestSessionEndTime,
                                                                      final Instant latestSessionStartTime) {
            return wrapped().findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(final Bytes keyFrom,
                                                                              final Bytes keyTo,
                                                                              final Instant earliestSessionEndTime,
                                                                              final Instant latestSessionStartTime) {
            return wrapped().backwardFindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
        }

        @Override
        public byte[] fetchSession(final Bytes key,
                                   final Instant sessionStartTime,
                                   final Instant sessionEndTime) {
            return wrapped().fetchSession(key, sessionStartTime, sessionEndTime);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes key) {
            return wrapped().backwardFetch(key);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom, final Bytes keyTo) {
            return wrapped().backwardFetch(keyFrom, keyTo);
        }

    }
}
