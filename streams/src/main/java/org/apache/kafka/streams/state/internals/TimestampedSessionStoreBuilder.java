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
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.TimestampedSessionStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Objects;

public class TimestampedSessionStoreBuilder<K, V>
    extends AbstractStoreBuilder<K, ValueAndTimestamp<V>, TimestampedSessionStore<K, V>> {

    private final SessionBytesStoreSupplier storeSupplier;

    public TimestampedSessionStoreBuilder(final SessionBytesStoreSupplier storeSupplier,
                                          final Serde<K> keySerde,
                                          final Serde<V> valueSerde,
                                          final Time time) {
        super(storeSupplier.name(),
            keySerde, valueSerde == null ? null : new ValueAndTimestampSerde<>(valueSerde),
            time);
        Objects.requireNonNull(storeSupplier, "bytesStoreSupplier can't be null");
        this.storeSupplier = storeSupplier;
    }

    @Override
    public TimestampedSessionStore<K, V> build() {
        SessionStore<Bytes, byte[]> store = storeSupplier.get();
        if (!(store instanceof TimestampedBytesStore)) {
            if (store.persistent()) {
                store = new SessionToTimestampedSessionByteStoreAdapter(store);
            } else {
                store = new InMemoryTimestampedSessionStoreMarker(store);
            }
        }
        return new MeteredTimestampedSessionStore<>(
            maybeWrapCaching(maybeWrapLogging(store)),
            storeSupplier.metricsScope(),
            time,
            keySerde,
            valueSerde);
    }

    private SessionStore<Bytes, byte[]> maybeWrapLogging(final SessionStore<Bytes, byte[]> inner) {
        if (!enableLogging) {
            return inner;
        }
        return new ChangeLoggingTimestampedSessionBytesStore(inner);
    }

    private SessionStore<Bytes, byte[]> maybeWrapCaching(final SessionStore<Bytes, byte[]> inner) {
        if (!enableCaching) {
            return inner;
        }
        return new CachingSessionStore(
            inner,
            storeSupplier.segmentIntervalMs());
    }

    public long retentionPeriod() {
        return storeSupplier.retentionPeriod();
    }

    private final static class InMemoryTimestampedSessionStoreMarker
        implements SessionStore<Bytes, byte[]>, TimestampedBytesStore {

        private final SessionStore<Bytes, byte[]> wrapped;

        private InMemoryTimestampedSessionStoreMarker(final SessionStore<Bytes, byte[]> wrapped) {
            if (wrapped.persistent()) {
                throw new IllegalArgumentException("Provided store must not be a persistent store, but it is.");
            }
            this.wrapped = wrapped;
        }

        @Override
        public void init(final ProcessorContext context,
                         final StateStore root) {
            wrapped.init(context, root);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes key,
                                                                      final long earliestSessionEndTime,
                                                                      final long latestSessionStartTime) {
            return wrapped.findSessions(key, earliestSessionEndTime, latestSessionStartTime);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes keyFrom,
                                                                      final Bytes keyTo,
                                                                      final long earliestSessionEndTime,
                                                                      final long latestSessionStartTime) {
            return wrapped.findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
        }

        @Override
        public byte[] fetchSession(final Bytes key, final long startTime, final long endTime) {
            return wrapped.fetchSession(key, startTime, endTime);
        }

        @Override
        public void remove(final Windowed<Bytes> sessionKey) {
            wrapped.remove(sessionKey);
        }

        @Override
        public void put(final Windowed<Bytes> sessionKey, final byte[] aggregate) {
            wrapped.put(sessionKey, aggregate);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes key) {
            return wrapped.fetch(key);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes from, final Bytes to) {
            return wrapped.fetch(from, to);
        }

        @Override
        public void flush() {
            wrapped.flush();
        }

        @Override
        public void close() {
            wrapped.close();
        }
        @Override
        public boolean isOpen() {
            return wrapped.isOpen();
        }

        @Override
        public String name() {
            return wrapped.name();
        }

        @Override
        public boolean persistent() {
            return false;
        }
    }
}
