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

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.internals.PrefixedSessionKeySchemas.TimeFirstSessionKeySchema;

import java.time.Instant;
import java.util.Objects;

public class RocksDBTimeOrderedSessionStore
    extends WrappedStateStore<AbstractRocksDBTimeOrderedSegmentedBytesStore<? extends Segment>, Object, Object>
    implements SessionStore<Bytes, byte[]> {

    private StateStoreContext stateStoreContext;

    RocksDBTimeOrderedSessionStore(final AbstractRocksDBTimeOrderedSegmentedBytesStore<? extends Segment> store) {
        super(store);
        Objects.requireNonNull(store, "store is null");
    }

    @Override
    public void init(final StateStoreContext stateStoreContext, final StateStore root) {
        wrapped().init(stateStoreContext, root);
        this.stateStoreContext = stateStoreContext;
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {

        final Position queryPosition = config.getIsolationLevel() == IsolationLevel.READ_COMMITTED
            ? wrapped().getCommittedPosition()
            : getPosition();
        return StoreQueryUtils.handleBasicQueries(
            query,
            positionBound,
            config,
            this,
            queryPosition,
            stateStoreContext
        );
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final long earliestSessionEndTime,
                                                                  final long latestSessionEndTime) {
        return sessionIterator(wrapped().fetchSessions(earliestSessionEndTime, latestSessionEndTime));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes key,
                                                                  final long earliestSessionEndTime,
                                                                  final long latestSessionStartTime) {
        return sessionIterator(wrapped().fetch(key, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(final Bytes key,
                                                                          final long earliestSessionEndTime,
                                                                          final long latestSessionStartTime) {
        return sessionIterator(wrapped().backwardFetch(key, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes keyFrom,
                                                                  final Bytes keyTo,
                                                                  final long earliestSessionEndTime,
                                                                  final long latestSessionStartTime) {
        return sessionIterator(wrapped().fetch(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(final Bytes keyFrom,
                                                                          final Bytes keyTo,
                                                                          final long earliestSessionEndTime,
                                                                          final long latestSessionStartTime) {
        return sessionIterator(wrapped().backwardFetch(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public byte[] fetchSession(final Bytes key,
                               final long sessionStartTime,
                               final long sessionEndTime) {
        return wrapped().fetchSession(key, sessionStartTime, sessionEndTime);
    }

    // Shared by the live methods above and the ReadOnlyView below; only the iterator's source differs.
    private static KeyValueIterator<Windowed<Bytes>, byte[]> sessionIterator(final KeyValueIterator<Bytes, byte[]> bytesIterator) {
        return new WrappedSessionStoreIterator(bytesIterator, TimeFirstSessionKeySchema::from);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes key) {
        return findSessions(key, 0, Long.MAX_VALUE);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes key) {
        return backwardFindSessions(key, 0, Long.MAX_VALUE);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom, final Bytes keyTo) {
        return findSessions(keyFrom, keyTo, 0, Long.MAX_VALUE);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom, final Bytes keyTo) {
        return backwardFindSessions(keyFrom, keyTo, 0, Long.MAX_VALUE);
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
    public ReadOnlySessionStore<Bytes, byte[]> readOnly(final IsolationLevel isolationLevel) {
        Objects.requireNonNull(isolationLevel, "isolationLevel cannot be null");
        return new ReadOnlyView(isolationLevel);
    }

    /** Read view; reads go through the segmented store's isolation view, so READ_COMMITTED hides staged writes. */
    private final class ReadOnlyView implements ReadOnlySessionStore<Bytes, byte[]> {

        private final AbstractRocksDBTimeOrderedSegmentedBytesStore.ReadOnlyView segmented;

        ReadOnlyView(final IsolationLevel isolationLevel) {
            this.segmented = wrapped().readOnly(isolationLevel);
        }

        @Override
        public byte[] fetchSession(final Bytes key, final long sessionStartTime, final long sessionEndTime) {
            return segmented.fetchSession(key, sessionStartTime, sessionEndTime);
        }

        @Override
        public byte[] fetchSession(final Bytes key, final Instant sessionStartTime, final Instant sessionEndTime) {
            return fetchSession(key, sessionStartTime.toEpochMilli(), sessionEndTime.toEpochMilli());
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes key,
                                                                      final long earliestSessionEndTime,
                                                                      final long latestSessionStartTime) {
            return sessionIterator(segmented.fetch(key, earliestSessionEndTime, latestSessionStartTime));
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(final Bytes key,
                                                                              final long earliestSessionEndTime,
                                                                              final long latestSessionStartTime) {
            return sessionIterator(segmented.backwardFetch(key, earliestSessionEndTime, latestSessionStartTime));
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes keyFrom,
                                                                      final Bytes keyTo,
                                                                      final long earliestSessionEndTime,
                                                                      final long latestSessionStartTime) {
            return sessionIterator(segmented.fetch(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime));
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(final Bytes keyFrom,
                                                                              final Bytes keyTo,
                                                                              final long earliestSessionEndTime,
                                                                              final long latestSessionStartTime) {
            return sessionIterator(segmented.backwardFetch(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime));
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes key) {
            return findSessions(key, 0, Long.MAX_VALUE);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes key) {
            return backwardFindSessions(key, 0, Long.MAX_VALUE);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom, final Bytes keyTo) {
            return findSessions(keyFrom, keyTo, 0, Long.MAX_VALUE);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom, final Bytes keyTo) {
            return backwardFindSessions(keyFrom, keyTo, 0, Long.MAX_VALUE);
        }
    }
}
