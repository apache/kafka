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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.query.internals.InternalQueryResultUtil;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;

import java.util.Map;

import static org.apache.kafka.streams.state.HeadersBytesStore.convertToHeaderFormat;

/**
 * This class is used to ensure backward compatibility at DSL level between
 * {@link org.apache.kafka.streams.state.SessionStoreWithHeaders} and
 * {@link org.apache.kafka.streams.state.SessionStore}.
 * <p>
 * If a user provides a supplier for {@code SessionStore} (without headers) via
 * {@link org.apache.kafka.streams.kstream.Materialized} when building
 * a {@code SessionStoreWithHeaders}, this adapter is used to translate between
 * the raw aggregation {@code byte[]} format and the aggregation-with-headers {@code byte[]} format.
 * <p>
 * On writes (put), the headers prefix is stripped from the aggregation-with-headers value
 * before delegating to the inner plain store, which stores raw aggregation bytes only.
 * On reads (get, fetch, findSessions), empty headers are prepended to the raw aggregation
 * value read from the inner store so the caller receives aggregation-with-headers bytes.
 *
 * @see SessionToHeadersIteratorAdapter
 */
@SuppressWarnings("unchecked")
public class SessionToHeadersStoreAdapter implements SessionStore<Bytes, byte[]> {
    final SessionStore<Bytes, byte[]> store;

    SessionToHeadersStoreAdapter(final SessionStore<Bytes, byte[]> store) {
        if (!store.persistent()) {
            throw new IllegalArgumentException("Provided store must be a persistent store, but it is not.");
        }
        this.store = store;
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes key,
                                                                  final long earliestSessionEndTime,
                                                                  final long latestSessionStartTime) {
        return new SessionToHeadersIteratorAdapter(
            store.findSessions(key, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final long earliestSessionEndTime,
                                                                  final long latestSessionEndTime) {
        return new SessionToHeadersIteratorAdapter(
            store.findSessions(earliestSessionEndTime, latestSessionEndTime));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(final Bytes key,
                                                                          final long earliestSessionEndTime,
                                                                          final long latestSessionStartTime) {
        return new SessionToHeadersIteratorAdapter(
            store.backwardFindSessions(key, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes keyFrom,
                                                                  final Bytes keyTo,
                                                                  final long earliestSessionEndTime,
                                                                  final long latestSessionStartTime) {
        return new SessionToHeadersIteratorAdapter(
            store.findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(final Bytes keyFrom,
                                                                          final Bytes keyTo,
                                                                          final long earliestSessionEndTime,
                                                                          final long latestSessionStartTime) {
        return new SessionToHeadersIteratorAdapter(
            store.backwardFindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public byte[] fetchSession(final Bytes key,
                               final long sessionStartTime,
                               final long sessionEndTime) {
        return convertToHeaderFormat(store.fetchSession(key, sessionStartTime, sessionEndTime));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes key) {
        return new SessionToHeadersIteratorAdapter(store.fetch(key));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes key) {
        return new SessionToHeadersIteratorAdapter(store.backwardFetch(key));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom, final Bytes keyTo) {
        return new SessionToHeadersIteratorAdapter(store.fetch(keyFrom, keyTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom, final Bytes keyTo) {
        return new SessionToHeadersIteratorAdapter(store.backwardFetch(keyFrom, keyTo));
    }

    @Override
    public void remove(final Windowed<Bytes> sessionKey) {
        store.remove(sessionKey);
    }

    @Override
    public void put(final Windowed<Bytes> sessionKey, final byte[] aggregateWithHeader) {
        store.put(sessionKey, Utils.rawAggregation(aggregateWithHeader));
    }

    @Override
    public String name() {
        return store.name();
    }

    @Override
    public void init(final StateStoreContext stateStoreContext, final StateStore root) {
        store.init(stateStoreContext, root);
    }

    @Override
    public void commit(final Map<TopicPartition, Long> changelogOffsets) {
        store.commit(changelogOffsets);
    }

    @SuppressWarnings("deprecation")
    @Override
    public boolean managesOffsets() {
        return store.managesOffsets();
    }

    @Override
    public Long committedOffset(final TopicPartition partition) {
        return store.committedOffset(partition);
    }

    @Override
    public void close() {
        store.close();
    }

    @Override
    public boolean persistent() {
        return store.persistent();
    }

    @Override
    public boolean isOpen() {
        return store.isOpen();
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {
        final long start = config.isCollectExecutionInfo() ? System.nanoTime() : -1L;
        final QueryResult<R> result;
        if (query instanceof WindowRangeQuery) {
            final WindowRangeQuery<Bytes, byte[]> windowRangeQuery = (WindowRangeQuery<Bytes, byte[]>) query;
            final QueryResult<KeyValueIterator<Windowed<Bytes>, byte[]>> rawResult = store.query(windowRangeQuery, positionBound, config);
            if (rawResult.isSuccess()) {
                final KeyValueIterator<Windowed<Bytes>, byte[]> wrappedIterator = new SessionToHeadersIteratorAdapter(rawResult.getResult());
                result = (QueryResult<R>) InternalQueryResultUtil.copyAndSubstituteDeserializedResult(rawResult, wrappedIterator);
            } else {
                result = (QueryResult<R>) rawResult;
            }
        } else {
            result = store.query(query, positionBound, config);
        }
        if (config.isCollectExecutionInfo()) {
            result.addExecutionInfo(
                "Handled in " + getClass() + " in " + (System.nanoTime() - start) + "ns");
        }
        return result;
    }

    @Override
    public Position getPosition() {
        return store.getPosition();
    }
}
