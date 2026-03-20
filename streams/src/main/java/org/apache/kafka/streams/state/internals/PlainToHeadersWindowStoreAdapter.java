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
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.query.internals.InternalQueryResultUtil;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;
import java.util.Map;

import static org.apache.kafka.streams.state.HeadersBytesStore.convertFromPlainToHeaderFormat;
import static org.apache.kafka.streams.state.internals.Utils.rawPlainValue;

/**
 * Adapter for backward compatibility between {@link TimestampedWindowStoreWithHeaders}
 * and plain {@link WindowStore}.
 * <p>
 * If a user provides a supplier for plain {@code WindowStore} (without timestamp or headers) when building
 * a {@code TimestampedWindowStoreWithHeaders}, this adapter translates between the plain
 * {@code byte[]} format and the timestamped-with-headers {@code byte[]} format.
 * <p>
 * Format conversion:
 * <ul>
 *   <li>Write: {@code [headers][timestamp][value]} → {@code [value]} (strip headers and timestamp)</li>
 *   <li>Read: {@code [value]} → {@code [headers][timestamp][value]} (add empty headers and timestamp=-1)</li>
 * </ul>
 */
public class PlainToHeadersWindowStoreAdapter implements WindowStore<Bytes, byte[]> {
    private final WindowStore<Bytes, byte[]> store;

    public PlainToHeadersWindowStoreAdapter(final WindowStore<Bytes, byte[]> store) {
        if (!store.persistent()) {
            throw new IllegalArgumentException("Provided store must be a persistent store, but it is not.");
        }
        if (store instanceof TimestampedBytesStore) {
            throw new IllegalArgumentException("Provided store must be a plain (non-timestamped) window store, but it is timestamped.");
        }
        this.store = store;
    }

    @Override
    public void put(final Bytes key, final byte[] valueWithTimestampAndHeaders, final long windowStartTimestamp) {
        store.put(key, rawPlainValue(valueWithTimestampAndHeaders), windowStartTimestamp);
    }

    @Override
    public byte[] fetch(final Bytes key, final long timestamp) {
        return convertFromPlainToHeaderFormat(store.fetch(key, timestamp));
    }

    @Override
    public WindowStoreIterator<byte[]> fetch(final Bytes key, final long timeFrom, final long timeTo) {
        return new PlainToHeadersWindowStoreIteratorAdapter(store.fetch(key, timeFrom, timeTo));
    }

    @Override
    public WindowStoreIterator<byte[]> fetch(final Bytes key, final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return new PlainToHeadersWindowStoreIteratorAdapter(store.fetch(key, timeFrom, timeTo));
    }

    @Override
    public WindowStoreIterator<byte[]> backwardFetch(final Bytes key, final long timeFrom, final long timeTo) {
        return new PlainToHeadersWindowStoreIteratorAdapter(store.backwardFetch(key, timeFrom, timeTo));
    }

    @Override
    public WindowStoreIterator<byte[]> backwardFetch(final Bytes key, final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return new PlainToHeadersWindowStoreIteratorAdapter(store.backwardFetch(key, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom, final Bytes keyTo,
                                                           final long timeFrom, final long timeTo) {
        return new PlainToHeadersIteratorAdapter<>(store.fetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom, final Bytes keyTo,
                                                           final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return new PlainToHeadersIteratorAdapter<>(store.fetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom, final Bytes keyTo,
                                                                   final long timeFrom, final long timeTo) {
        return new PlainToHeadersIteratorAdapter<>(store.backwardFetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom, final Bytes keyTo,
                                                                   final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return new PlainToHeadersIteratorAdapter<>(store.backwardFetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom, final long timeTo) {
        return new PlainToHeadersIteratorAdapter<>(store.fetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return new PlainToHeadersIteratorAdapter<>(store.fetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final long timeFrom, final long timeTo) {
        return new PlainToHeadersIteratorAdapter<>(store.backwardFetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return new PlainToHeadersIteratorAdapter<>(store.backwardFetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        return new PlainToHeadersIteratorAdapter<>(store.all());
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
        return new PlainToHeadersIteratorAdapter<>(store.backwardAll());
    }


    @SuppressWarnings("unchecked")
    @Override
    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {
        final long start = config.isCollectExecutionInfo() ? System.nanoTime() : -1L;
        final QueryResult<R> result;

        // Handle WindowKeyQuery: wrap iterator to convert from plain to headers format
        if (query instanceof WindowKeyQuery) {
            final WindowKeyQuery<Bytes, byte[]> windowKeyQuery = (WindowKeyQuery<Bytes, byte[]>) query;
            final QueryResult<WindowStoreIterator<byte[]>> rawResult = store.query(windowKeyQuery, positionBound, config);

            if (rawResult.isSuccess()) {
                final WindowStoreIterator<byte[]> wrappedIterator =
                    new PlainToHeadersWindowStoreIteratorAdapter(rawResult.getResult());
                result = (QueryResult<R>) InternalQueryResultUtil.copyAndSubstituteDeserializedResult(rawResult, wrappedIterator);
            } else {
                result = (QueryResult<R>) rawResult;
            }
        } else if (query instanceof WindowRangeQuery) {
            // Handle WindowRangeQuery: wrap iterator to convert values
            final WindowRangeQuery<Bytes, byte[]> windowRangeQuery = (WindowRangeQuery<Bytes, byte[]>) query;
            final QueryResult<KeyValueIterator<Windowed<Bytes>, byte[]>> rawResult =
                store.query(windowRangeQuery, positionBound, config);

            if (rawResult.isSuccess()) {
                final KeyValueIterator<Windowed<Bytes>, byte[]> wrappedIterator =
                    new PlainToHeadersIteratorAdapter<>(rawResult.getResult());
                result = (QueryResult<R>) InternalQueryResultUtil.copyAndSubstituteDeserializedResult(rawResult, wrappedIterator);
            } else {
                result = (QueryResult<R>) rawResult;
            }
        } else {
            // For other query types, delegate to the underlying store
            result = store.query(query, positionBound, config);
        }

        if (config.isCollectExecutionInfo()) {
            result.addExecutionInfo(
                "Handled in " + getClass() + " in " + (System.nanoTime() - start) + "ns"
            );
        }

        return result;
    }

    @Override
    public Position getPosition() {
        return store.getPosition();
    }

    @Override
    public String name() {
        return store.name();
    }

    @Override
    public void init(final StateStoreContext context,
                     final StateStore root) {
        store.init(context, root);
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
        return true;
    }

    @Override
    public boolean isOpen() {
        return store.isOpen();
    }
}
