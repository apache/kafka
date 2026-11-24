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
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;
import java.util.Map;

import static org.apache.kafka.streams.state.HeadersBytesStore.convertToHeaderFormat;
import static org.apache.kafka.streams.state.internals.Utils.rawTimestampedValue;

/**
 * Adapter for backward compatibility between {@link TimestampedWindowStoreWithHeaders}
 * and {@link TimestampedWindowStore}.
 * <p>
 * If a user provides a supplier for {@code TimestampedWindowStore} (without headers) when building
 * a {@code TimestampedWindowStoreWithHeaders}, this adapter translates between the timestamped
 * {@code byte[]} format and the timestamped-with-headers {@code byte[]} format.
 * <p>
 * Format conversion:
 * <ul>
 *   <li>Write: {@code [headers][timestamp][value]} → {@code [timestamp][value]} (strip headers)</li>
 *   <li>Read: {@code [timestamp][value]} → {@code [headers][timestamp][value]} (add empty headers)</li>
 * </ul>
 */
public class TimestampedToHeadersWindowStoreAdapter implements WindowStore<Bytes, byte[]>, WithRetentionPeriod {
    final WindowStore<Bytes, byte[]> store;

    /**
     * Report the retention of the store being adapted. This adapter holds its delegate in a field
     * rather than as a {@link WrappedStateStore}, so {@code extractRetentionPeriod}'s unwrap walk
     * terminates here; without this it resolves -1 and the windowed restore optimisation is
     * silently skipped for every store behind the adapter.
     */
    @Override
    public long retentionPeriod() {
        return WithRetentionPeriod.resolveRetentionPeriod(store);
    }

    public TimestampedToHeadersWindowStoreAdapter(final WindowStore<Bytes, byte[]> store) {
        if (!store.persistent()) {
            throw new IllegalArgumentException("Provided store must be a persistent store, but it is not.");
        }
        if (!(store instanceof TimestampedBytesStore)) {
            throw new IllegalArgumentException("Provided store must be a timestamped store, but it is not.");
        }
        this.store = store;
    }

    @Override
    public void put(final Bytes key, final byte[] valueWithTimestampAndHeaders, final long windowStartTimestamp) {
        store.put(key, rawTimestampedValue(valueWithTimestampAndHeaders), windowStartTimestamp);
    }

    @Override
    public byte[] fetch(final Bytes key, final long timestamp) {
        return convertToHeaderFormat(store.fetch(key, timestamp));
    }

    @Override
    public WindowStoreIterator<byte[]> fetch(final Bytes key, final long timeFrom, final long timeTo) {
        return MappingKeyValueIteratorAdapter.timestampedToHeadersWindow(store.fetch(key, timeFrom, timeTo));
    }

    @Override
    public WindowStoreIterator<byte[]> fetch(final Bytes key, final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return MappingKeyValueIteratorAdapter.timestampedToHeadersWindow(store.fetch(key, timeFrom, timeTo));
    }

    @Override
    public WindowStoreIterator<byte[]> backwardFetch(final Bytes key, final long timeFrom, final long timeTo) {
        return MappingKeyValueIteratorAdapter.timestampedToHeadersWindow(store.backwardFetch(key, timeFrom, timeTo));
    }

    @Override
    public WindowStoreIterator<byte[]> backwardFetch(final Bytes key, final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return MappingKeyValueIteratorAdapter.timestampedToHeadersWindow(store.backwardFetch(key, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom, final Bytes keyTo,
                                                           final long timeFrom, final long timeTo) {
        return MappingKeyValueIteratorAdapter.timestampedToHeaders(store.fetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom, final Bytes keyTo,
                                                           final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return MappingKeyValueIteratorAdapter.timestampedToHeaders(store.fetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom, final Bytes keyTo,
                                                                   final long timeFrom, final long timeTo) {
        return MappingKeyValueIteratorAdapter.timestampedToHeaders(store.backwardFetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom, final Bytes keyTo,
                                                                   final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return MappingKeyValueIteratorAdapter.timestampedToHeaders(store.backwardFetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom, final long timeTo) {
        return MappingKeyValueIteratorAdapter.timestampedToHeaders(store.fetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return MappingKeyValueIteratorAdapter.timestampedToHeaders(store.fetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final long timeFrom, final long timeTo) {
        return MappingKeyValueIteratorAdapter.timestampedToHeaders(store.backwardFetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return MappingKeyValueIteratorAdapter.timestampedToHeaders(store.backwardFetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        return MappingKeyValueIteratorAdapter.timestampedToHeaders(store.all());
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
        return MappingKeyValueIteratorAdapter.timestampedToHeaders(store.backwardAll());
    }

    @Override
    public String name() {
        return store.name();
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
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
    public long approximateNumUncommittedBytes() {
        return store.approximateNumUncommittedBytes();
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

    @SuppressWarnings("unchecked")
    @Override
    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {
        final long start = config.isCollectExecutionInfo() ? System.nanoTime() : -1L;
        final QueryResult<R> result;

        // Handle WindowKeyQuery: wrap iterator to convert from timestamped to headers format
        if (query instanceof WindowKeyQuery) {
            final WindowKeyQuery<Bytes, byte[]> windowKeyQuery = (WindowKeyQuery<Bytes, byte[]>) query;
            final QueryResult<WindowStoreIterator<byte[]>> rawResult = store.query(windowKeyQuery, positionBound, config);

            if (rawResult.isSuccess()) {
                final WindowStoreIterator<byte[]> wrappedIterator =
                    MappingKeyValueIteratorAdapter.timestampedToHeadersWindow(rawResult.getResult());
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
                    MappingKeyValueIteratorAdapter.timestampedToHeaders(rawResult.getResult());
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
}
