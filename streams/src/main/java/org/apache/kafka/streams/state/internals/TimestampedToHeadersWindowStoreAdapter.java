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
import org.apache.kafka.common.utils.ByteUtils;
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
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Map;

import static org.apache.kafka.streams.state.HeadersBytesStore.convertToHeaderFormat;

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
public class TimestampedToHeadersWindowStoreAdapter implements WindowStore<Bytes, byte[]> {
    private final WindowStore<Bytes, byte[]> store;

    public TimestampedToHeadersWindowStoreAdapter(final WindowStore<Bytes, byte[]> store) {
        if (!store.persistent()) {
            throw new IllegalArgumentException("Provided store must be a persistent store, but it is not.");
        }
        if (!(store instanceof TimestampedBytesStore)) {
            throw new IllegalArgumentException("Provided store must be a timestamped store, but it is not.");
        }
        this.store = store;
    }

    /**
     * Extract raw timestamped value (timestamp + value) from serialized ValueTimestampHeaders.
     * This strips the headers portion but keeps timestamp and value intact.
     *
     * Format conversion:
     * Input:  [headersSize(varint)][headers][timestamp(8)][value]
     * Output: [timestamp(8)][value]
     */
    // TODO: should be extract to util class, tracked by KAFKA-20205
    static byte[] rawTimestampedValue(final byte[] rawValueTimestampHeaders) {
        if (rawValueTimestampHeaders == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(rawValueTimestampHeaders);
        final int headersSize = ByteUtils.readVarint(buffer);
        // Skip headers, keep timestamp + value
        buffer.position(buffer.position() + headersSize);

        final byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
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
        return new TimestampedWindowToHeadersWindowStoreIteratorAdapter(store.fetch(key, timeFrom, timeTo));
    }

    @Override
    public WindowStoreIterator<byte[]> fetch(final Bytes key, final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return new TimestampedWindowToHeadersWindowStoreIteratorAdapter(store.fetch(key, timeFrom, timeTo));
    }

    @Override
    public WindowStoreIterator<byte[]> backwardFetch(final Bytes key, final long timeFrom, final long timeTo) {
        return new TimestampedWindowToHeadersWindowStoreIteratorAdapter(store.backwardFetch(key, timeFrom, timeTo));
    }

    @Override
    public WindowStoreIterator<byte[]> backwardFetch(final Bytes key, final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return new TimestampedWindowToHeadersWindowStoreIteratorAdapter(store.backwardFetch(key, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom, final Bytes keyTo,
                                                           final long timeFrom, final long timeTo) {
        return new TimestampedToHeadersIteratorAdapter<>(store.fetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom, final Bytes keyTo,
                                                           final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return new TimestampedToHeadersIteratorAdapter<>(store.fetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom, final Bytes keyTo,
                                                                   final long timeFrom, final long timeTo) {
        return new TimestampedToHeadersIteratorAdapter<>(store.backwardFetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom, final Bytes keyTo,
                                                                   final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return new TimestampedToHeadersIteratorAdapter<>(store.backwardFetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom, final long timeTo) {
        return new TimestampedToHeadersIteratorAdapter<>(store.fetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return new TimestampedToHeadersIteratorAdapter<>(store.fetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final long timeFrom, final long timeTo) {
        return new TimestampedToHeadersIteratorAdapter<>(store.backwardFetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final Instant timeFrom, final Instant timeTo) throws IllegalArgumentException {
        return new TimestampedToHeadersIteratorAdapter<>(store.backwardFetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        return new TimestampedToHeadersIteratorAdapter<>(store.all());
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
        return new TimestampedToHeadersIteratorAdapter<>(store.backwardAll());
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

        throw new UnsupportedOperationException("Queries (IQv2) are not supported for timestamped window stores with headers yet.");
    }

    @Override
    public Position getPosition() {
        return store.getPosition();
    }

    /**
     * Iterator adapter for WindowStoreIterator that converts timestamp-only values
     * to timestamp-with-headers format by adding empty headers.
     */
    private static class TimestampedWindowToHeadersWindowStoreIteratorAdapter
        extends TimestampedToHeadersIteratorAdapter<Long>
        implements WindowStoreIterator<byte[]> {

        TimestampedWindowToHeadersWindowStoreIteratorAdapter(final KeyValueIterator<Long, byte[]> innerIterator) {
            super(innerIterator);
        }
    }
}
