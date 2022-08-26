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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;

import static org.apache.kafka.streams.state.TimestampedBytesStore.convertToTimestampedFormat;
import static org.apache.kafka.streams.state.internals.ValueAndTimestampDeserializer.rawValue;

class WindowToTimestampedWindowByteStoreAdapter implements WindowStore<Bytes, byte[]> {
    final WindowStore<Bytes, byte[]> store;

    WindowToTimestampedWindowByteStoreAdapter(final WindowStore<Bytes, byte[]> store) {
        if (!store.persistent()) {
            throw new IllegalArgumentException("Provided store must be a persistent store, but it is not.");
        }
        this.store = store;
    }

    @Override
    public void put(final Bytes key,
                    final byte[] valueWithTimestamp,
                    final long windowStartTimestamp) {
        store.put(key, valueWithTimestamp == null ? null : rawValue(valueWithTimestamp), windowStartTimestamp);
    }

    @Override
    public byte[] fetch(final Bytes key,
                        final long time) {
        return convertToTimestampedFormat(store.fetch(key, time));
    }

    @Override
    public WindowStoreIterator<byte[]> fetch(final Bytes key,
                                             final long timeFrom,
                                             final long timeTo) {
        return new WindowToTimestampedWindowIteratorAdapter(store.fetch(key, timeFrom, timeTo));
    }

    @Override
    public WindowStoreIterator<byte[]> fetch(final Bytes key,
                                             final Instant timeFrom,
                                             final Instant timeTo) throws IllegalArgumentException {
        return new WindowToTimestampedWindowIteratorAdapter(store.fetch(key, timeFrom, timeTo));
    }

    @Override
    public WindowStoreIterator<byte[]> backwardFetch(final Bytes key,
                                                     final long timeFrom,
                                                     final long timeTo) {
        return new WindowToTimestampedWindowIteratorAdapter(store.backwardFetch(key, timeFrom, timeTo));
    }

    @Override
    public WindowStoreIterator<byte[]> backwardFetch(final Bytes key,
                                                     final Instant timeFrom,
                                                     final Instant timeTo) throws IllegalArgumentException {
        return new WindowToTimestampedWindowIteratorAdapter(store.backwardFetch(key, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom,
                                                           final Bytes keyTo,
                                                           final long timeFrom,
                                                           final long timeTo) {
        return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.fetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom,
                                                                   final Bytes keyTo,
                                                                   final Instant timeFrom,
                                                                   final Instant timeTo) throws IllegalArgumentException {
        return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.backwardFetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom,
                                                           final Bytes keyTo,
                                                           final Instant timeFrom,
                                                           final Instant timeTo)  throws IllegalArgumentException {
        return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.fetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom,
                                                                   final Bytes keyTo,
                                                                   final long timeFrom,
                                                                   final long timeTo) {
        return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.backwardFetch(keyFrom, keyTo, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.all());
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
        return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.backwardAll());
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom,
                                                              final long timeTo) {
        return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.fetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final Instant timeFrom,
                                                              final Instant timeTo) throws IllegalArgumentException {
        return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.fetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final long timeFrom, final long timeTo) {
        return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.backwardFetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final Instant timeFrom,
                                                                      final Instant timeTo) throws IllegalArgumentException {
        return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.backwardFetchAll(timeFrom, timeTo));
    }

    @Override
    public String name() {
        return store.name();
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        store.init(context, root);
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        store.init(context, root);
    }

    @Override
    public void flush() {
        store.flush();
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

    @Override
    public <R> QueryResult<R> query(
        final Query<R> query,
        final PositionBound positionBound,
        final QueryConfig config) {

        final long start = config.isCollectExecutionInfo() ? System.nanoTime() : -1L;
        final QueryResult<R> result = store.query(query, positionBound, config);
        if (config.isCollectExecutionInfo()) {
            final long end = System.nanoTime();
            result.addExecutionInfo(
                "Handled in " + getClass() + " in " + (end - start) + "ns"
            );
        }
        return result;
    }

    @Override
    public Position getPosition() {
        return store.getPosition();
    }


    private static class WindowToTimestampedWindowIteratorAdapter
        extends KeyValueToTimestampedKeyValueIteratorAdapter<Long>
        implements WindowStoreIterator<byte[]> {

        WindowToTimestampedWindowIteratorAdapter(final KeyValueIterator<Long, byte[]> innerIterator) {
            super(innerIterator);
        }
    }

}