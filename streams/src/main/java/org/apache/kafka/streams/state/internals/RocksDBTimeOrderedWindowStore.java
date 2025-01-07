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
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.TimeFirstWindowKeySchema;

import java.util.Objects;


public class RocksDBTimeOrderedWindowStore
    extends WrappedStateStore<RocksDBTimeOrderedWindowSegmentedBytesStore, Object, Object>
    implements WindowStore<Bytes, byte[]>, TimestampedBytesStore {

    private final boolean retainDuplicates;
    private final long windowSize;

    private int seqnum = 0;
    private StateStoreContext stateStoreContext;

    RocksDBTimeOrderedWindowStore(
        final RocksDBTimeOrderedWindowSegmentedBytesStore store,
        final boolean retainDuplicates,
        final long windowSize
    ) {
        super(store);
        Objects.requireNonNull(store, "store is null");
        this.retainDuplicates = retainDuplicates;
        this.windowSize = windowSize;
    }

    @Override
    public void init(final StateStoreContext stateStoreContext, final StateStore root) {
        wrapped().init(stateStoreContext, root);
        this.stateStoreContext = stateStoreContext;
    }

    @Override
    public void flush() {
        wrapped().flush();
    }

    @Override
    public void close() {
        wrapped().close();
    }

    @Override
    public boolean persistent() {
        return wrapped().persistent();
    }

    @Override
    public boolean isOpen() {
        return wrapped().isOpen();
    }

    @Override
    public void put(final Bytes key, final byte[] value, final long windowStartTimestamp) {
        // Skip if value is null and duplicates are allowed since this delete is a no-op
        if (!(value == null && retainDuplicates)) {
            maybeUpdateSeqnumForDups();
            wrapped().put(key, windowStartTimestamp, seqnum, value);
        }
    }

    @Override
    public byte[] fetch(final Bytes key, final long timestamp) {
        return wrapped().fetch(key, timestamp, seqnum);
    }

    @Override
    public WindowStoreIterator<byte[]> fetch(final Bytes key, final long timeFrom, final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetch(key, timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator,
            windowSize,
            TimeFirstWindowKeySchema::extractStoreTimestamp,
            TimeFirstWindowKeySchema::fromStoreBytesKey).valuesIterator();
    }

    @Override
    public WindowStoreIterator<byte[]> backwardFetch(final Bytes key, final long timeFrom, final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().backwardFetch(key, timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator,
            windowSize,
            TimeFirstWindowKeySchema::extractStoreTimestamp,
            TimeFirstWindowKeySchema::fromStoreBytesKey).valuesIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom,
                                                           final Bytes keyTo,
                                                           final long timeFrom,
                                                           final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetch(keyFrom, keyTo, timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator,
            windowSize,
            TimeFirstWindowKeySchema::extractStoreTimestamp,
            TimeFirstWindowKeySchema::fromStoreBytesKey).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom,
                                                                   final Bytes keyTo,
                                                                   final long timeFrom,
                                                                   final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator,
            windowSize,
            TimeFirstWindowKeySchema::extractStoreTimestamp,
            TimeFirstWindowKeySchema::fromStoreBytesKey).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().all();
        return new WindowStoreIteratorWrapper(bytesIterator,
            windowSize,
            TimeFirstWindowKeySchema::extractStoreTimestamp,
            TimeFirstWindowKeySchema::fromStoreBytesKey).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().backwardAll();
        return new WindowStoreIteratorWrapper(bytesIterator,
            windowSize,
            TimeFirstWindowKeySchema::extractStoreTimestamp,
            TimeFirstWindowKeySchema::fromStoreBytesKey).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom, final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetchAll(timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator,
            windowSize,
            TimeFirstWindowKeySchema::extractStoreTimestamp,
            TimeFirstWindowKeySchema::fromStoreBytesKey).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final long timeFrom, final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().backwardFetchAll(timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator,
            windowSize,
            TimeFirstWindowKeySchema::extractStoreTimestamp,
            TimeFirstWindowKeySchema::fromStoreBytesKey).keyValueIterator();
    }

    public boolean hasIndex() {
        return wrapped().hasIndex();
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {

        return StoreQueryUtils.handleBasicQueries(
            query,
            positionBound,
            config,
            this,
            getPosition(),
            stateStoreContext
        );
    }

    private void maybeUpdateSeqnumForDups() {
        if (retainDuplicates) {
            seqnum = (seqnum + 1) & 0x7FFFFFFF;
        }
    }
}
