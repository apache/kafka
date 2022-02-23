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

import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.StoreToProcessorContextAdapter;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.KeyFirstWindowKeySchema;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.TimeFirstWindowKeySchema;

/**
 * RocksDB store backed by two SegmentedBytesStores which can optimize scan by time as well as window
 * lookup for a specific key.
 *
 * Schema for first SegmentedBytesStore (base store) is as below:
 *     Key schema: | timestamp + recordkey |
 *     Value schema: | value |. Value here is determined by caller.
 *
 * Schema for second SegmentedBytesStore (index store) is as below:
 *     Key schema: | record + timestamp |
 *     Value schema: ||
 *
 * Operations:
 *     Put: 1. Put to index store. 2. Put to base store.
 *     Delete: 1. Delete from base store. 2. Delete from index store.
 * Since we need to update two stores, failure can happen in the middle. We put in index store first
 * to make sure if a failure happens in second step and the view is inconsistent, we can't get the
 * value for the key. We delete from base store first to make sure if a failure happens in second step
 * and the view is inconsistent, we can't get the value for the key.
 *
 * Note:
 *     Index store can be optional if we can construct the timestamp in base store instead of looking
 *     them up from index store.
 *
 */
public class RocksDBTimeOrderedWindowStore implements WindowStore<Bytes, byte[]> {



    private final boolean retainDuplicates;
    private final long windowSize;
    private final String name;
    private int seqnum = 0;
    private RocksDBTimeOrderedSegmentedBytesStore baseStore;
    private RocksDBSegmentedBytesStore indexStore;

    private StateStoreContext stateStoreContext;
    private Position position;
    private boolean open;


    private class IndexToBaseStoreIterator implements KeyValueIterator<Bytes, byte[]> {
        private final KeyValueIterator<Bytes, byte[]> indexIterator;
        private byte[] cachedValue;


        IndexToBaseStoreIterator(final KeyValueIterator<Bytes, byte[]> indexIterator) {
            this.indexIterator = indexIterator;
        }

        @Override
        public void close() {
            indexIterator.close();
        }

        @Override
        public Bytes peekNextKey() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return indexIterator.peekNextKey();
        }

        @Override
        public boolean hasNext() {
            while (indexIterator.hasNext()) {
                final Bytes key = indexIterator.peekNextKey();
                final Bytes keyBytes = Bytes.wrap(KeyFirstWindowKeySchema.extractStoreKeyBytes(key.get()));
                final long timestamp = KeyFirstWindowKeySchema.extractStoreTimestamp(key.get());
                final int seqnum = KeyFirstWindowKeySchema.extractStoreSeqnum(key.get());

                cachedValue = baseStore.get(TimeFirstWindowKeySchema.toTimeOrderedStoreKeyBinary(key, timestamp, seqnum));
                if (cachedValue == null) {
                    // Key not in base store, inconsistency happened. Skip this key and reply on
                    // segment store to clean this.
                    indexIterator.next();
                } else {
                    return true;
                }
            }
            return false;
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            KeyValue<Bytes, byte[]> ret = indexIterator.next();
            return KeyValue.pair(ret.key, cachedValue);
        }
    }

    RocksDBTimeOrderedWindowStore(
        final RocksDBTimeOrderedSegmentedBytesStore baseStore,
        final String name,
        final boolean retainDuplicates,
        final long windowSize
    ) {
        this(baseStore, null, name, retainDuplicates, windowSize);
    }

    RocksDBTimeOrderedWindowStore(
        final RocksDBTimeOrderedSegmentedBytesStore baseStore,
        final RocksDBSegmentedBytesStore indexStore,
        final String name,
        final boolean retainDuplicates,
        final long windowSize
    ) {
        Objects.requireNonNull(baseStore, "baseStore is null");
        Objects.requireNonNull(name, "name is null");
        this.baseStore = baseStore;
        this.indexStore = indexStore;
        this.name = name;
        this.retainDuplicates = retainDuplicates;
        this.windowSize = windowSize;
    }

    @Override
    public String name() {
        return name;
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        baseStore.openSegments(context);
        if (hasIndexStore()) {
            indexStore.openSegments(context);
        }
        open = true;

        // TODO: register changelog callback and populate base and index store
        // register callback here since only window store knows how the keys are serialized in changelog
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        this.stateStoreContext = context;
        init(StoreToProcessorContextAdapter.adapt(context), root);
    }

    @Override
    public void flush() {
        baseStore.flush();
        if (hasIndexStore()) {
            indexStore.flush();
        }
    }

    @Override
    public void close() {
        open = false;
        baseStore.close();
        if (hasIndexStore()) {
            indexStore.close();
        }
    }

    @Override
    public Position getPosition() {
        return position;
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void put(final Bytes key, final byte[] value, final long windowStartTimestamp) {
        // Skip if value is null and duplicates are allowed since this delete is a no-op
        if (!(value == null && retainDuplicates)) {
            maybeUpdateSeqnumForDups();
            if (hasIndexStore()) {
                indexStore.put(
                    KeyFirstWindowKeySchema.toStoreKeyBinary(key, windowStartTimestamp, seqnum), new byte[0]);
            }
            baseStore.put(
                TimeFirstWindowKeySchema.toTimeOrderedStoreKeyBinary(key, windowStartTimestamp, seqnum), value);
        }
    }

    @Override
    public byte[] fetch(final Bytes key, final long timestamp) {
        // TODO: check if some segments in index store can be purged
        return baseStore.get(TimeFirstWindowKeySchema.toTimeOrderedStoreKeyBinary(key, timestamp, seqnum));
    }

    @Override
    public WindowStoreIterator<byte[]> fetch(final Bytes key, final long timeFrom, final long timeTo) {
        if (hasIndexStore()) {
            // If index store exists, we still prefer to fetch from index store since it's ordered
            // by key. The number of invalid keys we fetched should be much less than fetching
            // from base store.
            final KeyValueIterator<Bytes, byte[]> bytesIterator = indexStore.fetch(key, timeFrom,
                timeTo);
            return new WindowStoreIteratorWrapper(new IndexToBaseStoreIterator(bytesIterator),
                windowSize,
                KeyFirstWindowKeySchema::extractStoreTimestamp,
                KeyFirstWindowKeySchema::fromStoreBytesKey).valuesIterator();
        }

        final KeyValueIterator<Bytes, byte[]> bytesIterator = baseStore.fetch(key, timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator,
            windowSize,
            TimeFirstWindowKeySchema::extractStoreTimestamp,
            TimeFirstWindowKeySchema::fromStoreBytesKey).valuesIterator();
    }

    @Override
    public WindowStoreIterator<byte[]> backwardFetch(final Bytes key, final long timeFrom, final long timeTo) {
        if (hasIndexStore()) {
            // If index store exists, we still prefer to fetch from index store since it's ordered
            // by key. The number of invalid keys we fetched should be much less than fetching
            // from base store.
            final KeyValueIterator<Bytes, byte[]> bytesIterator = indexStore.backwardFetch(key, timeFrom,
                timeTo);
            return new WindowStoreIteratorWrapper(new IndexToBaseStoreIterator(bytesIterator),
                windowSize,
                KeyFirstWindowKeySchema::extractStoreTimestamp,
                KeyFirstWindowKeySchema::fromStoreBytesKey).valuesIterator();
        }

        final KeyValueIterator<Bytes, byte[]> bytesIterator = baseStore.backwardFetch(key, timeFrom, timeTo);
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
        final KeyValueIterator<Bytes, byte[]> bytesIterator = baseStore.fetch(keyFrom, keyTo, timeFrom, timeTo);
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
        final KeyValueIterator<Bytes, byte[]> bytesIterator = baseStore.backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator,
            windowSize,
            TimeFirstWindowKeySchema::extractStoreTimestamp,
            TimeFirstWindowKeySchema::fromStoreBytesKey).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = baseStore.all();
        return new WindowStoreIteratorWrapper(bytesIterator,
            windowSize,
            TimeFirstWindowKeySchema::extractStoreTimestamp,
            TimeFirstWindowKeySchema::fromStoreBytesKey).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = baseStore.backwardAll();
        return new WindowStoreIteratorWrapper(bytesIterator,
            windowSize,
            TimeFirstWindowKeySchema::extractStoreTimestamp,
            TimeFirstWindowKeySchema::fromStoreBytesKey).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom, final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = baseStore.fetchAll(timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator,
            windowSize,
            TimeFirstWindowKeySchema::extractStoreTimestamp,
            TimeFirstWindowKeySchema::fromStoreBytesKey).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final long timeFrom, final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = baseStore.backwardFetchAll(timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator,
            windowSize,
            TimeFirstWindowKeySchema::extractStoreTimestamp,
            TimeFirstWindowKeySchema::fromStoreBytesKey).keyValueIterator();
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

    private boolean hasIndexStore() {
        return indexStore != null;
    }
}
