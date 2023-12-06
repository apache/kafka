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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.query.ResultOrder;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.VersionedRecord;
import org.rocksdb.Snapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LogicalSegmentRangeIterator implements KeyValueIterator {
    protected final ListIterator<LogicalKeyValueSegment> segmentIterator;
    private final Bytes fromKey;
    private final Bytes toKey;

    private final Long fromTime;
    private final Long toTime;
    private final ResultOrder keyOrder;
    protected ListIterator<KeyValue<Bytes, VersionedRecord<byte[]>>> iterator;

    // defined for creating/releasing the snapshot.
    private LogicalKeyValueSegment snapshotOwner;
    private Snapshot snapshot;



    public LogicalSegmentRangeIterator(final ListIterator<LogicalKeyValueSegment> segmentIterator,
                                       final Bytes fromKey,
                                       final Bytes toKey,
                                       final Long fromTime,
                                       final Long toTime,
                                       final ResultOrder keyOrder) {

        this.segmentIterator = segmentIterator;
        this.fromKey = fromKey;
        this.toKey = toKey;
        this.fromTime = fromTime;
        this.toTime = toTime;
        this.iterator = Collections.emptyListIterator();
        this.keyOrder = keyOrder;
        this.snapshot = null;
        this.snapshotOwner = null;
    }

    @Override
    public void close() {
        // user may refuse consuming all returned records, so release the snapshot when closing the iterator if it is not released yet!
        releaseSnapshot();
    }

    @Override
    public Object peekNextKey() {
        return null;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext() || maybeFillIterator();
    }

    @Override
    public Object next() {
        if (hasNext()) {
            return iterator.next();
        }
        return null;
    }
    private boolean maybeFillIterator() {

        final List<KeyValue<Bytes, VersionedRecord<byte[]>>> queryResults = new ArrayList<>();
        while (segmentIterator.hasNext()) {
            final LogicalKeyValueSegment segment = segmentIterator.next();

            if (snapshot == null) { // create the snapshot (this will happen only one time).
                this.snapshotOwner = segment;
                // take a RocksDB snapshot to return the segments content at the query time (in order to guarantee consistency)
                final Lock lock = new ReentrantLock();
                lock.lock();
                try {
                    this.snapshot = snapshotOwner.getSnapshot();
                } finally {
                    lock.unlock();
                }
            }

            final KeyValueIterator<Bytes, byte[]> rawSegmentValueIterator = segment.range(fromKey, toKey, snapshot);
            if (rawSegmentValueIterator != null) {
                if (segment.id() == -1) { // this is the latestValueStore
                    while (rawSegmentValueIterator.hasNext()) {
                        final KeyValue<Bytes, byte[]> next = rawSegmentValueIterator.next();
                        final long recordTimestamp = RocksDBVersionedStore.LatestValueFormatter.getTimestamp(next.value);
                        if (recordTimestamp <= toTime) {
                            // latest value satisfies timestamp bound
                            queryResults.add(new KeyValue<>(next.key, new VersionedRecord<>(RocksDBVersionedStore.LatestValueFormatter.getValue(next.value), recordTimestamp)));
                        }
                    }
                } else {
                    while (rawSegmentValueIterator.hasNext()) {
                        final KeyValue<Bytes, byte[]> next = rawSegmentValueIterator.next();
                        final List<RocksDBVersionedStoreSegmentValueFormatter.SegmentValue.SegmentSearchResult> searchResults =
                                RocksDBVersionedStoreSegmentValueFormatter.deserialize(next.value).findAll(fromTime, toTime);
                        for (final RocksDBVersionedStoreSegmentValueFormatter.SegmentValue.SegmentSearchResult searchResult : searchResults) {
                            queryResults.add(new KeyValue<>(next.key, new VersionedRecord<>(searchResult.value(), searchResult.validFrom(), Optional.of(searchResult.validTo()))));
                        }
                    }
                }
                if (!queryResults.isEmpty()) {
                    break;
                }
            }
        }
        if (!queryResults.isEmpty()) {
            if (!keyOrder.equals(ResultOrder.ANY)) {
                queryResults.sort((r1, r2) -> {
                    if (r1.value.timestamp() == r2.value.timestamp()) {
                        if (keyOrder.equals(ResultOrder.ASCENDING)) {
                            return r1.key.compareTo(r2.key);
                        } else if (keyOrder.equals(ResultOrder.DESCENDING)) {
                            return r2.key.compareTo(r1.key);
                        }
                    }
                    return 0;
                });
            }

            // if all segments have been processed, release the snapshot
            this.iterator = queryResults.listIterator();
            return true;
        }
        releaseSnapshot();
        return false;
    }

    private void releaseSnapshot() {
        if (snapshot != null) {
            snapshotOwner.releaseSnapshot(snapshot);
            snapshot = null;
        }
    }

}
