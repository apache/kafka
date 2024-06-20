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
import org.apache.kafka.streams.query.ResultOrder;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.streams.state.VersionedRecordIterator;
import org.rocksdb.Snapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

public class LogicalSegmentIterator implements VersionedRecordIterator {
    private final ListIterator<LogicalKeyValueSegment> segmentIterator;
    private final Bytes key;
    private final Long fromTime;
    private final Long toTime;
    private final ResultOrder order;
    private ListIterator<VersionedRecord<byte[]>> iterator;
    private volatile boolean open = true;

    // defined for creating/releasing the snapshot. 
    private LogicalKeyValueSegment snapshotOwner = null;
    private Snapshot snapshot = null;



    public LogicalSegmentIterator(final ListIterator<LogicalKeyValueSegment> segmentIterator,
                                  final Bytes key,
                                  final Long fromTime,
                                  final Long toTime,
                                  final ResultOrder order) {

        this.segmentIterator = segmentIterator;
        this.key = key;
        this.fromTime = fromTime;
        this.toTime = toTime;
        this.iterator = Collections.emptyListIterator();
        this.order = order;
    }

    @Override
    public void close() {
        open = false;
        // user may refuse consuming all returned records, so release the snapshot when closing the iterator if it is not released yet!
        releaseSnapshot();
    }

    @Override
    public boolean hasNext() {
        if (!open) {
            throw new IllegalStateException("The iterator is out of scope.");
        }
        // since data is stored in descending order in the segments, check whether there is any previous record, if the order is Ascending.
        final boolean hasStillLoad = order.equals(ResultOrder.ASCENDING) ? iterator.hasPrevious() : iterator.hasNext();
        return hasStillLoad || maybeFillIterator();
    }

    @Override
    public Object next() {
        if (hasNext()) {
            // since data is stored in descending order in the segments, retrieve previous record, if the order is Ascending.
            return order.equals(ResultOrder.ASCENDING) ? iterator.previous() : iterator.next();
        }
        throw new NoSuchElementException();
    }

    private boolean maybeFillIterator() {

        final List<VersionedRecord<byte[]>> queryResults = new ArrayList<>();
        while (segmentIterator.hasNext()) {
            final LogicalKeyValueSegment segment = segmentIterator.next();

            if (snapshot == null) { // create the snapshot (this will happen only one time).
                // any (random) segment, the latestValueStore or any of the older ones, can be the snapshotOwner, because in
                // fact all use the same physical RocksDB under-the-hood.
                this.snapshotOwner = segment;
                // take a RocksDB snapshot to return the segments content at the query time (in order to guarantee consistency)
                this.snapshot = snapshotOwner.getSnapshot();
            }

            final byte[] rawSegmentValue = segment.get(key, snapshot);
            if (rawSegmentValue != null) { // this segment contains record(s) with the specified key
                if (segment.id() == -1) { // this is the latestValueStore
                    final long recordTimestamp = RocksDBVersionedStore.LatestValueFormatter.getTimestamp(rawSegmentValue);
                    if (recordTimestamp <= toTime) {
                        // latest value satisfies timestamp bound
                        queryResults.add(new VersionedRecord<>(RocksDBVersionedStore.LatestValueFormatter.getValue(rawSegmentValue), recordTimestamp));
                    }
                } else {
                    // this segment contains records with the specified key and time range
                    final List<RocksDBVersionedStoreSegmentValueFormatter.SegmentValue.SegmentSearchResult> searchResults =
                            RocksDBVersionedStoreSegmentValueFormatter.deserialize(rawSegmentValue).findAll(fromTime, toTime);
                    for (final RocksDBVersionedStoreSegmentValueFormatter.SegmentValue.SegmentSearchResult searchResult : searchResults) {
                        queryResults.add(new VersionedRecord<>(searchResult.value(), searchResult.validFrom(), searchResult.validTo()));
                    }
                }
            }
            if (!queryResults.isEmpty()) {
                break;
            }
        }
        if (!queryResults.isEmpty()) {
            // since data is stored in descending order in the segments, create the list in reverse order, if the order is Ascending.
            this.iterator = order.equals(ResultOrder.ASCENDING) ? queryResults.listIterator(queryResults.size()) : queryResults.listIterator();
            return true;
        }
        // if all segments have been processed, release the snapshot
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
