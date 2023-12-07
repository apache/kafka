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

import java.util.ListIterator;
import java.util.NoSuchElementException;

public class LogicalSegmentIterator implements VersionedRecordIterator {
    private final ListIterator<LogicalKeyValueSegment> segmentIterator;
    private final Bytes key;
    private final Long fromTime;
    private final Long toTime;
    private final ResultOrder order;
    // stores the raw value of the latestValueStore when latestValueStore is the current segment
    private byte[] currentRawSegmentValue;
    // stores the deserialized value of the current segment (when current segment is one of the old segments)
    private RocksDBVersionedStoreSegmentValueFormatter.SegmentValue currentDeserializedSegmentValue;
    // current segment minTimestamp (when current segment is not the latestValueStore)
    private long minTimestamp;
    // current segment nextTimestamp (when current segment is not the latestValueStore)
    private long nextTimestamp;
    private VersionedRecord next;
    // current deserialization index
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
        this.order = order;
        this.next = null;
        prepareToFetchNextSegment();
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
        if (this.next != null) {
            return true;
        }

        while ((currentDeserializedSegmentValue != null || currentRawSegmentValue != null || segmentIterator.hasNext()) && this.next == null) {
            boolean hasSegmentValue = currentDeserializedSegmentValue != null || currentRawSegmentValue != null;
            if (!hasSegmentValue) {
                hasSegmentValue = maybeFillCurrentSegmentValue();
            }
            if (hasSegmentValue) {
                this.next  = (VersionedRecord) getNextRecord();
                if (this.next == null) {
                    prepareToFetchNextSegment();
                }
            }
        }
        return this.next != null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object next() {
        if (this.next == null) {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
        }
        final VersionedRecord clonedNext = this.next.validTo().isPresent() ? new VersionedRecord(this.next.value(), this.next.timestamp(), Long.parseLong(this.next.validTo().get().toString()))
                                                                           : new VersionedRecord(this.next.value(), this.next.timestamp());
        this.next = null;
        return clonedNext;
    }

    private void prepareToFetchNextSegment() {
        this.currentRawSegmentValue = null;
        this.currentDeserializedSegmentValue = null;
        this.minTimestamp = this.nextTimestamp = -1;
    }

    /**
     * Fills currentRawSegmentValue (for the latestValueStore) or currentDeserializedSegmentValue (for older segments) only if
     * segmentIterator.hasNext() and the segment has records with the query specified key
     */
    private boolean maybeFillCurrentSegmentValue() {
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
                    this.currentRawSegmentValue = rawSegmentValue;
                } else {
                    this.currentDeserializedSegmentValue = RocksDBVersionedStoreSegmentValueFormatter.deserialize(rawSegmentValue);
                    this.minTimestamp = RocksDBVersionedStoreSegmentValueFormatter.getMinTimestamp(rawSegmentValue);
                    this.nextTimestamp = RocksDBVersionedStoreSegmentValueFormatter.getNextTimestamp(rawSegmentValue);
                }
                return true;
            }
        }
        // if all segments have been processed, release the snapshot
        releaseSnapshot();
        return false;
    }

    private Object getNextRecord() {
        VersionedRecord nextRecord = null;
        if (currentRawSegmentValue != null) { // this is the latestValueStore
            final long recordTimestamp = RocksDBVersionedStore.LatestValueFormatter.getTimestamp(currentRawSegmentValue);
            if (recordTimestamp <= toTime) {
                final byte[] value = RocksDBVersionedStore.LatestValueFormatter.getValue(currentRawSegmentValue);
                // latest value satisfies timestamp bound
                nextRecord = new VersionedRecord<>(value, recordTimestamp);
            }
        } else {
            final RocksDBVersionedStoreSegmentValueFormatter.SegmentValue.SegmentSearchResult currentRecord =
                    currentDeserializedSegmentValue.find(fromTime, toTime, order);
            if (currentRecord != null) {
                nextRecord = new VersionedRecord<>(currentRecord.value(), currentRecord.validFrom(), currentRecord.validTo());
            }
        }
        // no relevant record can be found in the segment
        if (currentRawSegmentValue != null || nextRecord == null || !canSegmentHaveMoreRelevantRecords(nextRecord.timestamp(), Long.parseLong(nextRecord.validTo().get().toString()))) {
            prepareToFetchNextSegment();
        }
        return nextRecord;
    }

    private boolean canSegmentHaveMoreRelevantRecords(final long currentValidFrom, final long currentValidTo) {
        final boolean isCurrentOutsideTimeRange = (order.equals(ResultOrder.ASCENDING) && (currentValidTo > toTime || nextTimestamp == currentValidTo))
                                               || (!order.equals(ResultOrder.ASCENDING) && (currentValidFrom < fromTime || minTimestamp == currentValidFrom));
        return !isCurrentOutsideTimeRange;
    }

    private void releaseSnapshot() {
        if (snapshot != null) {
            snapshotOwner.releaseSnapshot(snapshot);
            snapshot = null;
        }
    }
}
