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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ChangelogRecordDeserializationHelper;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.ResultOrder;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.streams.state.VersionedRecordIterator;
import org.apache.kafka.streams.state.internals.RocksDBVersionedStoreSegmentValueFormatter.SegmentValue;
import org.apache.kafka.streams.state.internals.RocksDBVersionedStoreSegmentValueFormatter.SegmentValue.SegmentSearchResult;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;

import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.kafka.streams.StreamsConfig.InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED;
import static org.apache.kafka.streams.state.internals.RocksDBStore.DB_FILE_DIR;

/**
 * A persistent, versioned key-value store based on RocksDB.
 * <p>
 * This store implementation consists of a "latest value store" and "segment stores." The latest
 * record version for each key is stored in the latest value store, while older record versions
 * are stored in the segment stores. Conceptually, each record version has two associated
 * timestamps:
 * <ul>
 *     <li>a {@code validFrom} timestamp. This timestamp is explicitly associated with the record
 *     as part of the {@link VersionedKeyValueStore#put(Object, Object, long)}} call to the store;
 *     i.e., this is the record's timestamp.</li>
 *     <li>a {@code validTo} timestamp. This is the timestamp of the next record (or deletion)
 *     associated with the same key, and is implicitly associated with the record. This timestamp
 *     can change as new records are inserted into the store.</li>
 * </ul>
 * The validity interval of a record is from validFrom (inclusive) to validTo (exclusive), and
 * can change as new record versions are inserted into the store (and validTo changes as a result).
 * <p>
 * Old record versions are stored in segment stores according to their validTo timestamps. This
 * allows for efficient expiry of old record versions, as entire segments can be dropped from the
 * store at a time, once the records contained in the segment are no longer relevant based on the
 * store's history retention (for an explanation of "history retention", see
 * {@link VersionedKeyValueStore}). Multiple record versions (for the same key) within a single
 * segment are stored together using the format specified in {@link RocksDBVersionedStoreSegmentValueFormatter}.
 */
public class RocksDBVersionedStore implements VersionedKeyValueStore<Bytes, byte[]> {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBVersionedStore.class);
    // a marker to indicate that no record version has yet been found as part of an ongoing
    // put() procedure. any value which is not a valid record timestamp will do.
    private static final long SENTINEL_TIMESTAMP = Long.MIN_VALUE;

    private final String name;
    private final long historyRetention;
    private final long gracePeriod;
    private final RocksDBMetricsRecorder metricsRecorder;

    private final LogicalKeyValueSegment latestValueStore; // implemented as a "reserved segment" of the segments store
    private final LogicalKeyValueSegments segmentStores;
    private final RocksDBVersionedStoreClient versionedStoreClient;
    private final RocksDBVersionedStoreRestoreWriteBuffer restoreWriteBuffer;

    private InternalProcessorContext<?, ?> internalProcessorContext;
    private Sensor expiredRecordSensor;
    private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;
    private boolean consistencyEnabled = false;
    private Position position;
    private OffsetCheckpoint positionCheckpoint;
    private volatile boolean open;

    RocksDBVersionedStore(final String name, final String metricsScope, final long historyRetention, final long segmentInterval) {
        this.name = name;
        this.historyRetention = historyRetention;
        // history retention doubles as grace period for now. could be nice to allow users to
        // configure the two separately in the future. if/when we do, we should enforce that
        // history retention >= grace period, for sound semantics.
        this.gracePeriod = historyRetention;
        this.metricsRecorder = new RocksDBMetricsRecorder(metricsScope, name);
        // pass store name as segments name so state dir subdirectory uses the store name
        this.segmentStores = new LogicalKeyValueSegments(name, DB_FILE_DIR, historyRetention, segmentInterval, metricsRecorder);
        this.latestValueStore = this.segmentStores.createReservedSegment(-1L, latestValueStoreName(name));
        this.versionedStoreClient = new RocksDBVersionedStoreClient();
        this.restoreWriteBuffer = new RocksDBVersionedStoreRestoreWriteBuffer(versionedStoreClient);
    }

    @Override
    public long put(final Bytes key, final byte[] value, final long timestamp) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();

        synchronized (position) {
            if (timestamp < observedStreamTime - gracePeriod) {
                expiredRecordSensor.record(1.0d, internalProcessorContext.currentSystemTimeMs());
                LOG.warn("Skipping record for expired put.");
                StoreQueryUtils.updatePosition(position, internalProcessorContext);
                return PUT_RETURN_CODE_NOT_PUT;
            }
            observedStreamTime = Math.max(observedStreamTime, timestamp);

            final long foundTs = doPut(
                versionedStoreClient,
                observedStreamTime,
                key,
                value,
                timestamp
            );

            StoreQueryUtils.updatePosition(position, internalProcessorContext);

            return foundTs;
        }
    }

    @Override
    public VersionedRecord<byte[]> delete(final Bytes key, final long timestamp) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();

        synchronized (position) {
            if (timestamp < observedStreamTime - gracePeriod) {
                expiredRecordSensor.record(1.0d, internalProcessorContext.currentSystemTimeMs());
                LOG.warn("Skipping record for expired delete.");
                return null;
            }

            final VersionedRecord<byte[]> existingRecord = get(key, timestamp);

            observedStreamTime = Math.max(observedStreamTime, timestamp);
            doPut(
                versionedStoreClient,
                observedStreamTime,
                key,
                null,
                timestamp
            );

            StoreQueryUtils.updatePosition(position, internalProcessorContext);

            return existingRecord;
        }
    }

    @Override
    public VersionedRecord<byte[]> get(final Bytes key) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();

        // latest value (if present) is guaranteed to be in the latest value store
        final byte[] rawLatestValueAndTimestamp = latestValueStore.get(key);
        if (rawLatestValueAndTimestamp != null) {
            return new VersionedRecord<>(
                LatestValueFormatter.value(rawLatestValueAndTimestamp),
                LatestValueFormatter.timestamp(rawLatestValueAndTimestamp)
            );
        } else {
            return null;
        }
    }

    @Override
    public VersionedRecord<byte[]> get(final Bytes key, final long asOfTimestamp) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();

        if (asOfTimestamp < observedStreamTime - historyRetention) {
            // history retention exceeded. we still check the latest value store in case the
            // latest record version satisfies the timestamp bound, in which case it should
            // still be returned (i.e., the latest record version per key never expires).
            final byte[] rawLatestValueAndTimestamp = latestValueStore.get(key);
            if (rawLatestValueAndTimestamp != null) {
                final long latestTimestamp = LatestValueFormatter.timestamp(rawLatestValueAndTimestamp);
                if (latestTimestamp <= asOfTimestamp) {
                    // latest value satisfies timestamp bound
                    return new VersionedRecord<>(
                        LatestValueFormatter.value(rawLatestValueAndTimestamp),
                        latestTimestamp
                    );
                }
            }

            // history retention has elapsed and the latest record version (if present) does
            // not satisfy the timestamp bound. return null for predictability, even if data
            // is still present in segments.
            LOG.warn("Returning null for expired get.");
            return null;
        }

        // first check the latest value store
        final byte[] rawLatestValueAndTimestamp = latestValueStore.get(key);
        if (rawLatestValueAndTimestamp != null) {
            final long latestTimestamp = LatestValueFormatter.timestamp(rawLatestValueAndTimestamp);
            if (latestTimestamp <= asOfTimestamp) {
                return new VersionedRecord<>(LatestValueFormatter.value(rawLatestValueAndTimestamp), latestTimestamp);
            }
        }

        // check segment stores
        final List<LogicalKeyValueSegment> segments = segmentStores.segments(asOfTimestamp, Long.MAX_VALUE, false);
        for (final LogicalKeyValueSegment segment : segments) {
            final byte[] rawSegmentValue = segment.get(key);
            if (rawSegmentValue != null) {
                final long nextTs = RocksDBVersionedStoreSegmentValueFormatter.nextTimestamp(rawSegmentValue);
                if (nextTs <= asOfTimestamp) {
                    // this segment contains no data for the queried timestamp, so earlier segments
                    // cannot either
                    return null;
                }

                if (RocksDBVersionedStoreSegmentValueFormatter.minTimestamp(rawSegmentValue) > asOfTimestamp) {
                    // the segment only contains data for after the queried timestamp. skip and
                    // continue the search to earlier segments. as an optimization, this code
                    // could be updated to skip forward to the segment containing the minTimestamp
                    // in the if-condition above.
                    continue;
                }

                // the desired result is contained in this segment
                final SegmentSearchResult searchResult =
                        RocksDBVersionedStoreSegmentValueFormatter
                                .deserialize(rawSegmentValue)
                                .find(asOfTimestamp, true);
                if (searchResult.value() != null) {
                    return new VersionedRecord<>(searchResult.value(), searchResult.validFrom(), searchResult.validTo());
                } else {
                    return null;
                }
            }
        }

        // checked all segments and no results found
        return null;
    }

    @SuppressWarnings("unchecked")
    VersionedRecordIterator<byte[]> get(final Bytes key, final long fromTimestamp, final long toTimestamp, final ResultOrder order) {
        validateStoreOpen();

        if (toTimestamp < observedStreamTime - historyRetention) {
            // history retention exceeded. we still check the latest value store in case the
            // latest record version satisfies the timestamp bound, in which case it should
            // still be returned (i.e., the latest record version per key never expires).
            return new LogicalSegmentIterator(Collections.singletonList(latestValueStore).listIterator(), key, fromTimestamp, toTimestamp, order);
        } else {
            final List<LogicalKeyValueSegment> segments = new ArrayList<>();
            // add segment stores
            // consider the search lower bound as -INF (LONG.MIN_VALUE) to find the record that has been inserted before the {@code fromTimestamp}
            // but is still valid in query specified time interval.
            if (order.equals(ResultOrder.ASCENDING)) {
                segments.addAll(segmentStores.segments(Long.MIN_VALUE, toTimestamp, true));
                segments.add(latestValueStore);
            } else {
                segments.add(latestValueStore);
                segments.addAll(segmentStores.segments(Long.MIN_VALUE, toTimestamp, false));
            }
            return new LogicalSegmentIterator(segments.listIterator(), key, fromTimestamp, toTimestamp, order);
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void flush() {
        segmentStores.flush();
        // flushing segments store includes flushing latest value store, since they share the
        // same physical RocksDB instance
    }

    @Override
    public void close() {
        open = false;

        segmentStores.close();
        // closing segments store includes closing latest value store, since they share the
        // same physical RocksDB instance
    }

    @Override
    public <R> QueryResult<R> query(
            final Query<R> query,
            final PositionBound positionBound,
            final QueryConfig config) {
        return StoreQueryUtils.handleBasicQueries(
            query,
            positionBound,
            config,
            this,
            position,
            internalProcessorContext
        );
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
    public Position getPosition() {
        return position;
    }

    @Override
    public void init(final StateStoreContext stateStoreContext, final StateStore root) {
        this.internalProcessorContext = ProcessorContextUtils.asInternalProcessorContext(stateStoreContext);

        final StreamsMetricsImpl metrics = ProcessorContextUtils.metricsImpl(stateStoreContext);
        final String threadId = Thread.currentThread().getName();
        final String taskName = stateStoreContext.taskId().toString();

        expiredRecordSensor = TaskMetrics.droppedRecordsSensor(
                threadId,
                taskName,
                metrics
        );

        metricsRecorder.init(ProcessorContextUtils.metricsImpl(stateStoreContext), stateStoreContext.taskId());

        final File positionCheckpointFile = new File(stateStoreContext.stateDir(), name() + ".position");
        positionCheckpoint = new OffsetCheckpoint(positionCheckpointFile);
        position = StoreQueryUtils.readPositionFromCheckpoint(positionCheckpoint);
        segmentStores.setPosition(position);
        segmentStores.openExisting(internalProcessorContext, observedStreamTime);

        // register and possibly restore the state from the logs
        stateStoreContext.register(
                root,
                (RecordBatchingStateRestoreCallback) RocksDBVersionedStore.this::restoreBatch,
                () -> StoreQueryUtils.checkpointPosition(positionCheckpoint, position)
        );

        open = true;

        consistencyEnabled = StreamsConfig.InternalConfig.getBoolean(
                stateStoreContext.appConfigs(),
                IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED,
                false
        );
    }

    // VisibleForTesting
    void restoreBatch(final Collection<ConsumerRecord<byte[], byte[]>> records) {

        // compute the observed stream time at the end of the restore batch, in order to speed up
        // restore by not bothering to read from/write to segments which will have expired by the
        // time the restoration process is complete.
        long endOfBatchStreamTime = observedStreamTime;
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            endOfBatchStreamTime = Math.max(endOfBatchStreamTime, record.timestamp());
        }

        final VersionedStoreClient<?> restoreClient = restoreWriteBuffer.getClient();

        // note: there is increased risk for hitting an out-of-memory during this restore loop,
        // compared to for non-versioned key-value stores, because this versioned store
        // implementation stores multiple records (for the same key) together in a single RocksDB
        // "segment" entry -- restoring a single changelog entry could require loading multiple
        // records into memory. how high this memory amplification will be is very much dependent
        // on the specific workload and the value of the "segment interval" parameter.
        synchronized (position) {
            for (final ConsumerRecord<byte[], byte[]> record : records) {
                if (record.timestamp() < observedStreamTime - gracePeriod) {
                    // record is older than grace period and was therefore never written to the store
                    continue;
                }
                // advance observed stream time as usual, for use in deciding whether records have
                // exceeded the store's grace period and should be dropped.
                observedStreamTime = Math.max(observedStreamTime, record.timestamp());

                ChangelogRecordDeserializationHelper.applyChecksAndUpdatePosition(
                    record,
                    consistencyEnabled,
                    position
                );

                // put records to write buffer
                doPut(
                    restoreClient,
                    endOfBatchStreamTime,
                    new Bytes(record.key()),
                    record.value(),
                    record.timestamp()
                );
            }

            try {
                restoreWriteBuffer.flush();
            } catch (final RocksDBException e) {
                throw new ProcessorStateException("Error restoring batch to store " + name, e);
            }
        }

    }

    private void validateStoreOpen() {
        if (!open) {
            throw new InvalidStateStoreException("Store " + name + " is currently closed");
        }
    }

    /**
     * Generic interface for segment stores. See {@link VersionedStoreClient} for use.
     */
    interface VersionedStoreSegment {

        /**
         * @return segment id
         */
        long id();

        void put(Bytes key, byte[] value);

        byte[] get(Bytes key);
    }

    /**
     * Extracts all operations required for writing to the versioned store (via
     * {@link #put(Bytes, byte[], long)}) into a generic client interface, so that the same
     * {@code put(...)} logic can be shared during regular store operation and during restore.
     *
     * @param <T> the segment type used by this client
     */
    interface VersionedStoreClient<T extends VersionedStoreSegment> {

        /**
         * @return the contents of the latest value store, for the given key
         */
        byte[] latestValue(Bytes key);

        /**
         * Puts the provided key and value into the latest value store.
         */
        void putLatestValue(Bytes key, byte[] value);

        /**
         * Deletes the existing value (if any) from the latest value store, for the given key.
         */
        void deleteLatestValue(Bytes key);

        /**
         * @return the segment with the provided id, or {@code null} if the segment is expired
         */
        T getOrCreateSegmentIfLive(long segmentId, StateStoreContext context, long streamTime);

        /**
         * @return all segments in the store which contain timestamps at least the provided
         * timestamp bound, in reverse order by segment id (and time), i.e., such that
         * the most recent segment is first
         */
        List<T> reversedSegments(long timestampFrom);

        /**
         * @return the segment id associated with the provided timestamp
         */
        long segmentIdForTimestamp(long timestamp);
    }

    /**
     * Client for writing into (and reading from) this persistent {@link RocksDBVersionedStore}.
     */
    class RocksDBVersionedStoreClient implements VersionedStoreClient<LogicalKeyValueSegment> {

        @Override
        public byte[] latestValue(final Bytes key) {
            return latestValueStore.get(key);
        }

        @Override
        public void putLatestValue(final Bytes key, final byte[] value) {
            latestValueStore.put(key, value);
        }

        @Override
        public void deleteLatestValue(final Bytes key) {
            latestValueStore.delete(key);
        }

        @Override
        public LogicalKeyValueSegment getOrCreateSegmentIfLive(final long segmentId, final StateStoreContext context, final long streamTime) {
            return segmentStores.getOrCreateSegmentIfLive(segmentId, context, streamTime);
        }

        @Override
        public List<LogicalKeyValueSegment> reversedSegments(final long timestampFrom) {
            return segmentStores.segments(timestampFrom, Long.MAX_VALUE, false);
        }

        @Override
        public long segmentIdForTimestamp(final long timestamp) {
            return segmentStores.segmentId(timestamp);
        }

        /**
         * Adds the provided record into the provided batch, in preparation for writing into the
         * latest value store.
         * <p>
         * Together with {@link #writeLatestValues(WriteBatch)}, this method supports batch writes
         * into the latest value store.
         *
         * @throws RocksDBException if a failure occurs adding the record to the {@link WriteBatch}
         */
        public void addToLatestValueBatch(final KeyValue<byte[], byte[]> record, final WriteBatch batch) throws RocksDBException {
            latestValueStore.addToBatch(record, batch);
        }

        /**
         * Writes the provided batch of records into the latest value store.
         * <p>
         * Together with {@link #addToLatestValueBatch(KeyValue, WriteBatch)}, this method supports
         * batch writes into the latest value store.
         *
         * @throws RocksDBException if a failure occurs while writing the {@link WriteBatch} to the store
         */
        public void writeLatestValues(final WriteBatch batch) throws RocksDBException {
            latestValueStore.write(batch);
        }
    }

    /**
     * Helper method shared between put and restore.
     * <p>
     * This method does not check whether the record being put is expired based on grace period
     * or not; that is the caller's responsibility. This method does, however, check whether the
     * record is expired based on history retention, by using the current
     * {@code observedStreamTime}, and returns without inserting into the store if so. It can be
     * possible that a record is not expired based on grace period but is expired based on
     * history retention, even though history retention is always at least the grace period,
     * during restore because restore advances {@code observedStreamTime} to the largest timestamp
     * in the entire restore batch at the beginning of restore, in order to optimize for not
     * putting records into the store which will have expired by the end of the restore.
     */
    private <T extends VersionedStoreSegment> long doPut(
            final VersionedStoreClient<T> versionedStoreClient,
            final long observedStreamTime,
            final Bytes key,
            final byte[] value,
            final long timestamp
    ) {
        segmentStores.cleanupExpiredSegments(observedStreamTime);

        // track the smallest timestamp seen so far that is larger than insertion timestamp.
        // this timestamp determines, based on all segments searched so far, which segment the
        // new record should be inserted into.
        long foundTs;

        // check latest value store
        PutStatus status = maybePutToLatestValueStore(
                versionedStoreClient,
                observedStreamTime,
                key,
                value,
                timestamp
        );
        if (status.isComplete) {
            return status.foundTs == SENTINEL_TIMESTAMP ? PUT_RETURN_CODE_VALID_TO_UNDEFINED : status.foundTs;
        } else {
            foundTs = status.foundTs;
        }

        // continue search in segments
        status = maybePutToSegments(
                versionedStoreClient,
                observedStreamTime,
                key,
                value,
                timestamp,
                foundTs
        );
        if (status.isComplete) {
            return status.foundTs == SENTINEL_TIMESTAMP ? PUT_RETURN_CODE_VALID_TO_UNDEFINED : status.foundTs;
        } else {
            foundTs = status.foundTs;
        }

        // the record did not unconditionally belong in any specific store (latest value store
        // or segments store). insert based on foundTs here instead.
        foundTs = finishPut(
                versionedStoreClient,
                observedStreamTime,
                key,
                value,
                timestamp,
                foundTs
        );
        return foundTs == SENTINEL_TIMESTAMP ? PUT_RETURN_CODE_VALID_TO_UNDEFINED : foundTs;
    }

    /**
     * Represents the status of an ongoing put() operation.
     */
    private static class PutStatus {

        /**
         * Whether the put() call has been completed.
         */
        final boolean isComplete;

        /**
         * The smallest timestamp seen so far, as part of this current put() operation, that is
         * larger than insertion timestamp. This timestamp determines, based on all segments
         * searched so far, which segment the new record should be inserted into.
         */
        final long foundTs;

        PutStatus(final boolean isComplete, final long foundTs) {
            this.isComplete = isComplete;
            this.foundTs = foundTs;
        }
    }

    private <T extends VersionedStoreSegment> PutStatus maybePutToLatestValueStore(
            final VersionedStoreClient<T> versionedStoreClient,
            final long observedStreamTime,
            final Bytes key,
            final byte[] value,
            final long timestamp
    ) {
        // initialize with a starting "sentinel timestamp" which represents
        // that the segment should be inserted into the latest value store.
        long foundTs = SENTINEL_TIMESTAMP;

        final byte[] rawLatestValueAndTimestamp = versionedStoreClient.latestValue(key);
        if (rawLatestValueAndTimestamp != null) {
            final long latestValueStoreTimestamp = LatestValueFormatter.timestamp(rawLatestValueAndTimestamp);
            if (timestamp >= latestValueStoreTimestamp) {
                // new record belongs in the latest value store
                if (timestamp > latestValueStoreTimestamp) {
                    // move existing latest value into segment.
                    // it's important that this step happens before the update to the latest value
                    // store. if there is a partial failure (this step succeeds but the update to
                    // the latest value store fails), updating the segment first means there will
                    // not be data loss. (rather, there will be duplicated data which is fine as
                    // it can/will be reconciled later.)
                    final long segmentId = versionedStoreClient.segmentIdForTimestamp(timestamp);
                    final T segment = versionedStoreClient.getOrCreateSegmentIfLive(segmentId, internalProcessorContext, observedStreamTime);
                    // `segment == null` implies that all data in the segment is older than the
                    // history retention of this store, and therefore does not need to tracked.
                    // as a result, we only need to move the existing record from the latest value
                    // store into a segment if `segment != null`. (also, we do not call the
                    // `expiredRecordSensor` in this case because the expired record sensor is only
                    // called when the record being put is itself expired, which is not the case
                    // here. only the existing record already present in the latest value store
                    // is expired.) so, there is nothing to do for this step if `segment == null`,
                    // but we do still update the latest value store with the new record below.
                    if (segment != null) {
                        final byte[] rawValueToMove = LatestValueFormatter.value(rawLatestValueAndTimestamp);
                        final byte[] rawSegmentValue = segment.get(key);
                        if (rawSegmentValue == null) {
                            segment.put(
                                    key,
                                    RocksDBVersionedStoreSegmentValueFormatter
                                            .newSegmentValueWithRecord(rawValueToMove, latestValueStoreTimestamp, timestamp)
                                            .serialize()
                            );
                        } else {
                            final SegmentValue segmentValue = RocksDBVersionedStoreSegmentValueFormatter.deserialize(rawSegmentValue);
                            segmentValue.insertAsLatest(latestValueStoreTimestamp, timestamp, rawValueToMove);
                            segment.put(key, segmentValue.serialize());
                        }
                    }
                }

                // update latest value store
                if (value != null) {
                    versionedStoreClient.putLatestValue(key, LatestValueFormatter.from(value, timestamp));
                } else {
                    versionedStoreClient.deleteLatestValue(key);
                }
                return new PutStatus(true, foundTs);
            } else {
                foundTs = latestValueStoreTimestamp;
            }
        }
        return new PutStatus(false, foundTs);
    }

    private <T extends VersionedStoreSegment> PutStatus maybePutToSegments(
            final VersionedStoreClient<T> versionedStoreClient,
            final long observedStreamTime,
            final Bytes key,
            final byte[] value,
            final long timestamp,
            final long prevFoundTs
    ) {
        // initialize with current foundTs value
        long foundTs = prevFoundTs;

        final List<T> segments = versionedStoreClient.reversedSegments(timestamp);
        for (final T segment : segments) {
            final byte[] rawSegmentValue = segment.get(key);
            if (rawSegmentValue != null) {
                final long foundNextTs = RocksDBVersionedStoreSegmentValueFormatter.nextTimestamp(rawSegmentValue);
                if (foundNextTs <= timestamp) {
                    // this segment (and all earlier segments) does not contain records affected by
                    // this put. insert into the segment specified by foundTs (i.e., the next
                    // phase of the put() procedure) and conclude the procedure.
                    return new PutStatus(false, foundTs);
                }

                final long foundMinTs = RocksDBVersionedStoreSegmentValueFormatter.minTimestamp(rawSegmentValue);
                if (foundMinTs <= timestamp) {
                    // the record being inserted belongs in this segment.
                    // insert and conclude the procedure.
                    foundTs = putToSegment(
                            versionedStoreClient,
                            observedStreamTime,
                            segment,
                            rawSegmentValue,
                            key,
                            value,
                            timestamp
                    );
                    return new PutStatus(true, foundTs);
                }

                if (foundMinTs < observedStreamTime - historyRetention) {
                    // the record being inserted does not affect version history. discard and return.
                    // this can happen during restore because individual put calls are executed after
                    // observedStreamTime is fast-forwarded for the entire batch of records being
                    // restored at once.
                    return new PutStatus(true, PUT_RETURN_CODE_NOT_PUT);
                }

                // it's possible the record belongs in this segment, but also possible it belongs
                // in an earlier segment. mark as tentative and continue. as an optimization, this
                // code could be updated to skip forward to the segment containing foundMinTs.
                foundTs = foundMinTs;
            }
        }
        return new PutStatus(false, foundTs);
    }

    /**
     * @return updated {@code foundTs} value, i.e., the validTo timestamp of the record being put
     */
    private <T extends VersionedStoreSegment> long putToSegment(
            final VersionedStoreClient<T> versionedStoreClient,
            final long observedStreamTime,
            final T segment,
            final byte[] rawSegmentValue,
            final Bytes key,
            final byte[] value,
            final long timestamp
    ) {
        final long segmentIdForTimestamp = versionedStoreClient.segmentIdForTimestamp(timestamp);
        // it's possible that putting the current record into a segment will require moving an
        // existing record from this segment into an older segment. this is because records belong
        // in segments based on their validTo timestamps, and putting a new record into a segment
        // updates the validTo timestamp for the previous record. the new validTo timestamp
        // of the previous record will be the (validFrom) timestamp of the record being put.
        // if this (validFrom) timestamp belongs in a different segment, then it may be required
        // to move an existing record from this segment into an older one.
        final boolean writeToOlderSegmentMaybeNeeded = segmentIdForTimestamp != segment.id();

        final SegmentValue segmentValue = RocksDBVersionedStoreSegmentValueFormatter.deserialize(rawSegmentValue);
        // we pass `writeToOlderSegmentMaybeNeeded` as `includeValue` in the call to find() below
        // because moving a record from this segment into an older one will require knowing its
        // value. unless such a move is required, we do not need the existing record value.
        final SegmentSearchResult searchResult = segmentValue.find(timestamp, writeToOlderSegmentMaybeNeeded);

        if (searchResult.validFrom() == timestamp) {
            // this put() replaces an existing entry, rather than adding a new one. as a result,
            // the validTo timestamp of the previous record does not need to be updated either,
            // and we do not need to move an existing record to an older segment (i.e., we can
            // ignore `writeToOlderSegmentMaybeNeeded`).
            segmentValue.updateRecord(timestamp, value, searchResult.index());
            segment.put(key, segmentValue.serialize());
            return searchResult.validTo();
        }

        if (writeToOlderSegmentMaybeNeeded) {
            // existing record needs to be moved to an older segment.
            // it's important that this step happens before updating the current
            // segment. if there is a partial failure (this step succeeds but the
            // update to the current segment fails), updating the older segment
            // first means there will not be data loss. (rather, there will be
            // duplicated data which is fine as it can/will be reconciled later.)
            final T olderSegment = versionedStoreClient
                    .getOrCreateSegmentIfLive(segmentIdForTimestamp, internalProcessorContext, observedStreamTime);
            // `olderSegment == null` implies that all data in the older segment is older than the
            // history retention of this store, and therefore does not need to tracked.
            // as a result, we only need to move the existing record from the newer segment
            // into the older segment if `olderSegment != null`. (also, we do not call the
            // `expiredRecordSensor` in this case because the expired record sensor is only
            // called when the record being put is itself expired, which is not the case
            // here. only the existing record already present in the newer segment is
            // expired.) so, there is nothing to do for this step if `olderSegment == null`,
            // but we do still update the newer segment with the new record below.
            if (olderSegment != null) {
                final byte[] rawOlderSegmentValue = olderSegment.get(key);
                if (rawOlderSegmentValue == null) {
                    olderSegment.put(
                            key,
                            RocksDBVersionedStoreSegmentValueFormatter.newSegmentValueWithRecord(
                                    searchResult.value(), searchResult.validFrom(), timestamp
                            ).serialize()
                    );
                } else {
                    final SegmentValue olderSegmentValue
                            = RocksDBVersionedStoreSegmentValueFormatter.deserialize(rawOlderSegmentValue);
                    olderSegmentValue.insertAsLatest(searchResult.validFrom(), timestamp, searchResult.value());
                    olderSegment.put(key, olderSegmentValue.serialize());
                }
            }

            // update in newer segment (replace the record that was just moved with the new one)
            segmentValue.updateRecord(timestamp, value, searchResult.index());
            segment.put(key, segmentValue.serialize());
            return searchResult.validTo();
        }

        // plain insert into segment. no additional handling required.
        segmentValue.insert(timestamp, value, searchResult.index());
        segment.put(key, segmentValue.serialize());
        return searchResult.validTo();
    }

    /**
     * @return updated {@code foundTs} value, i.e., the validTo timestamp of the record being put
     */
    private <T extends VersionedStoreSegment> long finishPut(
            final VersionedStoreClient<T> versionedStoreClient,
            final long observedStreamTime,
            final Bytes key,
            final byte[] value,
            final long timestamp,
            final long foundTs
    ) {
        if (foundTs == SENTINEL_TIMESTAMP) {
            // insert into latest value store
            if (value != null) {
                versionedStoreClient.putLatestValue(key, LatestValueFormatter.from(value, timestamp));
            } else {
                // tombstones are not inserted into the latest value store. insert into segment instead.
                // the specific segment to insert to is determined based on the tombstone's timestamp
                final T segment = versionedStoreClient.getOrCreateSegmentIfLive(
                        versionedStoreClient.segmentIdForTimestamp(timestamp), internalProcessorContext, observedStreamTime);
                if (segment == null) {
                    // the record being inserted does not affect version history. discard and return.
                    // this can happen during restore because individual put calls are executed after
                    // observedStreamTime is fast-forwarded for the entire batch of records being
                    // restored at once.
                    return PUT_RETURN_CODE_NOT_PUT;
                }

                final byte[] rawSegmentValue = segment.get(key);
                if (rawSegmentValue == null) {
                    // in this special case where the latest record version (for a particular key)
                    // is a tombstone, and the segment that the tombstone belongs in contains no
                    // record versions for this key, create a new "degenerate" segment with the
                    // tombstone's timestamp as both validFrom and validTo timestamps for the segment
                    segment.put(
                            key,
                            RocksDBVersionedStoreSegmentValueFormatter
                                    .newSegmentValueWithRecord(null, timestamp, timestamp)
                                    .serialize()
                    );
                } else {
                    // insert as latest, since foundTs = sentinel means nothing later exists
                    if (RocksDBVersionedStoreSegmentValueFormatter.nextTimestamp(rawSegmentValue) == timestamp) {
                        // next timestamp equal to put() timestamp already represents a tombstone,
                        // so no additional insertion is needed in this case
                        return foundTs;
                    }
                    final SegmentValue segmentValue
                            = RocksDBVersionedStoreSegmentValueFormatter.deserialize(rawSegmentValue);
                    segmentValue.insertAsLatest(
                            RocksDBVersionedStoreSegmentValueFormatter.nextTimestamp(rawSegmentValue),
                            timestamp,
                            null
                    );
                    segment.put(key, segmentValue.serialize());
                }
            }
        } else {
            // insert into segment corresponding to foundTs, as foundTs represents the validTo
            // timestamp of the current put.
            // the new record is either the earliest or the latest in this segment, depending on the
            // circumstances of the fall-through. (it cannot belong in the middle because otherwise
            // putSegments() above would have identified a segment for which
            // minTimestamp <= timestamp < nextTimestamp, and putSegments would've completed the
            // put procedure without reaching this fall-through case.)
            final T segment = versionedStoreClient.getOrCreateSegmentIfLive(
                    versionedStoreClient.segmentIdForTimestamp(foundTs), internalProcessorContext, observedStreamTime);
            if (segment == null) {
                // the record being inserted does not affect version history. discard and return.
                // this can happen during restore because individual put calls are executed after
                // observedStreamTime is fast-forwarded for the entire batch of records being
                // restored at once.
                return PUT_RETURN_CODE_NOT_PUT;
            }

            final byte[] rawSegmentValue = segment.get(key);
            if (rawSegmentValue == null) {
                segment.put(
                        key,
                        RocksDBVersionedStoreSegmentValueFormatter
                                .newSegmentValueWithRecord(value, timestamp, foundTs)
                                .serialize()
                );
            } else {
                final long foundNextTs = RocksDBVersionedStoreSegmentValueFormatter.nextTimestamp(rawSegmentValue);
                if (foundNextTs <= timestamp) {
                    // insert as latest. this case is possible if the found segment is "degenerate"
                    // (cf RocksDBVersionedStoreSegmentValueFormatter.java for details) as older
                    // degenerate segments may result in "gaps" between one record version's validTo
                    // timestamp and the next record version's validFrom timestamp
                    final SegmentValue segmentValue = RocksDBVersionedStoreSegmentValueFormatter.deserialize(rawSegmentValue);
                    segmentValue.insertAsLatest(timestamp, foundTs, value);
                    segment.put(key, segmentValue.serialize());
                } else {
                    // insert as earliest
                    final SegmentValue segmentValue = RocksDBVersionedStoreSegmentValueFormatter.deserialize(rawSegmentValue);
                    segmentValue.insertAsEarliest(timestamp, value);
                    segment.put(key, segmentValue.serialize());
                }
            }
        }
        return foundTs;
    }

    /**
     * Bytes layout for the value portion of rows stored in the latest value store. The layout is
     * a fixed-size timestamp concatenated with the actual record value.
     */
    static final class LatestValueFormatter {

        private static final int TIMESTAMP_SIZE = 8;

        /**
         * @return the timestamp, from the latest value store value bytes (representing value
         * and timestamp)
         */
        static long timestamp(final byte[] rawLatestValueAndTimestamp) {
            return ByteBuffer.wrap(rawLatestValueAndTimestamp).getLong();
        }

        /**
         * @return the actual record value, from the latest value store value bytes (representing
         * value and timestamp)
         */
        static byte[] value(final byte[] rawLatestValueAndTimestamp) {
            final byte[] rawValue = new byte[rawLatestValueAndTimestamp.length - TIMESTAMP_SIZE];
            System.arraycopy(rawLatestValueAndTimestamp, TIMESTAMP_SIZE, rawValue, 0, rawValue.length);
            return rawValue;
        }

        /**
         * @return the formatted bytes containing the provided {@code rawValue} and
         * {@code timestamp}, ready to be stored into the latest value store
         */
        static byte[] from(final byte[] rawValue, final long timestamp) {
            if (rawValue == null) {
                throw new IllegalStateException("Cannot store tombstone in latest value");
            }

            return ByteBuffer.allocate(TIMESTAMP_SIZE + rawValue.length)
                    .putLong(timestamp)
                    .put(rawValue)
                    .array();
        }
    }

    private static String latestValueStoreName(final String storeName) {
        return storeName + ".latestValues";
    }
}