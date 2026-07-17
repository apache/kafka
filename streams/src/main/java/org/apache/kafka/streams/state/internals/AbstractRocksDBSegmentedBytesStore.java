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
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
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
import org.apache.kafka.streams.state.KeyValueIterator;

import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.StreamsConfig.InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED;
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;

public class AbstractRocksDBSegmentedBytesStore<S extends Segment> implements SegmentedBytesStore, WithRetentionPeriod {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractRocksDBSegmentedBytesStore.class);

    private final String name;
    private final AbstractSegments<S> segments;
    private final long retentionPeriod;
    private final KeySchema keySchema;

    private InternalProcessorContext<?, ?> internalProcessorContext;
    private Sensor expiredRecordSensor;
    private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;
    private boolean consistencyEnabled = false;
    private Position position;
    // Position of writes staged in the current transaction; merged into the committed position on commit().
    private Position pendingPosition = Position.emptyPosition();
    private boolean transactional;
    private volatile boolean open;

    AbstractRocksDBSegmentedBytesStore(final String name,
                                       final long retentionPeriod,
                                       final KeySchema keySchema,
                                       final AbstractSegments<S> segments) {
        this.name = name;
        this.retentionPeriod = retentionPeriod;
        this.keySchema = keySchema;
        this.segments = segments;
    }

    @Override
    public long retentionPeriod() {
        return retentionPeriod;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetch(final Bytes key,
                                                 final long from,
                                                 final long to) {
        return fetch(key, from, to, true, null);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> backwardFetch(final Bytes key,
                                                         final long from,
                                                         final long to) {
        return fetch(key, from, to, false, null);
    }

    private KeyValueIterator<Bytes, byte[]> fetch(final Bytes key,
                                                  final long from,
                                                  final long to,
                                                  final boolean forward,
                                                  final IsolationLevel isolationLevel) {
        final long actualFrom = getActualFrom(from);

        if (keySchema instanceof WindowKeySchema && to < actualFrom) {
            LOG.debug("Returning no records for key {} as to ({}) < actualFrom ({}) ", key.toString(), to, actualFrom);
            return KeyValueIterators.emptyIterator();
        }

        final List<S> searchSpace = keySchema.segmentsToSearch(segments, actualFrom, to, forward);

        final Bytes binaryFrom = keySchema.lowerRangeFixedSize(key, actualFrom);
        final Bytes binaryTo = keySchema.upperRangeFixedSize(key, to);

        return new SegmentIterator<>(
                searchSpace.iterator(),
                keySchema.hasNextCondition(key, key, actualFrom, to, forward),
                binaryFrom,
                binaryTo,
                forward,
                isolationLevel);
    }

    private long getActualFrom(final long from) {
        return Math.max(from, observedStreamTime - retentionPeriod + 1);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetch(final Bytes keyFrom,
                                                 final Bytes keyTo,
                                                 final long from,
                                                 final long to) {
        return fetch(keyFrom, keyTo, from, to, true, null);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> backwardFetch(final Bytes keyFrom,
                                                         final Bytes keyTo,
                                                         final long from,
                                                         final long to) {
        return fetch(keyFrom, keyTo, from, to, false, null);
    }

    private KeyValueIterator<Bytes, byte[]> fetch(final Bytes keyFrom,
                                                  final Bytes keyTo,
                                                  final long from,
                                                  final long to,
                                                  final boolean forward,
                                                  final IsolationLevel isolationLevel) {
        if (keyFrom != null && keyTo != null && keyFrom.compareTo(keyTo) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. " +
                    "This may be due to range arguments set in the wrong order, " +
                    "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                    "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        final long actualFrom = getActualFrom(from);

        if (keySchema instanceof WindowKeySchema && to < actualFrom) {
            LOG.debug("Returning no records for keys {}/{} as to ({}) < actualFrom ({}) ", keyFrom, keyTo, to, actualFrom);
            return KeyValueIterators.emptyIterator();
        }

        final List<S> searchSpace = keySchema.segmentsToSearch(segments, actualFrom, to, forward);

        final Bytes binaryFrom = keyFrom == null ? null : keySchema.lowerRange(keyFrom, actualFrom);
        final Bytes binaryTo = keyTo == null ? null : keySchema.upperRange(keyTo, to);

        return new SegmentIterator<>(
                searchSpace.iterator(),
                keySchema.hasNextCondition(keyFrom, keyTo, actualFrom, to, forward),
                binaryFrom,
                binaryTo,
                forward,
                isolationLevel);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        return all(true, null);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> backwardAll() {
        return all(false, null);
    }

    private KeyValueIterator<Bytes, byte[]> all(final boolean forward, final IsolationLevel isolationLevel) {
        final long actualFrom = getActualFrom(0);
        final List<S> searchSpace = keySchema.segmentsToSearch(segments, actualFrom, Long.MAX_VALUE, forward);

        return new SegmentIterator<>(
                searchSpace.iterator(),
                keySchema.hasNextCondition(null, null, actualFrom, Long.MAX_VALUE, forward),
                null,
                null,
                forward,
                isolationLevel);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetchAll(final long timeFrom,
                                                    final long timeTo) {
        return fetchAll(timeFrom, timeTo, true, null);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> backwardFetchAll(final long timeFrom,
                                                            final long timeTo) {
        return fetchAll(timeFrom, timeTo, false, null);
    }

    private KeyValueIterator<Bytes, byte[]> fetchAll(final long timeFrom,
                                                     final long timeTo,
                                                     final boolean forward,
                                                     final IsolationLevel isolationLevel) {
        final long actualFrom = getActualFrom(timeFrom);

        if (keySchema instanceof WindowKeySchema && timeTo < actualFrom) {
            LOG.debug("Returning no records for as timeTo ({}) < actualFrom ({}) ", timeTo, actualFrom);
            return KeyValueIterators.emptyIterator();
        }

        final List<S> searchSpace = segments.segments(actualFrom, timeTo, forward);

        return new SegmentIterator<>(
                searchSpace.iterator(),
                keySchema.hasNextCondition(null, null, actualFrom, timeTo, forward),
                null,
                null,
                forward,
                isolationLevel);
    }

    @Override
    public void remove(final Bytes key) {
        final long timestamp = keySchema.segmentTimestamp(key);
        observedStreamTime = Math.max(observedStreamTime, timestamp);
        final S segment = segments.segmentForTimestamp(timestamp);
        if (segment == null) {
            return;
        }
        segment.delete(key);
    }

    @Override
    public void put(final Bytes key,
                    final byte[] value) {
        final long timestamp = keySchema.segmentTimestamp(key);
        observedStreamTime = Math.max(observedStreamTime, timestamp);
        final long segmentId = segments.segmentId(timestamp);
        final S segment = segments.getOrCreateSegmentIfLive(segmentId, internalProcessorContext, observedStreamTime);
        if (segment == null) {
            expiredRecordSensor.record(1.0d, internalProcessorContext.currentSystemTimeMs());
        } else {
            synchronized (position) {
                // Transactional puts stage their position too, so READ_COMMITTED never sees a position ahead of the data.
                StoreQueryUtils.updatePosition(transactional ? pendingPosition : position, internalProcessorContext);
                segment.put(key, value);
            }
        }
    }

    @Override
    public byte[] get(final Bytes key) {
        return get(key, null);
    }

    private byte[] get(final Bytes key, final IsolationLevel isolationLevel) {
        final long timestampFromKey = keySchema.segmentTimestamp(key);
        // check if timestamp is expired
        if (timestampFromKey < observedStreamTime - retentionPeriod + 1) {
            LOG.debug("Record with key {} is expired as timestamp from key ({}) < actual stream time ({})",
                    key.toString(), timestampFromKey, observedStreamTime - retentionPeriod + 1);
            return null;
        }
        final S segment = segments.segmentForTimestamp(timestampFromKey);
        if (segment == null) {
            return null;
        }
        // Live read for the vanilla path; interactive queries read through the segment's isolation view.
        return isolationLevel == null ? segment.get(key) : segment.readOnly(isolationLevel).get(key);
    }

    /** An isolation-bound read view; the level is resolved once here and reads share the store's private utilities. */
    ReadOnlyView readOnly(final IsolationLevel isolationLevel) {
        Objects.requireNonNull(isolationLevel, "isolationLevel cannot be null");
        return new ReadOnlyView(this, isolationLevel);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(final StateStoreContext stateStoreContext, final StateStore root) {
        this.internalProcessorContext = asInternalProcessorContext(stateStoreContext);

        final StreamsMetricsImpl metrics = ProcessorContextUtils.metricsImpl(stateStoreContext);
        final String threadId = Thread.currentThread().getName();
        final String taskName = stateStoreContext.taskId().toString();

        expiredRecordSensor = TaskMetrics.droppedRecordsSensor(
                threadId,
                taskName,
                metrics
        );

        segments.openExisting(internalProcessorContext, observedStreamTime);
        this.position = segments.position;
        this.pendingPosition = Position.emptyPosition();
        StoreQueryUtils.maybeMigrateExistingPositionFile(stateStoreContext.stateDir(), name(), this.position);

        // register and possibly restore the state from the logs
        stateStoreContext.register(
            root,
            (RecordBatchingStateRestoreCallback) this::restoreAllInternal,
                segments::writePosition
        );

        open = true;

        consistencyEnabled = StreamsConfig.InternalConfig.getBoolean(
                stateStoreContext.appConfigs(),
                IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED,
                false);

        transactional = StreamsConfig.InternalConfig.getBoolean(
                stateStoreContext.appConfigs(),
                StreamsConfig.TRANSACTIONAL_STATE_STORES_CONFIG,
                false);
    }

    @Override
    public void commit(final Map<TopicPartition, Long> changelogOffsets) {
        synchronized (position) {
            if (transactional) {
                // Publish the staged position atomically with the segment-buffer flush below.
                position.merge(pendingPosition);
                pendingPosition = Position.emptyPosition();
            }
            segments.commit(changelogOffsets);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public boolean managesOffsets() {
        return segments.managesOffsets();
    }

    @Override
    public Long committedOffset(final TopicPartition partition) {
        return segments.committedOffset(partition);
    }

    @Override
    public long approximateNumUncommittedBytes() {
        long total = 0;
        for (final S segment : segments.allSegments(true)) {
            total += segment.approximateNumUncommittedBytes();
        }
        return total;
    }

    @Override
    public void close() {
        open = false;
        segments.close();
        // The segments discard their uncommitted writes on close; drop the staged position to match.
        pendingPosition = Position.emptyPosition();
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    // Visible for testing
    List<S> getSegments() {
        return segments.allSegments(false);
    }

    // Visible for testing
    void restoreAllInternal(final Collection<ConsumerRecord<byte[], byte[]>> records) {
        synchronized (position) {
            try {
                final Map<S, WriteBatch> writeBatchMap = getWriteBatches(records);
                for (final Map.Entry<S, WriteBatch> entry : writeBatchMap.entrySet()) {
                    final S segment = entry.getKey();
                    final WriteBatch batch = entry.getValue();
                    segment.write(batch);
                    batch.close();
                }
            } catch (final RocksDBException e) {
                throw new ProcessorStateException("Error restoring batch to store " + this.name, e);
            }
        }
    }

    // Visible for testing
    Map<S, WriteBatch> getWriteBatches(final Collection<ConsumerRecord<byte[], byte[]>> records) {
        // advance stream time to the max timestamp in the batch
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            final long timestamp = keySchema.segmentTimestamp(Bytes.wrap(record.key()));
            observedStreamTime = Math.max(observedStreamTime, timestamp);
        }

        final Map<S, WriteBatch> writeBatchMap = new HashMap<>();
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            final long timestamp = keySchema.segmentTimestamp(Bytes.wrap(record.key()));
            final long segmentId = segments.segmentId(timestamp);
            final S segment = segments.getOrCreateSegmentIfLive(segmentId, internalProcessorContext, observedStreamTime);
            if (segment != null) {
                ChangelogRecordDeserializationHelper.applyChecksAndUpdatePosition(
                    record,
                    consistencyEnabled,
                    position
                );
                WriteBatch batch = null;
                try {
                    batch = writeBatchMap.computeIfAbsent(segment, s -> new WriteBatch());
                    segment.addToBatch(new KeyValue<>(record.key(), record.value()), batch);
                } catch (final RocksDBException e) {
                    Utils.closeQuietly(batch, "rocksdb write batch");
                    throw new ProcessorStateException("Error restoring batch to store " + this.name, e);
                }
            }
        }
        return writeBatchMap;
    }

    @Override
    public Position getPosition() {
        // Include staged writes so the owner's view (and READ_UNCOMMITTED queries) stays consistent.
        if (transactional) {
            synchronized (position) {
                return position.copy().merge(pendingPosition);
            }
        }
        return position;
    }

    Position getCommittedPosition() {
        if (transactional) {
            synchronized (position) {
                return position.copy();
            }
        }
        return position;
    }

    /** Read view bound to an {@link IsolationLevel}, delegating to the store's private read utilities. */
    static final class ReadOnlyView {
        private final AbstractRocksDBSegmentedBytesStore<?> store;
        private final IsolationLevel isolationLevel;

        private ReadOnlyView(final AbstractRocksDBSegmentedBytesStore<?> store, final IsolationLevel isolationLevel) {
            this.store = store;
            this.isolationLevel = isolationLevel;
        }

        KeyValueIterator<Bytes, byte[]> fetch(final Bytes key, final long from, final long to) {
            return store.fetch(key, from, to, true, isolationLevel);
        }

        KeyValueIterator<Bytes, byte[]> backwardFetch(final Bytes key, final long from, final long to) {
            return store.fetch(key, from, to, false, isolationLevel);
        }

        KeyValueIterator<Bytes, byte[]> fetch(final Bytes keyFrom, final Bytes keyTo, final long from, final long to) {
            return store.fetch(keyFrom, keyTo, from, to, true, isolationLevel);
        }

        KeyValueIterator<Bytes, byte[]> backwardFetch(final Bytes keyFrom, final Bytes keyTo, final long from, final long to) {
            return store.fetch(keyFrom, keyTo, from, to, false, isolationLevel);
        }

        KeyValueIterator<Bytes, byte[]> all() {
            return store.all(true, isolationLevel);
        }

        KeyValueIterator<Bytes, byte[]> backwardAll() {
            return store.all(false, isolationLevel);
        }

        KeyValueIterator<Bytes, byte[]> fetchAll(final long from, final long to) {
            return store.fetchAll(from, to, true, isolationLevel);
        }

        KeyValueIterator<Bytes, byte[]> backwardFetchAll(final long from, final long to) {
            return store.fetchAll(from, to, false, isolationLevel);
        }

        byte[] get(final Bytes key) {
            return store.get(key, isolationLevel);
        }
    }
}
