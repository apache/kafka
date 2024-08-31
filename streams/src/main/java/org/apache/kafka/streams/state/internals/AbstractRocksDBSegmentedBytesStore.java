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

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED;
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;

public class AbstractRocksDBSegmentedBytesStore<S extends Segment> implements SegmentedBytesStore {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractRocksDBSegmentedBytesStore.class);

    private final String name;
    private final AbstractSegments<S> segments;
    private final String metricScope;
    private final long retentionPeriod;
    private final KeySchema keySchema;

    private InternalProcessorContext internalProcessorContext;
    private Sensor expiredRecordSensor;
    private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;
    private boolean consistencyEnabled = false;
    private Position position;
    protected OffsetCheckpoint positionCheckpoint;
    private volatile boolean open;

    AbstractRocksDBSegmentedBytesStore(final String name,
                                       final String metricScope,
                                       final long retentionPeriod,
                                       final KeySchema keySchema,
                                       final AbstractSegments<S> segments) {
        this.name = name;
        this.metricScope = metricScope;
        this.retentionPeriod = retentionPeriod;
        this.keySchema = keySchema;
        this.segments = segments;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetch(final Bytes key,
                                                 final long from,
                                                 final long to) {
        return fetch(key, from, to, true);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> backwardFetch(final Bytes key,
                                                         final long from,
                                                         final long to) {
        return fetch(key, from, to, false);
    }

    KeyValueIterator<Bytes, byte[]> fetch(final Bytes key,
                                          final long from,
                                          final long to,
                                          final boolean forward) {
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
                forward);
    }

    private long getActualFrom(final long from) {
        return Math.max(from, observedStreamTime - retentionPeriod + 1);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetch(final Bytes keyFrom,
                                                 final Bytes keyTo,
                                                 final long from,
                                                 final long to) {
        return fetch(keyFrom, keyTo, from, to, true);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> backwardFetch(final Bytes keyFrom,
                                                         final Bytes keyTo,
                                                         final long from,
                                                         final long to) {
        return fetch(keyFrom, keyTo, from, to, false);
    }

    KeyValueIterator<Bytes, byte[]> fetch(final Bytes keyFrom,
                                          final Bytes keyTo,
                                          final long from,
                                          final long to,
                                          final boolean forward) {
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
                forward);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        final long actualFrom = getActualFrom(0);
        final List<S> searchSpace = keySchema.segmentsToSearch(segments, actualFrom, Long.MAX_VALUE, true);

        return new SegmentIterator<>(
                searchSpace.iterator(),
                keySchema.hasNextCondition(null, null, actualFrom, Long.MAX_VALUE, true),
                null,
                null,
                true);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> backwardAll() {
        final long actualFrom = getActualFrom(0);

        final List<S> searchSpace = keySchema.segmentsToSearch(segments, actualFrom, Long.MAX_VALUE, false);

        return new SegmentIterator<>(
                searchSpace.iterator(),
                keySchema.hasNextCondition(null, null, actualFrom, Long.MAX_VALUE, false),
                null,
                null,
                false);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetchAll(final long timeFrom,
                                                    final long timeTo) {
        final long actualFrom = getActualFrom(timeFrom);

        if (keySchema instanceof WindowKeySchema && timeTo < actualFrom) {
            LOG.debug("Returning no records for as timeTo ({}) < actualFrom ({}) ", timeTo, actualFrom);
            return KeyValueIterators.emptyIterator();
        }

        final List<S> searchSpace = segments.segments(actualFrom, timeTo, true);

        return new SegmentIterator<>(
                searchSpace.iterator(),
                keySchema.hasNextCondition(null, null, actualFrom, timeTo, true),
                null,
                null,
                true);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> backwardFetchAll(final long timeFrom,
                                                            final long timeTo) {
        final long actualFrom = getActualFrom(timeFrom);

        if (keySchema instanceof WindowKeySchema && timeTo < actualFrom) {
            LOG.debug("Returning no records for as timeTo ({}) < actualFrom ({}) ", timeTo, actualFrom);
            return KeyValueIterators.emptyIterator();
        }

        final List<S> searchSpace = segments.segments(actualFrom, timeTo, false);

        return new SegmentIterator<>(
                searchSpace.iterator(),
                keySchema.hasNextCondition(null, null, actualFrom, timeTo, false),
                null,
                null,
                false);
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
    public void remove(final Bytes key, final long timestamp) {
        final Bytes keyBytes = keySchema.toStoreBinaryKeyPrefix(key, timestamp);
        final S segment = segments.segmentForTimestamp(timestamp);
        if (segment != null) {
            segment.deleteRange(keyBytes, keyBytes);
        }
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
                StoreQueryUtils.updatePosition(position, internalProcessorContext);
                segment.put(key, value);
            }
        }
    }

    @Override
    public byte[] get(final Bytes key) {
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
        return segment.get(key);
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

        final File positionCheckpointFile = new File(stateStoreContext.stateDir(), name() + ".position");
        this.positionCheckpoint = new OffsetCheckpoint(positionCheckpointFile);
        this.position = StoreQueryUtils.readPositionFromCheckpoint(positionCheckpoint);
        segments.setPosition(position);
        segments.openExisting(internalProcessorContext, observedStreamTime);

        // register and possibly restore the state from the logs
        stateStoreContext.register(
            root,
            (RecordBatchingStateRestoreCallback) this::restoreAllInternal,
            () -> StoreQueryUtils.checkpointPosition(positionCheckpoint, position)
        );

        open = true;

        consistencyEnabled = StreamsConfig.InternalConfig.getBoolean(
                stateStoreContext.appConfigs(),
                IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED,
                false);
    }

    @Override
    public void flush() {
        segments.flush();
    }

    @Override
    public void close() {
        open = false;
        segments.close();
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
        return position;
    }
}