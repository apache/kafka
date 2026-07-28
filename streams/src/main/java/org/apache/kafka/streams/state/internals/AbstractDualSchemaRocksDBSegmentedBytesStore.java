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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.streams.StreamsConfig.InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED;
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;

public abstract class AbstractDualSchemaRocksDBSegmentedBytesStore<S extends Segment> implements SegmentedBytesStore, WithRetentionPeriod {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractDualSchemaRocksDBSegmentedBytesStore.class);

    private final String name;
    protected final AbstractSegments<S> segments;
    protected final KeySchema baseKeySchema;
    protected final Optional<KeySchema> indexKeySchema;
    private final long retentionPeriod;

    protected InternalProcessorContext<?, ?> internalProcessorContext;
    private Sensor expiredRecordSensor;
    protected long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;
    protected boolean consistencyEnabled = false;
    protected Position position;
    // Position of writes staged in the current transaction; merged into the committed position on commit().
    private Position pendingPosition = Position.emptyPosition();
    private boolean transactional;
    private volatile boolean open;

    AbstractDualSchemaRocksDBSegmentedBytesStore(final String name,
                                                 final KeySchema baseKeySchema,
                                                 final Optional<KeySchema> indexKeySchema,
                                                 final AbstractSegments<S> segments,
                                                 final long retentionPeriod) {
        this.name = name;
        this.baseKeySchema = baseKeySchema;
        this.indexKeySchema = indexKeySchema;
        this.segments = segments;
        this.retentionPeriod = retentionPeriod;
    }

    @Override
    public long retentionPeriod() {
        return retentionPeriod;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        return all(true, null);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> backwardAll() {
        return all(false, null);
    }

    KeyValueIterator<Bytes, byte[]> all(final boolean forward, final IsolationLevel isolationLevel) {
        final long actualFrom = getActualFrom(0, baseKeySchema instanceof PrefixedWindowKeySchemas.TimeFirstWindowKeySchema);

        final List<S> searchSpace = segments.allSegments(forward);
        final Bytes from = baseKeySchema.lowerRange(null, actualFrom);
        final Bytes to = baseKeySchema.upperRange(null, Long.MAX_VALUE);

        return new SegmentIterator<>(
                searchSpace.iterator(),
                baseKeySchema.hasNextCondition(null, null, actualFrom, Long.MAX_VALUE, forward),
                from,
                to,
                forward,
                isolationLevel);
    }

    @Override
    public void remove(final Bytes rawBaseKey) {
        final long timestamp = baseKeySchema.segmentTimestamp(rawBaseKey);
        observedStreamTime = Math.max(observedStreamTime, timestamp);
        final S segment = segments.segmentForTimestamp(timestamp);
        if (segment == null) {
            return;
        }
        segment.delete(rawBaseKey);

        if (hasIndex()) {
            final KeyValue<Bytes, byte[]> kv = getIndexKeyValue(rawBaseKey, null);
            segment.delete(kv.key);
        }
    }

    protected abstract KeyValue<Bytes, byte[]> getIndexKeyValue(final Bytes baseKey, final byte[] baseValue);

    // isTimeFirstWindowSchema true implies ON_WINDOW_CLOSE semantics. There's an edge case
    // when retentionPeriod = grace Period. If we add 1, then actualFrom > to which would
    // lead to no records being returned.
    protected long getActualFrom(final long from, final boolean isTimeFirstWindowSchema) {
        return isTimeFirstWindowSchema ? Math.max(from, observedStreamTime - retentionPeriod) :
                Math.max(from, observedStreamTime - retentionPeriod + 1);

    }

    // For testing
    void putIndex(final Bytes indexKey, final byte[] value) {
        if (!hasIndex()) {
            throw new IllegalStateException("Index store doesn't exist");
        }

        final long timestamp = indexKeySchema.get().segmentTimestamp(indexKey);
        final long segmentId = segments.segmentId(timestamp);
        final S segment = segments.getOrCreateSegmentIfLive(segmentId, internalProcessorContext, observedStreamTime);

        if (segment != null) {
            segment.put(indexKey, value);
        }
    }

    byte[] getIndex(final Bytes indexKey) {
        if (!hasIndex()) {
            throw new IllegalStateException("Index store doesn't exist");
        }

        final long timestamp = indexKeySchema.get().segmentTimestamp(indexKey);
        final long segmentId = segments.segmentId(timestamp);
        final S segment = segments.getOrCreateSegmentIfLive(segmentId, internalProcessorContext, observedStreamTime);

        if (segment != null) {
            return segment.get(indexKey);
        }
        return null;
    }

    void removeIndex(final Bytes indexKey) {
        if (!hasIndex()) {
            throw new IllegalStateException("Index store doesn't exist");
        }

        final long timestamp = indexKeySchema.get().segmentTimestamp(indexKey);
        final long segmentId = segments.segmentId(timestamp);
        final S segment = segments.getOrCreateSegmentIfLive(segmentId, internalProcessorContext, observedStreamTime);

        if (segment != null) {
            segment.delete(indexKey);
        }
    }

    @Override
    public void put(final Bytes rawBaseKey,
                    final byte[] value) {
        final long timestamp = baseKeySchema.segmentTimestamp(rawBaseKey);
        observedStreamTime = Math.max(observedStreamTime, timestamp);
        final long segmentId = segments.segmentId(timestamp);
        final S segment = segments.getOrCreateSegmentIfLive(segmentId, internalProcessorContext, observedStreamTime);

        if (segment == null) {
            expiredRecordSensor.record(1.0d, internalProcessorContext.currentSystemTimeMs());
            LOG.warn("Skipping record for expired segment.");
        } else {
            synchronized (position) {
                // Transactional puts stage their position too, so READ_COMMITTED never sees a position ahead of the data.
                StoreQueryUtils.updatePosition(transactional ? pendingPosition : position, internalProcessorContext);

                // Put to index first so that if put to base failed, when we iterate index, we will
                // find no base value. If put to base first but putting to index fails, when we iterate
                // index, we can't find the key but if we iterate over base store, we can find the key
                // which lead to inconsistency.
                if (hasIndex()) {
                    final KeyValue<Bytes, byte[]> indexKeyValue = getIndexKeyValue(rawBaseKey, value);
                    segment.put(indexKeyValue.key, indexKeyValue.value);
                }
                segment.put(rawBaseKey, value);
            }
        }
    }

    @Override
    public byte[] get(final Bytes rawKey) {
        return get(rawKey, null);
    }

    byte[] get(final Bytes rawKey, final IsolationLevel isolationLevel) {
        final long timestampFromRawKey = baseKeySchema.segmentTimestamp(rawKey);
        // check if timestamp is expired

        if (baseKeySchema instanceof PrefixedWindowKeySchemas.TimeFirstWindowKeySchema) {
            if (timestampFromRawKey < observedStreamTime - retentionPeriod) {
                LOG.debug("Record with key {} is expired as timestamp from key ({}) < actual stream time ({})",
                        rawKey.toString(), timestampFromRawKey, observedStreamTime - retentionPeriod);
                return null;
            }
        } else {
            if (timestampFromRawKey < observedStreamTime - retentionPeriod + 1) {
                LOG.debug("Record with key {} is expired as timestamp from key ({}) < actual stream time ({})",
                        rawKey.toString(), timestampFromRawKey, observedStreamTime - retentionPeriod + 1);
                return null;
            }
        }

        final S segment = segments.segmentForTimestamp(timestampFromRawKey);
        if (segment == null) {
            return null;
        }
        // Live read for the vanilla path; interactive queries read through the segment's isolation view.
        return isolationLevel == null ? segment.get(rawKey) : segment.readOnly(isolationLevel).get(rawKey);
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
            false
        );

        transactional = StreamsConfig.InternalConfig.getBoolean(
            stateStoreContext.appConfigs(),
            StreamsConfig.TRANSACTIONAL_STATE_STORES_CONFIG,
            false
        );
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

    abstract Map<S, WriteBatch> getWriteBatches(final Collection<ConsumerRecord<byte[], byte[]>> records);

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

    public boolean hasIndex() {
        return indexKeySchema.isPresent();
    }
}
