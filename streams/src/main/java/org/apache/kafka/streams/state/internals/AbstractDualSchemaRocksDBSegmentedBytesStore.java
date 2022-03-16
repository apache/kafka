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

import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.internals.StoreToProcessorContextAdapter;
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
import java.util.List;
import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED;

public abstract class AbstractDualSchemaRocksDBSegmentedBytesStore<S extends Segment> implements SegmentedBytesStore {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractDualSchemaRocksDBSegmentedBytesStore.class);

    private final String name;
    protected final AbstractSegments<S> segments;
    private final String metricScope;
    protected final KeySchema baseKeySchema;
    protected final Optional<KeySchema> indexKeySchema;


    protected ProcessorContext context;
    private StateStoreContext stateStoreContext;
    private Sensor expiredRecordSensor;
    protected long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;
    protected boolean consistencyEnabled = false;
    protected Position position;
    protected OffsetCheckpoint positionCheckpoint;
    private volatile boolean open;

    AbstractDualSchemaRocksDBSegmentedBytesStore(final String name,
                                                 final String metricScope,
                                                 final KeySchema baseKeySchema,
                                                 final Optional<KeySchema> indexKeySchema,
                                                 final AbstractSegments<S> segments) {
        this.name = name;
        this.metricScope = metricScope;
        this.baseKeySchema = baseKeySchema;
        this.indexKeySchema = indexKeySchema;
        this.segments = segments;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        final List<S> searchSpace = segments.allSegments(true);
        final Bytes from = baseKeySchema.lowerRange(null, 0);
        final Bytes to = baseKeySchema.upperRange(null, Long.MAX_VALUE);

        return new SegmentIterator<>(
                searchSpace.iterator(),
                baseKeySchema.hasNextCondition(null, null, 0, Long.MAX_VALUE, true),
                from,
                to,
                true);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> backwardAll() {
        final List<S> searchSpace = segments.allSegments(false);
        final Bytes from = baseKeySchema.lowerRange(null, 0);
        final Bytes to = baseKeySchema.upperRange(null, Long.MAX_VALUE);

        return new SegmentIterator<>(
                searchSpace.iterator(),
                baseKeySchema.hasNextCondition(null, null, 0, Long.MAX_VALUE, false),
                from,
                to,
                false);
    }

    @Override
    public void remove(final Bytes rawBaseKey) {
        final long timestamp = baseKeySchema.segmentTimestamp(rawBaseKey);
        observedStreamTime = Math.max(observedStreamTime, timestamp);
        final S segment = segments.getSegmentForTimestamp(timestamp);
        if (segment == null) {
            return;
        }
        segment.delete(rawBaseKey);

        if (hasIndex()) {
            final KeyValue<Bytes, byte[]> kv = getIndexKeyValue(rawBaseKey, null);
            segment.delete(kv.key);
        }
    }

    abstract protected KeyValue<Bytes, byte[]> getIndexKeyValue(final Bytes baseKey, final byte[] baseValue);

    // For testing
    void putIndex(final Bytes indexKey, final byte[] value) {
        if (!hasIndex()) {
            throw new IllegalStateException("Index store doesn't exist");
        }

        final long timestamp = indexKeySchema.get().segmentTimestamp(indexKey);
        final long segmentId = segments.segmentId(timestamp);
        final S segment = segments.getOrCreateSegmentIfLive(segmentId, context, observedStreamTime);

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
        final S segment = segments.getOrCreateSegmentIfLive(segmentId, context, observedStreamTime);

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
        final S segment = segments.getOrCreateSegmentIfLive(segmentId, context, observedStreamTime);

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
        final S segment = segments.getOrCreateSegmentIfLive(segmentId, context, observedStreamTime);

        if (segment == null) {
            expiredRecordSensor.record(1.0d, ProcessorContextUtils.currentSystemTime(context));
            LOG.warn("Skipping record for expired segment.");
        } else {
            StoreQueryUtils.updatePosition(position, stateStoreContext);
            segment.put(rawBaseKey, value);

            if (hasIndex()) {
                final KeyValue<Bytes, byte[]> indexKeyValue = getIndexKeyValue(rawBaseKey, value);
                segment.put(indexKeyValue.key, indexKeyValue.value);
            }
        }
    }

    @Override
    public byte[] get(final Bytes rawKey) {
        final S segment = segments.getSegmentForTimestamp(baseKeySchema.segmentTimestamp(rawKey));
        if (segment == null) {
            return null;
        }
        return segment.get(rawKey);
    }

    @Override
    public String name() {
        return name;
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        this.context = context;

        final StreamsMetricsImpl metrics = ProcessorContextUtils.getMetricsImpl(context);
        final String threadId = Thread.currentThread().getName();
        final String taskName = context.taskId().toString();

        expiredRecordSensor = TaskMetrics.droppedRecordsSensor(
            threadId,
            taskName,
            metrics
        );

        segments.openExisting(context, observedStreamTime);

        final File positionCheckpointFile = new File(context.stateDir(), name() + ".position");
        this.positionCheckpoint = new OffsetCheckpoint(positionCheckpointFile);
        this.position = StoreQueryUtils.readPositionFromCheckpoint(positionCheckpoint);

        // register and possibly restore the state from the logs
        stateStoreContext.register(
            root,
            (RecordBatchingStateRestoreCallback) this::restoreAllInternal,
            () -> StoreQueryUtils.checkpointPosition(positionCheckpoint, position)
        );

        open = true;

        consistencyEnabled = StreamsConfig.InternalConfig.getBoolean(
            context.appConfigs(),
            IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED,
            false
        );
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        this.stateStoreContext = context;
        init(StoreToProcessorContextAdapter.adapt(context), root);
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

    abstract Map<S, WriteBatch> getWriteBatches(final Collection<ConsumerRecord<byte[], byte[]>> records);

    @Override
    public Position getPosition() {
        return position;
    }

    public boolean hasIndex() {
        return indexKeySchema.isPresent();
    }
}