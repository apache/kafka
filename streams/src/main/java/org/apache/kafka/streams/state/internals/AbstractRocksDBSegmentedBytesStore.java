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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.AbstractNotifyingBatchingRestoreCallback;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.EXPIRED_WINDOW_RECORD_DROP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCount;

public class AbstractRocksDBSegmentedBytesStore<S extends Segment> implements SegmentedBytesStore {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractRocksDBSegmentedBytesStore.class);
    private final String name;
    private final AbstractSegments<S> segments;
    private final String metricScope;
    private final KeySchema keySchema;
    private InternalProcessorContext context;
    private volatile boolean open;
    private Set<S> bulkLoadSegments;
    private Sensor expiredRecordSensor;
    private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;

    AbstractRocksDBSegmentedBytesStore(final String name,
                                       final String metricScope,
                                       final KeySchema keySchema,
                                       final AbstractSegments<S> segments) {
        this.name = name;
        this.metricScope = metricScope;
        this.keySchema = keySchema;
        this.segments = segments;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetch(final Bytes key,
                                                 final long from,
                                                 final long to) {
        final List<S> searchSpace = keySchema.segmentsToSearch(segments, from, to);

        final Bytes binaryFrom = keySchema.lowerRangeFixedSize(key, from);
        final Bytes binaryTo = keySchema.upperRangeFixedSize(key, to);

        return new SegmentIterator<>(
            searchSpace.iterator(),
            keySchema.hasNextCondition(key, key, from, to),
            binaryFrom,
            binaryTo);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetch(final Bytes keyFrom,
                                                 final Bytes keyTo,
                                                 final long from,
                                                 final long to) {
        if (keyFrom.compareTo(keyTo) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. "
                + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        final List<S> searchSpace = keySchema.segmentsToSearch(segments, from, to);

        final Bytes binaryFrom = keySchema.lowerRange(keyFrom, from);
        final Bytes binaryTo = keySchema.upperRange(keyTo, to);

        return new SegmentIterator<>(
            searchSpace.iterator(),
            keySchema.hasNextCondition(keyFrom, keyTo, from, to),
            binaryFrom,
            binaryTo);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        final List<S> searchSpace = segments.allSegments();

        return new SegmentIterator<>(
            searchSpace.iterator(),
            keySchema.hasNextCondition(null, null, 0, Long.MAX_VALUE),
            null,
            null);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetchAll(final long timeFrom,
                                                    final long timeTo) {
        final List<S> searchSpace = segments.segments(timeFrom, timeTo);

        return new SegmentIterator<>(
            searchSpace.iterator(),
            keySchema.hasNextCondition(null, null, timeFrom, timeTo),
            null,
            null);
    }

    @Override
    public void remove(final Bytes key) {
        final long timestamp = keySchema.segmentTimestamp(key);
        observedStreamTime = Math.max(observedStreamTime, timestamp);
        final S segment = segments.getSegmentForTimestamp(timestamp);
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
        final S segment = segments.getOrCreateSegmentIfLive(segmentId, context, observedStreamTime);
        if (segment == null) {
            expiredRecordSensor.record();
            LOG.debug("Skipping record for expired segment.");
        } else {
            segment.put(key, value);
        }
    }

    @Override
    public byte[] get(final Bytes key) {
        final S segment = segments.getSegmentForTimestamp(keySchema.segmentTimestamp(key));
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
    public void init(final ProcessorContext context,
                     final StateStore root) {
        this.context = (InternalProcessorContext) context;

        final StreamsMetricsImpl metrics = this.context.metrics();
        final String taskName = context.taskId().toString();

        expiredRecordSensor = metrics.storeLevelSensor(
            taskName,
            name(),
            EXPIRED_WINDOW_RECORD_DROP,
            Sensor.RecordingLevel.INFO
        );
        addInvocationRateAndCount(
            expiredRecordSensor,
            "stream-" + metricScope + "-metrics",
            metrics.tagMap("task-id", taskName, metricScope + "-id", name()),
            EXPIRED_WINDOW_RECORD_DROP
        );

        segments.openExisting(this.context, observedStreamTime);

        bulkLoadSegments = new HashSet<>(segments.allSegments());

        // register and possibly restore the state from the logs
        context.register(root, new RocksDBSegmentsBatchingRestoreCallback());

        open = true;
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
        return segments.allSegments();
    }

    // Visible for testing
    void restoreAllInternal(final Collection<KeyValue<byte[], byte[]>> records) {
        try {
            final Map<S, WriteBatch> writeBatchMap = getWriteBatches(records);
            for (final Map.Entry<S, WriteBatch> entry : writeBatchMap.entrySet()) {
                final S segment = entry.getKey();
                final WriteBatch batch = entry.getValue();
                segment.write(batch);
            }
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error restoring batch to store " + this.name, e);
        }
    }

    // Visible for testing
    Map<S, WriteBatch> getWriteBatches(final Collection<KeyValue<byte[], byte[]>> records) {
        // advance stream time to the max timestamp in the batch
        for (final KeyValue<byte[], byte[]> record : records) {
            final long timestamp = keySchema.segmentTimestamp(Bytes.wrap(record.key));
            observedStreamTime = Math.max(observedStreamTime, timestamp);
        }

        final Map<S, WriteBatch> writeBatchMap = new HashMap<>();
        for (final KeyValue<byte[], byte[]> record : records) {
            final long timestamp = keySchema.segmentTimestamp(Bytes.wrap(record.key));
            final long segmentId = segments.segmentId(timestamp);
            final S segment = segments.getOrCreateSegmentIfLive(segmentId, context, observedStreamTime);
            if (segment != null) {
                // This handles the case that state store is moved to a new client and does not
                // have the local RocksDB instance for the segment. In this case, toggleDBForBulkLoading
                // will only close the database and open it again with bulk loading enabled.
                if (!bulkLoadSegments.contains(segment)) {
                    segment.toggleDbForBulkLoading(true);
                    // If the store does not exist yet, the getOrCreateSegmentIfLive will call openDB that
                    // makes the open flag for the newly created store.
                    // if the store does exist already, then toggleDbForBulkLoading will make sure that
                    // the store is already open here.
                    bulkLoadSegments = new HashSet<>(segments.allSegments());
                }
                try {
                    final WriteBatch batch = writeBatchMap.computeIfAbsent(segment, s -> new WriteBatch());
                    segment.addToBatch(record, batch);
                } catch (final RocksDBException e) {
                    throw new ProcessorStateException("Error restoring batch to store " + this.name, e);
                }
            }
        }
        return writeBatchMap;
    }

    private void toggleForBulkLoading(final boolean prepareForBulkload) {
        for (final S segment : segments.allSegments()) {
            segment.toggleDbForBulkLoading(prepareForBulkload);
        }
    }

    private class RocksDBSegmentsBatchingRestoreCallback extends AbstractNotifyingBatchingRestoreCallback {

        @Override
        public void restoreAll(final Collection<KeyValue<byte[], byte[]>> records) {
            restoreAllInternal(records);
        }

        @Override
        public void onRestoreStart(final TopicPartition topicPartition,
                                   final String storeName,
                                   final long startingOffset,
                                   final long endingOffset) {
            toggleForBulkLoading(true);
        }

        @Override
        public void onRestoreEnd(final TopicPartition topicPartition,
                                 final String storeName,
                                 final long totalRestored) {
            toggleForBulkLoading(false);
        }
    }
}
