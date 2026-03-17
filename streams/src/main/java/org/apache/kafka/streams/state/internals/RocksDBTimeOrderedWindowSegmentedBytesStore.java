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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.internals.ChangelogRecordDeserializationHelper;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.KeyFirstWindowKeySchema;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.TimeFirstWindowKeySchema;

import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A RocksDB backed time-ordered segmented bytes store for window key schema.
 * <p>
 * This class supports different segment implementations via the generic type parameter.
 * It can be used with {@link KeyValueSegment} for regular stores or {@link WindowSegmentWithHeaders}
 * for stores with headers support.
 *
 * @param <S> the segment type
 */
public class RocksDBTimeOrderedWindowSegmentedBytesStore<S extends Segment> extends AbstractRocksDBTimeOrderedSegmentedBytesStore<S> {

    RocksDBTimeOrderedWindowSegmentedBytesStore(final String name,
                                                final long retention,
                                                final boolean withIndex,
                                                final AbstractSegments<S> segments) {
        super(name,
            retention,
            new TimeFirstWindowKeySchema(),
            Optional.ofNullable(withIndex ? new KeyFirstWindowKeySchema() : null),
            segments);
    }

    @Override
    public void put(final Bytes key, final long timestamp, final int seqnum, final byte[] value) {
        final Bytes baseKey = TimeFirstWindowKeySchema.toStoreKeyBinary(key, timestamp, seqnum);
        put(baseKey, value);
    }

    @Override
    public byte[] fetch(final Bytes key, final long timestamp, final int seqnum) {
        return get(TimeFirstWindowKeySchema.toStoreKeyBinary(key, timestamp, seqnum));
    }

    @Override
    protected KeyValue<Bytes, byte[]> getIndexKeyValue(final Bytes baseKey, final byte[] baseValue) {
        final byte[] key = TimeFirstWindowKeySchema.extractStoreKeyBytes(baseKey.get());
        final long timestamp = TimeFirstWindowKeySchema.extractStoreTimestamp(baseKey.get());
        final int seqnum = TimeFirstWindowKeySchema.extractStoreSequence(baseKey.get());

        return KeyValue.pair(KeyFirstWindowKeySchema.toStoreKeyBinary(key, timestamp, seqnum), new byte[0]);
    }

    @Override
    Map<S, WriteBatch> getWriteBatches(
        final Collection<ConsumerRecord<byte[], byte[]>> records) {
        // advance stream time to the max timestamp in the batch
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            final long timestamp = WindowKeySchema.extractStoreTimestamp(record.key());
            observedStreamTime = Math.max(observedStreamTime, timestamp);
        }

        final Map<S, WriteBatch> writeBatchMap = new HashMap<>();
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            final long timestamp = WindowKeySchema.extractStoreTimestamp(record.key());
            final long segmentId = segments.segmentId(timestamp);
            final S segment = segments.getOrCreateSegmentIfLive(segmentId, internalProcessorContext, observedStreamTime);
            if (segment != null) {
                ChangelogRecordDeserializationHelper.applyChecksAndUpdatePosition(
                    record,
                    consistencyEnabled,
                    position
                );
                try {
                    final WriteBatch batch = writeBatchMap.computeIfAbsent(segment, s -> new WriteBatch());

                    // Assuming changelog record is serialized using WindowKeySchema
                    // from ChangeLoggingTimestampedWindowBytesStore. Reconstruct key/value to restore
                    if (hasIndex()) {
                        final byte[] indexKey = KeyFirstWindowKeySchema.fromNonPrefixWindowKey(record.key());
                        // Take care of tombstone
                        final byte[] value = record.value() == null ? null : new byte[0];
                        segment.addToBatch(new KeyValue<>(indexKey, value), batch);
                    }

                    final byte[] baseKey = TimeFirstWindowKeySchema.fromNonPrefixWindowKey(record.key());
                    segment.addToBatch(new KeyValue<>(baseKey, record.value()), batch);
                } catch (final RocksDBException e) {
                    throw new ProcessorStateException("Error restoring batch to store " + name(), e);
                }
            }
        }
        return writeBatchMap;
    }

    @Override
    protected IndexToBaseStoreIterator getIndexToBaseStoreIterator(
        final SegmentIterator<S> segmentIterator) {
        return new WindowKeySchemaIndexToBaseStoreIterator(segmentIterator);
    }
}