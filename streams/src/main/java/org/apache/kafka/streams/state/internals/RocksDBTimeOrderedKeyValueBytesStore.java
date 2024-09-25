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
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.TimeFirstWindowKeySchema;

import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A RocksDB backed time-ordered segmented bytes store for window key schema.
 */
public class RocksDBTimeOrderedKeyValueBytesStore extends AbstractRocksDBTimeOrderedSegmentedBytesStore {

    private long minTimestamp;

    RocksDBTimeOrderedKeyValueBytesStore(final String name,
                                         final String metricsScope) {
        super(name, metricsScope, Long.MAX_VALUE, Long.MAX_VALUE, new TimeFirstWindowKeySchema(), Optional.empty());
        minTimestamp = Long.MAX_VALUE;
    }

    @Override
    protected KeyValue<Bytes, byte[]> getIndexKeyValue(final Bytes baseKey, final byte[] baseValue) {
        throw new UnsupportedOperationException("Do not use for TimeOrderedKeyValueStore");
    }

    @Override
    Map<KeyValueSegment, WriteBatch> getWriteBatches(final Collection<ConsumerRecord<byte[], byte[]>> records) {
        final Map<KeyValueSegment, WriteBatch> writeBatchMap = new HashMap<>();
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            final long timestamp = WindowKeySchema.extractStoreTimestamp(record.key());
            observedStreamTime = Math.max(observedStreamTime, timestamp);
            minTimestamp = Math.min(minTimestamp, timestamp);
            final long segmentId = segments.segmentId(timestamp);
            final KeyValueSegment segment = segments.getOrCreateSegmentIfLive(segmentId, internalProcessorContext, observedStreamTime);
            if (segment != null) {
                //null segment is if it has expired, so  we don't want those records
                ChangelogRecordDeserializationHelper.applyChecksAndUpdatePosition(
                    record,
                    consistencyEnabled,
                    position
                );
                try {
                    final WriteBatch batch = writeBatchMap.computeIfAbsent(segment, s -> new WriteBatch());

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
    protected IndexToBaseStoreIterator getIndexToBaseStoreIterator(final SegmentIterator<KeyValueSegment> segmentIterator) {
        throw new UnsupportedOperationException("Do not use for TimeOrderedKeyValueStore");
    }

    protected long minTimestamp() {
        return minTimestamp;
    }
}