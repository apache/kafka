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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.internals.ChangelogRecordDeserializationHelper;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.internals.PrefixedSessionKeySchemas.KeyFirstSessionKeySchema;
import org.apache.kafka.streams.state.internals.PrefixedSessionKeySchemas.TimeFirstSessionKeySchema;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

/**
 * A RocksDB backed time-ordered segmented bytes store for session key schema.
 */
public class RocksDBTimeOrderedSessionSegmentedBytesStore extends AbstractRocksDBTimeOrderedSegmentedBytesStore {

    private class SessionKeySchemaIndexToBaseStoreIterator extends IndexToBaseStoreIterator {
        SessionKeySchemaIndexToBaseStoreIterator(final KeyValueIterator<Bytes, byte[]> indexIterator) {
            super(indexIterator);
        }

        @Override
        protected Bytes getBaseKey(final Bytes indexKey) {
            final Window window = KeyFirstSessionKeySchema.extractWindow(indexKey.get());
            final byte[] key = KeyFirstSessionKeySchema.extractKeyBytes(indexKey.get());

            return TimeFirstSessionKeySchema.toBinary(Bytes.wrap(key), window.start(), window.end());
        }
    }

    RocksDBTimeOrderedSessionSegmentedBytesStore(final String name,
                                                 final String metricsScope,
                                                 final long retention,
                                                 final long segmentInterval,
                                                 final boolean withIndex) {
        super(name, metricsScope, retention, segmentInterval, new TimeFirstSessionKeySchema(),
            Optional.ofNullable(withIndex ? new KeyFirstSessionKeySchema() : null));
    }

    public byte[] fetchSession(final Bytes key,
                               final long sessionStartTime,
                               final long sessionEndTime) {
        return get(TimeFirstSessionKeySchema.toBinary(
            key,
            sessionStartTime,
            sessionEndTime
        ));
    }

    public KeyValueIterator<Bytes, byte[]> fetchSessions(final long earliestSessionEndTime,
                                                         final long latestSessionEndTime) {
        final List<KeyValueSegment> searchSpace = segments.segments(earliestSessionEndTime, latestSessionEndTime, true);

        // here we want [0, latestSE, FF] as the upper bound to cover any possible keys,
        // but since we can only get upper bound based on timestamps, we use a slight larger upper bound as [0, latestSE+1]
        final Bytes binaryFrom = baseKeySchema.lowerRangeFixedSize(null, earliestSessionEndTime);
        final Bytes binaryTo = baseKeySchema.lowerRangeFixedSize(null, latestSessionEndTime + 1);

        return new SegmentIterator<>(
                searchSpace.iterator(),
                iterator -> {
                    while (iterator.hasNext()) {
                        final Bytes bytes = iterator.peekNextKey();

                        final Windowed<Bytes> windowedKey = TimeFirstSessionKeySchema.from(bytes);
                        final long endTime = windowedKey.window().end();

                        if (endTime <= latestSessionEndTime && endTime >= earliestSessionEndTime) {
                            return true;
                        }
                        iterator.next();
                    }
                    return false;
                },
                binaryFrom,
                binaryTo,
                true);
    }

    public void remove(final Windowed<Bytes> key) {
        remove(TimeFirstSessionKeySchema.toBinary(key));
    }

    public void put(final Windowed<Bytes> sessionKey, final byte[] aggregate) {
        put(TimeFirstSessionKeySchema.toBinary(sessionKey), aggregate);
    }

    @Override
    protected KeyValue<Bytes, byte[]> getIndexKeyValue(final Bytes baseKey, final byte[] baseValue) {
        final Window window = TimeFirstSessionKeySchema.extractWindow(baseKey.get());
        final byte[] key = TimeFirstSessionKeySchema.extractKeyBytes(baseKey.get());
        return KeyValue.pair(KeyFirstSessionKeySchema.toBinary(Bytes.wrap(key), window.start(), window.end()), new byte[0]);
    }

    @Override
    Map<KeyValueSegment, WriteBatch> getWriteBatches(
        final Collection<ConsumerRecord<byte[], byte[]>> records) {
        // advance stream time to the max timestamp in the batch
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            final long timestamp = SessionKeySchema.extractEndTimestamp(record.key());
            observedStreamTime = Math.max(observedStreamTime, timestamp);
        }

        final Map<KeyValueSegment, WriteBatch> writeBatchMap = new HashMap<>();
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            final long timestamp = SessionKeySchema.extractEndTimestamp(record.key());
            final long segmentId = segments.segmentId(timestamp);
            final KeyValueSegment segment = segments.getOrCreateSegmentIfLive(segmentId, context, observedStreamTime);
            if (segment != null) {
                ChangelogRecordDeserializationHelper.applyChecksAndUpdatePosition(
                    record,
                    consistencyEnabled,
                    position
                );
                try {
                    final WriteBatch batch = writeBatchMap.computeIfAbsent(segment, s -> new WriteBatch());

                    // Assuming changelog record is serialized using SessionKeySchema
                    // from ChangeLoggingSessionBytesStore. Reconstruct key/value to restore
                    if (hasIndex()) {
                        final byte[] indexKey = KeyFirstSessionKeySchema.prefixNonPrefixSessionKey(record.key());
                        // Take care of tombstone
                        final byte[] value = record.value() == null ? null : new byte[0];
                        segment.addToBatch(new KeyValue<>(indexKey, value), batch);
                    }

                    final byte[] baseKey = TimeFirstSessionKeySchema.extractWindowBytesFromNonPrefixSessionKey(record.key());
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
        final SegmentIterator<KeyValueSegment> segmentIterator) {
        return new SessionKeySchemaIndexToBaseStoreIterator(segmentIterator);
    }
}