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
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.internals.ChangelogRecordDeserializationHelper;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.KeyFirstWindowKeySchema;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.TimeFirstWindowKeySchema;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocksDB store backed by two SegmentedBytesStores which can optimize scan by time as well as window
 * lookup for a specific key.
 *
 * Schema for first SegmentedBytesStore (base store) is as below:
 *     Key schema: | timestamp + recordkey |
 *     Value schema: | value |. Value here is determined by caller.
 *
 * Schema for second SegmentedBytesStore (index store) is as below:
 *     Key schema: | record + timestamp |
 *     Value schema: ||
 *
 * Operations:
 *     Put: 1. Put to index store. 2. Put to base store.
 *     Delete: 1. Delete from base store. 2. Delete from index store.
 * Since we need to update two stores, failure can happen in the middle. We put in index store first
 * to make sure if a failure happens in second step and the view is inconsistent, we can't get the
 * value for the key. We delete from base store first to make sure if a failure happens in second step
 * and the view is inconsistent, we can't get the value for the key.
 *
 * Note:
 *     Index store can be optional if we can construct the timestamp in base store instead of looking
 *     them up from index store.
 *
 */
public class RocksDBTimeOrderedSegmentedBytesStore extends AbstractDualSchemaRocksDBSegmentedBytesStore<KeyValueSegment> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractDualSchemaRocksDBSegmentedBytesStore.class);

    private class IndexToBaseStoreIterator implements KeyValueIterator<Bytes, byte[]> {
        private final KeyValueIterator<Bytes, byte[]> indexIterator;
        private byte[] cachedValue;


        IndexToBaseStoreIterator(final KeyValueIterator<Bytes, byte[]> indexIterator) {
            this.indexIterator = indexIterator;
        }

        @Override
        public void close() {
            indexIterator.close();
        }

        @Override
        public Bytes peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return getBaseKey(indexIterator.peekNextKey());
        }

        @Override
        public boolean hasNext() {
            while (indexIterator.hasNext()) {
                final Bytes key = indexIterator.peekNextKey();
                final Bytes baseKey = getBaseKey(key);

                cachedValue = get(baseKey);
                if (cachedValue == null) {
                    // Key not in base store, inconsistency happened and remove from index.
                    indexIterator.next();
                    RocksDBTimeOrderedSegmentedBytesStore.this.removeIndex(key);
                } else {
                    return true;
                }
            }
            return false;
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            if (cachedValue == null && !hasNext()) {
                throw new NoSuchElementException();
            }
            final KeyValue<Bytes, byte[]> ret = indexIterator.next();
            final byte[] value = cachedValue;
            cachedValue = null;
            return KeyValue.pair(getBaseKey(ret.key), value);
        }

        private Bytes getBaseKey(final Bytes indexKey) {
            final byte[] keyBytes = KeyFirstWindowKeySchema.extractStoreKeyBytes(indexKey.get());
            final long timestamp = KeyFirstWindowKeySchema.extractStoreTimestamp(indexKey.get());
            final int seqnum = KeyFirstWindowKeySchema.extractStoreSequence(indexKey.get());
            return TimeFirstWindowKeySchema.toStoreKeyBinary(keyBytes, timestamp, seqnum);
        }
    }

    RocksDBTimeOrderedSegmentedBytesStore(final String name,
                                          final String metricsScope,
                                          final long retention,
                                          final long segmentInterval,
                                          final boolean withIndex) {
        super(name, metricsScope, new TimeFirstWindowKeySchema(),
            Optional.ofNullable(withIndex ? new KeyFirstWindowKeySchema() : null),
            new KeyValueSegments(name, metricsScope, retention, segmentInterval));
    }

    public void put(final Bytes key, final long timestamp, final int seqnum, final byte[] value) {
        final Bytes baseKey = TimeFirstWindowKeySchema.toStoreKeyBinary(key, timestamp, seqnum);
        put(baseKey, value);
    }

    byte[] fetch(final Bytes key, final long timestamp, final int seqnum) {
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
    Map<KeyValueSegment, WriteBatch> getWriteBatches(
        final Collection<ConsumerRecord<byte[], byte[]>> records) {
        // advance stream time to the max timestamp in the batch
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            final long timestamp = WindowKeySchema.extractStoreTimestamp(record.key());
            observedStreamTime = Math.max(observedStreamTime, timestamp);
        }

        final Map<KeyValueSegment, WriteBatch> writeBatchMap = new HashMap<>();
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            final long timestamp = WindowKeySchema.extractStoreTimestamp(record.key());
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
        if (indexKeySchema.isPresent()) {
            final List<KeyValueSegment> searchSpace = indexKeySchema.get().segmentsToSearch(segments, from, to,
                forward);

            final Bytes binaryFrom = indexKeySchema.get().lowerRangeFixedSize(key, from);
            final Bytes binaryTo = indexKeySchema.get().upperRangeFixedSize(key, to);

            return new IndexToBaseStoreIterator(new SegmentIterator<>(
                searchSpace.iterator(),
                indexKeySchema.get().hasNextCondition(key, key, from, to, forward),
                binaryFrom,
                binaryTo,
                forward));
        }

        final List<KeyValueSegment> searchSpace = baseKeySchema.segmentsToSearch(segments, from, to,
            forward);

        final Bytes binaryFrom = baseKeySchema.lowerRangeFixedSize(key, from);
        final Bytes binaryTo = baseKeySchema.upperRangeFixedSize(key, to);

        return new SegmentIterator<>(
            searchSpace.iterator(),
            baseKeySchema.hasNextCondition(key, key, from, to, forward),
            binaryFrom,
            binaryTo,
            forward);
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

        if (indexKeySchema.isPresent()) {
            final List<KeyValueSegment> searchSpace = indexKeySchema.get().segmentsToSearch(segments, from, to,
                forward);

            final Bytes binaryFrom = indexKeySchema.get().lowerRange(keyFrom, from);
            final Bytes binaryTo = indexKeySchema.get().upperRange(keyTo, to);

            return new IndexToBaseStoreIterator(new SegmentIterator<>(
                searchSpace.iterator(),
                indexKeySchema.get().hasNextCondition(keyFrom, keyTo, from, to, forward),
                binaryFrom,
                binaryTo,
                forward));
        }

        final List<KeyValueSegment> searchSpace = baseKeySchema.segmentsToSearch(segments, from, to,
            forward);

        final Bytes binaryFrom = baseKeySchema.lowerRange(keyFrom, from);
        final Bytes binaryTo = baseKeySchema.upperRange(keyTo, to);

        return new SegmentIterator<>(
            searchSpace.iterator(),
            baseKeySchema.hasNextCondition(keyFrom, keyTo, from, to, forward),
            binaryFrom,
            binaryTo,
            forward);
    }


    @Override
    public void remove(final Bytes key, final long timestamp) {
        throw new UnsupportedOperationException("Not supported operation");
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetchAll(final long timeFrom,
                                                    final long timeTo) {
        final List<KeyValueSegment> searchSpace = segments.segments(timeFrom, timeTo, true);
        final Bytes binaryFrom = baseKeySchema.lowerRange(null, timeFrom);
        final Bytes binaryTo = baseKeySchema.upperRange(null, timeTo);

        return new SegmentIterator<>(
                searchSpace.iterator(),
                baseKeySchema.hasNextCondition(null, null, timeFrom, timeTo, true),
                binaryFrom,
                binaryTo,
                true);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> backwardFetchAll(final long timeFrom,
                                                            final long timeTo) {
        final List<KeyValueSegment> searchSpace = segments.segments(timeFrom, timeTo, false);
        final Bytes binaryFrom = baseKeySchema.lowerRange(null, timeFrom);
        final Bytes binaryTo = baseKeySchema.upperRange(null, timeTo);

        return new SegmentIterator<>(
                searchSpace.iterator(),
                baseKeySchema.hasNextCondition(null, null, timeFrom, timeTo, false),
                binaryFrom,
                binaryTo,
                false);
    }
}