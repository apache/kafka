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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.internals.ChangelogRecordDeserializationHelper;
import org.apache.kafka.streams.state.KeyValueIterator;

import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;

/**
 * RocksDB store backed by two SegmentedBytesStores which can optimize scan by time as well as window
 * lookup for a specific key.
 *
 * Schema for first SegmentedBytesStore (base store) is as below:
 *     Key schema: | timestamp + [timestamp] + recordkey |
 *     Value schema: | value |. Value here is determined by caller.
 *
 * Schema for second SegmentedBytesStore (index store) is as below:
 *     Key schema: | record + timestamp + [timestamp]|
 *     Value schema: ||
 *
 * Note there could be two timestamps if we store both window end time and window start time.
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
 * @see RocksDBTimeOrderedSessionSegmentedBytesStore
 * @see RocksDBTimeOrderedWindowSegmentedBytesStore
 */
public abstract class AbstractRocksDBTimeOrderedSegmentedBytesStore<S extends Segment> extends AbstractDualSchemaRocksDBSegmentedBytesStore<S> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractRocksDBTimeOrderedSegmentedBytesStore.class);

    private long minTimestamp;

    public abstract class IndexToBaseStoreIterator implements KeyValueIterator<Bytes, byte[]> {
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
                    // Key not in base store or key is expired, inconsistency happened and remove from index.
                    indexIterator.next();
                    AbstractRocksDBTimeOrderedSegmentedBytesStore.this.removeIndex(key);
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

        protected abstract Bytes getBaseKey(final Bytes indexKey);
    }

    AbstractRocksDBTimeOrderedSegmentedBytesStore(final String name,
                                                  final long retention,
                                                  final KeySchema baseKeySchema,
                                                  final Optional<KeySchema> indexKeySchema,
                                                  final AbstractSegments<S> segments) {
        super(name, baseKeySchema, indexKeySchema, segments, retention);

        minTimestamp = Long.MAX_VALUE;
    }

    Map<S, WriteBatch> getWriteBatches(
        final Collection<ConsumerRecord<byte[], byte[]>> records,
        final Function<byte[], Long> timestampExtractor,
        final Function<byte[], byte[]> indexedKeyExtractor,
        final Function<byte[], byte[]> baseKeyExtractor
    ) {
        // advance stream time to the max timestamp in the batch
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            final long timestamp = timestampExtractor.apply(record.key());
            minTimestamp = Math.min(minTimestamp, timestamp);
            observedStreamTime = Math.max(observedStreamTime, timestamp);
        }

        final Map<S, WriteBatch> writeBatchMap = new HashMap<>();
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            final long timestamp = timestampExtractor.apply(record.key());
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

                    // Assuming changelog record is serialized using SessionKeySchema
                    // from ChangeLoggingSessionBytesStore. Reconstruct key/value to restore
                    if (hasIndex()) {
                        final byte[] indexKey = indexedKeyExtractor.apply(record.key());
                        // Take care of tombstone
                        final byte[] value = record.value() == null ? null : new byte[0];
                        segment.addToBatch(new KeyValue<>(indexKey, value), batch);
                    }

                    final byte[] baseKey = baseKeyExtractor.apply(record.key());
                    segment.addToBatch(new KeyValue<>(baseKey, record.value()), batch);
                } catch (final RocksDBException e) {
                    throw new ProcessorStateException("Error restoring batch to store " + name(), e);
                }
            }
        }
        return writeBatchMap;
    }

    protected long minTimestamp() {
        return minTimestamp;
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

    protected abstract IndexToBaseStoreIterator getIndexToBaseStoreIterator(final SegmentIterator<S> segmentIterator);

    public void put(final Bytes key, final long timestamp, final int seqnum, final byte[] value) {
        throw new UnsupportedOperationException("This store does not support put with timestamp and seqnum");
    }

    public byte[] fetch(final Bytes key, final long timestamp, final int seqnum) {
        throw new UnsupportedOperationException("This store does not support fetch with timestamp and seqnum");
    }

    public byte[] fetchSession(final Bytes key, final long sessionStartTime, final long sessionEndTime) {
        throw new UnsupportedOperationException("This store does not support fetchSession");
    }

    public KeyValueIterator<Bytes, byte[]> fetchSessions(final long earliestSessionEndTime, final long latestSessionEndTime) {
        throw new UnsupportedOperationException("This store does not support fetchSessions");
    }

    public void remove(final Windowed<Bytes> key) {
        throw new UnsupportedOperationException("This store does not support remove with Windowed key");
    }

    public void put(final Windowed<Bytes> sessionKey, final byte[] aggregate) {
        throw new UnsupportedOperationException("This store does not support put with Windowed key");
    }

    KeyValueIterator<Bytes, byte[]> fetch(final Bytes key,
                                          final long from,
                                          final long to,
                                          final boolean forward) {

        final long actualFrom = getActualFrom(from, baseKeySchema instanceof PrefixedWindowKeySchemas.TimeFirstWindowKeySchema);

        if (baseKeySchema instanceof PrefixedWindowKeySchemas.TimeFirstWindowKeySchema && to < actualFrom) {
            return KeyValueIterators.emptyIterator();
        }

        if (indexKeySchema.isPresent()) {
            final List<S> searchSpace = indexKeySchema.get().segmentsToSearch(segments, actualFrom, to,
                forward);

            final Bytes binaryFrom = indexKeySchema.get().lowerRangeFixedSize(key, actualFrom);
            final Bytes binaryTo = indexKeySchema.get().upperRangeFixedSize(key, to);

            return getIndexToBaseStoreIterator(new SegmentIterator<>(
                searchSpace.iterator(),
                indexKeySchema.get().hasNextCondition(key, key, actualFrom, to, forward),
                binaryFrom,
                binaryTo,
                forward));
        }


        final List<S> searchSpace = baseKeySchema.segmentsToSearch(segments, actualFrom, to,
            forward);

        final Bytes binaryFrom = baseKeySchema.lowerRangeFixedSize(key, actualFrom);
        final Bytes binaryTo = baseKeySchema.upperRangeFixedSize(key, to);

        return new SegmentIterator<>(
            searchSpace.iterator(),
            baseKeySchema.hasNextCondition(key, key, actualFrom, to, forward),
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

        final long actualFrom = getActualFrom(from, baseKeySchema instanceof PrefixedWindowKeySchemas.TimeFirstWindowKeySchema);

        if (baseKeySchema instanceof PrefixedWindowKeySchemas.TimeFirstWindowKeySchema && to < actualFrom) {
            return KeyValueIterators.emptyIterator();
        }

        if (indexKeySchema.isPresent()) {
            final List<S> searchSpace = indexKeySchema.get().segmentsToSearch(segments, actualFrom, to,
                forward);

            final Bytes binaryFrom = indexKeySchema.get().lowerRange(keyFrom, actualFrom);
            final Bytes binaryTo = indexKeySchema.get().upperRange(keyTo, to);

            return getIndexToBaseStoreIterator(new SegmentIterator<>(
                searchSpace.iterator(),
                indexKeySchema.get().hasNextCondition(keyFrom, keyTo, actualFrom, to, forward),
                binaryFrom,
                binaryTo,
                forward));
        }

        final List<S> searchSpace = baseKeySchema.segmentsToSearch(segments, actualFrom, to,
            forward);

        final Bytes binaryFrom = baseKeySchema.lowerRange(keyFrom, actualFrom);
        final Bytes binaryTo = baseKeySchema.upperRange(keyTo, to);

        return new SegmentIterator<>(
            searchSpace.iterator(),
            baseKeySchema.hasNextCondition(keyFrom, keyTo, actualFrom, to, forward),
            binaryFrom,
            binaryTo,
            forward);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetchAll(final long timeFrom,
                                                    final long timeTo) {

        final long actualFrom = getActualFrom(timeFrom, baseKeySchema instanceof PrefixedWindowKeySchemas.TimeFirstWindowKeySchema);

        if (baseKeySchema instanceof PrefixedWindowKeySchemas.TimeFirstWindowKeySchema && timeTo < actualFrom) {
            return KeyValueIterators.emptyIterator();
        }

        final List<S> searchSpace = segments.segments(actualFrom, timeTo, true);
        final Bytes binaryFrom = baseKeySchema.lowerRange(null, actualFrom);
        final Bytes binaryTo = baseKeySchema.upperRange(null, timeTo);

        return new SegmentIterator<>(
                searchSpace.iterator(),
                baseKeySchema.hasNextCondition(null, null, actualFrom, timeTo, true),
                binaryFrom,
                binaryTo,
                true);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> backwardFetchAll(final long timeFrom,
                                                            final long timeTo) {

        final long actualFrom = getActualFrom(timeFrom, baseKeySchema instanceof PrefixedWindowKeySchemas.TimeFirstWindowKeySchema);

        if (baseKeySchema instanceof PrefixedWindowKeySchemas.TimeFirstWindowKeySchema && timeTo < actualFrom) {
            return KeyValueIterators.emptyIterator();
        }

        final List<S> searchSpace = segments.segments(actualFrom, timeTo, false);
        final Bytes binaryFrom = baseKeySchema.lowerRange(null, actualFrom);
        final Bytes binaryTo = baseKeySchema.upperRange(null, timeTo);

        return new SegmentIterator<>(
                searchSpace.iterator(),
                baseKeySchema.hasNextCondition(null, null, actualFrom, timeTo, false),
                binaryFrom,
                binaryTo,
                false);
    }
}
