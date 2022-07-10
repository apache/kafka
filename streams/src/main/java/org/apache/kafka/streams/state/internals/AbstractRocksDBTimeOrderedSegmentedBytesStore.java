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

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public abstract class AbstractRocksDBTimeOrderedSegmentedBytesStore extends AbstractDualSchemaRocksDBSegmentedBytesStore<KeyValueSegment> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractDualSchemaRocksDBSegmentedBytesStore.class);

    abstract class IndexToBaseStoreIterator implements KeyValueIterator<Bytes, byte[]> {
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

        abstract protected Bytes getBaseKey(final Bytes indexKey);
    }

    AbstractRocksDBTimeOrderedSegmentedBytesStore(final String name,
                                                  final String metricsScope,
                                                  final long retention,
                                                  final long segmentInterval,
                                                  final KeySchema baseKeySchema,
                                                  final Optional<KeySchema> indexKeySchema) {
        super(name, baseKeySchema, indexKeySchema,
            new KeyValueSegments(name, metricsScope, retention, segmentInterval), retention);
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

    abstract protected IndexToBaseStoreIterator getIndexToBaseStoreIterator(final SegmentIterator<KeyValueSegment> segmentIterator);

    KeyValueIterator<Bytes, byte[]> fetch(final Bytes key,
                                          final long from,
                                          final long to,
                                          final boolean forward) {

        final long actualFrom = getActualFrom(from, baseKeySchema instanceof PrefixedWindowKeySchemas.TimeFirstWindowKeySchema);

        if (baseKeySchema instanceof PrefixedWindowKeySchemas.TimeFirstWindowKeySchema && to < actualFrom) {
            return KeyValueIterators.emptyIterator();
        }

        if (indexKeySchema.isPresent()) {
            final List<KeyValueSegment> searchSpace = indexKeySchema.get().segmentsToSearch(segments, actualFrom, to,
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


        final List<KeyValueSegment> searchSpace = baseKeySchema.segmentsToSearch(segments, actualFrom, to,
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
            final List<KeyValueSegment> searchSpace = indexKeySchema.get().segmentsToSearch(segments, actualFrom, to,
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

        final List<KeyValueSegment> searchSpace = baseKeySchema.segmentsToSearch(segments, actualFrom, to,
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
    public void remove(final Bytes key, final long timestamp) {
        throw new UnsupportedOperationException("Not supported operation");
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetchAll(final long timeFrom,
                                                    final long timeTo) {

        final long actualFrom = getActualFrom(timeFrom, baseKeySchema instanceof PrefixedWindowKeySchemas.TimeFirstWindowKeySchema);

        if (baseKeySchema instanceof PrefixedWindowKeySchemas.TimeFirstWindowKeySchema && timeTo < actualFrom) {
            return KeyValueIterators.emptyIterator();
        }

        final List<KeyValueSegment> searchSpace = segments.segments(actualFrom, timeTo, true);
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

        final List<KeyValueSegment> searchSpace = segments.segments(actualFrom, timeTo, false);
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