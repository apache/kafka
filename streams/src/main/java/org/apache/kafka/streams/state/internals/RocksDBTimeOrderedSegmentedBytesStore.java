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
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.KeyFirstWindowKeySchema;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.TimeFirstWindowKeySchema;
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
        return indexIterator.peekNextKey();
        }

        @Override
        public boolean hasNext() {
            while (indexIterator.hasNext()) {
                final Bytes key = indexIterator.peekNextKey();
                final Bytes keyBytes = Bytes.wrap(KeyFirstWindowKeySchema.extractStoreKeyBytes(key.get()));
                final long timestamp = KeyFirstWindowKeySchema.extractStoreTimestamp(key.get());
                final int seqnum = KeyFirstWindowKeySchema.extractStoreSeqnum(key.get());

                cachedValue = get(TimeFirstWindowKeySchema.toTimeOrderedStoreKeyBinary(keyBytes, timestamp, seqnum));
                if (cachedValue == null) {
                    // Key not in base store, inconsistency happened and remove from index.
                    indexIterator.next();
                    // TODO: check if this works or not...
                    RocksDBTimeOrderedSegmentedBytesStore.this.remove(key);
                } else {
                    return true;
                }
            }
            return false;
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            KeyValue<Bytes, byte[]> ret = indexIterator.next();
            return KeyValue.pair(ret.key, cachedValue);
        }
    }

    RocksDBTimeOrderedSegmentedBytesStore(final String name,
                                          final String metricsScope,
                                          final long retention,
                                          final long segmentInterval) {
        super(name, metricsScope, new TimeFirstWindowKeySchema(), Optional.of(new KeyFirstWindowKeySchema()),
            new KeyValueSegments(name, metricsScope, retention, segmentInterval));
    }

    void put(final Bytes key, final long timestamp, final int seqnum, final byte[] value) {
        final Bytes baseKey = TimeFirstWindowKeySchema.toTimeOrderedStoreKeyBinary(key, timestamp, seqnum);
        final Bytes indexKey = KeyFirstWindowKeySchema.toStoreKeyBinary(key, timestamp, seqnum);

        put(indexKey, value);
        put(baseKey, value);
    }

    byte[] fetch(final Bytes key, final long timestamp, final int seqnum) {
        return get(TimeFirstWindowKeySchema.toTimeOrderedStoreKeyBinary(key, timestamp, seqnum));
    }

    @Override
    Map<KeyValueSegment, WriteBatch> getWriteBatches(
        final Collection<ConsumerRecord<byte[], byte[]>> records) {
        // TODO:
        return null;
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
                indexKeySchema.get().hasNextCondition(key, key, from, to),
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
            baseKeySchema.hasNextCondition(key, key, from, to),
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
                indexKeySchema.get().hasNextCondition(keyFrom, keyTo, from, to),
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
            baseKeySchema.hasNextCondition(keyFrom, keyTo, from, to),
            binaryFrom,
            binaryTo,
            forward);
    }

    @Override
    public void remove(final Bytes key, final long timestamp) {
        final Bytes baseKeyBytes = baseKeySchema.toStoreBinaryKeyPrefix(key, timestamp);
        final KeyValueSegment segment = segments.getSegmentForTimestamp(timestamp);
        if (segment != null) {
            segment.deleteRange(baseKeyBytes, baseKeyBytes);
        }
        if (segment != null && hasIndex()) {
            final Bytes keyBytes = baseKeySchema.toStoreBinaryKeyPrefix(key, timestamp);
            segment.deleteRange(keyBytes, keyBytes);
        }
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetchAll(final long timeFrom,
                                                    final long timeTo) {
        final List<KeyValueSegment> searchSpace = segments.segments(timeFrom, timeTo, true);
        final Bytes binaryFrom = baseKeySchema.lowerRange(null, timeFrom);
        final Bytes binaryTo = baseKeySchema.upperRange(null, timeTo);

        return new SegmentIterator<>(
                searchSpace.iterator(),
                baseKeySchema.hasNextCondition(null, null, timeFrom, timeTo),
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
                baseKeySchema.hasNextCondition(null, null, timeFrom, timeTo),
                binaryFrom,
                binaryTo,
                false);
    }
}