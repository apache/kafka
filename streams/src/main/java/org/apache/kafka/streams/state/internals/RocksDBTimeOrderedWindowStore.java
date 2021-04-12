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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

/**
 * A persistent (time-key)-value store based on RocksDB.
 *
 * The store uses the {@link TimeOrderedKeySchema} to serialize the record key bytes to generate the
 * combined (time-key) store key. This key schema is efficient when doing time range queries in
 * the store (i.e. fetchAll(from, to) ).
 *
 * For key range queries, like fetch(key, fromTime, toTime), use the {@link RocksDBWindowStore}
 * which uses the {@link WindowKeySchema} to serialize the record bytes for efficient key queries.
 */
public class RocksDBTimeOrderedWindowStore
    extends WrappedStateStore<SegmentedBytesStore, Object, Object>
    implements WindowStore<Bytes, byte[]> {

    private final boolean retainDuplicates;
    private final long windowSize;

    private int seqnum = 0;

    RocksDBTimeOrderedWindowStore(final SegmentedBytesStore bytesStore,
                                  final boolean retainDuplicates,
                                  final long windowSize) {
        super(bytesStore);
        this.retainDuplicates = retainDuplicates;
        this.windowSize = windowSize;
    }

    @Override
    public void put(final Bytes key, final byte[] value, final long timestamp) {
        // Skip if value is null and duplicates are allowed since this delete is a no-op
        if (!(value == null && retainDuplicates)) {
            maybeUpdateSeqnumForDups();
            wrapped().put(TimeOrderedKeySchema.toStoreKeyBinary(key, timestamp, seqnum), value);
        }
    }

    @Override
    public byte[] fetch(final Bytes key, final long timestamp) {
        return wrapped().get(TimeOrderedKeySchema.toStoreKeyBinary(key, timestamp, seqnum));
    }

    @SuppressWarnings("deprecation") // note, this method must be kept if super#fetch(...) is removed
    @Override
    public WindowStoreIterator<byte[]> fetch(final Bytes key, final long timeFrom, final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetch(key, timeFrom, timeTo);
        return new TimeOrderedWindowStoreIteratorWrapper(bytesIterator, windowSize).valuesIterator();
    }

    @Override
    public WindowStoreIterator<byte[]> backwardFetch(final Bytes key, final long timeFrom, final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().backwardFetch(key, timeFrom, timeTo);
        return new TimeOrderedWindowStoreIteratorWrapper(bytesIterator, windowSize).valuesIterator();
    }

    @SuppressWarnings("deprecation") // note, this method must be kept if super#fetch(...) is removed
    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom,
                                                           final Bytes keyTo,
                                                           final long timeFrom,
                                                           final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetch(keyFrom, keyTo, timeFrom, timeTo);
        return new TimeOrderedWindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom,
                                                                   final Bytes keyTo,
                                                                   final long timeFrom,
                                                                   final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
        return new TimeOrderedWindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().all();
        return new TimeOrderedWindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().backwardAll();
        return new TimeOrderedWindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator();
    }

    @SuppressWarnings("deprecation") // note, this method must be kept if super#fetchAll(...) is removed
    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom, final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetchAll(timeFrom, timeTo);
        return new TimeOrderedWindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final long timeFrom, final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().backwardFetchAll(timeFrom, timeTo);
        return new TimeOrderedWindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator();
    }

    private void maybeUpdateSeqnumForDups() {
        if (retainDuplicates) {
            seqnum = (seqnum + 1) & 0x7FFFFFFF;
        }
    }

    static class TimeOrderedWindowStoreIteratorWrapper {
        private final KeyValueIterator<Bytes, byte[]> bytesIterator;
        private final long windowSize;

        TimeOrderedWindowStoreIteratorWrapper(final KeyValueIterator<Bytes, byte[]> bytesIterator,
                                              final long windowSize) {
            this.bytesIterator = bytesIterator;
            this.windowSize = windowSize;
        }

        public WindowStoreIterator<byte[]> valuesIterator() {
            return new WrappedWindowStoreIterator(bytesIterator);
        }

        public KeyValueIterator<Windowed<Bytes>, byte[]> keyValueIterator() {
            return new WrappedKeyValueIterator(bytesIterator, windowSize);
        }

        private static class WrappedWindowStoreIterator implements WindowStoreIterator<byte[]> {
            final KeyValueIterator<Bytes, byte[]> bytesIterator;

            WrappedWindowStoreIterator(
                final KeyValueIterator<Bytes, byte[]> bytesIterator) {
                this.bytesIterator = bytesIterator;
            }

            @Override
            public Long peekNextKey() {
                return TimeOrderedKeySchema.extractStoreTimestamp(bytesIterator.peekNextKey().get());
            }

            @Override
            public boolean hasNext() {
                return bytesIterator.hasNext();
            }

            @Override
            public KeyValue<Long, byte[]> next() {
                final KeyValue<Bytes, byte[]> next = bytesIterator.next();
                final long timestamp = TimeOrderedKeySchema.extractStoreTimestamp(next.key.get());
                return KeyValue.pair(timestamp, next.value);
            }

            @Override
            public void close() {
                bytesIterator.close();
            }
        }

        private static class WrappedKeyValueIterator implements KeyValueIterator<Windowed<Bytes>, byte[]> {
            final KeyValueIterator<Bytes, byte[]> bytesIterator;
            final long windowSize;

            WrappedKeyValueIterator(final KeyValueIterator<Bytes, byte[]> bytesIterator,
                                    final long windowSize) {
                this.bytesIterator = bytesIterator;
                this.windowSize = windowSize;
            }

            @Override
            public Windowed<Bytes> peekNextKey() {
                final byte[] nextKey = bytesIterator.peekNextKey().get();
                return TimeOrderedKeySchema.fromStoreBytesKey(nextKey, windowSize);
            }

            @Override
            public boolean hasNext() {
                return bytesIterator.hasNext();
            }

            @Override
            public KeyValue<Windowed<Bytes>, byte[]> next() {
                final KeyValue<Bytes, byte[]> next = bytesIterator.next();
                return KeyValue.pair(TimeOrderedKeySchema.fromStoreBytesKey(next.key.get(), windowSize), next.value);
            }

            @Override
            public void close() {
                bytesIterator.close();
            }
        }
    }
}
