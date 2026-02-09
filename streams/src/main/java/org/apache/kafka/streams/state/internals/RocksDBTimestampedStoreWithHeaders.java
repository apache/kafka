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
import org.apache.kafka.streams.state.HeadersBytesStore;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.apache.kafka.streams.state.HeadersBytesStore.convertToHeaderFormat;

/**
 * A persistent key-(value-timestamp-headers) store based on RocksDB for use in window/session segments.
 * <p>
 * This store implements segment-level versioning:
 * - Legacy segments (created before upgrade) use TIMESTAMPED_VALUES_CF with format: [timestamp(8)][value]
 * - New segments (created after upgrade) use HEADERS_CF with format: [headersSize(varint)][headersBytes][timestamp(8)][value]
 * <p>
 * Unlike RocksDBHeadersStore which uses dual-CF within each store for lazy migration,
 * this implementation avoids dual-CF complexity by maintaining format consistency at the segment level.
 * Old segments naturally expire based on retention policy.
 */
public class RocksDBTimestampedStoreWithHeaders extends RocksDBStore implements HeadersBytesStore {
    private static final Logger log = LoggerFactory.getLogger(RocksDBTimestampedStoreWithHeaders.class);

    protected static final byte[] TIMESTAMPED_VALUES_COLUMN_FAMILY_NAME = "keyValueWithTimestamp".getBytes(StandardCharsets.UTF_8);
    protected static final byte[] HEADERS_COLUMN_FAMILY_NAME = "keyValueWithHeaders".getBytes(StandardCharsets.UTF_8);

    private boolean isLegacySegment = false;

    RocksDBTimestampedStoreWithHeaders(final String name,
                                        final String parentDir,
                                        final RocksDBMetricsRecorder metricsRecorder) {
        super(name, parentDir, metricsRecorder);
    }

    @Override
    void openRocksDB(final DBOptions dbOptions,
                     final ColumnFamilyOptions columnFamilyOptions) {
        // Attempt to open both column families to detect segment format
        final List<ColumnFamilyHandle> columnFamilies = openRocksDB(
                dbOptions,
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
                new ColumnFamilyDescriptor(TIMESTAMPED_VALUES_COLUMN_FAMILY_NAME, columnFamilyOptions),
                new ColumnFamilyDescriptor(HEADERS_COLUMN_FAMILY_NAME, columnFamilyOptions)
        );

        final ColumnFamilyHandle defaultColumnFamily = columnFamilies.get(0);
        final ColumnFamilyHandle timestampedColumnFamily = columnFamilies.get(1);
        final ColumnFamilyHandle headersColumnFamily = columnFamilies.get(2);

        // Check if this is a legacy segment (has data in timestamped CF)
        final RocksIterator timestampedIter = db.newIterator(timestampedColumnFamily);
        timestampedIter.seekToFirst();

        if (timestampedIter.isValid()) {
            // This is a legacy segment - use timestamped CF only
            log.info("Opening segment {} as legacy format (timestamped values only)", name);
            isLegacySegment = true;
            cfAccessor = new LegacySegmentAccessor(timestampedColumnFamily);
            defaultColumnFamily.close();
            headersColumnFamily.close();
        } else {
            // This is a new segment - use headers CF only
            log.info("Opening segment {} as new format (with headers)", name);
            isLegacySegment = false;
            cfAccessor = new SingleColumnFamilyAccessor(headersColumnFamily);
            defaultColumnFamily.close();
            timestampedColumnFamily.close();
        }
        timestampedIter.close();
    }

    /**
     * Accessor for legacy segments that only contain timestamped values without headers.
     * Reads return values converted to header format with empty headers.
     * Writes are not supported as legacy segments should be read-only.
     */
    private class LegacySegmentAccessor implements ColumnFamilyAccessor {
        private final ColumnFamilyHandle columnFamily;

        private LegacySegmentAccessor(final ColumnFamilyHandle columnFamily) {
            this.columnFamily = columnFamily;
        }

        @Override
        public void put(final DBAccessor accessor,
                        final byte[] key,
                        final byte[] valueWithHeaders) {
            // Legacy segments should not receive new writes
            // New data goes to new segments which use the headers CF
            throw new UnsupportedOperationException(
                "Cannot write to legacy segment. This indicates a bug - writes should go to new segments only."
            );
        }

        @Override
        public void prepareBatch(final java.util.List<org.apache.kafka.streams.KeyValue<Bytes, byte[]>> entries,
                                 final org.rocksdb.WriteBatchInterface batch) {
            throw new UnsupportedOperationException("Batch operations not supported on legacy segments");
        }

        @Override
        public byte[] get(final DBAccessor accessor, final byte[] key) throws RocksDBException {
            final byte[] timestampedValue = accessor.get(columnFamily, key);
            if (timestampedValue == null) {
                return null;
            }
            // Convert legacy [timestamp][value] to [headersSize(0)][timestamp][value]
            return convertToHeaderFormat(key, timestampedValue);
        }

        @Override
        public byte[] get(final DBAccessor accessor, final byte[] key, final org.rocksdb.ReadOptions readOptions) throws RocksDBException {
            final byte[] timestampedValue = accessor.get(columnFamily, readOptions, key);
            if (timestampedValue == null) {
                return null;
            }
            // Convert legacy [timestamp][value] to [headersSize(0)][timestamp][value]
            return convertToHeaderFormat(key, timestampedValue);
        }

        @Override
        public byte[] getOnly(final DBAccessor accessor, final byte[] key) throws RocksDBException {
            return get(accessor, key);
        }

        @Override
        public ManagedKeyValueIterator<Bytes, byte[]> range(final DBAccessor accessor,
                                                             final Bytes from,
                                                             final Bytes to,
                                                             final boolean forward) {
            // For legacy segments, iterator operations are read-only
            // We create a converting iterator wrapper
            final RocksDBRangeIterator baseIterator = new RocksDBRangeIterator(
                name,
                accessor.newIterator(columnFamily),
                from,
                to,
                forward,
                true
            );
            return new ConvertingIterator(baseIterator);
        }

        @Override
        public void deleteRange(final DBAccessor accessor, final byte[] from, final byte[] to) {
            throw new UnsupportedOperationException("Delete range not supported on legacy segments");
        }

        @Override
        public ManagedKeyValueIterator<Bytes, byte[]> all(final DBAccessor accessor, final boolean forward) {
            final RocksIterator innerIter = accessor.newIterator(columnFamily);
            if (forward) {
                innerIter.seekToFirst();
            } else {
                innerIter.seekToLast();
            }
            final RocksDbIterator baseIterator = new RocksDbIterator(name, innerIter, forward);
            return new ConvertingIterator(baseIterator);
        }

        @Override
        public ManagedKeyValueIterator<Bytes, byte[]> prefixScan(final DBAccessor accessor, final Bytes prefix) {
            final Bytes to = RocksDBStore.incrementWithoutOverflow(prefix);
            final RocksDBRangeIterator baseIterator = new RocksDBRangeIterator(
                name,
                accessor.newIterator(columnFamily),
                prefix,
                to,
                true,
                false
            );
            return new ConvertingIterator(baseIterator);
        }

        /**
         * Iterator wrapper that converts legacy timestamped values to header format on-the-fly.
         */
        private class ConvertingIterator implements ManagedKeyValueIterator<Bytes, byte[]> {
            private final ManagedKeyValueIterator<Bytes, byte[]> inner;

            ConvertingIterator(final ManagedKeyValueIterator<Bytes, byte[]> inner) {
                this.inner = inner;
            }

            @Override
            public boolean hasNext() {
                return inner.hasNext();
            }

            @Override
            public org.apache.kafka.streams.KeyValue<Bytes, byte[]> next() {
                final org.apache.kafka.streams.KeyValue<Bytes, byte[]> kv = inner.next();
                // Convert [timestamp][value] to [headersSize(0)][timestamp][value]
                final byte[] convertedValue = convertToHeaderFormat(kv.key.get(), kv.value);
                return new org.apache.kafka.streams.KeyValue<>(kv.key, convertedValue);
            }

            @Override
            public void close() {
                inner.close();
            }

            @Override
            public Bytes peekNextKey() {
                return inner.peekNextKey();
            }

            @Override
            public void onClose(final Runnable closeCallback) {
                inner.onClose(closeCallback);
            }
        }

        @Override
        public long approximateNumEntries(final DBAccessor accessor) throws RocksDBException {
            return accessor.approximateNumEntries(columnFamily);
        }

        @Override
        public void flush(final DBAccessor accessor) throws RocksDBException {
            accessor.flush(columnFamily);
        }

        @Override
        public void addToBatch(final byte[] key,
                               final byte[] value,
                               final org.rocksdb.WriteBatchInterface batch) {
            throw new UnsupportedOperationException("Batch operations not supported on legacy segments");
        }

        @Override
        public void close() {
            columnFamily.close();
        }
    }

    /**
     * Check if this segment is in legacy format.
     * Exposed for testing and metrics.
     */
    boolean isLegacySegment() {
        return isLegacySegment;
    }
}
