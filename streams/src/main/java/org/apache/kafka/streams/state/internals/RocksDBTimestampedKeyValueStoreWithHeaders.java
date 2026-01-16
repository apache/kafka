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
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.state.HeaderBytesStore;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.apache.kafka.streams.state.internals.ValueAndTimestampDeserializer.rawValue;
import static org.apache.kafka.streams.state.internals.ValueAndTimestampDeserializer.timestamp;

/**
 * This a PoC for 2CF solution
 * A RocksDB-backed key-value store that stores values with timestamp and headers.
 *
 * Uses dual column families for lazy migration from legacy format:
 * - oldColumnFamily (default): stores legacy format [Timestamp(8)][Payload]
 * - newColumnFamily (keyValueWithHeaders): stores new format [HeaderSize(2)][Headers][Timestamp(8)][Payload]
 *
 * Migration strategy:
 * - On read: check newColumnFamily first, fallback to oldColumnFamily
 * - If found in oldColumnFamily: convert to new format and migrate to newColumnFamily
 * - On write: always write to newColumnFamily
 */
public class RocksDBTimestampedKeyValueStoreWithHeaders extends RocksDBStore implements HeaderBytesStore {

    private static final byte[] HEADERS_COLUMN_FAMILY_NAME = "keyValueWithHeaders".getBytes(StandardCharsets.UTF_8);

    public RocksDBTimestampedKeyValueStoreWithHeaders(final String name,
                                                      final String metricsScope) {
        super(name, metricsScope);
    }

    @Override
    void openRocksDB(final DBOptions dbOptions,
                     final ColumnFamilyOptions columnFamilyOptions) {
        final List<ColumnFamilyHandle> columnFamilies = openRocksDB(
            dbOptions,
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
            new ColumnFamilyDescriptor(HEADERS_COLUMN_FAMILY_NAME, columnFamilyOptions)
        );

        final ColumnFamilyHandle oldColumnFamily = columnFamilies.get(0);
        final ColumnFamilyHandle newColumnFamily = columnFamilies.get(1);

        // Check if there's any data in oldColumnFamily (upgrade mode)
        final RocksIterator oldIter = db.newIterator(oldColumnFamily);
        oldIter.seekToFirst();
        if (oldIter.isValid()) {
            log.info("Opening store {} in upgrade mode (dual column family)", name);
            cfAccessor = new DualColumnFamilyAccessor(oldColumnFamily, newColumnFamily);
        } else {
            log.info("Opening store {} in regular mode (single column family)", name);
            cfAccessor = new SingleColumnFamilyAccessor(newColumnFamily);
            oldColumnFamily.close();
        }
        oldIter.close();
    }

    /**
     * Dual column family accessor for upgrade mode.
     * Manages migration from oldColumnFamily (legacy format) to newColumnFamily (with headers).
     */
    private class DualColumnFamilyAccessor implements ColumnFamilyAccessor {
        private final ColumnFamilyHandle oldColumnFamily;
        private final ColumnFamilyHandle newColumnFamily;

        private DualColumnFamilyAccessor(final ColumnFamilyHandle oldColumnFamily,
                                         final ColumnFamilyHandle newColumnFamily) {
            this.oldColumnFamily = oldColumnFamily;
            this.newColumnFamily = newColumnFamily;
        }

        @Override
        public void put(final DBAccessor accessor,
                       final byte[] key,
                       final byte[] valueWithHeadersAndTimestamp) {
            synchronized (position) {
                if (valueWithHeadersAndTimestamp == null) {
                    // Delete from both column families
                    try {
                        accessor.delete(oldColumnFamily, key);
                    } catch (final RocksDBException e) {
                        throw new ProcessorStateException("Error while removing key from old CF in store " + name, e);
                    }
                    try {
                        accessor.delete(newColumnFamily, key);
                    } catch (final RocksDBException e) {
                        throw new ProcessorStateException("Error while removing key from new CF in store " + name, e);
                    }
                } else {
                    // Delete from old CF (if exists), write to new CF
                    try {
                        accessor.delete(oldColumnFamily, key);
                    } catch (final RocksDBException e) {
                        throw new ProcessorStateException("Error while removing key from old CF in store " + name, e);
                    }
                    try {
                        accessor.put(newColumnFamily, key, valueWithHeadersAndTimestamp);
                        StoreQueryUtils.updatePosition(position, context);
                    } catch (final RocksDBException e) {
                        throw new ProcessorStateException("Error while putting key/value into new CF in store " + name, e);
                    }
                }
            }
        }

        @Override
        public void prepareBatch(final List<KeyValue<Bytes, byte[]>> entries,
                                final WriteBatch batch) throws RocksDBException {
            for (final KeyValue<Bytes, byte[]> entry : entries) {
                Objects.requireNonNull(entry.key, "key cannot be null");
                addToBatch(entry.key.get(), entry.value, batch);
            }
        }

        private void addToBatch(final byte[] key,
                               final byte[] value,
                               final WriteBatch batch) throws RocksDBException {
            if (value == null) {
                batch.delete(oldColumnFamily, key);
                batch.delete(newColumnFamily, key);
            } else {
                batch.delete(oldColumnFamily, key);
                batch.put(newColumnFamily, key, value);
            }
        }

        @Override
        public byte[] get(final DBAccessor accessor, final byte[] key) throws RocksDBException {
            return get(accessor, key, null);
        }

        @Override
        public byte[] get(final DBAccessor accessor, final byte[] key, final ReadOptions readOptions) throws RocksDBException {
            return get(accessor, key, readOptions);
        }

        private byte[] get(final DBAccessor accessor,
                          final byte[] key,
                          final ReadOptions readOptions) throws RocksDBException {
            // Try newColumnFamily first
            final byte[] valueWithHeaders = readOptions != null
                ? accessor.get(newColumnFamily, readOptions, key)
                : accessor.get(newColumnFamily, key);

            if (valueWithHeaders != null) {
                return valueWithHeaders;  // Found in new format
            }

            // Fallback to oldColumnFamily (legacy format)
            final byte[] legacyValue = readOptions != null
                ? accessor.get(oldColumnFamily, readOptions, key)
                : accessor.get(oldColumnFamily, key);

            if (legacyValue != null) {
                // Convert legacy format to new format with empty headers
                final byte[] migratedValue = convertToHeaderFormat(legacyValue);

                // Migrate to newColumnFamily
                put(accessor, key, migratedValue);

                return migratedValue;
            }

            return null;  // Not found in either CF
        }

        @Override
        public byte[] getOnly(final DBAccessor accessor, final byte[] key) throws RocksDBException {
            // Check newColumnFamily first
            final byte[] valueWithHeaders = accessor.get(newColumnFamily, key);
            if (valueWithHeaders != null) {
                return valueWithHeaders;
            }

            // Check oldColumnFamily (without migration)
            final byte[] legacyValue = accessor.get(oldColumnFamily, key);
            if (legacyValue != null) {
                return convertToHeaderFormat(legacyValue);
            }

            return null;
        }

        @Override
        public ManagedKeyValueIterator<Bytes, byte[]> range(final DBAccessor accessor,
                                                            final Bytes from,
                                                            final Bytes to,
                                                            final boolean forward) {
            return new RocksDBDualCFRangeIterator(
                name,
                accessor.newIterator(newColumnFamily),
                accessor.newIterator(oldColumnFamily),
                from,
                to,
                forward,
                true
            );
        }

        @Override
        public ManagedKeyValueIterator<Bytes, byte[]> all(final DBAccessor accessor,
                                                          final boolean forward) {
            return new RocksDBDualCFRangeIterator(
                name,
                accessor.newIterator(newColumnFamily),
                accessor.newIterator(oldColumnFamily),
                null,
                null,
                forward,
                false
            );
        }

        @Override
        public long approximateNumEntries(final DBAccessor dbAccessor) throws RocksDBException {
            return dbAccessor.approximateNumEntries(oldColumnFamily) +
                   dbAccessor.approximateNumEntries(newColumnFamily);
        }

        @Override
        public void close() {
            oldColumnFamily.close();
            newColumnFamily.close();
        }

        /**
         * Converts legacy format to new format with empty headers.
         * Legacy: [Timestamp(8)][Payload]
         * New: [HeaderSize(2)][Headers(4)][Timestamp(8)][Payload]
         *
         * Empty headers format: [NumHeaders(4)=0] = 4 bytes
         */
        private byte[] convertToHeaderFormat(final byte[] legacyValue) {
            if (legacyValue == null) {
                return null;
            }

            // Parse legacy format
            final long timestamp = timestamp(legacyValue);
            final byte[] payload = rawValue(legacyValue);

            // Create new format with empty headers
            // Empty headers: [NumHeaders(4)=0] = 4 bytes
            final int emptyHeadersSize = 4;
            final int totalSize = 2 + emptyHeadersSize + 8 + (payload != null ? payload.length : 0);
            final ByteBuffer buffer = ByteBuffer.allocate(totalSize);

            buffer.putShort((short) emptyHeadersSize);   // HeaderSize = 4
            buffer.putInt(0);                      // NumHeaders = 0 (empty headers)
            buffer.putLong(timestamp);                  // Timestamp
            if (payload != null) {
                buffer.put(payload);                    // Payload
            }

            return buffer.array();
        }
    }

    /**
     * Single column family accessor for regular mode (no migration needed).
     */
    private class SingleColumnFamilyAccessor implements ColumnFamilyAccessor {
        private final ColumnFamilyHandle columnFamily;

        private SingleColumnFamilyAccessor(final ColumnFamilyHandle columnFamily) {
            this.columnFamily = columnFamily;
        }

        @Override
        public void put(final DBAccessor accessor,
                       final byte[] key,
                       final byte[] value) {
            synchronized (position) {
                try {
                    if (value == null) {
                        accessor.delete(columnFamily, key);
                    } else {
                        accessor.put(columnFamily, key, value);
                        StoreQueryUtils.updatePosition(position, context);
                    }
                } catch (final RocksDBException e) {
                    throw new ProcessorStateException("Error while executing put/delete in store " + name, e);
                }
            }
        }

        @Override
        public void prepareBatch(final List<KeyValue<Bytes, byte[]>> entries,
                                final WriteBatch batch) throws RocksDBException {
            for (final KeyValue<Bytes, byte[]> entry : entries) {
                Objects.requireNonNull(entry.key, "key cannot be null");
                if (entry.value == null) {
                    batch.delete(columnFamily, entry.key.get());
                } else {
                    batch.put(columnFamily, entry.key.get(), entry.value);
                }
            }
        }

        @Override
        public byte[] get(final DBAccessor accessor, final byte[] key) throws RocksDBException {
            return accessor.get(columnFamily, key);
        }

        @Override
        public byte[] get(final DBAccessor accessor, final byte[] key, final ReadOptions readOptions) throws RocksDBException {
            return accessor.get(columnFamily, readOptions, key);
        }

        @Override
        public byte[] getOnly(final DBAccessor accessor, final byte[] key) throws RocksDBException {
            return accessor.get(columnFamily, key);
        }

        @Override
        public ManagedKeyValueIterator<Bytes, byte[]> range(final DBAccessor accessor,
                                                            final Bytes from,
                                                            final Bytes to,
                                                            final boolean forward) {
            return new RocksDBRangeIterator(
                name,
                accessor.newIterator(columnFamily),
                from,
                to,
                forward,
                true
            );
        }

        @Override
        public ManagedKeyValueIterator<Bytes, byte[]> all(final DBAccessor accessor,
                                                          final boolean forward) {
            return new RocksDBRangeIterator(
                name,
                accessor.newIterator(columnFamily),
                null,
                null,
                forward,
                false
            );
        }

        @Override
        public long approximateNumEntries(final DBAccessor dbAccessor) throws RocksDBException {
            return dbAccessor.approximateNumEntries(columnFamily);
        }

        @Override
        public void close() {
            columnFamily.close();
        }
    }
}
