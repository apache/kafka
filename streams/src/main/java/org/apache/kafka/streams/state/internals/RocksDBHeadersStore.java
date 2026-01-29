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

import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.state.HeadersBytesStore;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatchInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

/**
 * A persistent key-(value-timestamp-headers) store based on RocksDB.
 * <p>
 * This store extends RocksDBTimestampedStore to add header support using a dual column family approach.
 * The old column family contains timestamped values ([timestamp(8)][value]),
 * and the new column family contains values with headers ([headerSize(2)][headers][timestamp(8)][value]).
 * <p>
 * This implementation supports lazy migration from timestamped stores to header-aware stores.
 */
public class RocksDBHeadersStore extends RocksDBTimestampedStore implements HeadersBytesStore {
    private static final Logger log = LoggerFactory.getLogger(RocksDBHeadersStore.class);

    private static final byte[] HEADERS_COLUMN_FAMILY_NAME = "keyValueWithHeaders".getBytes(StandardCharsets.UTF_8);

    public RocksDBHeadersStore(final String name,
                               final String metricsScope) {
        super(name, metricsScope);
    }

    RocksDBHeadersStore(final String name,
                        final String parentDir,
                        final RocksDBMetricsRecorder metricsRecorder) {
        super(name, parentDir, metricsRecorder);
    }

    @Override
    void openRocksDB(final DBOptions dbOptions,
                     final ColumnFamilyOptions columnFamilyOptions) {
        // Note: We open three column families:
        // 0. DEFAULT_COLUMN_FAMILY - required by RocksDB but not used by this store
        // 1. TIMESTAMPED_VALUES_COLUMN_FAMILY_NAME (from parent) - contains [timestamp(8)][value]
        // 2. HEADERS_COLUMN_FAMILY_NAME - contains [headerSize(2)][headers][timestamp(8)][value]
        final List<ColumnFamilyHandle> columnFamilies = openRocksDB(
                dbOptions,
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
                new ColumnFamilyDescriptor(TIMESTAMPED_VALUES_COLUMN_FAMILY_NAME, columnFamilyOptions),
                new ColumnFamilyDescriptor(HEADERS_COLUMN_FAMILY_NAME, columnFamilyOptions)
        );

        // DEFAULT_COLUMN_FAMILY - not used, but RocksDB requires it to be present
        final ColumnFamilyHandle defaultColumnFamily = columnFamilies.get(0);

        // Actual column families for migration
        final ColumnFamilyHandle timestampedColumnFamily = columnFamilies.get(1); // oldCF
        final ColumnFamilyHandle headersColumnFamily = columnFamilies.get(2);     // newCF

        final RocksIterator timestampedIter = db.newIterator(timestampedColumnFamily);
        timestampedIter.seekToFirst();
        if (timestampedIter.isValid()) {
            log.info("Opening store {} in upgrade mode (timestamped to headers)", name);
            cfAccessor = new DualColumnFamilyAccessor(defaultColumnFamily, timestampedColumnFamily, headersColumnFamily);
        } else {
            log.info("Opening store {} in regular mode (headers only)", name);
            cfAccessor = new SingleColumnFamilyAccessor(headersColumnFamily);
            defaultColumnFamily.close();
            timestampedColumnFamily.close();
        }
        timestampedIter.close();
    }

    private class DualColumnFamilyAccessor implements ColumnFamilyAccessor {
        private final ColumnFamilyHandle defaultColumnFamily; // Not used, but needs to be closed
        private final ColumnFamilyHandle oldColumnFamily;     // Timestamped format
        private final ColumnFamilyHandle newColumnFamily;     // Headers format

        private DualColumnFamilyAccessor(final ColumnFamilyHandle defaultColumnFamily,
                                         final ColumnFamilyHandle oldColumnFamily,
                                         final ColumnFamilyHandle newColumnFamily) {
            this.defaultColumnFamily = defaultColumnFamily;
            this.oldColumnFamily = oldColumnFamily;
            this.newColumnFamily = newColumnFamily;
        }

        @Override
        public void put(final DBAccessor accessor,
                        final byte[] key,
                        final byte[] valueWithHeaders) {
            synchronized (position) {
                if (valueWithHeaders == null) {
                    try {
                        accessor.delete(oldColumnFamily, key);
                    } catch (final RocksDBException e) {
                        throw new ProcessorStateException("Error while removing key from store " + name, e);
                    }
                    try {
                        accessor.delete(newColumnFamily, key);
                    } catch (final RocksDBException e) {
                        throw new ProcessorStateException("Error while removing key from store " + name, e);
                    }
                } else {
                    try {
                        accessor.delete(oldColumnFamily, key);
                    } catch (final RocksDBException e) {
                        throw new ProcessorStateException("Error while removing key from store " + name, e);
                    }
                    try {
                        accessor.put(newColumnFamily, key, valueWithHeaders);
                        StoreQueryUtils.updatePosition(position, context);
                    } catch (final RocksDBException e) {
                        throw new ProcessorStateException("Error while putting key/value into store " + name, e);
                    }
                }
            }
        }

        @Override
        public void prepareBatch(final List<KeyValue<Bytes, byte[]>> entries,
                                 final WriteBatchInterface batch) throws RocksDBException {
            for (final KeyValue<Bytes, byte[]> entry : entries) {
                Objects.requireNonNull(entry.key, "key cannot be null");
                addToBatch(entry.key.get(), entry.value, batch);
            }
        }

        @Override
        public byte[] get(final DBAccessor accessor, final byte[] key) throws RocksDBException {
            return get(accessor, key, Optional.empty());
        }

        @Override
        public byte[] get(final DBAccessor accessor, final byte[] key, final ReadOptions readOptions) throws RocksDBException {
            return get(accessor, key, Optional.of(readOptions));
        }

        private byte[] get(final DBAccessor accessor, final byte[] key, final Optional<ReadOptions> readOptions) throws RocksDBException {
            // First, try the new column family (with headers)
            final byte[] valueWithHeaders = readOptions.isPresent()
                ? accessor.get(newColumnFamily, readOptions.get(), key)
                : accessor.get(newColumnFamily, key);
            if (valueWithHeaders != null) {
                return valueWithHeaders;
            }

            // Fallback to old column family (timestamped only)
            final byte[] timestampedValue = readOptions.isPresent()
                ? accessor.get(oldColumnFamily, readOptions.get(), key)
                : accessor.get(oldColumnFamily, key);
            if (timestampedValue != null) {
                // Convert from [timestamp(8)][value] to [headerSize(2)][headers][timestamp(8)][value]
                final byte[] valueWithEmptyHeaders = HeadersBytesStore.convertToHeaderFormat(key, timestampedValue);
                // Migrate the data to the new column family
                put(accessor, key, valueWithEmptyHeaders);
                return valueWithEmptyHeaders;
            }

            return null;
        }

        @Override
        public byte[] getOnly(final DBAccessor accessor, final byte[] key) throws RocksDBException {
            final byte[] valueWithHeaders = accessor.get(newColumnFamily, key);
            if (valueWithHeaders != null) {
                return valueWithHeaders;
            }

            final byte[] timestampedValue = accessor.get(oldColumnFamily, key);
            if (timestampedValue != null) {
                return HeadersBytesStore.convertToHeaderFormat(key, timestampedValue);
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
                true);
        }

        @Override
        public void deleteRange(final DBAccessor accessor, final byte[] from, final byte[] to) {
            try {
                accessor.deleteRange(oldColumnFamily, from, to);
            } catch (final RocksDBException e) {
                throw new ProcessorStateException("Error while removing key from store " + name, e);
            }
            try {
                accessor.deleteRange(newColumnFamily, from, to);
            } catch (final RocksDBException e) {
                throw new ProcessorStateException("Error while removing key from store " + name, e);
            }
        }

        @Override
        public ManagedKeyValueIterator<Bytes, byte[]> all(final DBAccessor accessor, final boolean forward) {
            final RocksIterator innerIterWithHeaders = accessor.newIterator(newColumnFamily);
            final RocksIterator innerIterTimestamped = accessor.newIterator(oldColumnFamily);
            if (forward) {
                innerIterWithHeaders.seekToFirst();
                innerIterTimestamped.seekToFirst();
            } else {
                innerIterWithHeaders.seekToLast();
                innerIterTimestamped.seekToLast();
            }
            return new RocksDBDualCFIterator(name, innerIterWithHeaders, innerIterTimestamped, forward);
        }

        @Override
        public ManagedKeyValueIterator<Bytes, byte[]> prefixScan(final DBAccessor accessor, final Bytes prefix) {
            final Bytes to = incrementWithoutOverflow(prefix);
            return new RocksDBDualCFRangeIterator(
                name,
                accessor.newIterator(newColumnFamily),
                accessor.newIterator(oldColumnFamily),
                prefix,
                to,
                true,
                false
            );
        }

        @Override
        public long approximateNumEntries(final DBAccessor accessor) throws RocksDBException {
            return accessor.approximateNumEntries(oldColumnFamily) +
                    accessor.approximateNumEntries(newColumnFamily);
        }

        @Override
        public void flush(final DBAccessor accessor) throws RocksDBException {
            accessor.flush(oldColumnFamily, newColumnFamily);
        }

        @Override
        public void addToBatch(final byte[] key,
                               final byte[] value,
                               final WriteBatchInterface batch) throws RocksDBException {
            if (value == null) {
                batch.delete(oldColumnFamily, key);
                batch.delete(newColumnFamily, key);
            } else {
                batch.delete(oldColumnFamily, key);
                batch.put(newColumnFamily, key, value);
            }
        }

        @Override
        public void close() {
            defaultColumnFamily.close();
            oldColumnFamily.close();
            newColumnFamily.close();
        }
    }

    private static class RocksDBDualCFIterator extends AbstractIterator<KeyValue<Bytes, byte[]>>
        implements ManagedKeyValueIterator<Bytes, byte[]> {

        private final Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;

        private final String storeName;
        private final RocksIterator iterWithHeaders;
        private final RocksIterator iterTimestamped;
        private final boolean forward;

        private volatile boolean open = true;

        private byte[] nextWithHeaders;
        private byte[] nextTimestamped;
        private KeyValue<Bytes, byte[]> next;
        private Runnable closeCallback = null;

        RocksDBDualCFIterator(final String storeName,
                              final RocksIterator iterWithHeaders,
                              final RocksIterator iterTimestamped,
                              final boolean forward) {
            this.iterWithHeaders = iterWithHeaders;
            this.iterTimestamped = iterTimestamped;
            this.storeName = storeName;
            this.forward = forward;
        }

        @Override
        public synchronized boolean hasNext() {
            if (!open) {
                throw new InvalidStateStoreException(String.format("RocksDB iterator for store %s has closed", storeName));
            }
            return super.hasNext();
        }

        @Override
        public synchronized KeyValue<Bytes, byte[]> next() {
            return super.next();
        }

        @Override
        protected KeyValue<Bytes, byte[]> makeNext() {
            if (nextTimestamped == null && iterTimestamped.isValid()) {
                nextTimestamped = iterTimestamped.key();
            }

            if (nextWithHeaders == null && iterWithHeaders.isValid()) {
                nextWithHeaders = iterWithHeaders.key();
            }

            if (nextTimestamped == null && !iterTimestamped.isValid()) {
                if (nextWithHeaders == null && !iterWithHeaders.isValid()) {
                    return allDone();
                } else {
                    next = KeyValue.pair(new Bytes(nextWithHeaders), iterWithHeaders.value());
                    nextWithHeaders = null;
                    if (forward) {
                        iterWithHeaders.next();
                    } else {
                        iterWithHeaders.prev();
                    }
                }
            } else {
                if (nextWithHeaders == null) {
                    next = KeyValue.pair(new Bytes(nextTimestamped),
                        HeadersBytesStore.convertToHeaderFormat(nextTimestamped, iterTimestamped.value()));
                    nextTimestamped = null;
                    if (forward) {
                        iterTimestamped.next();
                    } else {
                        iterTimestamped.prev();
                    }
                } else {
                    if (forward) {
                        if (comparator.compare(nextTimestamped, nextWithHeaders) <= 0) {
                            next = KeyValue.pair(new Bytes(nextTimestamped),
                                HeadersBytesStore.convertToHeaderFormat(nextTimestamped, iterTimestamped.value()));
                            nextTimestamped = null;
                            iterTimestamped.next();
                        } else {
                            next = KeyValue.pair(new Bytes(nextWithHeaders), iterWithHeaders.value());
                            nextWithHeaders = null;
                            iterWithHeaders.next();
                        }
                    } else {
                        if (comparator.compare(nextTimestamped, nextWithHeaders) >= 0) {
                            next = KeyValue.pair(new Bytes(nextTimestamped),
                                HeadersBytesStore.convertToHeaderFormat(nextTimestamped, iterTimestamped.value()));
                            nextTimestamped = null;
                            iterTimestamped.prev();
                        } else {
                            next = KeyValue.pair(new Bytes(nextWithHeaders), iterWithHeaders.value());
                            nextWithHeaders = null;
                            iterWithHeaders.prev();
                        }
                    }
                }
            }
            return next;
        }

        @Override
        public synchronized void close() {
            if (closeCallback == null) {
                throw new IllegalStateException("RocksDBDualCFIterator expects close callback to be set immediately upon creation");
            }
            closeCallback.run();

            iterTimestamped.close();
            iterWithHeaders.close();
            open = false;
        }

        @Override
        public Bytes peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return next.key;
        }

        @Override
        public void onClose(final Runnable closeCallback) {
            this.closeCallback = closeCallback;
        }
    }

    private static class RocksDBDualCFRangeIterator extends RocksDBDualCFIterator {
        private final Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;
        private final byte[] rawLastKey;
        private final boolean forward;
        private final boolean toInclusive;

        RocksDBDualCFRangeIterator(final String storeName,
                                   final RocksIterator iterWithHeaders,
                                   final RocksIterator iterTimestamped,
                                   final Bytes from,
                                   final Bytes to,
                                   final boolean forward,
                                   final boolean toInclusive) {
            super(storeName, iterWithHeaders, iterTimestamped, forward);
            this.forward = forward;
            this.toInclusive = toInclusive;
            if (forward) {
                if (from == null) {
                    iterWithHeaders.seekToFirst();
                    iterTimestamped.seekToFirst();
                } else {
                    iterWithHeaders.seek(from.get());
                    iterTimestamped.seek(from.get());
                }
                rawLastKey = to == null ? null : to.get();
            } else {
                if (to == null) {
                    iterWithHeaders.seekToLast();
                    iterTimestamped.seekToLast();
                } else {
                    iterWithHeaders.seekForPrev(to.get());
                    iterTimestamped.seekForPrev(to.get());
                }
                rawLastKey = from == null ? null : from.get();
            }
        }

        @Override
        protected KeyValue<Bytes, byte[]> makeNext() {
            final KeyValue<Bytes, byte[]> next = super.makeNext();

            if (next == null) {
                return allDone();
            } else if (rawLastKey == null) {
                return next;
            } else {
                if (forward) {
                    if (comparator.compare(next.key.get(), rawLastKey) < 0) {
                        return next;
                    } else if (comparator.compare(next.key.get(), rawLastKey) == 0) {
                        return toInclusive ? next : allDone();
                    } else {
                        return allDone();
                    }
                } else {
                    if (comparator.compare(next.key.get(), rawLastKey) >= 0) {
                        return next;
                    } else {
                        return allDone();
                    }
                }
            }
        }
    }
}
