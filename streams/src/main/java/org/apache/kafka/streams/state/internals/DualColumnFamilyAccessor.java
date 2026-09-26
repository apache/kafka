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
import org.apache.kafka.common.utils.internals.AbstractIterator;
import org.apache.kafka.common.utils.internals.ByteUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.state.internals.RocksDBStore.DBAccessor;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatchInterface;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.apache.kafka.streams.state.internals.RocksDBStore.incrementWithoutOverflow;

/**
 * A generic implementation of {@link RocksDBStore.ColumnFamilyAccessor} that supports dual-column-family
 * upgrade scenarios. This class manages two column families:
 * <ul>
 *   <li>oldColumnFamily: contains legacy data in the old format</li>
 *   <li>newColumnFamily: contains data in the new format</li>
 * </ul>
 *
 * When reading, it first checks the new column family, then falls back to the old column family
 * and converts values on-the-fly using the provided conversion function.
 */
class DualColumnFamilyAccessor extends AbstractColumnFamilyAccessor {

    private final ColumnFamilyHandle oldColumnFamily;
    private final ColumnFamilyHandle newColumnFamily;
    private final Function<byte[], byte[]> valueConverter;
    private final RocksDBStore store;

    /**
     * Constructs a DualColumnFamilyAccessor.
     *
     * @param offsetColumnFamily the column family for the managed offsets
     * @param oldColumnFamily the column family containing legacy data
     * @param newColumnFamily the column family for new format data
     * @param valueConverter  function to convert old format values to new format
     * @param store           the RocksDBStore instance (for accessing position, context, and name)
     */
    DualColumnFamilyAccessor(final ColumnFamilyHandle offsetColumnFamily,
                             final ColumnFamilyHandle oldColumnFamily,
                             final ColumnFamilyHandle newColumnFamily,
                             final Function<byte[], byte[]> valueConverter,
                             final RocksDBStore store,
                             final AtomicBoolean storeOpen) {
        super(offsetColumnFamily, storeOpen, store.isTransactional);
        this.oldColumnFamily = oldColumnFamily;
        this.newColumnFamily = newColumnFamily;
        this.valueConverter = valueConverter;
        this.store = store;
    }

    @Override
    public void put(final DBAccessor accessor,
                    final byte[] key,
                    final byte[] value) {
        synchronized (store.position) {
            if (value == null) {
                try {
                    accessor.delete(oldColumnFamily, key);
                } catch (final RocksDBException e) {
                    throw new ProcessorStateException("Error while removing key from store " + store.name(), e);
                }
                try {
                    accessor.delete(newColumnFamily, key);
                } catch (final RocksDBException e) {
                    throw new ProcessorStateException("Error while removing key from store " + store.name(), e);
                }
            } else {
                try {
                    accessor.delete(oldColumnFamily, key);
                } catch (final RocksDBException e) {
                    throw new ProcessorStateException("Error while removing key from store " + store.name(), e);
                }
                try {
                    accessor.put(newColumnFamily, key, value);
                    StoreQueryUtils.updatePosition(store.position, store.context);
                } catch (final RocksDBException e) {
                    throw new ProcessorStateException("Error while putting key/value into store " + store.name(), e);
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
    public byte[] get(final DBAccessor accessor, final byte[] key)
        throws RocksDBException {
        return get(accessor, key, Optional.empty());
    }

    @Override
    public byte[] get(final DBAccessor accessor, final byte[] key,
                      final ReadOptions readOptions)
        throws RocksDBException {
        return get(accessor, key, Optional.of(readOptions));
    }

    private byte[] get(final DBAccessor accessor, final byte[] key,
                       final Optional<ReadOptions> readOptions)
        throws RocksDBException {
        final byte[] valueInNewFormat = readOptions.isPresent()
                ? accessor.get(newColumnFamily, readOptions.get(), key)
                : accessor.get(newColumnFamily, key);
        if (valueInNewFormat != null) {
            return valueInNewFormat;
        }

        final byte[] valueInOldFormat = readOptions.isPresent()
                ? accessor.get(oldColumnFamily, readOptions.get(), key)
                : accessor.get(oldColumnFamily, key);
        if (valueInOldFormat != null) {
            final byte[] convertedValue = valueConverter.apply(valueInOldFormat);
            // This does only work because the changelog topic contains correct data already.
            // For other format changes, we cannot take this short cut and can only migrate data
            // from old to new store on put().
            put(accessor, key, convertedValue);
            return convertedValue;
        }

        return null;
    }

    @Override
    public byte[] getOnly(final DBAccessor accessor, final byte[] key) throws RocksDBException {
        final byte[] valueInNewFormat = accessor.get(newColumnFamily, key);
        if (valueInNewFormat != null) {
            return valueInNewFormat;
        }

        final byte[] valueInOldFormat = accessor.get(oldColumnFamily, key);
        if (valueInOldFormat != null) {
            return valueConverter.apply(valueInOldFormat);
        }

        return null;
    }

    @Override
    public ManagedKeyValueIterator<Bytes, byte[]> range(final DBAccessor accessor,
                                                        final Bytes from,
                                                        final Bytes to,
                                                        final boolean forward) {
        final ManagedKeyValueIterator<Bytes, byte[]> iterNew =
                accessor.range(newColumnFamily, store.name(), from, to, forward, true);
        iterNew.onClose(() -> { });
        final ManagedKeyValueIterator<Bytes, byte[]> iterOld =
                accessor.range(oldColumnFamily, store.name(), from, to, forward, true);
        iterOld.onClose(() -> { });
        return new RocksDBDualCFIterator(store.name(), iterNew, iterOld, forward, valueConverter);
    }

    @Override
    public void deleteRange(final DBAccessor accessor, final byte[] from, final byte[] to) {
        try {
            accessor.deleteRange(oldColumnFamily, from, to);
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error while removing key from store " + store.name(), e);
        }
        try {
            accessor.deleteRange(newColumnFamily, from, to);
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error while removing key from store " + store.name(), e);
        }
    }

    @Override
    public ManagedKeyValueIterator<Bytes, byte[]> all(final DBAccessor accessor,
                                                      final boolean forward) {
        final ManagedKeyValueIterator<Bytes, byte[]> iterNew =
                accessor.all(newColumnFamily, store.name(), forward);
        iterNew.onClose(() -> { });
        final ManagedKeyValueIterator<Bytes, byte[]> iterOld =
                accessor.all(oldColumnFamily, store.name(), forward);
        iterOld.onClose(() -> { });
        return new RocksDBDualCFIterator(store.name(), iterNew, iterOld, forward, valueConverter);
    }

    @Override
    public ManagedKeyValueIterator<Bytes, byte[]> prefixScan(final DBAccessor accessor,
                                                             final Bytes prefix) {
        final Bytes to = incrementWithoutOverflow(prefix);
        final ManagedKeyValueIterator<Bytes, byte[]> iterNew =
                accessor.prefixScan(newColumnFamily, store.name(), prefix, to);
        iterNew.onClose(() -> { });
        final ManagedKeyValueIterator<Bytes, byte[]> iterOld =
                accessor.prefixScan(oldColumnFamily, store.name(), prefix, to);
        iterOld.onClose(() -> { });
        return new RocksDBDualCFIterator(store.name(), iterNew, iterOld, true, valueConverter);
    }

    @Override
    public long approximateNumEntries(final DBAccessor accessor)
        throws RocksDBException {
        return accessor.approximateNumEntries(oldColumnFamily)
                + accessor.approximateNumEntries(newColumnFamily);
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
    public void close(final DBAccessor accessor) throws RocksDBException {
        try {
            super.close(accessor);
        } finally {
            try {
                oldColumnFamily.close();
            } finally {
                newColumnFamily.close();
            }
        }
    }

    // Visible for testing
    ColumnFamilyHandle oldColumnFamily() {
        return oldColumnFamily;
    }

    @Override
    public ColumnFamilyHandle dataColumnFamily() {
        return newColumnFamily;
    }

    // Visible for testing
    ColumnFamilyHandle newColumnFamily() {
        return newColumnFamily;
    }

    private static class RocksDBDualCFIterator
        extends AbstractIterator<KeyValue<Bytes, byte[]>>
        implements ManagedKeyValueIterator<Bytes, byte[]> {

        // RocksDB's JNI interface does not expose getters/setters that allow the
        // comparator to be pluggable, and the default is lexicographic, so it's
        // safe to just force lexicographic comparator here for now.
        private final Comparator<byte[]> comparator = ByteUtils.BYTES_LEXICO_COMPARATOR;

        private final String storeName;
        private final ManagedKeyValueIterator<Bytes, byte[]> iterNewFormat;
        private final ManagedKeyValueIterator<Bytes, byte[]> iterOldFormat;
        private final boolean forward;
        private final Function<byte[], byte[]> valueConverter;

        private volatile boolean open = true;

        private KeyValue<Bytes, byte[]> next;
        private Runnable closeCallback = null;

        RocksDBDualCFIterator(final String storeName,
                              final ManagedKeyValueIterator<Bytes, byte[]> iterNewFormat,
                              final ManagedKeyValueIterator<Bytes, byte[]> iterOldFormat,
                              final boolean forward,
                              final Function<byte[], byte[]> valueConverter) {
            this.iterNewFormat = iterNewFormat;
            this.iterOldFormat = iterOldFormat;
            this.storeName = storeName;
            this.forward = forward;
            this.valueConverter = valueConverter;
        }

        @Override
        public synchronized boolean hasNext() {
            if (!open) {
                throw new org.apache.kafka.streams.errors.InvalidStateStoreException(
                        String.format("RocksDB iterator for store %s has closed", storeName));
            }
            return super.hasNext();
        }

        @Override
        public synchronized KeyValue<Bytes, byte[]> next() {
            return super.next();
        }

        @Override
        protected KeyValue<Bytes, byte[]> makeNext() {
            final boolean oldHas = iterOldFormat.hasNext();
            final boolean newHas = iterNewFormat.hasNext();

            if (!oldHas && !newHas) {
                return allDone();
            }
            if (!oldHas) {
                next = iterNewFormat.next();
                return next;
            }
            if (!newHas) {
                final KeyValue<Bytes, byte[]> kv = iterOldFormat.next();
                next = KeyValue.pair(kv.key, valueConverter.apply(kv.value));
                return next;
            }

            final int cmp = comparator.compare(
                    iterOldFormat.peekNextKey().get(),
                    iterNewFormat.peekNextKey().get());
            // New-format wins on equality: the new CF value supersedes the old CF value for
            // the same key. Advance both sides to avoid re-emitting the shadowed old entry.
            if (cmp == 0) {
                iterOldFormat.next();
                next = iterNewFormat.next();
            } else if (forward ? (cmp < 0) : (cmp > 0)) {
                final KeyValue<Bytes, byte[]> kv = iterOldFormat.next();
                next = KeyValue.pair(kv.key, valueConverter.apply(kv.value));
            } else {
                next = iterNewFormat.next();
            }
            return next;
        }

        @Override
        public synchronized void close() {
            if (closeCallback == null) {
                throw new IllegalStateException(
                        "RocksDBDualCFIterator expects close callback to be set immediately upon creation");
            }
            closeCallback.run();

            iterOldFormat.close();
            iterNewFormat.close();
            open = false;
        }

        @Override
        public Bytes peekNextKey() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }
            return next.key;
        }

        @Override
        public void onClose(final Runnable closeCallback) {
            this.closeCallback = closeCallback;
        }
    }
}
