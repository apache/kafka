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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatchInterface;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
class DualColumnFamilyAccessor implements RocksDBStore.ColumnFamilyAccessor {

    private final ColumnFamilyHandle oldColumnFamily;
    private final ColumnFamilyHandle newColumnFamily;
    private final Function<byte[], byte[]> valueConverter;
    private final RocksDBStore store;

    /**
   * Constructs a DualColumnFamilyAccessor.
   *
   * @param oldColumnFamily the column family containing legacy data
   * @param newColumnFamily the column family for new format data
   * @param valueConverter function to convert old format values to new format
   * @param store the RocksDBStore instance (for accessing position, context, and name)
   */
    DualColumnFamilyAccessor(final ColumnFamilyHandle oldColumnFamily,
            final ColumnFamilyHandle newColumnFamily,
            final Function<byte[], byte[]> valueConverter,
            final RocksDBStore store) {
        this.oldColumnFamily = oldColumnFamily;
        this.newColumnFamily = newColumnFamily;
        this.valueConverter = valueConverter;
        this.store = store;
    }

    @Override
    public void put(final RocksDBStore.DBAccessor accessor,
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
    public byte[] get(final RocksDBStore.DBAccessor accessor, final byte[] key) throws RocksDBException {
        return get(accessor, key, Optional.empty());
    }

    @Override
    public byte[] get(final RocksDBStore.DBAccessor accessor, final byte[] key,
            final ReadOptions readOptions) throws RocksDBException {
        return get(accessor, key, Optional.of(readOptions));
    }

    private byte[] get(final RocksDBStore.DBAccessor accessor, final byte[] key,
            final Optional<ReadOptions> readOptions) throws RocksDBException {
        final byte[] newValue = readOptions.isPresent()
                ? accessor.get(newColumnFamily, readOptions.get(), key)
                : accessor.get(newColumnFamily, key);
        if (newValue != null) {
            return newValue;
        }

        final byte[] oldValue = readOptions.isPresent()
                ? accessor.get(oldColumnFamily, readOptions.get(), key)
                : accessor.get(oldColumnFamily, key);
        if (oldValue != null) {
            final byte[] convertedValue = valueConverter.apply(oldValue);
      // This does only work because the changelog topic contains correct data already.
      // For other format changes, we cannot take this short cut and can only migrate data
      // from old to new store on put().
            put(accessor, key, convertedValue);
            return convertedValue;
        }

        return null;
    }

    @Override
    public byte[] getOnly(final RocksDBStore.DBAccessor accessor, final byte[] key) throws RocksDBException {
        final byte[] newValue = accessor.get(newColumnFamily, key);
        if (newValue != null) {
            return newValue;
        }

        final byte[] oldValue = accessor.get(oldColumnFamily, key);
        if (oldValue != null) {
            return valueConverter.apply(oldValue);
        }

        return null;
    }

    @Override
    public ManagedKeyValueIterator<Bytes, byte[]> range(final RocksDBStore.DBAccessor accessor,
            final Bytes from,
            final Bytes to,
            final boolean forward) {
        return new RocksDBDualCFRangeIterator(
                store.name(),
                accessor.newIterator(newColumnFamily),
                accessor.newIterator(oldColumnFamily),
                from,
                to,
                forward,
                true,
                valueConverter);
    }

    @Override
    public void deleteRange(final RocksDBStore.DBAccessor accessor, final byte[] from,
            final byte[] to) {
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
    public ManagedKeyValueIterator<Bytes, byte[]> all(final RocksDBStore.DBAccessor accessor,
            final boolean forward) {
        final org.rocksdb.RocksIterator innerIterNew = accessor.newIterator(newColumnFamily);
        final org.rocksdb.RocksIterator innerIterOld = accessor.newIterator(oldColumnFamily);
        if (forward) {
            innerIterNew.seekToFirst();
            innerIterOld.seekToFirst();
        } else {
            innerIterNew.seekToLast();
            innerIterOld.seekToLast();
        }
        return new RocksDBDualCFIterator(store.name(), innerIterNew, innerIterOld, forward, valueConverter);
    }

    @Override
    public ManagedKeyValueIterator<Bytes, byte[]> prefixScan(final RocksDBStore.DBAccessor accessor,
            final Bytes prefix) {
        final Bytes to = incrementWithoutOverflow(prefix);
        return new RocksDBDualCFRangeIterator(
                store.name(),
                accessor.newIterator(newColumnFamily),
                accessor.newIterator(oldColumnFamily),
                prefix,
                to,
                true,
                false,
                valueConverter
        );
    }

    @Override
    public long approximateNumEntries(final RocksDBStore.DBAccessor accessor)
            throws RocksDBException {
        return accessor.approximateNumEntries(oldColumnFamily)
                + accessor.approximateNumEntries(newColumnFamily);
    }

    @Override
    public void commit(final RocksDBStore.DBAccessor accessor,
            final Map<TopicPartition, Long> changelogOffsets) throws RocksDBException {
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
        oldColumnFamily.close();
        newColumnFamily.close();
    }

    private static class RocksDBDualCFIterator
            extends org.apache.kafka.common.utils.AbstractIterator<KeyValue<Bytes, byte[]>>
            implements ManagedKeyValueIterator<Bytes, byte[]> {

    // RocksDB's JNI interface does not expose getters/setters that allow the
    // comparator to be pluggable, and the default is lexicographic, so it's
    // safe to just force lexicographic comparator here for now.
        private final Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;

        private final String storeName;
        private final org.rocksdb.RocksIterator iterNew;
        private final org.rocksdb.RocksIterator iterOld;
        private final boolean forward;
        private final Function<byte[], byte[]> valueConverter;

        private volatile boolean open = true;

        private byte[] nextNew;
        private byte[] nextOld;
        private KeyValue<Bytes, byte[]> next;
        private Runnable closeCallback = null;

        RocksDBDualCFIterator(final String storeName,
                final org.rocksdb.RocksIterator iterNew,
                final org.rocksdb.RocksIterator iterOld,
                final boolean forward,
                final Function<byte[], byte[]> valueConverter) {
            this.iterNew = iterNew;
            this.iterOld = iterOld;
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
            if (nextOld == null && iterOld.isValid()) {
                nextOld = iterOld.key();
            }

            if (nextNew == null && iterNew.isValid()) {
                nextNew = iterNew.key();
            }

            if (nextOld == null && !iterOld.isValid()) {
                if (nextNew == null && !iterNew.isValid()) {
                    return allDone();
                } else {
                    next = KeyValue.pair(new Bytes(nextNew), iterNew.value());
                    nextNew = null;
                    if (forward) {
                        iterNew.next();
                    } else {
                        iterNew.prev();
                    }
                }
            } else {
                if (nextNew == null) {
                    next = KeyValue.pair(new Bytes(nextOld), valueConverter.apply(iterOld.value()));
                    nextOld = null;
                    if (forward) {
                        iterOld.next();
                    } else {
                        iterOld.prev();
                    }
                } else {
                    if (forward) {
                        if (comparator.compare(nextOld, nextNew) <= 0) {
                            next = KeyValue.pair(new Bytes(nextOld), valueConverter.apply(iterOld.value()));
                            nextOld = null;
                            iterOld.next();
                        } else {
                            next = KeyValue.pair(new Bytes(nextNew), iterNew.value());
                            nextNew = null;
                            iterNew.next();
                        }
                    } else {
                        if (comparator.compare(nextOld, nextNew) >= 0) {
                            next = KeyValue.pair(new Bytes(nextOld), valueConverter.apply(iterOld.value()));
                            nextOld = null;
                            iterOld.prev();
                        } else {
                            next = KeyValue.pair(new Bytes(nextNew), iterNew.value());
                            nextNew = null;
                            iterNew.prev();
                        }
                    }
                }
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

            iterOld.close();
            iterNew.close();
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

    private static class RocksDBDualCFRangeIterator extends RocksDBDualCFIterator {
    // RocksDB's JNI interface does not expose getters/setters that allow the
    // comparator to be pluggable, and the default is lexicographic, so it's
    // safe to just force lexicographic comparator here for now.
        private final Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;
        private final byte[] rawLastKey;
        private final boolean forward;
        private final boolean toInclusive;

        RocksDBDualCFRangeIterator(final String storeName,
                final org.rocksdb.RocksIterator iterNew,
                final org.rocksdb.RocksIterator iterOld,
                final Bytes from,
                final Bytes to,
                final boolean forward,
                final boolean toInclusive,
                final Function<byte[], byte[]> valueConverter) {
            super(storeName, iterNew, iterOld, forward, valueConverter);
            this.forward = forward;
            this.toInclusive = toInclusive;
            if (forward) {
                if (from == null) {
                    iterNew.seekToFirst();
                    iterOld.seekToFirst();
                } else {
                    iterNew.seek(from.get());
                    iterOld.seek(from.get());
                }
                rawLastKey = to == null ? null : to.get();
            } else {
                if (to == null) {
                    iterNew.seekToLast();
                    iterOld.seekToLast();
                } else {
                    iterNew.seekForPrev(to.get());
                    iterOld.seekForPrev(to.get());
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
        // null means range endpoint is open
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
