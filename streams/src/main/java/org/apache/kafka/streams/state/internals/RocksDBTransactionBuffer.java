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

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TreeMap;

/**
 * A {@link TransactionBuffer} implementation for RocksDB-backed stores.
 * Uses a {@link WriteBatch} (without index) to accumulate writes for atomic commit.
 * Reads are handled entirely by the shared {@code ConcurrentSkipListMap} in
 * {@link AbstractTransactionBuffer}, so a {@code WriteBatchWithIndex} is not needed.
 * <p>
 * Range deletions ({@link #stageDeleteRange}) are only supported by RocksDB-backed stores
 * and are owned entirely by this class; {@link AbstractTransactionBuffer} carries no
 * tombstone state.
 */
class RocksDBTransactionBuffer extends AbstractTransactionBuffer<Bytes> {

    private final RocksDB db;
    private final ColumnFamilyHandle cfHandle;
    private final WriteOptions wOptions;
    private final String storeName;
    private WriteBatch writeBatch;
    private volatile NavigableMap<Bytes, List<Bytes>> rangeTombstones = Collections.emptyNavigableMap();

    RocksDBTransactionBuffer(final RocksDB db,
                             final ColumnFamilyHandle cfHandle,
                             final WriteOptions wOptions,
                             final String storeName) {
        this.db = db;
        this.cfHandle = cfHandle;
        this.wOptions = wOptions;
        this.storeName = storeName;
        this.writeBatch = new WriteBatch();
    }

    @Override
    int estimateKeySize(final Bytes key) {
        return key.get().length;
    }

    @Override
    void stageToBackend(final Bytes key, final byte[] value) {
        stage(cfHandle, key, value);
    }

    /**
     * Stages a write for an explicit column family. Updates the shared read buffer
     * ({@code pendingWrites}) and appends the write to the shared {@link WriteBatch}
     * under {@code cf}, so every staged CF is committed atomically on {@link #commit()}.
     */
    void stage(final ColumnFamilyHandle cf, final Bytes key, final byte[] value) {
        pendingWrites.put(key, Optional.ofNullable(value));
        pendingWritesBytes += estimateKeySize(key) + (value != null ? value.length : 0);
        try {
            if (value != null) {
                writeBatch.put(cf, key.get(), value);
            } else {
                writeBatch.delete(cf, key.get());
            }
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error staging write in transaction buffer for store " + storeName, e);
        }
    }

    /**
     * Stages a range deletion for an explicit column family. Updates the shared
     * {@code pendingWrites} and {@code rangeTombstones} so iterators opened before
     * commit hide the deleted range, and appends the range delete to the shared
     * {@link WriteBatch} under {@code cf}.
     */
    void stageDeleteRange(final ColumnFamilyHandle cf, final Bytes from, final Bytes to) {
        pendingWrites.subMap(from, true, to, false).clear();
        final TreeMap<Bytes, List<Bytes>> copy = new TreeMap<>(rangeTombstones);
        copy.computeIfAbsent(from, k -> new ArrayList<>()).add(to);
        rangeTombstones = copy;
        pendingWritesBytes += estimateKeySize(from) + estimateKeySize(to);
        try {
            writeBatch.deleteRange(cf, from.get(), to.get());
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error staging delete range in transaction buffer for store " + storeName, e);
        }
    }

    @Override
    public Optional<byte[]> get(final Bytes key) {
        final Optional<byte[]> staged = pendingWrites.get(key);
        if (staged != null) {
            return staged;
        }
        if (isCoveredByRangeTombstone(key, rangeTombstones)) {
            return Optional.empty();
        }
        return null;
    }

    @Override
    public boolean isEmpty() {
        return super.isEmpty() && rangeTombstones.isEmpty();
    }

    ManagedKeyValueIterator<Bytes, byte[]> all(final ColumnFamilyHandle cf, final boolean forward) {
        if (Thread.currentThread() == ownerThread) {
            final ManagedKeyValueIterator<Bytes, byte[]> baseIter = newBaseIterator(cf, null, null, forward, true);
            return new StagedMergeIterator<>(pendingWrites, baseIter, forward);
        }
        return snapshotScan(cf, null, null, forward, true);
    }

    ManagedKeyValueIterator<Bytes, byte[]> range(final ColumnFamilyHandle cf,
                                                 final Bytes from, final Bytes to,
                                                 final boolean forward, final boolean toInclusive) {
        if (Thread.currentThread() == ownerThread) {
            final NavigableMap<Bytes, Optional<byte[]>> stagingView = boundStaging(from, to, toInclusive);
            final ManagedKeyValueIterator<Bytes, byte[]> baseIter = newBaseIterator(cf, from, to, forward, toInclusive);
            return new StagedMergeIterator<>(stagingView, baseIter, forward);
        }
        return snapshotScan(cf, from, to, forward, toInclusive);
    }

    private ManagedKeyValueIterator<Bytes, byte[]> snapshotScan(final ColumnFamilyHandle cf,
                                                                final Bytes from, final Bytes to,
                                                                final boolean forward, final boolean toInclusive) {
        snapshotLock.readLock().lock();
        try {
            final NavigableMap<Bytes, Optional<byte[]>> stagingSnapshot =
                new TreeMap<>(boundStaging(from, to, toInclusive));
            final ManagedKeyValueIterator<Bytes, byte[]> baseIter = newBaseIterator(cf, from, to, forward, toInclusive);
            return new StagedMergeIterator<>(stagingSnapshot, baseIter, forward);
        } finally {
            snapshotLock.readLock().unlock();
        }
    }

    @Override
    ManagedKeyValueIterator<Bytes, byte[]> newBaseIterator(final Bytes from, final Bytes to) {
        return newBaseIterator(cfHandle, from, to, true, true);
    }

    @Override
    ManagedKeyValueIterator<Bytes, byte[]> newBaseIterator(final Bytes from, final Bytes to,
                                                           final boolean forward, final boolean toInclusive) {
        return newBaseIterator(cfHandle, from, to, forward, toInclusive);
    }

    @Override
    ManagedKeyValueIterator<Bytes, byte[]> newBaseSnapshotIterator(
        final Bytes from,
        final Bytes to,
        final boolean forward,
        final boolean toInclusive
    ) {
        return newBaseIterator(cfHandle, from, to, forward, toInclusive);
    }

    private ManagedKeyValueIterator<Bytes, byte[]> newBaseIterator(final ColumnFamilyHandle cf,
                                                                   final Bytes from, final Bytes to,
                                                                   final boolean forward, final boolean toInclusive) {
        final RocksIterator rocksIterator = db.newIterator(cf);
        final ManagedKeyValueIterator<Bytes, byte[]> iter;
        if (from != null && to != null) {
            iter = new RocksDBRangeIterator(storeName, rocksIterator, from, to, forward, toInclusive);
        } else if (from != null && forward) {
            rocksIterator.seek(from.get());
            iter = new RocksDbIterator(storeName, rocksIterator, true);
        } else if (!forward) {
            if (to != null) {
                rocksIterator.seekForPrev(to.get());
            } else {
                rocksIterator.seekToLast();
            }
            iter = new RocksDbIterator(storeName, rocksIterator, false);
        } else {
            rocksIterator.seekToFirst();
            iter = new RocksDbIterator(storeName, rocksIterator, true);
        }
        // RocksDbIterator requires onClose to be set before close() is called.
        // Since this iterator is used internally by StagedMergeIterator (not
        // tracked by RocksDBStore's open-iterator set), use a no-op callback.
        iter.onClose(() -> { });
        if (rangeTombstones.isEmpty()) {
            return iter;
        }
        return new RangeTombstoneFilterIterator(iter, rangeTombstones);
    }

    @Override
    void flushToBase() {
        try {
            db.write(wOptions, writeBatch);
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error committing transaction buffer for store " + storeName, e);
        }
        writeBatch.close();
        writeBatch = new WriteBatch();
        rangeTombstones = Collections.emptyNavigableMap();
    }

    @Override
    void discardPendingBatch() {
        writeBatch.clear();
        rangeTombstones = Collections.emptyNavigableMap();
    }

    @Override
    public long approximateNumUncommittedBytes() {
        return super.approximateNumUncommittedBytes() + writeBatch.getDataSize();
    }

    @Override
    public void close() {
        writeBatch.close();
    }

    static boolean isCoveredByRangeTombstone(final Bytes key,
                                              final NavigableMap<Bytes, List<Bytes>> tombstones) {
        if (tombstones.isEmpty()) {
            return false;
        }
        for (final Map.Entry<Bytes, List<Bytes>> entry : tombstones.headMap(key, true).entrySet()) {
            for (final Bytes to : entry.getValue()) {
                if (key.compareTo(to) < 0) {
                    return true;
                }
            }
        }
        return false;
    }

    private static class RangeTombstoneFilterIterator implements ManagedKeyValueIterator<Bytes, byte[]> {

        private final ManagedKeyValueIterator<Bytes, byte[]> wrapped;
        private final NavigableMap<Bytes, List<Bytes>> tombstones;
        private KeyValue<Bytes, byte[]> prefetched;
        private boolean closed = false;
        private Runnable closeCallback;

        RangeTombstoneFilterIterator(final ManagedKeyValueIterator<Bytes, byte[]> wrapped,
                                      final NavigableMap<Bytes, List<Bytes>> tombstones) {
            this.wrapped = wrapped;
            this.tombstones = tombstones;
        }

        @Override
        public void onClose(final Runnable closeCallback) {
            this.closeCallback = closeCallback;
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                throw new IllegalStateException("Iterator has already been closed.");
            }
            if (prefetched != null) {
                return true;
            }
            prefetched = computeNext();
            return prefetched != null;
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final KeyValue<Bytes, byte[]> result = prefetched;
            prefetched = null;
            return result;
        }

        @Override
        public Bytes peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return prefetched.key;
        }

        @Override
        public void close() {
            closed = true;
            try {
                wrapped.close();
            } finally {
                if (closeCallback != null) {
                    closeCallback.run();
                }
            }
        }

        private KeyValue<Bytes, byte[]> computeNext() {
            while (wrapped.hasNext()) {
                final KeyValue<Bytes, byte[]> entry = wrapped.next();
                if (!isCoveredByRangeTombstone(entry.key, tombstones)) {
                    return entry;
                }
            }
            return null;
        }
    }
}
