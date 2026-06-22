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

import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Base class for {@link TransactionBuffer} implementations. Provides the shared two-layer
 * staging design: a thread-safe {@link ConcurrentSkipListMap} for reads (any thread) and
 * backend-specific write accumulation for atomic commit.
 * <p>
 * Point lookups ({@link #get(Comparable)}) are lock-free. Scan methods automatically detect the
 * owner thread and use a lock-free fast path; non-owner threads acquire a read lock to
 * snapshot the staging map atomically with base iterator creation.
 *
 * @param <K> the key type, must be {@link Comparable}
 */
abstract class AbstractTransactionBuffer<K extends Comparable<K>> implements TransactionBuffer<K> {

    final ConcurrentSkipListMap<K, Optional<byte[]>> pendingWrites = new ConcurrentSkipListMap<>();
    final ReentrantReadWriteLock snapshotLock = new ReentrantReadWriteLock();
    final Thread ownerThread;
    long pendingWritesBytes;

    AbstractTransactionBuffer() {
        this.ownerThread = Thread.currentThread();
    }

    // -- Abstract methods to be implemented by backend-specific subclasses --

    /** Append the write to the backend-specific batch (e.g. WriteBatch for RocksDB). */
    abstract void stageToBackend(K key, byte[] value);

    /** Create a base store iterator for the given range. Upper bound is inclusive. Forward direction. */
    abstract ManagedKeyValueIterator<K, byte[]> newBaseIterator(K from, K to);

    /** Create a base store iterator with configurable direction and upper bound inclusiveness. */
    ManagedKeyValueIterator<K, byte[]> newBaseIterator(final K from, final K to,
                                                       final boolean forward, final boolean toInclusive) {
        return newBaseIterator(from, to);
    }

    /**
     * Create a base iterator that is isolated from concurrent base-store mutation.
     * <p>
     * Called only from the non-owner ({@link #snapshotScan}) path while the {@link #snapshotLock}
     * read-lock is held. Subclasses must provide mutation isolation appropriate to their backend
     * (e.g. an eager range copy for in-memory, a native snapshot for RocksDB).
     * <p>
     * The owner fast path uses the live {@link #newBaseIterator} and must not use this method.
     */
    abstract ManagedKeyValueIterator<K, byte[]> newBaseSnapshotIterator(K from, K to,
                                                                        boolean forward, boolean toInclusive);

    /** Atomically apply the accumulated writes to the base store. */
    abstract void flushToBase();

    /** Discard the backend-specific pending batch without applying it. */
    abstract void discardPendingBatch();

    /** Estimate the byte size of a key, for uncommitted bytes tracking. */
    abstract int estimateKeySize(K key);

    // -- TransactionBuffer implementation --

    @Override
    public void stage(final K key, final byte[] value) {
        pendingWrites.put(key, Optional.ofNullable(value));
        pendingWritesBytes += estimateKeySize(key) + (value != null ? value.length : 0);
        stageToBackend(key, value);
    }

    @Override
    public Optional<byte[]> get(final K key) {
        return pendingWrites.get(key);
    }

    @Override
    public ManagedKeyValueIterator<K, byte[]> all(final boolean forward) {
        if (Thread.currentThread() == ownerThread) {
            final ManagedKeyValueIterator<K, byte[]> baseIter = newBaseIterator(null, null, forward, true);
            return new StagedMergeIterator<>(pendingWrites, baseIter, forward);
        }
        return snapshotScan(null, null, forward, true);
    }

    /**
     * @throws IllegalArgumentException if {@code from > to} and {@code forward == true}; or {@code from < to} and {@code forward == false}.
     */
    @Override
    public ManagedKeyValueIterator<K, byte[]> range(final K from, final K to, final boolean forward, final boolean toInclusive) {
        if (Thread.currentThread() == ownerThread) {
            final NavigableMap<K, Optional<byte[]>> stagingView = boundStaging(from, to, toInclusive);
            final ManagedKeyValueIterator<K, byte[]> baseIter = newBaseIterator(from, to, forward, toInclusive);
            return new StagedMergeIterator<>(stagingView, baseIter, forward);
        }
        return snapshotScan(from, to, forward, toInclusive);
    }

    @Override
    public void commit() {
        snapshotLock.writeLock().lock();
        try {
            flushToBase();
            pendingWrites.clear();
            pendingWritesBytes = 0;
        } finally {
            snapshotLock.writeLock().unlock();
        }
    }

    @Override
    public void rollback() {
        snapshotLock.writeLock().lock();
        try {
            pendingWrites.clear();
            pendingWritesBytes = 0;
            discardPendingBatch();
        } finally {
            snapshotLock.writeLock().unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        return pendingWrites.isEmpty();
    }

    @Override
    public long approximateNumUncommittedBytes() {
        return pendingWritesBytes;
    }

    @Override
    public void close() {
        // Subclasses can override to release resources
    }

    // -- Internal helpers (package-private for subclass use) --

    /**
     * Constructs an iteratator over a *snapshot* of the current transaction buffer state.
     * This ensures snapshot-isolation for interactive queries, and prevents iterators suddenly becoming invalid when
     * the stream thread clears the transaction buffer state on commit/rollback.
     */
    ManagedKeyValueIterator<K, byte[]> snapshotScan(final K from, final K to,
                                                    final boolean forward, final boolean toInclusive) {
        snapshotLock.readLock().lock();
        try {
            final NavigableMap<K, Optional<byte[]>> stagingSnapshot = new TreeMap<>(boundStaging(from, to, toInclusive));
            final ManagedKeyValueIterator<K, byte[]> baseIter = newBaseSnapshotIterator(from, to, forward, toInclusive);
            return new StagedMergeIterator<>(stagingSnapshot, baseIter, forward);
        } finally {
            snapshotLock.readLock().unlock();
        }
    }

    /**
     * Constructs a view over the transaction buffer's read buffer consisting of only the keys in the given range.
     * If {@code from} or {@code to} are {@code null}, the range will be open-ended; if they are both {@code null}, the
     * range will include all keys (it will just return the original read buffer directly).
     */
    NavigableMap<K, Optional<byte[]>> boundStaging(final K from, final K to, final boolean toInclusive) {
        if (from != null && to != null) {
            return pendingWrites.subMap(from, true, to, toInclusive);
        } else if (from != null) {
            return pendingWrites.tailMap(from, true);
        } else if (to != null) {
            return pendingWrites.headMap(to, toInclusive);
        } else {
            return pendingWrites;
        }
    }
}
