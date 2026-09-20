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

import org.apache.kafka.common.IsolationLevel;

import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Base class for {@link TransactionBuffer} implementations. Provides the shared two-layer
 * staging design: a {@link TreeMap} staging buffer for uncommitted writes plus backend-specific
 * write accumulation for atomic commit.
 * <p>
 * Exactly one thread — the {@link #ownerThread} that constructs the buffer (the StreamThread) —
 * ever mutates {@code pendingWrites}; Interactive Query (IQ) threads are read-only. The staging
 * map is therefore a plain (non-thread-safe) {@link TreeMap} guarded by {@link #snapshotLock}:
 * <ul>
 *   <li>Owner structural mutations ({@code stage}/{@code commit}/{@code rollback}) hold the
 *       <b>write</b> lock.</li>
 *   <li>The owner reads ({@code get}) and scan-copies <b>lock-free</b>: because it is the sole
 *       mutator and is single-threaded, a read is never concurrent with a structural change, and
 *       IQ threads never mutate — so the tree is quiescent for the read.</li>
 *   <li>Non-owner (IQ) reads/snapshots hold the <b>read</b> lock; owner writes become visible to
 *       them via the write-unlock → read-lock happens-before edge.</li>
 *   <li>Owner scans eagerly copy the bounded staging range into a private {@link TreeMap} so a
 *       subsequent owner {@code stage} cannot invalidate an open iterator with a
 *       {@link java.util.ConcurrentModificationException}.</li>
 * </ul>
 * {@code isEmpty()} and {@code approximateNumUncommittedBytes()} read owner-thread-only state and
 * are not synchronized.
 *
 * @param <K> the key type, must be {@link Comparable}
 */
abstract class AbstractTransactionBuffer<K extends Comparable<K>> implements TransactionBuffer<K> {

    final NavigableMap<K, Optional<byte[]>> pendingWrites = new TreeMap<>();
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
        snapshotLock.writeLock().lock();
        try {
            pendingWrites.put(key, Optional.ofNullable(value));
            pendingWritesBytes += estimateKeySize(key) + (value != null ? value.length : 0);
            stageToBackend(key, value);
        } finally {
            snapshotLock.writeLock().unlock();
        }
    }

    @Override
    public Optional<byte[]> get(final K key) {
        // Owner reads lock-free: it is the sole mutator and single-threaded, so this read is never
        // concurrent with a structural change. Non-owner (IQ) reads take the read lock to exclude a
        // concurrent owner write on the non-thread-safe TreeMap.
        if (Thread.currentThread() == ownerThread) {
            return pendingWrites.get(key);
        }
        snapshotLock.readLock().lock();
        try {
            return pendingWrites.get(key);
        } finally {
            snapshotLock.readLock().unlock();
        }
    }

    @Override
    public ManagedKeyValueIterator<K, byte[]> all(final boolean forward) {
        return all(forward, IsolationLevel.READ_UNCOMMITTED);
    }

    /**
     * As {@link #all(boolean)}, but under {@link IsolationLevel#READ_COMMITTED} the staging layer is
     * excluded and only the committed base store is iterated.
     */
    ManagedKeyValueIterator<K, byte[]> all(final boolean forward, final IsolationLevel isolationLevel) {
        if (Thread.currentThread() == ownerThread) {
            // Eager copy of the staging layer (lock-free for the sole mutator) so a later owner
            // stage cannot invalidate this iterator; the base store is iterated live.
            final NavigableMap<K, Optional<byte[]>> stagingSnapshot = stagingSnapshot(null, null, true, isolationLevel);
            final ManagedKeyValueIterator<K, byte[]> baseIter = newBaseIterator(null, null, forward, true);
            return new StagedMergeIterator<>(stagingSnapshot, baseIter, forward);
        }
        return snapshotScan(null, null, forward, true, isolationLevel);
    }

    /**
     * @throws IllegalArgumentException if {@code from > to} and {@code forward == true}; or {@code from < to} and {@code forward == false}.
     */
    @Override
    public ManagedKeyValueIterator<K, byte[]> range(final K from, final K to, final boolean forward, final boolean toInclusive) {
        return range(from, to, forward, toInclusive, IsolationLevel.READ_UNCOMMITTED);
    }

    /**
     * As {@link #range(Object, Object, boolean, boolean)}, but under {@link IsolationLevel#READ_COMMITTED}
     * the staging layer is excluded and only the committed base store is iterated.
     */
    ManagedKeyValueIterator<K, byte[]> range(final K from, final K to, final boolean forward, final boolean toInclusive,
                                             final IsolationLevel isolationLevel) {
        if (Thread.currentThread() == ownerThread) {
            final NavigableMap<K, Optional<byte[]>> stagingSnapshot = stagingSnapshot(from, to, toInclusive, isolationLevel);
            final ManagedKeyValueIterator<K, byte[]> baseIter = newBaseIterator(from, to, forward, toInclusive);
            return new StagedMergeIterator<>(stagingSnapshot, baseIter, forward);
        }
        return snapshotScan(from, to, forward, toInclusive, isolationLevel);
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
        return snapshotScan(from, to, forward, toInclusive, IsolationLevel.READ_UNCOMMITTED);
    }

    /**
     * As {@link #snapshotScan(Object, Object, boolean, boolean)}, but under
     * {@link IsolationLevel#READ_COMMITTED} only the committed base store is snapshotted, excluding the
     * staging layer.
     */
    ManagedKeyValueIterator<K, byte[]> snapshotScan(final K from, final K to,
                                                    final boolean forward, final boolean toInclusive,
                                                    final IsolationLevel isolationLevel) {
        snapshotLock.readLock().lock();
        try {
            final NavigableMap<K, Optional<byte[]>> stagingSnapshot = stagingSnapshot(from, to, toInclusive, isolationLevel);
            final ManagedKeyValueIterator<K, byte[]> baseIter = newBaseSnapshotIterator(from, to, forward, toInclusive);
            return new StagedMergeIterator<>(stagingSnapshot, baseIter, forward);
        } finally {
            snapshotLock.readLock().unlock();
        }
    }

    /**
     * An eager copy of the bounded staging range, so a later owner {@code stage} cannot invalidate an
     * open iterator. Under {@link IsolationLevel#READ_COMMITTED} the copy is empty, which makes the
     * {@link StagedMergeIterator} degenerate to a committed-only (base) view.
     */
    private NavigableMap<K, Optional<byte[]>> stagingSnapshot(final K from, final K to, final boolean toInclusive,
                                                              final IsolationLevel isolationLevel) {
        return isolationLevel == IsolationLevel.READ_UNCOMMITTED
            ? new TreeMap<>(boundStaging(from, to, toInclusive))
            : new TreeMap<>();
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
