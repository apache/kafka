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
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;

import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TreeMap;

/**
 * A {@link TransactionBuffer} implementation for {@link InMemoryKeyValueStore}.
 * <p>
 * The owner thread (StreamThread) scans use a lock-free fast path: a live iterator over
 * the base {@code TreeMap} (owner is the sole writer; no concurrent mutation during its own
 * scan) merged live with the staged {@link java.util.concurrent.ConcurrentSkipListMap}.
 * <p>
 * Non-owner (Interactive Query) scans use {@link #newBaseSnapshotIterator}, which eagerly
 * copies the bounded range into a fresh {@link java.util.TreeMap} while the
 * {@link AbstractTransactionBuffer#snapshotLock} read-lock is held. Because commit acquires
 * the write-lock before mutating the base map, the copy is atomic with respect to commit,
 * giving IQ threads true point-in-time snapshot isolation. After construction the IQ iterator
 * never touches the live base map.
 */
class InMemoryTransactionBuffer extends AbstractTransactionBuffer<Bytes> {

    private final NavigableMap<Bytes, byte[]> baseMap;
    private Position pendingPosition = Position.emptyPosition();

    InMemoryTransactionBuffer(final NavigableMap<Bytes, byte[]> baseMap) {
        this.baseMap = baseMap;
    }

    @Override
    int estimateKeySize(final Bytes key) {
        return key.get().length;
    }

    @Override
    void stageToBackend(final Bytes key, final byte[] value) {
        // no-op — staging map is sufficient; no write-batch concept for in-memory
    }

    @Override
    ManagedKeyValueIterator<Bytes, byte[]> newBaseIterator(final Bytes from, final Bytes to) {
        return newBaseIterator(from, to, true, true);
    }

    @Override
    ManagedKeyValueIterator<Bytes, byte[]> newBaseIterator(final Bytes from, final Bytes to,
                                                           final boolean forward, final boolean toInclusive) {
        final NavigableMap<Bytes, byte[]> view = boundView(from, to, toInclusive);
        return new BaseMapIterator(forward ? view : view.descendingMap());
    }

    /**
     * Non-owner (IQ) path: eagerly copies the bounded range from the base map into a fresh
     * {@link java.util.TreeMap} while the caller holds the snapshot read-lock, providing true
     * point-in-time snapshot isolation. The returned iterator never touches the live base map.
     */
    @Override
    ManagedKeyValueIterator<Bytes, byte[]> newBaseSnapshotIterator(final Bytes from, final Bytes to,
                                                                   final boolean forward, final boolean toInclusive) {
        final NavigableMap<Bytes, byte[]> copy = new TreeMap<>(boundView(from, to, toInclusive));
        return new BaseMapIterator(forward ? copy : copy.descendingMap());
    }

    /**
     * Committed-only point read. Bypasses the staging layer and reads the base map directly.
     * <p>
     * The owner reads lock-free (it is the sole mutator and single-threaded). Non-owner (IQ) reads
     * take the snapshot read-lock: {@code commit}/{@code rollback} mutate the non-thread-safe base
     * {@link java.util.TreeMap} under the write-lock, so an unlocked read could traverse a
     * mid-rebalance tree (wrong value/NPE) or observe a partially-applied commit. The read-lock also
     * supplies the happens-before edge that makes committed writes visible.
     */
    byte[] getCommitted(final Bytes key) {
        if (Thread.currentThread() == ownerThread) {
            return baseMap.get(key);
        }
        snapshotLock.readLock().lock();
        try {
            return baseMap.get(key);
        } finally {
            snapshotLock.readLock().unlock();
        }
    }

    private NavigableMap<Bytes, byte[]> boundView(final Bytes from, final Bytes to, final boolean toInclusive) {
        if (from != null && to != null) {
            return baseMap.subMap(from, true, to, toInclusive);
        } else if (from != null) {
            return baseMap.tailMap(from, true);
        } else if (to != null) {
            return baseMap.headMap(to, toInclusive);
        } else {
            return baseMap;
        }
    }

    @Override
    void flushToBase() {
        for (final Map.Entry<Bytes, Optional<byte[]>> entry : pendingWrites.entrySet()) {
            if (entry.getValue().isPresent()) {
                baseMap.put(entry.getKey(), entry.getValue().get());
            } else {
                baseMap.remove(entry.getKey());
            }
        }
    }

    @Override
    void discardPendingBatch() {
        pendingPosition = Position.emptyPosition();
    }

    void updatePosition(final StateStoreContext stateStoreContext) {
        snapshotLock.writeLock().lock();
        try {
            StoreQueryUtils.updatePosition(pendingPosition, stateStoreContext);
        } finally {
            snapshotLock.writeLock().unlock();
        }
    }

    Position pendingPosition() {
        snapshotLock.readLock().lock();
        try {
            return pendingPosition.copy();
        } finally {
            snapshotLock.readLock().unlock();
        }
    }

    void mergePendingPositionInto(final Position committed) {
        snapshotLock.writeLock().lock();
        try {
            committed.merge(pendingPosition);
            pendingPosition = Position.emptyPosition();
        } finally {
            snapshotLock.writeLock().unlock();
        }
    }

    /**
     * An iterator over the entries of a supplied {@link NavigableMap} view of the base store.
     * When fed a private copy (the IQ snapshot path) it provides isolation from concurrent
     * base-map mutation. When fed a live sub-view (the owner fast path) it is not isolated,
     * but that is safe because the owner thread is the sole writer of the base map.
     */
    static class BaseMapIterator implements ManagedKeyValueIterator<Bytes, byte[]> {
        private final java.util.Iterator<Map.Entry<Bytes, byte[]>> delegate;
        private Map.Entry<Bytes, byte[]> next;
        private boolean closed = false;
        private Runnable closeCallback;

        BaseMapIterator(final NavigableMap<Bytes, byte[]> map) {
            this.delegate = map.entrySet().iterator();
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                throw new IllegalStateException("Iterator has already been closed.");
            }
            if (next != null) {
                return true;
            }
            if (delegate.hasNext()) {
                next = delegate.next();
                return true;
            }
            return false;
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final Map.Entry<Bytes, byte[]> entry = next;
            next = null;
            return new KeyValue<>(entry.getKey(), entry.getValue());
        }

        @Override
        public Bytes peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return next.getKey();
        }

        @Override
        public void onClose(final Runnable closeCallback) {
            this.closeCallback = closeCallback;
        }

        @Override
        public void close() {
            closed = true;
            if (closeCallback != null) {
                closeCallback.run();
            }
        }
    }
}
