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
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * A {@link TransactionBuffer} implementation for {@link InMemorySessionStore}.
 * Uses a composite key of (endTime, key, startTime) to maintain correct sort order in the staging map.
 */
class InMemorySessionTransactionBuffer extends AbstractTransactionBuffer<InMemorySessionTransactionBuffer.SessionEntryKey> {

    private final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>> endTimeMap;
    private Position pendingPosition = Position.emptyPosition();

    InMemorySessionTransactionBuffer(
            final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>> endTimeMap) {
        this.endTimeMap = endTimeMap;
    }

    /**
     * Composite key for the session store staging map. Sorts by endTime, then key, then startTime.
     */
    static final class SessionEntryKey implements Comparable<SessionEntryKey> {
        private final long endTime;
        private final Bytes key;
        private final long startTime;

        SessionEntryKey(final long endTime, final Bytes key, final long startTime) {
            this.endTime = endTime;
            this.key = key;
            this.startTime = startTime;
        }

        long endTime() {
            return endTime;
        }

        Bytes key() {
            return key;
        }

        long startTime() {
            return startTime;
        }

        @Override
        public int compareTo(final SessionEntryKey other) {
            int cmp = Long.compare(this.endTime, other.endTime);
            if (cmp != 0) {
                return cmp;
            }
            if (this.key != null && other.key != null) {
                cmp = this.key.compareTo(other.key);
                if (cmp != 0) {
                    return cmp;
                }
            }
            // Descending startTime, matching the order the reused InMemorySessionStoreIterator emits,
            // so the staged map and the base iterator merge in lock-step.
            return Long.compare(other.startTime, this.startTime);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (!(o instanceof SessionEntryKey)) return false;
            final SessionEntryKey that = (SessionEntryKey) o;
            return endTime == that.endTime && startTime == that.startTime && Objects.equals(key, that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(endTime, key, startTime);
        }
    }

    // -- Convenience methods for the store --

    void stage(final Windowed<Bytes> sessionKey, final byte[] value) {
        super.stage(new SessionEntryKey(sessionKey.window().end(), sessionKey.key(), sessionKey.window().start()), value);
    }

    Optional<byte[]> get(final Bytes key, final long startTime, final long endTime) {
        return super.get(new SessionEntryKey(endTime, key, startTime));
    }

    // -- AbstractTransactionBuffer implementation --

    @Override
    int estimateKeySize(final SessionEntryKey key) {
        return 2 * Long.BYTES + key.key().get().length;
    }

    @Override
    void stageToBackend(final SessionEntryKey key, final byte[] value) {
        // no-op — staging map is sufficient; no write-batch concept for in-memory
    }

    @Override
    ManagedKeyValueIterator<SessionEntryKey, byte[]> newBaseIterator(final SessionEntryKey from, final SessionEntryKey to) {
        return newBaseIterator(from, to, true, true);
    }

    @Override
    ManagedKeyValueIterator<SessionEntryKey, byte[]> newBaseIterator(final SessionEntryKey from, final SessionEntryKey to,
                                                                     final boolean forward, final boolean toInclusive) {
        final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>> timeRange;
        if (from != null && to != null) {
            timeRange = endTimeMap.subMap(from.endTime(), true, to.endTime(), true);
        } else if (from != null) {
            timeRange = endTimeMap.tailMap(from.endTime(), true);
        } else if (to != null) {
            timeRange = endTimeMap.headMap(to.endTime(), true);
        } else {
            timeRange = endTimeMap;
        }

        return baseIterator(forward ? timeRange : timeRange.descendingMap(), from, to, forward);
    }

    /**
     * Non-owner (IQ) path: eagerly deep-copies the bounded end-time range while the caller holds
     * the snapshot read-lock, providing true point-in-time isolation. The returned iterator never
     * touches the live end-time map, so concurrent owner mutation cannot disturb it.
     */
    @Override
    ManagedKeyValueIterator<SessionEntryKey, byte[]> newBaseSnapshotIterator(final SessionEntryKey from, final SessionEntryKey to,
                                                                             final boolean forward, final boolean toInclusive) {
        final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>> timeRange;
        if (from != null && to != null) {
            timeRange = endTimeMap.subMap(from.endTime(), true, to.endTime(), true);
        } else if (from != null) {
            timeRange = endTimeMap.tailMap(from.endTime(), true);
        } else if (to != null) {
            timeRange = endTimeMap.headMap(to.endTime(), true);
        } else {
            timeRange = endTimeMap;
        }

        final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>> copy = deepCopy(timeRange);
        return baseIterator(forward ? copy : copy.descendingMap(), from, to, forward);
    }

    /**
     * Committed-only point read for a single session, bypassing the staging layer. Non-owner (IQ)
     * reads take the snapshot read-lock so the read reflects a single committed state rather than a
     * commit in progress.
     */
    byte[] getCommitted(final Bytes key, final long startTime, final long endTime) {
        if (Thread.currentThread() == ownerThread) {
            return baseGet(key, startTime, endTime);
        }
        snapshotLock.readLock().lock();
        try {
            return baseGet(key, startTime, endTime);
        } finally {
            snapshotLock.readLock().unlock();
        }
    }

    private byte[] baseGet(final Bytes key, final long startTime, final long endTime) {
        final ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>> keyMap = endTimeMap.get(endTime);
        if (keyMap == null) {
            return null;
        }
        final ConcurrentNavigableMap<Long, byte[]> startTimeMap = keyMap.get(key);
        return startTimeMap == null ? null : startTimeMap.get(startTime);
    }

    /** Deep-copies a bounded end-time range into a private map so iterators are isolated from later mutation. */
    private static ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>> deepCopy(
            final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>> timeRange) {
        final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>> copy = new ConcurrentSkipListMap<>();
        for (final Map.Entry<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>> endTimeEntry : timeRange.entrySet()) {
            final ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>> keyCopy = new ConcurrentSkipListMap<>();
            for (final Map.Entry<Bytes, ConcurrentNavigableMap<Long, byte[]>> keyEntry : endTimeEntry.getValue().entrySet()) {
                keyCopy.put(keyEntry.getKey(), new ConcurrentSkipListMap<>(keyEntry.getValue()));
            }
            copy.put(endTimeEntry.getKey(), keyCopy);
        }
        return copy;
    }

    /**
     * Builds the committed-side base iterator by reusing the non-transactional
     * {@link InMemorySessionStore.InMemorySessionStoreIterator}, which bounds keys at every endTime and
     * emits in the same (endTime, key, descending-startTime) order as the staged map. The
     * latestSessionStartTime bound is left open here (the store's {@code TransactionalSessionIterator}
     * applies it); a null from/to key leaves the key dimension open.
     */
    private static ManagedKeyValueIterator<SessionEntryKey, byte[]> baseIterator(
            final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>> timeRange,
            final SessionEntryKey from,
            final SessionEntryKey to,
            final boolean forward) {
        return new SessionEntryKeyIterator(
            new InMemorySessionStore.InMemorySessionStoreIterator(
                from != null ? from.key() : null,
                to != null ? to.key() : null,
                Long.MAX_VALUE,
                timeRange.entrySet().iterator(),
                ignored -> { },
                forward));
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

    @Override
    void flushToBase() {
        for (final Map.Entry<SessionEntryKey, Optional<byte[]>> entry : pendingWrites.entrySet()) {
            final long endTime = entry.getKey().endTime();
            final Bytes key = entry.getKey().key();
            final long startTime = entry.getKey().startTime();
            if (entry.getValue().isPresent()) {
                endTimeMap.computeIfAbsent(endTime, t -> new ConcurrentSkipListMap<>())
                    .computeIfAbsent(key, k -> new ConcurrentSkipListMap<>())
                    .put(startTime, entry.getValue().get());
            } else {
                final ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>> keyMap = endTimeMap.get(endTime);
                if (keyMap != null) {
                    final ConcurrentNavigableMap<Long, byte[]> startTimeMap = keyMap.get(key);
                    if (startTimeMap != null) {
                        startTimeMap.remove(startTime);
                        if (startTimeMap.isEmpty()) {
                            keyMap.remove(key);
                            if (keyMap.isEmpty()) {
                                endTimeMap.remove(endTime);
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    void discardPendingBatch() {
        pendingPosition = Position.emptyPosition();
    }

    /**
     * Adapts the non-transactional session iterator's {@code Windowed<Bytes>} output to the
     * {@link SessionEntryKey} space the staged-merge consumes.
     */
    private static final class SessionEntryKeyIterator implements ManagedKeyValueIterator<SessionEntryKey, byte[]> {
        private final KeyValueIterator<Windowed<Bytes>, byte[]> delegate;
        private Runnable closeCallback;

        SessionEntryKeyIterator(final KeyValueIterator<Windowed<Bytes>, byte[]> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public KeyValue<SessionEntryKey, byte[]> next() {
            final KeyValue<Windowed<Bytes>, byte[]> entry = delegate.next();
            return new KeyValue<>(toKey(entry.key), entry.value);
        }

        @Override
        public SessionEntryKey peekNextKey() {
            return toKey(delegate.peekNextKey());
        }

        @Override
        public void onClose(final Runnable closeCallback) {
            this.closeCallback = closeCallback;
        }

        @Override
        public void close() {
            try {
                delegate.close();
            } finally {
                if (closeCallback != null) {
                    closeCallback.run();
                }
            }
        }

        private static SessionEntryKey toKey(final Windowed<Bytes> windowed) {
            return new SessionEntryKey(windowed.window().end(), windowed.key(), windowed.window().start());
        }
    }
}
