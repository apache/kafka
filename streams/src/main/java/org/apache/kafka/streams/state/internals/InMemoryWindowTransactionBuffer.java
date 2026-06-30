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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * A {@link TransactionBuffer} implementation for {@link InMemoryWindowStore}.
 * Uses a composite key of (timestamp, key) to maintain correct sort order in the staging map.
 */
class InMemoryWindowTransactionBuffer extends AbstractTransactionBuffer<InMemoryWindowTransactionBuffer.WindowEntryKey> {

    private final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, byte[]>> segmentMap;
    private final boolean retainDuplicates;

    InMemoryWindowTransactionBuffer(
            final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, byte[]>> segmentMap,
            final boolean retainDuplicates) {
        this.segmentMap = segmentMap;
        this.retainDuplicates = retainDuplicates;
    }

    /**
     * Composite key for the window store staging map. Sorts by timestamp first, then by key.
     */
    static final class WindowEntryKey implements Comparable<WindowEntryKey> {
        private final long timestamp;
        private final Bytes key;

        WindowEntryKey(final long timestamp, final Bytes key) {
            this.timestamp = timestamp;
            this.key = key;
        }

        long timestamp() {
            return timestamp;
        }

        Bytes key() {
            return key;
        }

        @Override
        public int compareTo(final WindowEntryKey other) {
            final int cmp = Long.compare(this.timestamp, other.timestamp);
            if (cmp != 0) {
                return cmp;
            }
            // A null key is an unbounded upper-bound marker used only in range scans; it sorts after
            // every real key at the same timestamp. (The lower bound uses empty bytes, the natural minimum.)
            if (this.key == null || other.key == null) {
                return Boolean.compare(this.key == null, other.key == null);
            }
            return this.key.compareTo(other.key);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (!(o instanceof WindowEntryKey)) return false;
            final WindowEntryKey that = (WindowEntryKey) o;
            return timestamp == that.timestamp && Objects.equals(key, that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, key);
        }
    }

    // -- Convenience methods for the store --

    void stage(final long timestamp, final Bytes key, final byte[] value) {
        super.stage(new WindowEntryKey(timestamp, key), value);
    }

    Optional<byte[]> get(final long timestamp, final Bytes key) {
        return super.get(new WindowEntryKey(timestamp, key));
    }

    // -- AbstractTransactionBuffer implementation --

    @Override
    int estimateKeySize(final WindowEntryKey key) {
        return Long.BYTES + key.key().get().length;
    }

    @Override
    void stageToBackend(final WindowEntryKey key, final byte[] value) {
        // no-op — staging map is sufficient; no write-batch concept for in-memory
    }

    @Override
    ManagedKeyValueIterator<WindowEntryKey, byte[]> newBaseIterator(final WindowEntryKey from, final WindowEntryKey to) {
        return newBaseIterator(from, to, true, true);
    }

    @Override
    ManagedKeyValueIterator<WindowEntryKey, byte[]> newBaseIterator(final WindowEntryKey from, final WindowEntryKey to,
                                                                    final boolean forward, final boolean toInclusive) {
        final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, byte[]>> timeRange;
        if (from != null && to != null) {
            timeRange = segmentMap.subMap(from.timestamp(), true, to.timestamp(), true);
        } else if (from != null) {
            timeRange = segmentMap.tailMap(from.timestamp(), true);
        } else if (to != null) {
            timeRange = segmentMap.headMap(to.timestamp(), true);
        } else {
            timeRange = segmentMap;
        }

        return baseIterator(forward ? timeRange : timeRange.descendingMap(), from, to, forward);
    }

    /**
     * Non-owner (IQ) path: eagerly deep-copies the bounded time range while the caller holds the
     * snapshot read-lock, providing true point-in-time isolation. The returned iterator never
     * touches the live segment map, so concurrent owner mutation cannot disturb it.
     */
    @Override
    ManagedKeyValueIterator<WindowEntryKey, byte[]> newBaseSnapshotIterator(final WindowEntryKey from, final WindowEntryKey to,
                                                                            final boolean forward, final boolean toInclusive) {
        final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, byte[]>> timeRange;
        if (from != null && to != null) {
            timeRange = segmentMap.subMap(from.timestamp(), true, to.timestamp(), true);
        } else if (from != null) {
            timeRange = segmentMap.tailMap(from.timestamp(), true);
        } else if (to != null) {
            timeRange = segmentMap.headMap(to.timestamp(), true);
        } else {
            timeRange = segmentMap;
        }

        final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, byte[]>> copy = new ConcurrentSkipListMap<>();
        for (final Map.Entry<Long, ConcurrentNavigableMap<Bytes, byte[]>> segment : timeRange.entrySet()) {
            copy.put(segment.getKey(), new ConcurrentSkipListMap<>(segment.getValue()));
        }

        return baseIterator(forward ? copy : copy.descendingMap(), from, to, forward);
    }

    /**
     * Builds the committed-side base iterator by reusing the non-transactional segment iterator
     * ({@link InMemoryWindowStore.WindowEntryKeyIterator}), which bounds keys at every timestamp and
     * emits in the same (timestamp, stored-key) order as the staged map. A null from/to key (and the
     * empty-bytes lower-bound placeholder) leaves that side of the key dimension open.
     */
    private ManagedKeyValueIterator<WindowEntryKey, byte[]> baseIterator(
            final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, byte[]>> timeRange,
            final WindowEntryKey from,
            final WindowEntryKey to,
            final boolean forward) {
        final Bytes keyFrom = (from != null && from.key() != null && from.key().get().length > 0) ? from.key() : null;
        final Bytes keyTo = (to != null) ? to.key() : null;
        return new InMemoryWindowStore.WindowEntryKeyIterator(
            keyFrom, keyTo, timeRange.entrySet().iterator(), retainDuplicates, forward);
    }

    @Override
    void flushToBase() {
        for (final Map.Entry<WindowEntryKey, Optional<byte[]>> entry : pendingWrites.entrySet()) {
            final long ts = entry.getKey().timestamp();
            final Bytes key = entry.getKey().key();
            if (entry.getValue().isPresent()) {
                segmentMap.computeIfAbsent(ts, t -> new ConcurrentSkipListMap<>()).put(key, entry.getValue().get());
            } else {
                final ConcurrentNavigableMap<Bytes, byte[]> kvMap = segmentMap.get(ts);
                if (kvMap != null) {
                    kvMap.remove(key);
                    if (kvMap.isEmpty()) {
                        segmentMap.remove(ts);
                    }
                }
            }
        }
    }

    @Override
    void discardPendingBatch() {
        // no-op — no backend batch to discard
    }

}
