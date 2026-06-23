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

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
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

    InMemoryWindowTransactionBuffer(
            final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, byte[]>> segmentMap) {
        this.segmentMap = segmentMap;
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

        return new FlattenedSegmentIterator(
            forward ? timeRange : timeRange.descendingMap(),
            from, to, forward, toInclusive
        );
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

        return new FlattenedSegmentIterator(
            forward ? copy : copy.descendingMap(),
            from, to, forward, toInclusive
        );
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

    /**
     * Iterator that flattens the two-level segmentMap structure into a stream of
     * WindowEntryKey/byte[] pairs, respecting key bounds and direction.
     */
    private static class FlattenedSegmentIterator implements ManagedKeyValueIterator<WindowEntryKey, byte[]> {
        private final Iterator<Map.Entry<Long, ConcurrentNavigableMap<Bytes, byte[]>>> segmentIterator;
        private final WindowEntryKey from;
        private final WindowEntryKey to;
        private final boolean forward;
        private final boolean toInclusive;

        private Iterator<Map.Entry<Bytes, byte[]>> currentKeyIterator;
        private long currentTimestamp;
        private KeyValue<WindowEntryKey, byte[]> prefetched;
        private boolean closed = false;
        private Runnable closeCallback;

        FlattenedSegmentIterator(
                final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, byte[]>> timeRange,
                final WindowEntryKey from,
                final WindowEntryKey to,
                final boolean forward,
                final boolean toInclusive) {
            this.from = from;
            this.to = to;
            this.forward = forward;
            this.toInclusive = toInclusive;
            this.segmentIterator = timeRange.entrySet().iterator();
            advanceSegment();
        }

        private void advanceSegment() {
            currentKeyIterator = null;
            while (segmentIterator.hasNext()) {
                final Map.Entry<Long, ConcurrentNavigableMap<Bytes, byte[]>> segment = segmentIterator.next();
                currentTimestamp = segment.getKey();

                final ConcurrentNavigableMap<Bytes, byte[]> subMap = boundKeyMap(segment.getValue());
                final ConcurrentNavigableMap<Bytes, byte[]> orderedMap = forward ? subMap : subMap.descendingMap();
                currentKeyIterator = orderedMap.entrySet().iterator();
                if (currentKeyIterator.hasNext()) {
                    return;
                }
            }
        }

        private ConcurrentNavigableMap<Bytes, byte[]> boundKeyMap(final ConcurrentNavigableMap<Bytes, byte[]> kvMap) {
            final Bytes keyFrom = (from != null && currentTimestamp == from.timestamp()) ? from.key() : null;
            final Bytes keyTo = (to != null && currentTimestamp == to.timestamp()) ? to.key() : null;
            final boolean keyToInclusive = (to != null && currentTimestamp == to.timestamp()) ? toInclusive : true;

            if (keyFrom != null && keyTo != null) {
                return kvMap.subMap(keyFrom, true, keyTo, keyToInclusive);
            } else if (keyFrom != null) {
                return kvMap.tailMap(keyFrom, true);
            } else if (keyTo != null) {
                return kvMap.headMap(keyTo, keyToInclusive);
            } else {
                return kvMap;
            }
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
        public KeyValue<WindowEntryKey, byte[]> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final KeyValue<WindowEntryKey, byte[]> result = prefetched;
            prefetched = null;
            return result;
        }

        @Override
        public WindowEntryKey peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return prefetched.key;
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

        private KeyValue<WindowEntryKey, byte[]> computeNext() {
            while (currentKeyIterator != null) {
                if (currentKeyIterator.hasNext()) {
                    final Map.Entry<Bytes, byte[]> entry = currentKeyIterator.next();
                    return new KeyValue<>(new WindowEntryKey(currentTimestamp, entry.getKey()), entry.getValue());
                }
                advanceSegment();
            }
            return null;
        }
    }
}
