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

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
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
            return Long.compare(this.startTime, other.startTime);
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

        return new FlattenedSessionIterator(
            forward ? timeRange : timeRange.descendingMap(),
            from, to, forward, toInclusive
        );
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

        final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>> copy = new ConcurrentSkipListMap<>();
        for (final Map.Entry<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>> endTimeEntry : timeRange.entrySet()) {
            final ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>> keyCopy = new ConcurrentSkipListMap<>();
            for (final Map.Entry<Bytes, ConcurrentNavigableMap<Long, byte[]>> keyEntry : endTimeEntry.getValue().entrySet()) {
                keyCopy.put(keyEntry.getKey(), new ConcurrentSkipListMap<>(keyEntry.getValue()));
            }
            copy.put(endTimeEntry.getKey(), keyCopy);
        }

        return new FlattenedSessionIterator(
            forward ? copy : copy.descendingMap(),
            from, to, forward, toInclusive
        );
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
        // no-op — no backend batch to discard
    }

    /**
     * Iterator that flattens the three-level endTimeMap structure into a stream of
     * SessionEntryKey/byte[] pairs, respecting key bounds and direction.
     */
    private static class FlattenedSessionIterator implements ManagedKeyValueIterator<SessionEntryKey, byte[]> {
        private final Iterator<Map.Entry<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>>> endTimeIterator;
        private final SessionEntryKey from;
        private final SessionEntryKey to;
        private final boolean forward;
        private final boolean toInclusive;

        private Iterator<Map.Entry<Bytes, ConcurrentNavigableMap<Long, byte[]>>> keyIterator;
        private Iterator<Map.Entry<Long, byte[]>> startTimeIterator;
        private long currentEndTime;
        private Bytes currentKey;
        private KeyValue<SessionEntryKey, byte[]> prefetched;
        private boolean closed = false;
        private Runnable closeCallback;

        FlattenedSessionIterator(
                final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>> timeRange,
                final SessionEntryKey from,
                final SessionEntryKey to,
                final boolean forward,
                final boolean toInclusive) {
            this.from = from;
            this.to = to;
            this.forward = forward;
            this.toInclusive = toInclusive;
            this.endTimeIterator = timeRange.entrySet().iterator();
            advanceToNextRecord();
        }

        private void advanceToNextRecord() {
            // Try advancing within current startTimeIterator
            if (startTimeIterator != null && startTimeIterator.hasNext()) {
                return;
            }
            // Try advancing within current keyIterator
            if (advanceWithinKeyIterator()) {
                return;
            }
            // Advance to next endTime segment
            advanceToNextEndTimeSegment();
        }

        private boolean advanceWithinKeyIterator() {
            if (keyIterator == null) {
                return false;
            }
            while (keyIterator.hasNext()) {
                final Map.Entry<Bytes, ConcurrentNavigableMap<Long, byte[]>> keyEntry = keyIterator.next();
                currentKey = keyEntry.getKey();
                startTimeIterator = getStartTimeIterator(keyEntry.getValue());
                if (startTimeIterator.hasNext()) {
                    return true;
                }
            }
            return false;
        }

        private void advanceToNextEndTimeSegment() {
            while (endTimeIterator.hasNext()) {
                final Map.Entry<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>> endTimeEntry =
                    endTimeIterator.next();
                currentEndTime = endTimeEntry.getKey();

                final ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>> subMap =
                    boundKeyMap(endTimeEntry.getValue());
                keyIterator = (forward ? subMap : subMap.descendingMap()).entrySet().iterator();
                if (advanceWithinKeyIterator()) {
                    return;
                }
            }
            startTimeIterator = null;
        }

        private ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>> boundKeyMap(
                final ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>> kvMap) {
            final Bytes keyFrom = (from != null && currentEndTime == from.endTime()) ? from.key() : null;
            final Bytes keyTo = (to != null && currentEndTime == to.endTime()) ? to.key() : null;
            if (keyFrom != null && keyTo != null) {
                return kvMap.subMap(keyFrom, true, keyTo, true);
            } else if (keyFrom != null) {
                return kvMap.tailMap(keyFrom, true);
            } else if (keyTo != null) {
                return kvMap.headMap(keyTo, true);
            } else {
                return kvMap;
            }
        }

        private Iterator<Map.Entry<Long, byte[]>> getStartTimeIterator(
                final ConcurrentNavigableMap<Long, byte[]> startTimeMap) {
            final ConcurrentNavigableMap<Long, byte[]> subMap = boundStartTimeMap(startTimeMap);
            return (forward ? subMap : subMap.descendingMap()).entrySet().iterator();
        }

        private ConcurrentNavigableMap<Long, byte[]> boundStartTimeMap(
                final ConcurrentNavigableMap<Long, byte[]> startTimeMap) {
            final Long startFrom = (from != null && currentEndTime == from.endTime()
                && currentKey.equals(from.key())) ? from.startTime() : null;
            final Long startTo = (to != null && currentEndTime == to.endTime()
                && currentKey.equals(to.key())) ? to.startTime() : null;
            final boolean startToInclusive = (startTo != null) ? toInclusive : true;

            if (startFrom != null && startTo != null) {
                return startTimeMap.subMap(startFrom, true, startTo, startToInclusive);
            } else if (startFrom != null) {
                return startTimeMap.tailMap(startFrom, true);
            } else if (startTo != null) {
                return startTimeMap.headMap(startTo, startToInclusive);
            } else {
                return startTimeMap;
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
        public KeyValue<SessionEntryKey, byte[]> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final KeyValue<SessionEntryKey, byte[]> result = prefetched;
            prefetched = null;
            return result;
        }

        @Override
        public SessionEntryKey peekNextKey() {
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

        private KeyValue<SessionEntryKey, byte[]> computeNext() {
            if (startTimeIterator == null) {
                return null;
            }
            if (!startTimeIterator.hasNext()) {
                advanceToNextRecord();
                if (startTimeIterator == null || !startTimeIterator.hasNext()) {
                    return null;
                }
            }
            final Map.Entry<Long, byte[]> entry = startTimeIterator.next();
            final KeyValue<SessionEntryKey, byte[]> result =
                new KeyValue<>(new SessionEntryKey(currentEndTime, currentKey, entry.getKey()), entry.getValue());

            // Pre-advance so hasNext() works correctly
            if (!startTimeIterator.hasNext()) {
                advanceToNextRecord();
            }
            return result;
        }
    }
}
