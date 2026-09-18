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

import org.apache.kafka.streams.KeyValue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AbstractTransactionBufferTest {

    private TestBuffer buffer;

    @BeforeEach
    void setUp() {
        buffer = new TestBuffer();
    }

    // -- stage / get --

    @Test
    void getStagedValueReturnsOptionalOfValue() {
        final byte[] val = {1, 2, 3};
        buffer.stage(1, val);
        assertArrayEquals(val, buffer.get(1).get());
    }

    @Test
    void getStagedTombstoneReturnsEmptyOptional() {
        buffer.stage(1, null);
        assertEquals(Optional.empty(), buffer.get(1));
    }

    @Test
    void getUnstaggedKeyReturnsNull() {
        assertNull(buffer.get(99));
    }

    // -- isEmpty / approximateNumUncommittedBytes --

    @Test
    void isEmptyAndZeroBytesOnFreshBuffer() {
        assertTrue(buffer.isEmpty());
        assertEquals(0L, buffer.approximateNumUncommittedBytes());
    }

    @Test
    void isNotEmptyAfterStage() {
        buffer.stage(1, new byte[]{1});
        assertFalse(buffer.isEmpty());
    }

    @Test
    void byteCountReflectsStagedValue() {
        final byte[] val = {1, 2, 3};
        buffer.stage(1, val);
        assertEquals(Integer.BYTES + val.length, buffer.approximateNumUncommittedBytes());
    }

    @Test
    void byteCountReflectsStagedTombstone() {
        buffer.stage(1, null);
        assertEquals(Integer.BYTES, buffer.approximateNumUncommittedBytes());
    }

    @Test
    void byteCountAccumulatesAcrossMultipleStages() {
        buffer.stage(1, new byte[4]);
        buffer.stage(2, new byte[8]);
        assertEquals(2 * Integer.BYTES + 4 + 8, buffer.approximateNumUncommittedBytes());
    }

    // -- commit --

    @Test
    void commitClearsPendingWritesAndBytes() {
        buffer.stage(1, new byte[]{42});
        buffer.commit();
        assertTrue(buffer.isEmpty());
        assertEquals(0L, buffer.approximateNumUncommittedBytes());
    }

    @Test
    void commitFlushesWritesToBase() {
        final byte[] val = {7, 8, 9};
        buffer.stage(5, val);
        buffer.commit();
        assertArrayEquals(val, buffer.base.get(5));
    }

    @Test
    void commitFlushesTombstoneToBase() {
        buffer.base.put(3, new byte[]{1});
        buffer.stage(3, null);
        buffer.commit();
        assertFalse(buffer.base.containsKey(3));
    }

    @Test
    void subsequentBatchAfterCommitIsIndependent() {
        buffer.stage(1, new byte[]{1});
        buffer.commit();
        buffer.stage(2, new byte[]{2});
        assertEquals(Integer.BYTES + 1, buffer.approximateNumUncommittedBytes());
        buffer.commit();
        assertTrue(buffer.base.containsKey(1));
        assertTrue(buffer.base.containsKey(2));
    }

    // -- rollback --

    @Test
    void rollbackClearsPendingWritesAndBytes() {
        buffer.stage(1, new byte[]{42});
        buffer.rollback();
        assertTrue(buffer.isEmpty());
        assertEquals(0L, buffer.approximateNumUncommittedBytes());
    }

    @Test
    void rollbackLeavesBaseUnchanged() {
        buffer.base.put(3, new byte[]{99});
        buffer.stage(3, new byte[]{1});
        buffer.rollback();
        assertArrayEquals(new byte[]{99}, buffer.base.get(3));
    }

    @Test
    void rollbackDiscardsNewEntries() {
        buffer.stage(5, new byte[]{1});
        buffer.rollback();
        assertFalse(buffer.base.containsKey(5));
    }

    // -- all (owner thread) --

    @Test
    void allForwardMergesStagedAndBase() {
        buffer.base.put(2, new byte[]{2});
        buffer.stage(1, new byte[]{1});
        buffer.stage(3, new byte[]{3});
        assertEquals(List.of(1, 2, 3), drainKeys(buffer.all(true)));
    }

    @Test
    void allReverseReturnsKeysDescending() {
        buffer.base.put(2, new byte[]{2});
        buffer.stage(1, new byte[]{1});
        buffer.stage(3, new byte[]{3});
        assertEquals(List.of(3, 2, 1), drainKeys(buffer.all(false)));
    }

    @Test
    void allStagedTombstoneHidesBaseEntry() {
        buffer.base.put(2, new byte[]{2});
        buffer.stage(2, null);
        assertFalse(drainKeys(buffer.all(true)).contains(2));
    }

    // -- range (owner thread) --

    @Test
    void rangeInclusiveUpperBound() {
        buffer.stage(1, new byte[]{1});
        buffer.stage(2, new byte[]{2});
        buffer.stage(3, new byte[]{3});
        assertEquals(List.of(1, 2), drainKeys(buffer.range(1, 2, true, true)));
    }

    @Test
    void rangeExclusiveUpperBound() {
        buffer.stage(1, new byte[]{1});
        buffer.stage(2, new byte[]{2});
        buffer.stage(3, new byte[]{3});
        assertEquals(List.of(1), drainKeys(buffer.range(1, 2, true, false)));
    }

    @Test
    void rangeNullFromOpensLowerBound() {
        buffer.stage(1, new byte[]{1});
        buffer.stage(2, new byte[]{2});
        buffer.stage(3, new byte[]{3});
        assertEquals(List.of(1, 2), drainKeys(buffer.range(null, 2, true, true)));
    }

    @Test
    void rangeNullToOpensUpperBound() {
        buffer.stage(1, new byte[]{1});
        buffer.stage(2, new byte[]{2});
        buffer.stage(3, new byte[]{3});
        assertEquals(List.of(2, 3), drainKeys(buffer.range(2, null, true, true)));
    }

    @Test
    void rangeNullBothBoundsWithReverseReturnsSameAsAllReverse() {
        buffer.stage(1, new byte[]{1});
        buffer.stage(2, new byte[]{2});
        buffer.stage(3, new byte[]{3});
        assertEquals(List.of(3, 2, 1), drainKeys(buffer.range(null, null, false, true)));
    }

    // -- boundStaging --

    @Test
    void boundStagingBothNullReturnsFullMap() {
        buffer.stage(1, new byte[]{1});
        buffer.stage(2, new byte[]{2});
        assertEquals(buffer.pendingWrites, buffer.boundStaging(null, null, true));
    }

    @Test
    void boundStagingOnlyFromReturnsTailMap() {
        buffer.stage(1, new byte[]{1});
        buffer.stage(2, new byte[]{2});
        buffer.stage(3, new byte[]{3});
        assertEquals(List.of(2, 3), new ArrayList<>(buffer.boundStaging(2, null, true).keySet()));
    }

    @Test
    void boundStagingOnlyToInclusiveReturnsHeadMapInclusive() {
        buffer.stage(1, new byte[]{1});
        buffer.stage(2, new byte[]{2});
        buffer.stage(3, new byte[]{3});
        assertEquals(List.of(1, 2), new ArrayList<>(buffer.boundStaging(null, 2, true).keySet()));
    }

    @Test
    void boundStagingOnlyToExclusiveReturnsHeadMapExclusive() {
        buffer.stage(1, new byte[]{1});
        buffer.stage(2, new byte[]{2});
        buffer.stage(3, new byte[]{3});
        assertEquals(List.of(1), new ArrayList<>(buffer.boundStaging(null, 2, false).keySet()));
    }

    @Test
    void boundStagingBothBoundsInclusiveReturnsSubMap() {
        buffer.stage(1, new byte[]{1});
        buffer.stage(2, new byte[]{2});
        buffer.stage(3, new byte[]{3});
        buffer.stage(4, new byte[]{4});
        assertEquals(List.of(2, 3), new ArrayList<>(buffer.boundStaging(2, 3, true).keySet()));
    }

    @Test
    void boundStagingBothBoundsExclusiveUpperReturnsSubMap() {
        buffer.stage(1, new byte[]{1});
        buffer.stage(2, new byte[]{2});
        buffer.stage(3, new byte[]{3});
        buffer.stage(4, new byte[]{4});
        assertEquals(List.of(2), new ArrayList<>(buffer.boundStaging(2, 3, false).keySet()));
    }

    // -- non-owner thread: point lookup --

    @Test
    void nonOwnerThreadCanReadStagedEntry() throws Exception {
        buffer.stage(1, new byte[]{42});
        final ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            final Future<Optional<byte[]>> future = exec.submit(() -> buffer.get(1));
            final Optional<byte[]> result = future.get();
            assertNotNull(result);
            assertArrayEquals(new byte[]{42}, result.get());
        } finally {
            exec.shutdown();
        }
    }

    // -- non-owner thread: snapshot isolation --

    @Test
    void nonOwnerAllSnapshotsAtCreationTime() throws Exception {
        buffer.stage(1, new byte[]{1});

        final ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            // Non-owner captures the iterator — snapshot of staging is taken here
            final Future<ManagedKeyValueIterator<Integer, byte[]>> futureIter =
                exec.submit(() -> buffer.all(true));
            final ManagedKeyValueIterator<Integer, byte[]> iter = futureIter.get();

            // Owner stages a new entry after the snapshot was taken
            buffer.stage(2, new byte[]{2});

            // Iterator must not reflect the post-snapshot stage
            assertEquals(List.of(1), drainKeys(iter));
        } finally {
            exec.shutdown();
        }
    }

    @Test
    void nonOwnerRangeSnapshotsAtCreationTime() throws Exception {
        buffer.stage(1, new byte[]{1});

        final ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            final Future<ManagedKeyValueIterator<Integer, byte[]>> futureIter =
                exec.submit(() -> buffer.range(null, null, true, true));
            final ManagedKeyValueIterator<Integer, byte[]> iter = futureIter.get();

            buffer.stage(2, new byte[]{2});

            assertEquals(List.of(1), drainKeys(iter));
        } finally {
            exec.shutdown();
        }
    }

    // -- owner mutate-during-iteration (no CME on the TreeMap staging buffer) --

    @Test
    void ownerStageDuringOwnAllIterationDoesNotThrow() {
        buffer.stage(1, new byte[]{1});
        buffer.stage(3, new byte[]{3});

        final ManagedKeyValueIterator<Integer, byte[]> iter = buffer.all(true);
        assertEquals(1, iter.next().key);

        // Owner structurally mutates the staging buffer while its own iterator is open.
        buffer.stage(2, new byte[]{2});
        buffer.stage(4, new byte[]{4});

        // No ConcurrentModificationException, and the iterator reflects the snapshot at creation
        // (the keys staged after the iterator was opened are absent).
        assertEquals(List.of(3), drainKeys(iter));
    }

    @Test
    void ownerStageDuringOwnRangeIterationDoesNotThrow() {
        buffer.stage(1, new byte[]{1});
        buffer.stage(5, new byte[]{5});

        final ManagedKeyValueIterator<Integer, byte[]> iter = buffer.range(1, 9, true, true);
        assertEquals(1, iter.next().key);

        buffer.stage(3, new byte[]{3});

        assertEquals(List.of(5), drainKeys(iter));
    }

    // -- non-owner snapshot survives a concurrent owner commit --

    @Test
    void nonOwnerAllSurvivesConcurrentOwnerCommit() throws Exception {
        buffer.base.put(2, new byte[]{2});
        buffer.stage(1, new byte[]{1});

        final ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            final ManagedKeyValueIterator<Integer, byte[]> iter =
                exec.submit(() -> buffer.all(true)).get();

            // Owner commits (flushes staging to base and clears pendingWrites) while the non-owner
            // snapshot iterator is open.
            buffer.commit();

            // Snapshot captured at creation time: staged key 1 plus base key 2.
            assertEquals(List.of(1, 2), drainKeys(iter));
        } finally {
            exec.shutdown();
        }
    }

    // -- concurrency stress: owner writer + non-owner readers on the TreeMap staging buffer --

    @Test
    void concurrentOwnerWritesAndNonOwnerReadsAreConsistent() throws Exception {
        final int readerCount = 4;
        final ExecutorService exec = Executors.newFixedThreadPool(readerCount);
        final AtomicBoolean stop = new AtomicBoolean(false);
        final List<Future<?>> readers = new ArrayList<>();
        try {
            for (int r = 0; r < readerCount; r++) {
                readers.add(exec.submit(() -> {
                    while (!stop.get()) {
                        buffer.get(7);
                        // Non-owner scan → snapshotScan: must never throw and must be sorted.
                        final List<Integer> keys = drainKeys(buffer.all(true));
                        for (int i = 1; i < keys.size(); i++) {
                            if (keys.get(i - 1) >= keys.get(i)) {
                                throw new AssertionError("non-owner scan returned unsorted keys: " + keys);
                            }
                        }
                    }
                    return null;
                }));
            }

            for (int i = 0; i < 10_000; i++) {
                buffer.stage(i % 64, new byte[]{(byte) i});
                if (i % 200 == 0) {
                    buffer.commit();
                }
                if (i % 500 == 0) {
                    buffer.rollback();
                }
            }
            stop.set(true);
            for (final Future<?> reader : readers) {
                reader.get(); // surfaces any AssertionError / ConcurrentModificationException
            }
        } finally {
            exec.shutdownNow();
        }
    }

    // -- Helpers --

    private static List<Integer> drainKeys(final ManagedKeyValueIterator<Integer, byte[]> iter) {
        final List<Integer> keys = new ArrayList<>();
        while (iter.hasNext()) {
            keys.add(iter.next().key);
        }
        return keys;
    }

    // -- Test double --

    static class TestBuffer extends AbstractTransactionBuffer<Integer> {
        final TreeMap<Integer, byte[]> base = new TreeMap<>();
        private final TreeMap<Integer, Optional<byte[]>> pendingBatch = new TreeMap<>();

        @Override
        void stageToBackend(final Integer key, final byte[] value) {
            pendingBatch.put(key, Optional.ofNullable(value));
        }

        @Override
        ManagedKeyValueIterator<Integer, byte[]> newBaseIterator(final Integer from, final Integer to) {
            return newBaseIterator(from, to, true, true);
        }

        @Override
        ManagedKeyValueIterator<Integer, byte[]> newBaseIterator(
            final Integer from, final Integer to, final boolean forward, final boolean toInclusive) {
            final NavigableMap<Integer, byte[]> view = boundedBaseView(from, to, toInclusive);
            return new TestIterator(forward ? view : view.descendingMap());
        }

        private NavigableMap<Integer, byte[]> boundedBaseView(
            final Integer from, final Integer to, final boolean toInclusive) {
            if (from != null && to != null) {
                return base.subMap(from, true, to, toInclusive);
            } else if (from != null) {
                return base.tailMap(from, true);
            } else if (to != null) {
                return base.headMap(to, toInclusive);
            }
            return base;
        }

        @Override
        ManagedKeyValueIterator<Integer, byte[]> newBaseSnapshotIterator(
            final Integer from, final Integer to, final boolean forward, final boolean toInclusive) {
            final NavigableMap<Integer, byte[]> copy = new TreeMap<>(boundedBaseView(from, to, toInclusive));
            return new TestIterator(forward ? copy : copy.descendingMap());
        }

        @Override
        void flushToBase() {
            for (final Map.Entry<Integer, Optional<byte[]>> entry : pendingBatch.entrySet()) {
                if (entry.getValue().isPresent()) {
                    base.put(entry.getKey(), entry.getValue().get());
                } else {
                    base.remove(entry.getKey());
                }
            }
            pendingBatch.clear();
        }

        @Override
        void discardPendingBatch() {
            pendingBatch.clear();
        }

        @Override
        int estimateKeySize(final Integer key) {
            return Integer.BYTES;
        }
    }

    private static class TestIterator implements ManagedKeyValueIterator<Integer, byte[]> {
        private final Iterator<Map.Entry<Integer, byte[]>> iter;
        private Runnable closeCallback;

        TestIterator(final NavigableMap<Integer, byte[]> view) {
            this.iter = view.entrySet().iterator();
        }

        @Override
        public void onClose(final Runnable closeCallback) {
            this.closeCallback = closeCallback;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public KeyValue<Integer, byte[]> next() {
            final Map.Entry<Integer, byte[]> entry = iter.next();
            return new KeyValue<>(entry.getKey(), entry.getValue());
        }

        @Override
        public Integer peekNextKey() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            if (closeCallback != null) closeCallback.run();
        }
    }
}
