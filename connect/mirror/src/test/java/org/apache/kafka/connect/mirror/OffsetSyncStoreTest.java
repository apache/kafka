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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OffsetSyncStoreTest {

    static TopicPartition tp = new TopicPartition("topic1", 2);

    static class FakeOffsetSyncStore extends OffsetSyncStore {

        FakeOffsetSyncStore() {
            super();
        }

        @Override
        public void start() {
            // do not call super to avoid NPE without a KafkaBasedLog.
            readToEnd = true;
        }

        void sync(TopicPartition topicPartition, long upstreamOffset, long downstreamOffset) {
            OffsetSync offsetSync = new OffsetSync(topicPartition, upstreamOffset, downstreamOffset);
            byte[] key = offsetSync.recordKey();
            byte[] value = offsetSync.recordValue();
            ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test.offsets.internal", 0, 3, key, value);
            handleRecord(record);
        }
    }

    @Test
    public void testOffsetTranslation() {
        try (FakeOffsetSyncStore store = new FakeOffsetSyncStore()) {
            store.start();

            // Emit synced downstream offset without dead-reckoning
            store.sync(tp, 100, 200);
            assertEquals(OptionalLong.of(201), store.translateDownstream(null, tp, 150));

            // Translate exact offsets
            store.sync(tp, 150, 251);
            assertEquals(OptionalLong.of(251), store.translateDownstream(null, tp, 150));

            // Use old offset (5) prior to any sync -> can't translate
            assertEquals(OptionalLong.of(-1), store.translateDownstream(null, tp, 5));

            // Downstream offsets reset
            store.sync(tp, 200, 10);
            assertEquals(OptionalLong.of(10), store.translateDownstream(null, tp, 200));

            // Upstream offsets reset
            store.sync(tp, 20, 20);
            assertEquals(OptionalLong.of(20), store.translateDownstream(null, tp, 20));
        }
    }

    @Test
    public void testNoTranslationIfStoreNotStarted() {
        try (FakeOffsetSyncStore store = new FakeOffsetSyncStore()) {
            // no offsets exist and store is not started
            assertEquals(OptionalLong.empty(), store.translateDownstream(null, tp, 0));
            assertEquals(OptionalLong.empty(), store.translateDownstream(null, tp, 100));
            assertEquals(OptionalLong.empty(), store.translateDownstream(null, tp, 200));

            // read a sync during startup
            store.sync(tp, 100, 200);
            assertEquals(OptionalLong.empty(), store.translateDownstream(null, tp, 0));
            assertEquals(OptionalLong.empty(), store.translateDownstream(null, tp, 100));
            assertEquals(OptionalLong.empty(), store.translateDownstream(null, tp, 200));

            // After the store is started all offsets are visible
            store.start();
            assertEquals(OptionalLong.of(-1), store.translateDownstream(null, tp, 0));
            assertEquals(OptionalLong.of(200), store.translateDownstream(null, tp, 100));
            assertEquals(OptionalLong.of(201), store.translateDownstream(null, tp, 200));
        }
    }

    @Test
    public void testNoTranslationIfNoOffsetSync() {
        try (FakeOffsetSyncStore store = new FakeOffsetSyncStore()) {
            store.start();
            assertEquals(OptionalLong.empty(), store.translateDownstream(null, tp, 0));
        }
    }

    @Test
    public void testPastOffsetTranslation() {
        try (FakeOffsetSyncStore store = new FakeOffsetSyncStore()) {
            long maxOffsetLag = 10;
            int offset = 0;
            for (; offset <= 1000; offset += maxOffsetLag) {
                store.sync(tp, offset, offset);
                assertSparseSyncInvariant(store, tp);
            }
            store.start();

            // After starting but before seeing new offsets, only the latest startup offset can be translated
            assertSparseSync(store, 1000, -1);

            for (; offset <= 10000; offset += maxOffsetLag) {
                store.sync(tp, offset, offset);
                assertSparseSyncInvariant(store, tp);
            }

            // After seeing new offsets, we still cannot translate earlier than the latest startup offset
            // Invariant D: the last sync from the initial read-to-end is still stored
            assertSparseSync(store, 1000, -1);

            // We can translate offsets between the latest startup offset and the latest offset with variable precision
            // Older offsets are less precise and translation ends up farther apart
            assertSparseSync(store, 4840, 1000);
            assertSparseSync(store, 6760, 4840);
            assertSparseSync(store, 8680, 6760);
            assertSparseSync(store, 9160, 8680);
            assertSparseSync(store, 9640, 9160);
            assertSparseSync(store, 9880, 9640);
            assertSparseSync(store, 9940, 9880);
            assertSparseSync(store, 9970, 9940);
            assertSparseSync(store, 9990, 9970);
            assertSparseSync(store, 10000, 9990);

            // Rewinding upstream offsets should clear all historical syncs
            store.sync(tp, 1500, 11000);
            assertSparseSyncInvariant(store, tp);
            assertEquals(OptionalLong.of(-1), store.translateDownstream(null, tp, 1499));
            assertEquals(OptionalLong.of(11000), store.translateDownstream(null, tp, 1500));
            assertEquals(OptionalLong.of(11001), store.translateDownstream(null, tp, 2000));
        }
    }

    @Test
    public void testKeepMostDistinctSyncs() {
        // Under normal operation, the incoming syncs will be regularly spaced and the store should keep a set of syncs
        // which provide the best translation accuracy (expires as few syncs as possible)
        // Each new sync should be added to the cache and expire at most one other sync from the cache
        long iterations = 10000;
        long maxStep = Long.MAX_VALUE / iterations;
        // Test a variety of steps (corresponding to the offset.lag.max configuration)
        for (long step = 1; step < maxStep; step = (step * 2) + 1)  {
            for (long firstOffset = 0; firstOffset < 30; firstOffset++) {
                try (FakeOffsetSyncStore store = new FakeOffsetSyncStore()) {
                    int lastCount = 1;
                    store.start();
                    for (long offset = firstOffset; offset <= iterations; offset += step) {
                        store.sync(tp, offset, offset);
                        // Invariant A: the latest sync is present
                        assertEquals(offset, store.syncFor(tp, 0).upstreamOffset());
                        // Invariant D: the earliest sync is present
                        assertEquals(firstOffset, store.syncFor(tp, 63).upstreamOffset());
                        int count = countDistinctStoredSyncs(store, tp);
                        int diff = count - lastCount;
                        assertTrue(diff >= 0,
                                "Store expired too many syncs: " + diff + " after receiving offset " + offset);
                        lastCount = count;
                    }
                }
            }
        }
    }

    private void assertSparseSync(FakeOffsetSyncStore store, long syncOffset, long previousOffset) {
        assertEquals(OptionalLong.of(previousOffset == -1 ? previousOffset : previousOffset + 1), store.translateDownstream(null, tp, syncOffset - 1));
        assertEquals(OptionalLong.of(syncOffset), store.translateDownstream(null, tp, syncOffset));
        assertEquals(OptionalLong.of(syncOffset + 1), store.translateDownstream(null, tp, syncOffset + 1));
        assertEquals(OptionalLong.of(syncOffset + 1), store.translateDownstream(null, tp, syncOffset + 2));
    }

    private int countDistinctStoredSyncs(FakeOffsetSyncStore store, TopicPartition topicPartition) {
        int count = 1;
        for (int i = 1; i < OffsetSyncStore.SYNCS_PER_PARTITION; i++) {
            if (store.syncFor(topicPartition, i - 1) != store.syncFor(topicPartition, i)) {
                count++;
            }
        }
        return count;
    }

    private void assertSparseSyncInvariant(FakeOffsetSyncStore store, TopicPartition topicPartition) {
        for (int j = 0; j < OffsetSyncStore.SYNCS_PER_PARTITION; j++) {
            for (int i = 0; i < j; i++) {
                long jUpstream = store.syncFor(topicPartition, j).upstreamOffset();
                long iUpstream = store.syncFor(topicPartition, i).upstreamOffset();
                if (jUpstream == iUpstream) {
                    continue;
                }
                int exponent = Math.max(i - 2, 0);
                long iUpstreamLowerBound = jUpstream + (1L << exponent);
                if (iUpstreamLowerBound < 0) {
                    continue;
                }
                assertTrue(
                        iUpstream >= iUpstreamLowerBound,
                        "Invariant C(" + i + "," + j + "): Upstream offset " + iUpstream + " at position " + i
                                + " should be at least " + iUpstreamLowerBound
                                + " (" + jUpstream + " + 2^" + exponent + ")"
                );
                long iUpstreamUpperBound = jUpstream + (1L << j) - (1L << i);
                if (iUpstreamUpperBound < 0)
                    continue;
                assertTrue(
                        iUpstream <= iUpstreamUpperBound,
                        "Invariant B(" + i + "," + j + "): Upstream offset " + iUpstream + " at position " + i
                                + " should be no greater than " + iUpstreamUpperBound
                                + " (" + jUpstream + " + 2^" + j + " - 2^" + i + ")"

                );
            }
        }
    }
}
