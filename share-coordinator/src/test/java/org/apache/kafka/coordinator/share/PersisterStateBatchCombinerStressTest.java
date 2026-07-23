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

package org.apache.kafka.coordinator.share;

import org.apache.kafka.server.share.persister.PersisterStateBatch;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PersisterStateBatchCombinerStressTest {

    @Test
    public void manyContiguousSameStateBatchesCoalesceIntoOne() {
        final int batchCount = 10_000;
        List<PersisterStateBatch> input = new ArrayList<>(batchCount);
        for (int i = 0; i < batchCount; i++) {
            long first = i * 10L;
            input.add(new PersisterStateBatch(first, first + 9, (byte) 0, (short) 1));
        }

        List<PersisterStateBatch> result = new PersisterStateBatchCombiner(input, List.of(), -1)
            .combineStateBatches();

        assertEquals(1, result.size(), "all contiguous same-state batches must coalesce");
        PersisterStateBatch only = result.get(0);
        assertEquals(0L, only.firstOffset());
        assertEquals(batchCount * 10L - 1, only.lastOffset());
        assertEquals((byte) 0, only.deliveryState());
        assertEquals((short) 1, only.deliveryCount());
    }

    @Test
    public void manyShuffledSameStateBatchesCoalesceIntoOne() {
        final int batchCount = 5_000;
        List<PersisterStateBatch> input = new ArrayList<>(batchCount);
        for (int i = 0; i < batchCount; i++) {
            long first = i * 10L;
            input.add(new PersisterStateBatch(first, first + 9, (byte) 0, (short) 1));
        }
        Collections.shuffle(input, new Random(42));

        List<PersisterStateBatch> result = new PersisterStateBatchCombiner(input, List.of(), -1)
            .combineStateBatches();

        assertEquals(1, result.size());
        assertEquals(0L, result.get(0).firstOffset());
        assertEquals(batchCount * 10L - 1, result.get(0).lastOffset());
    }

    @Test
    public void alternatingStateBatchesAreNotMerged() {
        final int batchCount = 1_000;
        List<PersisterStateBatch> input = new ArrayList<>(batchCount);
        for (int i = 0; i < batchCount; i++) {
            long first = i * 10L;
            byte state = (byte) (i % 2 == 0 ? 0 : 2);
            input.add(new PersisterStateBatch(first, first + 9, state, (short) 1));
        }

        List<PersisterStateBatch> result = new PersisterStateBatchCombiner(input, List.of(), -1)
            .combineStateBatches();

        assertEquals(batchCount, result.size(), "alternating-state batches must not coalesce");
        for (int i = 0; i < batchCount; i++) {
            assertEquals(i * 10L, result.get(i).firstOffset());
            assertEquals(i * 10L + 9, result.get(i).lastOffset());
        }
    }

    @Test
    public void resultIsSortedDisjointCoverage() {
        final int batchCount = 2_000;
        List<PersisterStateBatch> input = new ArrayList<>(batchCount);
        Random rng = new Random(7);
        for (int i = 0; i < batchCount; i++) {
            long first = rng.nextInt(100_000);
            long last = first + rng.nextInt(50);
            byte state = (byte) (rng.nextInt(4));
            short count = (short) (rng.nextInt(3) + 1);
            input.add(new PersisterStateBatch(first, last, state, count));
        }

        List<PersisterStateBatch> result = new PersisterStateBatchCombiner(input, List.of(), -1)
            .combineStateBatches();

        for (int i = 1; i < result.size(); i++) {
            PersisterStateBatch a = result.get(i - 1);
            PersisterStateBatch b = result.get(i);
            assertTrue(a.firstOffset() <= a.lastOffset(), "batch " + a + " must have first <= last");
            assertTrue(a.lastOffset() < b.firstOffset(),
                "batches must be strictly disjoint: " + a + " vs " + b);
        }
    }
}
