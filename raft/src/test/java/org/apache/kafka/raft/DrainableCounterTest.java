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
package org.apache.kafka.raft;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public final class DrainableCounterTest {

    @Test
    public void testDrainDeltaReturnsIncreaseSinceLastDrain() {
        AtomicInteger source = new AtomicInteger();
        DrainableCounter counter = new DrainableCounter(source::get);

        source.addAndGet(3);
        assertEquals(3, counter.drainDelta());

        source.addAndGet(2);
        assertEquals(2, counter.drainDelta());
    }

    @Test
    public void testDrainDeltaIsZeroWhenSourceUnchanged() {
        AtomicInteger source = new AtomicInteger(7);
        DrainableCounter counter = new DrainableCounter(source::get);

        assertEquals(0, counter.drainDelta());
        assertEquals(0, counter.drainDelta());
    }

    @Test
    public void testDrainDeltaAdvancesTheBaseline() {
        AtomicInteger source = new AtomicInteger();
        DrainableCounter counter = new DrainableCounter(source::get);

        source.addAndGet(6);
        assertEquals(6, counter.drainDelta());
        // The previous drainDelta() advanced the baseline, so the same increase is not counted twice.
        assertEquals(0, counter.drainDelta());
    }

    @Test
    public void testBaselineIsSnapshottedAtConstruction() {
        AtomicInteger source = new AtomicInteger(9);
        DrainableCounter counter = new DrainableCounter(source::get);

        // The constructor snapshots the current value, so the first drain only reflects increases
        // since construction, not the source's prior history.
        assertEquals(0, counter.drainDelta());

        source.addAndGet(3);
        assertEquals(3, counter.drainDelta());
    }

    @Test
    public void testIgnoredDrainDeltaExcludesPriorIncreasesFromTheNextDrain() {
        AtomicInteger source = new AtomicInteger();
        DrainableCounter counter = new DrainableCounter(source::get);

        source.addAndGet(3);
        assertEquals(3, counter.drainDelta());

        // Draining with the result ignored re-baselines the counter, e.g. to exclude work done while
        // setting up a benchmark from the measured region.
        source.addAndGet(5);
        counter.drainDelta();
        assertEquals(0, counter.drainDelta());

        source.addAndGet(4);
        assertEquals(4, counter.drainDelta());
    }

    @Test
    public void testDrainDeltaThrowsWhenSourceDecreases() {
        AtomicInteger source = new AtomicInteger(10);
        DrainableCounter counter = new DrainableCounter(source::get);

        source.set(4);
        assertThrows(IllegalStateException.class, counter::drainDelta);
    }
}
