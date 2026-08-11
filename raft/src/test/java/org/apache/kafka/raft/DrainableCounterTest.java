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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public final class DrainableCounterTest {

    @Test
    public void testDrainDeltaReturnsIncreaseSinceLastDrain() {
        DrainableCounter counter = new DrainableCounter(0);

        assertEquals(3, counter.drainDelta(3));
        assertEquals(2, counter.drainDelta(5));
    }

    @Test
    public void testDrainDeltaIsZeroWhenReadingUnchanged() {
        DrainableCounter counter = new DrainableCounter(7);

        assertEquals(0, counter.drainDelta(7));
        assertEquals(0, counter.drainDelta(7));
    }

    @Test
    public void testDrainDeltaAdvancesTheBaseline() {
        DrainableCounter counter = new DrainableCounter(0);

        assertEquals(6, counter.drainDelta(6));
        // The previous drain advanced the baseline, so the same reading is not counted twice.
        assertEquals(0, counter.drainDelta(6));
    }

    @Test
    public void testBaselineIsSetAtConstruction() {
        DrainableCounter counter = new DrainableCounter(9);

        // The constructor sets the baseline, so the first drain only reflects increases since
        // construction, not the reading's prior history.
        assertEquals(0, counter.drainDelta(9));
        assertEquals(3, counter.drainDelta(12));
    }

    @Test
    public void testIgnoredDrainDeltaExcludesPriorIncreasesFromTheNextDrain() {
        DrainableCounter counter = new DrainableCounter(0);

        assertEquals(3, counter.drainDelta(3));

        // Draining with the result ignored re-baselines the counter, e.g. to exclude work done
        // while setting up a benchmark from the measured region.
        counter.drainDelta(8);
        assertEquals(0, counter.drainDelta(8));

        assertEquals(4, counter.drainDelta(12));
    }

    @Test
    public void testDrainDeltaThrowsWhenReadingDecreases() {
        DrainableCounter counter = new DrainableCounter(10);

        assertThrows(IllegalStateException.class, () -> counter.drainDelta(4));
    }
}
