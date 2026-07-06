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

public final class DrainableCounterTest {

    @Test
    public void testDeltaReturnsIncreaseSinceLastDelta() {
        AtomicInteger source = new AtomicInteger();
        DrainableCounter counter = new DrainableCounter(source::get);
        counter.reset();

        source.addAndGet(3);
        assertEquals(3, counter.delta());

        source.addAndGet(2);
        assertEquals(2, counter.delta());
    }

    @Test
    public void testDeltaIsZeroWhenSourceUnchanged() {
        AtomicInteger source = new AtomicInteger(7);
        DrainableCounter counter = new DrainableCounter(source::get);
        counter.reset();

        assertEquals(0, counter.delta());
        assertEquals(0, counter.delta());
    }

    @Test
    public void testDeltaAdvancesTheBaseline() {
        AtomicInteger source = new AtomicInteger();
        DrainableCounter counter = new DrainableCounter(source::get);
        counter.reset();

        source.addAndGet(6);
        assertEquals(6, counter.delta());
        assertEquals(0, counter.delta());
    }

    @Test
    public void testResetExcludesPriorIncreasesFromTheNextDelta() {
        AtomicInteger source = new AtomicInteger();
        DrainableCounter counter = new DrainableCounter(source::get);

        // e.g. work done while setting up a benchmark, before the measured region begins
        source.addAndGet(5);
        counter.reset();
        assertEquals(0, counter.delta());

        source.addAndGet(4);
        assertEquals(4, counter.delta());
    }

    @Test
    public void testBaselineStartsAtZeroWithoutReset() {
        AtomicInteger source = new AtomicInteger(9);
        DrainableCounter counter = new DrainableCounter(source::get);

        assertEquals(9, counter.delta());
    }

    @Test
    public void testResetDiscardsIncreasesSinceLastDelta() {
        AtomicInteger source = new AtomicInteger();
        DrainableCounter counter = new DrainableCounter(source::get);
        counter.reset();

        source.addAndGet(3);
        assertEquals(3, counter.delta());

        // Increases that happen between a delta and a reset are never counted.
        source.addAndGet(5);
        counter.reset();
        assertEquals(0, counter.delta());

        source.addAndGet(2);
        assertEquals(2, counter.delta());
    }

    @Test
    public void testDeltaIsCorrectWhenSourceOverflows() {
        AtomicInteger source = new AtomicInteger(Integer.MAX_VALUE);
        DrainableCounter counter = new DrainableCounter(source::get);
        counter.reset();

        source.incrementAndGet();
        assertEquals(1, counter.delta());
    }
}
