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

package org.apache.kafka.connect.integration;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.utils.Time;

public class StartAndStopCounter {

    private final AtomicInteger startCounter = new AtomicInteger(0);
    private final AtomicInteger stopCounter = new AtomicInteger(0);
    private final List<StartAndStopLatch> restartLatches = new CopyOnWriteArrayList<>();
    private final Time clock;

    public StartAndStopCounter() {
        this(Time.SYSTEM);
    }

    public StartAndStopCounter(Time clock) {
        this.clock = clock != null ? clock : Time.SYSTEM;
    }

    /**
     * Record a start.
     */
    public void recordStart() {
        startCounter.incrementAndGet();
        restartLatches.forEach(StartAndStopLatch::recordStart);
    }

    /**
     * Record a stop.
     */
    public void recordStop() {
        stopCounter.incrementAndGet();
        restartLatches.forEach(StartAndStopLatch::recordStop);
    }

    /**
     * Get the number of starts.
     *
     * @return the number of starts
     */
    public int starts() {
        return startCounter.get();
    }

    /**
     * Get the number of stops.
     *
     * @return the number of stops
     */
    public int stops() {
        return stopCounter.get();
    }

    public StartsAndStops countsSnapshot() {
        return new StartsAndStops(starts(), stops());
    }

    /**
     * Obtain a {@link StartAndStopLatch} that can be used to wait until the expected number of restarts
     * has been completed.
     *
     * @param expectedStarts   the expected number of starts; may be 0
     * @param expectedStops    the expected number of stops; may be 0
     * @return the latch; never null
     */
    public StartAndStopLatch expectedRestarts(int expectedStarts, int expectedStops) {
        return expectedRestarts(expectedStarts, expectedStops, null);
    }

    /**
     * Obtain a {@link StartAndStopLatch} that can be used to wait until the expected number of restarts
     * has been completed.
     *
     * @param expectedStarts   the expected number of starts; may be 0
     * @param expectedStops    the expected number of stops; may be 0
     * @param dependents       any dependent latches that must also complete in order for the
     *                         resulting latch to complete
     * @return the latch; never null
     */
    public StartAndStopLatch expectedRestarts(int expectedStarts, int expectedStops, List<StartAndStopLatch> dependents) {
        StartAndStopLatch latch = new StartAndStopLatch(expectedStarts, expectedStops, this::remove, dependents, clock);
        restartLatches.add(latch);
        return latch;
    }

    /**
     * Obtain a {@link StartAndStopLatch} that can be used to wait until the expected number of restarts
     * has been completed.
     *
     * @param expectedRestarts the expected number of restarts
     * @return the latch; never null
     */
    public StartAndStopLatch expectedRestarts(int expectedRestarts) {
        return expectedRestarts(expectedRestarts, expectedRestarts);
    }

    /**
     * Obtain a {@link StartAndStopLatch} that can be used to wait until the expected number of restarts
     * has been completed.
     *
     * @param expectedRestarts the expected number of restarts
     * @param dependents       any dependent latches that must also complete in order for the
     *                         resulting latch to complete
     * @return the latch; never null
     */
    public StartAndStopLatch expectedRestarts(int expectedRestarts, List<StartAndStopLatch> dependents) {
        return expectedRestarts(expectedRestarts, expectedRestarts, dependents);
    }

    /**
     * Obtain a {@link StartAndStopLatch} that can be used to wait until the expected number of starts
     * has been completed.
     *
     * @param expectedStarts the expected number of starts
     * @return the latch; never null
     */
    public StartAndStopLatch expectedStarts(int expectedStarts) {
        return expectedRestarts(expectedStarts, 0);
    }

    /**
     * Obtain a {@link StartAndStopLatch} that can be used to wait until the expected number of starts
     * has been completed.
     *
     * @param expectedStarts the expected number of starts
     * @param dependents     any dependent latches that must also complete in order for the
     *                       resulting latch to complete
     * @return the latch; never null
     */
    public StartAndStopLatch expectedStarts(int expectedStarts, List<StartAndStopLatch> dependents) {
        return expectedRestarts(expectedStarts, 0, dependents);
    }


    /**
     * Obtain a {@link StartAndStopLatch} that can be used to wait until the expected number of
     * stops has been completed.
     *
     * @param expectedStops the expected number of stops
     * @return the latch; never null
     */
    public StartAndStopLatch expectedStops(int expectedStops) {
        return expectedRestarts(0, expectedStops);
    }

    /**
     * Obtain a {@link StartAndStopLatch} that can be used to wait until the expected number of
     * stops has been completed.
     *
     * @param expectedStops the expected number of stops
     * @param dependents    any dependent latches that must also complete in order for the
     *                      resulting latch to complete
     * @return the latch; never null
     */
    public StartAndStopLatch expectedStops(int expectedStops, List<StartAndStopLatch> dependents) {
        return expectedRestarts(0, expectedStops, dependents);
    }

    protected void remove(StartAndStopLatch restartLatch) {
        restartLatches.remove(restartLatch);
    }
}
