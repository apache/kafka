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
    private final List<RestartLatch> restartLatches = new CopyOnWriteArrayList<>();
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
        restartLatches.forEach(RestartLatch::recordStart);
    }

    /**
     * Record a stop.
     */
    public void recordStop() {
        stopCounter.incrementAndGet();
        restartLatches.forEach(RestartLatch::recordStop);
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

    /**
     * Obtain a {@link RestartLatch} that can be used to wait until the expected number of restarts
     * has been completed.
     *
     * @param expectedStarts   the expected number of starts; may be 0
     * @param expectedStops    the expected number of stops; may be 0
     * @return the latch; never null
     */
    public RestartLatch expectedRestarts(int expectedStarts, int expectedStops) {
        return expectedRestarts(expectedStarts, expectedStops, null);
    }

    /**
     * Obtain a {@link RestartLatch} that can be used to wait until the expected number of restarts
     * has been completed.
     *
     * @param expectedStarts   the expected number of starts; may be 0
     * @param expectedStops    the expected number of stops; may be 0
     * @param dependents       any dependent latches that must also complete in order for the
     *                         resulting latch to complete
     * @return the latch; never null
     */
    public RestartLatch expectedRestarts(int expectedStarts, int expectedStops, List<RestartLatch> dependents) {
        RestartLatch latch = new RestartLatch(expectedStarts, expectedStops, this::remove, dependents, clock);
        restartLatches.add(latch);
        return latch;
    }

    /**
     * Obtain a {@link RestartLatch} that can be used to wait until the expected number of restarts
     * has been completed.
     *
     * @param expectedRestarts the expected number of restarts
     * @return the latch; never null
     */
    public RestartLatch expectedRestarts(int expectedRestarts) {
        return expectedRestarts(expectedRestarts, expectedRestarts);
    }

    /**
     * Obtain a {@link RestartLatch} that can be used to wait until the expected number of restarts
     * has been completed.
     *
     * @param expectedRestarts the expected number of restarts
     * @param dependents       any dependent latches that must also complete in order for the
     *                         resulting latch to complete
     * @return the latch; never null
     */
    public RestartLatch expectedRestarts(int expectedRestarts, List<RestartLatch> dependents) {
        return expectedRestarts(expectedRestarts, expectedRestarts, dependents);
    }

    /**
     * Obtain a {@link RestartLatch} that can be used to wait until the expected number of starts
     * has been completed.
     *
     * @param expectedStarts    the expected number of starts
     * @return the latch; never null
     */
    public RestartLatch expectedStarts(int expectedStarts) {
        return expectedRestarts(expectedStarts, 0);
    }

    /**
     * Obtain a {@link RestartLatch} that can be used to wait until the expected number of starts
     * has been completed.
     *
     * @param expectedStarts    the expected number of starts
     * @param dependents       any dependent latches that must also complete in order for the
     *                         resulting latch to complete
     * @return the latch; never null
     */
    public RestartLatch expectedStarts(int expectedStarts, List<RestartLatch> dependents) {
        return expectedRestarts(expectedStarts, 0, dependents);
    }

    protected void remove(RestartLatch restartLatch) {
        restartLatches.remove(restartLatch);
    }
}
