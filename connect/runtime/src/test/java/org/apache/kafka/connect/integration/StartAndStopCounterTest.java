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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StartAndStopCounterTest {

    private StartAndStopCounter counter;
    private Time clock;
    private ExecutorService waiters;
    private StartAndStopLatch latch;

    @Before
    public void setup() {
        clock = new MockTime();
        counter = new StartAndStopCounter(clock);
    }

    @After
    public void teardown() {
        if (waiters != null) {
            try {
                waiters.shutdownNow();
            } finally {
                waiters = null;
            }
        }
    }

    @Test
    public void shouldRecordStarts() {
        assertEquals(0, counter.starts());
        counter.recordStart();
        assertEquals(1, counter.starts());
        counter.recordStart();
        assertEquals(2, counter.starts());
        assertEquals(2, counter.starts());
    }

    @Test
    public void shouldRecordStops() {
        assertEquals(0, counter.stops());
        counter.recordStop();
        assertEquals(1, counter.stops());
        counter.recordStop();
        assertEquals(2, counter.stops());
        assertEquals(2, counter.stops());
    }

    @Test
    public void shouldExpectRestarts() throws Exception {
        waiters = Executors.newSingleThreadExecutor();

        latch = counter.expectedRestarts(1);
        Future<Boolean> future = asyncAwait(100, TimeUnit.MILLISECONDS);

        clock.sleep(1000);
        counter.recordStop();
        counter.recordStart();
        assertTrue(future.get(200, TimeUnit.MILLISECONDS));
        assertTrue(future.isDone());
    }
    @Test
    public void shouldFailToWaitForRestartThatNeverHappens() throws Exception {
        waiters = Executors.newSingleThreadExecutor();

        latch = counter.expectedRestarts(1);
        Future<Boolean> future = asyncAwait(100, TimeUnit.MILLISECONDS);

        clock.sleep(1000);
        // Record a stop but NOT a start
        counter.recordStop();
        assertFalse(future.get(200, TimeUnit.MILLISECONDS));
        assertTrue(future.isDone());
    }

    private Future<Boolean> asyncAwait(long duration, TimeUnit unit) {
        return waiters.submit(() -> {
            try {
                return latch.await(duration, unit);
            } catch (InterruptedException e) {
                Thread.interrupted();
                return false;
            }
        });
    }
}
