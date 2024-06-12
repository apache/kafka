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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class StartAndStopLatchTest {

    private final AtomicBoolean completed = new AtomicBoolean();
    private Time clock;
    private StartAndStopLatch latch;
    private List<StartAndStopLatch> dependents;
    private ExecutorService waiters;
    private Future<Boolean> future;

    @Before
    public void setup() {
        clock = new MockTime();
        waiters = Executors.newSingleThreadExecutor();
    }

    @After
    public void teardown() {
        if (waiters != null) {
            waiters.shutdownNow();
        }
    }

    @Test
    public void shouldReturnFalseWhenAwaitingForStartToNeverComplete() throws Throwable {
        latch = new StartAndStopLatch(1, 1, this::complete, dependents, clock);
        future = asyncAwait(100);
        clock.sleep(10);
        assertFalse(future.get(200, TimeUnit.MILLISECONDS));
        assertTrue(future.isDone());
    }

    @Test
    public void shouldReturnFalseWhenAwaitingForStopToNeverComplete() throws Throwable {
        latch = new StartAndStopLatch(1, 1, this::complete, dependents, clock);
        future = asyncAwait(100);
        latch.recordStart();
        clock.sleep(10);
        assertFalse(future.get(200, TimeUnit.MILLISECONDS));
        assertTrue(future.isDone());
    }

    @Test
    public void shouldReturnTrueWhenAwaitingForStartAndStopToComplete() throws Throwable {
        latch = new StartAndStopLatch(1, 1, this::complete, dependents, clock);
        future = asyncAwait(100);
        latch.recordStart();
        latch.recordStop();
        clock.sleep(10);
        assertTrue(future.get(200, TimeUnit.MILLISECONDS));
        assertTrue(future.isDone());
    }

    @Test
    public void shouldReturnFalseWhenAwaitingForDependentLatchToComplete() throws Throwable {
        StartAndStopLatch depLatch = new StartAndStopLatch(1, 1, this::complete, null, clock);
        dependents = Collections.singletonList(depLatch);
        latch = new StartAndStopLatch(1, 1, this::complete, dependents, clock);

        future = asyncAwait(100);
        latch.recordStart();
        latch.recordStop();
        clock.sleep(10);
        assertFalse(future.get(200, TimeUnit.MILLISECONDS));
        assertTrue(future.isDone());
    }

    @Test
    public void shouldReturnTrueWhenAwaitingForStartAndStopAndDependentLatch() throws Throwable {
        StartAndStopLatch depLatch = new StartAndStopLatch(1, 1, this::complete, null, clock);
        dependents = Collections.singletonList(depLatch);
        latch = new StartAndStopLatch(1, 1, this::complete, dependents, clock);

        future = asyncAwait(100);
        latch.recordStart();
        latch.recordStop();
        depLatch.recordStart();
        depLatch.recordStop();
        clock.sleep(10);
        assertTrue(future.get(200, TimeUnit.MILLISECONDS));
        assertTrue(future.isDone());
    }

    private Future<Boolean> asyncAwait(long duration) {
        return asyncAwait(duration, TimeUnit.MILLISECONDS);
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

    private void complete(StartAndStopLatch latch) {
        completed.set(true);
    }
}
