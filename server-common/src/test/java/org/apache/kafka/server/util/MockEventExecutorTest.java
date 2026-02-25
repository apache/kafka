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

package org.apache.kafka.server.util;

import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MockEventExecutorTest {
    private MockTime time;
    private MockEventExecutor executor;

    @BeforeEach
    void setUp() {
        time = new MockTime();
        executor = new MockEventExecutor(time);
    }

    @Test
    void testSubmitRunnable() {
        AtomicBoolean executed = new AtomicBoolean(false);
        CompletableFuture<Void> future = executor.submit(() -> executed.set(true));

        assertFalse(executed.get());
        assertFalse(future.isDone());

        assertTrue(executor.poll());

        assertTrue(executed.get());
        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertFalse(future.isCompletedExceptionally());
    }

    @Test
    void testSubmitRunnableException() {
        CompletableFuture<Void> future = executor.submit(() -> {
            throw new RuntimeException();
        });

        assertFalse(future.isDone());

        assertTrue(executor.poll());

        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertTrue(future.isCompletedExceptionally());
        CompletionException exception = assertThrows(CompletionException.class, future::join);
        assertEquals(RuntimeException.class, exception.getCause().getClass());
    }

    @Test
    void testSubmitCallable() {
        int expected = 42;
        CompletableFuture<Integer> future = executor.submit(() -> expected);

        assertFalse(future.isDone());

        assertTrue(executor.poll());

        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertFalse(future.isCompletedExceptionally());
        assertEquals(expected, future.join());
    }

    @Test
    void testScheduleRunnable() {
        AtomicBoolean executed = new AtomicBoolean(false);
        long delayMs = 100;
        CompletableFuture<Void> future = executor.schedule(() -> executed.set(true), delayMs, TimeUnit.MILLISECONDS);

        assertFalse(executed.get());
        assertFalse(future.isDone());

        assertFalse(executor.poll());

        assertFalse(executed.get());
        assertFalse(future.isDone());

        time.sleep(delayMs - 1);
        assertFalse(executor.poll());

        assertFalse(executed.get());
        assertFalse(future.isDone());

        time.sleep(1);
        assertTrue(executor.poll());

        assertTrue(executed.get());
        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertFalse(future.isCompletedExceptionally());
    }

    @Test
    void testScheduleCallable() {
        long delayMs = 100;
        int expected = 42;
        CompletableFuture<Integer> future = executor.schedule(() -> expected, delayMs, TimeUnit.MILLISECONDS);

        assertFalse(future.isDone());

        assertFalse(executor.poll());

        assertFalse(future.isDone());

        time.sleep(delayMs - 1);
        assertFalse(executor.poll());

        assertFalse(future.isDone());

        time.sleep(1);
        assertTrue(executor.poll());

        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertFalse(future.isCompletedExceptionally());
        assertEquals(expected, future.join());
    }

    @Test
    void testScheduleCallableException() {
        long delayMs = 100;
        CompletableFuture<Integer> future = executor.schedule(
            () -> {
                throw new RuntimeException();
            },
            delayMs,
            TimeUnit.MILLISECONDS
        );

        assertFalse(future.isDone());

        assertFalse(executor.poll());

        assertFalse(future.isDone());

        time.sleep(delayMs - 1);
        assertFalse(executor.poll());

        assertFalse(future.isDone());

        time.sleep(1);
        assertTrue(executor.poll());

        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertTrue(future.isCompletedExceptionally());
        CompletionException exception = assertThrows(CompletionException.class, future::join);
        assertEquals(RuntimeException.class, exception.getCause().getClass());
    }

    @Test
    void testCancelSubmit() {
        AtomicBoolean executed = new AtomicBoolean(false);
        CompletableFuture<Void> future = executor.submit(() -> executed.set(true));

        assertFalse(executed.get());
        assertFalse(future.isDone());

        future.cancel(false);
        assertTrue(executor.poll());

        assertFalse(executed.get());
        assertTrue(future.isDone());
        assertTrue(future.isCancelled());
        assertTrue(future.isCompletedExceptionally());
    }

    @Test
    void testCancelSchedule() {
        AtomicBoolean executed = new AtomicBoolean(false);
        long delayMs = 100;
        CompletableFuture<Void> future = executor.schedule(() -> executed.set(true), delayMs, TimeUnit.MILLISECONDS);

        assertFalse(executed.get());
        assertFalse(future.isDone());

        time.sleep(delayMs - 1);
        assertFalse(executor.poll());

        assertFalse(executed.get());
        assertFalse(future.isDone());

        future.cancel(false);
        time.sleep(1);
        assertTrue(executor.poll());

        assertFalse(executed.get());
        assertTrue(future.isDone());
        assertTrue(future.isCancelled());
        assertTrue(future.isCompletedExceptionally());
    }

    @Test
    void testShutdown() {
        AtomicBoolean delayedExecuted = new AtomicBoolean(false);
        long delayMs = 100;
        CompletableFuture<Void> delayed = executor.schedule(
            () -> delayedExecuted.set(true),
            delayMs,
            TimeUnit.MILLISECONDS
        );

        AtomicBoolean veryDelayedExecuted = new AtomicBoolean(false);
        long veryDelayMs = 2 * delayMs;
        CompletableFuture<Void> veryDelayed = executor.schedule(
            () -> veryDelayedExecuted.set(true),
            veryDelayMs,
            TimeUnit.MILLISECONDS
        );

        AtomicBoolean submittedExecuted = new AtomicBoolean(false);
        CompletableFuture<Void> submitted = executor.submit(() -> submittedExecuted.set(true));

        time.sleep(delayMs);
        // executes submitted and queues delayed
        assertTrue(executor.poll());

        // cancels veryDelayed
        CompletableFuture<Void> shutdown = executor.shutdown();
        // executes delayed
        assertTrue(executor.poll());

        // the delayed task should have executed
        assertTrue(delayedExecuted.get());
        assertTrue(delayed.isDone());
        assertFalse(delayed.isCancelled());

        // the submitted task should have executed
        assertTrue(submittedExecuted.get());
        assertTrue(submitted.isDone());
        assertFalse(submitted.isCancelled());

        // the very delayed task didn't execute and was not canceled
        assertFalse(veryDelayedExecuted.get());
        assertFalse(veryDelayed.isDone());
        assertFalse(veryDelayed.isCancelled());
        assertFalse(veryDelayed.isCompletedExceptionally());

        // check that the shutdown future completed
        assertTrue(shutdown.isDone());
        assertFalse(shutdown.isCancelled());
        assertFalse(shutdown.isCompletedExceptionally());
    }

    @Test
    void testNoOpPoll() {
        assertFalse(executor.poll());
    }
}
