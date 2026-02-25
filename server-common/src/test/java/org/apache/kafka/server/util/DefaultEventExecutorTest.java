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

import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class DefaultEventExecutorTest {
    int defaultTimeoutSec = 10;

    private DefaultEventExecutor createExecutor(int capacity) {
        return new DefaultEventExecutor(Executors.defaultThreadFactory(), capacity);
    }

    @Test
    void testSubmitRunnable() throws Exception {
        DefaultEventExecutor executor = createExecutor(1);
        CompletableFuture<Void> shutdown;
        try {
            AtomicBoolean executed = new AtomicBoolean(false);
            CompletableFuture<Void> future = executor.submit(() -> executed.set(true));

            // await until future finishes
            assertNull(future.get(defaultTimeoutSec, TimeUnit.SECONDS));
            assertTrue(executed.get());
        } finally {
            shutdown = executor.shutdown();
        }

        // wait for the executor to shutdown
        assertNull(shutdown.get(defaultTimeoutSec, TimeUnit.SECONDS));
    }

    @Test
    void testSubmitRunnableException() throws Exception {
        DefaultEventExecutor executor = createExecutor(1);
        CompletableFuture<Void> shutdown;
        try {
            CompletableFuture<Void> future = executor.submit(() -> {
                throw new RuntimeException();
            });

            ExecutionException exception = assertThrows(
                ExecutionException.class,
                () -> future.get(defaultTimeoutSec, TimeUnit.SECONDS)
            );
            assertEquals(RuntimeException.class, exception.getCause().getClass());
        } finally {
            shutdown = executor.shutdown();
        }

        // wait for the executor to shutdown
        assertNull(shutdown.get(defaultTimeoutSec, TimeUnit.SECONDS));
    }

    @Test
    void testSubmitCallable() throws Exception {
        DefaultEventExecutor executor = createExecutor(1);
        CompletableFuture<Void> shutdown;
        try {
            int expected = 42;
            CompletableFuture<Integer> future = executor.submit(() -> expected);

            // await until future finishes
            assertEquals(expected, future.get(defaultTimeoutSec, TimeUnit.SECONDS));
        } finally {
            shutdown = executor.shutdown();
        }

        // wait for the executor to shutdown
        assertNull(shutdown.get(defaultTimeoutSec, TimeUnit.SECONDS));
    }

    @Test
    void testScheduleRunnable() throws Exception {
        DefaultEventExecutor executor = createExecutor(1);
        CompletableFuture<Void> shutdown;
        try {
            AtomicBoolean executed = new AtomicBoolean(false);
            long delaySec = 1;
            long start = Time.SYSTEM.milliseconds();
            CompletableFuture<Void> future = executor.schedule(() -> executed.set(true), delaySec, TimeUnit.SECONDS);

            // await until future finishes
            assertNull(future.get(defaultTimeoutSec, TimeUnit.SECONDS));
            long now = Time.SYSTEM.milliseconds();
            assertTrue(
                now - start >= TimeUnit.SECONDS.toMillis(delaySec),
                String.format(
                    "now (%s) ms - start (%s) ms (%s) is not greater than delay (%s) seconds",
                    now,
                    start,
                    now - start,
                    delaySec
                )
            );
            assertTrue(executed.get());
        } finally {
            shutdown = executor.shutdown();
        }

        // wait for the executor to shutdown
        assertNull(shutdown.get(defaultTimeoutSec, TimeUnit.SECONDS));
    }

    @Test
    void testScheduleCallable() throws Exception {
        DefaultEventExecutor executor = createExecutor(1);
        CompletableFuture<Void> shutdown;
        try {
            long delaySec = 1;
            int expected = 42;
            long start = Time.SYSTEM.milliseconds();
            CompletableFuture<Integer> future = executor.schedule(() -> expected, delaySec, TimeUnit.SECONDS);

            // await until future finishes
            assertEquals(expected, future.get(defaultTimeoutSec, TimeUnit.SECONDS));
            long now = Time.SYSTEM.milliseconds();
            assertTrue(
                now - start >= TimeUnit.SECONDS.toMillis(delaySec),
                String.format(
                    "now (%s) ms - start (%s) ms (%s) is not greater than delay (%s) seconds",
                    now,
                    start,
                    now - start,
                    delaySec
                )
            );
        } finally {
            shutdown = executor.shutdown();
        }

        // wait for the executor to shutdown
        assertNull(shutdown.get(defaultTimeoutSec, TimeUnit.SECONDS));
    }

    @Test
    void testScheduleCallableException() throws Exception {
        DefaultEventExecutor executor = createExecutor(1);
        CompletableFuture<Void> shutdown;
        try {
            long delaySec = 1;
            long start = Time.SYSTEM.milliseconds();
            CompletableFuture<Integer> future = executor.schedule(
                () -> {
                    throw new RuntimeException();
                },
                delaySec,
                TimeUnit.SECONDS
            );

            // await until future finishes
            ExecutionException exception = assertThrows(
                ExecutionException.class,
                () -> future.get(defaultTimeoutSec, TimeUnit.SECONDS)
            );
            assertEquals(RuntimeException.class, exception.getCause().getClass());
            long now = Time.SYSTEM.milliseconds();
            assertTrue(
                now - start >= TimeUnit.SECONDS.toMillis(delaySec),
                String.format(
                    "now (%s) ms - start (%s) ms (%s) is not greater than delay (%s) seconds",
                    now,
                    start,
                    now - start,
                    delaySec
                )
            );
        } finally {
            shutdown = executor.shutdown();
        }

        // wait for the executor to shutdown
        assertNull(shutdown.get(defaultTimeoutSec, TimeUnit.SECONDS));
    }

    @Test
    void testCancelSubmit() throws Exception {
        DefaultEventExecutor executor = createExecutor(2);
        CompletableFuture<Void> shutdown;
        try {
            // block the executor so that the next task is not picked up
            CountDownLatch startLatch = new CountDownLatch(1);
            CompletableFuture<Void> latchFuture = executor.submit(() -> assertDoesNotThrow(() -> startLatch.await()));

            AtomicBoolean executed = new AtomicBoolean(false);
            CompletableFuture<Void> future = executor.submit(() -> executed.set(true));

            assertFalse(executed.get());
            assertFalse(future.isDone());

            future.cancel(false);
            startLatch.countDown();

            // the blocking future finished
            assertNull(latchFuture.get(defaultTimeoutSec, TimeUnit.SECONDS));

            // the future was canceled and didn't execute
            assertFalse(executed.get());
            assertTrue(future.isDone());
            assertTrue(future.isCancelled());
            assertTrue(future.isCompletedExceptionally());
        } finally {
            shutdown = executor.shutdown();
        }

        // wait for the executor to shutdown
        assertNull(shutdown.get(defaultTimeoutSec, TimeUnit.SECONDS));
    }

    @Test
    void testCancelSchedule() throws Exception {
        DefaultEventExecutor executor = createExecutor(2);
        CompletableFuture<Void> shutdown;
        try {
            // block the executor so that the next task is not picked up
            CountDownLatch startLatch = new CountDownLatch(1);
            CompletableFuture<Void> latchFuture = executor.submit(() -> assertDoesNotThrow(() -> startLatch.await()));

            AtomicBoolean executed = new AtomicBoolean(false);
            long delayMs = 100;
            CompletableFuture<Void> future = executor.schedule(() -> executed.set(true), delayMs, TimeUnit.MILLISECONDS);

            assertFalse(executed.get());
            assertFalse(future.isDone());

            future.cancel(false);
            startLatch.countDown();

            // the blocking future finished
            assertNull(latchFuture.get(defaultTimeoutSec, TimeUnit.SECONDS));

            // the future didn't execute and was canceled
            assertFalse(executed.get());
            assertTrue(future.isDone());
            assertTrue(future.isCancelled());
            assertTrue(future.isCompletedExceptionally());
        } finally {
            shutdown = executor.shutdown();
        }

        // wait for the executor to shutdown
        assertNull(shutdown.get(defaultTimeoutSec, TimeUnit.SECONDS));
    }

    @Test
    void testCapacitySubmit() throws Exception {
        DefaultEventExecutor executor = createExecutor(1);
        CompletableFuture<Void> shutdown;
        try {
            // block the executor so that the next task is not picked up
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch scheduledLatch = new CountDownLatch(1);
            CompletableFuture<Void> latchFuture = executor.submit(() -> {
                scheduledLatch.countDown();
                assertDoesNotThrow(() -> startLatch.await());
            });

            // wait for the blocking task to get removed from the queue
            scheduledLatch.await();
            // add task to queue
            CompletableFuture<Void> future = executor.submit(() -> null);
            // show that capacity is reached
            assertThrows(RejectedExecutionException.class, () -> executor.submit(() -> null));
            assertThrows(RejectedExecutionException.class, () -> executor.schedule(() -> null, 1, TimeUnit.NANOSECONDS));

            startLatch.countDown();

            // wait for all futures to finish
            assertNull(latchFuture.get(defaultTimeoutSec, TimeUnit.SECONDS));
            assertNull(future.get(defaultTimeoutSec, TimeUnit.SECONDS));

            // executor is able to accept new tasks
            executor.submit(() -> null);
        } finally {
            shutdown = executor.shutdown();
        }

        // wait for the executor to shutdown
        assertNull(shutdown.get(defaultTimeoutSec, TimeUnit.SECONDS));
    }

    @Test
    void testCapacitySchedule() throws Exception {
        DefaultEventExecutor executor = createExecutor(1);
        CompletableFuture<Void> shutdown;
        try {
            // block the executor so that the next task is not picked up
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch scheduledLatch = new CountDownLatch(1);
            CompletableFuture<Void> latchFuture = executor.schedule(
                () -> {
                    scheduledLatch.countDown();
                    assertDoesNotThrow(() -> startLatch.await());
                },
                1,
                TimeUnit.NANOSECONDS
            );

            // wait for the blocking task to get removed from the queue
            scheduledLatch.await();
            // add task to queue
            CompletableFuture<Void> future = executor.schedule(() -> null, 1, TimeUnit.NANOSECONDS);
            // show that capacity is reached
            assertThrows(RejectedExecutionException.class, () -> executor.submit(() -> null));
            assertThrows(RejectedExecutionException.class, () -> executor.schedule(() -> null, 1, TimeUnit.NANOSECONDS));

            startLatch.countDown();

            // wait for all futures to finish
            assertNull(latchFuture.get(defaultTimeoutSec, TimeUnit.SECONDS));
            assertNull(future.get(defaultTimeoutSec, TimeUnit.SECONDS));

            // executor is able to accept new tasks
            executor.submit(() -> null);
        } finally {
            shutdown = executor.shutdown();
        }

        // wait for the executor to shutdown
        assertNull(shutdown.get(defaultTimeoutSec, TimeUnit.SECONDS));
    }

    @Test
    void testShutdown() throws Exception {
        DefaultEventExecutor executor = createExecutor(1);
        CompletableFuture<Void> shutdown;
        try {
            // schedule a task far in the future
            AtomicBoolean executed = new AtomicBoolean(false);
            long delaySec = 1000;
            CompletableFuture<Void> future = executor.schedule(
                () -> executed.set(true),
                delaySec,
                TimeUnit.SECONDS
            );

            // shutdown executor
            shutdown = executor.shutdown();

            // scheduling new tasks is not allowed
            assertThrows(RejectedExecutionException.class, () -> executor.submit(() -> null));

            // wait for the executor to shutdown
            assertNull(shutdown.get(defaultTimeoutSec, TimeUnit.SECONDS));

            // verify that the delayed task didn't execute and wasn't canceled
            assertFalse(executed.get());
            assertFalse(future.isDone());
            assertFalse(future.isCancelled());
            assertFalse(future.isCompletedExceptionally());
        } finally {
            shutdown = executor.shutdown();
        }

        // wait for the executor to shutdown
        assertNull(shutdown.get(defaultTimeoutSec, TimeUnit.SECONDS));
    }
}
