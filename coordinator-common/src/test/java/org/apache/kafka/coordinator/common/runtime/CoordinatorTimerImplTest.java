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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.util.timer.MockTimer;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("unchecked")
public class CoordinatorTimerImplTest {
    private static final LogContext LOG_CONTEXT = new LogContext();
    private static final String TIMER_KEY = "timer-key";

    @Test
    public void testTimerSuccessfulLifecycle() throws InterruptedException {
        var mockTimer = new MockTimer();
        var operationCalled = new AtomicBoolean(false);

        CoordinatorShardScheduler<String> scheduler = (operationName, operation) -> {
            var result = operation.generate();
            assertEquals(new CoordinatorResult<>(List.of("record"), null), result);
            operationCalled.set(true);
            return CompletableFuture.completedFuture(null);
        };

        var timer = new CoordinatorTimerImpl<>(
            LOG_CONTEXT,
            mockTimer,
            scheduler
        );

        timer.schedule(
            TIMER_KEY,
            100,
            TimeUnit.MILLISECONDS,
            false,
            () -> new CoordinatorResult<>(List.of("record"), null)
        );

        assertTrue(timer.isScheduled(TIMER_KEY));
        assertEquals(1, timer.size());

        // Advance time to trigger the timer.
        mockTimer.advanceClock(100 + 1);

        assertTrue(operationCalled.get());
        assertFalse(timer.isScheduled(TIMER_KEY));
        assertEquals(0, timer.size());
    }

    @Test
    public void testTimerCancelledBeforeExpiry() throws InterruptedException {
        var mockTimer = new MockTimer();
        var operationCalled = new AtomicBoolean(false);

        CoordinatorShardScheduler<String> scheduler = (operationName, operation) -> {
            operationCalled.set(true);
            return CompletableFuture.completedFuture(null);
        };

        var timer = new CoordinatorTimerImpl<>(
            LOG_CONTEXT,
            mockTimer,
            scheduler
        );

        timer.schedule(
            TIMER_KEY,
            100,
            TimeUnit.MILLISECONDS,
            false,
            () -> new CoordinatorResult<>(List.of("record"), null)
        );

        assertTrue(timer.isScheduled(TIMER_KEY));
        assertEquals(1, timer.size());

        // Cancel before expiry.
        timer.cancel(TIMER_KEY);
        assertFalse(timer.isScheduled(TIMER_KEY));
        assertEquals(0, timer.size());

        // Advance time.
        mockTimer.advanceClock(100 + 1);

        // Operation should not be called.
        assertFalse(operationCalled.get());
    }

    @Test
    public void testTimerCancelledAfterExpiryButBeforeWriteOperation() throws InterruptedException {
        var mockTimer = new MockTimer();
        var operationCalled = new AtomicBoolean(false);
        var rejectedExceptionThrown = new AtomicBoolean(false);
        var timerRef = new AtomicReference<CoordinatorTimerImpl<String>>();

        CoordinatorShardScheduler<String> scheduler = (operationName, operation) -> {
            // Cancel the timer BEFORE executing the write operation.
            // This simulates the case where the timer is cancelled while the write
            // event is waiting to be processed.
            timerRef.get().cancel(TIMER_KEY);

            try {
                operation.generate();
                operationCalled.set(true);
            } catch (RejectedExecutionException e) {
                rejectedExceptionThrown.set(true);
                return CompletableFuture.failedFuture(e);
            }
            return CompletableFuture.completedFuture(null);
        };

        var timer = new CoordinatorTimerImpl<>(
            LOG_CONTEXT,
            mockTimer,
            scheduler
        );
        timerRef.set(timer);

        timer.schedule(
            TIMER_KEY,
            100,
            TimeUnit.MILLISECONDS,
            false,
            () -> new CoordinatorResult<>(List.of("record"), null)
        );

        assertTrue(timer.isScheduled(TIMER_KEY));
        assertEquals(1, timer.size());

        // Advance time to trigger the timer.
        mockTimer.advanceClock(100 + 1);

        // The operation should not have been called because we cancelled
        // the timer before the write operation executed.
        assertFalse(operationCalled.get());
        assertTrue(rejectedExceptionThrown.get());
        assertFalse(timer.isScheduled(TIMER_KEY));
        assertEquals(0, timer.size());
    }

    @Test
    public void testTimerOverridden() throws InterruptedException {
        var mockTimer = new MockTimer();
        var firstOperationCalled = new AtomicBoolean(false);
        var secondOperationCalled = new AtomicBoolean(false);

        CoordinatorShardScheduler<String> scheduler = (operationName, operation) -> {
            try {
                operation.generate();
            } catch (RejectedExecutionException e) {
                // Expected for the overridden timer.
                return CompletableFuture.failedFuture(e);
            }
            return CompletableFuture.completedFuture(null);
        };

        var timer = new CoordinatorTimerImpl<>(
            LOG_CONTEXT,
            mockTimer,
            scheduler
        );

        // Schedule first timer.
        timer.schedule(
            TIMER_KEY,
            100,
            TimeUnit.MILLISECONDS,
            false,
            () -> {
                firstOperationCalled.set(true);
                return new CoordinatorResult<>(List.of("record1"), null);
            }
        );

        // Override with second timer.
        timer.schedule(
            TIMER_KEY,
            200,
            TimeUnit.MILLISECONDS,
            false,
            () -> {
                secondOperationCalled.set(true);
                return new CoordinatorResult<>(List.of("record2"), null);
            }
        );

        assertTrue(timer.isScheduled(TIMER_KEY));
        assertEquals(1, timer.size());

        // Advance time to trigger the second timer.
        mockTimer.advanceClock(200 + 1);

        assertFalse(firstOperationCalled.get());
        assertTrue(secondOperationCalled.get());
        assertFalse(timer.isScheduled(TIMER_KEY));
        assertEquals(0, timer.size());
    }

    @Test
    public void testTimerRetryOnFailure() throws InterruptedException {
        var mockTimer = new MockTimer();
        var callCount = new AtomicInteger(0);

        CoordinatorShardScheduler<String> scheduler = (operationName, operation) -> {
            operation.generate();
            var count = callCount.incrementAndGet();
            if (count == 1) {
                // Fail the first time.
                return CompletableFuture.failedFuture(new RuntimeException("Simulated failure"));
            }
            return CompletableFuture.completedFuture(null);
        };

        var timer = new CoordinatorTimerImpl<>(
            LOG_CONTEXT,
            mockTimer,
            scheduler
        );

        timer.schedule(
            TIMER_KEY,
            100,
            TimeUnit.MILLISECONDS,
            true, // retry enabled
            50,   // retry backoff
            () -> new CoordinatorResult<>(List.of("record"), null)
        );

        assertTrue(timer.isScheduled(TIMER_KEY));
        assertEquals(1, timer.size());

        // Advance time to trigger the first attempt.
        mockTimer.advanceClock(100 + 1);

        assertEquals(1, callCount.get());

        // Advance time for retry backoff.
        mockTimer.advanceClock(50 + 1);

        assertEquals(2, callCount.get());
        assertFalse(timer.isScheduled(TIMER_KEY));
        assertEquals(0, timer.size());
    }

    @Test
    public void testTimerNoRetryOnFailure() throws InterruptedException {
        var mockTimer = new MockTimer();
        var callCount = new AtomicInteger(0);

        CoordinatorShardScheduler<String> scheduler = (operationName, operation) -> {
            operation.generate();
            callCount.incrementAndGet();
            return CompletableFuture.failedFuture(new RuntimeException("Simulated failure"));
        };

        var timer = new CoordinatorTimerImpl<>(
            LOG_CONTEXT,
            mockTimer,
            scheduler
        );

        timer.schedule(
            TIMER_KEY,
            100,
            TimeUnit.MILLISECONDS,
            false, // retry disabled
            () -> new CoordinatorResult<>(List.of("record"), null)
        );

        assertTrue(timer.isScheduled(TIMER_KEY));
        assertEquals(1, timer.size());

        // Advance time to trigger the timer.
        mockTimer.advanceClock(100 + 1);

        assertEquals(1, callCount.get());
        assertFalse(timer.isScheduled(TIMER_KEY));
        assertEquals(0, timer.size());

        // Advance more time - should not retry.
        mockTimer.advanceClock(500 + 1);

        assertEquals(1, callCount.get());
    }

    @Test
    public void testTimerIgnoredOnNotCoordinatorException() throws InterruptedException {
        var mockTimer = new MockTimer();
        var callCount = new AtomicInteger(0);

        CoordinatorShardScheduler<String> scheduler = (operationName, operation) -> {
            operation.generate();
            callCount.incrementAndGet();
            return CompletableFuture.failedFuture(new NotCoordinatorException("Not coordinator"));
        };

        var timer = new CoordinatorTimerImpl<>(
            LOG_CONTEXT,
            mockTimer,
            scheduler
        );

        timer.schedule(
            TIMER_KEY,
            100,
            TimeUnit.MILLISECONDS,
            true, // retry enabled, but should be ignored for NotCoordinatorException
            () -> new CoordinatorResult<>(List.of("record"), null)
        );

        assertTrue(timer.isScheduled(TIMER_KEY));
        assertEquals(1, timer.size());

        // Advance time to trigger the timer.
        mockTimer.advanceClock(100 + 1);

        assertEquals(1, callCount.get());
        assertFalse(timer.isScheduled(TIMER_KEY));
        assertEquals(0, timer.size());

        // Should not retry for NotCoordinatorException.
        mockTimer.advanceClock(500 + 1);

        assertEquals(1, callCount.get());
    }

    @Test
    public void testTimerIgnoredOnCoordinatorLoadInProgressException() throws InterruptedException {
        var mockTimer = new MockTimer();
        var callCount = new AtomicInteger(0);

        CoordinatorShardScheduler<String> scheduler = (operationName, operation) -> {
            operation.generate();
            callCount.incrementAndGet();
            return CompletableFuture.failedFuture(new CoordinatorLoadInProgressException("Loading"));
        };

        var timer = new CoordinatorTimerImpl<>(
            LOG_CONTEXT,
            mockTimer,
            scheduler
        );

        timer.schedule(
            TIMER_KEY,
            100,
            TimeUnit.MILLISECONDS,
            true, // retry enabled, but should be ignored for CoordinatorLoadInProgressException
            () -> new CoordinatorResult<>(List.of("record"), null)
        );

        assertTrue(timer.isScheduled(TIMER_KEY));
        assertEquals(1, timer.size());

        // Advance time to trigger the timer.
        mockTimer.advanceClock(100 + 1);

        assertEquals(1, callCount.get());
        assertFalse(timer.isScheduled(TIMER_KEY));
        assertEquals(0, timer.size());

        // Should not retry for CoordinatorLoadInProgressException.
        mockTimer.advanceClock(500 + 1);

        assertEquals(1, callCount.get());
    }

    @Test
    public void testScheduleIfAbsentWhenAbsent() throws InterruptedException {
        var mockTimer = new MockTimer();
        var operationCalled = new AtomicBoolean(false);

        CoordinatorShardScheduler<String> scheduler = (operationName, operation) -> {
            operation.generate();
            return CompletableFuture.completedFuture(null);
        };

        var timer = new CoordinatorTimerImpl<>(
            LOG_CONTEXT,
            mockTimer,
            scheduler
        );

        timer.scheduleIfAbsent(
            TIMER_KEY,
            100,
            TimeUnit.MILLISECONDS,
            false,
            () -> {
                operationCalled.set(true);
                return new CoordinatorResult<>(List.of("record"), null);
            }
        );

        assertTrue(timer.isScheduled(TIMER_KEY));
        assertEquals(1, timer.size());

        // Advance time to trigger the timer.
        mockTimer.advanceClock(100 + 1);

        assertTrue(operationCalled.get());
        assertFalse(timer.isScheduled(TIMER_KEY));
        assertEquals(0, timer.size());
    }

    @Test
    public void testScheduleIfAbsentWhenPresent() throws InterruptedException {
        var mockTimer = new MockTimer();
        var firstOperationCalled = new AtomicBoolean(false);
        var secondOperationCalled = new AtomicBoolean(false);

        CoordinatorShardScheduler<String> scheduler = (operationName, operation) -> {
            operation.generate();
            return CompletableFuture.completedFuture(null);
        };

        var timer = new CoordinatorTimerImpl<>(
            LOG_CONTEXT,
            mockTimer,
            scheduler
        );

        // Schedule first timer.
        timer.schedule(
            TIMER_KEY,
            100,
            TimeUnit.MILLISECONDS,
            false,
            () -> {
                firstOperationCalled.set(true);
                return new CoordinatorResult<>(List.of("record1"), null);
            }
        );

        assertTrue(timer.isScheduled(TIMER_KEY));
        assertEquals(1, timer.size());

        // Try to schedule second timer with scheduleIfAbsent - should be ignored.
        timer.scheduleIfAbsent(
            TIMER_KEY,
            200,
            TimeUnit.MILLISECONDS,
            false,
            () -> {
                secondOperationCalled.set(true);
                return new CoordinatorResult<>(List.of("record2"), null);
            }
        );

        // Size should still be 1.
        assertEquals(1, timer.size());

        // Advance time to trigger the first timer.
        mockTimer.advanceClock(100 + 1);

        assertTrue(firstOperationCalled.get());
        assertFalse(secondOperationCalled.get());
        assertFalse(timer.isScheduled(TIMER_KEY));
        assertEquals(0, timer.size());
    }

    @Test
    public void testCancelAll() throws InterruptedException {
        var mockTimer = new MockTimer();
        var operationCallCount = new AtomicInteger(0);

        CoordinatorShardScheduler<String> scheduler = (operationName, operation) -> {
            operation.generate();
            operationCallCount.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        };

        var timer = new CoordinatorTimerImpl<>(
            LOG_CONTEXT,
            mockTimer,
            scheduler
        );

        // Schedule multiple timers.
        for (int i = 0; i < 3; i++) {
            timer.schedule(
                TIMER_KEY + i,
                100,
                TimeUnit.MILLISECONDS,
                false,
                () -> new CoordinatorResult<>(List.of("record"), null)
            );
        }

        assertEquals(3, timer.size());

        // Cancel all.
        timer.cancelAll();

        assertEquals(0, timer.size());
        assertFalse(timer.isScheduled(TIMER_KEY + "0"));
        assertFalse(timer.isScheduled(TIMER_KEY + "1"));
        assertFalse(timer.isScheduled(TIMER_KEY + "2"));

        // Advance time - no operations should be called.
        mockTimer.advanceClock(100 + 1);

        assertEquals(0, operationCallCount.get());
    }

    @Test
    public void testDefaultRetryBackoff() throws InterruptedException {
        var mockTimer = new MockTimer();
        var callCount = new AtomicInteger(0);

        CoordinatorShardScheduler<String> scheduler = (operationName, operation) -> {
            operation.generate();
            var count = callCount.incrementAndGet();
            if (count == 1) {
                return CompletableFuture.failedFuture(new RuntimeException("Simulated failure"));
            }
            return CompletableFuture.completedFuture(null);
        };

        var timer = new CoordinatorTimerImpl<>(
            LOG_CONTEXT,
            mockTimer,
            scheduler
        );

        // Use the schedule method without explicit retryBackoff (defaults to 500ms).
        timer.schedule(
            TIMER_KEY,
            100,
            TimeUnit.MILLISECONDS,
            true, // retry enabled
            () -> new CoordinatorResult<>(List.of("record"), null)
        );

        assertTrue(timer.isScheduled(TIMER_KEY));
        assertEquals(1, timer.size());

        // Advance time to trigger the first attempt.
        mockTimer.advanceClock(100 + 1);

        assertEquals(1, callCount.get());

        // Advance time for default retry backoff (500ms).
        mockTimer.advanceClock(500 + 1);

        assertEquals(2, callCount.get());
        assertFalse(timer.isScheduled(TIMER_KEY));
        assertEquals(0, timer.size());
    }

    @Test
    public void testTaskCleanupOnFailedFutureWithoutOperationExecution() throws InterruptedException {
        var mockTimer = new MockTimer();
        var operationCalled = new AtomicBoolean(false);

        // Scheduler returns failed future WITHOUT calling operation.generate().
        // This simulates: (1) wrapped synchronous exceptions, or
        // (2) events failing before being executed.
        CoordinatorShardScheduler<String> scheduler = (operationName, operation) -> {
            // Don't call operation.generate() - simulates event never being executed
            return CompletableFuture.failedFuture(new NotCoordinatorException("Not coordinator"));
        };

        var timer = new CoordinatorTimerImpl<>(
            LOG_CONTEXT,
            mockTimer,
            scheduler
        );

        timer.schedule(
            TIMER_KEY,
            100,
            TimeUnit.MILLISECONDS,
            false,
            () -> {
                operationCalled.set(true);
                return new CoordinatorResult<>(List.of("record"), null);
            }
        );

        assertTrue(timer.isScheduled(TIMER_KEY));
        assertEquals(1, timer.size());

        // Advance time to trigger the timer.
        mockTimer.advanceClock(100 + 1);

        // Operation was never called.
        assertFalse(operationCalled.get());
        // But task should still be removed by exceptionally handler.
        assertFalse(timer.isScheduled(TIMER_KEY));
        assertEquals(0, timer.size());
    }
}
