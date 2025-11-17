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

package org.apache.kafka.queue;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.HOURS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 60)
public class KafkaEventQueueTest {
    private static class FutureEvent<T> implements EventQueue.Event {
        private final CompletableFuture<T> future;
        private final Supplier<T> supplier;

        FutureEvent(CompletableFuture<T> future, Supplier<T> supplier) {
            this.future = future;
            this.supplier = supplier;
        }

        @Override
        public void run() {
            T value = supplier.get();
            future.complete(value);
        }

        @Override
        public void handleException(Throwable e) {
            future.completeExceptionally(e);
        }
    }

    private LogContext logContext;

    @AfterAll
    public static void tearDown() throws InterruptedException {
        TestUtils.waitForCondition(
                () -> Thread.getAllStackTraces().keySet().stream()
                        .map(Thread::getName)
                        .noneMatch(t -> t.endsWith(KafkaEventQueue.EVENT_HANDLER_THREAD_SUFFIX)),
                "Thread leak detected"
        );
    }

    @BeforeEach
    public void setUp(TestInfo testInfo) {
        logContext = new LogContext("[KafkaEventQueue test=" + testInfo.getDisplayName() + "]");
    }

    @Test
    public void testCreateAndClose() throws Exception {
        KafkaEventQueue queue =
            new KafkaEventQueue(Time.SYSTEM, logContext, "testCreateAndClose");
        queue.close();
    }

    @Test
    public void testHandleEvents() throws Exception {
        try (KafkaEventQueue queue = new KafkaEventQueue(Time.SYSTEM, logContext, "testHandleEvents")) {
            AtomicInteger numEventsExecuted = new AtomicInteger(0);
            CompletableFuture<Integer> future1 = new CompletableFuture<>();
            queue.prepend(new FutureEvent<>(future1, () -> {
                assertEquals(1, numEventsExecuted.incrementAndGet());
                return 1;
            }));
            CompletableFuture<Integer> future2 = new CompletableFuture<>();
            queue.appendWithDeadline(Time.SYSTEM.nanoseconds() + TimeUnit.SECONDS.toNanos(60),
                    new FutureEvent<>(future2, () -> {
                        assertEquals(2, numEventsExecuted.incrementAndGet());
                        return 2;
                    }));
            CompletableFuture<Integer> future3 = new CompletableFuture<>();
            queue.append(new FutureEvent<>(future3, () -> {
                assertEquals(3, numEventsExecuted.incrementAndGet());
                return 3;
            }));
            assertEquals(Integer.valueOf(1), future1.get());
            assertEquals(Integer.valueOf(3), future3.get());
            assertEquals(Integer.valueOf(2), future2.get());
            CompletableFuture<Integer> future4 = new CompletableFuture<>();
            queue.appendWithDeadline(Time.SYSTEM.nanoseconds() + TimeUnit.SECONDS.toNanos(60),
                    new FutureEvent<>(future4, () -> {
                        assertEquals(4, numEventsExecuted.incrementAndGet());
                        return 4;
                    }));
            future4.get();
        }
    }

    @Test
    public void testTimeouts() throws Exception {
        try (KafkaEventQueue queue = new KafkaEventQueue(Time.SYSTEM, logContext, "testTimeouts")) {
            AtomicInteger numEventsExecuted = new AtomicInteger(0);
            CompletableFuture<Integer> future1 = new CompletableFuture<>();
            queue.append(new FutureEvent<>(future1, () -> {
                assertEquals(1, numEventsExecuted.incrementAndGet());
                return 1;
            }));
            CompletableFuture<Integer> future2 = new CompletableFuture<>();
            queue.append(new FutureEvent<>(future2, () -> {
                assertEquals(2, numEventsExecuted.incrementAndGet());
                Time.SYSTEM.sleep(1);
                return 2;
            }));
            CompletableFuture<Integer> future3 = new CompletableFuture<>();
            queue.appendWithDeadline(Time.SYSTEM.nanoseconds() + 1,
                    new FutureEvent<>(future3, () -> {
                        numEventsExecuted.incrementAndGet();
                        return 3;
                    }));
            CompletableFuture<Integer> future4 = new CompletableFuture<>();
            queue.append(new FutureEvent<>(future4, () -> {
                numEventsExecuted.incrementAndGet();
                return 4;
            }));
            assertEquals(Integer.valueOf(1), future1.get());
            assertEquals(Integer.valueOf(2), future2.get());
            assertEquals(Integer.valueOf(4), future4.get());
            assertEquals(TimeoutException.class,
                    assertThrows(ExecutionException.class,
                            () -> future3.get()).getCause().getClass());
            assertEquals(3, numEventsExecuted.get());
        }
    }

    @Test
    public void testScheduleDeferred() throws Exception {
        try (KafkaEventQueue queue = new KafkaEventQueue(Time.SYSTEM, logContext, "testAppendDeferred")) {

            // Wait for the deferred event to happen after the non-deferred event.
            // It may not happen every time, so we keep trying until it does.
            AtomicLong counter = new AtomicLong(0);
            CompletableFuture<Boolean> future1;
            do {
                counter.addAndGet(1);
                future1 = new CompletableFuture<>();
                queue.scheduleDeferred(null,
                        __ -> OptionalLong.of(Time.SYSTEM.nanoseconds() + 1000000),
                        new FutureEvent<>(future1, () -> counter.get() % 2 == 0));
                CompletableFuture<Long> future2 = new CompletableFuture<>();
                queue.append(new FutureEvent<>(future2, () -> counter.addAndGet(1)));
                future2.get();
            } while (!future1.get());
        }
    }

    private static final long ONE_HOUR_NS = TimeUnit.NANOSECONDS.convert(1, HOURS);

    @Test
    public void testScheduleDeferredWithTagReplacement() throws Exception {
        try (KafkaEventQueue queue = new KafkaEventQueue(Time.SYSTEM, logContext, "testScheduleDeferredWithTagReplacement")) {

            AtomicInteger ai = new AtomicInteger(0);
            CompletableFuture<Integer> future1 = new CompletableFuture<>();
            queue.scheduleDeferred("foo",
                    __ -> OptionalLong.of(Time.SYSTEM.nanoseconds() + ONE_HOUR_NS),
                    new FutureEvent<>(future1, () -> ai.addAndGet(1000)));
            CompletableFuture<Integer> future2 = new CompletableFuture<>();
            queue.scheduleDeferred("foo", prev -> OptionalLong.of(prev.orElse(0) - ONE_HOUR_NS),
                    new FutureEvent<>(future2, () -> ai.addAndGet(1)));
            assertFalse(future1.isDone());
            assertEquals(Integer.valueOf(1), future2.get());
            assertEquals(1, ai.get());
        }
    }

    @Test
    public void testDeferredIsQueuedAfterTriggering() throws Exception {
        MockTime time = new MockTime(0, 100000, 1);
        try (KafkaEventQueue queue = new KafkaEventQueue(time, logContext, "testDeferredIsQueuedAfterTriggering")) {
            AtomicInteger count = new AtomicInteger(0);
            List<CompletableFuture<Integer>> futures = List.of(
                    new CompletableFuture<>(),
                    new CompletableFuture<>(),
                    new CompletableFuture<>());
            queue.scheduleDeferred("foo", __ -> OptionalLong.of(2L),
                    new FutureEvent<>(futures.get(0), () -> count.getAndIncrement()));
            queue.append(new FutureEvent<>(futures.get(1), () -> count.getAndAdd(1)));
            assertEquals(Integer.valueOf(0), futures.get(1).get());
            time.sleep(1);
            queue.append(new FutureEvent<>(futures.get(2), () -> count.getAndAdd(1)));
            assertEquals(Integer.valueOf(1), futures.get(0).get());
            assertEquals(Integer.valueOf(2), futures.get(2).get());
        }
    }

    @Test
    public void testShutdownBeforeDeferred() throws Exception {
        try (KafkaEventQueue queue = new KafkaEventQueue(Time.SYSTEM, logContext, "testShutdownBeforeDeferred")) {
            final AtomicInteger count = new AtomicInteger(0);
            CompletableFuture<Integer> future = new CompletableFuture<>();
            queue.scheduleDeferred("myDeferred",
                    __ -> OptionalLong.of(Time.SYSTEM.nanoseconds() + HOURS.toNanos(1)),
                    new FutureEvent<>(future, () -> count.getAndAdd(1)));
            queue.beginShutdown("testShutdownBeforeDeferred");
            assertEquals(RejectedExecutionException.class, assertThrows(ExecutionException.class, () -> future.get()).getCause().getClass());
            assertEquals(0, count.get());
        }
    }

    @Test
    public void testRejectedExecutionException() throws Exception {
        KafkaEventQueue queue = new KafkaEventQueue(Time.SYSTEM, logContext,
            "testRejectedExecutionException");
        queue.close();
        CompletableFuture<Void> future = new CompletableFuture<>();
        queue.append(new EventQueue.Event() {
                @Override
                public void run() {
                    future.complete(null);
                }

                @Override
                public void handleException(Throwable e) {
                    future.completeExceptionally(e);
                }
            });
        assertEquals(RejectedExecutionException.class, assertThrows(
            ExecutionException.class, () -> future.get()).getCause().getClass());
    }

    @Test
    public void testSize() throws Exception {
        try (KafkaEventQueue queue = new KafkaEventQueue(Time.SYSTEM, logContext, "testEmpty")) {
            assertTrue(queue.isEmpty());
            CompletableFuture<Void> future = new CompletableFuture<>();
            queue.append(() -> future.get());
            assertFalse(queue.isEmpty());
            assertEquals(1, queue.size());
            queue.append(() -> future.get());
            assertEquals(2, queue.size());
            future.complete(null);
            TestUtils.waitForCondition(() -> queue.isEmpty(), "Failed to see the queue become empty.");
            queue.scheduleDeferred("later",
                    __ -> OptionalLong.of(Time.SYSTEM.nanoseconds() + HOURS.toNanos(1)),
                    () -> {
                    });
            assertFalse(queue.isEmpty());
            queue.scheduleDeferred("soon",
                    __ -> OptionalLong.of(Time.SYSTEM.nanoseconds() + TimeUnit.MILLISECONDS.toNanos(1)),
                    () -> {
                    });
            assertFalse(queue.isEmpty());
            queue.cancelDeferred("later");
            queue.cancelDeferred("soon");
            TestUtils.waitForCondition(() -> queue.isEmpty(), "Failed to see the queue become empty.");
            assertTrue(queue.isEmpty());
        }
    }

    /**
     * Test that we continue handling events after Event#handleException itself throws an exception.
     */
    @Test
    public void testHandleExceptionThrowingAnException() throws Exception {
        try (KafkaEventQueue queue = new KafkaEventQueue(Time.SYSTEM, logContext, "testHandleExceptionThrowingAnException")) {
            CompletableFuture<Void> initialFuture = new CompletableFuture<>();
            queue.append(() -> initialFuture.get());
            AtomicInteger counter = new AtomicInteger(0);
            queue.append(new EventQueue.Event() {
                @Override
                public void run() {
                    counter.incrementAndGet();
                    throw new IllegalStateException("First exception");
                }

                @Override
                public void handleException(Throwable e) {
                    if (e instanceof IllegalStateException) {
                        counter.incrementAndGet();
                        throw new RuntimeException("Second exception");
                    }
                }
            });
            queue.append(() -> counter.incrementAndGet());
            assertEquals(3, queue.size());
            initialFuture.complete(null);
            TestUtils.waitForCondition(() -> counter.get() == 3,
                    "Failed to see all events execute as planned.");
        }
    }

    private static class InterruptibleEvent implements EventQueue.Event {
        private final CompletableFuture<Void> runFuture;
        private final CompletableFuture<Thread> queueThread;
        private final AtomicInteger numCallsToRun;
        private final AtomicInteger numInterruptedExceptionsSeen;

        InterruptibleEvent(
            CompletableFuture<Thread> queueThread,
            AtomicInteger numCallsToRun,
            AtomicInteger numInterruptedExceptionsSeen
        ) {
            this.runFuture = new CompletableFuture<>();
            this.queueThread = queueThread;
            this.numCallsToRun = numCallsToRun;
            this.numInterruptedExceptionsSeen = numInterruptedExceptionsSeen;
        }

        @Override
        public void run() throws Exception {
            numCallsToRun.incrementAndGet();
            queueThread.complete(Thread.currentThread());
            runFuture.get();
        }

        @Override
        public void handleException(Throwable e) {
            if (e instanceof InterruptedException) {
                numInterruptedExceptionsSeen.incrementAndGet();
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    public void testInterruptedExceptionHandling() throws Exception {
        try (KafkaEventQueue queue = new KafkaEventQueue(Time.SYSTEM, logContext, "testInterruptedExceptionHandling")) {
            CompletableFuture<Thread> queueThread = new CompletableFuture<>();
            AtomicInteger numCallsToRun = new AtomicInteger(0);
            AtomicInteger numInterruptedExceptionsSeen = new AtomicInteger(0);
            queue.append(new InterruptibleEvent(queueThread, numCallsToRun, numInterruptedExceptionsSeen));
            queue.append(new InterruptibleEvent(queueThread, numCallsToRun, numInterruptedExceptionsSeen));
            queue.append(new InterruptibleEvent(queueThread, numCallsToRun, numInterruptedExceptionsSeen));
            queue.append(new InterruptibleEvent(queueThread, numCallsToRun, numInterruptedExceptionsSeen));
            queueThread.get().interrupt();
            TestUtils.retryOnExceptionWithTimeout(30000,
                    () -> assertEquals(1, numCallsToRun.get()));
            TestUtils.retryOnExceptionWithTimeout(30000,
                    () -> assertEquals(3, numInterruptedExceptionsSeen.get()));
        }
    }

    static class ExceptionTrapperEvent implements EventQueue.Event {
        final CompletableFuture<Throwable> exception = new CompletableFuture<>();

        @Override
        public void run() throws Exception {
            exception.complete(null);
        }

        @Override
        public void handleException(Throwable e) {
            exception.complete(e);
        }
    }

    @Test
    public void testInterruptedWithEmptyQueue() throws Exception {
        CompletableFuture<Void> cleanupFuture = new CompletableFuture<>();
        try (KafkaEventQueue queue = new KafkaEventQueue(Time.SYSTEM, logContext, "testInterruptedWithEmptyQueue", () -> cleanupFuture.complete(null))) {
            CompletableFuture<Thread> queueThread = new CompletableFuture<>();
            queue.append(() -> queueThread.complete(Thread.currentThread()));
            TestUtils.retryOnExceptionWithTimeout(30000, () -> assertEquals(0, queue.size()));
            queueThread.get().interrupt();
            cleanupFuture.get();
            ExceptionTrapperEvent ieTrapper = new ExceptionTrapperEvent();
            queue.append(ieTrapper);
            assertEquals(InterruptedException.class, ieTrapper.exception.get().getClass());
            queue.close();
            ExceptionTrapperEvent reTrapper = new ExceptionTrapperEvent();
            queue.append(reTrapper);
            assertEquals(RejectedExecutionException.class, reTrapper.exception.get().getClass());
        }
    }

    @Test
    public void testInterruptedWithDeferredEvents() throws Exception {
        CompletableFuture<Void> cleanupFuture = new CompletableFuture<>();
        try (KafkaEventQueue queue = new KafkaEventQueue(Time.SYSTEM, logContext, "testInterruptedWithDeferredEvents", () -> cleanupFuture.complete(null))) {
            CompletableFuture<Thread> queueThread = new CompletableFuture<>();
            queue.append(() -> queueThread.complete(Thread.currentThread()));
            ExceptionTrapperEvent ieTrapper1 = new ExceptionTrapperEvent();
            ExceptionTrapperEvent ieTrapper2 = new ExceptionTrapperEvent();
            queue.scheduleDeferred("ie2",
                    __ -> OptionalLong.of(Time.SYSTEM.nanoseconds() + HOURS.toNanos(2)),
                    ieTrapper2);
            queue.scheduleDeferred("ie1",
                    __ -> OptionalLong.of(Time.SYSTEM.nanoseconds() + HOURS.toNanos(1)),
                    ieTrapper1);
            TestUtils.retryOnExceptionWithTimeout(30000, () -> assertEquals(2, queue.size()));
            queueThread.get().interrupt();
            cleanupFuture.get();
            assertEquals(InterruptedException.class, ieTrapper1.exception.get().getClass());
            assertEquals(InterruptedException.class, ieTrapper2.exception.get().getClass());
        }
    }

    @Test
    public void testIdleTimeCallback() throws Exception {
        MockTime time = new MockTime();
        AtomicLong lastIdleTimeMs = new AtomicLong(0);
        AtomicLong lastCurrentTimeMs = new AtomicLong(0);

        try (KafkaEventQueue queue = new KafkaEventQueue(
                time,
                logContext,
                "testIdleTimeCallback",
                EventQueue.VoidEvent.INSTANCE,
                (idleDuration, currentTime) -> {
                    lastIdleTimeMs.set(idleDuration);
                    lastCurrentTimeMs.set(currentTime);
                })) {
            time.sleep(2);
            assertEquals(0, lastIdleTimeMs.get(), "Last idle time should be 0ms");

            // Test 1: Two events with a wait in between using FutureEvent
            CompletableFuture<String> event1 = new CompletableFuture<>();
            queue.append(new FutureEvent<>(event1, () -> {
                time.sleep(1);
                return "event1-processed";
            }));
            assertEquals("event1-processed", event1.get());

            long timeBeforeWait = time.milliseconds();
            long waitTime5Ms = 5;
            time.sleep(waitTime5Ms);
            CompletableFuture<String> event2 = new CompletableFuture<>();
            queue.append(new FutureEvent<>(event2, () -> {
                time.sleep(1);
                return "event2-processed";
            }));
            assertEquals("event2-processed", event2.get());
            assertEquals(waitTime5Ms, lastIdleTimeMs.get(), "Idle time should be " + waitTime5Ms + "ms, was: " + lastIdleTimeMs.get());
            assertEquals(timeBeforeWait + waitTime5Ms, lastCurrentTimeMs.get(), "Current time should be " + (timeBeforeWait + waitTime5Ms) + "ms, was: " + lastCurrentTimeMs.get());

            // Test 2: Deferred event
            long timeBeforeDeferred = time.milliseconds();
            long waitTime2Ms = 2;
            CompletableFuture<Void> deferredEvent2 = new CompletableFuture<>();
            queue.scheduleDeferred("deferred2",
                    __ -> OptionalLong.of(time.nanoseconds() + TimeUnit.MILLISECONDS.toNanos(waitTime2Ms)),
                    () -> deferredEvent2.complete(null));
            time.sleep(waitTime2Ms);
            deferredEvent2.get();
            assertEquals(waitTime2Ms, lastIdleTimeMs.get(), "Idle time should be " + waitTime2Ms + "ms, was: " + lastIdleTimeMs.get());
            assertEquals(timeBeforeDeferred + waitTime2Ms, lastCurrentTimeMs.get(), "Current time should be " + (timeBeforeDeferred + waitTime2Ms) + "ms, was: " + lastCurrentTimeMs.get());
        }
    }
}
