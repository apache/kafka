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

import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;


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
        public void run() throws Exception {
            T value = supplier.get();
            future.complete(value);
        }

        @Override
        public void handleException(Throwable e) {
            future.completeExceptionally(e);
        }
    }

    @Test
    public void testCreateAndClose() throws Exception {
        KafkaEventQueue queue =
            new KafkaEventQueue(Time.SYSTEM, new LogContext(), "testCreateAndClose");
        queue.close();
    }

    @Test
    public void testHandleEvents() throws Exception {
        KafkaEventQueue queue =
            new KafkaEventQueue(Time.SYSTEM, new LogContext(), "testHandleEvents");
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
        queue.beginShutdown("testHandleEvents");
        queue.close();
    }

    @Test
    public void testTimeouts() throws Exception {
        KafkaEventQueue queue =
            new KafkaEventQueue(Time.SYSTEM, new LogContext(), "testTimeouts");
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
        queue.close();
        assertEquals(3, numEventsExecuted.get());
    }

    @Test
    public void testScheduleDeferred() throws Exception {
        KafkaEventQueue queue =
            new KafkaEventQueue(Time.SYSTEM, new LogContext(), "testAppendDeferred");

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
        queue.close();
    }

    private final static long ONE_HOUR_NS = TimeUnit.NANOSECONDS.convert(1, TimeUnit.HOURS);

    @Test
    public void testScheduleDeferredWithTagReplacement() throws Exception {
        KafkaEventQueue queue = new KafkaEventQueue(Time.SYSTEM, new LogContext(),
            "testScheduleDeferredWithTagReplacement");

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
        queue.close();
    }

    @Test
    public void testDeferredIsQueuedAfterTriggering() throws Exception {
        MockTime time = new MockTime(0, 100000, 1);
        KafkaEventQueue queue = new KafkaEventQueue(time, new LogContext(),
            "testDeferredIsQueuedAfterTriggering");
        AtomicInteger count = new AtomicInteger(0);
        List<CompletableFuture<Integer>> futures = Arrays.asList(
                new CompletableFuture<Integer>(),
                new CompletableFuture<Integer>(),
                new CompletableFuture<Integer>());
        queue.scheduleDeferred("foo", __ -> OptionalLong.of(2L),
            new FutureEvent<>(futures.get(0), () -> count.getAndIncrement()));
        queue.append(new FutureEvent<>(futures.get(1), () -> count.getAndAdd(1)));
        assertEquals(Integer.valueOf(0), futures.get(1).get());
        time.sleep(1);
        queue.append(new FutureEvent<>(futures.get(2), () -> count.getAndAdd(1)));
        assertEquals(Integer.valueOf(1), futures.get(0).get());
        assertEquals(Integer.valueOf(2), futures.get(2).get());
        queue.close();
    }

    @Test
    public void testShutdownBeforeDeferred() throws Exception {
        KafkaEventQueue queue = new KafkaEventQueue(Time.SYSTEM, new LogContext(),
            "testShutdownBeforeDeferred");
        final AtomicInteger count = new AtomicInteger(0);
        CompletableFuture<Integer> future = new CompletableFuture<>();
        queue.scheduleDeferred("myDeferred",
            __ -> OptionalLong.of(Time.SYSTEM.nanoseconds() + TimeUnit.HOURS.toNanos(1)),
            new FutureEvent<>(future, () -> count.getAndAdd(1)));
        queue.beginShutdown("testShutdownBeforeDeferred");
        assertThrows(ExecutionException.class, () -> future.get());
        assertEquals(0, count.get());
        queue.close();
    }

    @Test
    public void testRejectedExecutionExecption() throws Exception {
        KafkaEventQueue queue = new KafkaEventQueue(Time.SYSTEM, new LogContext(),
            "testRejectedExecutionExecption");
        queue.close();
        CompletableFuture<Void> future = new CompletableFuture<>();
        queue.append(new EventQueue.Event() {
                @Override
                public void run() throws Exception {
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
}
