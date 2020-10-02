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

package org.apache.kafka.common.utils;

import org.apache.kafka.common.errors.TimeoutException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class KafkaEventQueueTest {
    @Rule
    final public Timeout globalTimeout = Timeout.seconds(20);

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
        queue.appendWithDeadline(Time.SYSTEM.nanoseconds() + TimeUnit.SECONDS.toNanos(30),
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
        queue.appendWithDeadline(Time.SYSTEM.nanoseconds() + TimeUnit.SECONDS.toNanos(30),
            new FutureEvent<>(future4, () -> {
                assertEquals(4, numEventsExecuted.incrementAndGet());
                return 4;
            }));
        future4.get();
        queue.beginShutdown();
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
        // It may not happpen every time, so we keep trying until it does.
        AtomicLong counter = new AtomicLong(0);
        CompletableFuture<Boolean> future1;
        do {
            counter.addAndGet(1);
            future1 = new CompletableFuture<>();
            queue.scheduleDeferred(null, __ -> Time.SYSTEM.nanoseconds() + 1000000,
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
        queue.scheduleDeferred("foo", __ -> Time.SYSTEM.nanoseconds() + ONE_HOUR_NS,
                new FutureEvent<>(future1, () -> ai.addAndGet(1000)));
        CompletableFuture<Integer> future2 = new CompletableFuture<>();
        queue.scheduleDeferred("foo", prev -> prev - ONE_HOUR_NS,
                new FutureEvent<>(future2, () -> ai.addAndGet(1)));
        assertThrows(CancellationException.class, () -> future1.get());
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
        queue.scheduleDeferred("foo", __ -> 2L,
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
            __ -> SystemTime.SYSTEM.nanoseconds() + TimeUnit.HOURS.toNanos(1),
            new FutureEvent<>(future, () -> count.getAndAdd(1)));
        queue.beginShutdown();
        assertThrows(ExecutionException.class, () -> future.get());
        assertEquals(0, count.get());
        queue.close();
    }
}
