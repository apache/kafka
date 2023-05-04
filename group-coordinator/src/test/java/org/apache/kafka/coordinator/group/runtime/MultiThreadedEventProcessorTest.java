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
package org.apache.kafka.coordinator.group.runtime;

import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 60)
public class MultiThreadedEventProcessorTest {

    private static class FutureEvent<T> implements CoordinatorEvent {
        private final int key;
        private final CompletableFuture<T> future;
        private final Supplier<T> supplier;

        FutureEvent(
            int key,
            Supplier<T> supplier
        ) {
            this.key = key;
            this.future = new CompletableFuture<>();
            this.supplier = supplier;
        }

        @Override
        public void run() {
            future.complete(supplier.get());
        }

        @Override
        public void complete(Throwable ex) {
            future.completeExceptionally(ex);
        }

        @Override
        public Integer key() {
            return key;
        }

        public CompletableFuture<T> future() {
            return future;
        }

        @Override
        public String toString() {
            return "FutureEvent(key=" + key + ")";
        }
    }

    @Test
    public void testCreateAndClose() throws Exception {
        CoordinatorEventProcessor eventProcessor = new MultiThreadedEventProcessor(
            new LogContext(),
            "event-processor-",
            2
        );
        eventProcessor.close();
    }

    @Test
    public void testEventsAreProcessed() throws Exception {
        try (CoordinatorEventProcessor eventProcessor = new MultiThreadedEventProcessor(
            new LogContext(),
            "event-processor-",
            2
        )) {
            AtomicInteger numEventsExecuted = new AtomicInteger(0);

            List<FutureEvent<Integer>> events = Arrays.asList(
                new FutureEvent<>(0, numEventsExecuted::incrementAndGet),
                new FutureEvent<>(1, numEventsExecuted::incrementAndGet),
                new FutureEvent<>(2, numEventsExecuted::incrementAndGet),
                new FutureEvent<>(0, numEventsExecuted::incrementAndGet),
                new FutureEvent<>(1, numEventsExecuted::incrementAndGet),
                new FutureEvent<>(2, numEventsExecuted::incrementAndGet)
            );

            events.forEach(eventProcessor::enqueue);

            CompletableFuture.allOf(events
                .stream()
                .map(FutureEvent::future)
                .toArray(CompletableFuture[]::new)
            ).get(10, TimeUnit.SECONDS);

            events.forEach(event -> {
                assertTrue(event.future.isDone());
                assertFalse(event.future.isCompletedExceptionally());
            });

            assertEquals(events.size(), numEventsExecuted.get());
        }
    }

    @Test
    public void testEventsAreRejectedWhenClosed() throws Exception {
        CoordinatorEventProcessor eventProcessor = new MultiThreadedEventProcessor(
            new LogContext(),
            "event-processor-",
            2
        );

        eventProcessor.close();

        assertThrows(RejectedExecutionException.class,
            () -> eventProcessor.enqueue(new FutureEvent<>(0, () -> 0)));
    }

    @Test
    public void testEventsAreDrainedWhenClosed() throws Exception {
        try (MultiThreadedEventProcessor eventProcessor = new MultiThreadedEventProcessor(
            new LogContext(),
            "event-processor-",
            1
        )) {
            AtomicInteger numEventsExecuted = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(1);

            // Special event which blocks until the latch is released.
            FutureEvent<Integer> blockingEvent = new FutureEvent<>(0, () -> {
                numEventsExecuted.incrementAndGet();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    // Ignore.
                }
                return 0;
            });

            List<FutureEvent<Integer>> events = Arrays.asList(
                new FutureEvent<>(0, numEventsExecuted::incrementAndGet),
                new FutureEvent<>(0, numEventsExecuted::incrementAndGet),
                new FutureEvent<>(0, numEventsExecuted::incrementAndGet),
                new FutureEvent<>(0, numEventsExecuted::incrementAndGet),
                new FutureEvent<>(0, numEventsExecuted::incrementAndGet),
                new FutureEvent<>(0, numEventsExecuted::incrementAndGet)
            );

            // Enqueue the blocking event.
            eventProcessor.enqueue(blockingEvent);

            // Ensure that the blocking event is executed.
            waitForCondition(() -> numEventsExecuted.get() > 0,
                "Blocking event not executed.");

            // Enqueue the other events.
            events.forEach(eventProcessor::enqueue);

            // Events should not be completed.
            events.forEach(event -> assertFalse(event.future.isDone()));

            // Initiate the shutting down.
            eventProcessor.beginShutdown();

            // Release the blocking event to unblock
            // the thread.
            latch.countDown();

            // The blocking event should be completed.
            blockingEvent.future.get(DEFAULT_MAX_WAIT_MS, TimeUnit.SECONDS);
            assertTrue(blockingEvent.future.isDone());
            assertFalse(blockingEvent.future.isCompletedExceptionally());

            // The other events should be failed.
            events.forEach(event -> {
                Throwable t = assertThrows(
                    ExecutionException.class,
                    () -> event.future.get(DEFAULT_MAX_WAIT_MS, TimeUnit.SECONDS)
                );
                assertEquals(RejectedExecutionException.class, t.getCause().getClass());
            });

            // The other events should not have been processed.
            assertEquals(1, numEventsExecuted.get());
        }
    }
}
