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

import org.apache.kafka.common.TopicPartition;
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
        private final TopicPartition key;
        private final CompletableFuture<T> future;
        private final Supplier<T> supplier;
        private final boolean block;
        private final CountDownLatch latch;
        private final CountDownLatch executed;

        FutureEvent(
            TopicPartition key,
            Supplier<T> supplier
        ) {
            this(key, supplier, false);
        }

        FutureEvent(
            TopicPartition key,
            Supplier<T> supplier,
            boolean block
        ) {
            this.key = key;
            this.future = new CompletableFuture<>();
            this.supplier = supplier;
            this.block = block;
            this.latch = new CountDownLatch(1);
            this.executed = new CountDownLatch(1);
        }

        @Override
        public void run() {
            T result = supplier.get();
            executed.countDown();

            if (block) {
                try {
                    latch.await();
                } catch (InterruptedException ex) {
                    // ignore
                }
            }

            future.complete(result);
        }

        @Override
        public void complete(Throwable ex) {
            future.completeExceptionally(ex);
        }

        @Override
        public TopicPartition key() {
            return key;
        }

        public CompletableFuture<T> future() {
            return future;
        }

        public void release() {
            latch.countDown();
        }

        public boolean awaitExecution(long timeout, TimeUnit unit) throws InterruptedException {
            return executed.await(timeout, unit);
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
                new FutureEvent<>(new TopicPartition("foo", 0), numEventsExecuted::incrementAndGet),
                new FutureEvent<>(new TopicPartition("foo", 1), numEventsExecuted::incrementAndGet),
                new FutureEvent<>(new TopicPartition("foo", 2), numEventsExecuted::incrementAndGet),
                new FutureEvent<>(new TopicPartition("foo", 0), numEventsExecuted::incrementAndGet),
                new FutureEvent<>(new TopicPartition("foo", 1), numEventsExecuted::incrementAndGet),
                new FutureEvent<>(new TopicPartition("foo", 2), numEventsExecuted::incrementAndGet)
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
    public void testProcessingGuarantees() throws Exception {
        try (CoordinatorEventProcessor eventProcessor = new MultiThreadedEventProcessor(
            new LogContext(),
            "event-processor-",
            2
        )) {
            AtomicInteger numEventsExecuted = new AtomicInteger(0);

            List<FutureEvent<Integer>> events = Arrays.asList(
                new FutureEvent<>(new TopicPartition("foo", 0), numEventsExecuted::incrementAndGet, true), // Event 0
                new FutureEvent<>(new TopicPartition("foo", 1), numEventsExecuted::incrementAndGet, true), // Event 1
                new FutureEvent<>(new TopicPartition("foo", 0), numEventsExecuted::incrementAndGet, true), // Event 2
                new FutureEvent<>(new TopicPartition("foo", 1), numEventsExecuted::incrementAndGet, true), // Event 3
                new FutureEvent<>(new TopicPartition("foo", 0), numEventsExecuted::incrementAndGet, true), // Event 4
                new FutureEvent<>(new TopicPartition("foo", 1), numEventsExecuted::incrementAndGet, true)  // Event 5
            );

            events.forEach(eventProcessor::enqueue);

            // Events 0 and 1 are executed.
            assertTrue(events.get(0).awaitExecution(5, TimeUnit.SECONDS));
            assertTrue(events.get(1).awaitExecution(5, TimeUnit.SECONDS));

            // Release event 0.
            events.get(0).release();

            // Event 0 is completed.
            int result = events.get(0).future.get(5, TimeUnit.SECONDS);
            assertTrue(result == 1 || result == 2, "Expected 1 or 2 but was " + result);

            // Event 2 is executed.
            assertTrue(events.get(2).awaitExecution(5, TimeUnit.SECONDS));

            // Release event 2.
            events.get(2).release();

            // Event 2 is completed.
            assertEquals(3, events.get(2).future.get(5, TimeUnit.SECONDS));

            // Event 4 is executed.
            assertTrue(events.get(4).awaitExecution(5, TimeUnit.SECONDS));

            // Release event 1.
            events.get(1).release();

            // Event 1 is completed.
            result = events.get(1).future.get(5, TimeUnit.SECONDS);
            assertTrue(result == 1 || result == 2, "Expected 1 or 2 but was " + result);

            // Event 3 is executed.
            assertTrue(events.get(3).awaitExecution(5, TimeUnit.SECONDS));

            // Release event 4.
            events.get(4).release();

            // Event 4 is completed.
            assertEquals(4, events.get(4).future.get(5, TimeUnit.SECONDS));

            // Release event 3.
            events.get(3).release();

            // Event 3 is completed.
            assertEquals(5, events.get(3).future.get(5, TimeUnit.SECONDS));

            // Event 5 is executed.
            assertTrue(events.get(5).awaitExecution(5, TimeUnit.SECONDS));

            // Release event 5.
            events.get(5).release();

            // Event 5 is completed.
            assertEquals(6, events.get(5).future.get(5, TimeUnit.SECONDS));

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
            () -> eventProcessor.enqueue(new FutureEvent<>(new TopicPartition("foo", 0), () -> 0)));
    }

    @Test
    public void testEventsAreDrainedWhenClosed() throws Exception {
        try (MultiThreadedEventProcessor eventProcessor = new MultiThreadedEventProcessor(
            new LogContext(),
            "event-processor-",
            1 // Use a single thread to block event in the processor.
        )) {
            AtomicInteger numEventsExecuted = new AtomicInteger(0);

            // Special event which blocks until the latch is released.
            FutureEvent<Integer> blockingEvent = new FutureEvent<>(new TopicPartition("foo", 0), numEventsExecuted::incrementAndGet, true);

            List<FutureEvent<Integer>> events = Arrays.asList(
                new FutureEvent<>(new TopicPartition("foo", 0), numEventsExecuted::incrementAndGet),
                new FutureEvent<>(new TopicPartition("foo", 0), numEventsExecuted::incrementAndGet),
                new FutureEvent<>(new TopicPartition("foo", 0), numEventsExecuted::incrementAndGet),
                new FutureEvent<>(new TopicPartition("foo", 0), numEventsExecuted::incrementAndGet),
                new FutureEvent<>(new TopicPartition("foo", 0), numEventsExecuted::incrementAndGet),
                new FutureEvent<>(new TopicPartition("foo", 0), numEventsExecuted::incrementAndGet)
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

            // Enqueuing a new event is rejected.
            assertThrows(RejectedExecutionException.class,
                () -> eventProcessor.enqueue(blockingEvent));

            // Release the blocking event to unblock the thread.
            blockingEvent.release();

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
