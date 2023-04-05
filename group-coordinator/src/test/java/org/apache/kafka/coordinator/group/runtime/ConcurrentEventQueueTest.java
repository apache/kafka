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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ConcurrentEventQueueTest {

    private class MockEvent implements ConcurrentEventQueue.Event<Integer> {
        int key;
        int value;

        MockEvent(int key, int value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public Integer key() {
            return key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MockEvent mockEvent = (MockEvent) o;

            if (key != mockEvent.key) return false;
            return value == mockEvent.value;
        }

        @Override
        public int hashCode() {
            int result = key;
            result = 31 * result + value;
            return result;
        }

        @Override
        public String toString() {
            return "MockEvent(key=" + key + ", value=" + value + ')';
        }
    }

    @Test
    public void testBasicOperations() {
        ConcurrentEventQueue<Integer, MockEvent> queue = new ConcurrentEventQueue<>();

        assertEquals(0, queue.size());
        assertNull(queue.poll(0, TimeUnit.MICROSECONDS));

        List<MockEvent> events = Arrays.asList(
            new MockEvent(1, 0),
            new MockEvent(1, 1),
            new MockEvent(1, 2),
            new MockEvent(2, 0),
            new MockEvent(2, 1),
            new MockEvent(2, 3),
            new MockEvent(3, 0),
            new MockEvent(3, 1),
            new MockEvent(3, 2)
        );

        events.forEach(queue::add);
        assertEquals(9, queue.size());

        Set<MockEvent> polledEvents = new HashSet<>();
        for (int i = 0; i < events.size(); i++) {
            MockEvent event = queue.poll(0, TimeUnit.MICROSECONDS);
            assertNotNull(event);
            polledEvents.add(event);
            assertEquals(events.size() - 1 - i, queue.size());
            queue.done(event);
        }

        assertNull(queue.poll(0, TimeUnit.MICROSECONDS));
        assertEquals(new HashSet<>(events), polledEvents);
        assertEquals(0, queue.size());

        queue.close();
    }

    @Test
    public void testKeyConcurrentProcessingAndOrdering() {
        ConcurrentEventQueue<Integer, MockEvent> queue = new ConcurrentEventQueue<>();

        MockEvent event0 = new MockEvent(1, 0);
        MockEvent event1 = new MockEvent(1, 1);
        MockEvent event2 = new MockEvent(1, 2);
        queue.add(event0);
        queue.add(event1);
        queue.add(event2);
        assertEquals(3, queue.size());

        MockEvent event = null;

        // Poll event0.
        event = queue.poll(0, TimeUnit.MICROSECONDS);
        assertEquals(event0, event);

        // Poll returns null because key is inflight.
        assertNull(queue.poll(0, TimeUnit.MICROSECONDS));
        queue.done(event);

        // Poll event1.
        event = queue.poll(0, TimeUnit.MICROSECONDS);
        assertEquals(event1, event);

        // Poll returns null because key is inflight.
        assertNull(queue.poll(0, TimeUnit.MICROSECONDS));
        queue.done(event);

        // Poll event2.
        event = queue.poll(0, TimeUnit.MICROSECONDS);
        assertEquals(event2, event);

        // Poll returns null because key is inflight.
        assertNull(queue.poll(0, TimeUnit.MICROSECONDS));
        queue.done(event);

        queue.close();
    }

    @Test
    public void testDoneUnblockWaitingThreads() throws ExecutionException, InterruptedException, TimeoutException {
        ConcurrentEventQueue<Integer, MockEvent> queue = new ConcurrentEventQueue<>();

        MockEvent event0 = new MockEvent(1, 0);
        MockEvent event1 = new MockEvent(1, 1);
        MockEvent event2 = new MockEvent(1, 2);

        CompletableFuture<MockEvent> future0 = CompletableFuture.supplyAsync(queue::poll);
        CompletableFuture<MockEvent> future1 = CompletableFuture.supplyAsync(queue::poll);
        CompletableFuture<MockEvent> future2 = CompletableFuture.supplyAsync(queue::poll);
        List<CompletableFuture<MockEvent>> futures = Arrays.asList(future0, future1, future2);

        assertFalse(future0.isDone());
        assertFalse(future1.isDone());
        assertFalse(future2.isDone());

        queue.add(event0);
        queue.add(event1);
        queue.add(event2);

        // One future should be completed with event0.
        assertEquals(event0, CompletableFuture
            .anyOf(futures.toArray(new CompletableFuture[0]))
            .get(5, TimeUnit.SECONDS));

        futures = futures.stream().filter(future -> !future.isDone()).collect(Collectors.toList());
        assertEquals(2, futures.size());

        // Processing of event0 is done.
        queue.done(event0);

        // One future should be completed with event1.
        assertEquals(event1, CompletableFuture
            .anyOf(futures.toArray(new CompletableFuture[0]))
            .get(5, TimeUnit.SECONDS));

        futures = futures.stream().filter(future -> !future.isDone()).collect(Collectors.toList());
        assertEquals(1, futures.size());

        // Processing of event1 is done.
        queue.done(event1);

        // One future should be completed with event1.
        assertEquals(event2, CompletableFuture
            .anyOf(futures.toArray(new CompletableFuture[0]))
            .get(5, TimeUnit.SECONDS));

        futures = futures.stream().filter(future -> !future.isDone()).collect(Collectors.toList());
        assertEquals(0, futures.size());

        // Processing of event2 is done.
        queue.done(event2);

        assertEquals(0, queue.size());

        queue.close();
    }

    @Test
    public void testCloseUnblockWaitingThreads() throws ExecutionException, InterruptedException, TimeoutException {
        ConcurrentEventQueue<Integer, MockEvent> queue = new ConcurrentEventQueue<>();

        CompletableFuture<MockEvent> future0 = CompletableFuture.supplyAsync(queue::poll);
        CompletableFuture<MockEvent> future1 = CompletableFuture.supplyAsync(queue::poll);
        CompletableFuture<MockEvent> future2 = CompletableFuture.supplyAsync(queue::poll);

        assertFalse(future0.isDone());
        assertFalse(future1.isDone());
        assertFalse(future2.isDone());

        // Closing should release all the pending futures.
        queue.close();

        assertNull(future0.get(5, TimeUnit.SECONDS));
        assertNull(future1.get(5, TimeUnit.SECONDS));
        assertNull(future2.get(5, TimeUnit.SECONDS));
    }
}
