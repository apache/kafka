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

package org.apache.kafka.deferred;

import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 40)
public class DeferredEventQueueTest {

    static class SampleDeferredEvent implements DeferredEvent {
        final CompletableFuture<Void> future = new CompletableFuture<>();

        @Override
        public void complete(Throwable exception) {
            if (exception != null) {
                future.completeExceptionally(exception);
            } else {
                future.complete(null);
            }
        }
    }

    static class UnstableDeferredEvent extends SampleDeferredEvent {
        @Override
        public boolean allowUnstableCompletion() {
            return true;
        }
    }

    @Test
    public void testCompleteEvents() {
        DeferredEventQueue deferredEventQueue = new DeferredEventQueue(new LogContext());
        SampleDeferredEvent event1 = new SampleDeferredEvent();
        SampleDeferredEvent event2 = new SampleDeferredEvent();
        SampleDeferredEvent event3 = new SampleDeferredEvent();
        deferredEventQueue.add(1, event1);
        assertEquals(OptionalLong.of(1L), deferredEventQueue.highestPendingOffset());
        deferredEventQueue.add(1, event2);
        assertEquals(OptionalLong.of(1L), deferredEventQueue.highestPendingOffset());
        deferredEventQueue.add(3, event3);
        assertEquals(OptionalLong.of(3L), deferredEventQueue.highestPendingOffset());
        deferredEventQueue.completeUpTo(2);
        assertTrue(event1.future.isDone());
        assertTrue(event2.future.isDone());
        assertFalse(event3.future.isDone());
        deferredEventQueue.completeUpTo(4);
        assertTrue(event3.future.isDone());
        assertEquals(OptionalLong.empty(), deferredEventQueue.highestPendingOffset());
    }

    @Test
    public void testCompleteUnstableEvents() {
        DeferredEventQueue deferredEventQueue = new DeferredEventQueue(new LogContext());
        SampleDeferredEvent event1 = new SampleDeferredEvent();
        SampleDeferredEvent event2 = new SampleDeferredEvent();
        SampleDeferredEvent event3 = new SampleDeferredEvent();
        UnstableDeferredEvent event4 = new UnstableDeferredEvent();
        SampleDeferredEvent event5 = new SampleDeferredEvent();
        UnstableDeferredEvent event6 = new UnstableDeferredEvent();
        SampleDeferredEvent event7 = new SampleDeferredEvent();

        Consumer<List<SampleDeferredEvent>> assertEventsNotDone = events ->
            events.forEach(event -> assertFalse(event.future.isDone()));

        deferredEventQueue.add(1, event1);
        deferredEventQueue.add(1, event2);
        deferredEventQueue.add(3, event3);
        deferredEventQueue.add(4, event4);
        deferredEventQueue.add(4, event5);
        deferredEventQueue.add(6, event6);
        deferredEventQueue.add(7, event7);
        assertEquals(OptionalLong.of(7L), deferredEventQueue.highestPendingOffset());
        deferredEventQueue.completeUpTo(2, 2);
        assertTrue(event1.future.isDone());
        assertTrue(event2.future.isDone());
        assertEventsNotDone.accept(Arrays.asList(event3, event4, event5, event6, event7));

        // Only event4 can be completed since its unstable
        deferredEventQueue.completeUpTo(2, 4);
        assertTrue(event4.future.isDone());
        assertEventsNotDone.accept(Arrays.asList(event3, event5, event6, event7));

        deferredEventQueue.completeUpTo(4, 7);
        assertTrue(event3.future.isDone());
        assertTrue(event5.future.isDone());
        assertTrue(event6.future.isDone());
        assertEventsNotDone.accept(Collections.singletonList(event7));

        // Make sure subsequent call is a no-op
        deferredEventQueue.completeUpTo(4, 7);
        assertTrue(event3.future.isDone());
        assertTrue(event5.future.isDone());
        assertTrue(event6.future.isDone());
        assertEventsNotDone.accept(Collections.singletonList(event7));

        deferredEventQueue.completeUpTo(7, 7);
        assertTrue(event7.future.isDone());

        assertEquals(OptionalLong.empty(), deferredEventQueue.highestPendingOffset());
    }

    @Test
    public void testInvalidOffsets() {
        DeferredEventQueue deferredEventQueue = new DeferredEventQueue(new LogContext());
        assertThrows(IllegalArgumentException.class, () ->
            deferredEventQueue.completeUpTo(2, 1));
    }

    @Test
    public void testFailOnIncorrectOrdering() {
        DeferredEventQueue deferredEventQueue = new DeferredEventQueue(new LogContext());
        SampleDeferredEvent event1 = new SampleDeferredEvent();
        SampleDeferredEvent event2 = new SampleDeferredEvent();
        deferredEventQueue.add(2, event1);
        assertThrows(RuntimeException.class, () -> deferredEventQueue.add(1, event2));
    }

    @Test
    public void testFailEvents() {
        DeferredEventQueue deferredEventQueue = new DeferredEventQueue(new LogContext());
        SampleDeferredEvent event1 = new SampleDeferredEvent();
        SampleDeferredEvent event2 = new SampleDeferredEvent();
        SampleDeferredEvent event3 = new SampleDeferredEvent();
        UnstableDeferredEvent event4 = new UnstableDeferredEvent();

        deferredEventQueue.add(1, event1);
        deferredEventQueue.add(3, event2);
        deferredEventQueue.add(3, event3);
        deferredEventQueue.add(3, event4);

        deferredEventQueue.completeUpTo(2);
        assertTrue(event1.future.isDone());
        assertFalse(event2.future.isDone());
        assertFalse(event3.future.isDone());
        deferredEventQueue.failAll(new RuntimeException("failed"));
        assertTrue(event2.future.isCompletedExceptionally());
        assertTrue(event3.future.isCompletedExceptionally());
        assertTrue(event4.future.isCompletedExceptionally());
        assertEquals(RuntimeException.class, assertThrows(ExecutionException.class,
            () -> event2.future.get()).getCause().getClass());
        assertEquals(RuntimeException.class, assertThrows(ExecutionException.class,
            () -> event3.future.get()).getCause().getClass());
        assertEquals(RuntimeException.class, assertThrows(ExecutionException.class,
            () -> event4.future.get()).getCause().getClass());
    }
}
