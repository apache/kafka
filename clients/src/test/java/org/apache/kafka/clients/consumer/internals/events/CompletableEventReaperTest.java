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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.internals.ConsumerUtils;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;

import static org.apache.kafka.clients.consumer.internals.events.CompletableEvent.calculateDeadlineMs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CompletableEventReaperTest {

    private final LogContext logContext = new LogContext();
    private final Time time = new MockTime();
    private final CompletableEventReaper reaper = new CompletableEventReaper(logContext);

    @Test
    public void testExpired() {
        // Add a new event to the reaper.
        long timeoutMs = 100;
        UnsubscribeEvent event = new UnsubscribeEvent(calculateDeadlineMs(time.milliseconds(), timeoutMs));
        reaper.add(event);

        // Without any time passing, we check the reaper and verify that the event is not done amd is still
        // being tracked.
        assertEquals(0, reaper.reap(time.milliseconds()));
        assertFalse(event.future().isDone());
        assertEquals(1, reaper.size());

        // Sleep for at least 1 ms. *more* than the timeout so that the event is considered expired.
        time.sleep(timeoutMs + 1);

        // However, until we actually invoke the reaper, the event isn't complete and is still being tracked.
        assertFalse(event.future().isDone());
        assertEquals(1, reaper.size());

        // Call the reaper and validate that the event is now "done" (expired), the correct exception type is
        // thrown, and the event is no longer tracked.
        assertEquals(1, reaper.reap(time.milliseconds()));
        assertTrue(event.future().isDone());
        assertThrows(TimeoutException.class, () -> ConsumerUtils.getResult(event.future()));
        assertEquals(0, reaper.size());
    }

    @Test
    public void testCompleted() {
        // Add a new event to the reaper.
        long timeoutMs = 100;
        UnsubscribeEvent event = new UnsubscribeEvent(calculateDeadlineMs(time.milliseconds(), timeoutMs));
        reaper.add(event);

        // Without any time passing, we check the reaper and verify that the event is not done amd is still
        // being tracked.
        assertEquals(0, reaper.reap(time.milliseconds()));
        assertFalse(event.future().isDone());
        assertEquals(1, reaper.size());

        // We'll cause the event to be completed normally. Note that because we haven't called the reaper, the
        // event is still being tracked.
        event.future().complete(null);
        assertTrue(event.future().isDone());
        assertEquals(1, reaper.size());

        // To ensure we don't accidentally expire an event that completed normally, sleep past the timeout.
        time.sleep(timeoutMs + 1);

        // Call the reaper and validate that the event is not considered expired, but is still no longer tracked.
        assertEquals(0, reaper.reap(time.milliseconds()));
        assertTrue(event.future().isDone());
        assertNull(ConsumerUtils.getResult(event.future()));
        assertEquals(0, reaper.size());
    }

    @Test
    public void testCompletedAndExpired() {
        // Add two events to the reaper. One event will be completed, the other we will let expire.
        long timeoutMs = 100;
        UnsubscribeEvent event1 = new UnsubscribeEvent(calculateDeadlineMs(time.milliseconds(), timeoutMs));
        UnsubscribeEvent event2 = new UnsubscribeEvent(calculateDeadlineMs(time.milliseconds(), timeoutMs));
        reaper.add(event1);
        reaper.add(event2);

        // Without any time passing, we check the reaper and verify that the event is not done amd is still
        // being tracked.
        assertEquals(0, reaper.reap(time.milliseconds()));
        assertFalse(event1.future().isDone());
        assertFalse(event2.future().isDone());
        assertEquals(2, reaper.size());

        // We'll cause the first event to be completed normally, but then sleep past the timer deadline.
        event1.future().complete(null);
        assertTrue(event1.future().isDone());

        time.sleep(timeoutMs + 1);

        // Though the first event is completed, it's still being tracked, along with the second expired event.
        assertEquals(2, reaper.size());

        // Validate that the first (completed) event is not expired, but the second one is expired. In either case,
        // both should be completed and neither should be tracked anymore.
        assertEquals(1, reaper.reap(time.milliseconds()));
        assertTrue(event1.future().isDone());
        assertTrue(event2.future().isDone());
        assertNull(ConsumerUtils.getResult(event1.future()));
        assertThrows(TimeoutException.class, () -> ConsumerUtils.getResult(event2.future()));
        assertEquals(0, reaper.size());
    }

    @Test
    public void testIncompleteQueue() {
        long timeoutMs = 100;
        UnsubscribeEvent event1 = new UnsubscribeEvent(calculateDeadlineMs(time.milliseconds(), timeoutMs));
        UnsubscribeEvent event2 = new UnsubscribeEvent(calculateDeadlineMs(time.milliseconds(), timeoutMs));

        Collection<CompletableApplicationEvent<?>> queue = new ArrayList<>();
        queue.add(event1);
        queue.add(event2);

        // Complete one of our events, just to make sure it isn't inadvertently canceled.
        event1.future().complete(null);

        // In this test, our events aren't tracked in the reaper, just in the queue.
        assertEquals(0, reaper.size());
        assertEquals(2, queue.size());

        // Go ahead and reap the incomplete from the queue.
        assertEquals(1, reaper.reap(queue));

        // The first event was completed, so we didn't expire it in the reaper.
        assertTrue(event1.future().isDone());
        assertFalse(event1.future().isCancelled());
        assertNull(ConsumerUtils.getResult(event1.future()));

        // The second event was incomplete, so it was expired.
        assertTrue(event2.future().isCompletedExceptionally());
        assertThrows(TimeoutException.class, () -> ConsumerUtils.getResult(event2.future()));

        // Because the events aren't tracked in the reaper *and* the queue is cleared as part of the
        // cancellation process, our data structures should both be the same as above.
        assertEquals(0, reaper.size());
        assertEquals(0, queue.size());
    }

    @Test
    public void testIncompleteTracked() {
        // This queue is just here to test the case where the queue is empty.
        Collection<CompletableApplicationEvent<?>> queue = new ArrayList<>();

        // Add two events for the reaper to track.
        long timeoutMs = 100;
        UnsubscribeEvent event1 = new UnsubscribeEvent(calculateDeadlineMs(time.milliseconds(), timeoutMs));
        UnsubscribeEvent event2 = new UnsubscribeEvent(calculateDeadlineMs(time.milliseconds(), timeoutMs));
        reaper.add(event1);
        reaper.add(event2);

        // Complete one of our events, just to make sure it isn't inadvertently canceled.
        event1.future().complete(null);

        // In this test, our events are tracked exclusively in the reaper, not the queue.
        assertEquals(2, reaper.size());

        // Go ahead and reap the incomplete events. Both sets should be zero after that.
        assertEquals(1, reaper.reap(queue));
        assertEquals(0, reaper.size());
        assertEquals(0, queue.size());

        // The first event was completed, so we didn't cancel it in the reaper.
        assertTrue(event1.future().isDone());
        assertNull(ConsumerUtils.getResult(event1.future()));

        // The second event was incomplete, so it was canceled.
        assertTrue(event2.future().isCompletedExceptionally());
        assertThrows(TimeoutException.class, () -> ConsumerUtils.getResult(event2.future()));
    }
}
