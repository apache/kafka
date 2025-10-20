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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.queue.KafkaDeadlineEventQueue.Event;
import org.apache.kafka.server.util.MockTime;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaDeadlineEventQueueTest {
    private KafkaDeadlineEventQueue<Long> queue;
    private final Time mockTime;

    public KafkaDeadlineEventQueueTest() {
        this.mockTime = new MockTime();
        this.queue = new KafkaDeadlineEventQueue<>();
    }

    @Test
    public void testEnqueueAndDequeue() {
        queue.enqueue(new Event<>(mockTime.timer(Long.MAX_VALUE), "insert no deadline task", 10L));
        assertEquals(10L, queue.dequeue(mockTime.milliseconds()).get());
    }

    @Test
    public void testOrder() {
        queue.enqueue(new Event<>(mockTime.timer(20), "insert deadline task 20", 20L));
        queue.enqueue(new Event<>(mockTime.timer(Long.MAX_VALUE), "insert no deadline task", 100L));
        queue.enqueue(new Event<>(mockTime.timer(10), "insert deadline task 10", 10L));
        assertEquals(3, queue.eventQueue().size());
        assertEquals(10L, queue.dequeue(mockTime.milliseconds()).get());
        assertEquals(20L, queue.dequeue(mockTime.milliseconds()).get());
        assertEquals(100L, queue.dequeue(mockTime.milliseconds()).get());
    }

    @Test
    public void testSameTimeIsFIFO() {
        queue.enqueue(new Event<>(mockTime.timer(20), "insert deadline task 20 first", 20L));
        queue.enqueue(new Event<>(mockTime.timer(20), "insert deadline task 20 second", 20L));
        assertEquals(2, queue.eventQueue().size());
        assertEquals("insert deadline task 20 first", queue.dequeueContext().get().getTag());
        assertEquals("insert deadline task 20 second", queue.dequeueContext().get().getTag());
    }

    @Test
    public void testTimeout() {
        queue.enqueue(new Event<>(mockTime.timer(Long.MAX_VALUE), "insert no deadline task", 100L));
        queue.enqueue(new Event<>(mockTime.timer(20), "insert deadline task 20", 20L));
        queue.enqueue(new Event<>(mockTime.timer(10), "insert deadline task 10", 10L));
        mockTime.sleep(10);
        // At the point, queue size still 3 because it timeout element will be removed
        // after call dequeue or checkTimeout
        assertEquals(3, queue.eventQueue().size());
        assertEquals(20L, queue.dequeue(mockTime.milliseconds()).get());
        assertEquals(1, queue.eventQueue().size());
        assertEquals(100L, queue.dequeue(mockTime.milliseconds()).get());
        assertEquals(0, queue.eventQueue().size());
    }

    @Test
    public void testEnqueueAfterCheckTimeout() {
        queue.enqueue(new Event<>(mockTime.timer(Long.MAX_VALUE), "insert no deadline task", Long.MAX_VALUE));
        queue.enqueue(new Event<>(mockTime.timer(20), "insert deadline task 20", 20L));
        queue.enqueue(new Event<>(mockTime.timer(10), "insert deadline task 10", 10L));
        mockTime.sleep(10);
        // At the point, queue size still 3 because it timeout element will be removed
        // after call dequeue or checkTimeout
        assertEquals(3, queue.eventQueue().size());
        assertEquals(20L, queue.dequeue(mockTime.milliseconds()).get());
        queue.enqueue(new Event<>(mockTime.timer(100), "insert no deadline task", 100L));
        assertEquals(2, queue.eventQueue().size());
        assertEquals(100, queue.dequeue(mockTime.milliseconds()).get());
        assertEquals(1, queue.eventQueue().size());
        assertEquals(Long.MAX_VALUE, queue.dequeue(mockTime.milliseconds()).get());
    }


    @Test
    public void testTimeoutConsumerWithDequeue() {
        final AtomicBoolean isConsumered = new AtomicBoolean(false);
        this.queue = new KafkaDeadlineEventQueue<>(c -> isConsumered.set(true));
        queue.enqueue(new Event<>(mockTime.timer(100), "insert no deadline task", 100L));
        mockTime.sleep(100);
        queue.dequeue(mockTime.milliseconds());
        assertEquals(true, isConsumered.get());
    }

    @Test
    public void testTimeoutConsumerWithCheckTimeout() {
        final AtomicBoolean isConsumered = new AtomicBoolean(false);
        this.queue = new KafkaDeadlineEventQueue<>(c -> isConsumered.set(true));
        queue.enqueue(new Event<>(mockTime.timer(100), "insert no deadline task", 100L));
        mockTime.sleep(100);
        queue.checkTimeout(mockTime.milliseconds());
        assertEquals(true, isConsumered.get());
    }
}