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
package org.apache.kafka.server.util.timer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TimingWheelTest {

    private TimingWheel timingWheel;
    private AtomicInteger taskCounter;
    private DelayQueue<TimerTaskList> queue;
    private final long startMs = 1000L;
    private final long tickMs = 10L;
    private final int wheelSize = 5;

    @BeforeEach
    void setUp() {
        taskCounter = new AtomicInteger(0);
        queue = new DelayQueue<>();
        timingWheel = new TimingWheel(tickMs, wheelSize, startMs, taskCounter, queue);
    }

    @Test
    void testAddValidTask() {
        // Create task within current time interval
        long expirationMs = startMs + tickMs * 2; // 1020ms
        TimerTask task = new TestTimerTask(tickMs * 2);
        TimerTaskEntry entry = new TimerTaskEntry(task, expirationMs);

        assertTrue(timingWheel.add(entry), "Should successfully add valid task");
        assertFalse(queue.isEmpty());
        assertEquals(1, taskCounter.get());
    }

    @Test
    void testAddExpiredTask() {
        long expirationMs = startMs - 1; // 999ms, less than current time
        TimerTask task = new TestTimerTask(-1);
        TimerTaskEntry entry = new TimerTaskEntry(task, expirationMs);

        assertFalse(timingWheel.add(entry), "Expired task should not be added");
    }

    @Test
    void testAddCancelledTask() {
        long expirationMs = startMs + tickMs * 2;
        TimerTask task = new TestTimerTask(tickMs * 2);
        TimerTaskEntry entry = new TimerTaskEntry(task, expirationMs);

        task.cancel();

        assertFalse(timingWheel.add(entry), "Cancelled task should not be added");
        assertTrue(task.isCancelled(), "Task should be marked as cancelled");
    }

    @Test
    void testAddTaskInCurrentBucket() {
        long expirationMs = startMs + 5; // Within current tick
        TimerTask task = new TestTimerTask(5);
        TimerTaskEntry entry = new TimerTaskEntry(task, expirationMs);

        assertFalse(timingWheel.add(entry), "Task within current tick should be expired immediately");
    }

    @Test
    void testAdvanceClockWithinTick() {
        timingWheel.advanceClock(startMs + 5);

        assertEquals(startMs, timingWheel.currentTimeMs(), "Clock should not advance within the same tick");
    }

    @Test
    void testAdvanceClockToNextTick() {
        timingWheel.advanceClock(startMs + tickMs);

        assertEquals(startMs + tickMs, timingWheel.currentTimeMs(), "Clock should advance to next tick");
    }

    @Test
    void testOverflowWheelCreation() {
        assertNull(timingWheel.overflowWheel(), "Overflow wheel should not exist initially");

        // First overflow task should create parent wheel
        long interval = tickMs * wheelSize;
        long overflowTime = startMs + interval + tickMs;

        TimerTask task = new TestTimerTask(interval + tickMs);
        TimerTaskEntry entry = new TimerTaskEntry(task, overflowTime);

        assertTrue(timingWheel.add(entry));
        assertNotNull(timingWheel.overflowWheel(), "Overflow wheel should be created");

        // Adding second overflow task should use existing parent wheel
        TimingWheel existingOverflowWheel = timingWheel.overflowWheel();
        TimerTask task2 = new TestTimerTask(interval + tickMs + 1);
        TimerTaskEntry entry2 = new TimerTaskEntry(task2, overflowTime + 1);

        assertTrue(timingWheel.add(entry2));
        assertSame(existingOverflowWheel, timingWheel.overflowWheel());
    }

    @Test
    void testAdvanceClockWithOverflowWheel() {
        // Create overflow wheel
        long interval = tickMs * wheelSize;
        long overflowTime = startMs + interval + tickMs;
        TimerTask task = new TestTimerTask(interval + tickMs);
        TimerTaskEntry entry = new TimerTaskEntry(task, overflowTime);
        timingWheel.add(entry);

        assertNotNull(timingWheel.overflowWheel(), "Overflow wheel should be created");

        // Advancing clock should also advance overflow wheel clock
        long advanceTime = startMs + tickMs * wheelSize + 10; // 1060ms
        timingWheel.advanceClock(advanceTime);

        // Verify both wheels advanced
        assertEquals(advanceTime, timingWheel.currentTimeMs(), "Main wheel clock should advance");
        assertEquals(startMs + tickMs * wheelSize, timingWheel.overflowWheel().currentTimeMs(), "Overflow wheel clock should also advance");
    }

    private static class TestTimerTask extends TimerTask {

        TestTimerTask(long delayMs) {
            super(delayMs);
        }

        @Override
        public void run() {
            // No-op
        }
    }
}