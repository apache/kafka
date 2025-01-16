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

package org.apache.kafka.controller;

import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(10)
public class PeriodicTaskControlManagerTest {
    static class FakePeriodicTask {
        final AtomicInteger numCalls;
        final AtomicBoolean continuation = new AtomicBoolean(false);
        final PeriodicTask task;
        final AtomicBoolean shouldFail = new AtomicBoolean(false);

        FakePeriodicTask(
            String name,
            long periodNs
        ) {
            this.numCalls = new AtomicInteger();
            this.task = new PeriodicTask(name,
                () -> {
                    numCalls.addAndGet(1);
                    if (shouldFail.getAndSet(false)) {
                        throw new NullPointerException("uh oh");
                    }
                    return ControllerResult.of(Collections.emptyList(),
                        continuation.getAndSet(false));
                },
                periodNs,
                EnumSet.noneOf(PeriodicTaskFlag.class));
        }
    }

    static class TrackedTask {
        final String tag;
        final long deadlineNs;
        final Supplier<ControllerResult<Void>> op;

        TrackedTask(
            String tag,
            long deadlineNs,
            Supplier<ControllerResult<Void>> op
        ) {
            this.tag = tag;
            this.deadlineNs = deadlineNs;
            this.op = op;
        }
    }

    static class PeriodicTaskControlManagerTestEnv implements PeriodicTaskControlManager.QueueAccessor {
        final MockTime time;

        final PeriodicTaskControlManager manager;

        final TreeMap<Long, List<TrackedTask>> tasks;

        int numCalls = 10_000;

        PeriodicTaskControlManagerTestEnv() {
            this.time = new MockTime(0, 0, 0);
            this.manager = new PeriodicTaskControlManager.Builder().
                setTime(time).
                setQueueAccessor(this).
                build();
            this.tasks = new TreeMap<>();
        }

        @Override
        public void scheduleDeferred(
            String tag,
            long deadlineNs,
            Supplier<ControllerResult<Void>> op
        ) {
            if (numCalls <= 0) {
                throw new RuntimeException("too many deferred calls.");
            }
            numCalls--;
            cancelDeferred(tag);
            TrackedTask task = new TrackedTask(tag, deadlineNs, op);
            tasks.computeIfAbsent(deadlineNs, __ -> new ArrayList<>()).add(task);
        }

        @Override
        public void cancelDeferred(String tag) {
            Iterator<Map.Entry<Long, List<TrackedTask>>> iter = tasks.entrySet().iterator();
            boolean foundTask = false;
            while (iter.hasNext() && (!foundTask)) {
                Map.Entry<Long, List<TrackedTask>> entry = iter.next();
                Iterator<TrackedTask> taskIter = entry.getValue().iterator();
                while (taskIter.hasNext()) {
                    TrackedTask task = taskIter.next();
                    if (task.tag.equals(tag)) {
                        taskIter.remove();
                        foundTask = true;
                        break;
                    }
                }
                if (entry.getValue().isEmpty()) {
                    iter.remove();
                }
            }
        }

        int numDeferred() {
            int count = 0;
            for (List<TrackedTask> taskList : tasks.values()) {
                count += taskList.size();
            }
            return count;
        }

        void advanceTime(long ms) {
            time.sleep(ms);
            while (true) {
                Iterator<Map.Entry<Long, List<TrackedTask>>> iter = tasks.entrySet().iterator();
                if (!iter.hasNext()) {
                    return;
                }
                Map.Entry<Long, List<TrackedTask>> entry = iter.next();
                if (time.nanoseconds() < entry.getKey()) {
                    return;
                }
                if (!entry.getValue().isEmpty()) {
                    Iterator<TrackedTask> taskIter = entry.getValue().iterator();
                    TrackedTask task = taskIter.next();
                    taskIter.remove();
                    try {
                        task.op.get();
                    } catch (Exception e) {
                        // discard exception
                    }
                    continue;
                }
                iter.remove();
            }
        }
    }

    @Test
    public void testActivate() {
        PeriodicTaskControlManagerTestEnv env = new PeriodicTaskControlManagerTestEnv();
        assertFalse(env.manager.active());
        env.manager.activate();
        assertTrue(env.manager.active());
        assertEquals(0, env.numDeferred());
    }

    @Test
    public void testDeactivate() {
        PeriodicTaskControlManagerTestEnv env = new PeriodicTaskControlManagerTestEnv();
        assertFalse(env.manager.active());
        env.manager.activate();
        env.manager.deactivate();
        assertFalse(env.manager.active());
        assertEquals(0, env.numDeferred());
    }

    @Test
    public void testRegisterTaskWhenDeactivated() {
        FakePeriodicTask foo = new FakePeriodicTask("foo", MILLISECONDS.toNanos(100));
        PeriodicTaskControlManagerTestEnv env = new PeriodicTaskControlManagerTestEnv();
        env.manager.registerTask(foo.task);
        assertEquals(0, env.numDeferred());
    }

    @Test
    public void testRegisterTaskWhenActivated() {
        FakePeriodicTask foo = new FakePeriodicTask("foo", MILLISECONDS.toNanos(100));
        PeriodicTaskControlManagerTestEnv env = new PeriodicTaskControlManagerTestEnv();
        env.manager.activate();
        env.manager.registerTask(foo.task);
        assertEquals(1, env.numDeferred());
    }

    @Test
    public void testRegisterTaskWhenActivatedThenDeactivate() {
        FakePeriodicTask foo = new FakePeriodicTask("foo", MILLISECONDS.toNanos(100));
        PeriodicTaskControlManagerTestEnv env = new PeriodicTaskControlManagerTestEnv();
        env.manager.activate();
        env.manager.registerTask(foo.task);
        env.manager.deactivate();
        assertEquals(0, env.numDeferred());
    }

    @Test
    public void testRegisterTaskAndAdvanceTime() {
        FakePeriodicTask foo = new FakePeriodicTask("foo", MILLISECONDS.toNanos(100));
        FakePeriodicTask bar = new FakePeriodicTask("bar", MILLISECONDS.toNanos(50));
        PeriodicTaskControlManagerTestEnv env = new PeriodicTaskControlManagerTestEnv();
        env.manager.activate();
        env.manager.registerTask(foo.task);
        env.manager.registerTask(bar.task);
        assertEquals(2, env.numDeferred());
        env.advanceTime(50);
        assertEquals(0, foo.numCalls.get());
        assertEquals(1, bar.numCalls.get());
        assertEquals(2, env.numDeferred());
        env.advanceTime(50);
        assertEquals(1, foo.numCalls.get());
        assertEquals(2, bar.numCalls.get());
        assertEquals(2, env.numDeferred());
        env.manager.deactivate();
    }

    @Test
    public void testContinuation() {
        FakePeriodicTask foo = new FakePeriodicTask("foo", MILLISECONDS.toNanos(100));
        FakePeriodicTask bar = new FakePeriodicTask("bar", MILLISECONDS.toNanos(50));
        bar.continuation.set(true);
        PeriodicTaskControlManagerTestEnv env = new PeriodicTaskControlManagerTestEnv();
        env.manager.activate();
        env.manager.registerTask(foo.task);
        env.manager.registerTask(bar.task);
        assertEquals(2, env.numDeferred());
        env.advanceTime(50);
        assertEquals(0, foo.numCalls.get());
        assertEquals(1, bar.numCalls.get());
        assertEquals(2, env.numDeferred());
        env.advanceTime(10);
        assertEquals(2, bar.numCalls.get());
        env.advanceTime(40);
        assertEquals(1, foo.numCalls.get());
        assertEquals(2, bar.numCalls.get());
        assertEquals(2, env.numDeferred());
        env.advanceTime(10);
        assertEquals(3, bar.numCalls.get());
        env.manager.deactivate();
    }

    @Test
    public void testRegisterTaskAndUnregister() {
        FakePeriodicTask foo = new FakePeriodicTask("foo", MILLISECONDS.toNanos(100));
        FakePeriodicTask bar = new FakePeriodicTask("bar", MILLISECONDS.toNanos(50));
        PeriodicTaskControlManagerTestEnv env = new PeriodicTaskControlManagerTestEnv();
        env.manager.activate();
        env.manager.registerTask(foo.task);
        env.manager.registerTask(bar.task);
        assertEquals(2, env.numDeferred());
        env.advanceTime(50);
        assertEquals(0, foo.numCalls.get());
        assertEquals(1, bar.numCalls.get());
        env.manager.unregisterTask(foo.task.name());
        assertEquals(1, env.numDeferred());
        env.manager.unregisterTask(bar.task.name());
        assertEquals(0, env.numDeferred());
        env.advanceTime(200);
        assertEquals(0, foo.numCalls.get());
        assertEquals(1, bar.numCalls.get());
        env.manager.deactivate();
    }

    @Test
    public void testReschedulingAfterFailure() {
        FakePeriodicTask foo = new FakePeriodicTask("foo", MILLISECONDS.toNanos(100));
        foo.shouldFail.set(true);
        PeriodicTaskControlManagerTestEnv env = new PeriodicTaskControlManagerTestEnv();
        env.manager.activate();
        env.manager.registerTask(foo.task);
        assertEquals(1, env.numDeferred());
        env.advanceTime(100);
        assertEquals(1, foo.numCalls.get());
        env.advanceTime(300000);
        assertEquals(2, foo.numCalls.get());
        env.manager.deactivate();
    }
}
