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
package org.apache.kafka.server.util;

import org.apache.kafka.common.utils.Time;

import java.util.Comparator;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * A mock scheduler that executes tasks synchronously using a mock time instance. Tasks are executed synchronously when
 * the time is advanced. This class is meant to be used in conjunction with MockTime.
 *
 * Example usage
 * <code>
 *   val time = new MockTime
 *   time.scheduler.schedule("a task", println("hello world: " + time.milliseconds), delay = 1000)
 *   time.sleep(1001) // this should cause our scheduled task to fire
 * </code>
 *
 * Incrementing the time to the exact next execution time of a task will result in that task executing (it as if execution itself takes no time).
 */
public class MockScheduler implements Scheduler {

    private static class MockTask implements ScheduledFuture<Void> {
        final String name;
        final Runnable task;
        final long period;
        final Time time;

        private final AtomicLong nextExecution;

        private MockTask(String name, Runnable task, long nextExecution, long period, Time time) {
            this.name = name;
            this.task = task;
            this.nextExecution = new AtomicLong(nextExecution);
            this.period = period;
            this.time = time;
        }

        /**
         * If this task is periodic, reschedule it and return true. Otherwise, do nothing and return false.
         */
        public boolean rescheduleIfPeriodic() {
            if (periodic()) {
                nextExecution.addAndGet(period);
                return true;
            } else {
                return false;
            }
        }

        public long nextExecution() {
            return nextExecution.get();
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return time.milliseconds() - nextExecution();
        }

        @Override
        public int compareTo(Delayed o) {
            return Long.compare(getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
        }

        /**
         * Not used, so not fully implemented
         */
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public Void get() throws InterruptedException, ExecutionException {
            return null;
        }

        @Override
        public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }

        private boolean periodic() {
            return period >= 0;
        }
    }

    /* a priority queue of tasks ordered by next execution time */
    private final PriorityQueue<MockTask> tasks = new PriorityQueue<>(Comparator.comparing(t -> t.nextExecution()));
    private final Time time;

    public MockScheduler(Time time) {
        this.time = time;
    }

    @Override
    public void startup() {}

    @Override
    public ScheduledFuture<?> schedule(String name, Runnable task, long delayMs, long periodMs) {
        MockTask mockTask = new MockTask(name, task, time.milliseconds() + delayMs, periodMs, time);
        add(mockTask);
        tick();
        return mockTask;
    }

    @Override
    public void shutdown() throws InterruptedException {
        Optional<MockTask> currentTask;
        do {
            currentTask = poll(t -> true);
            currentTask.ifPresent(t -> t.task.run());
        } while (currentTask.isPresent());
    }

    @Override
    public void resizeThreadPool(int newSize) {}

    /**
     * Check for any tasks that need to execute. Since this is a mock scheduler this check only occurs
     * when this method is called and the execution happens synchronously in the calling thread.
     * If you are using the scheduler associated with a MockTime instance this call be triggered automatically.
     */
    public void tick() {
        long now = time.milliseconds();
        Optional<MockTask> currentTask;
        /* pop and execute the task with the lowest next execution time if ready */
        do {
            currentTask = poll(t -> t.nextExecution() <= now);
            currentTask.ifPresent(t -> {
                t.task.run();
                /* if the task is periodic, reschedule it and re-enqueue */
                if (t.rescheduleIfPeriodic())
                    add(t);
            });
        } while (currentTask.isPresent());
    }

    public void clear() {
        synchronized (this) {
            tasks.clear();
        }
    }

    private Optional<MockTask> poll(Predicate<MockTask> predicate) {
        synchronized (this) {
            Optional<MockTask> result = Optional.ofNullable(tasks.peek()).filter(predicate);
            // Remove element from the queue if `predicate` returned `true`
            result.ifPresent(t -> tasks.poll());
            return result;
        }
    }

    private void add(MockTask task) {
        synchronized (this) {
            tasks.add(task);
        }
    }

}
