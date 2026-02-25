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

import org.apache.kafka.common.utils.MockTime;

import java.util.ArrayDeque;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Non-concurrent implementation of an event executor.
 *
 * This implementation if useful for testing functionality that uses an event executor.
 *
 * Tasks submitted to this event executor will be executed when poll is called. To executed delay
 *  tasks make sure to sleep the mock time before calling poll.
 */
public final class MockEventExecutor implements EventExecutor {
    private final MockTime time;
    private final Queue<Runnable> fifo = new ArrayDeque<>();
    private final PriorityQueue<ScheduledRunnable> delayed = new PriorityQueue<>();
    private Optional<CompletableFuture<Void>> shutdownFuture = Optional.empty();

    private final class ScheduledRunnable implements Runnable, Delayed {
        private final Runnable runnable;
        private final long expires;
        private final CompletableFuture<?> future;

        ScheduledRunnable(CompletableFuture<?> future, Runnable runnable, long delay, TimeUnit timeUnit) {
            this.runnable = runnable;
            this.expires = time.milliseconds() + timeUnit.toMillis(delay);
            this.future = future;
        }

        @Override
        public void run() {
            runnable.run();
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(expires - time.milliseconds(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            ScheduledRunnable that = (ScheduledRunnable) other;
            return Long.compare(expires, that.expires);
        }

        public CompletableFuture<?> future() {
            return future;
        }
    }


    public MockEventExecutor(MockTime time) {
        this.time = time;
    }

    @Override
    public CompletableFuture<Void> submit(Runnable task) {
        return submit(new VoidCallable(task));
    }

    @Override
    public <T> CompletableFuture<T> submit(Callable<T> task) {
        ensureAccepting();

        CompletableFuture<T> future = new CompletableFuture<>();
        fifo.add(completioner(future, task));
        return future;
    }

    @Override
    public CompletableFuture<Void> schedule(Runnable task, long delay, TimeUnit unit) {
        return schedule(new VoidCallable(task), delay, unit);
    }

    @Override
    public <T> CompletableFuture<T> schedule(Callable<T> task, long delay, TimeUnit unit) {
        ensureAccepting();

        CompletableFuture<T> future = new CompletableFuture<>();
        delayed.add(new ScheduledRunnable(future, completioner(future, task), delay, unit));
        return future;
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        if (!shutdownFuture.isPresent()) {
            shutdownFuture = Optional.of(new CompletableFuture<>());
        }

        // forget all the tasks and do not cancel them
        delayed.clear();

        // immediately complete the shutdown future if the fifo queue is empty
        if (fifo.isEmpty()) {
            shutdownFuture.get().complete(null);
        }

        return shutdownFuture.get();
    }

    /**
     * Executes a task.
     *
     * @return true if a task was executed by this call; false otherwise
     */
    public boolean poll() {
        // Add to the fifo queue any task with an expired delay
        while (!delayed.isEmpty() && delayed.peek().getDelay(TimeUnit.MILLISECONDS) <= 0) {
            fifo.add(delayed.poll());
        }

        Runnable runnable = fifo.poll();
        boolean ranTask = false;
        if (runnable != null) {
            runnable.run();
            ranTask = true;
        }

        if (shutdownFuture.isPresent() && fifo.isEmpty()) {
            shutdownFuture.get().complete(null);
        }

        return ranTask;
    }

    private void ensureAccepting() {
        if (shutdownFuture.isPresent()) {
            throw new RejectedExecutionException("event executor shutting down");
        }
    }

    private static <T> Runnable completioner(CompletableFuture<T> future, Callable<T> task) {
        return () -> {
            try {
                if (!future.isDone()) {
                    future.complete(task.call());
                }
            } catch (Throwable e) {
                future.completeExceptionally(e);
            }
        };
    }
}
