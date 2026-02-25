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

import java.util.OptionalInt;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class DefaultEventExecutor implements EventExecutor {
    private final ScheduledThreadPoolExecutor scheduler;
    private final int pendingTasksCapacity;

    private final AtomicInteger pendingTasksCounter = new AtomicInteger(0);

    // variables used to reliably shutdown the scheduler
    private OptionalInt prevQueueSize = OptionalInt.empty();
    private final AtomicReference<CompletableFuture<Void>> shutdownFuture = new AtomicReference<>(null);

    public DefaultEventExecutor(
        ThreadFactory threadFactory,
        int pendingTasksCapacity
    ) {
        scheduler = new ScheduledThreadPoolExecutor(1, threadFactory);
        scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        scheduler.setRemoveOnCancelPolicy(true);

        this.pendingTasksCapacity = pendingTasksCapacity;
    }

    @Override
    public CompletableFuture<Void> submit(Runnable task) {
        return submit(new VoidCallable(task));
    }

    @Override
    public <T> CompletableFuture<T> submit(Callable<T> task) {
        ensureAccepting();

        Task<T> submitter = new Task<>(task);
        Future<?> scheduledFuture = scheduler.submit(submitter);
        pendingTasksCounter.incrementAndGet();
        hookupCancellation(scheduledFuture, submitter);

        return submitter.future();
    }

    @Override
    public CompletableFuture<Void> schedule(Runnable task, long delay, TimeUnit unit) {
        return schedule(new VoidCallable(task), delay, unit);
    }

    @Override
    public <T> CompletableFuture<T> schedule(Callable<T> task, long delay, TimeUnit unit) {
        ensureAccepting();

        Task<T> submitter = new Task<>(task);
        ScheduledFuture<?> scheduledFuture = scheduler.schedule(submitter, delay, unit);
        pendingTasksCounter.incrementAndGet();
        hookupCancellation(scheduledFuture, submitter);

        return submitter.future();
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        if (shutdownFuture.compareAndSet(null, new CompletableFuture<>())) {
            scheduleShutdown();
        }

        // must be non-null
        return shutdownFuture.get();
    }

    private void scheduleShutdown() {
        scheduler.submit(() -> {
            int currentSize = scheduler.getQueue().size();
            if (prevQueueSize.isPresent() && currentSize == prevQueueSize.getAsInt()) {
                scheduler.shutdown();
                shutdownFuture.get().complete(null);
                return;
            }

            prevQueueSize = OptionalInt.of(currentSize);
            scheduleShutdown();
        });
    }

    private void hookupCancellation(Future<?> scheduledFuture, Task<?> submitter) {
        submitter
            .future()
            .whenComplete((value, exception) -> {
                if (exception instanceof CancellationException) {
                    if (scheduledFuture.cancel(false)) {
                        pendingTasksCounter.decrementAndGet();
                    }
                }
            });
    }

    private void ensureAccepting() {
        if (shutdownFuture.get() != null) {
            throw new RejectedExecutionException("event executor was shutdown");
        } else if (pendingTasksCounter.get() >= pendingTasksCapacity) {
            throw new RejectedExecutionException(String.format("pending task capacity reached %s", pendingTasksCapacity));
        }
    }

    private final class Task<T> implements Runnable {
        private final Callable<T> task;
        private final CompletableFuture<T> future = new CompletableFuture<>();

        Task(Callable<T> task) {
            this.task = task;
        }

        CompletableFuture<T> future() {
            return future;
        }

        @Override
        public void run() {
            try {
                pendingTasksCounter.decrementAndGet();
                if (!future.isDone()) {
                    future.complete(task.call());
                }
            } catch (Throwable e) {
                future.completeExceptionally(e);
            }
        }
    }
}
