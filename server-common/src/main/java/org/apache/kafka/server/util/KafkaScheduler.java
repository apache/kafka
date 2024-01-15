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

import org.apache.kafka.common.utils.KafkaThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A scheduler based on java.util.concurrent.ScheduledThreadPoolExecutor
 * <br>
 * It has a pool of kafka-scheduler- threads that do the actual work.
 */
public class KafkaScheduler implements Scheduler {

    private static class NoOpScheduledFutureTask implements ScheduledFuture<Void> {

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return true;
        }

        @Override
        public boolean isCancelled() {
            return true;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Void get() {
            return null;
        }

        @Override
        public Void get(long timeout, TimeUnit unit) {
            return null;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return 0L;
        }

        @Override
        public int compareTo(Delayed o) {
            long diff = getDelay(TimeUnit.NANOSECONDS) - o.getDelay(TimeUnit.NANOSECONDS);
            if (diff < 0)
                return -1;
            else if (diff > 0)
                return 1;
            else
                return 0;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(KafkaScheduler.class);

    private final AtomicInteger schedulerThreadId = new AtomicInteger(0);
    private final int threads;
    private final boolean daemon;
    private final String threadNamePrefix;

    private volatile ScheduledThreadPoolExecutor executor;

    public KafkaScheduler(int threads) {
        this(threads, true);
    }

    public KafkaScheduler(int threads, boolean daemon) {
        this(threads, daemon, "kafka-scheduler-");
    }

    /*
     * Creates an instance of this.
     *
     * @param threads The number of threads in the thread pool
     * @param daemon If true the scheduler threads will be "daemon" threads and will not block jvm shutdown.
     * @param threadNamePrefix The name to use for scheduler threads. This prefix will have a number appended to it.
     */
    public KafkaScheduler(int threads, boolean daemon, String threadNamePrefix) {
        this.threads = threads;
        this.daemon = daemon;
        this.threadNamePrefix = threadNamePrefix;
    }

    @Override
    public void startup() {
        log.debug("Initializing task scheduler.");
        synchronized (this) {
            if (isStarted())
                throw new IllegalStateException("This scheduler has already been started.");
            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(threads);
            executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            executor.setRemoveOnCancelPolicy(true);
            executor.setThreadFactory(runnable ->
                new KafkaThread(threadNamePrefix + schedulerThreadId.getAndIncrement(), runnable, daemon));
            this.executor = executor;
        }
    }

    @Override
    public void shutdown() throws InterruptedException {
        log.debug("Shutting down task scheduler.");
        // We use the local variable to avoid NullPointerException if another thread shuts down scheduler at same time.
        ScheduledThreadPoolExecutor maybeExecutor = null;
        synchronized (this) {
            if (isStarted()) {
                maybeExecutor = executor;
                maybeExecutor.shutdown();
                this.executor = null;
            }
        }
        if (maybeExecutor != null)
            maybeExecutor.awaitTermination(1, TimeUnit.DAYS);
    }

    @Override
    public ScheduledFuture<?> schedule(String name, Runnable task, long delayMs, long periodMs) {
        log.debug("Scheduling task {} with initial delay {} ms and period {} ms.", name, delayMs, periodMs);
        synchronized (this) {
            if (isStarted()) {
                Runnable runnable = () -> {
                    try {
                        log.trace("Beginning execution of scheduled task '{}'.", name);
                        task.run();
                    } catch (Throwable t) {
                        log.error("Uncaught exception in scheduled task '{}'", name, t);
                    } finally {
                        log.trace("Completed execution of scheduled task '{}'.", name);
                    }
                };
                if (periodMs > 0)
                    return executor.scheduleAtFixedRate(runnable, delayMs, periodMs, TimeUnit.MILLISECONDS);
                else
                    return executor.schedule(runnable, delayMs, TimeUnit.MILLISECONDS);
            } else {
                log.info("Kafka scheduler is not running at the time task '{}' is scheduled. The task is ignored.", name);
                return new NoOpScheduledFutureTask();
            }
        }
    }

    public final boolean isStarted() {
        return executor != null;
    }

    public void resizeThreadPool(int newSize) {
        synchronized (this) {
            if (isStarted())
                executor.setCorePoolSize(newSize);
        }
    }

    // Visible for testing
    public String threadNamePrefix() {
        return threadNamePrefix;
    }

    // Visible for testing
    public boolean taskRunning(ScheduledFuture<?> task) {
        ScheduledThreadPoolExecutor e = executor;
        return e != null && e.getQueue().contains(task);
    }
}
