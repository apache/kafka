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
package org.apache.kafka.streams.processor.internals.tasks;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.internals.ReadOnlyTask;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class DefaultTaskExecutor implements TaskExecutor {

    private class TaskExecutorThread extends Thread {

        private final AtomicBoolean isRunning = new AtomicBoolean(true);
        private final AtomicReference<KafkaFutureImpl<StreamTask>> pauseRequested = new AtomicReference<>(null);

        private final Logger log;

        public TaskExecutorThread(final String name) {
            super(name);
            final String logPrefix = String.format("%s ", name);
            final LogContext logContext = new LogContext(logPrefix);
            log = logContext.logger(DefaultTaskExecutor.class);
        }

        @Override
        public void run() {
            log.info("Task executor thread started");
            try {
                while (isRunning.get()) {
                    runOnce(time.milliseconds());
                }
                // TODO: add exception handling
            } finally {
                if (currentTask != null) {
                    unassignCurrentTask();
                }

                shutdownGate.countDown();
                log.info("Task executor thread shutdown");
            }
        }

        private void runOnce(final long nowMs) {
            final KafkaFutureImpl<StreamTask> pauseFuture;
            if ((pauseFuture = pauseRequested.getAndSet(null)) != null) {
                final StreamTask unassignedTask = unassignCurrentTask();
                pauseFuture.complete(unassignedTask);
            }

            if (currentTask == null) {
                currentTask = taskManager.assignNextTask(DefaultTaskExecutor.this);
            } else {
                // if a task is no longer processable, ask task-manager to give it another
                // task in the next iteration
                if (currentTask.isProcessable(nowMs)) {
                    currentTask.process(nowMs);
                } else {
                    unassignCurrentTask();
                }
            }
        }

        private StreamTask unassignCurrentTask() {
            if (currentTask == null)
                throw new IllegalStateException("Does not own any task while being ask to unassign from task manager");

            // flush the task before giving it back to task manager
            // TODO: we can add a separate function in StreamTask to just flush and not return offsets
            currentTask.prepareCommit();
            taskManager.unassignTask(currentTask, DefaultTaskExecutor.this);

            final StreamTask retTask = currentTask;
            currentTask = null;
            return retTask;
        }
    }

    private final Time time;
    private final String name;
    private final TaskManager taskManager;

    private StreamTask currentTask = null;
    private TaskExecutorThread taskExecutorThread = null;
    private CountDownLatch shutdownGate;

    public DefaultTaskExecutor(final TaskManager taskManager,
                               final String name,
                               final Time time) {
        this.time = time;
        this.name = name;
        this.taskManager = taskManager;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void start() {
        if (taskExecutorThread == null) {
            taskExecutorThread = new TaskExecutorThread(name);
            taskExecutorThread.start();
            shutdownGate = new CountDownLatch(1);
        }
    }

    @Override
    public void shutdown(final Duration timeout) {
        if (taskExecutorThread != null) {
            taskExecutorThread.isRunning.set(false);
            taskExecutorThread.interrupt();
            try {
                if (!shutdownGate.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                    throw new StreamsException("State updater thread did not shutdown within the timeout");
                }
                taskExecutorThread = null;
            } catch (final InterruptedException ignored) {
            }
        }
    }

    @Override
    public ReadOnlyTask currentTask() {
        return currentTask != null ? new ReadOnlyTask(currentTask) : null;
    }

    @Override
    public KafkaFuture<StreamTask> unassign() {
        final KafkaFutureImpl<StreamTask> future = new KafkaFutureImpl<>();

        if (taskExecutorThread != null) {
            taskExecutorThread.pauseRequested.set(future);
        } else {
            future.complete(null);
        }

        return future;
    }
}
