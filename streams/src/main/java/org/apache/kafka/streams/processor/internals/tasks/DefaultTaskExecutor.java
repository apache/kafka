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

import static org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode.EXACTLY_ONCE_V2;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.internals.ReadOnlyTask;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.processor.internals.TaskExecutionMetadata;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class DefaultTaskExecutor implements TaskExecutor {

    private class TaskExecutorThread extends Thread {

        private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
        private final AtomicReference<KafkaFutureImpl<StreamTask>> taskReleaseRequested = new AtomicReference<>(null);

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
                while (!shutdownRequested.get()) {
                    try {
                        runOnce(time.milliseconds());
                    } catch (final StreamsException e) {
                        handleException(e);
                    } catch (final Exception e) {
                        handleException(new StreamsException(e));
                    }
                }
            } finally {
                if (currentTask != null) {
                    log.debug("Releasing task {} due to shutdown.", currentTask.id());
                    unassignCurrentTask();
                }

                shutdownGate.countDown();

                final KafkaFutureImpl<StreamTask> taskReleaseFuture;
                if ((taskReleaseFuture = taskReleaseRequested.getAndSet(null)) != null) {
                    log.debug("Asked to return current task, but shutting down.");
                    taskReleaseFuture.complete(null);
                }
                log.info("Task executor thread shutdown");
            }
        }

        private void handleTaskReleaseRequested() {
            final KafkaFutureImpl<StreamTask> taskReleaseFuture;
            if ((taskReleaseFuture = taskReleaseRequested.getAndSet(null)) != null) {
                if (currentTask != null) {
                    log.debug("Releasing task {} upon request.", currentTask.id());
                    final StreamTask unassignedTask = unassignCurrentTask();
                    taskReleaseFuture.complete(unassignedTask);
                } else {
                    log.debug("Asked to return current task, but returned current task already.");
                    taskReleaseFuture.complete(null);
                }
            }
        }

        private void handleException(final StreamsException e) {
            if (currentTask != null) {
                taskManager.setUncaughtException(e, currentTask.id());

                log.debug("Releasing task {} due to uncaught exception.", currentTask.id());
                unassignCurrentTask();
            } else {
                // If we do not currently have a task assigned and still get an error, this is fatal for the executor thread
                throw e;
            }
        }

        private void runOnce(final long nowMs) {
            handleTaskReleaseRequested();

            if (currentTask == null) {
                currentTask = taskManager.assignNextTask(DefaultTaskExecutor.this);
            }

            if (currentTask == null) {
                try {
                    taskManager.awaitProcessableTasks();
                } catch (final InterruptedException ignored) {
                    // Can be ignored, the cause of the interrupted will be handled in the event loop
                }
            } else {
                boolean progressed = false;

                if (taskExecutionMetadata.canProcessTask(currentTask, nowMs) && currentTask.isProcessable(nowMs)) {
                    if (processTask(currentTask, nowMs, time)) {
                        log.trace("processed a record for {}", currentTask.id());
                        progressed = true;
                    }
                }

                if (taskExecutionMetadata.canPunctuateTask(currentTask)) {
                    if (currentTask.maybePunctuateStreamTime()) {
                        log.trace("punctuated stream time for task {} ", currentTask.id());
                        progressed = true;
                    }
                    if (currentTask.maybePunctuateSystemTime()) {
                        log.trace("punctuated system time for task {} ", currentTask.id());
                        progressed = true;
                    }
                }

                if (!progressed) {
                    log.debug("Releasing task {} because we are not making progress.", currentTask.id());
                    unassignCurrentTask();
                }
            }
        }

        private boolean processTask(final Task task, final long now, final Time time) {
            boolean processed = false;
            try {
                processed = task.process(now);
                if (processed) {
                    log.trace("Successfully processed task {}", task.id());
                    task.clearTaskTimeout();
                    // TODO: enable regardless of whether using named topologies
                    if (taskExecutionMetadata.hasNamedTopologies() && taskExecutionMetadata.processingMode() != EXACTLY_ONCE_V2) {
                        taskExecutionMetadata.addToSuccessfullyProcessed(task);
                    }
                }
            } catch (final TimeoutException timeoutException) {
                // TODO consolidate TimeoutException retries with general error handling
                task.maybeInitTaskTimeoutOrThrow(now, timeoutException);
                log.error(
                    String.format(
                        "Could not complete processing records for %s due to the following exception; will move to next task and retry later",
                        task.id()),
                    timeoutException
                );
            } catch (final TaskMigratedException e) {
                log.info("Failed to process stream task {} since it got migrated to another thread already. " +
                    "Will trigger a new rebalance and close all tasks as zombies together.", task.id());
                throw e;
            } catch (final StreamsException e) {
                log.error(String.format("Failed to process stream task %s due to the following error:", task.id()), e);
                e.setTaskId(task.id());
                throw e;
            } catch (final RuntimeException e) {
                log.error(String.format("Failed to process stream task %s due to the following error:", task.id()), e);
                throw new StreamsException(e, task.id());
            } finally {
                task.recordProcessBatchTime(time.milliseconds() - now);
            }
            return processed;
        }

        private StreamTask unassignCurrentTask() {
            if (currentTask == null)
                throw new IllegalStateException("Does not own any task while being ask to unassign from task manager");

            // flush the task before giving it back to task manager, if we are not handing it back because of an error.
            if (!taskManager.hasUncaughtException(currentTask.id())) {
                try {
                    currentTask.flush();
                } catch (final StreamsException e) {
                    log.error(String.format("Failed to flush stream task %s due to the following error:", currentTask.id()), e);
                    e.setTaskId(currentTask.id());
                    taskManager.setUncaughtException(e, currentTask.id());
                } catch (final RuntimeException e) {
                    log.error(String.format("Failed to flush stream task %s due to the following error:", currentTask.id()), e);
                    taskManager.setUncaughtException(new StreamsException(e, currentTask.id()), currentTask.id());
                }
            }
            taskManager.unassignTask(currentTask, DefaultTaskExecutor.this);

            final StreamTask retTask = currentTask;
            currentTask = null;
            return retTask;
        }
    }

    private final Time time;
    private final String name;
    private final TaskManager taskManager;
    private final TaskExecutionMetadata taskExecutionMetadata;
    private final Logger log;

    private StreamTask currentTask = null;
    private TaskExecutorThread taskExecutorThread = null;
    private CountDownLatch shutdownGate;

    public DefaultTaskExecutor(final TaskManager taskManager,
                               final String name,
                               final Time time,
                               final TaskExecutionMetadata taskExecutionMetadata) {
        this.time = time;
        this.name = name;
        this.taskManager = taskManager;
        this.taskExecutionMetadata = taskExecutionMetadata;
        final LogContext logContext = new LogContext(name);
        this.log = logContext.logger(DefaultTaskExecutor.class);
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
    public boolean isRunning() {
        return taskExecutorThread != null && taskExecutorThread.isAlive() && shutdownGate.getCount() != 0;
    }

    @Override
    public void requestShutdown() {
        if (taskExecutorThread != null) {
            taskExecutorThread.shutdownRequested.set(true);
        }
    }

    @Override
    public void awaitShutdown(final Duration timeout) {
        if (taskExecutorThread != null) {
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
            log.debug("Asking {} to hand back task", taskExecutorThread.getName());
            if (!taskExecutorThread.taskReleaseRequested.compareAndSet(null, future)) {
                throw new IllegalStateException("There was already a task release request registered");
            }
            if (shutdownGate.getCount() == 0) {
                log.debug("Completing future, because task executor was just shut down");
                future.complete(null);
            } else {
                taskManager.signalTaskExecutors();
            }
        } else {
            log.debug("Tried to unassign but no thread is running");
            future.complete(null);
        }

        return future;
    }
}
