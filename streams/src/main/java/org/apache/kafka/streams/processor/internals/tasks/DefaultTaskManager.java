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
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ReadOnlyTask;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.processor.internals.TaskExecutionMetadata;
import org.apache.kafka.streams.processor.internals.TasksRegistry;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * An active task could only be in one of the following status:
 *
 * 1. It's assigned to one of the executors for processing.
 * 2. It's locked for committing, removal, other manipulations etc.
 * 3. Neither 1 or 2, i.e. it stays idle. This is possible if we do not have enough executors or because those tasks
 *    are not processable (e.g. because no records fetched) yet.
 */
public final class DefaultTaskManager implements TaskManager {

    private final Time time;
    private final Logger log;
    private final TasksRegistry tasks;

    private final Lock tasksLock = new ReentrantLock();
    private final Condition tasksCondition = tasksLock.newCondition();
    private final List<TaskId> lockedTasks = new ArrayList<>();
    private final Map<TaskId, StreamsException> uncaughtExceptions = new HashMap<>();
    private final Map<TaskId, TaskExecutor> assignedTasks = new HashMap<>();
    private final TaskExecutionMetadata taskExecutionMetadata;

    private final List<TaskExecutor> taskExecutors;

    public static class DefaultTaskExecutorCreator implements TaskExecutorCreator {
        @Override
        public TaskExecutor create(final TaskManager taskManager, final String name, final Time time, final TaskExecutionMetadata taskExecutionMetadata) {
            return new DefaultTaskExecutor(taskManager, name, time, taskExecutionMetadata);
        }
    }

    public DefaultTaskManager(final Time time,
                              final String clientId,
                              final TasksRegistry tasks,
                              final TaskExecutorCreator executorCreator,
                              final TaskExecutionMetadata taskExecutionMetadata,
                              final int numExecutors
                              ) {
        final String logPrefix = String.format("%s ", clientId);
        final LogContext logContext = new LogContext(logPrefix);
        this.log = logContext.logger(DefaultTaskManager.class);
        this.time = time;
        this.tasks = tasks;
        this.taskExecutionMetadata = taskExecutionMetadata;

        this.taskExecutors = new ArrayList<>(numExecutors);
        for (int i = 1; i <= numExecutors; i++) {
            final String name = clientId + "-TaskExecutor-" + i;
            this.taskExecutors.add(executorCreator.create(this, name, time, taskExecutionMetadata));
        }
    }

    @Override
    public StreamTask assignNextTask(final TaskExecutor executor) {
        return returnWithTasksLocked(() -> {
            if (!taskExecutors.contains(executor)) {
                throw new IllegalArgumentException("The requested executor for getting next task to assign is unrecognized");
            }

            // the most naive scheduling algorithm for now: give the next unlocked, unassigned, and  processable task
            for (final Task task : tasks.activeTasks()) {
                if (!assignedTasks.containsKey(task.id()) &&
                    !lockedTasks.contains(task.id()) &&
                    canProgress((StreamTask) task, time.milliseconds()) &&
                    !hasUncaughtException(task.id())
                ) {

                    assignedTasks.put(task.id(), executor);

                    log.debug("Assigned task {} to executor {}", task.id(), executor.name());

                    return (StreamTask) task;
                }
            }

            log.debug("Found no assignable task for executor {}", executor.name());

            return null;
        });
    }

    @Override
    public void awaitProcessableTasks(final Supplier<Boolean> isShuttingDown) throws InterruptedException {
        final boolean interrupted = returnWithTasksLocked(() -> {
            for (final Task task : tasks.activeTasks()) {
                if (!assignedTasks.containsKey(task.id()) &&
                    !lockedTasks.contains(task.id()) &&
                    canProgress((StreamTask) task, time.milliseconds()) &&
                    !hasUncaughtException(task.id())
                ) {
                    log.debug("Await unblocked: returning early from await since a processable task {} was found", task.id());
                    return false;
                }
            }
            try {
                // We re-check the shutdownRequest atomic boolean to avoid a race condition. If this thread was
                // previously interrupted while awaiting tasksCondition, it is possible to miss the signalAll that
                // is called during shutdown. If this happens, we end up blocking in the await forever.
                if (!isShuttingDown.get()) {
                    log.debug("Await blocking");
                    tasksCondition.await();
                } else {
                    log.debug("Not awaiting since shutdown was requested");
                }
            } catch (final InterruptedException ignored) {
                // we interrupt the thread for shut down and pause.
                // we can ignore this exception.
                log.debug("Await unblocked: Interrupted while waiting for processable tasks");
                return true;
            }
            log.debug("Await unblocked: Woken up to check for processable tasks");
            return false;
        });

        if (interrupted) {
            throw new InterruptedException();
        }
    }

    public void signalTaskExecutors() {
        log.debug("Waking up task executors");
        executeWithTasksLocked(tasksCondition::signalAll);
    }

    @Override
    public void unassignTask(final StreamTask task, final TaskExecutor executor) {
        executeWithTasksLocked(() -> {
            if (!taskExecutors.contains(executor)) {
                throw new IllegalArgumentException("The requested executor for unassign task is unrecognized");
            }

            final TaskExecutor lockedExecutor = assignedTasks.get(task.id());
            if (lockedExecutor == null || lockedExecutor != executor) {
                throw new IllegalArgumentException("Task " + task.id() + " is not locked by the executor");
            }

            assignedTasks.remove(task.id());

            log.debug("Unassigned {} from executor {}", task.id(), executor.name());
            tasksCondition.signalAll();
        });
    }

    @Override
    public KafkaFuture<Void> lockTasks(final Set<TaskId> taskIds) {
        final KafkaFutureImpl<Void> result = new KafkaFutureImpl<>();

        if (taskIds.isEmpty()) {
            result.complete(null);
            return result;
        }

        return returnWithTasksLocked(() -> {
            lockedTasks.addAll(taskIds);

            final Set<TaskId> remainingTaskIds = new ConcurrentSkipListSet<>(taskIds);

            for (final TaskId taskId : taskIds) {
                final Task task = tasks.task(taskId);

                if (task == null) {
                    throw new IllegalArgumentException("Trying to lock task " + taskId + " but it's not owned");
                }

                if (!task.isActive()) {
                    throw new IllegalArgumentException("The locking task " + taskId + " is not an active task");
                }

                if (assignedTasks.containsKey(taskId)) {
                    final TaskExecutor executor = assignedTasks.get(taskId);
                    log.debug("Requesting release of task {} from {}", taskId, executor.name());
                    final KafkaFuture<StreamTask> future = executor.unassign();
                    future.whenComplete((streamTask, throwable) -> {
                        if (throwable != null) {
                            result.completeExceptionally(throwable);
                        } else {
                            assert !assignedTasks.containsKey(taskId);
                            // It can happen that the executor handed back the task before we asked it to
                            // in which case `streamTask` will be null here.
                            assert streamTask == null || streamTask.id() == taskId;
                            remainingTaskIds.remove(taskId);
                            if (remainingTaskIds.isEmpty()) {
                                result.complete(null);
                            }
                        }
                    });
                } else {
                    remainingTaskIds.remove(taskId);
                    if (remainingTaskIds.isEmpty()) {
                        result.complete(null);
                    }
                }
            }

            return result;
        });
    }

    @Override
    public KafkaFuture<Void> lockAllTasks() {
        return returnWithTasksLocked(() ->
            lockTasks(tasks.activeTasks().stream().map(Task::id).collect(Collectors.toSet()))
        );
    }

    @Override
    public void unlockTasks(final Set<TaskId> taskIds) {

        if (taskIds.isEmpty()) {
            return;
        }

        executeWithTasksLocked(() -> {
            lockedTasks.removeAll(taskIds);
            log.debug("Waking up task executors");
            tasksCondition.signalAll();
        });
    }

    @Override
    public void unlockAllTasks() {
        executeWithTasksLocked(() -> unlockTasks(tasks.activeTasks().stream().map(Task::id).collect(Collectors.toSet())));
    }

    @Override
    public void add(final Set<StreamTask> tasksToAdd) {
        executeWithTasksLocked(() -> {
            for (final StreamTask task : tasksToAdd) {
                tasks.addTask(task);
            }
            log.debug("Waking up task executors");
            tasksCondition.signalAll();
        });

        log.info("Added tasks {} to the task manager to process", tasksToAdd);
    }

    @Override
    public void remove(final TaskId taskId) {
        executeWithTasksLocked(() -> {
            if (assignedTasks.containsKey(taskId)) {
                throw new IllegalArgumentException("The task to remove is still assigned to executors");
            }

            if (!lockedTasks.contains(taskId)) {
                throw new IllegalArgumentException("The task to remove is not locked yet by the task manager");
            }

            if (!tasks.contains(taskId)) {
                throw new IllegalArgumentException("The task to remove is not owned by the task manager");
            }

            tasks.removeTask(tasks.task(taskId));
        });

        log.info("Removed task {} from the task manager", taskId);
    }

    @Override
    public Set<ReadOnlyTask> getTasks() {
        return returnWithTasksLocked(() -> tasks.activeTasks().stream().map(ReadOnlyTask::new).collect(Collectors.toSet()));
    }

    @Override
    public void setUncaughtException(final StreamsException exception, final TaskId taskId) {
        executeWithTasksLocked(() -> {

            if (!assignedTasks.containsKey(taskId)) {
                throw new IllegalArgumentException("An uncaught exception can only be set as long as the task is still assigned");
            }

            if (uncaughtExceptions.containsKey(taskId)) {
                throw new IllegalArgumentException("The uncaught exception must be cleared before restarting processing");
            }

            uncaughtExceptions.put(taskId, exception);
        });

        log.info("Set an uncaught exception of type {} for task {}, with error message: {}",
            exception.getClass().getName(),
            taskId,
            exception.getMessage());
    }

    public Map<TaskId, RuntimeException> drainUncaughtExceptions() {
        final Map<TaskId, RuntimeException> returnValue = returnWithTasksLocked(() -> {
            final Map<TaskId, RuntimeException> result = new HashMap<>(uncaughtExceptions);
            uncaughtExceptions.clear();
            return result;
        });

        if (!returnValue.isEmpty()) {
            log.debug("Drained {} uncaught exceptions", returnValue.size());
        }

        return returnValue;
    }

    public boolean hasUncaughtException(final TaskId taskId) {
        return returnWithTasksLocked(() -> uncaughtExceptions.containsKey(taskId));
    }

    private void executeWithTasksLocked(final Runnable action) {
        tasksLock.lock();
        try {
            action.run();
        } finally {
            tasksLock.unlock();
        }
    }

    private <T> T returnWithTasksLocked(final Supplier<T> action) {
        tasksLock.lock();
        try {
            return action.get();
        } finally {
            tasksLock.unlock();
        }
    }

    private boolean canProgress(final StreamTask task, final long nowMs) {
        return
            taskExecutionMetadata.canProcessTask(task, nowMs) && task.isProcessable(nowMs) ||
                taskExecutionMetadata.canPunctuateTask(task) && (task.canPunctuateStreamTime() || task.canPunctuateSystemTime());
    }

    public void startTaskExecutors() {
        for (final TaskExecutor t: taskExecutors) {
            t.start();
        }
    }

    public void shutdown(final Duration duration) {
        for (final TaskExecutor t: taskExecutors) {
            t.requestShutdown();
        }
        signalTaskExecutors();
        for (final TaskExecutor t: taskExecutors) {
            t.awaitShutdown(duration);
        }
    }
}
