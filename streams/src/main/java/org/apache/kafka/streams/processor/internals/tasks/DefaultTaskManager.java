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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ReadOnlyTask;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.processor.internals.TaskExecutionMetadata;
import org.apache.kafka.streams.processor.internals.TasksRegistry;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
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
public class DefaultTaskManager implements TaskManager {

    private final Time time;
    private final Logger log;
    private final TasksRegistry tasks;

    private final Lock tasksLock = new ReentrantLock();
    private final List<TaskId> lockedTasks = new ArrayList<>();
    private final Map<TaskId, StreamsException> uncaughtExceptions = new HashMap<>();
    private final Map<TaskId, TaskExecutor> assignedTasks = new HashMap<>();
    private final TaskExecutionMetadata taskExecutionMetadata;

    private final List<TaskExecutor> taskExecutors;

    static class DefaultTaskExecutorCreator implements TaskExecutorCreator {
        @Override
        public TaskExecutor create(final TaskManager taskManager, final String name, final Time time, final TaskExecutionMetadata taskExecutionMetadata) {
            return new DefaultTaskExecutor(taskManager, name, time, taskExecutionMetadata);
        }
    }

    public DefaultTaskManager(final Time time,
                              final String clientId,
                              final TasksRegistry tasks,
                              final StreamsConfig config,
                              final TaskExecutorCreator executorCreator,
                              final TaskExecutionMetadata taskExecutionMetadata
                              ) {
        final String logPrefix = String.format("%s ", clientId);
        final LogContext logContext = new LogContext(logPrefix);
        this.log = logContext.logger(DefaultTaskManager.class);
        this.time = time;
        this.tasks = tasks;
        this.taskExecutionMetadata = taskExecutionMetadata;

        final int numExecutors = config.getInt(StreamsConfig.NUM_STREAM_THREADS_CONFIG);
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
                    canProgress((StreamTask) task, time.milliseconds())
                ) {

                    assignedTasks.put(task.id(), executor);

                    log.info("Assigned {} to executor {}", task.id(), executor.name());

                    return (StreamTask) task;
                }
            }

            return null;
        });
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

            log.info("Unassigned {} from executor {}", task.id(), executor.name());
        });
    }

    @Override
    public KafkaFuture<Void> lockTasks(final Set<TaskId> taskIds) {
        return returnWithTasksLocked(() -> {
            lockedTasks.addAll(taskIds);

            final KafkaFutureImpl<Void> result = new KafkaFutureImpl<>();
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
                    final KafkaFuture<StreamTask> future = assignedTasks.get(taskId).unassign();
                    future.whenComplete((streamTask, throwable) -> {
                        if (throwable != null) {
                            result.completeExceptionally(throwable);
                        } else {
                            remainingTaskIds.remove(streamTask.id());
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
        executeWithTasksLocked(() -> lockedTasks.removeAll(taskIds));
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

    public Map<TaskId, StreamsException> drainUncaughtExceptions() {
        final Map<TaskId, StreamsException> returnValue = returnWithTasksLocked(() -> {
            final Map<TaskId, StreamsException> result = new HashMap<>(uncaughtExceptions);
            uncaughtExceptions.clear();
            return result;
        });

        log.info("Drained {} uncaught exceptions", returnValue.size());

        return returnValue;
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
}

