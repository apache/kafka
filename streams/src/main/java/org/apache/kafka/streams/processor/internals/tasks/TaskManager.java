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

import java.time.Duration;
import java.util.Map;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ReadOnlyTask;
import org.apache.kafka.streams.processor.internals.StreamTask;
import java.util.Set;

public interface TaskManager {

    /**
     * Get the next processable active task for the requested executor. Once the task is assigned to
     * the requested task executor, it should not be assigned to any other executors until it was
     * returned to the task manager.
     *
     * @param executor the requesting {@link TaskExecutor}
     * @return a processable active task not assigned to any other executors, or null if there is no such task available
     */
    StreamTask assignNextTask(final TaskExecutor executor);

    /**
     * Unassign the stream task so that it can be assigned to other executors later
     * or be removed from the task manager. The requested executor must have
     * the task already, otherwise an exception would be thrown.
     *
     * @param executor the requesting {@link TaskExecutor}
     */
    void unassignTask(final StreamTask task, final TaskExecutor executor);

    /**
     * Lock a set of active tasks from the task manager so that they will not be assigned to
     * any {@link TaskExecutor}s anymore until they are unlocked. At the time this function
     * is called, the requested tasks may already be locked by some {@link TaskExecutor}s,
     * and in that case the task manager need to first unassign these tasks from the
     * executors.
     *
     * This function is needed when we need to 1) commit these tasks, 2) remove these tasks.
     *
     * This method does not block, instead a future is returned.
     */
    KafkaFuture<Void> lockTasks(final Set<TaskId> taskIds);

    /**
     * Lock all the managed active tasks from the task manager. Similar to {@link #lockTasks(Set)}.
     *
     * This method does not block, instead a future is returned.
     */
    KafkaFuture<Void> lockAllTasks();

    /**
     * Unlock the tasks so that they can be assigned to executors
     */
    void unlockTasks(final Set<TaskId> taskIds);

    /**
     * Unlock all the managed active tasks from the task manager. Similar to {@link #unlockTasks(Set)}.
     */
    void unlockAllTasks();

    /**
     * Add a new active task to the task manager.
     *
     * @param tasks task to add
     */
    void add(final Set<StreamTask> tasks);

    /**
     * Remove an active task from the task manager.
     *
     * The task to remove must be locked.
     *
     * @param taskId ID of the task to remove
     */
    void remove(final TaskId taskId);

    /**
     * Gets all active tasks that are managed by this manager. The returned tasks are read-only
     * and cannot be manipulated.
     *
     * @return set of all managed active tasks
     */
    Set<ReadOnlyTask> getTasks();

    /**
     * Called whenever an existing task has thrown an uncaught exception.
     *
     * Setting an uncaught exception for a task prevents it from being reassigned until the
     * corresponding exception has been handled in the polling thread.
     *
     */
    void setUncaughtException(StreamsException exception, TaskId taskId);

    /**
     * Returns and clears all uncaught exceptions that were fell through to the processing
     * threads and need to be handled in the polling thread.
     *
     * Called by the polling thread to handle processing exceptions, e.g. to abort
     * transactions or shut down the application.
     *
     * @return A map from task ID to the exception that occurred.
     */
    Map<TaskId, RuntimeException> drainUncaughtExceptions();

    /**
     * Can be used to check if a specific task has an uncaught exception.
     *
     * @param taskId the task ID to check for
     */
    boolean hasUncaughtException(final TaskId taskId);

    /**
     * Signals that at least one task has become processable, e.g. because it was resumed or new records may be available.
     */
    void signalTaskExecutors();

    /**
     * Blocks until unassigned processable tasks may be available.
     */
    void awaitProcessableTasks() throws InterruptedException;

    /**
     * Starts all threads associated with this task manager.
     */
    void startTaskExecutors();

    /**
     * Shuts down all threads associated with this task manager.
     * All tasks will be unlocked and unassigned by the end of this.
     *
     * @param duration Time to wait for each thread to shut down.
     */
    void shutdown(final Duration duration);

}
