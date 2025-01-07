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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.streams.processor.TaskId;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface StateUpdater {

    class ExceptionAndTask {
        private final Task task;
        private final RuntimeException exception;

        public ExceptionAndTask(final RuntimeException exception, final Task task) {
            this.exception = Objects.requireNonNull(exception);
            this.task = Objects.requireNonNull(task);
        }

        public Task task() {
            return task;
        }

        public RuntimeException exception() {
            return exception;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (!(o instanceof ExceptionAndTask)) return false;
            final ExceptionAndTask that = (ExceptionAndTask) o;
            return task.id().equals(that.task.id()) && exception.equals(that.exception);
        }

        @Override
        public int hashCode() {
            return Objects.hash(task, exception);
        }

        @Override
        public String toString() {
            return "ExceptionAndTask{" +
                "task=" + task.id() +
                ", exception=" + exception +
                '}';
        }
    }

    class RemovedTaskResult {

        private final Task task;
        private final Optional<RuntimeException> exception;

        public RemovedTaskResult(final Task task) {
            this(task, null);
        }

        public RemovedTaskResult(final Task task, final RuntimeException exception) {
            this.task = Objects.requireNonNull(task);
            this.exception = Optional.ofNullable(exception);
        }

        public Task task() {
            return task;
        }

        public Optional<RuntimeException> exception() {
            return exception;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (!(o instanceof RemovedTaskResult)) return false;
            final RemovedTaskResult that = (RemovedTaskResult) o;
            return Objects.equals(task.id(), that.task.id()) && Objects.equals(exception, that.exception);
        }

        @Override
        public int hashCode() {
            return Objects.hash(task, exception);
        }

        @Override
        public String toString() {
            return "RemovedTaskResult{" +
                "task=" + task.id() +
                ", exception=" + exception +
                '}';
        }
    }

    /**
     * Starts the state updater.
     */
    void start();

    /**
     * Shuts down the state updater.
     *
     * @param timeout duration how long to wait until the state updater is shut down
     *
     * @throws
     *     org.apache.kafka.streams.errors.StreamsException if the state updater thread cannot shutdown within the timeout
     */
    void shutdown(final Duration timeout);

    /**
     * Adds a task (active or standby) to the state updater.
     *
     * This method does not block until the task is added to the state updater.
     *
     * @param task task to add
     */
    void add(final Task task);

    /**
     * Removes a task (active or standby) from the state updater.
     *
     * This method does not block until the removed task is removed from the state updater. But it returns a future on
     * which processing can be blocked. The task to remove is removed from the updating tasks, paused tasks,
     * restored tasks, or failed tasks.
     *
     * @param taskId ID of the task to remove
     */
    CompletableFuture<RemovedTaskResult> remove(final TaskId taskId);

    /**
     * Wakes up the state updater if it is currently dormant, to check if a paused task should be resumed.
     */
    void signalResume();

    /**
     * Drains the restored active tasks from the state updater.
     *
     * The returned active tasks are removed from the state updater.
     *
     * With a timeout of zero the method tries to drain the restored active tasks at least once.
     *
     * @param timeout duration how long the calling thread should wait for restored active tasks
     *
     * @return set of active tasks with up-to-date states
     */
    Set<StreamTask> drainRestoredActiveTasks(final Duration timeout);

    /**
     * Drains the failed tasks and the corresponding exceptions.
     *
     * The returned failed tasks are removed from the state updater
     *
     * @return list of failed tasks and the corresponding exceptions
     */
    List<ExceptionAndTask> drainExceptionsAndFailedTasks();

    /**
     * Checks if the state updater has any failed tasks that should be returned to the StreamThread
     * using `drainExceptionsAndFailedTasks`.
     *
     * @return true if a subsequent call to `drainExceptionsAndFailedTasks` would return a non-empty collection.
     */
    boolean hasExceptionsAndFailedTasks();

    /**
     * Gets all tasks that are managed by the state updater.
     *
     * The state updater manages all tasks that were added with the {@link StateUpdater#add(Task)} and that have
     * not been removed from the state updater with one of the following methods:
     * <ul>
     *   <li>{@link StateUpdater#drainRestoredActiveTasks(Duration)}</li>
     *   <li>{@link StateUpdater#drainExceptionsAndFailedTasks()}</li>
     *   <li>{@link StateUpdater#remove(org.apache.kafka.streams.processor.TaskId)}</li>
     * </ul>
     *
     * @return set of all tasks managed by the state updater
     */
    Set<Task> tasks();

    /**
     * Gets all tasks that are currently being restored inside the state updater.
     *
     * Tasks that have just being added into the state updater via {@link StateUpdater#add(Task)}
     * or have restored completely or removed will not be returned; tasks that have just being
     * removed via {@link StateUpdater#remove(TaskId)} may still be returned.
     *
     * @return set of all updating tasks inside the state updater
     */
    Set<Task> updatingTasks();

    /**
     * Returns if the state updater restores active tasks.
     *
     * The state updater restores active tasks if at least one active task was added with {@link StateUpdater#add(Task)},
     * and the task was not removed from the state updater with one of the following methods:
     * <ul>
     *   <li>{@link StateUpdater#drainRestoredActiveTasks(Duration)}</li>
     *   <li>{@link StateUpdater#drainExceptionsAndFailedTasks()}</li>
     *   <li>{@link StateUpdater#remove(org.apache.kafka.streams.processor.TaskId)}</li>
     * </ul>
     *
     * @return {@code true} if the state updater restores active tasks, {@code false} otherwise
     */
    // TODO: We would still return true if all active tasks to be restored
    //       are paused, in order to keep consistent behavior compared with
    //       state updater disabled. In the future we would modify this criterion
    //       with state updater always enabled to allow mixed processing / restoration.
    boolean restoresActiveTasks();

    /**
     * Gets standby tasks that are managed by the state updater.
     *
     * The state updater manages all standby tasks that were added with the {@link StateUpdater#add(Task)} and that have
     * not been removed from the state updater with one of the following methods:
     * <ul>
     *   <li>{@link StateUpdater#drainExceptionsAndFailedTasks()}</li>
     * </ul>
     *
     * @return set of all tasks managed by the state updater
     */
    Set<StandbyTask> standbyTasks();

    /**
     * Get the restore consumer instance id for telemetry, and complete the given future to return it.
     */
    KafkaFutureImpl<Uuid> restoreConsumerInstanceId(final Duration timeout);
}
