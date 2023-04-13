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

import org.apache.kafka.streams.processor.TaskId;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public interface StateUpdater {

    class ExceptionAndTasks {
        private final Set<Task> tasks;
        private final RuntimeException exception;

        public ExceptionAndTasks(final Set<Task> tasks, final RuntimeException exception) {
            this.tasks = Objects.requireNonNull(tasks);
            this.exception = Objects.requireNonNull(exception);
        }

        public Set<Task> getTasks() {
            return Collections.unmodifiableSet(tasks);
        }

        public RuntimeException exception() {
            return exception;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (!(o instanceof ExceptionAndTasks)) return false;
            final ExceptionAndTasks that = (ExceptionAndTasks) o;
            return tasks.equals(that.tasks) && exception.equals(that.exception);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tasks, exception);
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
     * Removes a task (active or standby) from the state updater and adds the removed task to the removed tasks.
     *
     * This method does not block until the removed task is removed from the state updater.
     *
     * The task to be removed is not removed from the restored active tasks and the failed tasks.
     * Stateless tasks will never be added to the removed tasks since they are immediately added to the
     * restored active tasks.
     *
     * @param taskId ID of the task to remove
     */
    void remove(final TaskId taskId);

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
     * Drains the removed tasks (active and standbys) from the state updater.
     *
     * Removed tasks returned by this method are tasks extraordinarily removed from the state updater. These do not
     * include restored or failed tasks.
     *
     * The returned removed tasks are removed from the state updater
     *
     * @return set of tasks removed from the state updater
     */
    Set<Task> drainRemovedTasks();

    /**
     * Checks if the state updater has any tasks that should be removed and returned to the StreamThread
     * using `drainRemovedTasks`.
     *
     * @return true if a subsequent call to `drainRemovedTasks` would return a non-empty collection.
     */
    boolean hasRemovedTasks();

    /**
     * Drains the failed tasks and the corresponding exceptions.
     *
     * The returned failed tasks are removed from the state updater
     *
     * @return list of failed tasks and the corresponding exceptions
     */
    List<ExceptionAndTasks> drainExceptionsAndFailedTasks();

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
     *   <li>{@link StateUpdater#drainRemovedTasks()}</li>
     *   <li>{@link StateUpdater#drainExceptionsAndFailedTasks()}</li>
     * </ul>
     *
     * @return set of all tasks managed by the state updater
     */
    Set<Task> getTasks();

    /**
     * Gets all tasks that are currently being restored inside the state updater.
     *
     * Tasks that have just being added into the state updater via {@link StateUpdater#add(Task)}
     * or have restored completely or removed will not be returned; similarly tasks that have just being
     * removed via {@link StateUpdater#remove(TaskId)} maybe returned still.
     *
     * @return set of all updating tasks inside the state updater
     */
    Set<Task> getUpdatingTasks();

    /**
     * Returns if the state updater restores active tasks.
     *
     * The state updater restores active tasks if at least one active task was added with {@link StateUpdater#add(Task)},
     * and the task was not removed from the state updater with one of the following methods:
     * <ul>
     *   <li>{@link StateUpdater#drainRestoredActiveTasks(Duration)}</li>
     *   <li>{@link StateUpdater#drainRemovedTasks()}</li>
     *   <li>{@link StateUpdater#drainExceptionsAndFailedTasks()}</li>
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
     *   <li>{@link StateUpdater#drainRemovedTasks()}</li>
     *   <li>{@link StateUpdater#drainExceptionsAndFailedTasks()}</li>
     * </ul>
     *
     * @return set of all tasks managed by the state updater
     */
    Set<StandbyTask> getStandbyTasks();
}
