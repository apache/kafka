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

import java.time.Duration;
import java.util.List;
import java.util.Set;

public interface StateUpdater {

    class ExceptionAndTasks {
        public final Set<Task> tasks;
        public final RuntimeException exception;

        public ExceptionAndTasks(final Set<Task> tasks, final RuntimeException exception) {
            this.tasks = tasks;
            this.exception = exception;
        }
    }

    /**
     * Adds a task (active or standby) to the state updater.
     *
     * @param task task to add
     */
    void add(final Task task);

    /**
     * Removes a task (active or standby) from the state updater.
     *
     * @param task task ro remove
     */
    void remove(final Task task);

    /**
     * Gets restored active tasks from state restoration/update
     *
     * @param timeout duration how long the calling thread should wait for restored active tasks
     *
     * @return set of active tasks with up-to-date states
     */
    Set<StreamTask> getRestoredActiveTasks(final Duration timeout);

    /**
     * Gets failed tasks and the corresponding exceptions
     *
     * @return list of failed tasks and the corresponding exceptions
     */
    List<ExceptionAndTasks> getFailedTasksAndExceptions();

    /**
     * Get all tasks (active and standby) that are managed by the state updater.
     *
     * @return set of tasks managed by the state updater
     */
    Set<Task> getAllTasks();

    /**
     * Get standby tasks that are managed by the state updater.
     *
     * @return set of standby tasks managed by the state updater
     */
    Set<StandbyTask> getStandbyTasks();

    /**
     * Shuts down the state updater.
     *
     * @param timeout duration how long to wait until the state updater is shut down
     *
     * @throws
     *     org.apache.kafka.streams.errors.StreamsException if the state updater thread cannot shutdown within the timeout
     */
    void shutdown(final Duration timeout);
}
