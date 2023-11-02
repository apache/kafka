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
import org.apache.kafka.streams.processor.internals.ReadOnlyTask;
import org.apache.kafka.streams.processor.internals.StreamTask;

import java.time.Duration;

public interface TaskExecutor {

    /**
     * @return ID name string of the task executor.
     */
    String name();

    /**
     * Starts the task executor.
     * Idempotent operation - will have no effect if thread is already started.
     */
    void start();

    /**
     * Returns true if the task executor thread is running.
     */
    boolean isRunning();

    /**
     * Asks the task executor to shut down.
     * Idempotent operation - will have no effect if thread was already asked to shut down
     *
     * @throws
     *     org.apache.kafka.streams.errors.StreamsException if the state updater thread cannot shutdown within the timeout
     */
    void requestShutdown();

    /**
     * Shuts down the task processor updater.
     * Idempotent operation - will have no effect if thread is already shut down.
     * Must call `requestShutdown` first.
     *
     * @param timeout duration how long to wait until the state updater is shut down
     *
     * @throws
     *     org.apache.kafka.streams.errors.StreamsException if the state updater thread does not shutdown within the timeout
     */
    void awaitShutdown(final Duration timeout);

    /**
     * Get the current assigned processing task. The task returned is read-only and cannot be modified.
     *
     * @return the current processing task
     */
    ReadOnlyTask currentTask();

    /**
     * Unassign the current processing task from the task processor and give it back to the state manager.
     *
     * Note there is an asymmetry between assignment and unassignment between {@link TaskManager} and {@link TaskExecutor},
     * since assigning a task from task manager to task executor is always initiated by the task executor itself, by calling
     * {@link TaskManager#assignNextTask(TaskExecutor)},
     * while unassigning a task and returning it to task manager could be triggered either by the task executor proactively
     * when it finds the task not processable anymore, or by the task manager when it needs to commit / close it.
     * This function is used for the second case, where task manager will call this function asking the task executor
     * to give back the task.
     *
     * The task must be flushed before being unassigned, since it may be committed or closed by the task manager next.
     *
     * This method does not block, instead a future is returned; when the task executor finishes
     * unassigning the task this future will then complete.
     *
     * @return the future capturing the completion of the unassign process
     */
    KafkaFuture<StreamTask> unassign();
}
