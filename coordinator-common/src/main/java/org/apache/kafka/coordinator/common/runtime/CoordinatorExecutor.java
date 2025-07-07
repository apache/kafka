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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.KafkaException;

/**
 * An interface to schedule and cancel asynchronous tasks. The TaskRunnable
 * interface defines the tasks to be executed in the executor and the
 * TaskOperation defines the operation scheduled to the runtime to
 * process the output of the executed task.
 *
 * @param <T> The record type.
 */
public interface CoordinatorExecutor<T> {
    /**
     * The task's runnable.
     *
     * @param <R> The return type.
     */
    interface TaskRunnable<R> {
        R run() throws Throwable;
    }

    /**
     * The task's write operation to handle the output
     * of the task.
     *
     * @param <T> The record type.
     * @param <R> The return type of the task.
     */
    interface TaskOperation<T, R> {
        CoordinatorResult<Void, T> onComplete(
            R result,
            Throwable exception
        ) throws KafkaException;
    }

    /**
     * Schedule an asynchronous task. Note that only one task for a given key can
     * be executed at the time.
     *
     * @param key       The key to identify the task.
     * @param task      The task itself.
     * @param operation The runtime operation to handle the output of the task.
     * @return True if the task was scheduled; False otherwise.
     *
     * @param <R> The return type of the task.
     */
    <R> boolean schedule(
        String key,
        TaskRunnable<R> task,
        TaskOperation<T, R> operation
    );

    /**
     * Return true if the key is associated to a task; false otherwise.
     *
     * @param key The key to identify the task.
     * @return A boolean indicating whether the task is scheduled or not.
     */
    boolean isScheduled(String key);

    /**
     * Cancel the given task
     *
     * @param key The key to identify the task.
     */
    void cancel(String key);
}
