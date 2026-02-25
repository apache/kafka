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

package org.apache.kafka.server.util;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Single threaded executor.
 * <p>
 * Handle the all events submitted and schedule to the executor.
 * <p>
 * All Runnable and Callable submitted to an executor are guaranteed to be executed by the same thread. This means
 * that the events don't need to handle concurrent access to shared state.
 * <p>
 * When canceling the futures returned by the submit and schedule methods the Runnable and Callable will not be
 * executed. This is a best effort. If the task is already running then the Runnable and Callable will execute but the
 * future may not get updated.
 * <p>
 * Shutting down the event executor will make it so that it will stop accepting new tasks for execution. The methods
 * submit and schedule will throw a RejectedExecutionException when called after calling shutdown. All scheduled tasks
 * that have not expired will not be executed by the event executor.
 */
public interface EventExecutor {
    /**
     * Submits a task for immediate execution.
     *
     * @param task runnable to execute
     * @return a completable future that will complete when the task as finished executing
     */
    CompletableFuture<Void> submit(Runnable task);

    /**
     * Submits a task for immediate execution.
     *
     * @param task callable to execute
     * @return a completable future that will complete with the task's value
     */
    <T> CompletableFuture<T> submit(Callable<T> task);

    /**
     * Submits a task for delayed execution.
     *
     * @param task runnable to execute
     * @param delay the time from now to delay execution
     * @param unit the time unit of the delay parameter
     * @return a completable future that will complete when the task as finished executing
     */
    CompletableFuture<Void> schedule(Runnable task, long delay, TimeUnit unit);

    /**
     * Submits a task for delayed execution.
     *
     * @param task runnable to execute
     * @param delay the time from now to delay execution
     * @param unit the time unit of the delay parameter
     * @return a completable future that will complete with the task's value
     */
    <T> CompletableFuture<T> schedule(Callable<T> task, long delay, TimeUnit unit);

    /**
     * Shuts down the event executor.
     * <p>
     * No additional task will be accepted by the submit and schedule methods. Both methods will throw a
     * RejectedExecutionException after this method is called.
     * <p>
     * All scheduled tasks that have not expired will not be executed by the event executor.
     *
     * @return a completable future that will complete when all pending tasks have executed
     */
    CompletableFuture<Void> shutdown();
}
