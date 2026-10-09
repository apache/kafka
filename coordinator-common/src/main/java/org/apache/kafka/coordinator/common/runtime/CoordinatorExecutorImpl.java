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

import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

public class CoordinatorExecutorImpl<U> implements CoordinatorExecutor<U> {
    private record TaskResult<R>(R result, Throwable exception) { }

    /**
     * A scheduled task. The task holds its runnable, its operation and the result of
     * the runnable only while they are needed. The runnable is released when it is
     * executed, and the operation and the result are released when the operation is
     * executed. All of them are released when the task is cancelled because the task
     * may remain in the executor queue or in the event processor queue long after its
     * cancellation.
     *
     * @param <R> The return type of the task.
     */
    class Task<R> {
        private final String key;
        private TaskRunnable<R> runnable;
        private TaskOperation<U, R> operation;
        private TaskResult<R> result;

        Task(
            String key,
            TaskRunnable<R> runnable,
            TaskOperation<U, R> operation
        ) {
            this.key = key;
            this.runnable = runnable;
            this.operation = operation;
        }

        /**
         * Executes the runnable and schedules the write operation to handle its result.
         * This is called by the executor.
         */
        private void run() {
            // If the task associated with the key is not us, it means
            // that the task was either replaced or cancelled. We stop.
            if (tasks.get(key) != this) return;

            // The runnable is null if the task was cancelled in the meantime.
            var runnable = takeRunnable();
            if (runnable == null) return;

            // Execute the task. The result is not stored if the task
            // was cancelled while it was running.
            if (!maybeSetResult(executeTask(runnable))) return;

            // Schedule the operation.
            scheduler.scheduleWriteOperation(
                key,
                this::complete
            ).exceptionally(exception -> {
                // Exceptions may be wrapped in CompletionException when propagated
                // through CompletableFuture chains, so we unwrap them before
                // checking types with instanceof.
                exception = Errors.maybeUnwrapException(exception);

                // Remove the task after a failure.
                if (tasks.remove(key, this)) cancel();

                if (exception instanceof RejectedExecutionException) {
                    log.debug("The write event for the task {} was not executed because it was " +
                        "cancelled or overridden.", key);
                } else if (exception instanceof NotCoordinatorException || exception instanceof CoordinatorLoadInProgressException) {
                    log.debug("The write event for the task {} failed due to {}. Ignoring it because " +
                        "the coordinator is not active.", key, exception.getMessage());
                } else {
                    log.error("The write event for the task {} failed due to {}. Ignoring it. ",
                        key, exception.getMessage(), exception);
                }

                return null;
            });
        }

        /**
         * Calls the operation with the result of the runnable. This is called by
         * the write operation scheduled in the runtime.
         */
        private CoordinatorResult<Void, U> complete() {
            // If the task associated with the key is not us, it means
            // that the task was either replaced or cancelled. We stop.
            if (!tasks.remove(key, this)) {
                throw new RejectedExecutionException(String.format("Task %s was overridden or cancelled", key));
            }

            // The task cannot be cancelled anymore because it is no longer
            // in the map. The operation and the result are released because
            // the write operation is retained until its records are committed.
            TaskOperation<U, R> operation;
            TaskResult<R> result;
            synchronized (this) {
                operation = this.operation;
                result = this.result;
                this.operation = null;
                this.result = null;
            }

            // Call the underlying write operation with the result of the task.
            return operation.onComplete(result.result(), result.exception());
        }

        /**
         * Takes the runnable and releases it.
         *
         * @return The runnable or null if the task was cancelled.
         */
        private synchronized TaskRunnable<R> takeRunnable() {
            var runnable = this.runnable;
            this.runnable = null;
            return runnable;
        }

        /**
         * Stores the result of the runnable unless the task was cancelled.
         *
         * @param result The result of the runnable.
         * @return True if the result was stored; False if the task was cancelled.
         */
        private synchronized boolean maybeSetResult(TaskResult<R> result) {
            if (operation == null) return false;
            this.result = result;
            return true;
        }

        /**
         * Releases the runnable, the operation and the result. The caller must
         * remove the task from the map beforehand.
         */
        private synchronized void cancel() {
            runnable = null;
            operation = null;
            result = null;
        }

        /**
         * Return true if the runnable, the operation and the result are released.
         * Visible for testing.
         */
        synchronized boolean isReleased() {
            return runnable == null && operation == null && result == null;
        }
    }

    private final Logger log;
    private final ExecutorService executor;
    private final CoordinatorShardScheduler<U> scheduler;
    private final Map<String, Task<?>> tasks = new ConcurrentHashMap<>();

    public CoordinatorExecutorImpl(
        LogContext logContext,
        ExecutorService executor,
        CoordinatorShardScheduler<U> scheduler
    ) {
        this.log = logContext.logger(CoordinatorExecutorImpl.class);
        this.executor = executor;
        this.scheduler = scheduler;
    }

    /**
     * Executes the runnable and captures its result or its exception.
     */
    private <R> TaskResult<R> executeTask(TaskRunnable<R> runnable) {
        try {
            return new TaskResult<>(runnable.run(), null);
        } catch (Throwable ex) {
            return new TaskResult<>(null, ex);
        }
    }

    @Override
    public <R> boolean schedule(
        String key,
        TaskRunnable<R> runnable,
        TaskOperation<U, R> operation
    ) {
        var task = new Task<>(key, runnable, operation);

        // Put the task if the key is free. Otherwise, reject it.
        if (tasks.putIfAbsent(key, task) != null) return false;

        // Submit the task.
        executor.submit(task::run);

        return true;
    }

    @Override
    public boolean isScheduled(String key) {
        return tasks.containsKey(key);
    }

    @Override
    public void cancel(String key) {
        var task = tasks.remove(key);
        if (task != null) task.cancel();
    }

    /**
     * Cancels all the tasks.
     */
    public void cancelAll() {
        tasks.keySet().forEach(this::cancel);
    }

    /**
     * Return the task associated with the key or null if there is none.
     * Visible for testing.
     */
    Task<?> task(String key) {
        return tasks.get(key);
    }
}
