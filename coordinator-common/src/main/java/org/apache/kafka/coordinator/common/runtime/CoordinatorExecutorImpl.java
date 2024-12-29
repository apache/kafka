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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

public class CoordinatorExecutorImpl<S extends CoordinatorShard<U>, U> implements CoordinatorExecutor<U> {
    private static class TaskResult<R> {
        final R result;
        final Throwable exception;

        TaskResult(
            R result,
            Throwable exception
        ) {
            this.result = result;
            this.exception = exception;
        }
    }

    private final Logger log;
    private final TopicPartition shard;
    private final CoordinatorRuntime<S, U> runtime;
    private final ExecutorService executor;
    private final Duration writeTimeout;
    private final Map<String, TaskRunnable<?>> tasks = new ConcurrentHashMap<>();

    public CoordinatorExecutorImpl(
        LogContext logContext,
        TopicPartition shard,
        CoordinatorRuntime<S, U> runtime,
        ExecutorService executor,
        Duration writeTimeout
    ) {
        this.log = logContext.logger(CoordinatorExecutorImpl.class);
        this.shard = shard;
        this.runtime = runtime;
        this.executor = executor;
        this.writeTimeout = writeTimeout;
    }

    private <R> TaskResult<R> executeTask(TaskRunnable<R> task) {
        try {
            return new TaskResult<>(task.run(), null);
        } catch (Throwable ex) {
            return new TaskResult<>(null, ex);
        }
    }

    @Override
    public <R> boolean schedule(
        String key,
        TaskRunnable<R> task,
        TaskOperation<U, R> operation
    ) {
        // Put the task if the key is free. Otherwise, reject it.
        if (tasks.putIfAbsent(key, task) != null) return false;

        // Submit the task.
        executor.submit(() -> {
            // If the task associated with the key is not us, it means
            // that the task was either replaced or cancelled. We stop.
            if (tasks.get(key) != task) return;

            // Execute the task.
            final TaskResult<R> result = executeTask(task);

            // Schedule the operation.
            runtime.scheduleWriteOperation(
                key,
                shard,
                writeTimeout,
                coordinator -> {
                    // If the task associated with the key is not us, it means
                    // that the task was either replaced or cancelled. We stop.
                    if (!tasks.remove(key, task)) {
                        throw new RejectedExecutionException(String.format("Task %s was overridden or cancelled", key));
                    }

                    // Call the underlying write operation with the result of the task.
                    return operation.onComplete(result.result, result.exception);
                }
            ).exceptionally(exception -> {
                // Remove the task after a failure.
                tasks.remove(key, task);

                if (exception instanceof RejectedExecutionException) {
                    log.debug("The write event for the task {} was not executed because it was " +
                        "cancelled or overridden.", key);
                } else if (exception instanceof NotCoordinatorException || exception instanceof CoordinatorLoadInProgressException) {
                    log.debug("The write event for the task {} failed due to {}. Ignoring it because " +
                        "the coordinator is not active.", key, exception.getMessage());
                } else {
                    log.error("The write event for the task {} failed due to {}. Ignoring it. ",
                        key, exception.getMessage());
                }

                return null;
            });
        });

        return true;
    }

    @Override
    public boolean isScheduled(String key) {
        return tasks.containsKey(key);
    }

    @Override
    public void cancel(String key) {
        tasks.remove(key);
    }

    public void cancelAll() {
        Iterator<String> iterator = tasks.keySet().iterator();
        while (iterator.hasNext()) {
            iterator.remove();
        }
    }
}
