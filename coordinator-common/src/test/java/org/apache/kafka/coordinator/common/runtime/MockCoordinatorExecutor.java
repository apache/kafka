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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;

public class MockCoordinatorExecutor<T> implements CoordinatorExecutor<T> {
    private class ExecutorTask<R> {
        public final String key;
        public final TaskRunnable<R> task;
        public final TaskOperation<T, R> operation;

        ExecutorTask(
            String key,
            TaskRunnable<R> task,
            TaskOperation<T, R> operation
        ) {
            this.key = Objects.requireNonNull(key);
            this.task = Objects.requireNonNull(task);
            this.operation = Objects.requireNonNull(operation);
        }

        CoordinatorResult<Void, T> execute() {
            try {
                return operation.onComplete(task.run(), null);
            } catch (Throwable ex) {
                return operation.onComplete(null, ex);
            }
        }
    }

    public static class ExecutorResult<T> {
        public final String key;
        public final CoordinatorResult<Void, T> result;

        public ExecutorResult(
            String key,
            CoordinatorResult<Void, T> result
        ) {
            this.key = Objects.requireNonNull(key);
            this.result = Objects.requireNonNull(result);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ExecutorResult<?> that = (ExecutorResult<?>) o;

            if (!Objects.equals(key, that.key)) return false;
            return Objects.equals(result, that.result);
        }

        @Override
        public int hashCode() {
            int result = key.hashCode();
            result = 31 * result + this.result.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "ExecutorResult(" +
                "key='" + key + '\'' +
                ", result=" + result +
                ')';
        }
    }

    private final Map<String, TaskRunnable<?>> tasks = new HashMap<>();
    private final Queue<ExecutorTask<?>> queue = new ArrayDeque<>();

    @Override
    public <R> boolean schedule(
        String key,
        TaskRunnable<R> task,
        TaskOperation<T, R> operation
    ) {
        if (tasks.putIfAbsent(key, task) != null) return false;
        return queue.add(new ExecutorTask<>(key, task, operation));
    }

    @Override
    public boolean isScheduled(String key) {
        return tasks.containsKey(key);
    }

    public int size() {
        return queue.size();
    }

    @Override
    public void cancel(String key) {
        tasks.remove(key);
    }

    public List<ExecutorResult<T>> poll() {
        List<ExecutorResult<T>> results = new ArrayList<>();
        for (ExecutorTask<?> task : queue) {
            tasks.remove(task.key, task.task);
            results.add(new ExecutorResult<>(task.key, task.execute()));
        }
        queue.clear();
        return results;
    }
}
