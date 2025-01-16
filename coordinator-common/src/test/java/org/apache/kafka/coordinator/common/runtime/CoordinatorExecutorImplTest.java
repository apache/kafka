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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.util.FutureUtils;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// Creating mocks of classes using generics creates unsafe assignment.
@SuppressWarnings("unchecked")
public class CoordinatorExecutorImplTest {
    private static final LogContext LOG_CONTEXT = new LogContext();
    private static final TopicPartition SHARD_PARTITION = new TopicPartition("__consumer_offsets", 0);
    private static final Duration WRITE_TIMEOUT = Duration.ofMillis(1000);
    private static final String TASK_KEY = "task";

    @Test
    public void testTaskSuccessfulLifecycle() {
        CoordinatorShard<String> coordinatorShard = mock(CoordinatorShard.class);
        CoordinatorRuntime<CoordinatorShard<String>, String> runtime = mock(CoordinatorRuntime.class);
        ExecutorService executorService = mock(ExecutorService.class);
        CoordinatorExecutorImpl<CoordinatorShard<String>, String> executor = new CoordinatorExecutorImpl<>(
            LOG_CONTEXT,
            SHARD_PARTITION,
            runtime,
            executorService,
            WRITE_TIMEOUT
        );

        when(runtime.scheduleWriteOperation(
            eq(TASK_KEY),
            eq(SHARD_PARTITION),
            eq(WRITE_TIMEOUT),
            any()
        )).thenAnswer(args -> {
            assertTrue(executor.isScheduled(TASK_KEY));
            CoordinatorRuntime.CoordinatorWriteOperation<CoordinatorShard<String>, Void, String> op =
                args.getArgument(3);
            assertEquals(
                new CoordinatorResult<>(Collections.singletonList("record"), null),
                op.generateRecordsAndResult(coordinatorShard)
            );
            return CompletableFuture.completedFuture(null);
        });

        when(executorService.submit(any(Runnable.class))).thenAnswer(args -> {
            assertTrue(executor.isScheduled(TASK_KEY));
            Runnable op = args.getArgument(0);
            op.run();
            return CompletableFuture.completedFuture(null);
        });

        AtomicBoolean taskCalled = new AtomicBoolean(false);
        CoordinatorExecutor.TaskRunnable<String> taskRunnable = () -> {
            taskCalled.set(true);
            return "Hello!";
        };

        AtomicBoolean operationCalled = new AtomicBoolean(false);
        CoordinatorExecutor.TaskOperation<String, String> taskOperation = (result, exception) -> {
            operationCalled.set(true);
            assertEquals("Hello!", result);
            assertNull(exception);
            return new CoordinatorResult<>(Collections.singletonList("record"), null);
        };

        executor.schedule(
            TASK_KEY,
            taskRunnable,
            taskOperation
        );

        assertTrue(taskCalled.get());
        assertTrue(operationCalled.get());
    }

    @Test
    public void testTaskFailedLifecycle() {
        CoordinatorShard<String> coordinatorShard = mock(CoordinatorShard.class);
        CoordinatorRuntime<CoordinatorShard<String>, String> runtime = mock(CoordinatorRuntime.class);
        ExecutorService executorService = mock(ExecutorService.class);
        CoordinatorExecutorImpl<CoordinatorShard<String>, String> executor = new CoordinatorExecutorImpl<>(
            LOG_CONTEXT,
            SHARD_PARTITION,
            runtime,
            executorService,
            WRITE_TIMEOUT
        );

        when(runtime.scheduleWriteOperation(
            eq(TASK_KEY),
            eq(SHARD_PARTITION),
            eq(WRITE_TIMEOUT),
            any()
        )).thenAnswer(args -> {
            CoordinatorRuntime.CoordinatorWriteOperation<CoordinatorShard<String>, Void, String> op =
                args.getArgument(3);
            assertEquals(
                new CoordinatorResult<>(Collections.emptyList(), null),
                op.generateRecordsAndResult(coordinatorShard)
            );
            return CompletableFuture.completedFuture(null);
        });

        when(executorService.submit(any(Runnable.class))).thenAnswer(args -> {
            Runnable op = args.getArgument(0);
            op.run();
            return CompletableFuture.completedFuture(null);
        });

        AtomicBoolean taskCalled = new AtomicBoolean(false);
        CoordinatorExecutor.TaskRunnable<String> taskRunnable = () -> {
            taskCalled.set(true);
            throw new Exception("Oh no!");
        };

        AtomicBoolean operationCalled = new AtomicBoolean(false);
        CoordinatorExecutor.TaskOperation<String, String> taskOperation = (result, exception) -> {
            operationCalled.set(true);
            assertNull(result);
            assertNotNull(exception);
            assertEquals("Oh no!", exception.getMessage());
            return new CoordinatorResult<>(Collections.emptyList(), null);
        };

        executor.schedule(
            TASK_KEY,
            taskRunnable,
            taskOperation
        );

        assertTrue(taskCalled.get());
        assertTrue(operationCalled.get());
    }

    @Test
    public void testTaskCancelledBeforeBeingExecuted() {
        CoordinatorRuntime<CoordinatorShard<String>, String> runtime = mock(CoordinatorRuntime.class);
        ExecutorService executorService = mock(ExecutorService.class);
        CoordinatorExecutorImpl<CoordinatorShard<String>, String> executor = new CoordinatorExecutorImpl<>(
            LOG_CONTEXT,
            SHARD_PARTITION,
            runtime,
            executorService,
            WRITE_TIMEOUT
        );

        when(executorService.submit(any(Runnable.class))).thenAnswer(args -> {
            // Cancel the task before running it.
            executor.cancel(TASK_KEY);

            // Running the task.
            Runnable op = args.getArgument(0);
            op.run();
            return CompletableFuture.completedFuture(null);
        });

        AtomicBoolean taskCalled = new AtomicBoolean(false);
        CoordinatorExecutor.TaskRunnable<String> taskRunnable = () -> {
            taskCalled.set(true);
            return null;
        };

        AtomicBoolean operationCalled = new AtomicBoolean(false);
        CoordinatorExecutor.TaskOperation<String, String> taskOperation = (result, exception) -> {
            operationCalled.set(true);
            return null;
        };

        executor.schedule(
            TASK_KEY,
            taskRunnable,
            taskOperation
        );

        assertFalse(taskCalled.get());
        assertFalse(operationCalled.get());
    }

    @Test
    public void testTaskCancelledAfterBeingExecutedButBeforeWriteOperationIsExecuted() {
        CoordinatorShard<String> coordinatorShard = mock(CoordinatorShard.class);
        CoordinatorRuntime<CoordinatorShard<String>, String> runtime = mock(CoordinatorRuntime.class);
        ExecutorService executorService = mock(ExecutorService.class);
        CoordinatorExecutorImpl<CoordinatorShard<String>, String> executor = new CoordinatorExecutorImpl<>(
            LOG_CONTEXT,
            SHARD_PARTITION,
            runtime,
            executorService,
            WRITE_TIMEOUT
        );

        when(runtime.scheduleWriteOperation(
            eq(TASK_KEY),
            eq(SHARD_PARTITION),
            eq(WRITE_TIMEOUT),
            any()
        )).thenAnswer(args -> {
            // Cancel the task before running the write operation.
            executor.cancel(TASK_KEY);

            CoordinatorRuntime.CoordinatorWriteOperation<CoordinatorShard<String>, Void, String> op =
                args.getArgument(3);
            Throwable ex = assertThrows(RejectedExecutionException.class, () -> op.generateRecordsAndResult(coordinatorShard));
            return FutureUtils.failedFuture(ex);
        });

        when(executorService.submit(any(Runnable.class))).thenAnswer(args -> {
            Runnable op = args.getArgument(0);
            op.run();
            return CompletableFuture.completedFuture(null);
        });

        AtomicBoolean taskCalled = new AtomicBoolean(false);
        CoordinatorExecutor.TaskRunnable<String> taskRunnable = () -> {
            taskCalled.set(true);
            return "Hello!";
        };

        AtomicBoolean operationCalled = new AtomicBoolean(false);
        CoordinatorExecutor.TaskOperation<String, String> taskOperation = (result, exception) -> {
            operationCalled.set(true);
            return null;
        };

        executor.schedule(
            TASK_KEY,
            taskRunnable,
            taskOperation
        );

        assertTrue(taskCalled.get());
        assertFalse(operationCalled.get());
    }

    @Test
    public void testTaskSchedulingWriteOperationFailed() {
        CoordinatorRuntime<CoordinatorShard<String>, String> runtime = mock(CoordinatorRuntime.class);
        ExecutorService executorService = mock(ExecutorService.class);
        CoordinatorExecutorImpl<CoordinatorShard<String>, String> executor = new CoordinatorExecutorImpl<>(
            LOG_CONTEXT,
            SHARD_PARTITION,
            runtime,
            executorService,
            WRITE_TIMEOUT
        );

        when(runtime.scheduleWriteOperation(
            eq(TASK_KEY),
            eq(SHARD_PARTITION),
            eq(WRITE_TIMEOUT),
            any()
        )).thenReturn(FutureUtils.failedFuture(new Throwable("Oh no!")));

        when(executorService.submit(any(Runnable.class))).thenAnswer(args -> {
            Runnable op = args.getArgument(0);
            op.run();
            return CompletableFuture.completedFuture(null);
        });

        AtomicBoolean taskCalled = new AtomicBoolean(false);
        CoordinatorExecutor.TaskRunnable<String> taskRunnable = () -> {
            taskCalled.set(true);
            return "Hello!";
        };

        AtomicBoolean operationCalled = new AtomicBoolean(false);
        CoordinatorExecutor.TaskOperation<String, String> taskOperation = (result, exception) -> {
            operationCalled.set(true);
            return new CoordinatorResult<>(Collections.emptyList(), null);
        };

        executor.schedule(
            TASK_KEY,
            taskRunnable,
            taskOperation
        );

        assertTrue(taskCalled.get());
        assertFalse(operationCalled.get());
        assertFalse(executor.isScheduled(TASK_KEY));
    }
}
