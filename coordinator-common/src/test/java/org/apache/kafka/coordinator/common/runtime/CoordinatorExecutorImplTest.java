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

import org.apache.kafka.common.utils.LogContext;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// Creating mocks of classes using generics creates unsafe assignment.
@SuppressWarnings("unchecked")
public class CoordinatorExecutorImplTest {
    private static final LogContext LOG_CONTEXT = new LogContext();
    private static final String TASK_KEY = "task";

    @Test
    public void testTaskSuccessfulLifecycle() {
        CoordinatorShardScheduler<String> scheduler = mock(CoordinatorShardScheduler.class);
        ExecutorService executorService = mock(ExecutorService.class);
        CoordinatorExecutorImpl<String> executor = new CoordinatorExecutorImpl<>(
            LOG_CONTEXT,
            executorService,
            scheduler
        );

        when(scheduler.scheduleWriteOperation(
            eq(TASK_KEY),
            any()
        )).thenAnswer(args -> {
            assertTrue(executor.isScheduled(TASK_KEY));
            CoordinatorShardScheduler.WriteOperation<String> op = args.getArgument(1);
            assertEquals(
                new CoordinatorResult<>(List.of("record"), null),
                op.generate()
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
            return new CoordinatorResult<>(List.of("record"), null);
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
        CoordinatorShardScheduler<String> scheduler = mock(CoordinatorShardScheduler.class);
        ExecutorService executorService = mock(ExecutorService.class);
        CoordinatorExecutorImpl<String> executor = new CoordinatorExecutorImpl<>(
            LOG_CONTEXT,
            executorService,
            scheduler
        );

        when(scheduler.scheduleWriteOperation(
            eq(TASK_KEY),
            any()
        )).thenAnswer(args -> {
            CoordinatorShardScheduler.WriteOperation<String> op = args.getArgument(1);
            assertEquals(
                new CoordinatorResult<>(List.of(), null),
                op.generate()
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
            return new CoordinatorResult<>(List.of(), null);
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
        CoordinatorShardScheduler<String> scheduler = mock(CoordinatorShardScheduler.class);
        ExecutorService executorService = mock(ExecutorService.class);
        CoordinatorExecutorImpl<String> executor = new CoordinatorExecutorImpl<>(
            LOG_CONTEXT,
            executorService,
            scheduler
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
        CoordinatorShardScheduler<String> scheduler = mock(CoordinatorShardScheduler.class);
        ExecutorService executorService = mock(ExecutorService.class);
        CoordinatorExecutorImpl<String> executor = new CoordinatorExecutorImpl<>(
            LOG_CONTEXT,
            executorService,
            scheduler
        );

        when(scheduler.scheduleWriteOperation(
            eq(TASK_KEY),
            any()
        )).thenAnswer(args -> {
            // Cancel the task before running the write operation.
            executor.cancel(TASK_KEY);

            CoordinatorShardScheduler.WriteOperation<String> op = args.getArgument(1);
            Throwable ex = assertThrows(RejectedExecutionException.class, op::generate);
            return CompletableFuture.failedFuture(ex);
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
        CoordinatorShardScheduler<String> scheduler = mock(CoordinatorShardScheduler.class);
        ExecutorService executorService = mock(ExecutorService.class);
        CoordinatorExecutorImpl<String> executor = new CoordinatorExecutorImpl<>(
            LOG_CONTEXT,
            executorService,
            scheduler
        );

        when(scheduler.scheduleWriteOperation(
            eq(TASK_KEY),
            any()
        )).thenReturn(CompletableFuture.failedFuture(new Throwable("Oh no!")));

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
            return new CoordinatorResult<>(List.of(), null);
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

    @Test
    public void testCancelAllTasks() {
        CoordinatorShardScheduler<String> scheduler = mock(CoordinatorShardScheduler.class);
        ExecutorService executorService = mock(ExecutorService.class);
        CoordinatorExecutorImpl<String> executor = new CoordinatorExecutorImpl<>(
            LOG_CONTEXT,
            executorService,
            scheduler
        );

        List<CoordinatorShardScheduler.WriteOperation<String>> writeOperations = new ArrayList<>();
        List<CompletableFuture<Void>> writeFutures = new ArrayList<>();
        when(scheduler.scheduleWriteOperation(
            anyString(),
            any()
        )).thenAnswer(args -> {
            writeOperations.add(args.getArgument(1));
            CompletableFuture<Void> writeFuture = new CompletableFuture<>();
            writeFutures.add(writeFuture);
            return writeFuture;
        });

        when(executorService.submit(any(Runnable.class))).thenAnswer(args -> {
            Runnable op = args.getArgument(0);
            op.run();
            return CompletableFuture.completedFuture(null);
        });

        AtomicInteger taskCallCount = new AtomicInteger(0);
        CoordinatorExecutor.TaskRunnable<String> taskRunnable = () -> {
            taskCallCount.incrementAndGet();
            return "Hello!";
        };

        AtomicInteger operationCallCount = new AtomicInteger(0);
        CoordinatorExecutor.TaskOperation<String, String> taskOperation = (result, exception) -> {
            operationCallCount.incrementAndGet();
            return null;
        };

        for (int i = 0; i < 2; i++) {
            executor.schedule(
                TASK_KEY + i,
                taskRunnable,
                taskOperation
            );
        }

        executor.cancelAll();

        for (int i = 0; i < writeOperations.size(); i++) {
            CoordinatorShardScheduler.WriteOperation<String> writeOperation = writeOperations.get(i);
            CompletableFuture<Void> writeFuture = writeFutures.get(i);
            Throwable ex = assertThrows(RejectedExecutionException.class, writeOperation::generate);
            writeFuture.completeExceptionally(ex);
        }

        assertEquals(2, taskCallCount.get());
        assertEquals(0, operationCallCount.get());
    }
}
