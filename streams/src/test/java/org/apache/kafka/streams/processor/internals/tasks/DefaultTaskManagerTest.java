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

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ReadOnlyTask;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.TaskExecutionMetadata;
import org.apache.kafka.streams.processor.internals.TasksRegistry;
import org.apache.kafka.test.StreamsTestUtils.TaskBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultTaskManagerTest {

    private final static long VERIFICATION_TIMEOUT = 15000;

    private final Time time = new MockTime(1L);
    private final TaskId taskId = new TaskId(0, 0, "A");
    private final StreamTask task = TaskBuilder.statelessTask(taskId).build();
    private final TasksRegistry tasks = mock(TasksRegistry.class);
    private final TaskExecutor taskExecutor = mock(TaskExecutor.class);
    private final StreamsException exception = mock(StreamsException.class);
    private final TaskExecutionMetadata taskExecutionMetadata = mock(TaskExecutionMetadata.class);

    private final TaskManager taskManager = new DefaultTaskManager(time, "TaskManager", tasks,
        (taskManager, name, time, taskExecutionMetadata) -> taskExecutor, taskExecutionMetadata, 1);

    @BeforeEach
    public void setUp() {
        when(task.isProcessable(anyLong())).thenReturn(true);
        when(tasks.activeTasks()).thenReturn(Collections.singleton(task));
        when(tasks.task(taskId)).thenReturn(task);
    }

    @Test
    public void shouldShutdownTaskExecutors() {
        final Duration duration = mock(Duration.class);
        taskManager.shutdown(duration);

        verify(taskExecutor).requestShutdown();
        verify(taskExecutor).awaitShutdown(duration);
    }

    @Test
    public void shouldStartTaskExecutors() {
        taskManager.startTaskExecutors();

        verify(taskExecutor).start();
    }

    @Test
    public void shouldAddTask() {
        taskManager.add(Collections.singleton(task));

        verify(tasks).addTask(task);
        when(tasks.activeTasks()).thenReturn(Collections.singleton(task));
        assertEquals(1, taskManager.getTasks().size());
    }

    @Test
    public void shouldAssignTaskThatCanBeProcessed() {
        taskManager.add(Collections.singleton(task));
        when(tasks.activeTasks()).thenReturn(Collections.singleton(task));
        when(taskExecutionMetadata.canProcessTask(eq(task), anyLong())).thenReturn(true);

        assertEquals(task, taskManager.assignNextTask(taskExecutor));
        assertNull(taskManager.assignNextTask(taskExecutor));
    }

    private class AwaitingRunnable implements Runnable {
        private final CountDownLatch awaitDone = new CountDownLatch(1);
        private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
        @Override
        public void run() {
            while (!shutdownRequested.get()) {
                try {
                    taskManager.awaitProcessableTasks();
                } catch (final InterruptedException ignored) {
                }
                awaitDone.countDown();
            }
        }

        public void shutdown() {
            shutdownRequested.set(true);
            taskManager.signalTaskExecutors();
        }
    }

    @Test
    public void shouldBlockOnAwait() throws InterruptedException {
        final AwaitingRunnable awaitingRunnable = new AwaitingRunnable();
        final Thread awaitingThread = new Thread(awaitingRunnable);
        awaitingThread.start();

        assertFalse(awaitingRunnable.awaitDone.await(100, TimeUnit.MILLISECONDS));

        awaitingRunnable.shutdown();
    }

    @Test
    public void shouldReturnFromAwaitOnInterruption() throws InterruptedException {
        final AwaitingRunnable awaitingRunnable = new AwaitingRunnable();
        final Thread awaitingThread = new Thread(awaitingRunnable);
        awaitingThread.start();
        verify(tasks, timeout(VERIFICATION_TIMEOUT).atLeastOnce()).activeTasks();

        awaitingThread.interrupt();

        assertTrue(awaitingRunnable.awaitDone.await(VERIFICATION_TIMEOUT, TimeUnit.MILLISECONDS));

        awaitingRunnable.shutdown();
    }

    @Test
    public void shouldReturnFromAwaitOnSignalProcessableTasks() throws InterruptedException {
        final AwaitingRunnable awaitingRunnable = new AwaitingRunnable();
        final Thread awaitingThread = new Thread(awaitingRunnable);
        awaitingThread.start();
        verify(tasks, timeout(VERIFICATION_TIMEOUT).atLeastOnce()).activeTasks();

        taskManager.signalTaskExecutors();

        assertTrue(awaitingRunnable.awaitDone.await(VERIFICATION_TIMEOUT, TimeUnit.MILLISECONDS));

        awaitingRunnable.shutdown();
    }

    @Test
    public void shouldReturnFromAwaitOnUnassignment() throws InterruptedException {
        taskManager.add(Collections.singleton(task));
        when(taskExecutionMetadata.canProcessTask(eq(task), anyLong())).thenReturn(true);

        final StreamTask task = taskManager.assignNextTask(taskExecutor);
        assertNotNull(task);
        final AwaitingRunnable awaitingRunnable = new AwaitingRunnable();
        final Thread awaitingThread = new Thread(awaitingRunnable);
        awaitingThread.start();
        verify(tasks, timeout(VERIFICATION_TIMEOUT).atLeastOnce()).activeTasks();

        taskManager.unassignTask(task, taskExecutor);

        assertTrue(awaitingRunnable.awaitDone.await(VERIFICATION_TIMEOUT, TimeUnit.MILLISECONDS));

        awaitingRunnable.shutdown();
    }

    @Test
    public void shouldReturnFromAwaitOnAdding() throws InterruptedException {
        final AwaitingRunnable awaitingRunnable = new AwaitingRunnable();
        final Thread awaitingThread = new Thread(awaitingRunnable);
        awaitingThread.start();
        verify(tasks, timeout(VERIFICATION_TIMEOUT).atLeastOnce()).activeTasks();

        taskManager.add(Collections.singleton(task));

        assertTrue(awaitingRunnable.awaitDone.await(VERIFICATION_TIMEOUT, TimeUnit.MILLISECONDS));

        awaitingRunnable.shutdown();
    }

    @Test
    public void shouldReturnFromAwaitOnUnlocking() throws InterruptedException {
        taskManager.add(Collections.singleton(task));
        taskManager.lockTasks(Collections.singleton(task.id()));
        final AwaitingRunnable awaitingRunnable = new AwaitingRunnable();
        final Thread awaitingThread = new Thread(awaitingRunnable);
        awaitingThread.start();
        verify(tasks, timeout(VERIFICATION_TIMEOUT).atLeastOnce()).activeTasks();

        taskManager.unlockAllTasks();

        assertTrue(awaitingRunnable.awaitDone.await(VERIFICATION_TIMEOUT, TimeUnit.MILLISECONDS));

        awaitingRunnable.shutdown();
    }

    @Test
    public void shouldAssignTasksThatCanBeSystemTimePunctuated() {
        taskManager.add(Collections.singleton(task));
        when(tasks.activeTasks()).thenReturn(Collections.singleton(task));
        when(taskExecutionMetadata.canProcessTask(eq(task), anyLong())).thenReturn(true);
        when(task.canPunctuateSystemTime()).thenReturn(true);

        assertEquals(task, taskManager.assignNextTask(taskExecutor));
        assertNull(taskManager.assignNextTask(taskExecutor));
    }

    @Test
    public void shouldAssignTasksThatCanBeStreamTimePunctuated() {
        taskManager.add(Collections.singleton(task));
        when(tasks.activeTasks()).thenReturn(Collections.singleton(task));
        when(taskExecutionMetadata.canPunctuateTask(eq(task))).thenReturn(true);
        when(task.canPunctuateStreamTime()).thenReturn(true);

        assertEquals(task, taskManager.assignNextTask(taskExecutor));
        assertNull(taskManager.assignNextTask(taskExecutor));
    }

    @Test
    public void shouldNotAssignTasksIfUncaughtExceptionPresent() {
        taskManager.add(Collections.singleton(task));
        when(tasks.activeTasks()).thenReturn(Collections.singleton(task));
        ensureTaskMakesProgress();
        taskManager.assignNextTask(taskExecutor);
        taskManager.setUncaughtException(new StreamsException("Exception"), taskId);
        taskManager.unassignTask(task, taskExecutor);

        assertNull(taskManager.assignNextTask(taskExecutor));
    }

    @Test
    public void shouldNotAssignTasksForPunctuationIfPunctuationDisabled() {
        taskManager.add(Collections.singleton(task));
        when(tasks.activeTasks()).thenReturn(Collections.singleton(task));
        when(taskExecutionMetadata.canPunctuateTask(eq(task))).thenReturn(false);
        when(task.canPunctuateStreamTime()).thenReturn(true);
        when(task.canPunctuateSystemTime()).thenReturn(true);

        assertNull(taskManager.assignNextTask(taskExecutor));
    }

    @Test
    public void shouldNotAssignTasksForProcessingIfProcessingDisabled() {
        taskManager.add(Collections.singleton(task));
        when(tasks.activeTasks()).thenReturn(Collections.singleton(task));
        when(taskExecutionMetadata.canProcessTask(eq(task), anyLong())).thenReturn(false);
        when(task.isProcessable(anyLong())).thenReturn(true);

        assertNull(taskManager.assignNextTask(taskExecutor));
    }

    @Test
    public void shouldUnassignTask() {
        taskManager.add(Collections.singleton(task));
        when(tasks.activeTasks()).thenReturn(Collections.singleton(task));
        when(taskExecutionMetadata.canProcessTask(eq(task), anyLong())).thenReturn(true);

        assertEquals(task, taskManager.assignNextTask(taskExecutor));

        taskManager.unassignTask(task, taskExecutor);
        assertEquals(task, taskManager.assignNextTask(taskExecutor));
    }

    @Test
    public void shouldNotUnassignNotOwnedTask() {
        taskManager.add(Collections.singleton(task));
        when(tasks.activeTasks()).thenReturn(Collections.singleton(task));
        when(taskExecutionMetadata.canProcessTask(eq(task), anyLong())).thenReturn(true);

        assertEquals(task, taskManager.assignNextTask(taskExecutor));

        final TaskExecutor anotherExecutor = mock(TaskExecutor.class);
        assertThrows(IllegalArgumentException.class, () -> taskManager.unassignTask(task, anotherExecutor));
    }

    @Test
    public void shouldNotRemoveUnlockedTask() {
        taskManager.add(Collections.singleton(task));

        assertThrows(IllegalArgumentException.class, () -> taskManager.remove(task.id()));
    }

    @Test
    public void shouldNotRemoveAssignedTask() {
        taskManager.add(Collections.singleton(task));
        taskManager.assignNextTask(taskExecutor);

        assertThrows(IllegalArgumentException.class, () -> taskManager.remove(task.id()));
    }

    @Test
    public void shouldRemoveTask() {
        taskManager.add(Collections.singleton(task));
        when(tasks.activeTasks()).thenReturn(Collections.singleton(task));
        when(tasks.task(task.id())).thenReturn(task);
        when(tasks.contains(task.id())).thenReturn(true);

        taskManager.lockTasks(Collections.singleton(task.id()));
        taskManager.remove(task.id());

        verify(tasks).removeTask(task);
        reset(tasks);
        when(tasks.activeTasks()).thenReturn(Collections.emptySet());

        assertEquals(0, taskManager.getTasks().size());
    }

    @Test
    public void shouldNotAssignLockedTask() {
        taskManager.add(Collections.singleton(task));
        when(tasks.activeTasks()).thenReturn(Collections.singleton(task));
        when(taskExecutionMetadata.canProcessTask(eq(task), anyLong())).thenReturn(true);
        when(tasks.task(task.id())).thenReturn(task);
        when(tasks.contains(task.id())).thenReturn(true);

        assertTrue(taskManager.lockTasks(Collections.singleton(task.id())).isDone());

        assertNull(taskManager.assignNextTask(taskExecutor));
    }

    @Test
    public void shouldLockAnEmptySetOfTasks() {
        taskManager.add(Collections.singleton(task));
        when(tasks.activeTasks()).thenReturn(Collections.singleton(task));
        when(taskExecutionMetadata.canProcessTask(eq(task), anyLong())).thenReturn(true);
        when(tasks.task(task.id())).thenReturn(task);
        when(tasks.contains(task.id())).thenReturn(true);

        assertTrue(taskManager.lockTasks(Collections.emptySet()).isDone());

        assertEquals(task, taskManager.assignNextTask(taskExecutor));
    }


    @Test
    public void shouldLockATaskThatWasVoluntarilyReleased() {
        final KafkaFutureImpl<StreamTask> future = new KafkaFutureImpl<>();

        taskManager.add(Collections.singleton(task));
        when(tasks.activeTasks()).thenReturn(Collections.singleton(task));
        when(taskExecutionMetadata.canProcessTask(eq(task), anyLong())).thenReturn(true);
        when(tasks.task(task.id())).thenReturn(task);
        when(tasks.contains(task.id())).thenReturn(true);
        when(taskExecutor.unassign()).thenReturn(future);

        assertEquals(task, taskManager.assignNextTask(taskExecutor));

        final KafkaFuture<Void> lockingFuture = taskManager.lockTasks(Collections.singleton(task.id()));
        assertFalse(lockingFuture.isDone());

        taskManager.unassignTask(task, taskExecutor);
        future.complete(null);

        assertTrue(lockingFuture.isDone());
        assertNull(taskManager.assignNextTask(taskExecutor));
    }

    @Test
    public void shouldNotAssignAnyLockedTask() {
        taskManager.add(Collections.singleton(task));
        when(tasks.activeTasks()).thenReturn(Collections.singleton(task));
        when(taskExecutionMetadata.canProcessTask(eq(task), anyLong())).thenReturn(true);
        when(tasks.task(task.id())).thenReturn(task);
        when(tasks.contains(task.id())).thenReturn(true);

        assertTrue(taskManager.lockAllTasks().isDone());

        assertNull(taskManager.assignNextTask(taskExecutor));
    }

    @Test
    public void shouldNotSetUncaughtExceptionsForUnassignedTasks() {
        taskManager.add(Collections.singleton(task));

        final Exception e = assertThrows(IllegalArgumentException.class, () -> taskManager.setUncaughtException(exception, task.id()));
        assertEquals("An uncaught exception can only be set as long as the task is still assigned", e.getMessage());
    }

    @Test
    public void shouldNotSetUncaughtExceptionsTwice() {
        taskManager.add(Collections.singleton(task));
        when(tasks.activeTasks()).thenReturn(Collections.singleton(task));
        when(taskExecutionMetadata.canProcessTask(eq(task), anyLong())).thenReturn(true);
        taskManager.assignNextTask(taskExecutor);
        taskManager.setUncaughtException(exception, task.id());

        final Exception e = assertThrows(IllegalArgumentException.class, () -> taskManager.setUncaughtException(exception, task.id()));
        assertEquals("The uncaught exception must be cleared before restarting processing", e.getMessage());
    }

    @Test
    public void shouldReturnAndClearExceptionsOnDrainExceptions() {
        taskManager.add(Collections.singleton(task));
        when(tasks.activeTasks()).thenReturn(Collections.singleton(task));
        when(taskExecutionMetadata.canProcessTask(eq(task), anyLong())).thenReturn(true);
        taskManager.assignNextTask(taskExecutor);
        taskManager.setUncaughtException(exception, task.id());

        assertEquals(taskManager.drainUncaughtExceptions(), Collections.singletonMap(task.id(), exception));
        assertEquals(taskManager.drainUncaughtExceptions(), Collections.emptyMap());
    }

    @Test
    public void shouldUnassignLockingTask() {
        final KafkaFutureImpl<StreamTask> future = new KafkaFutureImpl<>();

        taskManager.add(Collections.singleton(task));
        when(tasks.activeTasks()).thenReturn(Collections.singleton(task));
        when(tasks.task(task.id())).thenReturn(task);
        when(tasks.contains(task.id())).thenReturn(true);
        when(taskExecutor.unassign()).thenReturn(future);
        when(taskExecutionMetadata.canProcessTask(eq(task), anyLong())).thenReturn(true);

        assertEquals(task, taskManager.assignNextTask(taskExecutor));

        when(taskExecutor.currentTask()).thenReturn(new ReadOnlyTask(task));
        final KafkaFuture<Void> lockFuture = taskManager.lockAllTasks();
        assertFalse(lockFuture.isDone());

        verify(taskExecutor).unassign();

        taskManager.unassignTask(task, taskExecutor);
        future.complete(task);

        assertTrue(lockFuture.isDone());
    }

    private void ensureTaskMakesProgress() {
        when(taskExecutionMetadata.canPunctuateTask(eq(task))).thenReturn(true);
        when(task.canPunctuateStreamTime()).thenReturn(true);
    }

}
