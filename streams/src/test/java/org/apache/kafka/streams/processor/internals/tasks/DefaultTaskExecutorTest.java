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
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.TaskExecutionMetadata;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;

import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultTaskExecutorTest {

    private static final long VERIFICATION_TIMEOUT = 15000;

    private final Time time = new MockTime(1L);
    private final StreamTask task = mock(StreamTask.class);
    private final TaskManager taskManager = mock(TaskManager.class);
    private final TaskExecutionMetadata taskExecutionMetadata = mock(TaskExecutionMetadata.class);

    private final DefaultTaskExecutor taskExecutor = new DefaultTaskExecutor(taskManager, "TaskExecutor", time, taskExecutionMetadata);

    @BeforeEach
    public void setUp() {
        // only assign a task for the first time
        when(taskManager.assignNextTask(taskExecutor)).thenReturn(task).thenReturn(null);
        when(taskExecutionMetadata.canProcessTask(eq(task), anyLong())).thenReturn(true);
        when(task.isProcessable(anyLong())).thenReturn(true);
        when(task.id()).thenReturn(new TaskId(0, 0, "A"));
        when(task.process(anyLong())).thenReturn(true);
        when(task.prepareCommit()).thenReturn(Collections.emptyMap());
    }

    @AfterEach
    public void tearDown() {
        taskExecutor.requestShutdown();
        taskExecutor.awaitShutdown(Duration.ofMinutes(1));
    }

    @Test
    public void shouldShutdownTaskExecutor() {
        assertNull(taskExecutor.currentTask(), "Have task assigned before startup");
        assertFalse(taskExecutor.isRunning());

        taskExecutor.start();

        assertTrue(taskExecutor.isRunning());
        verify(taskManager, timeout(VERIFICATION_TIMEOUT)).assignNextTask(taskExecutor);

        taskExecutor.requestShutdown();
        taskExecutor.awaitShutdown(Duration.ofMinutes(1));

        verify(task).flush();
        verify(taskManager).unassignTask(task, taskExecutor);

        assertNull(taskExecutor.currentTask(), "Have task assigned after shutdown");
        assertFalse(taskExecutor.isRunning());
    }

    @Test
    public void shouldClearTaskReleaseFutureOnShutdown() throws InterruptedException {
        assertNull(taskExecutor.currentTask(), "Have task assigned before startup");

        taskExecutor.start();

        verify(taskManager, timeout(VERIFICATION_TIMEOUT)).assignNextTask(taskExecutor);

        final KafkaFuture<StreamTask> future = taskExecutor.unassign();
        taskExecutor.requestShutdown();
        taskExecutor.awaitShutdown(Duration.ofMinutes(1));

        waitForCondition(future::isDone, "Await for unassign future to complete");
        assertNull(taskExecutor.currentTask(), "Have task assigned after shutdown");
    }

    @Test
    public void shouldAwaitProcessableTasksIfNoneAssignable() throws InterruptedException {
        assertNull(taskExecutor.currentTask(), "Have task assigned before startup");
        when(taskManager.assignNextTask(taskExecutor)).thenReturn(null);

        taskExecutor.start();

        verify(taskManager, timeout(VERIFICATION_TIMEOUT).atLeastOnce()).awaitProcessableTasks(any());
    }

    @Test
    public void shouldUnassignTaskWhenNotProgressing() {
        when(task.isProcessable(anyLong())).thenReturn(false);
        when(task.maybePunctuateStreamTime()).thenReturn(false);
        when(task.maybePunctuateSystemTime()).thenReturn(false);

        taskExecutor.start();

        verify(taskManager, timeout(VERIFICATION_TIMEOUT)).unassignTask(task, taskExecutor);
        verify(task).flush();
        assertNull(taskExecutor.currentTask());
    }

    @Test
    public void shouldProcessTasks() {
        when(taskExecutionMetadata.canProcessTask(any(), anyLong())).thenReturn(true);
        when(task.isProcessable(anyLong())).thenReturn(true);

        taskExecutor.start();

        verify(task, timeout(VERIFICATION_TIMEOUT).atLeast(2)).process(anyLong());
        verify(task, timeout(VERIFICATION_TIMEOUT).atLeastOnce()).recordProcessBatchTime(anyLong());
    }


    @Test
    public void shouldClearTaskTimeoutOnProcessed() {
        when(taskExecutionMetadata.canProcessTask(any(), anyLong())).thenReturn(true);
        when(task.isProcessable(anyLong())).thenReturn(true);
        when(task.process(anyLong())).thenReturn(true);

        taskExecutor.start();

        verify(task, timeout(VERIFICATION_TIMEOUT).atLeastOnce()).clearTaskTimeout();
    }

    @Test
    public void shouldSetTaskTimeoutOnTimeoutException() {
        final TimeoutException e = new TimeoutException();
        when(taskExecutionMetadata.canProcessTask(any(), anyLong())).thenReturn(true);
        when(task.isProcessable(anyLong())).thenReturn(true);
        when(task.process(anyLong())).thenReturn(true).thenThrow(e);

        taskExecutor.start();
        verify(task, timeout(VERIFICATION_TIMEOUT).atLeastOnce()).process(anyLong());
        verify(task, timeout(VERIFICATION_TIMEOUT).atLeastOnce()).maybeInitTaskTimeoutOrThrow(anyLong(), eq(e));
    }

    @Test
    public void shouldPunctuateStreamTime() {
        when(taskExecutionMetadata.canProcessTask(eq(task), anyLong())).thenReturn(false);
        when(taskExecutionMetadata.canPunctuateTask(task)).thenReturn(true);
        when(task.maybePunctuateStreamTime()).thenReturn(true);

        taskExecutor.start();

        verify(task, timeout(VERIFICATION_TIMEOUT).atLeast(2)).maybePunctuateStreamTime();
    }

    @Test
    public void shouldPunctuateSystemTime() {
        when(taskExecutionMetadata.canProcessTask(eq(task), anyLong())).thenReturn(false);
        when(taskExecutionMetadata.canPunctuateTask(task)).thenReturn(true);
        when(task.maybePunctuateSystemTime()).thenReturn(true);

        taskExecutor.start();

        verify(task, timeout(VERIFICATION_TIMEOUT).atLeast(2)).maybePunctuateSystemTime();
    }

    @Test
    public void shouldRespectPunctuationDisabledByTaskExecutionMetadata() {
        when(taskExecutionMetadata.canProcessTask(eq(task), anyLong())).thenReturn(true);
        when(taskExecutionMetadata.canPunctuateTask(task)).thenReturn(false);
        when(task.isProcessable(anyLong())).thenReturn(true);

        taskExecutor.start();

        verify(task, timeout(VERIFICATION_TIMEOUT).atLeast(2)).process(anyLong());

        taskExecutor.unassign();

        verify(taskManager, timeout(VERIFICATION_TIMEOUT)).unassignTask(task, taskExecutor);
        verify(task, never()).maybePunctuateStreamTime();
        verify(task, never()).maybePunctuateSystemTime();
    }

    @Test
    public void shouldRespectProcessingDisabledByTaskExecutionMetadata() {
        when(taskExecutionMetadata.canProcessTask(eq(task), anyLong())).thenReturn(false);
        when(taskExecutionMetadata.canPunctuateTask(task)).thenReturn(true);
        when(task.isProcessable(anyLong())).thenReturn(true);

        taskExecutor.start();

        verify(task, timeout(VERIFICATION_TIMEOUT)).maybePunctuateSystemTime();
        verify(task, timeout(VERIFICATION_TIMEOUT)).maybePunctuateStreamTime();

        taskExecutor.unassign();

        verify(taskManager, timeout(VERIFICATION_TIMEOUT)).unassignTask(task, taskExecutor);
        verify(task, never()).process(anyLong());
    }

    @Test
    public void shouldUnassignTaskWhenRequired() throws Exception {
        taskExecutor.start();

        verify(taskManager, timeout(VERIFICATION_TIMEOUT)).assignNextTask(taskExecutor);
        TestUtils.waitForCondition(() -> taskExecutor.currentTask() != null,
                VERIFICATION_TIMEOUT,
                "Task reassign take too much time");

        final KafkaFuture<StreamTask> future = taskExecutor.unassign();

        verify(taskManager, timeout(VERIFICATION_TIMEOUT)).unassignTask(task, taskExecutor);
        verify(task).flush();
        assertNull(taskExecutor.currentTask());

        assertTrue(future.isDone(), "Unassign is not completed");
        assertEquals(task, future.get(), "Unexpected task was unassigned");
    }

    @Test
    public void shouldSetUncaughtStreamsException() {
        final StreamsException exception = mock(StreamsException.class);
        when(task.process(anyLong())).thenThrow(exception);

        taskExecutor.start();

        verify(taskManager, timeout(VERIFICATION_TIMEOUT)).setUncaughtException(exception, task.id());
        verify(taskManager, timeout(VERIFICATION_TIMEOUT)).unassignTask(task, taskExecutor);
        assertNull(taskExecutor.currentTask());
        assertTrue(taskExecutor.isRunning(), "should not shut down upon exception");
    }

    @Test
    public void shouldNotFlushOnException() {
        final StreamsException exception = mock(StreamsException.class);
        when(task.process(anyLong())).thenThrow(exception);
        when(taskManager.hasUncaughtException(task.id())).thenReturn(true);

        taskExecutor.start();

        verify(taskManager, timeout(VERIFICATION_TIMEOUT)).unassignTask(task, taskExecutor);
        verify(task, never()).flush();
    }

}
