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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.TaskExecutionMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

    private final static long VERIFICATION_TIMEOUT = 15000;

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
        when(task.process(anyLong())).thenReturn(true);
        when(task.prepareCommit()).thenReturn(Collections.emptyMap());
    }

    @AfterEach
    public void tearDown() {
        taskExecutor.shutdown(Duration.ofMinutes(1));
    }

    @Test
    public void shouldShutdownTaskExecutor() {
        assertNull(taskExecutor.currentTask(), "Have task assigned before startup");

        taskExecutor.start();

        verify(taskManager, timeout(VERIFICATION_TIMEOUT)).assignNextTask(taskExecutor);

        taskExecutor.shutdown(Duration.ofMinutes(1));

        verify(task).prepareCommit();
        verify(taskManager).unassignTask(task, taskExecutor);

        assertNull(taskExecutor.currentTask(), "Have task assigned after shutdown");
    }

    @Test
    public void shouldUnassignTaskWhenNotProgressing() {
        when(task.isProcessable(anyLong())).thenReturn(false);
        when(task.maybePunctuateStreamTime()).thenReturn(false);
        when(task.maybePunctuateSystemTime()).thenReturn(false);

        taskExecutor.start();

        verify(taskManager, timeout(VERIFICATION_TIMEOUT)).unassignTask(task, taskExecutor);
        verify(task).prepareCommit();
        assertNull(taskExecutor.currentTask());
    }

    @Test
    public void shouldProcessTasks() {
        when(taskExecutionMetadata.canProcessTask(any(), anyLong())).thenReturn(true);
        when(task.isProcessable(anyLong())).thenReturn(true);

        taskExecutor.start();

        verify(task, timeout(VERIFICATION_TIMEOUT).atLeast(2)).process(anyLong());
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
        assertNotNull(taskExecutor.currentTask());

        final KafkaFuture<StreamTask> future = taskExecutor.unassign();

        verify(taskManager, timeout(VERIFICATION_TIMEOUT)).unassignTask(task, taskExecutor);
        verify(task).prepareCommit();
        assertNull(taskExecutor.currentTask());

        assertTrue(future.isDone(), "Unassign is not completed");
        assertEquals(task, future.get(), "Unexpected task was unassigned");
    }

    @Test
    public void shouldSetUncaughtStreamsException() {
        final StreamsException exception = mock(StreamsException.class);
        when(task.process(anyLong())).thenThrow(exception);

        taskExecutor.start();

        verify(taskManager, timeout(VERIFICATION_TIMEOUT)).assignNextTask(taskExecutor);
        verify(taskManager, timeout(VERIFICATION_TIMEOUT)).setUncaughtException(exception, task.id());
        verify(taskManager, timeout(VERIFICATION_TIMEOUT)).unassignTask(task, taskExecutor);
        assertNull(taskExecutor.currentTask());
    }

}
