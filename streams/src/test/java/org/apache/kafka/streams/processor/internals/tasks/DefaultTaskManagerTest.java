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
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.TaskExecutionMetadata;
import org.apache.kafka.streams.processor.internals.TasksRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkObjectProperties;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultTaskManagerTest {

    private final Time time = new MockTime(1L);
    private final StreamTask task = mock(StreamTask.class);
    private final TasksRegistry tasks = mock(TasksRegistry.class);
    private final TaskExecutor taskExecutor = mock(TaskExecutor.class);
    private final StreamsException exception = mock(StreamsException.class);
    private final TaskExecutionMetadata taskExecutionMetadata = mock(TaskExecutionMetadata.class);

    private final StreamsConfig config = new StreamsConfig(configProps());
    private final TaskManager taskManager = new DefaultTaskManager(time, "TaskManager", tasks, config,
        (taskManager, name, time, taskExecutionMetadata) -> taskExecutor, taskExecutionMetadata);

    private Properties configProps() {
        return mkObjectProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171"),
            mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2)
        ));
    }

    @BeforeEach
    public void setUp() {
        when(task.id()).thenReturn(new TaskId(0, 0, "A"));
        when(task.isProcessable(anyLong())).thenReturn(true);
        when(task.isActive()).thenReturn(true);
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
        when(tasks.task(task.id())).thenReturn(task);
        when(tasks.contains(task.id())).thenReturn(true);

        assertTrue(taskManager.lockTasks(Collections.singleton(task.id())).isDone());

        assertNull(taskManager.assignNextTask(taskExecutor));
    }

    @Test
    public void shouldNotAssignAnyLockedTask() {
        taskManager.add(Collections.singleton(task));
        when(tasks.activeTasks()).thenReturn(Collections.singleton(task));
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

        final KafkaFuture<Void> lockFuture = taskManager.lockAllTasks();
        assertFalse(lockFuture.isDone());

        verify(taskExecutor).unassign();

        future.complete(task);
        assertTrue(lockFuture.isDone());
    }
}
