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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.processor.TaskId;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.apache.kafka.streams.processor.internals.TaskAndAction.Action.ADD;
import static org.apache.kafka.streams.processor.internals.TaskAndAction.Action.REMOVE;
import static org.apache.kafka.streams.processor.internals.TaskAndAction.createAddTask;
import static org.apache.kafka.streams.processor.internals.TaskAndAction.createRemoveTask;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class TaskAndActionTest {

    @Test
    public void shouldCreateAddTaskAction() {
        final StreamTask task = mock(StreamTask.class);

        final TaskAndAction addTask = createAddTask(task);

        assertEquals(ADD, addTask.action());
        assertEquals(task, addTask.task());
        final Exception exceptionForTaskId = assertThrows(IllegalStateException.class, addTask::taskId);
        assertEquals("Action type ADD cannot have a task ID!", exceptionForTaskId.getMessage());
        final Exception exceptionForFutureForRemove = assertThrows(IllegalStateException.class, addTask::futureForRemove);
        assertEquals("Action type ADD cannot have a future with a single result!", exceptionForFutureForRemove.getMessage());
    }

    @Test
    public void shouldCreateRemoveTaskAction() {
        final TaskId taskId = new TaskId(0, 0);
        final CompletableFuture<StateUpdater.RemovedTaskResult> future = new CompletableFuture<>();

        final TaskAndAction removeTask = createRemoveTask(taskId, future);

        assertEquals(REMOVE, removeTask.action());
        assertEquals(taskId, removeTask.taskId());
        assertEquals(future, removeTask.futureForRemove());
        final Exception exceptionForTask = assertThrows(IllegalStateException.class, removeTask::task);
        assertEquals("Action type REMOVE cannot have a task!", exceptionForTask.getMessage());
    }

    @Test
    public void shouldThrowIfAddTaskActionIsCreatedWithNullTask() {
        final Exception exception = assertThrows(NullPointerException.class, () -> createAddTask(null));
        assertTrue(exception.getMessage().contains("Task to add is null!"));
    }

    @Test
    public void shouldThrowIfRemoveTaskActionIsCreatedWithNullTaskId() {
        final Exception exception = assertThrows(
            NullPointerException.class,
            () -> createRemoveTask(null, new CompletableFuture<>())
        );
        assertTrue(exception.getMessage().contains("Task ID of task to remove is null!"));
    }

    @Test
    public void shouldThrowIfRemoveTaskActionIsCreatedWithNullFuture() {
        final Exception exception = assertThrows(
            NullPointerException.class,
            () -> createRemoveTask(new TaskId(0, 0), null)
        );
        assertTrue(exception.getMessage().contains("Future for task to remove is null!"));
    }
}