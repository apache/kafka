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

        assertEquals(ADD, addTask.getAction());
        assertEquals(task, addTask.getTask());
        final Exception exception = assertThrows(IllegalStateException.class, addTask::getTaskId);
        assertEquals("Action type ADD cannot have a task ID!", exception.getMessage());
    }

    @Test
    public void shouldCreateRemoveTaskAction() {
        final TaskId taskId = new TaskId(0, 0);

        final TaskAndAction removeTask = createRemoveTask(taskId);

        assertEquals(REMOVE, removeTask.getAction());
        assertEquals(taskId, removeTask.getTaskId());
        final Exception exception = assertThrows(IllegalStateException.class, removeTask::getTask);
        assertEquals("Action type REMOVE cannot have a task!", exception.getMessage());
    }

    @Test
    public void shouldThrowIfAddTaskActionIsCreatedWithNullTask() {
        final Exception exception = assertThrows(NullPointerException.class, () -> createAddTask(null));
        assertTrue(exception.getMessage().contains("Task to add is null!"));
    }

    @Test
    public void shouldThrowIfRemoveTaskActionIsCreatedWithNullTaskId() {
        final Exception exception = assertThrows(NullPointerException.class, () -> createRemoveTask(null));
        assertTrue(exception.getMessage().contains("Task ID of task to remove is null!"));
    }
}