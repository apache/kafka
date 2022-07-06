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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.TaskId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.test.StreamsTestUtils.createStandbyTask;
import static org.apache.kafka.test.StreamsTestUtils.createStatefulTask;
import static org.apache.kafka.test.StreamsTestUtils.createStatelessTask;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TasksTest {

    private final static TaskId TASK_0_0 = new TaskId(0, 0);
    private final static TopicPartition TOPIC_PARTITION_A_0 = new TopicPartition("topicA", 0);

    private final TopologyMetadata topologyMetadata = mock(TopologyMetadata.class);

    private final Tasks tasks = new Tasks(
        new LogContext(),
        topologyMetadata,
        mock(ActiveTaskCreator.class),
        mock(StandbyTaskCreator.class)
    );

    @Test
    public void testNotPausedTasks() {
        final String unnamedTopologyName = null;
        when(topologyMetadata.isPaused(unnamedTopologyName))
            .thenReturn(false)
            .thenReturn(false).thenReturn(false)
            .thenReturn(true)
            .thenReturn(true).thenReturn(true);

        final TaskId taskId1 = new TaskId(0, 1);
        final TaskId taskId2 = new TaskId(0, 2);

        final StreamTask streamTask = mock(StreamTask.class);
        when(streamTask.isActive()).thenReturn(true);
        when(streamTask.id()).thenReturn(taskId1);

        final StandbyTask standbyTask1 = mock(StandbyTask.class);
        when(standbyTask1.isActive()).thenReturn(false);
        when(standbyTask1.id()).thenReturn(taskId2);

        tasks.addTask(streamTask);
        tasks.addTask(standbyTask1);
        Assert.assertEquals(tasks.notPausedActiveTasks().size(), 1);
        Assert.assertEquals(tasks.notPausedTasks().size(), 2);

        Assert.assertEquals(tasks.notPausedActiveTasks().size(), 0);
        Assert.assertEquals(tasks.notPausedTasks().size(), 0);
    }

    @Test
    public void shouldAddStatelessTaskToTasks() {
        final Task task = createStatelessTask(TASK_0_0);
        when(task.inputPartitions())
            .thenReturn(mkSet(new TopicPartition("input1", 0), new TopicPartition("input2", 0)));
        shouldAddActiveTaskToTasks(task);
    }

    @Test
    public void shouldAddStatefulTaskToTasks() {
        final Task task = createStatefulTask(TASK_0_0, Collections.singleton(TOPIC_PARTITION_A_0));
        when(task.inputPartitions())
            .thenReturn(mkSet(new TopicPartition("input1", 0), new TopicPartition("input2", 0)));
        shouldAddActiveTaskToTasks(task);
    }

    private void shouldAddActiveTaskToTasks(final Task task) {
        tasks.addTask(task);

        final Collection<Task> allTasks = tasks.allTasks();
        assertEquals(1, allTasks.size());
        assertTrue(allTasks.contains(task));
        final Collection<Task> activeTasks = tasks.activeTasks();
        assertEquals(1, activeTasks.size());
        assertTrue(activeTasks.contains(task));
        final Map<TaskId, Task> tasksPerId = tasks.tasksPerId();
        assertEquals(1, tasksPerId.size());
        assertEquals(task, tasksPerId.get(task.id()));
        assertEquals(task, tasks.task(task.id()));
        final Collection<Task> foundTasks = tasks.tasks(mkSet(task.id()));
        assertEquals(1, foundTasks.size());
        assertTrue(foundTasks.contains(task));
        final Collection<Task> nonPausedActiveTasks = tasks.notPausedActiveTasks();
        assertEquals(1, nonPausedActiveTasks.size());
        assertTrue(nonPausedActiveTasks.contains(task));
        final Collection<Task> nonPausedTasks = tasks.notPausedTasks();
        assertEquals(1, nonPausedTasks.size());
        assertTrue(nonPausedTasks.contains(task));
        for (final TopicPartition topicPartition : task.inputPartitions()) {
            assertEquals(task, tasks.activeTasksForInputPartition(topicPartition));
        }
        assertTrue(tasks.standbyTasks().isEmpty());
    }

    @Test
    public void shouldAddStandbyTaskToTasks() {
        final Task task = createStandbyTask(TASK_0_0, Collections.singleton(TOPIC_PARTITION_A_0));

        tasks.addTask(task);

        final Collection<Task> allTasks = tasks.allTasks();
        assertEquals(1, allTasks.size());
        assertTrue(allTasks.contains(task));
        final Map<TaskId, Task> tasksPerId = tasks.tasksPerId();
        assertEquals(1, tasksPerId.size());
        assertEquals(task, tasksPerId.get(task.id()));
        assertEquals(task, tasks.task(task.id()));
        final Collection<Task> foundTasks = tasks.tasks(mkSet(task.id()));
        assertEquals(1, foundTasks.size());
        assertTrue(foundTasks.contains(task));
        final Collection<Task> nonPausedTasks = tasks.notPausedTasks();
        assertEquals(1, nonPausedTasks.size());
        assertTrue(nonPausedTasks.contains(task));
        final Collection<Task> standbyTasks = tasks.standbyTasks();
        assertEquals(1, standbyTasks.size());
        assertTrue(standbyTasks.contains(task));
        assertTrue(tasks.notPausedActiveTasks().isEmpty());
        assertTrue(tasks.activeTasks().isEmpty());
    }

    @Test
    public void shouldRemoveStatelessTaskFromTasks() {
        final Task task = createStatelessTask(TASK_0_0);
        when(task.inputPartitions())
            .thenReturn(mkSet(new TopicPartition("input1", 0), new TopicPartition("input2", 0)));
        shouldRemoveActiveTaskFromTasks(task);
    }

    @Test
    public void shouldRemoveStatefulTaskFromTasks() {
        final Task task = createStatefulTask(TASK_0_0, Collections.singleton(TOPIC_PARTITION_A_0));
        when(task.inputPartitions())
            .thenReturn(mkSet(new TopicPartition("input1", 0), new TopicPartition("input2", 0)));
        shouldRemoveActiveTaskFromTasks(task);
    }

    private void shouldRemoveActiveTaskFromTasks(final Task task) {
        tasks.addTask(task);

        final Task removedTask = tasks.removeTask(task.id());

        assertEquals(task, removedTask);
        assertTrue(tasks.allTasks().isEmpty());
        assertTrue(tasks.activeTasks().isEmpty());
        assertTrue(tasks.tasksPerId().isEmpty());
        final String expectedMessage = "Task unknown: " + task.id();
        final Exception exceptionGetSingleTask = assertThrows(IllegalStateException.class, () -> tasks.task(task.id()));
        assertEquals(expectedMessage, exceptionGetSingleTask.getMessage());
        final Exception exceptionGetMultipleTask =
            assertThrows(IllegalStateException.class, () -> tasks.tasks(mkSet(task.id())));
        assertEquals(expectedMessage, exceptionGetMultipleTask.getMessage());
        assertTrue(tasks.notPausedActiveTasks().isEmpty());
        assertTrue(tasks.notPausedTasks().isEmpty());
        for (final TopicPartition topicPartition : task.inputPartitions()) {
            assertNull(tasks.activeTasksForInputPartition(topicPartition));
        }
        assertTrue(tasks.standbyTasks().isEmpty());
    }

    @Test
    public void shouldRemoveStandbyTaskFromTasks() {
        final Task task = createStandbyTask(TASK_0_0, Collections.singleton(TOPIC_PARTITION_A_0));
        tasks.addTask(task);

        final Task removedTask = tasks.removeTask(task.id());

        assertEquals(task, removedTask);
        assertTrue(tasks.allTasks().isEmpty());
        assertTrue(tasks.activeTasks().isEmpty());
        assertTrue(tasks.tasksPerId().isEmpty());
        final String expectedMessage = "Task unknown: " + task.id();
        final Exception exceptionGetSingleTask = assertThrows(IllegalStateException.class, () -> tasks.task(task.id()));
        assertEquals(expectedMessage, exceptionGetSingleTask.getMessage());
        final Exception exceptionGetMultipleTask =
            assertThrows(IllegalStateException.class, () -> tasks.tasks(mkSet(task.id())));
        assertEquals(expectedMessage, exceptionGetMultipleTask.getMessage());
        assertTrue(tasks.notPausedActiveTasks().isEmpty());
        assertTrue(tasks.notPausedTasks().isEmpty());
        for (final TopicPartition topicPartition : task.inputPartitions()) {
            assertNull(tasks.activeTasksForInputPartition(topicPartition));
        }
        assertTrue(tasks.standbyTasks().isEmpty());
    }
}
