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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.TaskId;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.test.StreamsTestUtils.TaskBuilder.standbyTask;
import static org.apache.kafka.test.StreamsTestUtils.TaskBuilder.statefulTask;
import static org.apache.kafka.test.StreamsTestUtils.TaskBuilder.statelessTask;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TasksTest {

    private final static TopicPartition TOPIC_PARTITION_A_0 = new TopicPartition("topicA", 0);
    private final static TopicPartition TOPIC_PARTITION_A_1 = new TopicPartition("topicA", 1);
    private final static TaskId TASK_0_0 = new TaskId(0, 0);
    private final static TaskId TASK_0_1 = new TaskId(0, 1);
    private final static TaskId TASK_1_0 = new TaskId(1, 0);

    private final LogContext logContext = new LogContext();
    private final ActiveTaskCreator activeTaskCreator = mock(ActiveTaskCreator.class);
    private final StandbyTaskCreator standbyTaskCreator = mock(StandbyTaskCreator.class);
    private final StateUpdater stateUpdater = mock(StateUpdater.class);

    private Consumer<byte[], byte[]> mainConsumer = null;

    @Test
    public void shouldCreateTasksWithStateUpdater() {
        final Tasks tasks = new Tasks(logContext, activeTaskCreator, standbyTaskCreator, stateUpdater);
        tasks.setMainConsumer(mainConsumer);
        final StreamTask statefulTask = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).build();
        final StandbyTask standbyTask = standbyTask(TASK_0_1, mkSet(TOPIC_PARTITION_A_1)).build();
        final StreamTask statelessTask = statelessTask(TASK_1_0).build();
        final Map<TaskId, Set<TopicPartition>> activeTasks = mkMap(
            mkEntry(statefulTask.id(), statefulTask.changelogPartitions()),
            mkEntry(statelessTask.id(), statelessTask.changelogPartitions())
        );
        final Map<TaskId, Set<TopicPartition>> standbyTasks =
            mkMap(mkEntry(standbyTask.id(), standbyTask.changelogPartitions()));
        when(activeTaskCreator.createTasks(mainConsumer, activeTasks)).thenReturn(Arrays.asList(statefulTask, statelessTask));
        when(standbyTaskCreator.createTasks(standbyTasks)).thenReturn(Collections.singletonList(standbyTask));

        tasks.createTasks(activeTasks, standbyTasks);

        final Exception exceptionForStatefulTaskOnTask = assertThrows(IllegalStateException.class, () -> tasks.task(statefulTask.id()));
        assertEquals("Task unknown: " + statefulTask.id(), exceptionForStatefulTaskOnTask.getMessage());
        assertFalse(tasks.activeTasks().contains(statefulTask));
        assertFalse(tasks.allTasks().contains(statefulTask));
        final Exception exceptionForStatefulTaskOnTasks = assertThrows(IllegalStateException.class, () -> tasks.tasks(mkSet(statefulTask.id())));
        assertEquals("Task unknown: " + statefulTask.id(), exceptionForStatefulTaskOnTasks.getMessage());
        final Exception exceptionForStatelessTaskOnTask = assertThrows(IllegalStateException.class, () -> tasks.task(statelessTask.id()));
        assertEquals("Task unknown: " + statelessTask.id(), exceptionForStatelessTaskOnTask.getMessage());
        assertFalse(tasks.activeTasks().contains(statelessTask));
        assertFalse(tasks.allTasks().contains(statelessTask));
        final Exception exceptionForStatelessTaskOnTasks = assertThrows(IllegalStateException.class, () -> tasks.tasks(mkSet(statelessTask.id())));
        assertEquals("Task unknown: " + statelessTask.id(), exceptionForStatelessTaskOnTasks.getMessage());
        final Exception exceptionForStandbyTaskOnTask = assertThrows(IllegalStateException.class, () -> tasks.task(standbyTask.id()));
        assertEquals("Task unknown: " + standbyTask.id(), exceptionForStandbyTaskOnTask.getMessage());
        assertFalse(tasks.allTasks().contains(standbyTask));
        final Exception exceptionForStandByTaskOnTasks = assertThrows(IllegalStateException.class, () -> tasks.tasks(mkSet(standbyTask.id())));
        assertEquals("Task unknown: " + standbyTask.id(), exceptionForStandByTaskOnTasks.getMessage());
        verify(activeTaskCreator).createTasks(mainConsumer, activeTasks);
        verify(standbyTaskCreator).createTasks(standbyTasks);
        verify(stateUpdater).add(statefulTask);
    }

    @Test
    public void shouldCreateTasksWithoutStateUpdater() {
        final Tasks tasks = new Tasks(logContext, activeTaskCreator, standbyTaskCreator, null);
        tasks.setMainConsumer(mainConsumer);
        final StreamTask statefulTask = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).build();
        final StandbyTask standbyTask = standbyTask(TASK_0_1, mkSet(TOPIC_PARTITION_A_1)).build();
        final StreamTask statelessTask = statelessTask(TASK_1_0).build();
        final Map<TaskId, Set<TopicPartition>> activeTasks = mkMap(
            mkEntry(statefulTask.id(), statefulTask.changelogPartitions()),
            mkEntry(statelessTask.id(), statelessTask.changelogPartitions())
        );
        final Map<TaskId, Set<TopicPartition>> standbyTasks =
            mkMap(mkEntry(standbyTask.id(), standbyTask.changelogPartitions()));
        when(activeTaskCreator.createTasks(mainConsumer, activeTasks)).thenReturn(Arrays.asList(statefulTask, statelessTask));
        when(standbyTaskCreator.createTasks(standbyTasks)).thenReturn(Collections.singletonList(standbyTask));

        tasks.createTasks(activeTasks, standbyTasks);

        assertEquals(statefulTask, tasks.task(statefulTask.id()));
        assertTrue(tasks.activeTasks().contains(statefulTask));
        assertTrue(tasks.allTasks().contains(statefulTask));
        assertTrue(tasks.tasks(mkSet(statefulTask.id())).contains(statefulTask));
        assertEquals(statelessTask, tasks.task(statelessTask.id()));
        assertTrue(tasks.activeTasks().contains(statelessTask));
        assertTrue(tasks.allTasks().contains(statelessTask));
        assertTrue(tasks.tasks(mkSet(statelessTask.id())).contains(statelessTask));
        assertEquals(standbyTask, tasks.task(standbyTask.id()));
        assertTrue(tasks.allTasks().contains(standbyTask));
        assertTrue(tasks.tasks(mkSet(standbyTask.id())).contains(standbyTask));
        verify(activeTaskCreator).createTasks(mainConsumer, activeTasks);
        verify(standbyTaskCreator).createTasks(standbyTasks);
        verify(stateUpdater, never()).add(statefulTask);
    }
}