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

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TasksTest {

    private static final String TOPOLOGY_NAME_0 = "TOPOLOGY_NAME_0";
    private static final String TOPOLOGY_NAME_1 = "TOPOLOGY_NAME_1";

    private static final StreamTask TASK_0_0_0 = createActiveTask(new TaskId(0, 0, TOPOLOGY_NAME_0));
    private static final StreamTask TASK_0_1_0 = createActiveTask(new TaskId(0, 1, TOPOLOGY_NAME_0));
    private static final StreamTask TASK_0_2_0 = createActiveTask(new TaskId(0, 2, TOPOLOGY_NAME_0));
    private static final StreamTask TASK_0_0_1 = createActiveTask(new TaskId(0, 0, TOPOLOGY_NAME_1));
    private static final StreamTask TASK_0_1_1 = createActiveTask(new TaskId(0, 1, TOPOLOGY_NAME_1));

    @Test
    void shouldCreateTasks() {
        final StreamsConfig streamsConfig = new StreamsConfig(StreamsTestUtils.getStreamsConfig());
        final Tasks tasks = new Tasks(
            "[test]",
            new TopologyMetadata(new ConcurrentSkipListMap<>(), streamsConfig),
            null,
            null,
            null);
        tasks.addTask(TASK_0_0_1);
        tasks.addTask(TASK_0_1_0);
        tasks.addTask(TASK_0_1_1);
        tasks.addTask(TASK_0_2_0);

        final List<Task> orderedActiveTasks = tasks.orderedActiveTasks();

        assertIterableEquals(Arrays.asList(TASK_0_0_0, TASK_0_0_1, TASK_0_1_0, TASK_0_1_1, TASK_0_2_0), orderedActiveTasks);
        assertThrows(UnsupportedOperationException.class, () -> orderedActiveTasks.add(TASK_0_0_0));
    }

    @Test
    void shouldReturnActiveTasksOrderedImmutable() {
        final StreamsConfig streamsConfig = new StreamsConfig(StreamsTestUtils.getStreamsConfig());
        final Tasks tasks = new Tasks(
            "[test]",
            new TopologyMetadata(new ConcurrentSkipListMap<>(), streamsConfig),
            null,
            null,
            null);
        tasks.addTask(TASK_0_0_0);
        tasks.addTask(TASK_0_0_1);
        tasks.addTask(TASK_0_1_0);
        tasks.addTask(TASK_0_1_1);
        tasks.addTask(TASK_0_2_0);

        final List<Task> orderedActiveTasks = tasks.orderedActiveTasks();

        assertIterableEquals(Arrays.asList(TASK_0_0_0, TASK_0_0_1, TASK_0_1_0, TASK_0_1_1, TASK_0_2_0), orderedActiveTasks);
        assertThrows(UnsupportedOperationException.class, () -> orderedActiveTasks.add(TASK_0_0_0));
    }

    @Test
    void shouldMoveTaskOfGivenNamedTopologyToTailOfList() {
        final StreamsConfig streamsConfig = new StreamsConfig(StreamsTestUtils.getStreamsConfig());
        final Tasks tasks = new Tasks(
            "[test]",
            new TopologyMetadata(new ConcurrentSkipListMap<>(), streamsConfig),
            null,
            null,
            null);
        tasks.addTask(TASK_0_0_0);
        tasks.addTask(TASK_0_0_1);
        tasks.addTask(TASK_0_1_0);
        tasks.addTask(TASK_0_1_1);
        tasks.addTask(TASK_0_2_0);

        tasks.moveActiveTasksToTailFor(TOPOLOGY_NAME_1);

        final List<Task> orderedActiveTasks = tasks.orderedActiveTasks();
        assertEquals(
            3,
            orderedActiveTasks.subList(0, 3).stream().filter(task -> task.id().topologyName().equals(TOPOLOGY_NAME_0)).count()
        );
        assertEquals(
            2,
            orderedActiveTasks.subList(3, 5).stream().filter(task -> task.id().topologyName().equals(TOPOLOGY_NAME_1)).count()
        );
    }

    private static StreamTask createActiveTask(final TaskId taskId) {
        final StreamTask task = mock(StreamTask.class);
        when(task.id()).thenReturn(taskId);
        when(task.isActive()).thenReturn(true);
        return task;
    }
}