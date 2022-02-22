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

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.apache.kafka.streams.processor.internals.TopologyMetadata.UNNAMED_TOPOLOGY;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TasksTest {

    private static final String TOPOLOGY_NAME_0 = "TOPOLOGY_NAME_0";
    private static final String TOPOLOGY_NAME_1 = "TOPOLOGY_NAME_1";

    private static final StreamTask TASK_0_0 = createActiveTask(new TaskId(0, 0));
    private static final StreamTask TASK_0_1 = createActiveTask(new TaskId(0, 1));

    private static final StreamTask TASK_0_0_0 = createActiveTask(new TaskId(0, 0, TOPOLOGY_NAME_0));
    private static final StreamTask TASK_0_1_0 = createActiveTask(new TaskId(0, 1, TOPOLOGY_NAME_0));
    private static final StreamTask TASK_0_2_0 = createActiveTask(new TaskId(0, 2, TOPOLOGY_NAME_0));
    private static final StreamTask TASK_0_0_1 = createActiveTask(new TaskId(0, 0, TOPOLOGY_NAME_1));
    private static final StreamTask TASK_0_1_1 = createActiveTask(new TaskId(0, 1, TOPOLOGY_NAME_1));

    @Test
    void shouldReturnAllTasksInUnnamedTopology() {
        final StreamsConfig streamsConfig = new StreamsConfig(StreamsTestUtils.getStreamsConfig());
        final Tasks tasks = new Tasks(
            new LogContext("[test]"),
            new TopologyMetadata(new ConcurrentSkipListMap<>(), streamsConfig),
            null,
            null,
            null);
        tasks.addTask(TASK_0_0);
        tasks.addTask(TASK_0_1);

        final Map<String, Set<Task>> activeTasksByTopology = tasks.activeTasksByTopology();

        assertThat(activeTasksByTopology.size(), equalTo(1));
        assertThat(activeTasksByTopology.containsKey(UNNAMED_TOPOLOGY), is(true));
        assertIterableEquals(Arrays.asList(TASK_0_0, TASK_0_1), activeTasksByTopology.get(UNNAMED_TOPOLOGY));
    }

    @Test
    void shouldReturnStreamTasksInNamedTopologies() {
        final StreamsConfig streamsConfig = new StreamsConfig(StreamsTestUtils.getStreamsConfig());
        final Tasks tasks = new Tasks(
            new LogContext("[test]"),
            new TopologyMetadata(new ConcurrentSkipListMap<>(), streamsConfig),
            null,
            null,
            null);
        tasks.addTask(TASK_0_0_0);
        tasks.addTask(TASK_0_0_1);
        tasks.addTask(TASK_0_1_0);
        tasks.addTask(TASK_0_1_1);
        tasks.addTask(TASK_0_2_0);

        final Map<String, Set<Task>> activeTasksByTopology = tasks.activeTasksByTopology();

        assertThat(activeTasksByTopology.size(), equalTo(2));
        assertIterableEquals(
            Arrays.asList(TASK_0_0_0, TASK_0_1_0, TASK_0_2_0),
            activeTasksByTopology.get(TOPOLOGY_NAME_0)
        );
        assertIterableEquals(
            Arrays.asList(TASK_0_0_1, TASK_0_1_1),
            activeTasksByTopology.get(TOPOLOGY_NAME_1)
        );
    }

    private static StreamTask createActiveTask(final TaskId taskId) {
        final StreamTask task = mock(StreamTask.class);
        when(task.id()).thenReturn(taskId);
        when(task.isActive()).thenReturn(true);
        return task;
    }
}