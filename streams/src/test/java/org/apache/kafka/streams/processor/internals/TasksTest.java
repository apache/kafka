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
import org.apache.kafka.streams.processor.TaskId;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TasksTest {
    @Test
    public void testNotPausedTasks() {
        final TopologyMetadata topologyMetadata = mock(TopologyMetadata.class);
        final String unnamedTopologyName = null;
        when(topologyMetadata.isPaused(unnamedTopologyName))
            .thenReturn(false)
            .thenReturn(false).thenReturn(false)
            .thenReturn(true)
            .thenReturn(true).thenReturn(true);

        final Tasks tasks = new Tasks(
            new LogContext(),
            topologyMetadata,
            mock(ActiveTaskCreator.class),
            mock(StandbyTaskCreator.class)
        );

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
}
