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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.junit.Assert;
import org.junit.Test;

public class TasksTest {
    final static String CLIENT_ID = "client-id";
    final static String VERSION = "latest";
    final MockTime time = new MockTime(0);

    @Test
    public void testNotPausedTasks() {
        final LogContext logContext = new LogContext();
        final Metrics metrics = new Metrics();
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, CLIENT_ID, VERSION, time);

        final ActiveTaskCreator mockActiveTaskCreater = mock(ActiveTaskCreator.class);
        final StandbyTaskCreator mockStandbyTaskCreator = mock(StandbyTaskCreator.class);

        final TopologyMetadata topologyMetadata = mock(TopologyMetadata.class);
        when(topologyMetadata.isPaused(null))
            .thenReturn(false)
            .thenReturn(false).thenReturn(false)
            .thenReturn(true)
            .thenReturn(true).thenReturn(true);

        final Tasks tasks =
            new Tasks(logContext, topologyMetadata, streamsMetrics, mockActiveTaskCreater, mockStandbyTaskCreator);

        final TaskId taskId1 = new TaskId(0, 1);
        final TopicPartition topicPartition1 = new TopicPartition("topic", 1);
        final TaskId taskId2 = new TaskId(0, 2);
        final TopicPartition topicPartition2 = new TopicPartition("topic", 2);
        final Map<TaskId, Set<TopicPartition>> activeTasks = Collections.singletonMap(taskId1,
            Collections.singleton(topicPartition1));
        final Map<TaskId, Set<TopicPartition>> standbyTasks = Collections.singletonMap(taskId2,
            Collections.singleton(topicPartition2));

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
    public void testNotPausedTasksWithNamedTopologies() {

    }
}
