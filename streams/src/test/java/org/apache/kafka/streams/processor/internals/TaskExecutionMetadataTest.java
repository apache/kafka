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

import org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode;
import org.apache.kafka.streams.processor.TaskId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.kafka.streams.processor.internals.TopologyMetadata.UNNAMED_TOPOLOGY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaskExecutionMetadataTest {
    final static String TOPOLOGY1 = "topology1";
    final static String TOPOLOGY2 = "topology2";
    final static Set<String> NAMED_TOPOLOGIES = new HashSet<>(Arrays.asList(TOPOLOGY1, TOPOLOGY2));
    final static int TIME_ZERO = 0;
    final static int CONSTANT_BACKOFF_MS = 5000;

    @Test
    public void testCanProcessWithoutNamedTopologies() {
        final Set<String> topologies = Collections.singleton(UNNAMED_TOPOLOGY);
        final Set<String> pausedTopologies = new HashSet<>();

        final TaskExecutionMetadata metadata = new TaskExecutionMetadata(topologies, pausedTopologies, ProcessingMode.AT_LEAST_ONCE);

        final Task mockTask = createMockTask(UNNAMED_TOPOLOGY);

        Assert.assertTrue(metadata.canProcessTask(mockTask, TIME_ZERO));
        // This pauses an UNNAMED_TOPOLOGY / a KafkaStreams instance without named/modular
        // topologies.
        pausedTopologies.add(UNNAMED_TOPOLOGY);
        Assert.assertFalse(metadata.canProcessTask(mockTask, TIME_ZERO));
    }

    @Test
    public void testNamedTopologiesCanBePausedIndependently() {
        final Set<String> pausedTopologies = new HashSet<>();
        final TaskExecutionMetadata metadata = new TaskExecutionMetadata(NAMED_TOPOLOGIES, pausedTopologies, ProcessingMode.AT_LEAST_ONCE);

        final Task mockTask1 = createMockTask(TOPOLOGY1);
        final Task mockTask2 = createMockTask(TOPOLOGY2);

        Assert.assertTrue(metadata.canProcessTask(mockTask1, TIME_ZERO));
        Assert.assertTrue(metadata.canProcessTask(mockTask2, TIME_ZERO));

        pausedTopologies.add(TOPOLOGY1);
        Assert.assertFalse(metadata.canProcessTask(mockTask1, TIME_ZERO));
        Assert.assertTrue(metadata.canProcessTask(mockTask2, TIME_ZERO));

        pausedTopologies.remove(TOPOLOGY1);
        Assert.assertTrue(metadata.canProcessTask(mockTask1, TIME_ZERO));
        Assert.assertTrue(metadata.canProcessTask(mockTask2, TIME_ZERO));
    }

    @Test
    public void testNamedTopologiesCanBeStartedPaused() {
        final Set<String> pausedTopologies = new HashSet<>();
        pausedTopologies.add(TOPOLOGY1);

        final TaskExecutionMetadata metadata = new TaskExecutionMetadata(NAMED_TOPOLOGIES, pausedTopologies, ProcessingMode.AT_LEAST_ONCE);

        final Task mockTask1 = createMockTask(TOPOLOGY1);
        final Task mockTask2 = createMockTask(TOPOLOGY2);

        Assert.assertFalse(metadata.canProcessTask(mockTask1, TIME_ZERO));
        Assert.assertTrue(metadata.canProcessTask(mockTask2, TIME_ZERO));

        pausedTopologies.remove(TOPOLOGY1);
        Assert.assertTrue(metadata.canProcessTask(mockTask1, TIME_ZERO));
        Assert.assertTrue(metadata.canProcessTask(mockTask2, TIME_ZERO));
    }

    @Test
    public void testNamedTopologiesCanBackoff() {
        final Set<String> pausedTopologies = new HashSet<>();

        final TaskExecutionMetadata metadata = new TaskExecutionMetadata(NAMED_TOPOLOGIES, pausedTopologies, ProcessingMode.AT_LEAST_ONCE);

        final Task mockTask1 = createMockTask(TOPOLOGY1);
        final Task mockTask2 = createMockTask(TOPOLOGY2);

        Assert.assertTrue(metadata.canProcessTask(mockTask1, TIME_ZERO));
        Assert.assertTrue(metadata.canProcessTask(mockTask2, TIME_ZERO));

        metadata.registerTaskError(mockTask1, new Throwable("Error"), TIME_ZERO);
        Assert.assertFalse(metadata.canProcessTask(mockTask1, CONSTANT_BACKOFF_MS - 1));
        Assert.assertTrue(metadata.canProcessTask(mockTask2, CONSTANT_BACKOFF_MS - 1));

        Assert.assertFalse(metadata.canProcessTask(mockTask1, CONSTANT_BACKOFF_MS));
        Assert.assertTrue(metadata.canProcessTask(mockTask2, CONSTANT_BACKOFF_MS));

        Assert.assertTrue(metadata.canProcessTask(mockTask1, CONSTANT_BACKOFF_MS + 1));
        Assert.assertTrue(metadata.canProcessTask(mockTask2, CONSTANT_BACKOFF_MS + 1));
    }

    private static Task createMockTask(final String topologyName) {
        final Task mockTask = mock(Task.class);
        final TaskId taskId = new TaskId(0, 0, topologyName);
        when(mockTask.id()).thenReturn(taskId);
        return mockTask;
    }
}