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

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.kafka.streams.processor.internals.TopologyMetadata.UNNAMED_TOPOLOGY;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaskExecutionMetadataTest {
    static final String TOPOLOGY1 = "topology1";
    static final String TOPOLOGY2 = "topology2";
    static final Set<String> NAMED_TOPOLOGIES = Set.of(TOPOLOGY1, TOPOLOGY2);
    static final int TIME_ZERO = 0;
    static final int CONSTANT_BACKOFF_MS = 5000;

    @Test
    public void testCanProcessWithoutNamedTopologies() {
        final Set<String> topologies = Collections.singleton(UNNAMED_TOPOLOGY);
        final Set<String> pausedTopologies = new HashSet<>();

        final TaskExecutionMetadata metadata = new TaskExecutionMetadata(topologies, pausedTopologies, ProcessingMode.AT_LEAST_ONCE);

        final Task mockTask = createMockTask(UNNAMED_TOPOLOGY);

        assertTrue(metadata.canProcessTask(mockTask, TIME_ZERO));
        // This pauses an UNNAMED_TOPOLOGY / a KafkaStreams instance without named/modular
        // topologies.
        pausedTopologies.add(UNNAMED_TOPOLOGY);
        assertFalse(metadata.canProcessTask(mockTask, TIME_ZERO));
    }

    @Test
    public void testNamedTopologiesCanBePausedIndependently() {
        final Set<String> pausedTopologies = new HashSet<>();
        final TaskExecutionMetadata metadata = new TaskExecutionMetadata(NAMED_TOPOLOGIES, pausedTopologies, ProcessingMode.AT_LEAST_ONCE);

        final Task mockTask1 = createMockTask(TOPOLOGY1);
        final Task mockTask2 = createMockTask(TOPOLOGY2);

        assertTrue(metadata.canProcessTask(mockTask1, TIME_ZERO));
        assertTrue(metadata.canProcessTask(mockTask2, TIME_ZERO));

        pausedTopologies.add(TOPOLOGY1);
        assertFalse(metadata.canProcessTask(mockTask1, TIME_ZERO));
        assertTrue(metadata.canProcessTask(mockTask2, TIME_ZERO));

        pausedTopologies.remove(TOPOLOGY1);
        assertTrue(metadata.canProcessTask(mockTask1, TIME_ZERO));
        assertTrue(metadata.canProcessTask(mockTask2, TIME_ZERO));
    }

    @Test
    public void testNamedTopologiesCanBeStartedPaused() {
        final Set<String> pausedTopologies = new HashSet<>();
        pausedTopologies.add(TOPOLOGY1);

        final TaskExecutionMetadata metadata = new TaskExecutionMetadata(NAMED_TOPOLOGIES, pausedTopologies, ProcessingMode.AT_LEAST_ONCE);

        final Task mockTask1 = createMockTask(TOPOLOGY1);
        final Task mockTask2 = createMockTask(TOPOLOGY2);

        assertFalse(metadata.canProcessTask(mockTask1, TIME_ZERO));
        assertTrue(metadata.canProcessTask(mockTask2, TIME_ZERO));

        pausedTopologies.remove(TOPOLOGY1);
        assertTrue(metadata.canProcessTask(mockTask1, TIME_ZERO));
        assertTrue(metadata.canProcessTask(mockTask2, TIME_ZERO));
    }

    @Test
    public void testNamedTopologiesCanBackoff() {
        final Set<String> pausedTopologies = new HashSet<>();

        final TaskExecutionMetadata metadata = new TaskExecutionMetadata(NAMED_TOPOLOGIES, pausedTopologies, ProcessingMode.AT_LEAST_ONCE);

        final Task mockTask1 = createMockTask(TOPOLOGY1);
        final Task mockTask2 = createMockTask(TOPOLOGY2);

        assertTrue(metadata.canProcessTask(mockTask1, TIME_ZERO));
        assertTrue(metadata.canProcessTask(mockTask2, TIME_ZERO));

        metadata.registerTaskError(mockTask1, new Throwable("Error"), TIME_ZERO);
        assertFalse(metadata.canProcessTask(mockTask1, CONSTANT_BACKOFF_MS - 1));
        assertTrue(metadata.canProcessTask(mockTask2, CONSTANT_BACKOFF_MS - 1));

        assertFalse(metadata.canProcessTask(mockTask1, CONSTANT_BACKOFF_MS));
        assertTrue(metadata.canProcessTask(mockTask2, CONSTANT_BACKOFF_MS));

        assertTrue(metadata.canProcessTask(mockTask1, CONSTANT_BACKOFF_MS + 1));
        assertTrue(metadata.canProcessTask(mockTask2, CONSTANT_BACKOFF_MS + 1));
    }

    private static Task createMockTask(final String topologyName) {
        final Task mockTask = mock(Task.class);
        final TaskId taskId = new TaskId(0, 0, topologyName);
        when(mockTask.id()).thenReturn(taskId);
        return mockTask;
    }
}
