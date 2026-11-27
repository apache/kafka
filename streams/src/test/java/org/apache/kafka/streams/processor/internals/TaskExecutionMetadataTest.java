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

import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode;
import org.apache.kafka.streams.processor.TaskId;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.streams.processor.internals.TopologyMetadata.UNNAMED_TOPOLOGY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TaskExecutionMetadataTest {
    static final int TIME_ZERO = 0;
    static final long NOT_READY_LOG_INTERVAL_MS = 120_000L;

    @Test
    public void testCanProcess() {
        final Set<String> pausedTopologies = new HashSet<>();

        final TaskExecutionMetadata metadata = new TaskExecutionMetadata(pausedTopologies, ProcessingMode.AT_LEAST_ONCE);

        final Task mockTask = mock(Task.class);
        when(mockTask.id()).thenReturn(new TaskId(0, 0));

        assertTrue(metadata.canProcessTask(mockTask, TIME_ZERO));
        pausedTopologies.add(UNNAMED_TOPOLOGY);
        assertFalse(metadata.canProcessTask(mockTask, TIME_ZERO));
        pausedTopologies.remove(UNNAMED_TOPOLOGY);
        assertTrue(metadata.canProcessTask(mockTask, TIME_ZERO));
    }

    @Test
    public void shouldNotFormatLogMessageUntilItIsLogged() {
        // KAFKA-21372: canProcessTask runs for every task on every loop iteration
        final Set<String> pausedTopologies = new HashSet<>();
        final TaskExecutionMetadata metadata = new TaskExecutionMetadata(pausedTopologies, ProcessingMode.AT_LEAST_ONCE);
        final String taskName = new TaskId(0, 0).toString();
        final AtomicInteger formatted = new AtomicInteger();
        final TaskId taskId = spy(new TaskId(0, 0));
        doAnswer(invocation -> {
            formatted.incrementAndGet();
            return invocation.callRealMethod();
        }).when(taskId).toString();
        final Task mockTask = mock(Task.class);
        when(mockTask.id()).thenReturn(taskId);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(TaskExecutionMetadata.class)) {
            assertTrue(metadata.canProcessTask(mockTask, TIME_ZERO));
            pausedTopologies.add(UNNAMED_TOPOLOGY);
            assertFalse(metadata.canProcessTask(mockTask, TIME_ZERO));
            assertFalse(metadata.canProcessTask(mockTask, NOT_READY_LOG_INTERVAL_MS - 1));
            assertEquals(0, formatted.get());

            assertFalse(metadata.canProcessTask(mockTask, NOT_READY_LOG_INTERVAL_MS));
            assertEquals(
                List.of("Task " + taskName + " is not ready to process: Task " + taskName + " can't be processed: topology is paused"),
                appender.getMessages(Level.INFO));
        }
    }
}
