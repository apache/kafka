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
package org.apache.kafka.streams.processor.internals.tasks;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultTaskExecutorTest {

    private final static long VERIFICATION_TIMEOUT = 15000;

    private final Time time = new MockTime(1L);
    private final StreamTask task = mock(StreamTask.class);
    private final TaskManager taskManager = mock(TaskManager.class);

    private final DefaultTaskExecutor taskExecutor = new DefaultTaskExecutor(taskManager, "TaskExecutor", time);

    @BeforeEach
    public void setUp() {
        // only assign a task for the first time
        when(taskManager.assignNextTask(taskExecutor)).thenReturn(task).thenReturn(null);
        when(task.isProcessable(anyLong())).thenReturn(true);
        when(task.process(anyLong())).thenReturn(true);
        when(task.prepareCommit()).thenReturn(Collections.emptyMap());
    }

    @AfterEach
    public void tearDown() {
        taskExecutor.shutdown(Duration.ofMinutes(1));
    }

    @Test
    public void shouldShutdownTaskExecutor() {
        assertNull(taskExecutor.currentTask(), "Have task assigned before startup");

        taskExecutor.start();

        verify(taskManager, timeout(VERIFICATION_TIMEOUT)).assignNextTask(taskExecutor);

        taskExecutor.shutdown(Duration.ofMinutes(1));

        verify(task).prepareCommit();
        verify(taskManager).unassignTask(task, taskExecutor);

        assertNull(taskExecutor.currentTask(), "Have task assigned after shutdown");
    }

    @Test
    public void shouldUnassignTaskWhenNotProcessable() {
        when(task.isProcessable(anyLong())).thenReturn(false);

        taskExecutor.start();

        verify(taskManager, timeout(VERIFICATION_TIMEOUT)).unassignTask(task, taskExecutor);
        verify(task).prepareCommit();
        assertNull(taskExecutor.currentTask());
    }

    @Test
    public void shouldUnassignTaskWhenRequired() throws Exception {
        taskExecutor.start();

        verify(taskManager, timeout(VERIFICATION_TIMEOUT)).assignNextTask(taskExecutor);
        assertNotNull(taskExecutor.currentTask());

        final KafkaFuture<StreamTask> future = taskExecutor.unassign();

        verify(taskManager, timeout(VERIFICATION_TIMEOUT)).unassignTask(task, taskExecutor);
        verify(task).prepareCommit();
        assertNull(taskExecutor.currentTask());

        assertTrue(future.isDone(), "Unassign is not completed");
        assertEquals(task, future.get(), "Unexpected task was unassigned");
    }
}
