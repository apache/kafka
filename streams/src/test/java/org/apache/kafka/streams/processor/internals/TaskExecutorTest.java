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

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.errors.StreamsException;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode.EXACTLY_ONCE_V2;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TaskExecutorTest {
    @Test
    public void testPunctuateWithPause() {
        final Tasks tasks = mock(Tasks.class);
        final TaskManager taskManager = mock(TaskManager.class);
        final TaskExecutionMetadata metadata = mock(TaskExecutionMetadata.class);

        final TaskExecutor taskExecutor = new TaskExecutor(tasks, taskManager, metadata, new LogContext());

        taskExecutor.punctuate();
        verify(tasks).activeInitializedTasks();
    }

    @Test
    public void testCommitWithOpenTransactionButNoOffsetsEOSV2() {
        final Tasks tasks = mock(Tasks.class);
        final TaskManager taskManager = mock(TaskManager.class);
        final ConsumerGroupMetadata groupMetadata = mock(ConsumerGroupMetadata.class);
        when(taskManager.consumerGroupMetadata()).thenReturn(groupMetadata);

        final TaskExecutionMetadata metadata = mock(TaskExecutionMetadata.class);
        final StreamsProducer producer = mock(StreamsProducer.class);
        when(metadata.processingMode()).thenReturn(EXACTLY_ONCE_V2);
        when(taskManager.streamsProducer()).thenReturn(producer);
        when(producer.transactionInFlight()).thenReturn(true);

        final TaskExecutor taskExecutor = new TaskExecutor(tasks, taskManager, metadata, new LogContext());
        taskExecutor.commitOffsetsOrTransaction(Collections.emptyMap());

        verify(producer).commitTransaction(Collections.emptyMap(), groupMetadata);
    }

    @Test
    public void shouldFlushTerminalE2ELatencyAgainstTheBatchEndTime() {
        final Tasks tasks = mock(Tasks.class);
        final TaskManager taskManager = mock(TaskManager.class);
        final TaskExecutionMetadata metadata = mock(TaskExecutionMetadata.class);
        final StreamTask task = mock(StreamTask.class);
        final MockTime time = new MockTime(0L, 0L, 0L);

        when(tasks.activeInitializedTasks()).thenReturn(Collections.singleton(task));
        when(metadata.canProcessTask(eq(task), anyLong())).thenReturn(true);
        // every step of the batch takes 5ms, and the batch ends once process() returns false
        final AtomicInteger processCalls = new AtomicInteger();
        when(task.process(anyLong())).thenAnswer(invocation -> {
            time.sleep(5L);
            return processCalls.getAndIncrement() < 2;
        });

        final TaskExecutor taskExecutor = new TaskExecutor(tasks, taskManager, metadata, new LogContext());

        assertEquals(2, taskExecutor.process(10, time));
        // three calls to process() means the batch ends at 15, not at the time it began
        verify(task).maybeFlushTerminalE2ELatency(15L);
    }

    @Test
    public void shouldFlushTerminalE2ELatencyWhenProcessingThrows() {
        final Tasks tasks = mock(Tasks.class);
        final TaskManager taskManager = mock(TaskManager.class);
        final TaskExecutionMetadata metadata = mock(TaskExecutionMetadata.class);
        final StreamTask task = mock(StreamTask.class);
        final MockTime time = new MockTime(0L, 0L, 0L);

        when(tasks.activeInitializedTasks()).thenReturn(Collections.singleton(task));
        when(metadata.canProcessTask(eq(task), anyLong())).thenReturn(true);
        when(task.process(anyLong())).thenThrow(new RuntimeException("KABOOM!"));

        final TaskExecutor taskExecutor = new TaskExecutor(tasks, taskManager, metadata, new LogContext());

        assertThrows(StreamsException.class, () -> taskExecutor.process(10, time));
        verify(task).maybeFlushTerminalE2ELatency(anyLong());
    }
}
