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
import org.apache.kafka.common.utils.LogContext;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode.EXACTLY_ONCE_V2;
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
        verify(tasks).activeTasks();
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
}