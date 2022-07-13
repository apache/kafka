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
import org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class TaskExecutorTest {
    @Test
    public void testPunctuateWithPause() {
        final Tasks tasks = mock(Tasks.class);
        final TaskExecutionMetadata metadata = mock(TaskExecutionMetadata.class);

        final TaskExecutor taskExecutor =
            new TaskExecutor(tasks, metadata, ProcessingMode.AT_LEAST_ONCE, false, new LogContext());

        taskExecutor.punctuate();
        verify(tasks).notPausedActiveTasks();
        verify(tasks, never()).notPausedTasks();
    }
}
