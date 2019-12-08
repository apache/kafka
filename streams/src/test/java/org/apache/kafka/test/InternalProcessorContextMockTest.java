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
package org.apache.kafka.test;

import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class InternalProcessorContextMockTest {

    @Test
    public void shouldReturnDefaultApplicationId() {
        final ProcessorContext processorContext = createProcessorContext();
        final InternalProcessorContext mock = defaultMock(processorContext);

        final String applicationId = mock.applicationId();

        assertEquals(processorContext.applicationId(), applicationId);
    }

    @Test
    public void shouldReturnDefaultTaskId() {
        final ProcessorContext processorContext = createProcessorContext();
        final InternalProcessorContext mock = defaultMock(processorContext);

        final TaskId taskId = mock.taskId();

        assertEquals(processorContext.taskId(), taskId);
    }


    private static ProcessorContext createProcessorContext() {
        return new MockProcessorContext();
    }

    private static InternalProcessorContext defaultMock(final ProcessorContext processorContext) {
        return new InternalProcessorContextMock.Builder(processorContext).build();
    }
}