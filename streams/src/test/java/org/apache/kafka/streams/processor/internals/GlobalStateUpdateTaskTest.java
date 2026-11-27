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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class GlobalStateUpdateTaskTest {

    @Mock
    private ProcessorTopology topology;
    @Mock
    private InternalProcessorContext<Object, Object> processorContext;
    @Mock
    private GlobalStateManager stateMgr;
    @Mock
    private DeserializationExceptionHandler deserializationExceptionHandler;
    @Mock
    private ProcessingExceptionHandler processingExceptionHandler;

    @Test
    public void shouldSkipTopologyAndProcessorInitWhenBootstrapInterrupted() {
        when(stateMgr.initialize()).thenReturn(Optional.empty());

        final GlobalStateUpdateTask task = new GlobalStateUpdateTask(
            new LogContext("test"),
            topology,
            processorContext,
            stateMgr,
            deserializationExceptionHandler,
            processingExceptionHandler,
            new MockTime(),
            0L
        );

        final Map<TopicPartition, Long> offsets = task.initialize();

        verify(topology, never()).processors();
        verify(processorContext, never()).initialize();
        verify(stateMgr, never()).changelogOffsets();
        assertTrue(offsets.isEmpty());
    }
}
