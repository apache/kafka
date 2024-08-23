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

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.To;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class StoreToProcessorContextAdapterTest {
    @Mock
    private StateStoreContext delegate;
    private InternalProcessorContext internalProcessorContext;
    @Mock
    private Punctuator punctuator;

    @BeforeEach
    public void setUp() {
        this.internalProcessorContext = ProcessorContextUtils.asInternalProcessorContext(delegate);
    }

    @Test
    public void shouldThrowOnCurrentSystemTime() {
        assertThrows(UnsupportedOperationException.class, () -> internalProcessorContext.currentSystemTimeMs());
    }

    @Test
    public void shouldThrowOnCurrentStreamTime() {
        assertThrows(UnsupportedOperationException.class, () -> internalProcessorContext.currentStreamTimeMs());
    }

    @Test
    public void shouldThrowOnGetStateStore() {
        assertThrows(UnsupportedOperationException.class, () -> internalProcessorContext.getStateStore("store"));
    }

    @Test
    public void shouldThrowOnScheduleWithDuration() {
        assertThrows(UnsupportedOperationException.class, () -> internalProcessorContext.schedule(Duration.ZERO, PunctuationType.WALL_CLOCK_TIME, punctuator));
    }

    @Test
    public void shouldThrowOnForward() {
        assertThrows(UnsupportedOperationException.class, () -> internalProcessorContext.forward("key", "value"));
    }

    @Test
    public void shouldThrowOnForwardWithTo() {
        assertThrows(UnsupportedOperationException.class, () -> internalProcessorContext.forward("key", "value", To.all()));
    }

    @Test
    public void shouldThrowOnCommit() {
        assertThrows(UnsupportedOperationException.class, () -> internalProcessorContext.commit());
    }

    @Test
    public void shouldThrowOnTopic() {
        assertThrows(UnsupportedOperationException.class, () -> internalProcessorContext.topic());
    }

    @Test
    public void shouldThrowOnPartition() {
        assertThrows(UnsupportedOperationException.class, () -> internalProcessorContext.partition());
    }

    @Test
    public void shouldThrowOnOffset() {
        assertThrows(UnsupportedOperationException.class, () -> internalProcessorContext.offset());
    }

    @Test
    public void shouldThrowOnHeaders() {
        assertThrows(UnsupportedOperationException.class, () -> internalProcessorContext.headers());
    }

    @Test
    public void shouldThrowOnTimestamp() {
        assertThrows(UnsupportedOperationException.class, () -> internalProcessorContext.timestamp());
    }
}
