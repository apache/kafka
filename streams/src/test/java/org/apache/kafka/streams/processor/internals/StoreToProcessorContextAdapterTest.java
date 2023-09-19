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

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.To;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertThrows;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Duration;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class StoreToProcessorContextAdapterTest {
    @Mock
    private StateStoreContext delegate;
    private ProcessorContext context;
    @Mock
    private Punctuator punctuator;

    @Before
    public void setUp() {
        context = StoreToProcessorContextAdapter.adapt(delegate);
    }

    @Test
    public void shouldThrowOnCurrentSystemTime() {
        assertThrows(UnsupportedOperationException.class, () -> context.currentSystemTimeMs());
    }

    @Test
    public void shouldThrowOnCurrentStreamTime() {
        assertThrows(UnsupportedOperationException.class, () -> context.currentStreamTimeMs());
    }

    @Test
    public void shouldThrowOnGetStateStore() {
        assertThrows(UnsupportedOperationException.class, () -> context.getStateStore("store"));
    }

    @Test
    public void shouldThrowOnScheduleWithDuration() {
        assertThrows(UnsupportedOperationException.class, () -> context.schedule(Duration.ZERO, PunctuationType.WALL_CLOCK_TIME, punctuator));
    }

    @Test
    public void shouldThrowOnForward() {
        assertThrows(UnsupportedOperationException.class, () -> context.forward("key", "value"));
    }

    @Test
    public void shouldThrowOnForwardWithTo() {
        assertThrows(UnsupportedOperationException.class, () -> context.forward("key", "value", To.all()));
    }

    @Test
    public void shouldThrowOnCommit() {
        assertThrows(UnsupportedOperationException.class, () -> context.commit());
    }

    @Test
    public void shouldThrowOnTopic() {
        assertThrows(UnsupportedOperationException.class, () -> context.topic());
    }

    @Test
    public void shouldThrowOnPartition() {
        assertThrows(UnsupportedOperationException.class, () -> context.partition());
    }

    @Test
    public void shouldThrowOnOffset() {
        assertThrows(UnsupportedOperationException.class, () -> context.offset());
    }

    @Test
    public void shouldThrowOnHeaders() {
        assertThrows(UnsupportedOperationException.class, () -> context.headers());
    }

    @Test
    public void shouldThrowOnTimestamp() {
        assertThrows(UnsupportedOperationException.class, () -> context.timestamp());
    }
}
