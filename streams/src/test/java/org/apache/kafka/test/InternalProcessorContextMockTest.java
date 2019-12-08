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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.junit.Test;

import java.io.File;
import java.time.Duration;
import java.util.List;

import static org.apache.kafka.streams.processor.MockProcessorContext.CapturedPunctuator;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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

    @Test
    public void shouldReturnDefaultKeySerde() {
        final ProcessorContext processorContext = createProcessorContext();
        final InternalProcessorContext mock = defaultMock(processorContext);

        final Serde<?> keySerde = mock.keySerde();

        assertThat(keySerde, samePropertyValuesAs(processorContext.keySerde()));
    }

    @Test
    public void shouldReturnDefaultValueSerde() {
        final ProcessorContext processorContext = createProcessorContext();
        final InternalProcessorContext mock = defaultMock(processorContext);

        final Serde<?> valueSerde = mock.valueSerde();

        assertThat(valueSerde, samePropertyValuesAs(processorContext.valueSerde()));
    }

    @Test
    public void shouldReturnDefaultStateDir() {
        final ProcessorContext processorContext = createProcessorContext();
        final InternalProcessorContext mock = defaultMock(processorContext);

        final File stateDir = mock.stateDir();

        assertEquals(processorContext.stateDir(), stateDir);
    }

    @Test
    public void shouldReturnDefaultMetrics() {
        final ProcessorContext processorContext = createProcessorContext();
        final InternalProcessorContext mock = defaultMock(processorContext);

        final StreamsMetricsImpl metrics =  mock.metrics();

        assertEquals(processorContext.metrics(), metrics);
    }

    @Test
    public void shouldRegisterAndReturnStore() {
        final InternalProcessorContext mock = defaultMock();
        final String storeName = "store_name";
        final StateStore stateStore = new MockKeyValueStore(storeName, false);
        final StateRestoreCallback stateRestoreCallback = new MockRestoreCallback();

        mock.register(stateStore, stateRestoreCallback);
        final StateStore store = mock.getStateStore(storeName);

        assertEquals(stateStore, store);
    }

    @Test
    public void shouldCapturePunctuatorOnSchedule() {
        final ProcessorContext processorContext = createProcessorContext();
        final InternalProcessorContext mock = defaultMock(processorContext);
        final Duration interval = Duration.ofMillis(1);
        final PunctuationType type = PunctuationType.WALL_CLOCK_TIME;
        final Punctuator punctuator = timestamp -> { };

        final int size = 2;
        for ( int i = 0; i < size; i++) {
            final Cancellable cancellable = mock.schedule(interval, type, punctuator);
            assertNotNull(cancellable);
        }

        final List<CapturedPunctuator> punctuatorList = punctuatorList(processorContext);
        assertEquals(size, punctuatorList.size());
        capturedPunctuatorListElementsEqualToDurationTypeAndPunctuator(punctuatorList, interval, type, punctuator);
    }

    private static void capturedPunctuatorListElementsEqualToDurationTypeAndPunctuator(
            final List<CapturedPunctuator> punctuatorList,
            final Duration interval,
            final PunctuationType type,
            final Punctuator punctuator) {
        for ( CapturedPunctuator capturedPunctuator : punctuatorList ) {
            assertEquals(interval.toMillis(), capturedPunctuator.getIntervalMs());
            assertEquals(type, capturedPunctuator.getType());
            assertEquals(punctuator, capturedPunctuator.getPunctuator());
        }
    }

    private static ProcessorContext createProcessorContext() {
        return new MockProcessorContext();
    }

    private static List<CapturedPunctuator> punctuatorList(final ProcessorContext processorContext) {
        return ((MockProcessorContext) processorContext).scheduledPunctuators();
    }

    private static InternalProcessorContext defaultMock() {
        return defaultMock(createProcessorContext());
    }

    private static InternalProcessorContext defaultMock(final ProcessorContext processorContext) {
        return new InternalProcessorContextMock.Builder(processorContext).build();
    }
}