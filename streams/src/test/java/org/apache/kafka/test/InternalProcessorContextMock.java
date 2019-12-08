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
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.easymock.Capture;
import org.easymock.EasyMock;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class InternalProcessorContextMock {

    public static Builder builder() {
        return new Builder(processorContext());
    }

    public static class Builder {

        private InternalProcessorContext mock;

        private String applicationId;
        private TaskId taskId;
        private Serde<?> keySerde;
        private Serde<?> valueSerde;
        private File stateDir;
        private StreamsMetricsImpl metrics;
        private final Map<String, StateStore> stateStoreMap;
        private final Map<String, StateRestoreCallback> stateRestoreCallbackMap;

        Builder(final ProcessorContext processorContext) {
            mock = EasyMock.mock(InternalProcessorContext.class);
            stateStoreMap = new HashMap<>();
            stateRestoreCallbackMap = new HashMap<>();

            applicationId = processorContext.applicationId();
            taskId = processorContext.taskId();
            keySerde = processorContext.keySerde();
            valueSerde = processorContext.valueSerde();
            stateDir = processorContext.stateDir();
            metrics = (StreamsMetricsImpl) processorContext.metrics();
        }

        public InternalProcessorContext build() {
            applicationId();
            taskId();
            keySerde();
            valueSerde();
            stateDir();
            metrics();
            register();
            getStateStore();

            EasyMock.replay(mock);
            return mock;
        }

        private void getStateStore() {
            final Capture<String> stateStoreNameCapture = Capture.newInstance();
            EasyMock.expect(mock.getStateStore(EasyMock.capture(stateStoreNameCapture)))
                    .andAnswer(() -> stateStoreMap.get(stateStoreNameCapture.getValue()));
        }

        private void register() {
            final Capture<StateStore> storeCapture = Capture.newInstance();
            final Capture<StateRestoreCallback> restoreCallbackCapture = Capture.newInstance();

            mock.register(EasyMock.capture(storeCapture), EasyMock.capture(restoreCallbackCapture));

            EasyMock.expectLastCall()
                    .andAnswer(() -> {
                        stateStoreMap.put(storeCapture.getValue().name(), storeCapture.getValue());
                        stateRestoreCallbackMap.put(storeCapture.getValue().name(), restoreCallbackCapture.getValue());
                        return null;
                    });
        }

        private void metrics() {
            EasyMock.expect(mock.metrics()).andReturn(metrics);
        }

        private void stateDir() {
            EasyMock.expect(mock.stateDir()).andReturn(stateDir);
        }

        private void valueSerde() {
            EasyMock.expect((Serde) mock.valueSerde()).andReturn(valueSerde);
        }

        private void keySerde() {
            EasyMock.expect((Serde) mock.keySerde()).andReturn(keySerde);
        }

        private void taskId() {
            EasyMock.expect(mock.taskId()).andReturn(taskId);
        }

        private void applicationId() {
            EasyMock.expect(mock.applicationId()).andReturn(applicationId);
        }
    }

    private static ProcessorContext processorContext() {
        return new MockProcessorContext();
    }
}
