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
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.easymock.Capture;
import org.easymock.EasyMock;

import java.io.File;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

public class InternalProcessorContextMockFactory {

    public static InternalProcessorContextMock getInstance() {
        return getInstance(InternalProcessorContextMockFactory::getProcessorContext);
    }

    public static InternalProcessorContextMock getInstance(final Properties config) {
        return getInstance(config, new TaskId(0, 0));
    }

    public static InternalProcessorContextMock getInstance(final Properties config,
                                                           final TaskId taskId) {
        final File stateDir = TestUtils.tempDirectory();
        return getInstance(() -> getProcessorContext(config, taskId, stateDir));
    }

    private static MockProcessorContext getProcessorContext() {
        return new MockProcessorContext();
    }

    private static MockProcessorContext getProcessorContext(
            final Properties config) {
        return new MockProcessorContext(config);
    }

    private static MockProcessorContext getProcessorContext(
            final Properties config,
            final TaskId taskId,
            final File stateDir) {
        return new MockProcessorContext(config, taskId, stateDir);
    }

    private static InternalProcessorContextMock getInstance(
            final Supplier<MockProcessorContext> mockProcessorContextSupplier) {
        final MockProcessorContext processorContext =
                mockProcessorContextSupplier.get();
        processorContext
                // Avoid IllegalStateException
                .setRecordMetadata("", 0, 0L, null, 0L);

        return EasyMockBuilder.newInstance()
                .initialize()
                .uninitialize()
                .stateStoreCallback()
                .setRecordCollector()
                .recordCollector()
                .setCurrentNode()
                // Delegate to a ProcessorContext
                .applicationId(processorContext)
                .taskId(processorContext)
                .appConfigs(processorContext)
                .appConfigsWithPrefix(processorContext)
                .keySerde(processorContext)
                .valueSerde(processorContext)
                .stateDir(processorContext)
                .metrics(processorContext)
                .topic(processorContext)
                .partition(processorContext)
                .offset(processorContext)
                .headers(processorContext)
                .timestamp(processorContext)
                .getStateStore(processorContext)
                .schedule(processorContext)
                .forwardKeyValue(processorContext)
                .forwardKeyValueTo(processorContext)
                .commit(processorContext)
                .register(processorContext)
                .recordContext(processorContext)
                // Delegate to a MockProcessorContext
                .forwarded(processorContext)
                .setRecordContext(processorContext)
                .build();
    }

    private static class EasyMockBuilder {

        private final InternalProcessorContextMock internalProcessorContext;
        private final Capture<Object> keyCapture = Capture.newInstance();
        private final Capture<Object> valueCapture = Capture.newInstance();
        private final Capture<To> toCapture = Capture.newInstance();
        private final Capture<Duration> intervalDurationCapture = Capture.newInstance();
        private final Capture<PunctuationType> punctuationTypeCapture = Capture.newInstance();
        private final Capture<Punctuator> punctuatorCapture = Capture.newInstance();
        private final Capture<String> stateStoreNameCapture = Capture.newInstance();
        private final Capture<String> prefixCapture = Capture.newInstance();
        private final Capture<Boolean> initializeCapture = Capture.newInstance();
        private final Capture<String> stateRestoreCallbackNameCapture = Capture.newInstance();
        private final Map<String, StateRestoreCallback> restoreCallbacks = new LinkedHashMap<>();
        private final Capture<StateStore> stateStoreCapture = Capture.newInstance();
        private final Capture<StateRestoreCallback> stateRestoreCallbackCapture = Capture.newInstance();
        private final Capture<RecordCollector> recordCollectorCapture = Capture.newInstance();
        private final Capture<ProcessorRecordContext> processorRecordContextCapture = Capture.newInstance();
        private final Capture<ProcessorNode> processorNodeCapture = Capture.newInstance();

        private EasyMockBuilder() {
            internalProcessorContext = EasyMock.niceMock(InternalProcessorContextMock.class);
        }

        static EasyMockBuilder newInstance() {
            return new EasyMockBuilder();
        }

        EasyMockBuilder setCurrentNode() {
            internalProcessorContext
                    .setCurrentNode(EasyMock.capture(processorNodeCapture));
            EasyMock.expectLastCall().anyTimes();
            EasyMock.expect(internalProcessorContext.currentNode())
                    .andAnswer(processorNodeCapture::getValue)
                    .anyTimes();
            return this;
        }

        EasyMockBuilder setRecordContext(final MockProcessorContext processorContext) {
            internalProcessorContext.setRecordContext(EasyMock.capture(processorRecordContextCapture));
            EasyMock.expectLastCall()
                    .andAnswer(() -> {
                        final ProcessorRecordContext recordContext =
                                processorRecordContextCapture.getValue();
                        processorContext.setRecordMetadata(
                                recordContext.topic(),
                                recordContext.partition(),
                                recordContext.offset(),
                                recordContext.headers(),
                                recordContext.timestamp()
                        );
                        return null;
                    })
                    .anyTimes();
            return this;
        }

        EasyMockBuilder recordContext(final ProcessorContext processorContext) {
            EasyMock.expect(internalProcessorContext.recordContext())
                    .andAnswer(() -> new ProcessorRecordContext(
                            processorContext.timestamp(),
                            processorContext.offset(),
                            processorContext.partition(),
                            processorContext.topic(),
                            processorContext.headers()
                    ))
                    .anyTimes();
            return this;
        }

        EasyMockBuilder setRecordCollector() {
            internalProcessorContext.setRecordCollector(EasyMock.capture(recordCollectorCapture));
            EasyMock.expectLastCall()
                    .anyTimes();
            return this;
        }

        EasyMockBuilder recordCollector() {
            EasyMock.expect(internalProcessorContext.recordCollector())
                    .andAnswer(() -> {
                        if (recordCollectorCapture.hasCaptured()) {
                            return recordCollectorCapture.getValue();
                        }
                        return new MockRecordCollector();
                    })
                    .anyTimes();
            return this;
        }

        EasyMockBuilder stateStoreCallback() {
            EasyMock.expect(internalProcessorContext
                    .stateRestoreCallback(
                            EasyMock.capture(stateRestoreCallbackNameCapture)))
                    .andAnswer(() -> restoreCallbacks
                            .get(stateRestoreCallbackNameCapture.getValue()))
                    .anyTimes();
            return this;
        }

        EasyMockBuilder initialize() {
            internalProcessorContext.initialize();
            EasyMock.expectLastCall()
                    .andAnswer(() -> {
                        initializeCapture.setValue(true);
                        return null;
                    })
                    .anyTimes();
            return this;
        }

        EasyMockBuilder uninitialize() {
            internalProcessorContext.uninitialize();
            EasyMock.expectLastCall()
                    .andAnswer(() -> {
                        initializeCapture.setValue(false);
                        return null;
                    })
                    .anyTimes();
            return this;
        }

        EasyMockBuilder applicationId(final ProcessorContext processorContext) {
            EasyMock.expect(internalProcessorContext.applicationId())
                    .andAnswer(processorContext::applicationId)
                    .anyTimes();
            return this;
        }

        EasyMockBuilder taskId(final ProcessorContext processorContext) {
            EasyMock.expect(internalProcessorContext.taskId())
                    .andAnswer(processorContext::taskId)
                    .anyTimes();
            return this;
        }

        EasyMockBuilder appConfigs(final ProcessorContext processorContext) {
            EasyMock.expect(internalProcessorContext.appConfigs())
                    .andAnswer(processorContext::appConfigs)
                    .anyTimes();
            return this;
        }

        EasyMockBuilder appConfigsWithPrefix(
                final ProcessorContext processorContext) {
            EasyMock.expect(internalProcessorContext
                    .appConfigsWithPrefix(EasyMock.capture(prefixCapture)))
                    .andAnswer(() -> processorContext
                            .appConfigsWithPrefix(prefixCapture.getValue()));
            return this;
        }

        EasyMockBuilder keySerde(final ProcessorContext processorContext) {
            EasyMock.expect((Serde) internalProcessorContext.keySerde())
                    .andAnswer(processorContext::keySerde)
                    .anyTimes();
            return this;
        }

        EasyMockBuilder valueSerde(final ProcessorContext processorContext) {
            EasyMock.expect((Serde) internalProcessorContext.valueSerde())
                    .andAnswer(processorContext::valueSerde)
                    .anyTimes();
            return this;
        }

        EasyMockBuilder stateDir(final ProcessorContext processorContext) {
            EasyMock.expect(internalProcessorContext.stateDir())
                    .andAnswer(processorContext::stateDir)
                    .anyTimes();
            return this;
        }

        EasyMockBuilder metrics(final ProcessorContext processorContext) {
            EasyMock.expect(internalProcessorContext.metrics())
                    // FIXME KAFKA-8630
                    .andAnswer(() -> (StreamsMetricsImpl) processorContext
                            .metrics())
                    .anyTimes();
            return this;
        }

        EasyMockBuilder topic(final ProcessorContext processorContext) {
            EasyMock.expect(internalProcessorContext.topic())
                    .andAnswer(processorContext::topic)
                    .anyTimes();
            return this;
        }

        EasyMockBuilder partition(final ProcessorContext processorContext) {
            EasyMock.expect(internalProcessorContext.partition())
                    .andAnswer(processorContext::partition)
                    .anyTimes();
            return this;
        }

        EasyMockBuilder offset(final ProcessorContext processorContext) {
            EasyMock.expect(internalProcessorContext.offset())
                    .andAnswer(processorContext::offset)
                    .anyTimes();
            return this;
        }

        EasyMockBuilder headers(final ProcessorContext processorContext) {
            EasyMock.expect(internalProcessorContext.headers())
                    .andAnswer(processorContext::headers)
                    .anyTimes();
            return this;
        }

        EasyMockBuilder timestamp(final ProcessorContext processorContext) {
            EasyMock.expect(internalProcessorContext.timestamp())
                    .andAnswer(processorContext::timestamp)
                    .anyTimes();
            return this;
        }

        EasyMockBuilder getStateStore(final ProcessorContext processorContext) {
            EasyMock.expect(internalProcessorContext
                    .getStateStore(EasyMock.capture(stateStoreNameCapture)))
                    .andAnswer(() -> processorContext
                            .getStateStore(stateStoreNameCapture.getValue()))
                    .anyTimes();
            return this;
        }

        EasyMockBuilder schedule(final ProcessorContext processorContext) {
            EasyMock.expect(internalProcessorContext.schedule(
                    EasyMock.capture(intervalDurationCapture),
                    EasyMock.capture(punctuationTypeCapture),
                    EasyMock.capture(punctuatorCapture)
            ))
                    .andAnswer(() -> processorContext.schedule(
                            intervalDurationCapture.getValue(),
                            punctuationTypeCapture.getValue(),
                            punctuatorCapture.getValue()))
                    .anyTimes();
            return this;
        }

        EasyMockBuilder forwardKeyValue(
                final ProcessorContext processorContext) {
            internalProcessorContext.forward(
                    EasyMock.capture(keyCapture),
                    EasyMock.capture(valueCapture));
            EasyMock.expectLastCall()
                    .andAnswer(() -> {
                        processorContext.forward(keyCapture.getValue(),
                                valueCapture.getValue());
                        return null;
                    })
                    .anyTimes();
            return this;
        }

        EasyMockBuilder forwardKeyValueTo(
                final ProcessorContext processorContext) {
            internalProcessorContext.forward(
                    EasyMock.capture(keyCapture),
                    EasyMock.capture(valueCapture),
                    EasyMock.capture(toCapture));
            EasyMock.expectLastCall()
                    .andAnswer(() -> {
                        processorContext.forward(
                                keyCapture.getValue(),
                                valueCapture.getValue(),
                                toCapture.getValue());
                        return null;
                    })
                    .anyTimes();
            return this;
        }

        EasyMockBuilder forwarded(final MockProcessorContext processorContext) {
            EasyMock.expect(internalProcessorContext.forwarded())
                    .andAnswer(processorContext::forwarded)
                    .anyTimes();
            return this;
        }

        EasyMockBuilder commit(final ProcessorContext processorContext) {
            internalProcessorContext.commit();
            EasyMock.expectLastCall()
                    .andAnswer(() -> {
                        processorContext.commit();
                        return null;
                    })
                    .anyTimes();
            return this;
        }

        EasyMockBuilder register(final ProcessorContext processorContext) {
            internalProcessorContext.register(
                    EasyMock.capture(stateStoreCapture),
                    EasyMock.capture(stateRestoreCallbackCapture));

            EasyMock.expectLastCall()
                    .andAnswer(() -> {
                        restoreCallbacks.put(
                                stateStoreCapture.getValue().name(),
                                stateRestoreCallbackCapture.getValue());
                        processorContext.register(
                                stateStoreCapture.getValue(),
                                stateRestoreCallbackCapture.getValue());
                        return null;
                    })
                    .anyTimes();
            return this;
        }

        InternalProcessorContextMock build() {
            EasyMock.replay(internalProcessorContext);
            return internalProcessorContext;
        }
    }
}
