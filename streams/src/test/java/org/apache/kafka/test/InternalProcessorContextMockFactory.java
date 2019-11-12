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
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
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

    private static InternalProcessorContextMock getInstance(final Supplier<MockProcessorContext> mockProcessorContextSupplier) {
        final InternalProcessorContextMock internalProcessorContext =
                EasyMock.niceMock(InternalProcessorContextMock.class);
        final MockProcessorContext processorContext = mockProcessorContextSupplier.get();
        processorContext
                // Avoid IllegalStateException
                .setRecordMetadata("", 0, 0L, null, 0L);

        delegateProcessorContextCallTo(internalProcessorContext, processorContext);

        Capture<ProcessorNode> processorNodeCapture = Capture.newInstance();
        internalProcessorContext.setCurrentNode(EasyMock.capture(processorNodeCapture));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(internalProcessorContext.currentNode())
                .andAnswer(processorNodeCapture::getValue)
                .anyTimes();

        final Capture<ProcessorRecordContext> processorRecordContextCapture = Capture.newInstance();
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
        EasyMock.expect(internalProcessorContext.recordContext())
                .andAnswer(() -> new ProcessorRecordContext(
                        processorContext.timestamp(),
                        processorContext.offset(),
                        processorContext.partition(),
                        processorContext.topic(),
                        processorContext.headers()
                ))
                .anyTimes();

        final Capture<RecordCollector> recordCollectorCapture = Capture.newInstance();
        internalProcessorContext.setRecordCollector(EasyMock.capture(recordCollectorCapture));
        EasyMock.expectLastCall()
                .anyTimes();

        EasyMock.expect(internalProcessorContext.recordCollector())
                .andAnswer(() -> {
                    if (recordCollectorCapture.hasCaptured()) {
                        return recordCollectorCapture.getValue();
                    }
                    return new MockRecordCollector();
                })
                .anyTimes();

        final Capture<StateStore> stateStoreCapture = Capture.newInstance();
        final Capture<StateRestoreCallback> stateRestoreCallbackCapture = Capture.newInstance();
        final Map<String, StateRestoreCallback> restoreCallbacks = new LinkedHashMap<>();
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

        final Capture<String> stateRestoreCallbackNameCapture = Capture.newInstance();
        EasyMock.expect(internalProcessorContext
                .stateRestoreCallback(EasyMock.capture(stateRestoreCallbackNameCapture)))
                .andAnswer(() -> restoreCallbacks.get(stateRestoreCallbackNameCapture.getValue()))
                .anyTimes();

        EasyMock.expect(internalProcessorContext.forwarded())
                .andAnswer(processorContext::forwarded)
                .anyTimes();

        EasyMock.replay(internalProcessorContext);
        return internalProcessorContext;
    }

    private static void delegateProcessorContextCallTo(final InternalProcessorContext internalProcessorContext,
                                                       final ProcessorContext processorContext) {
        EasyMock.expect(internalProcessorContext.applicationId())
                .andAnswer(processorContext::applicationId)
                .anyTimes();
        EasyMock.expect(internalProcessorContext.taskId())
                .andAnswer(processorContext::taskId)
                .anyTimes();
        EasyMock.expect(internalProcessorContext.appConfigs())
                .andAnswer(processorContext::appConfigs)
                .anyTimes();
        final Capture<String> prefixCapture = Capture.newInstance();
        EasyMock.expect(internalProcessorContext.appConfigsWithPrefix(EasyMock.capture(prefixCapture)))
                .andAnswer(() -> processorContext.appConfigsWithPrefix(prefixCapture.getValue()));
        EasyMock.expect((Serde) internalProcessorContext.keySerde())
                .andAnswer(processorContext::keySerde)
                .anyTimes();
        EasyMock.expect((Serde) internalProcessorContext.valueSerde())
                .andAnswer(processorContext::valueSerde)
                .anyTimes();
        EasyMock.expect(internalProcessorContext.stateDir())
                .andAnswer(processorContext::stateDir)
                .anyTimes();
        EasyMock.expect(internalProcessorContext.metrics())
                // FIXME KAFKA-8630
                .andAnswer(() -> (StreamsMetricsImpl) processorContext.metrics())
                .anyTimes();
        EasyMock.expect(internalProcessorContext.topic())
                .andAnswer(processorContext::topic)
                .anyTimes();
        EasyMock.expect(internalProcessorContext.partition())
                .andAnswer(processorContext::partition)
                .anyTimes();
        EasyMock.expect(internalProcessorContext.offset())
                .andAnswer(processorContext::offset)
                .anyTimes();
        EasyMock.expect(internalProcessorContext.headers())
                .andAnswer(processorContext::headers)
                .anyTimes();
        EasyMock.expect(internalProcessorContext.timestamp())
                .andAnswer(processorContext::timestamp)
                .anyTimes();
        final Capture<String> stateStoreNameCapture = Capture.newInstance();
        EasyMock.expect(internalProcessorContext.getStateStore(EasyMock.capture(stateStoreNameCapture)))
                .andAnswer(() -> processorContext.getStateStore(stateStoreNameCapture.getValue()))
                .anyTimes();
        final Capture<PunctuationType> punctuationTypeCapture = Capture.newInstance();
//        final Capture<Long> intervalMsCapture = Capture.newInstance();
        final Capture<Punctuator> punctuatorCapture = Capture.newInstance();
//        EasyMock.expect(internalProcessorContext.schedule(
//                EasyMock.capture(intervalMsCapture),
//                EasyMock.capture(punctuationTypeCapture),
//                EasyMock.capture(punctuatorCapture)
//        ))
//                .andAnswer(() -> processorContext.schedule(
//                        intervalMsCapture.getValue(),
//                        punctuationTypeCapture.getValue(),
//                        punctuatorCapture.getValue()))
//                .anyTimes();
        final Capture<Duration> intervalDurationCapture = Capture.newInstance();
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
        final Capture<Object> keyCapture = Capture.newInstance();
        final Capture<Object> valueCapture = Capture.newInstance();
        internalProcessorContext.forward(
                EasyMock.capture(keyCapture),
                EasyMock.capture(valueCapture));
        EasyMock.expectLastCall()
                .andAnswer(() -> {
                    processorContext.forward(keyCapture.getValue(), valueCapture.getValue());
                    return null;
                })
                .anyTimes();
        final Capture<To> toCapture = Capture.newInstance();
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
        internalProcessorContext.commit();
        EasyMock.expectLastCall()
                .andAnswer(() -> {
                    processorContext.commit();
                    return null;
                })
                .anyTimes();
    }

    private static MockProcessorContext getProcessorContext() {
        return new MockProcessorContext();
    }

    private static MockProcessorContext getProcessorContext(final Properties config) {
        return new MockProcessorContext(config);
    }

    private static MockProcessorContext getProcessorContext(final Properties config,
                                                            final TaskId taskId,
                                                            final File stateDir) {
        return new MockProcessorContext(config, taskId, stateDir);
    }

}
