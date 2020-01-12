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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.internals.QuietStreamsConfig;
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
import org.apache.kafka.streams.processor.internals.ToInternal;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecordingTrigger;
import org.easymock.Capture;

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;

public class InternalProcessorContextMock {

    public static Builder builder() {
        return new Builder();
    }

    @SuppressWarnings("rawtypes")
    public static class Builder {

        private InternalProcessorContext mock;
        private MockProcessorContext processorContext;

        private String applicationId;
        private TaskId taskId;
        private Serde<?> keySerde;
        private Serde<?> valueSerde;
        private File stateDir;
        private StreamsMetricsImpl metrics;
        private ProcessorRecordContext recordContext;
        private StreamsConfig config;
        private ProcessorNode processorNode;
        private ThreadCache cache;

        private final Map<String, StateStore> stateStoreMap;
        private final ToInternal toInternal;

        public Builder() {
            this(new MockProcessorContext());
        }

        public Builder(final MockProcessorContext processorContext) {
            mock = mock(InternalProcessorContext.class);
            this.processorContext = processorContext;

            stateStoreMap = new HashMap<>();
            toInternal = new ToInternal();
            setRecordContext(new ProcessorRecordContext(-1L, -1L, -1, null, new RecordHeaders()));
            appConfigs(null);

            applicationId = processorContext.applicationId();
            taskId = processorContext.taskId();
            keySerde = processorContext.keySerde();
            valueSerde = processorContext.valueSerde();
            stateDir = processorContext.stateDir();
            metrics = (StreamsMetricsImpl) processorContext.metrics();
        }


        public Builder cache(final ThreadCache cache) {
            this.cache = cache;
            return this;
        }

        public Builder appConfigs(Map<String, Object> config) {
            if (config == null) {
                config = new HashMap<>();
            }
            if (!config.containsKey(StreamsConfig.APPLICATION_ID_CONFIG)) {
                config.put(StreamsConfig.APPLICATION_ID_CONFIG, "");
            }
            if (!config.containsKey(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG)) {
                config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");
            }
            this.config = new QuietStreamsConfig(config);
            return this;
        }

        public Builder metrics(final StreamsMetricsImpl metrics) {
            this.metrics = metrics;
            return this;
        }

        public Builder stateDir(final File stateDir) {
            this.stateDir = stateDir;
            return this;
        }

        public Builder valueSerde(final Serde<?> valueSerde) {
            this.valueSerde = valueSerde;
            return this;
        }

        public Builder keySerde(final Serde<?> keySerde) {
            this.keySerde = keySerde;
            return this;
        }

        public Builder taskId(final TaskId taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder applicationId(final String applicationId) {
            this.applicationId = applicationId;
            return this;
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
            schedule();
            forwardKeyValue();
            forwardKeyValueTo();
            commit();
            topic();
            partition();
            setRecordContext();
            offset();
            headers();
            timestamp();
            appConfigs();
            appConfigsWithPrefix();
            recordContext();
            setCurrentNode();
            currentNode();
            getCache();
            initialize();
            uninitialize();
            this.metrics.setRocksDBMetricsRecordingTrigger(new RocksDBMetricsRecordingTrigger());

            replay(mock);
            return mock;
        }

        private void uninitialize() {
            mock.uninitialize();
            expectLastCall().anyTimes();
        }

        private void initialize() {
            mock.initialize();
            expectLastCall().anyTimes();
        }

        private void getCache() {
            expect(mock.getCache()).andAnswer(() -> cache).anyTimes();
        }

        private void currentNode() {
            expect(mock.currentNode()).andAnswer(() -> processorNode).anyTimes();
        }

        private void setCurrentNode() {
            Capture<ProcessorNode> nodeCapture = Capture.newInstance();
            mock.setCurrentNode(capture(nodeCapture));
            expectLastCall().andAnswer(() -> {
                setCurrentNode(nodeCapture.getValue());
                return null;
            }).anyTimes();
        }

        private void setCurrentNode(ProcessorNode processorNode) {
            this.processorNode = processorNode;
        }

        private void setRecordContext() {
            final Capture<ProcessorRecordContext> recordContextCapture = Capture.newInstance();
            mock.setRecordContext(capture(recordContextCapture));
            expectLastCall().andAnswer(() -> {
                setRecordContext(recordContextCapture.getValue());
                return null;
            }).anyTimes();
        }

        private void setRecordContext(final ProcessorRecordContext recordContext) {
            this.recordContext = recordContext;
            processorContext.setRecordMetadata(
                    recordContext.topic(),
                    recordContext.partition(),
                    recordContext.offset(),
                    recordContext.headers(),
                    recordContext.timestamp()
            );
        }

        private void recordContext() {
            expect(mock.recordContext()).andAnswer(() -> recordContext).anyTimes();
        }

        private void appConfigsWithPrefix() {
            final Capture<String> prefixCapture = Capture.newInstance();
            expect(mock.appConfigsWithPrefix(capture(prefixCapture)))
                    .andAnswer(() -> config.originalsWithPrefix(prefixCapture.getValue()))
                    .anyTimes();
        }

        private void appConfigs() {
            expect(mock.appConfigs()).andAnswer(() -> {
                final Map<String, Object> combined = new HashMap<>();
                combined.putAll(config.originals());
                combined.putAll(config.values());
                return combined;
            }).anyTimes();
        }

        private void timestamp() {
            expect(mock.timestamp()).andAnswer(() -> recordContext.timestamp()).anyTimes();
        }

        private void headers() {
            expect(mock.headers()).andAnswer(() -> recordContext.headers()).anyTimes();
        }

        private void offset() {
            expect(mock.offset()).andAnswer(() -> recordContext.offset()).anyTimes();
        }

        private void partition() {
            expect(mock.partition()).andAnswer(() -> recordContext.partition()).anyTimes();
        }

        private void topic() {
            expect(mock.topic()).andAnswer(() -> recordContext.topic()).anyTimes();
        }

        private void commit() {
            mock.commit();
            expectLastCall().andAnswer(() -> {
                processorContext.commit();
                return null;
            }).anyTimes();
        }

        private void forwardKeyValue() {
            final Capture<Object> keyCapture = Capture.newInstance();
            final Capture<Object> valueCapture = Capture.newInstance();

            mock.forward(capture(keyCapture), capture(valueCapture));
            expectLastCall().andAnswer(() -> {
                mock.forward(keyCapture.getValue(), valueCapture.getValue(), To.all());
                return null;
            }).anyTimes();
        }

        @SuppressWarnings("unchecked")
        private <K, V> void forwardKeyValueTo() {
            final Capture<Object> keyCapture = Capture.newInstance();
            final Capture<Object> valueCapture = Capture.newInstance();
            final Capture<To> toCapture = Capture.newInstance();

            mock.forward(capture(keyCapture), capture(valueCapture), capture(toCapture));
            expectLastCall().andAnswer(() -> {
                final To to = toCapture.getValue();
                processorContext.forward(keyCapture.getValue(), valueCapture.getValue(), to);
                toInternal.update(to);
                if (toInternal.hasTimestamp()) {
                    setRecordContext(new ProcessorRecordContext(toInternal.timestamp(), mock.offset(), mock.partition(), mock.topic(), mock.headers()));
                }
                final ProcessorNode thisNode = processorNode;
                try {
                    for (final ProcessorNode childNode : (List<ProcessorNode<K, V>>) thisNode.children()) {
                        if (toInternal.child() == null || toInternal.child().equals(childNode.name())) {
                            processorNode = childNode;
                            childNode.process(keyCapture.getValue(), valueCapture.getValue());
                            toInternal.update(to); // need to reset because MockProcessorContext is shared over multiple Processors and toInternal might have been modified
                        }
                    }
                } finally {
                    processorNode = thisNode;
                }
                return null;
            }).anyTimes();
        }

        private void schedule() {
            final Capture<Duration> interval = Capture.newInstance();
            final Capture<PunctuationType> type = Capture.newInstance();
            final Capture<Punctuator> punctuator = Capture.newInstance();
            expect(mock.schedule(capture(interval), capture(type), capture(punctuator)))
                    .andAnswer(() -> processorContext.schedule(interval.getValue(), type.getValue(), punctuator.getValue()))
                    .anyTimes();
        }

        private void getStateStore() {
            final Capture<String> stateStoreNameCapture = Capture.newInstance();
            expect(mock.getStateStore(capture(stateStoreNameCapture)))
                    .andAnswer(() -> stateStoreMap.get(stateStoreNameCapture.getValue()))
                    .anyTimes();
        }

        private void register() {
            final Capture<StateStore> storeCapture = Capture.newInstance();
            final Capture<StateRestoreCallback> restoreCallbackCapture = Capture.newInstance();

            mock.register(capture(storeCapture), capture(restoreCallbackCapture));

            expectLastCall()
                    .andAnswer(() -> {
                        stateStoreMap.put(storeCapture.getValue().name(), storeCapture.getValue());
                        return null;
                    })
                    .anyTimes();
        }

        private void metrics() {
            expect(mock.metrics()).andReturn(metrics).anyTimes();
        }

        private void stateDir() {
            expect(mock.stateDir()).andReturn(stateDir).anyTimes();
        }

        private void valueSerde() {
            expect((Serde) mock.valueSerde()).andReturn(valueSerde).anyTimes();
        }

        private void keySerde() {
            expect((Serde) mock.keySerde()).andReturn(keySerde).anyTimes();
        }

        private void taskId() {
            expect(mock.taskId()).andReturn(taskId).anyTimes();
        }

        private void applicationId() {
            expect(mock.applicationId()).andReturn(applicationId).anyTimes();
        }

    }
}
