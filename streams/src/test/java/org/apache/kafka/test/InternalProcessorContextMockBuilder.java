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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.internals.QuietStreamsConfig;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.ToInternal;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecordingTrigger;
import org.easymock.Capture;
import org.easymock.EasyMock;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;

@SuppressWarnings("rawtypes")
public class InternalProcessorContextMockBuilder {

    public static final String DEFAULT_APPLICATION_ID = "test-app-id";
    public static final TaskId DEFAULT_TASK_ID = new TaskId(0, 0);
    public static final Serde DEFAULT_KEY_SERDE = new Serdes.StringSerde();
    public static final Serde DEFAULT_VALUE_SERDE = new Serdes.StringSerde();
    public static final File DEFAULT_STATE_DIR = TestUtils.tempDirectory();
    public static final long DEFAULT_TIMESTAMP = -1;
    public static final long DEFAULT_OFFSET = -1;
    public static final int DEFAULT_PARTITION = -1;
    public static final String DEFAULT_TOPIC = "";
    public static final Headers DEFAULT_RECORD_HEADERS = new RecordHeaders();
    public static final StreamsConfig DEFAULT_STREAMS_CONFIG;
    public static final StreamsMetricsImpl DEFAULT_STREAMS_METRICS;

    private static final Map<String, Object> REQUIRED_STREAMS_CONFIG;

    static {
        final Map<String, Object> appConfig = new HashMap<>();
        appConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "");
        appConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        REQUIRED_STREAMS_CONFIG = Collections.unmodifiableMap(appConfig);
        DEFAULT_STREAMS_CONFIG = new QuietStreamsConfig(REQUIRED_STREAMS_CONFIG);

        final MetricConfig metricConfig = new MetricConfig();
        metricConfig.recordLevel(Sensor.RecordingLevel.DEBUG);
        final String threadId = Thread.currentThread().getName();
        DEFAULT_STREAMS_METRICS = new StreamsMetricsImpl(new Metrics(metricConfig), threadId, StreamsConfig.METRICS_LATEST);
    }

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

    private final Map<String, StateStore> stateStores;
    private final ToInternal toInternal;

    public InternalProcessorContextMockBuilder() {
        this(DEFAULT_APPLICATION_ID, DEFAULT_TASK_ID, DEFAULT_KEY_SERDE, DEFAULT_VALUE_SERDE, DEFAULT_STATE_DIR, DEFAULT_STREAMS_METRICS);
    }

    public InternalProcessorContextMockBuilder(final MockProcessorContext processorContext) {
        this(
                processorContext.applicationId(),
                processorContext.taskId(),
                processorContext.keySerde(),
                processorContext.valueSerde(),
                processorContext.stateDir(),
                (StreamsMetricsImpl) processorContext.metrics()
        );
        this.processorContext = processorContext;
    }

    private InternalProcessorContextMockBuilder(final String applicationId,
                                                final TaskId taskId,
                                                final Serde keySerde,
                                                final Serde valueSerde,
                                                final File stateDir,
                                                final StreamsMetricsImpl metrics) {
        mock = mock(InternalProcessorContext.class);
        stateStores = new HashMap<>();
        toInternal = new ToInternal();
        setRecordContext(new ProcessorRecordContext(DEFAULT_TIMESTAMP, DEFAULT_OFFSET, DEFAULT_PARTITION, DEFAULT_TOPIC, DEFAULT_RECORD_HEADERS));
        this.config = DEFAULT_STREAMS_CONFIG;
        this.applicationId = applicationId;
        this.taskId = taskId;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.stateDir = stateDir;
        this.metrics = metrics;
    }


    public InternalProcessorContextMockBuilder cache(final ThreadCache cache) {
        this.cache = cache;
        return this;
    }

    public InternalProcessorContextMockBuilder appConfigs(Map<String, Object> config) {
        if (config == null) {
            config = new HashMap<>();
        }
        REQUIRED_STREAMS_CONFIG.forEach(config::putIfAbsent);
        this.config = new QuietStreamsConfig(config);
        return this;
    }

    public InternalProcessorContextMockBuilder metrics(final StreamsMetricsImpl metrics) {
        this.metrics = metrics;
        return this;
    }

    public InternalProcessorContextMockBuilder stateDir(final File stateDir) {
        this.stateDir = stateDir;
        return this;
    }

    public InternalProcessorContextMockBuilder valueSerde(final Serde<?> valueSerde) {
        this.valueSerde = valueSerde;
        return this;
    }

    public InternalProcessorContextMockBuilder keySerde(final Serde<?> keySerde) {
        this.keySerde = keySerde;
        return this;
    }

    public InternalProcessorContextMockBuilder taskId(final TaskId taskId) {
        this.taskId = taskId;
        return this;
    }

    public InternalProcessorContextMockBuilder applicationId(final String applicationId) {
        this.applicationId = applicationId;
        return this;
    }

    public InternalProcessorContext buildWithoutReplaying() {
        expect(mock.applicationId()).andStubReturn(applicationId);
        expect(mock.taskId()).andStubReturn(taskId);
        expect((Serde) mock.keySerde()).andStubReturn(keySerde);
        expect((Serde) mock.valueSerde()).andStubReturn(valueSerde);
        expect(mock.stateDir()).andStubReturn(stateDir);
        expect(mock.metrics()).andStubReturn(metrics);
        register();
        getStateStore();
        schedule();
        forwardKeyValue();
        forwardKeyValueTo();
        commit();
        expect(mock.topic()).andStubAnswer(() -> recordContext.topic());
        expect(mock.partition()).andStubAnswer(() -> recordContext.partition());
        setRecordContext();
        expect(mock.offset()).andStubAnswer(() -> recordContext.offset());
        expect(mock.headers()).andStubAnswer(() -> recordContext.headers());
        expect(mock.timestamp()).andStubAnswer(() -> recordContext.timestamp());
        appConfigs();
        appConfigsWithPrefix();
        expect(mock.recordContext()).andStubAnswer(() -> recordContext);
        setCurrentNode();
        expect(mock.currentNode()).andStubAnswer(() -> processorNode);
        expect(mock.getCache()).andStubAnswer(() -> cache);
        initialize();
        uninitialize();
        if (metrics != null) {
            this.metrics.setRocksDBMetricsRecordingTrigger(new RocksDBMetricsRecordingTrigger());
        }
        final String threadId = Thread.currentThread().getName();
        TaskMetrics.droppedRecordsSensorOrSkippedRecordsSensor(threadId, taskId.toString(), metrics);
        return mock;
    }

    public InternalProcessorContext build() {
        buildWithoutReplaying();
        replay(mock);
        return mock;
    }

    private void uninitialize() {
        mock.uninitialize();
        expectLastCall().andStubAnswer(() -> null);
    }

    private void initialize() {
        mock.initialize();
        expectLastCall().andStubAnswer(() -> null);
    }

    private void setCurrentNode() {
        Capture<ProcessorNode> nodeCapture = Capture.newInstance();
        mock.setCurrentNode(capture(nodeCapture));
        expectLastCall().andStubAnswer(() -> {
            processorNode = nodeCapture.getValue();
            return null;
        });
    }

    private void setRecordContext() {
        final Capture<ProcessorRecordContext> recordContextCapture = Capture.newInstance();
        mock.setRecordContext(capture(recordContextCapture));
        expectLastCall().andStubAnswer(() -> {
            setRecordContext(recordContextCapture.getValue());
            return null;
        });
    }

    private void setRecordContext(final ProcessorRecordContext recordContext) {
        this.recordContext = recordContext;
        if (processorContext != null) {
            processorContext.setRecordMetadata(
                    recordContext.topic(),
                    recordContext.partition(),
                    recordContext.offset(),
                    recordContext.headers(),
                    recordContext.timestamp()
            );
        }
    }

    private void appConfigsWithPrefix() {
        final Capture<String> prefixCapture = Capture.newInstance();
        expect(mock.appConfigsWithPrefix(capture(prefixCapture)))
                .andStubAnswer(() -> config.originalsWithPrefix(prefixCapture.getValue()));
    }

    private void appConfigs() {
        expect(mock.appConfigs()).andStubAnswer(() -> {
            final Map<String, Object> combined = new HashMap<>();
            combined.putAll(config.originals());
            combined.putAll(config.values());
            return combined;
        });
    }

    private void commit() {
        mock.commit();
        expectLastCall().andStubAnswer(() -> {
            if (processorContext == null) {
                throw new IllegalStateException("processorContext must be set before use commit() via constructor.");
            }
            processorContext.commit();
            return null;
        });
    }

    private void forwardKeyValue() {
        final Capture<Object> keyCapture = Capture.newInstance();
        final Capture<Object> valueCapture = Capture.newInstance();
        mock.forward(capture(keyCapture), capture(valueCapture));
        expectLastCall().andStubAnswer(() -> {
            mock.forward(keyCapture.getValue(), valueCapture.getValue(), To.all());
            return null;
        });
    }

    @SuppressWarnings("unchecked")
    private <K, V> void forwardKeyValueTo() {
        final Capture<Object> keyCapture = Capture.newInstance();
        final Capture<Object> valueCapture = Capture.newInstance();
        final Capture<To> toCapture = Capture.newInstance();
        mock.forward(capture(keyCapture), capture(valueCapture), capture(toCapture));
        expectLastCall().andStubAnswer(() -> {
            final To to = toCapture.getValue();
            if (processorContext != null) {
                processorContext.forward(keyCapture.getValue(), valueCapture.getValue(), to);
            }
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
        });
    }

    private void schedule() {
        final Capture<Duration> interval = Capture.newInstance();
        final Capture<PunctuationType> type = Capture.newInstance();
        final Capture<Punctuator> punctuator = Capture.newInstance();
        expect(mock.schedule(capture(interval), capture(type), capture(punctuator))).andStubAnswer(() -> {
            if (processorContext == null) {
                throw new IllegalStateException("processorContext must be set before use schedule() via constructor.");
            }
            return processorContext.schedule(interval.getValue(), type.getValue(), punctuator.getValue());
        });
    }

    private void getStateStore() {
        final Capture<String> stateStoreNameCapture = Capture.newInstance();
        expect(mock.getStateStore(capture(stateStoreNameCapture)))
                .andStubAnswer(() -> stateStores.get(stateStoreNameCapture.getValue()));
    }

    private void register() {
        final Capture<StateStore> storeCapture = Capture.newInstance();
        mock.register(capture(storeCapture), EasyMock.anyObject());
        expectLastCall().andStubAnswer(() -> {
            stateStores.put(storeCapture.getValue().name(), storeCapture.getValue());
            return null;
        });
    }
}
