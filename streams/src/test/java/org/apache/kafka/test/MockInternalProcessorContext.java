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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.AbstractProcessorContext;
import org.apache.kafka.streams.processor.internals.CompositeRestoreListener;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.ToInternal;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecordingTrigger;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.kafka.streams.processor.internals.StateRestoreCallbackAdapter.adapt;
import static org.easymock.EasyMock.capture;

/**
 *  How to use this class:
 *
 *  Client                                                  MockInternalProcessorContext
 *    |                                                                 |
 *    |     {@link MockInternalProcessorContext#builder()}              |
 *    | --------------------------------------------------------------> |
 *    |                                                                 |
 *    |                                     Builder <------------------ |
 *    |                                        |                        |
 *    |                                        |                        |
 *    |     {@link Builder}                    |                        |
 *    | <-------------------------------------------------------------- |
 *    |                                        |                        |
 *    |                                        |
 *    | {@link Builder#expectedAnswers()}      |
 *    | -------------------------------------> |
 *    |                                        |
 *    |      change expected answers           |
 *    |                                        |
 *    | {@link Builder#build()}                |
 *    | -------------------------------------> |
 *    | {@link InternalProcessorContextMock}   |
 *    | <------------------------------------- |
 */
public class MockInternalProcessorContext {

    private static final String PROCESSOR_NODE_NAME = "TESTING_NODE";
    private static final String CLIENT_ID = "mock";

    public static Builder builder() {
        return new Builder(getDefaultExpectedAnswer(MockInternalProcessorContext::getProcessorContext));
    }

    public static Builder builder(final Properties config) {
        return builder(config, new TaskId(0, 0));
    }

    public static Builder builder(final Properties config, final TaskId taskId) {
        return builder(config, taskId, TestUtils.tempDirectory());
    }

    public static Builder builder(final Properties config, final TaskId taskId, final File stateDir) {
        return new Builder(getDefaultExpectedAnswer(() -> getProcessorContext(config, taskId, stateDir)));
    }

    public static Builder builder(final File stateDir,
                                  final StreamsConfig config) {
        final StreamsMetricsImpl metrics = createStreamsMetrics(config, StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG);
        return builder(stateDir, null, null, metrics, config, null, null);
    }

    public static Builder builder(final File stateDir,
                                  final StreamsConfig config,
                                  final RecordCollector collector) {
        final String version = StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG;
        final StreamsMetricsImpl streamsMetrics = createStreamsMetrics(config, version);
        final RecordCollector.Supplier supplier = () -> collector;
        return builder(stateDir, null, null, streamsMetrics, config, supplier, null);
    }

    public static Builder builder(final File stateDir,
                                  final Serde<?> keySerde,
                                  final Serde<?> valSerde,
                                  final StreamsConfig config) {
        final StreamsMetricsImpl streamsMetrics = createStreamsMetrics(StreamsConfig.METRICS_LATEST);
        return builder(stateDir, keySerde, valSerde, streamsMetrics, config, null, null);
    }

    public static Builder builder(final StateSerdes<?, ?> serdes,
                                  final RecordCollector collector) {
        final Serde<?> keySerde = serdes.keySerde();
        final Serde<?> valueSerde = serdes.valueSerde();
        return builder(null, keySerde, valueSerde, collector, null);
    }

    public static Builder builder(final StateSerdes<?, ?> serdes,
                                  final RecordCollector collector,
                                  final Metrics metrics) {
        final Serde<?> keySerde = serdes.keySerde();
        final Serde<?> valueSerde = serdes.valueSerde();
        final StreamsMetricsImpl streamsMetrics = createStreamsMetrics(metrics);
        final StreamsConfig streamsConfig = createStreamConfig();
        final RecordCollector.Supplier supplier = () -> collector;
        return builder(null, keySerde, valueSerde, streamsMetrics, streamsConfig, supplier, null);
    }

    public static Builder builder(final File stateDir,
                                  final Serde<?> keySerde,
                                  final Serde<?> valSerde,
                                  final RecordCollector collector,
                                  final ThreadCache cache) {
        final StreamsMetricsImpl streamsMetrics = createStreamsMetrics(StreamsConfig.METRICS_LATEST);
        final StreamsConfig streamsConfig = createStreamConfig();
        final RecordCollector.Supplier supplier = () -> collector;
        return builder(stateDir, keySerde, valSerde, streamsMetrics, streamsConfig, supplier, cache);
    }

    public static Builder builder(final File stateDir,
                                  final Serde<?> keySerde,
                                  final Serde<?> valSerde,
                                  final StreamsMetricsImpl metrics,
                                  final StreamsConfig config,
                                  final RecordCollector.Supplier collectorSupplier,
                                  final ThreadCache cache) {
        final MockProcessorContext mockProcessorContext = getProcessorContext();
        final ExpectedAnswers expectedAnswers = getDefaultExpectedAnswer(() -> mockProcessorContext);
        final AbstractProcessorContext abstractProcessorContext =
                getAbstractProcessorContext(mockProcessorContext.taskId(), metrics, config, cache);
        final Captures captures = expectedAnswers.getCaptures();
        // Delegate expected answers to the abstract processor context
        expectedAnswers.setCurrentNode(DefaultExpectedAnswers
                .currentNode(abstractProcessorContext, captures.processorNode));
        expectedAnswers.setApplicationId(DefaultExpectedAnswers
                .applicationId(abstractProcessorContext));
        expectedAnswers.setTaskId(DefaultExpectedAnswers
                .taskId(abstractProcessorContext));
        expectedAnswers.setAppConfigs(DefaultExpectedAnswers
                .appConfigs(abstractProcessorContext));
        expectedAnswers.setAppConfigsWithPrefix(DefaultExpectedAnswers
                .appConfigsWithPrefix(abstractProcessorContext, captures.prefix));
        expectedAnswers.setMetrics(DefaultExpectedAnswers
                .metrics(abstractProcessorContext));
        expectedAnswers.setForwardKeyValue(DefaultExpectedAnswers
                .forwardKeyValue(abstractProcessorContext, captures.key, captures.value));
        expectedAnswers.setForwardKeyValueTo(DefaultExpectedAnswers
                .forwardKeyValueTo(abstractProcessorContext, captures.key, captures.value, captures.to));
        expectedAnswers.setGetCache(DefaultExpectedAnswers
                .getCache(abstractProcessorContext));
        expectedAnswers.setKeySerde(DefaultExpectedAnswers
                .keySerde(keySerde, captures.keySerde));
        expectedAnswers.setValueSerde(DefaultExpectedAnswers
                .valueSerde(valSerde, captures.valueSerde));
        expectedAnswers.setStateDir(DefaultExpectedAnswers
                .stateDir(stateDir));
        expectedAnswers.setRecordCollector(DefaultExpectedAnswers
                .recordCollector(collectorSupplier));
        final Builder builder = new Builder(expectedAnswers);
        mockForwardAbstractProcessorContext(abstractProcessorContext, builder.internalProcessorContextMock());
        EasyMock.replay(abstractProcessorContext);
        abstractProcessorContext.metrics().setRocksDBMetricsRecordingTrigger(new RocksDBMetricsRecordingTrigger());
        abstractProcessorContext.setCurrentNode(new ProcessorNode(PROCESSOR_NODE_NAME));
        return builder;
    }

    public static Builder builder(final RecordCollector collector) {
        return builder(TestUtils.tempDirectory(),
                Serdes.String(),
                Serdes.Long(),
                collector,
                new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics())));
    }

    public static Builder builder(final ThreadCache cache) {
        return builder(null, null, null, null, cache);
    }

    private static StreamsMetricsImpl createStreamsMetrics(final StreamsConfig config, String version) {
        final Metrics metrics = new Metrics();
        final String metricsVersion = config.getString(version);
        return new StreamsMetricsImpl(metrics, CLIENT_ID, metricsVersion);
    }

    private static StreamsMetricsImpl createStreamsMetrics(String version) {
        final Metrics metrics = new Metrics();
        return new StreamsMetricsImpl(metrics, CLIENT_ID, version);
    }

    private static StreamsMetricsImpl createStreamsMetrics(final Metrics metrics) {
        return new StreamsMetricsImpl(metrics, CLIENT_ID, StreamsConfig.METRICS_LATEST);
    }

    private static StreamsConfig createStreamConfig() {
        return new StreamsConfig(StreamsTestUtils.getStreamsConfig());
    }

    private static ExpectedAnswers getDefaultExpectedAnswer(
            final Supplier<MockProcessorContext> mockProcessorContextSupplier) {
        return new DefaultExpectedAnswers(mockProcessorContextSupplier.get()).get();
    }


    private static MockProcessorContext getProcessorContext() {
        return new MockProcessorContext();
    }

    private static MockProcessorContext getProcessorContext(final Properties config,
                                                            final TaskId taskId,
                                                            final File stateDir) {
        return new MockProcessorContext(config, taskId, stateDir);
    }

    @SuppressWarnings("unchecked")
    private static <K, V>  void mockForwardAbstractProcessorContext(
            final AbstractProcessorContext abstractProcessorContext,
            final InternalProcessorContextMock internalProcessorContextMock) {
        final Capture<K> keyCapture = Capture.newInstance();
        final Capture<V> valueCapture = Capture.newInstance();
        final Capture<To> toCapture = Capture.newInstance();
        final ToInternal toInternal = new ToInternal();

        abstractProcessorContext.forward(
                capture(keyCapture),
                capture(valueCapture));
        EasyMock.expectLastCall()
                .andAnswer(() -> {
                    abstractProcessorContext.forward(
                            keyCapture.getValue(),
                            valueCapture.getValue(),
                            To.all());
                    return null;
                }).anyTimes();

        abstractProcessorContext.forward(
                capture(keyCapture),
                capture(valueCapture),
                capture(toCapture));
        EasyMock.expectLastCall().andAnswer(() -> {
            toInternal.update(toCapture.getValue());
            if (toInternal.hasTimestamp()) {
                internalProcessorContextMock.setTimestamp(toInternal.timestamp());
            }
            final ProcessorNode thisNode =
                    internalProcessorContextMock.currentNode();
            try {
                for (final ProcessorNode childNode : (List<ProcessorNode<K, V>>) thisNode.children()) {
                    if (toInternal.child() == null || toInternal.child().equals(childNode.name())) {
                        internalProcessorContextMock.setCurrentNode(childNode);
                        childNode.process(keyCapture.getValue(), valueCapture.getValue());
                        // need to reset because MockProcessorContext is shared
                        // over multiple Processors and toInternal might have been modified
                        toInternal.update(toCapture.getValue());
                    }
                }
            } finally {
                internalProcessorContextMock.setCurrentNode(thisNode);
            }
            return null;
        }).anyTimes();
    }

    public static class Captures {
        final Capture<Object> key = Capture.newInstance();
        final Capture<Object> value = Capture.newInstance();
        final Capture<To> to = Capture.newInstance();
        final Capture<Duration> intervalDuration = Capture.newInstance();
        final Capture<PunctuationType> punctuationType = Capture.newInstance();
        final Capture<Punctuator> punctuator = Capture.newInstance();
        final Capture<String> stateStoreName = Capture.newInstance();
        final Capture<String> prefix = Capture.newInstance();
        final Capture<Boolean> initialize = Capture.newInstance();
        final Capture<String> stateRestoreCallbackName = Capture.newInstance();
        final Map<String, StateRestoreCallback> restoreCallbacks = new HashMap<>();
        final Capture<StateStore> stateStore = Capture.newInstance();
        final Capture<StateRestoreCallback> stateRestoreCallback = Capture.newInstance();
        final Capture<RecordCollector> recordCollector = Capture.newInstance();
        final Capture<ProcessorRecordContext> processorRecordContext = Capture.newInstance();
        final Capture<ProcessorNode> processorNode = Capture.newInstance();
        final Capture<Long> timestamp = Capture.newInstance();
        final Capture<Serde<?>> keySerde = Capture.newInstance();
        final Capture<Serde<?>> valueSerde = Capture.newInstance();
        final Capture<Iterable<KeyValue<byte[], byte[]>>> changelogCapture = Capture.newInstance();
        final Capture<String> storeNameCapture = Capture.newInstance();
        final Map<String, StateStore> stateStoreMap = new LinkedHashMap<>();

        public Capture<Object> getKey() {
            return key;
        }

        public Capture<Object> getValue() {
            return value;
        }

        public Capture<To> getTo() {
            return to;
        }

        public Capture<Duration> getIntervalDuration() {
            return intervalDuration;
        }

        public Capture<PunctuationType> getPunctuationType() {
            return punctuationType;
        }

        public Capture<Punctuator> getPunctuator() {
            return punctuator;
        }

        public Capture<String> getStateStoreName() {
            return stateStoreName;
        }

        public Capture<String> getPrefix() {
            return prefix;
        }

        public Capture<Boolean> getInitialize() {
            return initialize;
        }

        public Capture<String> getStateRestoreCallbackName() {
            return stateRestoreCallbackName;
        }

        public Map<String, StateRestoreCallback> getRestoreCallbacks() {
            return restoreCallbacks;
        }

        public Capture<StateStore> getStateStore() {
            return stateStore;
        }

        public Capture<StateRestoreCallback> getStateRestoreCallback() {
            return stateRestoreCallback;
        }

        public Capture<RecordCollector> getRecordCollector() {
            return recordCollector;
        }

        public Capture<ProcessorRecordContext> getProcessorRecordContext() {
            return processorRecordContext;
        }

        public Capture<ProcessorNode> getProcessorNode() {
            return processorNode;
        }

        public Capture<Long> getTimestamp() {
            return timestamp;
        }

        public Capture<Serde<?>> getKeySerde() {
            return keySerde;
        }

        public Capture<Serde<?>> getValueSerde() {
            return valueSerde;
        }

        public Map<String, StateStore> getStateStoreMap() {
            return stateStoreMap;
        }
    }

    public static class Builder {

        private final InternalProcessorContextMock internalProcessorContext;
        private final ExpectedAnswers expectedAnswers;
        private final Captures captures;

        Builder(final ExpectedAnswers expectedAnswers) {
            internalProcessorContext = EasyMock.mock(InternalProcessorContextMock.class);
            this.expectedAnswers = expectedAnswers;
            this.captures = expectedAnswers.getCaptures();
        }

        public ExpectedAnswers expectedAnswers() {
            return expectedAnswers;
        }

        public InternalProcessorContextMock build() {
            this
                    .stateDir()
                    .setCurrentNode()
                    .currentNode()
                    .setRecordContext()
                    .recordContext()
                    .setRecordCollector()
                    .recordCollector()
                    .stateStoreCallback()
                    .initialize()
                    .uninitialize()
                    .applicationId()
                    .taskId()
                    .appConfigs()
                    .appConfigsWithPrefix()
                    .keySerde()
                    .setKeySerde()
                    .valueSerde()
                    .setValueSerde()
                    .metrics()
                    .topic()
                    .partition()
                    .offset()
                    .headers()
                    .timestamp()
                    .setTimestamp()
                    .getStateStore()
                    .schedule()
                    .forward()
                    .forwarded()
                    .commit()
                    .register()
                    .restore()
                    .getCache();
            EasyMock.replay(internalProcessorContext);
            return internalProcessorContext;
        }

        // Only for internal use
        InternalProcessorContextMock internalProcessorContextMock() {
            return internalProcessorContext;
        }

        private Builder stateDir() {
            EasyMock.expect(internalProcessorContext.stateDir())
                    .andAnswer(expectedAnswers.getStateDir().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder setCurrentNode() {
            internalProcessorContext.setCurrentNode(capture(captures.processorNode));
            EasyMock.expectLastCall()
                    .andAnswer(expectedAnswers.getSetCurrentNode().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder currentNode() {
            EasyMock.expect(internalProcessorContext.currentNode())
                    .andAnswer(expectedAnswers.getCurrentNode().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder setRecordContext() {
            internalProcessorContext.setRecordContext(capture(captures.processorRecordContext));
            EasyMock.expectLastCall()
                    .andAnswer(expectedAnswers.getSetRecordContext().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder recordContext() {
            EasyMock.expect(internalProcessorContext.recordContext())
                    .andAnswer(expectedAnswers.getRecordContext().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder setRecordCollector() {
            internalProcessorContext.setRecordCollector(capture(captures.recordCollector));
            EasyMock.expectLastCall()
                    .andAnswer(expectedAnswers.getSetRecordCollector().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder recordCollector() {
            EasyMock.expect(internalProcessorContext.recordCollector())
                    .andAnswer(expectedAnswers.getRecordCollector().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder stateStoreCallback() {
            EasyMock.expect(internalProcessorContext.stateRestoreCallback(capture(captures.stateRestoreCallbackName)))
                    .andAnswer(expectedAnswers.getStateStoreCallback().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder initialize() {
            internalProcessorContext.initialize();
            EasyMock.expectLastCall()
                    .andAnswer(expectedAnswers.getInitialize().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder uninitialize() {
            internalProcessorContext.uninitialize();
            EasyMock.expectLastCall()
                    .andAnswer(expectedAnswers.getUninitialize().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder applicationId() {
            EasyMock.expect(internalProcessorContext.applicationId())
                    .andAnswer(expectedAnswers.getApplicationId().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder taskId() {
            EasyMock.expect(internalProcessorContext.taskId())
                    .andAnswer(expectedAnswers.getTaskId().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder appConfigs() {
            EasyMock.expect(internalProcessorContext.appConfigs())
                    .andAnswer(expectedAnswers.getAppConfigs().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder appConfigsWithPrefix() {
            EasyMock.expect(internalProcessorContext.appConfigsWithPrefix(
                    capture(captures.prefix)))
                    .andAnswer(expectedAnswers.getAppConfigsWithPrefix().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder keySerde() {
            EasyMock.expect((Serde) internalProcessorContext.keySerde())
                    .andAnswer(expectedAnswers.getKeySerde().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder setKeySerde() {
            internalProcessorContext.setKeySerde(capture(captures.keySerde));
            EasyMock.expectLastCall()
                    .andAnswer(expectedAnswers.getSetKeySerde().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder valueSerde() {
            EasyMock.expect((Serde) internalProcessorContext.valueSerde())
                    .andAnswer(expectedAnswers.getValueSerde().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder setValueSerde() {
            internalProcessorContext.setValueSerde(capture(captures.valueSerde));
            EasyMock.expectLastCall()
                    .andAnswer(expectedAnswers.getSetValueSerde().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder metrics() {
            EasyMock.expect(internalProcessorContext.metrics())
                    .andAnswer(expectedAnswers.getMetrics().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder topic() {
            EasyMock.expect(internalProcessorContext.topic())
                    .andAnswer(expectedAnswers.getTopic().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder partition() {
            EasyMock.expect(internalProcessorContext.partition())
                    .andAnswer(expectedAnswers.getPartition().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder offset() {
            EasyMock.expect(internalProcessorContext.offset())
                    .andAnswer(expectedAnswers.getOffset().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder headers() {
            EasyMock.expect(internalProcessorContext.headers())
                    .andAnswer(expectedAnswers.getHeaders().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder timestamp() {
            EasyMock.expect(internalProcessorContext.timestamp())
                    .andAnswer(expectedAnswers.getTimestamp().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder setTimestamp() {
            internalProcessorContext.setTimestamp(EasyMock.captureLong(captures.timestamp));
            EasyMock.expectLastCall()
                    .andAnswer(expectedAnswers.getSetTimestamp().apply(internalProcessorContext));
            EasyMock.expectLastCall().anyTimes();
            return this;
        }

        private Builder getStateStore() {
            EasyMock.expect(internalProcessorContext.getStateStore(capture(captures.stateStoreName)))
                    .andAnswer(expectedAnswers.getGetStateStore().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder schedule() {
            EasyMock.expect(internalProcessorContext.schedule(
                    capture(captures.intervalDuration),
                    capture(captures.punctuationType),
                    capture(captures.punctuator)))
                    .andAnswer(expectedAnswers.getSchedule().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder forward() {
            internalProcessorContext.forward(
                    capture(captures.key),
                    capture(captures.value));
            EasyMock.expectLastCall()
                    .andAnswer(expectedAnswers.getForwardKeyValue().apply(internalProcessorContext))
                    .anyTimes();
            internalProcessorContext.forward(capture(captures.key),
                    capture(captures.value),
                    capture(captures.to));
            EasyMock.expectLastCall()
                    .andAnswer(expectedAnswers.getForwardKeyValueTo().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder forwarded() {
            EasyMock.expect(internalProcessorContext.forwarded())
                    .andAnswer(expectedAnswers.getForwarded().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder commit() {
            internalProcessorContext.commit();
            EasyMock.expectLastCall()
                    .andAnswer(expectedAnswers.getCommit().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder register() {
            internalProcessorContext.register(
                    capture(captures.stateStore),
                    capture(captures.stateRestoreCallback));
            EasyMock.expectLastCall()
                    .andAnswer(expectedAnswers.getRegister().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder restore() {
            EasyMock.expect(internalProcessorContext.getRestoreListener(capture(captures.storeNameCapture)))
                    .andAnswer(expectedAnswers.getGetRestoreListener().apply(internalProcessorContext))
                    .anyTimes();

            internalProcessorContext.restore(
                    capture(captures.storeNameCapture),
                    capture(captures.changelogCapture));
            EasyMock.expectLastCall()
                    .andAnswer(expectedAnswers.getRestore().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }

        private Builder getCache() {
            EasyMock.expect(internalProcessorContext.getCache())
                    .andAnswer(expectedAnswers.getGetCache().apply(internalProcessorContext))
                    .anyTimes();
            return this;
        }
    }

    /**
     * {@link IPCFunction<R>} is a function that takes an InternalProcessorContext
     * and returns an answer of type IAnswer<R>.
     *
     * Note: IPC => InternalProcessorContext
     *
     * @param <R> return type
     */
    @FunctionalInterface
    public interface IPCFunction<R> extends Function<InternalProcessorContext, IAnswer<R>> { }

    @FunctionalInterface
    public interface IPCBiFunction<P, R> extends BiFunction<InternalProcessorContext, P, IAnswer<R>> { }

    @FunctionalInterface
    public interface IPCTriFunction<P1, P2, R> {
        IAnswer<R> apply(InternalProcessorContext context, P1 var1, P2 var2);
    }

    @FunctionalInterface
    public interface IPCQuadrFunction<P1, P2, P3, R> {
        IAnswer<R> apply(InternalProcessorContext context, P1 var1, P2 var2, P3 var3);
    }

    @FunctionalInterface
    public interface IPCQuintFunction<P1, P2, P3, P4, R> {
        IAnswer<R> apply(InternalProcessorContext context, P1 var1, P2 var2, P3 var3, P4 var4);
    }

    /**
     * ExpectedAnswers keeps track of all expected answers to the
     * {@link InternalProcessorContextMock} methods.
     */
    public static class ExpectedAnswers {
        private IPCFunction<? extends String> applicationId;
        private IPCFunction<? extends Map<String, Object>> appConfigs;
        private IPCFunction<? extends Map<String, Object>> appConfigsWithPrefix;
        private IPCFunction<? extends TaskId> taskId;
        private IPCFunction<? extends Serde> keySerde;
        private IPCFunction<? extends Serde> valueSerde;
        private IPCFunction<? extends File> stateDir;
        private IPCFunction<? extends StreamsMetricsImpl> metrics;
        private IPCFunction<?> register;
        private IPCFunction<? extends Cancellable> schedule;
        private IPCFunction<? extends StateStore> getStateStore;
        private IPCFunction<? extends ProcessorRecordContext> recordContext;
        private IPCFunction<? extends RecordCollector> recordCollector;
        private IPCFunction<?> setRecordCollector;
        private IPCFunction<? extends StateRestoreCallback> stateStoreCallback;
        private IPCFunction<?> setRecordContext;
        private IPCFunction<?> setCurrentNode;
        private IPCFunction<? extends ProcessorNode> currentNode;
        private IPCFunction<? extends ThreadCache> getCache;
        private IPCFunction<?> initialize;
        private IPCFunction<?> uninitialize;
        private IPCFunction<? extends StateRestoreListener> getRestoreListener;
        private IPCFunction<?> restore;
        private IPCFunction<?> commit;
        private IPCFunction<? extends List<MockProcessorContext.CapturedForward>> forwarded;
        private IPCFunction<?> forwardKeyValue;
        private IPCFunction<?> forwardKeyValueTo;
        private IPCFunction<?> setTimestamp;
        private IPCFunction<? extends Long> timestamp;
        private IPCFunction<? extends Headers> headers;
        private IPCFunction<? extends Long> offset;
        private IPCFunction<? extends Integer> partition;
        private IPCFunction<? extends String> topic;
        private IPCFunction<?> setValueSerde;
        private IPCFunction<?> setKeySerde;
        private Captures captures;

        ExpectedAnswers(final Captures captures) {
            this.captures = captures;
        }

        public void setApplicationId(
                final IPCFunction<? extends String> applicationId) {
            this.applicationId = applicationId;
        }

        public void setAppConfigs(
                final IPCFunction<? extends Map<String, Object>> appConfigs) {
            this.appConfigs = appConfigs;
        }

        public void setAppConfigsWithPrefix(
                final IPCFunction<? extends Map<String, Object>> appConfigsWithPrefix) {
            this.appConfigsWithPrefix = appConfigsWithPrefix;
        }

        public void setTaskId(
                final IPCFunction<? extends TaskId> taskId) {
            this.taskId = taskId;
        }

        public void setKeySerde(
                final IPCFunction<? extends Serde> keySerde) {
            this.keySerde = keySerde;
        }

        public void setValueSerde(
                final IPCFunction<? extends Serde> valueSerde) {
            this.valueSerde = valueSerde;
        }

        public void setStateDir(
                final IPCFunction<? extends File> stateDir) {
            this.stateDir = stateDir;
        }

        public void setMetrics(
                final IPCFunction<? extends StreamsMetricsImpl> metrics) {
            this.metrics = metrics;
        }

        public void setRegister(
                final IPCFunction<?> register) {
            this.register = register;
        }

        public void setSchedule(
                final IPCFunction<? extends Cancellable> schedule) {
            this.schedule = schedule;
        }

        public void setGetStateStore(
                final IPCFunction<? extends StateStore> getStateStore) {
            this.getStateStore = getStateStore;
        }

        public void setRecordContext(
                final IPCFunction<? extends ProcessorRecordContext> recordContext) {
            this.recordContext = recordContext;
        }

        public void setRecordCollector(
                final IPCFunction<? extends RecordCollector> recordCollector) {
            this.recordCollector = recordCollector;
        }

        public void setSetRecordCollector(
                final IPCFunction<?> setRecordCollector) {
            this.setRecordCollector = setRecordCollector;
        }

        public void setStateStoreCallback(
                final IPCFunction<? extends StateRestoreCallback> stateStoreCallback) {
            this.stateStoreCallback = stateStoreCallback;
        }

        public void setSetRecordContext(
                final IPCFunction<?> setRecordContext) {
            this.setRecordContext = setRecordContext;
        }

        public void setSetCurrentNode(
                final IPCFunction<?> setCurrentNode) {
            this.setCurrentNode = setCurrentNode;
        }

        public void setCurrentNode(
                final IPCFunction<? extends ProcessorNode> currentNode) {
            this.currentNode = currentNode;
        }

        public void setGetCache(
                final IPCFunction<? extends ThreadCache> getCache) {
            this.getCache = getCache;
        }

        public void setInitialize(
                final IPCFunction<?> initialize) {
            this.initialize = initialize;
        }

        public void setUninitialize(
                final IPCFunction<?> uninitialize) {
            this.uninitialize = uninitialize;
        }

        public void setGetRestoreListener(
                final IPCFunction<? extends StateRestoreListener> getRestoreListener) {
            this.getRestoreListener = getRestoreListener;
        }

        public void setRestore(
                final IPCFunction<?> restore) {
            this.restore = restore;
        }

        public void setCommit(
                final IPCFunction<?> commit) {
            this.commit = commit;
        }

        public void setForwarded(
                final IPCFunction<? extends List<MockProcessorContext.CapturedForward>> forwarded) {
            this.forwarded = forwarded;
        }

        public void setForwardKeyValue(
                final IPCFunction<?> forwardKeyValue) {
            this.forwardKeyValue = forwardKeyValue;
        }

        public void setForwardKeyValueTo(
                final IPCFunction<?> forwardKeyValueTo) {
            this.forwardKeyValueTo = forwardKeyValueTo;
        }

        public void setSetTimestamp(
                final IPCFunction<?> setTimestamp) {
            this.setTimestamp = setTimestamp;
        }

        public void setTimestamp(
                final IPCFunction<? extends Long> timestamp) {
            this.timestamp = timestamp;
        }

        public void setHeaders(
                final IPCFunction<? extends Headers> headers) {
            this.headers = headers;
        }

        public void setOffset(
                final IPCFunction<? extends Long> offset) {
            this.offset = offset;
        }

        public void setPartition(
                final IPCFunction<? extends Integer> partition) {
            this.partition = partition;
        }

        public void setTopic(
                final IPCFunction<? extends String> topic) {
            this.topic = topic;
        }

        public void setSetValueSerde(
                final IPCFunction<?> setValueSerde) {
            this.setValueSerde = setValueSerde;
        }

        public void setSetKeySerde(
                final IPCFunction<?> setKeySerde) {
            this.setKeySerde = setKeySerde;
        }

        IPCFunction<? extends String> getApplicationId() {
            return applicationId;
        }

        IPCFunction<? extends Map<String, Object>> getAppConfigs() {
            return appConfigs;
        }

        IPCFunction<? extends Map<String, Object>> getAppConfigsWithPrefix() {
            return appConfigsWithPrefix;
        }

        IPCFunction<? extends TaskId> getTaskId() {
            return taskId;
        }

        IPCFunction<? extends Serde> getKeySerde() {
            return keySerde;
        }

        IPCFunction<? extends Serde> getValueSerde() {
            return valueSerde;
        }

        IPCFunction<? extends File> getStateDir() {
            return stateDir;
        }

        IPCFunction<? extends StreamsMetricsImpl> getMetrics() {
            return metrics;
        }

        IPCFunction<?> getRegister() {
            return register;
        }

        IPCFunction<? extends Cancellable> getSchedule() {
            return schedule;
        }

        IPCFunction<? extends StateStore> getGetStateStore() {
            return getStateStore;
        }

        IPCFunction<? extends ProcessorRecordContext> getRecordContext() {
            return recordContext;
        }

        IPCFunction<? extends RecordCollector> getRecordCollector() {
            return recordCollector;
        }

        IPCFunction<?> getSetRecordCollector() {
            return setRecordCollector;
        }

        IPCFunction<? extends StateRestoreCallback> getStateStoreCallback() {
            return stateStoreCallback;
        }

        IPCFunction<?> getSetRecordContext() {
            return setRecordContext;
        }

        IPCFunction<?> getSetCurrentNode() {
            return setCurrentNode;
        }

        IPCFunction<? extends ProcessorNode> getCurrentNode() {
            return currentNode;
        }

        IPCFunction<? extends ThreadCache> getGetCache() {
            return getCache;
        }

        public IPCFunction<?> getInitialize() {
            return initialize;
        }

        IPCFunction<?> getUninitialize() {
            return uninitialize;
        }

        IPCFunction<? extends StateRestoreListener> getGetRestoreListener() {
            return getRestoreListener;
        }

        IPCFunction<?> getRestore() {
            return restore;
        }

        IPCFunction<?> getCommit() {
            return commit;
        }

        IPCFunction<? extends List<MockProcessorContext.CapturedForward>> getForwarded() {
            return forwarded;
        }

        IPCFunction<?> getForwardKeyValue() {
            return forwardKeyValue;
        }

        IPCFunction<?> getForwardKeyValueTo() {
            return forwardKeyValueTo;
        }

        IPCFunction<?> getSetTimestamp() {
            return setTimestamp;
        }

        IPCFunction<? extends Long> getTimestamp() {
            return timestamp;
        }

        IPCFunction<? extends Headers> getHeaders() {
            return headers;
        }

        IPCFunction<? extends Long> getOffset() {
            return offset;
        }

        IPCFunction<? extends Integer> getPartition() {
            return partition;
        }

        IPCFunction<? extends String> getTopic() {
            return topic;
        }

        IPCFunction<?> getSetValueSerde() {
            return setValueSerde;
        }

        IPCFunction<?> getSetKeySerde() {
            return setKeySerde;
        }

        public Captures getCaptures() {
            return captures;
        }
    }

    public static class DefaultExpectedAnswers implements Supplier<ExpectedAnswers> {

        private static final long DEFAULT_LONG = -1L;

        private final MockProcessorContext mockProcessorContext;

        private DefaultExpectedAnswers(final MockProcessorContext mockProcessorContext) {
            this.mockProcessorContext = mockProcessorContext;
        }

        @Override
        public ExpectedAnswers get() {
            final Captures captures = new Captures();
            final ExpectedAnswers expectedAnswers = new ExpectedAnswers(captures);
            expectedAnswers.setApplicationId(applicationId(mockProcessorContext));
            expectedAnswers.setAppConfigs(appConfigs(mockProcessorContext));
            expectedAnswers.setAppConfigsWithPrefix(appConfigsWithPrefix(mockProcessorContext, captures.getPrefix()));
            expectedAnswers.setTaskId(taskId(mockProcessorContext));
            expectedAnswers.setKeySerde(keySerde(mockProcessorContext, captures.getKeySerde()));
            expectedAnswers.setValueSerde(valueSerde(mockProcessorContext, captures.getValueSerde()));
            expectedAnswers.setStateDir(stateDir(mockProcessorContext));
            expectedAnswers.setMetrics(metrics(mockProcessorContext));
            expectedAnswers.setRegister(register(
                    captures.stateStore, captures.stateRestoreCallback,
                    captures.restoreCallbacks, captures.stateStoreMap));
            expectedAnswers.setSchedule(schedule(
                    mockProcessorContext, captures.intervalDuration,
                    captures.punctuationType, captures.punctuator));
            expectedAnswers.setGetStateStore(getStateStore(
                    captures.stateStoreMap, captures.stateStoreName));
            expectedAnswers.setRecordContext(recordContext());
            expectedAnswers.setRecordCollector(recordCollector(
                    captures.recordCollector));
            expectedAnswers.setSetRecordCollector(setRecordCollector());
            expectedAnswers.setStateStoreCallback(stateStoreCallback(
                    captures.stateRestoreCallbackName,
                    captures.restoreCallbacks));
            expectedAnswers.setSetRecordContext(setRecordContext(
                    mockProcessorContext,
                    captures.processorRecordContext));
            expectedAnswers.setSetCurrentNode(setCurrentNode());
            expectedAnswers.setCurrentNode(currentNode(
                    captures.processorNode));
            expectedAnswers.setGetCache(getCache());
            expectedAnswers.setInitialize(initialize(captures.initialize));
            expectedAnswers.setUninitialize(uninitialize(captures.initialize));
            expectedAnswers.setCommit(commit(mockProcessorContext));
            expectedAnswers.setForwarded(forwarded(mockProcessorContext));
            expectedAnswers.setForwardKeyValue(forwardKeyValue(
                    mockProcessorContext, captures.key, captures.value));
            expectedAnswers.setForwardKeyValueTo(forwardKeyValueTo(
                    mockProcessorContext, captures.key,
                    captures.value, captures.to));
            expectedAnswers.setSetTimestamp(setTimestamp(captures.timestamp));
            expectedAnswers.setTimestamp(getTimestamp(mockProcessorContext));
            expectedAnswers.setHeaders(headers(mockProcessorContext));
            expectedAnswers.setOffset(offset(mockProcessorContext));
            expectedAnswers.setPartition(partition(mockProcessorContext));
            expectedAnswers.setTopic(topic(mockProcessorContext));
            expectedAnswers.setSetValueSerde(setValueSerde());
            expectedAnswers.setSetKeySerde(setKeySerde());
            InternalProcessorContextRestorer restorer = new InternalProcessorContextRestorer(captures.restoreCallbacks);
            expectedAnswers.setRestore(restore(restorer, captures.changelogCapture, captures.storeNameCapture));
            expectedAnswers.setGetRestoreListener(getRestoreListener(restorer, captures.storeNameCapture));
            return expectedAnswers;
        }

        public static IPCFunction<?> setCurrentNode() {
            return internalProcessorContext -> () -> null;
        }

        public static IPCFunction<? extends ProcessorNode> currentNode(
                final InternalProcessorContext processorContext,
                final Capture<ProcessorNode> processorNodeCapture) {
            return internalProcessorContext -> () -> {
                if (processorNodeCapture.hasCaptured()) {
                    return processorNodeCapture.getValue();
                }
                return processorContext.currentNode();
            };
        }

        public static IPCFunction<? extends ProcessorNode> currentNode(
                final Capture<ProcessorNode> processorNodeCapture) {
            return internalProcessorContext -> () -> {
                if (processorNodeCapture.hasCaptured()) {
                    return processorNodeCapture.getValue();
                }
                return null;
            };
        }

        public static IPCFunction<?> setRecordContext(final MockProcessorContext processorContext,
                                               final Capture<ProcessorRecordContext> processorRecordContextCapture) {
            return internalProcessorContext -> () -> {
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
            };
        }

        public static IPCFunction<? extends ProcessorRecordContext> recordContext() {
            return internalProcessorContext -> () -> new ProcessorRecordContext(
                    internalProcessorContext.timestamp(),
                    internalProcessorContext.offset(),
                    internalProcessorContext.partition(),
                    internalProcessorContext.topic(),
                    internalProcessorContext.headers()
            );
        }

        public static IPCFunction<?> setRecordCollector() {
            return internalProcessorContext -> () -> null;
        }

        public static IPCFunction<? extends RecordCollector> recordCollector(
                final Capture<RecordCollector> recordCollectorCapture) {
            return internalProcessorContext -> () -> {
                if (recordCollectorCapture.hasCaptured()) {
                    return recordCollectorCapture.getValue();
                }
                return new MockRecordCollector();
            };
        }

        public static IPCFunction<? extends RecordCollector> recordCollector(final RecordCollector.Supplier supplier) {
            return internalProcessorContext -> () -> Optional
                    .ofNullable(supplier.recordCollector())
                    .orElseThrow(() -> new UnsupportedOperationException("No RecordCollector specified"));
        }

        public static IPCFunction<? extends StateRestoreCallback> stateStoreCallback(
                final Capture<String> stateRestoreCallbackNameCapture,
                final Map<String, StateRestoreCallback> restoreCallbacks) {
            return internalProcessorContext -> () ->
                    restoreCallbacks.get(stateRestoreCallbackNameCapture.getValue());
        }

        public static IPCFunction<?> initialize(final Capture<Boolean> initializeCapture) {
            return internalProcessorContext -> () -> {
                initializeCapture.setValue(true);
                return null;
            };
        }

        public static IPCFunction<?> uninitialize(final Capture<Boolean> initializeCapture) {
            return internalProcessorContext -> () -> {
                initializeCapture.setValue(false);
                return null;
            };
        }

        public static IPCFunction<? extends String> applicationId(final ProcessorContext processorContext) {
            return internalProcessorContext -> processorContext::applicationId;
        }

        public static IPCFunction<? extends TaskId> taskId(final ProcessorContext processorContext) {
            return internalProcessorContext -> processorContext::taskId;
        }

        public static IPCFunction<? extends Map<String, Object>> appConfigs(final ProcessorContext processorContext) {
            return internalProcessorContext -> processorContext::appConfigs;
        }

        public static IPCFunction<? extends Map<String, Object>> appConfigsWithPrefix(
                final ProcessorContext processorContext,
                final Capture<String> prefixCapture) {
            return internalProcessorContext -> () -> processorContext
                    .appConfigsWithPrefix(prefixCapture.getValue());
        }

        public static IPCFunction<? extends Serde> keySerde(
                final ProcessorContext processorContext,
                final Capture<Serde<?>> keySerdeCapture) {
            return internalProcessorContext1 -> () -> {
                if (keySerdeCapture.hasCaptured()) {
                    return keySerdeCapture.getValue();
                }
                return processorContext.keySerde();
            };
        }

        public static IPCFunction<? extends Serde> keySerde(final Serde<?> keySerde,
                                                            final Capture<Serde<?>> keySerdeCapture) {
            return internalProcessorContext -> () -> {
                if (keySerdeCapture.hasCaptured()) {
                    return keySerdeCapture.getValue();
                }
                return keySerde;
            };
        }

        public static IPCFunction<?> setKeySerde() {
            return internalProcessorContext -> () -> null;
        }

        public static IPCFunction<? extends Serde> valueSerde(
                final ProcessorContext processorContext,
                final Capture<Serde<?>> valueSerdeCapture) {
            return internalProcessorContext1 -> () -> {
                if (valueSerdeCapture.hasCaptured()) {
                    return valueSerdeCapture.getValue();
                }
                return processorContext.valueSerde();
            };
        }

        public static IPCFunction<? extends Serde> valueSerde(final Serde<?> valueSerde,
                                                              final Capture<Serde<?>> valueSerdeCapture) {
            return internalProcessorContext -> () -> {
                if (valueSerdeCapture.hasCaptured()) {
                    return valueSerdeCapture.getValue();
                }
                return valueSerde;
            };
        }

        public static IPCFunction<?> setValueSerde() {
            return internalProcessorContext -> () -> null;
        }

        public static IPCFunction<? extends File> stateDir(final File stateDir) {
            return internalProcessorContext -> () -> Optional
                    .ofNullable(stateDir)
                    .orElseThrow(() -> new UnsupportedOperationException("State directory not specified"));
        }

        public static IPCFunction<? extends File> stateDir(final ProcessorContext processorContext) {
            return internalProcessorContext -> processorContext::stateDir;
        }

        public static IPCFunction<? extends StreamsMetricsImpl> metrics(final ProcessorContext processorContext) {
            return internalProcessorContext -> () -> (StreamsMetricsImpl) processorContext.metrics();
        }

        public static IPCFunction<? extends String> topic(final ProcessorContext processorContext) {
            return internalProcessorContext -> () -> {
                try {
                    return processorContext.topic();
                } catch (Exception e) {
                    return "";
                }
            };
        }

        public static IPCFunction<? extends Integer> partition(final ProcessorContext processorContext) {
            return internalProcessorContext -> () -> {
                try {
                    return processorContext.partition();
                } catch (Exception e) {
                    return (int) DEFAULT_LONG;
                }
            };
        }

        public static IPCFunction<? extends Long> offset(final ProcessorContext processorContext) {
            return internalProcessorContext -> () -> {
                try {
                    return processorContext.offset();
                } catch (Exception e) {
                    return DEFAULT_LONG;
                }
            };
        }

        public static IPCFunction<? extends Headers> headers(final MockProcessorContext processorContext) {
            return internalProcessorContext -> () -> {
                try {
                    return Optional
                            .ofNullable(processorContext.headers())
                            .orElseGet(() -> {
                                processorContext.setRecordMetadata(
                                        processorContext.topic(),
                                        processorContext.partition(),
                                        processorContext.offset(),
                                        new RecordHeaders(),
                                        processorContext.timestamp()
                                );
                                return processorContext.headers();
                            });
                } catch (Exception e) {
                    return new RecordHeaders();
                }
            };
        }

        public static IPCFunction<? extends Long> getTimestamp(
                final ProcessorContext processorContext) {
            return internalProcessorContext -> () -> {
                try {
                    return processorContext.timestamp();
                } catch (Exception e) {
                    return DEFAULT_LONG;
                }
            };
        }

        public static IPCFunction<?> setTimestamp(final Capture<Long> timestampCapture) {
            return internalProcessorContext -> () -> {
                internalProcessorContext.setRecordContext(new ProcessorRecordContext(
                        timestampCapture.getValue(),
                        internalProcessorContext.offset(),
                        internalProcessorContext.partition(),
                        internalProcessorContext.topic(),
                        internalProcessorContext.headers()));
                return null;
            };
        }

        public static IPCFunction<? extends StateStore> getStateStore(
                final Map<String, StateStore> stateStoreMap,
                final Capture<String> stateStoreNameCapture) {
            return internalProcessorContext -> () ->
                    stateStoreMap.get(stateStoreNameCapture.getValue());
        }

        public static IPCFunction<?> forwardKeyValue(
                final ProcessorContext processorContext,
                final Capture<Object> keyCapture,
                final Capture<Object> valueCapture) {
            return internalProcessorContext -> () -> {
                processorContext.forward(keyCapture.getValue(), valueCapture.getValue());
                return null;
            };
        }

        public static IPCFunction<?> forwardKeyValueTo(
                final ProcessorContext processorContext,
                final Capture<Object> keyCapture,
                final Capture<Object> valueCapture,
                final Capture<To> toCapture) {
            return internalProcessorContext -> () -> {
                processorContext.forward(
                        keyCapture.getValue(),
                        valueCapture.getValue(),
                        toCapture.getValue());
                return null;
            };
        }

        public static IPCFunction<? extends Cancellable> schedule(
                final ProcessorContext processorContext,
                final Capture<Duration> intervalDurationCapture,
                final Capture<PunctuationType> punctuationTypeCapture,
                final Capture<Punctuator> punctuatorCapture) {
            return internalProcessorContext -> () -> processorContext.schedule(
                    intervalDurationCapture.getValue(),
                    punctuationTypeCapture.getValue(),
                    punctuatorCapture.getValue());
        }

        public static IPCFunction<?
                extends List<MockProcessorContext.CapturedForward>> forwarded(
                final MockProcessorContext processorContext) {
            return internalProcessorContext -> processorContext::forwarded;
        }

        public static IPCFunction<?> commit(final ProcessorContext processorContext) {
            return internalProcessorContext ->  () -> {
                processorContext.commit();
                return null;
            };
        }

        public static IPCFunction<?> register(
                final Capture<StateStore> stateStoreCapture,
                final Capture<StateRestoreCallback> stateRestoreCallbackCapture,
                final Map<String, StateRestoreCallback> restoreCallbacks,
                final Map<String, StateStore> stateStoreMap) {
            return internalProcessorContext -> () -> {
                final StateStore stateStore = stateStoreCapture.getValue();
                final StateRestoreCallback stateRestoreCallback = stateRestoreCallbackCapture.getValue();
                restoreCallbacks.put(stateStore.name(), stateRestoreCallback);
                stateStoreMap.put(stateStore.name(), stateStore);
                return null;
            };
        }

        public static IPCFunction<?> restore(
                final InternalProcessorContextRestorer restorer,
                final Capture<Iterable<KeyValue<byte[], byte[]>>> changelogCapture,
                final Capture<String> storeNameCapture) {
            return internalProcessorContext1 -> () -> {
                restorer.restore(storeNameCapture.getValue(), changelogCapture.getValue());
                return null;
            };
        }

        public static IPCFunction<? extends StateRestoreListener> getRestoreListener(
                final InternalProcessorContextRestorer restorer,
                final Capture<String> storeNameCapture) {
            return internalProcessorContext -> () -> restorer.getRestoreListener(storeNameCapture.getValue());
        }

        public static IPCFunction<? extends  ThreadCache> getCache(
                final InternalProcessorContext processorContext) {
            return internalProcessorContext ->  processorContext::getCache;
        }

        public static IPCFunction<? extends ThreadCache> getCache() {
            return internalProcessorContext -> () -> null;
        }
    }

    /*
     * For internal use so we can mock an AbstractProcessorContext
     * without exposing the StateManager.
     */
    private static abstract class AbstractProcessorContextMock extends AbstractProcessorContext {

        public AbstractProcessorContextMock(
                final TaskId taskId,
                final StreamsConfig config,
                final StreamsMetricsImpl metrics,
                final ThreadCache cache) {
            super(taskId, config, metrics, null, cache);
        }
    }

    private static AbstractProcessorContext getAbstractProcessorContext(final TaskId taskId,
                                                                        final StreamsMetricsImpl metrics,
                                                                        final StreamsConfig config,
                                                                        final ThreadCache cache) {
        return EasyMock.partialMockBuilder(AbstractProcessorContextMock.class)
                .withConstructor(
                        TaskId.class,
                        StreamsConfig.class,
                        StreamsMetricsImpl.class,
                        ThreadCache.class)
                .withArgs(taskId, config, metrics, cache)
                .createMock();
    }

    private static class InternalProcessorContextRestorer {

        private final Map<String, StateRestoreCallback> restoreCallbacks;

        InternalProcessorContextRestorer(final Map<String, StateRestoreCallback> restoreCallbacks) {
            this.restoreCallbacks = restoreCallbacks;
        }

        StateRestoreListener getRestoreListener(final String storeName) {
            return getStateRestoreListener(restoreCallbacks.get(storeName));
        }

        void restore(final String storeName, final Iterable<KeyValue<byte[], byte[]>> changeLog) {
            final RecordBatchingStateRestoreCallback restoreCallback = adapt(restoreCallbacks.get(storeName));
            final StateRestoreListener restoreListener = getRestoreListener(storeName);

            restoreListener.onRestoreStart(null, storeName, 0L, 0L);

            final List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
            for (final KeyValue<byte[], byte[]> keyValue : changeLog) {
                records.add(new ConsumerRecord<>("", 0, 0L, keyValue.key, keyValue.value));
            }
            restoreCallback.restoreBatch(records);
            restoreListener.onRestoreEnd(null, storeName, 0L);
        }

        private StateRestoreListener getStateRestoreListener(final StateRestoreCallback restoreCallback) {
            if (restoreCallback instanceof StateRestoreListener) {
                return (StateRestoreListener) restoreCallback;
            }
            return CompositeRestoreListener.NO_OP_STATE_RESTORE_LISTENER;
        }
    }
}
