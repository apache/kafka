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
import static org.easymock.EasyMock.captureLong;

/**
 * {@link MockInternalProcessorContext} creates a
 * {@link InternalProcessorContextMock} mock object that can be used for
 * creating a mock of an {@link InternalProcessorContext}
 *
 * The interface {@link InternalProcessorContextMock} adds some methods to
 * the {@link InternalProcessorContext} interface useful for testing.
 *
 * How to use this class:
 * <pre>
 * {@code
 *  Client                                                  MockInternalProcessorContext
 *    |                                                                 |
 *    |     MockInternalProcessorContext#builder()                      |
 *    | --------------------------------------------------------------> |
 *    |                                                                 |
 *    |                                     Builder <------------------ |
 *    |                                        |                        |
 *    |     Builder                            |                        |
 *    | <-------------------------------------------------------------- |
 *    |  Change expected answer of method <MethodName> in
 *    |  InternalProcessorContextMock using set<MethodName>
 *    |  method of the builder.
 *    | -------------------------------------> |
 *    |                                        |
 *    | Builder#build()                        |
 *    | -------------------------------------> |
 *    | InternalProcessorContextMock           |
 *    | <------------------------------------- |
 * }
 * </pre>
 * <p>
 *     So, for example, if you want to override the default
 *     answer to the method {@link InternalProcessorContext#getCache()} the
 *     method {@link Builder#setGetCache(IPCFunction)} can be used as shown
 *     below:
 * </p>
 * <pre>
 * {@code
 * MockInternalProcessorContext
 *   .builder(stateDir, serdes.keySerde(), serdes.valueSerde(), recordCollector, null)
 *   .setGetCache(new MockInternalProcessorContext.IPCFunction<ThreadCache>() {
 *
 *      private ThreadCache threadCache;
 *
 *      @Override
 *      public IAnswer<? extends ThreadCache> apply(final InternalProcessorContext internalProcessorContext) {
 *          return () -> {
 *              if (threadCache == null) {
 *                  threadCache = new ThreadCache(new LogContext("testCache "), 1024 * 1024L, internalProcessorContext.metrics());
 *              }
 *              return threadCache;
 *          };
 *      }
 *   })
 *   .build()
 * }
 * </pre>
 *
 * <p>Notes:</p>
 * <ul>
 *     <li>
 *          the {@code apply} method has different parameters, which are
 *          used for passing some context that depends on the method you
 *          want to override.
 *     </li>
 *     <li>
 *         the method returns an {@code IAnswer<? extends ThreadCache>} which
 *         is a lambda expression that takes no parameters and returns a
 *         {@code ? extends ThreadCache} - see EasyMock {@link IAnswer} class
 *     </li>
 * </ul>
 *
 */
public class MockInternalProcessorContext {

    /**
     * Depending on the parameters passed to the overloaded
     * {@code builder(#param)} method the class delegates method calls to an
     * instance of {@link MockProcessorContext} or
     * {@link AbstractProcessorContext}.
     *
     * Default answers are set inside methods:
     * {@link DefaultExpectedAnswers#get()} and
     * {@link MockInternalProcessorContext#builder(File, Serde, Serde, StreamsMetricsImpl, StreamsConfig, RecordCollector.Supplier, ThreadCache)}
     */

    private static final String PROCESSOR_NODE_NAME = "TESTING_NODE";
    private static final String CLIENT_ID = "mock";

    public interface InternalProcessorContextMock extends InternalProcessorContext, RecordCollector.Supplier {

        void setRecordCollector(final RecordCollector recordCollector);

        StateRestoreCallback stateRestoreCallback(final String storeName);

        List<MockProcessorContext.CapturedForward> forwarded();

        void setTimestamp(final long timestamp);

        void setKeySerde(final Serde<?> keySerde);

        void setValueSerde(final Serde<?> valSerde);

        StateRestoreListener getRestoreListener(final String storeName);

        void restore(final String storeName, final Iterable<KeyValue<byte[], byte[]>> changeLog);
    }

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

    public static Builder builder(final RecordCollector collector) {
        return builder(
                TestUtils.tempDirectory(),
                Serdes.String(),
                Serdes.Long(),
                collector,
                new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics())));
    }

    public static Builder builder(final ThreadCache cache) {
        return builder(null, null, null, null, cache);
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
        final AbstractProcessorContext abstractProcessorContext = mockAbstractProcessorContext(mockProcessorContext.taskId(), metrics, config, cache);
        final Builder builder = new Builder(expectedAnswers);
        // Delegate answers to the abstract processor context
        builder
                .setCurrentNode(DefaultExpectedAnswers.currentNode(abstractProcessorContext))
                .setApplicationId(DefaultExpectedAnswers.applicationId(abstractProcessorContext))
                .setTaskId(DefaultExpectedAnswers.taskId(abstractProcessorContext))
                .setAppConfigs(DefaultExpectedAnswers.appConfigs(abstractProcessorContext))
                .setAppConfigsWithPrefix(DefaultExpectedAnswers.appConfigsWithPrefix(abstractProcessorContext))
                .setMetrics(DefaultExpectedAnswers.metrics(abstractProcessorContext))
                .setForwardKeyValue(DefaultExpectedAnswers.forwardKeyValue(abstractProcessorContext))
                .setForwardKeyValueTo(DefaultExpectedAnswers.forwardKeyValueTo(abstractProcessorContext))
                .setGetCache(DefaultExpectedAnswers.getCache(abstractProcessorContext))
                .setKeySerde(DefaultExpectedAnswers.keySerde(keySerde))
                .setValueSerde(DefaultExpectedAnswers.valueSerde(valSerde))
                .setStateDir(DefaultExpectedAnswers.stateDir(stateDir))
                .setRecordCollector(DefaultExpectedAnswers.recordCollector(collectorSupplier));

        mockForwardAbstractProcessorContext(abstractProcessorContext, builder.internalProcessorContextMock());

        EasyMock.replay(abstractProcessorContext);
        abstractProcessorContext.metrics().setRocksDBMetricsRecordingTrigger(new RocksDBMetricsRecordingTrigger());
        abstractProcessorContext.setCurrentNode(new ProcessorNode(PROCESSOR_NODE_NAME));

        return builder;
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

    private static ExpectedAnswers getDefaultExpectedAnswer(final Supplier<MockProcessorContext> mockProcessorContextSupplier) {
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

        abstractProcessorContext.forward(capture(keyCapture), capture(valueCapture));
        EasyMock.expectLastCall()
                .andAnswer(() -> {
                    abstractProcessorContext.forward(
                            keyCapture.getValue(),
                            valueCapture.getValue(),
                            To.all());
                    return null;
                }).anyTimes();

        abstractProcessorContext.forward(capture(keyCapture), capture(valueCapture), capture(toCapture));
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

    public static class Builder {

        private final InternalProcessorContextMock internalProcessorContext;
        private final ExpectedAnswers expectedAnswers;

        Builder(final ExpectedAnswers expectedAnswers) {
            internalProcessorContext = EasyMock.mock(InternalProcessorContextMock.class);
            this.expectedAnswers = expectedAnswers;
        }

        public Builder setApplicationId(final IPCFunction<String> applicationId) {
            expectedAnswers.setApplicationId(applicationId);
            return this;
        }

        public Builder setAppConfigs(final IPCFunction<Map<String, Object>> appConfigs) {
            expectedAnswers.setAppConfigs(appConfigs);
            return this;
        }

        public Builder setAppConfigsWithPrefix(final IPCBiFunction<String, Map<String, Object>> appConfigsWithPrefix) {
            expectedAnswers.setAppConfigsWithPrefix(appConfigsWithPrefix);
            return this;
        }

        public Builder setTaskId(final IPCFunction<TaskId> taskId) {
            expectedAnswers.setTaskId(taskId);
            return this;
        }

        public Builder setStateDir(final IPCFunction<File> stateDir) {
            expectedAnswers.setStateDir(stateDir);
            return this;
        }

        public Builder setMetrics(final IPCFunction<StreamsMetricsImpl> metrics) {
            expectedAnswers.setMetrics(metrics);
            return this;
        }

        public Builder setRegister(final IPCTriFunction<StateStore, StateRestoreCallback, Void> register) {
            expectedAnswers.setRegister(register);
            return this;
        }

        public Builder setSchedule(final IPCQuadruFunction<Duration, PunctuationType, Punctuator, Cancellable> schedule) {
            expectedAnswers.setSchedule(schedule);
            return this;
        }

        public Builder setGetStateStore(final IPCBiFunction<String, StateStore> getStateStore) {
            expectedAnswers.setGetStateStore(getStateStore);
            return this;
        }

        public Builder setSetRecordCollector(final IPCBiFunction<RecordCollector, Void> setRecordCollector) {
            expectedAnswers.setSetRecordCollector(setRecordCollector);
            return this;
        }

        public Builder setStateStoreCallback(final IPCBiFunction<String, StateRestoreCallback> stateStoreCallback) {
            expectedAnswers.setStateStoreCallback(stateStoreCallback);
            return this;
        }

        public Builder setSetRecordContext(final IPCBiFunction<ProcessorRecordContext, Void> setRecordContext) {
            expectedAnswers.setSetRecordContext(setRecordContext);
            return this;
        }

        public Builder setSetCurrentNode(final IPCBiFunction<ProcessorNode, Void> setCurrentNode) {
            expectedAnswers.setSetCurrentNode(setCurrentNode);
            return this;
        }

        public Builder setGetCache(final IPCFunction<ThreadCache> getCache) {
            expectedAnswers.setGetCache(getCache);
            return this;
        }

        public Builder setInitialize(final IPCFunction<Void> initialize) {
            expectedAnswers.setInitialize(initialize);
            return this;
        }

        public Builder setUninitialize(final IPCFunction<Void> uninitialize) {
            expectedAnswers.setUninitialize(uninitialize);
            return this;
        }

        public Builder setGetRestoreListener(final IPCBiFunction<String, StateRestoreListener> getRestoreListener) {
            expectedAnswers.setGetRestoreListener(getRestoreListener);
            return this;
        }

        public Builder setRestore(final IPCTriFunction<String, Iterable<KeyValue<byte[], byte[]>>, Void> restore) {
            expectedAnswers.setRestore(restore);
            return this;
        }

        public Builder setCommit(final IPCFunction<Void> commit) {
            expectedAnswers.setCommit(commit);
            return this;
        }

        public Builder setForwarded(final IPCFunction<List<MockProcessorContext.CapturedForward>> forwarded) {
            expectedAnswers.setForwarded(forwarded);
            return this;
        }

        public Builder setForwardKeyValue(final IPCTriFunction<Object, Object, Void> forwardKeyValue) {
            expectedAnswers.setForwardKeyValue(forwardKeyValue);
            return this;
        }

        public Builder setForwardKeyValueTo(final IPCQuadruFunction<Object, Object, To, Void> forwardKeyValueTo) {
            expectedAnswers.setForwardKeyValueTo(forwardKeyValueTo);
            return this;
        }

        public Builder setSetTimestamp(final IPCBiFunction<Long, Void> setTimestamp) {
            expectedAnswers.setSetTimestamp(setTimestamp);
            return this;
        }

        public Builder setHeaders(final IPCFunction<Headers> headers) {
            expectedAnswers.setHeaders(headers);
            return this;
        }

        public Builder setOffset(final IPCFunction<Long> offset) {
            expectedAnswers.setOffset(offset);
            return this;
        }

        public Builder setPartition(final IPCFunction<Integer> partition) {
            expectedAnswers.setPartition(partition);
            return this;
        }

        public Builder setTopic(final IPCFunction<String> topic) {
            expectedAnswers.setTopic(topic);
            return this;
        }

        public Builder setSetValueSerde(final IPCBiFunction<Serde<?>, Void> setValueSerde) {
            expectedAnswers.setSetValueSerde(setValueSerde);
            return this;
        }

        public Builder setSetKeySerde(final IPCBiFunction<Serde<?>, Void> setKeySerde) {
            expectedAnswers.setSetKeySerde(setKeySerde);
            return this;
        }

        public Builder setRecordCollector(final IPCBiFunction<RecordCollector, RecordCollector> recordCollector) {
            expectedAnswers.setRecordCollector(recordCollector);
            return this;
        }

        public Builder setKeySerde(final IPCBiFunction<Serde<?>, Serde> keySerde) {
            expectedAnswers.setKeySerde(keySerde);
            return this;
        }

        public Builder setValueSerde(final IPCBiFunction<Serde<?>, Serde> valueSerde) {
            expectedAnswers.setValueSerde(valueSerde);
            return this;
        }

        public Builder setRecordContext(final IPCBiFunction<ProcessorRecordContext, ProcessorRecordContext> recordContext) {
            expectedAnswers.setRecordContext(recordContext);
            return this;
        }

        public Builder setCurrentNode(final IPCBiFunction<ProcessorNode, ProcessorNode> currentNode) {
            expectedAnswers.setCurrentNode(currentNode);
            return this;
        }

        public Builder setTimestamp(final IPCBiFunction<Long, Long> timestamp) {
            expectedAnswers.setTimestamp(timestamp);
            return this;
        }

        public InternalProcessorContextMock build() {
            this
                    .stateDir()
                    .currentNode()
                    .recordContext()
                    .recordCollector()
                    .stateStoreCallback()
                    .initialize()
                    .uninitialize()
                    .applicationId()
                    .taskId()
                    .appConfigs()
                    .appConfigsWithPrefix()
                    .keySerde()
                    .valueSerde()
                    .metrics()
                    .topic()
                    .partition()
                    .offset()
                    .headers()
                    .timestamp()
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

        private Builder currentNode() {
            final Capture<ProcessorNode> processorNode = Capture.newInstance();
            EasyMock.expect(internalProcessorContext.currentNode())
                    .andAnswer(expectedAnswers.getCurrentNode()
                            .apply(internalProcessorContext, processorNode))
                    .anyTimes();
            internalProcessorContext.setCurrentNode(capture(processorNode));
            EasyMock.expectLastCall()
                    .andAnswer(expectedAnswers.getSetCurrentNode()
                            .apply(internalProcessorContext, processorNode))
                    .anyTimes();
            return this;
        }

        private Builder recordContext() {
            final Capture<ProcessorRecordContext> processorRecordContext = Capture.newInstance();

            EasyMock.expect(internalProcessorContext.recordContext())
                    .andAnswer(expectedAnswers.getRecordContext()
                            .apply(internalProcessorContext, processorRecordContext))
                    .anyTimes();

            internalProcessorContext.setRecordContext(capture(processorRecordContext));
            EasyMock.expectLastCall()
                    .andAnswer(expectedAnswers.getSetRecordContext()
                            .apply(internalProcessorContext, processorRecordContext))
                    .anyTimes();
            return this;
        }

        private Builder recordCollector() {
            final Capture<RecordCollector> recordCollector = Capture.newInstance();
            EasyMock.expect(internalProcessorContext.recordCollector())
                    .andAnswer(expectedAnswers.getRecordCollector()
                            .apply(internalProcessorContext, recordCollector))
                    .anyTimes();

            internalProcessorContext.setRecordCollector(capture(recordCollector));
            EasyMock.expectLastCall()
                    .andAnswer(expectedAnswers.getSetRecordCollector()
                            .apply(internalProcessorContext, recordCollector))
                    .anyTimes();
            return this;
        }

        private Builder stateStoreCallback() {
            final Capture<String> stateRestoreCallbackName = Capture.newInstance();
            EasyMock.expect(internalProcessorContext.stateRestoreCallback(capture(stateRestoreCallbackName)))
                    .andAnswer(expectedAnswers.getStateStoreCallback()
                            .apply(internalProcessorContext, stateRestoreCallbackName))
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
            final Capture<String> prefix = Capture.newInstance();
            EasyMock.expect(internalProcessorContext.appConfigsWithPrefix(capture(prefix)))
                    .andAnswer(expectedAnswers.getAppConfigsWithPrefix().apply(internalProcessorContext, prefix))
                    .anyTimes();
            return this;
        }

        private Builder keySerde() {
            final Capture<Serde<?>> keySerde = Capture.newInstance();

            EasyMock.expect((Serde) internalProcessorContext.keySerde())
                    .andAnswer(expectedAnswers.getKeySerde().apply(internalProcessorContext, keySerde))
                    .anyTimes();

            internalProcessorContext.setKeySerde(capture(keySerde));
            EasyMock.expectLastCall()
                    .andAnswer(expectedAnswers.getSetKeySerde().apply(internalProcessorContext, keySerde))
                    .anyTimes();
            return this;
        }

        private Builder valueSerde() {
            final Capture<Serde<?>> valueSerde = Capture.newInstance();

            EasyMock.expect((Serde) internalProcessorContext.valueSerde())
                    .andAnswer(expectedAnswers.getValueSerde().apply(internalProcessorContext, valueSerde))
                    .anyTimes();

            internalProcessorContext.setValueSerde(capture(valueSerde));
            EasyMock.expectLastCall()
                    .andAnswer(expectedAnswers.getSetValueSerde()
                            .apply(internalProcessorContext, valueSerde))
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
            final Capture<Long> timestamp = Capture.newInstance();

            EasyMock.expect(internalProcessorContext.timestamp())
                    .andAnswer(expectedAnswers.getTimestamp().apply(internalProcessorContext, timestamp))
                    .anyTimes();

            internalProcessorContext.setTimestamp(captureLong(timestamp));
            EasyMock.expectLastCall()
                    .andAnswer(expectedAnswers.getSetTimestamp().apply(internalProcessorContext, timestamp));
            EasyMock.expectLastCall().anyTimes();
            return this;
        }

        private Builder getStateStore() {
            final Capture<String> stateStoreName = Capture.newInstance();
            EasyMock.expect(internalProcessorContext.getStateStore(capture(stateStoreName)))
                    .andAnswer(expectedAnswers.getGetStateStore().apply(internalProcessorContext, stateStoreName))
                    .anyTimes();
            return this;
        }

        private Builder schedule() {
            final Capture<Duration> intervalDuration = Capture.newInstance();
            final Capture<PunctuationType> punctuationType = Capture.newInstance();
            final Capture<Punctuator> punctuator = Capture.newInstance();
            EasyMock.expect(internalProcessorContext
                    .schedule(capture(intervalDuration), capture(punctuationType), capture(punctuator)))
                    .andAnswer(expectedAnswers.getSchedule()
                            .apply(internalProcessorContext, intervalDuration, punctuationType, punctuator))
                    .anyTimes();
            return this;
        }

        private Builder forward() {
            final Capture<Object> key = Capture.newInstance();
            final Capture<Object> value = Capture.newInstance();
            final Capture<To> to = Capture.newInstance();

            internalProcessorContext.forward(capture(key), capture(value));
            EasyMock.expectLastCall()
                    .andAnswer(expectedAnswers.getForwardKeyValue().apply(internalProcessorContext, key, value))
                    .anyTimes();

            internalProcessorContext.forward(capture(key), capture(value), capture(to));
            EasyMock.expectLastCall()
                    .andAnswer(expectedAnswers.getForwardKeyValueTo().apply(internalProcessorContext, key, value, to))
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
            final Capture<StateStore> stateStore = Capture.newInstance();
            final Capture<StateRestoreCallback> stateRestoreCallback = Capture.newInstance();
            internalProcessorContext.register(capture(stateStore), capture(stateRestoreCallback));
            EasyMock.expectLastCall()
                    .andAnswer(expectedAnswers.getRegister()
                            .apply(internalProcessorContext, stateStore, stateRestoreCallback))
                    .anyTimes();
            return this;
        }

        private Builder restore() {
            final Capture<String> storeNameCapture = Capture.newInstance();
            final Capture<Iterable<KeyValue<byte[], byte[]>>> changelogCapture = Capture.newInstance();
            EasyMock.expect(internalProcessorContext.getRestoreListener(capture(storeNameCapture)))
                    .andAnswer(expectedAnswers.getGetRestoreListener()
                            .apply(internalProcessorContext, storeNameCapture))
                    .anyTimes();

            internalProcessorContext.restore(capture(storeNameCapture), capture(changelogCapture));
            EasyMock.expectLastCall()
                    .andAnswer(expectedAnswers.getRestore()
                            .apply(internalProcessorContext, storeNameCapture, changelogCapture))
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
     * and returns an answer of type IAnswer<? extends R>.
     *
     * Note: IPC => InternalProcessorContext
     *
     * @param <R> return type
     */
    @FunctionalInterface
    public interface IPCFunction<R> extends Function<InternalProcessorContext, IAnswer<? extends R>> { }

    /**
     * {@link IPCFunction<R>} is a function that takes an
     * InternalProcessorContext and one captures,
     * and returns an answer of type IAnswer<? extends R>.
     *
     * Note: IPC => InternalProcessorContext
     *
     * @param <R> return type
     */
    @FunctionalInterface
    public interface IPCBiFunction<P, R> extends BiFunction<InternalProcessorContext, Capture<P>, IAnswer<? extends R>> { }

    /**
     * {@link IPCFunction<R>} is a function that takes an
     * InternalProcessorContext and two captures,
     * and returns an answer of type IAnswer<? extends R>.
     *
     * Note: IPC => InternalProcessorContext
     *
     * @param <R> return type
     */
    @FunctionalInterface
    public interface IPCTriFunction<P1, P2, R> {
        IAnswer<? extends R> apply(InternalProcessorContext context, Capture<P1> var1, Capture<P2> var2);
    }

    /**
     * {@link IPCFunction<R>} is a function that takes an
     * InternalProcessorContext and three captures,
     * and returns an answer of type IAnswer<? extends R>.
     *
     * Note: IPC => InternalProcessorContext
     *
     * @param <R> return type
     */
    @FunctionalInterface
    public interface IPCQuadruFunction<P1, P2, P3, R> {
        IAnswer<? extends R> apply(InternalProcessorContext context, Capture<P1> var1, Capture<P2> var2, Capture<P3> var3);
    }

    private static class ExpectedAnswers {
        private IPCBiFunction<RecordCollector, RecordCollector> recordCollector;
        private IPCBiFunction<RecordCollector, Void> setRecordCollector;
        private IPCFunction<String> applicationId;
        private IPCFunction<Map<String, Object>> appConfigs;
        private IPCBiFunction<String, Map<String, Object>> appConfigsWithPrefix;
        private IPCFunction<TaskId> taskId;
        private IPCBiFunction<Serde<?>, Serde> keySerde;
        private IPCBiFunction<Serde<?>, Serde> valueSerde;
        private IPCFunction<File> stateDir;
        private IPCFunction<StreamsMetricsImpl> metrics;
        private IPCTriFunction<StateStore, StateRestoreCallback, Void> register;
        private IPCQuadruFunction<Duration, PunctuationType, Punctuator, Cancellable> schedule;
        private IPCBiFunction<String, StateStore> getStateStore;
        private IPCBiFunction<ProcessorRecordContext, ProcessorRecordContext> recordContext;
        private IPCBiFunction<String, StateRestoreCallback> stateStoreCallback;
        private IPCBiFunction<ProcessorRecordContext, Void> setRecordContext;
        private IPCBiFunction<ProcessorNode, Void> setCurrentNode;
        private IPCBiFunction<ProcessorNode, ProcessorNode> currentNode;
        private IPCFunction<ThreadCache> getCache;
        private IPCFunction<Void> initialize;
        private IPCFunction<Void> uninitialize;
        private IPCBiFunction<String, StateRestoreListener> getRestoreListener;
        private IPCTriFunction<String, Iterable<KeyValue<byte[], byte[]>>, Void> restore;
        private IPCFunction<Void> commit;
        private IPCFunction<List<MockProcessorContext.CapturedForward>> forwarded;
        private IPCTriFunction<Object, Object, Void> forwardKeyValue;
        private IPCQuadruFunction<Object, Object, To, Void> forwardKeyValueTo;
        private IPCBiFunction<Long, Void> setTimestamp;
        private IPCBiFunction<Long, Long> timestamp;
        private IPCFunction<Headers> headers;
        private IPCFunction<Long> offset;
        private IPCFunction<Integer> partition;
        private IPCFunction<String> topic;
        private IPCBiFunction<Serde<?>, Void> setValueSerde;
        private IPCBiFunction<Serde<?>, Void> setKeySerde;

        public void setRecordCollector(final IPCBiFunction<RecordCollector, RecordCollector> recordCollector) {
            this.recordCollector = recordCollector;
        }

        public void setSetRecordCollector(final IPCBiFunction<RecordCollector, Void> setRecordCollector) {
            this.setRecordCollector = setRecordCollector;
        }

        public void setApplicationId(final IPCFunction<String> applicationId) {
            this.applicationId = applicationId;
        }

        public void setAppConfigs(final IPCFunction<Map<String, Object>> appConfigs) {
            this.appConfigs = appConfigs;
        }

        public void setAppConfigsWithPrefix(final IPCBiFunction<String, Map<String, Object>> appConfigsWithPrefix) {
            this.appConfigsWithPrefix = appConfigsWithPrefix;
        }

        public void setTaskId(final IPCFunction<TaskId> taskId) {
            this.taskId = taskId;
        }

        public void setKeySerde(final IPCBiFunction<Serde<?>, Serde> keySerde) {
            this.keySerde = keySerde;
        }

        public void setValueSerde(final IPCBiFunction<Serde<?>, Serde> valueSerde) {
            this.valueSerde = valueSerde;
        }

        public void setStateDir(final IPCFunction<File> stateDir) {
            this.stateDir = stateDir;
        }

        public void setMetrics(final IPCFunction<StreamsMetricsImpl> metrics) {
            this.metrics = metrics;
        }

        public void setRegister(final IPCTriFunction<StateStore, StateRestoreCallback, Void> register) {
            this.register = register;
        }

        public void setSchedule(final IPCQuadruFunction<Duration, PunctuationType, Punctuator, Cancellable> schedule) {
            this.schedule = schedule;
        }

        public void setGetStateStore(final IPCBiFunction<String, StateStore> getStateStore) {
            this.getStateStore = getStateStore;
        }

        public void setRecordContext(final IPCBiFunction<ProcessorRecordContext, ProcessorRecordContext> recordContext) {
            this.recordContext = recordContext;
        }

        public void setStateStoreCallback(final IPCBiFunction<String, StateRestoreCallback> stateStoreCallback) {
            this.stateStoreCallback = stateStoreCallback;
        }

        public void setSetRecordContext(final IPCBiFunction<ProcessorRecordContext, Void> setRecordContext) {
            this.setRecordContext = setRecordContext;
        }

        public void setSetCurrentNode(final IPCBiFunction<ProcessorNode, Void> setCurrentNode) {
            this.setCurrentNode = setCurrentNode;
        }

        public void setCurrentNode(final IPCBiFunction<ProcessorNode, ProcessorNode> currentNode) {
            this.currentNode = currentNode;
        }

        public void setGetCache(final IPCFunction<ThreadCache> getCache) {
            this.getCache = getCache;
        }

        public void setInitialize(final IPCFunction<Void> initialize) {
            this.initialize = initialize;
        }

        public void setUninitialize(final IPCFunction<Void> uninitialize) {
            this.uninitialize = uninitialize;
        }

        public void setGetRestoreListener(final IPCBiFunction<String, StateRestoreListener> getRestoreListener) {
            this.getRestoreListener = getRestoreListener;
        }

        public void setRestore(final IPCTriFunction<String, Iterable<KeyValue<byte[], byte[]>>, Void> restore) {
            this.restore = restore;
        }

        public void setCommit(final IPCFunction<Void> commit) {
            this.commit = commit;
        }

        public void setForwarded(final IPCFunction<List<MockProcessorContext.CapturedForward>> forwarded) {
            this.forwarded = forwarded;
        }

        public void setForwardKeyValue(final IPCTriFunction<Object, Object, Void> forwardKeyValue) {
            this.forwardKeyValue = forwardKeyValue;
        }

        public void setForwardKeyValueTo(final IPCQuadruFunction<Object, Object, To, Void> forwardKeyValueTo) {
            this.forwardKeyValueTo = forwardKeyValueTo;
        }

        public void setSetTimestamp(final IPCBiFunction<Long, Void> setTimestamp) {
            this.setTimestamp = setTimestamp;
        }

        public void setTimestamp(final IPCBiFunction<Long, Long> timestamp) {
            this.timestamp = timestamp;
        }

        public void setHeaders(final IPCFunction<Headers> headers) {
            this.headers = headers;
        }

        public void setOffset(final IPCFunction<Long> offset) {
            this.offset = offset;
        }

        public void setPartition(final IPCFunction<Integer> partition) {
            this.partition = partition;
        }

        public void setTopic(final IPCFunction<String> topic) {
            this.topic = topic;
        }

        public void setSetValueSerde(final IPCBiFunction<Serde<?>, Void> setValueSerde) {
            this.setValueSerde = setValueSerde;
        }

        public void setSetKeySerde(final IPCBiFunction<Serde<?>, Void> setKeySerde) {
            this.setKeySerde = setKeySerde;
        }

        IPCBiFunction<RecordCollector, RecordCollector> getRecordCollector() {
            return recordCollector;
        }

        IPCBiFunction<RecordCollector, Void> getSetRecordCollector() {
            return setRecordCollector;
        }

        IPCFunction<String> getApplicationId() {
            return applicationId;
        }

        IPCFunction<Map<String, Object>> getAppConfigs() {
            return appConfigs;
        }

        IPCBiFunction<String, Map<String, Object>> getAppConfigsWithPrefix() {
            return appConfigsWithPrefix;
        }

        IPCFunction<TaskId> getTaskId() {
            return taskId;
        }

        IPCBiFunction<Serde<?>, Serde> getKeySerde() {
            return keySerde;
        }

        IPCBiFunction<Serde<?>, Serde> getValueSerde() {
            return valueSerde;
        }

        IPCFunction<File> getStateDir() {
            return stateDir;
        }

        IPCFunction<StreamsMetricsImpl> getMetrics() {
            return metrics;
        }

        IPCTriFunction<StateStore, StateRestoreCallback, Void> getRegister() {
            return register;
        }

        IPCQuadruFunction<Duration, PunctuationType, Punctuator, Cancellable> getSchedule() {
            return schedule;
        }

        IPCBiFunction<String, StateStore> getGetStateStore() {
            return getStateStore;
        }

        IPCBiFunction<ProcessorRecordContext, ProcessorRecordContext> getRecordContext() {
            return recordContext;
        }

        IPCBiFunction<String, StateRestoreCallback> getStateStoreCallback() {
            return stateStoreCallback;
        }

        IPCBiFunction<ProcessorRecordContext, Void> getSetRecordContext() {
            return setRecordContext;
        }

        IPCBiFunction<ProcessorNode, Void> getSetCurrentNode() {
            return setCurrentNode;
        }

        IPCBiFunction<ProcessorNode, ProcessorNode> getCurrentNode() {
            return currentNode;
        }

        IPCFunction<ThreadCache> getGetCache() {
            return getCache;
        }

        IPCFunction<Void> getInitialize() {
            return initialize;
        }

        IPCFunction<Void> getUninitialize() {
            return uninitialize;
        }

        IPCBiFunction<String, StateRestoreListener> getGetRestoreListener() {
            return getRestoreListener;
        }

        IPCTriFunction<String, Iterable<KeyValue<byte[], byte[]>>, Void> getRestore() {
            return restore;
        }

        IPCFunction<Void> getCommit() {
            return commit;
        }

        IPCFunction<List<MockProcessorContext.CapturedForward>> getForwarded() {
            return forwarded;
        }

        IPCTriFunction<Object, Object, Void> getForwardKeyValue() {
            return forwardKeyValue;
        }

        IPCQuadruFunction<Object, Object, To, Void> getForwardKeyValueTo() {
            return forwardKeyValueTo;
        }

        IPCBiFunction<Long, Void> getSetTimestamp() {
            return setTimestamp;
        }

        IPCBiFunction<Long, Long> getTimestamp() {
            return timestamp;
        }

        IPCFunction<Headers> getHeaders() {
            return headers;
        }

        IPCFunction<Long> getOffset() {
            return offset;
        }

        IPCFunction<Integer> getPartition() {
            return partition;
        }

        IPCFunction<String> getTopic() {
            return topic;
        }

        IPCBiFunction<Serde<?>, Void> getSetValueSerde() {
            return setValueSerde;
        }

        IPCBiFunction<Serde<?>, Void> getSetKeySerde() {
            return setKeySerde;
        }
    }

    private static class DefaultExpectedAnswers implements Supplier<ExpectedAnswers> {

        private static final long DEFAULT_LONG = -1L;

        private final MockProcessorContext mockProcessorContext;

        private DefaultExpectedAnswers(final MockProcessorContext mockProcessorContext) {
            this.mockProcessorContext = mockProcessorContext;
        }

        @Override
        public ExpectedAnswers get() {
            final State state = new State();
            final ExpectedAnswers expectedAnswers = new ExpectedAnswers();
            expectedAnswers.setApplicationId(applicationId(mockProcessorContext));
            expectedAnswers.setAppConfigs(appConfigs(mockProcessorContext));
            expectedAnswers.setAppConfigsWithPrefix(appConfigsWithPrefix(mockProcessorContext));
            expectedAnswers.setTaskId(taskId(mockProcessorContext));
            expectedAnswers.setKeySerde(keySerde(mockProcessorContext));
            expectedAnswers.setValueSerde(valueSerde(mockProcessorContext));
            expectedAnswers.setStateDir(stateDir(mockProcessorContext));
            expectedAnswers.setMetrics(metrics(mockProcessorContext));
            expectedAnswers.setRegister(register(state.restoreCallbacks, state.stateStoreMap));
            expectedAnswers.setSchedule(schedule(mockProcessorContext));
            expectedAnswers.setGetStateStore(getStateStore(state.stateStoreMap));
            expectedAnswers.setRecordContext(recordContext());
            expectedAnswers.setRecordCollector(recordCollector());
            expectedAnswers.setSetRecordCollector(setRecordCollector());
            expectedAnswers.setStateStoreCallback(stateStoreCallback(state.restoreCallbacks));
            expectedAnswers.setSetRecordContext(setRecordContext(mockProcessorContext));
            expectedAnswers.setSetCurrentNode(setCurrentNode());
            expectedAnswers.setCurrentNode(currentNode());
            expectedAnswers.setGetCache(getCache());
            expectedAnswers.setInitialize(initialize(state.initialize));
            expectedAnswers.setUninitialize(uninitialize(state.initialize));
            expectedAnswers.setCommit(commit(mockProcessorContext));
            expectedAnswers.setForwarded(forwarded(mockProcessorContext));
            expectedAnswers.setForwardKeyValue(forwardKeyValue(mockProcessorContext));
            expectedAnswers.setForwardKeyValueTo(forwardKeyValueTo(mockProcessorContext));
            expectedAnswers.setSetTimestamp(setTimestamp());
            expectedAnswers.setTimestamp(getTimestamp(mockProcessorContext));
            expectedAnswers.setHeaders(headers(mockProcessorContext));
            expectedAnswers.setOffset(offset(mockProcessorContext));
            expectedAnswers.setPartition(partition(mockProcessorContext));
            expectedAnswers.setTopic(topic(mockProcessorContext));
            expectedAnswers.setSetValueSerde(setValueSerde());
            expectedAnswers.setSetKeySerde(setKeySerde());
            final Restorer restorer = new Restorer(state.restoreCallbacks);
            expectedAnswers.setRestore(restore(restorer));
            expectedAnswers.setGetRestoreListener(getRestoreListener(restorer));
            return expectedAnswers;
        }

        private static class State {
            final Map<String, StateRestoreCallback> restoreCallbacks = new HashMap<>();
            final Map<String, StateStore> stateStoreMap = new LinkedHashMap<>();
            final Capture<Boolean> initialize = Capture.newInstance();
        }

        static IPCBiFunction<ProcessorNode, Void> setCurrentNode() {
            return (internalProcessorContext, pn) -> () -> null;
        }

        static IPCBiFunction<ProcessorNode, ProcessorNode> currentNode(final InternalProcessorContext processorContext) {
            return (internalProcessorContext, processorNodeCapture) -> () -> {
                if (processorNodeCapture.hasCaptured()) {
                    return processorNodeCapture.getValue();
                }
                return processorContext.currentNode();
            };
        }

        static IPCBiFunction<ProcessorNode, ProcessorNode> currentNode() {
            return (internalProcessorContext, processorNodeCapture) -> () -> {
                if (processorNodeCapture.hasCaptured()) {
                    return processorNodeCapture.getValue();
                }
                return null;
            };
        }

        static IPCBiFunction<ProcessorRecordContext, Void> setRecordContext(final MockProcessorContext processorContext) {
            return (internalProcessorContext, processorRecordContextCapture) -> () -> {
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

        static IPCBiFunction<ProcessorRecordContext, ProcessorRecordContext> recordContext() {
            return (internalProcessorContext, processorRecordContextCapture) -> () -> new ProcessorRecordContext(
                    internalProcessorContext.timestamp(),
                    internalProcessorContext.offset(),
                    internalProcessorContext.partition(),
                    internalProcessorContext.topic(),
                    internalProcessorContext.headers()
            );
        }

        static IPCBiFunction<RecordCollector, Void> setRecordCollector() {
            return (internalProcessorContext, rcc) -> () -> null;
        }

        static IPCBiFunction<RecordCollector, RecordCollector> recordCollector() {
            return (internalProcessorContext, recordCollectorCapture) -> () -> {
                if (recordCollectorCapture.hasCaptured()) {
                    return recordCollectorCapture.getValue();
                }
                return new MockRecordCollector();
            };
        }

        static IPCBiFunction<RecordCollector, RecordCollector> recordCollector(final RecordCollector.Supplier supplier) {
            return (internalProcessorContext, recordCollector) -> () -> Optional
                    .of(recordCollector)
                    .filter(Capture::hasCaptured)
                    .map(rc -> Optional.ofNullable(rc.getValue()))
                    .orElseGet(() -> Optional.of(supplier.recordCollector()))
                    .orElseThrow(() -> new UnsupportedOperationException("No RecordCollector specified"));

        }

        static IPCBiFunction<String, StateRestoreCallback> stateStoreCallback(
                final Map<String, StateRestoreCallback> restoreCallbacks) {
            return (internalProcessorContext, stateRestoreCallbackNameCapture) -> () ->
                    restoreCallbacks.get(stateRestoreCallbackNameCapture.getValue());
        }

        static IPCFunction<Void> initialize(final Capture<Boolean> initializeCapture) {
            return internalProcessorContext -> () -> {
                initializeCapture.setValue(true);
                return null;
            };
        }

        static IPCFunction<Void> uninitialize(final Capture<Boolean> initializeCapture) {
            return internalProcessorContext -> () -> {
                initializeCapture.setValue(false);
                return null;
            };
        }

        static IPCFunction<String> applicationId(final ProcessorContext processorContext) {
            return internalProcessorContext -> processorContext::applicationId;
        }

        static IPCFunction<TaskId> taskId(final ProcessorContext processorContext) {
            return internalProcessorContext -> processorContext::taskId;
        }

        static IPCFunction<Map<String, Object>> appConfigs(final ProcessorContext processorContext) {
            return internalProcessorContext -> processorContext::appConfigs;
        }

        static IPCBiFunction<String, Map<String, Object>> appConfigsWithPrefix(
                final ProcessorContext processorContext) {
            return (internalProcessorContext, prefix) -> () -> processorContext
                    .appConfigsWithPrefix(prefix.getValue());
        }

        static IPCBiFunction<Serde<?>, Serde> keySerde(final ProcessorContext processorContext) {
            return (internalProcessorContext, keySerdeCapture) -> () -> {
                if (keySerdeCapture.hasCaptured()) {
                    return keySerdeCapture.getValue();
                }
                return processorContext.keySerde();
            };
        }

        static IPCBiFunction<Serde<?>, Serde> keySerde(final Serde<?> keySerde) {
            return (internalProcessorContext, keySerdeCapture) -> () -> {
                if (keySerdeCapture.hasCaptured()) {
                    return keySerdeCapture.getValue();
                }
                return keySerde;
            };
        }

        static IPCBiFunction<Serde<?>, Void> setKeySerde() {
            return (internalProcessorContext, serdeCapture) -> () -> null;
        }

        static IPCBiFunction<Serde<?>, Serde> valueSerde(final ProcessorContext processorContext) {
            return (internalProcessorContext, valueSerdeCapture) -> () -> {
                if (valueSerdeCapture.hasCaptured()) {
                    return valueSerdeCapture.getValue();
                }
                return processorContext.valueSerde();
            };
        }

        static IPCBiFunction<Serde<?>, Serde> valueSerde(final Serde<?> valueSerde) {
            return (internalProcessorContext, valueSerdeCapture) -> () -> {
                if (valueSerdeCapture.hasCaptured()) {
                    return valueSerdeCapture.getValue();
                }
                return valueSerde;
            };
        }

        static IPCBiFunction<Serde<?>, Void> setValueSerde() {
            return (internalProcessorContext, serdeCapture) -> () -> null;
        }

        static IPCFunction<File> stateDir(final File stateDir) {
            return internalProcessorContext -> () -> Optional
                    .ofNullable(stateDir)
                    .orElseThrow(() -> new UnsupportedOperationException("State directory not specified"));
        }

        static IPCFunction<File> stateDir(final ProcessorContext processorContext) {
            return internalProcessorContext -> processorContext::stateDir;
        }

        static IPCFunction<StreamsMetricsImpl> metrics(final ProcessorContext processorContext) {
            return internalProcessorContext -> () -> (StreamsMetricsImpl) processorContext.metrics();
        }

        static IPCFunction<String> topic(final ProcessorContext processorContext) {
            return internalProcessorContext -> () -> {
                try {
                    return processorContext.topic();
                } catch (Exception e) {
                    return "";
                }
            };
        }

        static IPCFunction<Integer> partition(final ProcessorContext processorContext) {
            return internalProcessorContext -> () -> {
                try {
                    return processorContext.partition();
                } catch (Exception e) {
                    return (int) DEFAULT_LONG;
                }
            };
        }

        static IPCFunction<Long> offset(final ProcessorContext processorContext) {
            return internalProcessorContext -> () -> {
                try {
                    return processorContext.offset();
                } catch (Exception e) {
                    return DEFAULT_LONG;
                }
            };
        }

        static IPCFunction<Headers> headers(final MockProcessorContext processorContext) {
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

        static IPCBiFunction<Long, Long> getTimestamp(final ProcessorContext processorContext) {
            return (internalProcessorContext, timestampCapture) -> () -> {
                try {
                    return processorContext.timestamp();
                } catch (Exception e) {
                    return DEFAULT_LONG;
                }
            };
        }

        static IPCBiFunction<Long, Void> setTimestamp() {
            return (internalProcessorContext, timestampCapture) -> () -> {
                internalProcessorContext.setRecordContext(new ProcessorRecordContext(
                        timestampCapture.getValue(),
                        internalProcessorContext.offset(),
                        internalProcessorContext.partition(),
                        internalProcessorContext.topic(),
                        internalProcessorContext.headers()));
                return null;
            };
        }

        static IPCBiFunction<String, StateStore> getStateStore(final Map<String, StateStore> stateStoreMap) {
            return (internalProcessorContext, stateStoreNameCapture) -> () ->
                    stateStoreMap.get(stateStoreNameCapture.getValue());
        }

        static IPCTriFunction<Object, Object, Void> forwardKeyValue(final ProcessorContext processorContext) {
            return (internalProcessorContext, keyCapture, valueCapture) -> () -> {
                processorContext.forward(keyCapture.getValue(), valueCapture.getValue());
                return null;
            };
        }

        static IPCQuadruFunction<Object, Object, To, Void> forwardKeyValueTo(final ProcessorContext processorContext) {
            return (internalProcessorContext, keyCapture, valueCapture, toCapture) -> () -> {
                processorContext.forward(
                        keyCapture.getValue(),
                        valueCapture.getValue(),
                        toCapture.getValue());
                return null;
            };
        }

        static IPCQuadruFunction<Duration, PunctuationType, Punctuator, Cancellable> schedule(final ProcessorContext processorContext) {
            return (internalProcessorContext, durationCapture, punctuationTypeCapture, punctuatorCapture) -> () ->
                    processorContext.schedule(
                            durationCapture.getValue(),
                            punctuationTypeCapture.getValue(),
                            punctuatorCapture.getValue());
        }

        static IPCFunction<List<MockProcessorContext.CapturedForward>> forwarded(
                final MockProcessorContext processorContext) {
            return internalProcessorContext -> processorContext::forwarded;
        }

        static IPCFunction<Void> commit(final ProcessorContext processorContext) {
            return internalProcessorContext ->  () -> {
                processorContext.commit();
                return null;
            };
        }

        static IPCTriFunction<StateStore, StateRestoreCallback, Void> register(
                final Map<String, StateRestoreCallback> restoreCallbacks,
                final Map<String, StateStore> stateStoreMap) {
            return (internalProcessorContext, stateStoreCapture, stateRestoreCallbackCapture) -> () -> {
                final StateStore stateStore = stateStoreCapture.getValue();
                restoreCallbacks.put(stateStore.name(), stateRestoreCallbackCapture.getValue());
                stateStoreMap.put(stateStore.name(), stateStore);
                return null;
            };
        }

        static IPCTriFunction<String, Iterable<KeyValue<byte[], byte[]>>, Void> restore(final Restorer restorer) {
            return (internalProcessorContext, storeNameCapture, changelogCapture) -> () -> {
                restorer.restore(storeNameCapture.getValue(), changelogCapture.getValue());
                return null;
            };
        }

        static IPCBiFunction<String, StateRestoreListener> getRestoreListener(final Restorer restorer) {
            return (internalProcessorContext, storeNameCapture) -> () ->
                    restorer.getRestoreListener(storeNameCapture.getValue());
        }

        static IPCFunction<ThreadCache> getCache(final InternalProcessorContext processorContext) {
            return internalProcessorContext -> processorContext::getCache;
        }

        static IPCFunction<ThreadCache> getCache() {
            return internalProcessorContext -> () -> null;
        }
    }

    /*
     * For internal use so we can mock an AbstractProcessorContext
     * without exposing the StateManager.
     */
    private static abstract class AbstractProcessorContextMock extends AbstractProcessorContext {

        AbstractProcessorContextMock(
                final TaskId taskId,
                final StreamsConfig config,
                final StreamsMetricsImpl metrics,
                final ThreadCache cache) {
            super(taskId, config, metrics, null, cache);
        }
    }

    private static AbstractProcessorContext mockAbstractProcessorContext(final TaskId taskId,
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

    private static class Restorer {

        private final Map<String, StateRestoreCallback> restoreCallbacks;

        Restorer(final Map<String, StateRestoreCallback> restoreCallbacks) {
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
