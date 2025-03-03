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
package org.apache.kafka.streams;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsNotStartedException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.errors.UnknownStateStoreException;
import org.apache.kafka.streams.internals.StreamsConfigUtils;
import org.apache.kafka.streams.internals.metrics.ClientMetrics;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StandbyUpdateListener;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.GlobalStreamThread;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.StreamsMetadataState;
import org.apache.kafka.streams.processor.internals.TopologyMetadata;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecordingTrigger;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockMetricsReporter;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singletonList;
import static org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.utils.TestUtils.waitForApplicationState;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(600)
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class KafkaStreamsTest {

    private static final int NUM_THREADS = 2;
    private static final String APPLICATION_ID = "appId-";
    private static final String CLIENT_ID = "test-client";
    private static final Duration DEFAULT_DURATION = Duration.ofSeconds(30);

    private MockClientSupplier supplier;
    private MockTime time;
    private Properties props;
    private MockAdminClient adminClient;
    private StateListenerStub streamsStateListener;
    
    @Mock
    private StreamThread streamThreadOne;
    @Mock
    private StreamThread streamThreadTwo;
    @Captor
    private ArgumentCaptor<StreamThread.StateListener> threadStateListenerCapture;

    private MockedStatic<ClientMetrics> clientMetricsMockedStatic;
    private MockedStatic<StreamThread> streamThreadMockedStatic;
    private MockedStatic<StreamsConfigUtils> streamsConfigUtils;

    private MockedConstruction<GlobalStreamThread> globalStreamThreadMockedConstruction;
    private MockedConstruction<Metrics> metricsMockedConstruction;

    public static class StateListenerStub implements KafkaStreams.StateListener {
        int numChanges = 0;
        KafkaStreams.State oldState;
        KafkaStreams.State newState;
        public Map<KafkaStreams.State, Long> mapStates = new HashMap<>();

        @Override
        public void onChange(final KafkaStreams.State newState,
                             final KafkaStreams.State oldState) {
            final long prevCount = mapStates.containsKey(newState) ? mapStates.get(newState) : 0;
            numChanges++;
            this.oldState = oldState;
            this.newState = newState;
            mapStates.put(newState, prevCount + 1);
        }
    }

    @BeforeEach
    public void before(final TestInfo testInfo) throws Exception {
        time = new MockTime();
        supplier = new MockClientSupplier();
        supplier.setCluster(Cluster.bootstrap(singletonList(new InetSocketAddress("localhost", 9999))));
        adminClient = (MockAdminClient) supplier.getAdmin(null);
        streamsStateListener = new StateListenerStub();
        props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID + safeUniqueTestName(testInfo));
        props.put(StreamsConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2018");
        props.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, NUM_THREADS);
    }

    @AfterEach
    public void tearDown() {
        if (clientMetricsMockedStatic != null)
            clientMetricsMockedStatic.close();
        if (streamThreadMockedStatic != null)
            streamThreadMockedStatic.close();
        if (globalStreamThreadMockedConstruction != null)
            globalStreamThreadMockedConstruction.close();
        if (metricsMockedConstruction != null)
            metricsMockedConstruction.close();
        if (streamsConfigUtils != null)
            streamsConfigUtils.close();
        if (adminClient != null)
            adminClient.close();
        reset(streamThreadOne, streamThreadTwo);
    }

    @SuppressWarnings("unchecked")
    private void prepareStreams() {
        // setup metrics
        metricsMockedConstruction = mockConstruction(Metrics.class, (mock, context) -> {
            assertEquals(4, context.arguments().size());
            final List<MetricsReporter> reporters = (List<MetricsReporter>) context.arguments().get(1);
            for (final MetricsReporter reporter : reporters) {
                reporter.init(Collections.emptyList());
            }

            doAnswer(invocation -> {
                for (final MetricsReporter reporter : reporters) {
                    reporter.close();
                }
                return null;
            }).when(mock).close();
        });

        clientMetricsMockedStatic = mockStatic(ClientMetrics.class);
        clientMetricsMockedStatic.when(ClientMetrics::version).thenReturn("1.56");
        clientMetricsMockedStatic.when(ClientMetrics::commitId).thenReturn("1a2b3c4d5e");
        ClientMetrics.addVersionMetric(any(StreamsMetricsImpl.class));
        ClientMetrics.addCommitIdMetric(any(StreamsMetricsImpl.class));
        ClientMetrics.addApplicationIdMetric(any(StreamsMetricsImpl.class), eq(APPLICATION_ID));
        ClientMetrics.addTopologyDescriptionMetric(any(StreamsMetricsImpl.class), any());
        ClientMetrics.addStateMetric(any(StreamsMetricsImpl.class), any());
        ClientMetrics.addNumAliveStreamThreadMetric(any(StreamsMetricsImpl.class), any());

        // setup stream threads
        streamThreadMockedStatic = mockStatic(StreamThread.class);
        streamThreadMockedStatic.when(() -> StreamThread.create(
                any(TopologyMetadata.class),
                any(StreamsConfig.class),
                any(KafkaClientSupplier.class),
                any(Admin.class),
                any(UUID.class),
                any(String.class),
                any(StreamsMetricsImpl.class),
                any(Time.class),
                any(StreamsMetadataState.class),
                anyLong(),
                any(StateDirectory.class),
                any(StateRestoreListener.class),
                any(StandbyUpdateListener.class),
                anyInt(),
                any(Runnable.class),
                any()
        )).thenReturn(streamThreadOne).thenReturn(streamThreadTwo);

        streamsConfigUtils = mockStatic(StreamsConfigUtils.class);
        streamsConfigUtils.when(() -> StreamsConfigUtils.processingMode(any(StreamsConfig.class))).thenReturn(StreamsConfigUtils.ProcessingMode.AT_LEAST_ONCE);
        streamsConfigUtils.when(() -> StreamsConfigUtils.eosEnabled(any(StreamsConfig.class))).thenReturn(false);
        streamsConfigUtils.when(() -> StreamsConfigUtils.totalCacheSize(any(StreamsConfig.class))).thenReturn(10 * 1024 * 1024L);

        // setup global threads
        final AtomicReference<GlobalStreamThread.State> globalThreadState = new AtomicReference<>(GlobalStreamThread.State.CREATED);

        globalStreamThreadMockedConstruction = mockConstruction(GlobalStreamThread.class,
                (mock, context) -> {
                    when(mock.state()).thenAnswer(invocation -> globalThreadState.get());
                    doNothing().when(mock).setStateListener(threadStateListenerCapture.capture());
                    doAnswer(invocation -> {
                        globalThreadState.set(GlobalStreamThread.State.RUNNING);
                        threadStateListenerCapture.getValue().onChange(mock,
                                GlobalStreamThread.State.RUNNING,
                                GlobalStreamThread.State.CREATED);
                        return null;
                    }).when(mock).start();
                    doAnswer(invocation -> {
                        supplier.restoreConsumer.close();

                        for (final MockProducer<byte[], byte[]> producer : supplier.producers) {
                            producer.close();
                        }
                        globalThreadState.set(GlobalStreamThread.State.DEAD);
                        threadStateListenerCapture.getValue().onChange(mock,
                                GlobalStreamThread.State.PENDING_SHUTDOWN,
                                GlobalStreamThread.State.RUNNING);
                        threadStateListenerCapture.getValue().onChange(mock,
                                GlobalStreamThread.State.DEAD,
                                GlobalStreamThread.State.PENDING_SHUTDOWN);
                        return null;
                    }).when(mock).shutdown();
                    when(mock.stillRunning()).thenReturn(globalThreadState.get() == GlobalStreamThread.State.RUNNING);
                });
    }

    private AtomicReference<StreamThread.State> prepareStreamThread(final StreamThread thread, final int threadId) {
        when(thread.getId()).thenReturn((long) threadId);
        final AtomicReference<StreamThread.State> state = new AtomicReference<>(StreamThread.State.CREATED);
        when(thread.state()).thenAnswer(invocation -> state.get());
        doNothing().when(thread).setStateListener(threadStateListenerCapture.capture());
        when(thread.getName()).thenReturn("processId-StreamThread-" + threadId);
        return state;
    }

    private void prepareConsumer(final StreamThread thread, final AtomicReference<StreamThread.State> state) {
        doAnswer(invocation -> {
            supplier.consumer.close();
            supplier.restoreConsumer.close();
            for (final MockProducer<byte[], byte[]> producer : supplier.producers) {
                producer.close();
            }
            state.set(StreamThread.State.DEAD);

            threadStateListenerCapture.getValue().onChange(thread, StreamThread.State.PENDING_SHUTDOWN, StreamThread.State.RUNNING);
            threadStateListenerCapture.getValue().onChange(thread, StreamThread.State.DEAD, StreamThread.State.PENDING_SHUTDOWN);
            return null;
        }).when(thread).shutdown();
    }

    private void prepareThreadLock(final StreamThread thread) {
        when(thread.getStateLock()).thenReturn(new Object());
    }

    private void prepareThreadState(final StreamThread thread, final AtomicReference<StreamThread.State> state) {
        doAnswer(invocation -> {
            state.set(StreamThread.State.STARTING);
            threadStateListenerCapture.getValue().onChange(thread,
                    StreamThread.State.STARTING,
                    StreamThread.State.CREATED);
            threadStateListenerCapture.getValue().onChange(thread,
                    StreamThread.State.PARTITIONS_REVOKED,
                    StreamThread.State.STARTING);
            threadStateListenerCapture.getValue().onChange(thread,
                    StreamThread.State.PARTITIONS_ASSIGNED,
                    StreamThread.State.PARTITIONS_REVOKED);
            threadStateListenerCapture.getValue().onChange(thread,
                    StreamThread.State.RUNNING,
                    StreamThread.State.PARTITIONS_ASSIGNED);
            return null;
        }).when(thread).start();
    }

    private void prepareTerminableThread(final StreamThread thread) throws InterruptedException {
        doAnswer(invocation -> {
            Thread.sleep(2000L);
            return null;
        }).when(thread).join();
    }

    @Test
    public void testShouldTransitToNotRunningIfCloseRightAfterCreated() {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.close();

            assertEquals(KafkaStreams.State.NOT_RUNNING, streams.state());
        }
    }

    @Test
    public void shouldInitializeTasksForLocalStateOnStart() {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);

        try (final MockedConstruction<StateDirectory> constructed = mockConstruction(StateDirectory.class,
                (mock, context) -> when(mock.initializeProcessId()).thenReturn(UUID.randomUUID()))) {
            try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
                assertEquals(1, constructed.constructed().size());
                final StateDirectory stateDirectory = constructed.constructed().get(0);
                verify(stateDirectory, times(0)).initializeStartupTasks(any(), any(), any());
                streams.start();
                verify(stateDirectory, times(1)).initializeStartupTasks(any(), any(), any());
            }
        }
    }

    @Test
    public void shouldCloseStartupTasksAfterFirstRebalance() throws Exception {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        try (final MockedConstruction<StateDirectory> constructed = mockConstruction(StateDirectory.class,
            (mock, context) -> when(mock.initializeProcessId()).thenReturn(UUID.randomUUID()))) {
            try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
                assertEquals(1, constructed.constructed().size());
                final StateDirectory stateDirectory = constructed.constructed().get(0);
                streams.setStateListener(streamsStateListener);
                streams.start();
                waitForCondition(() -> streams.state() == State.RUNNING, "Streams never started.");
                verify(stateDirectory, times(1)).closeStartupTasks();
            }
        }
    }

    @Test
    public void stateShouldTransitToRunningIfNonDeadThreadsBackToRunning() throws Exception {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.setStateListener(streamsStateListener);

            assertEquals(0, streamsStateListener.numChanges);
            assertEquals(KafkaStreams.State.CREATED, streams.state());

            streams.start();

            waitForCondition(
                () -> streamsStateListener.numChanges == 2,
                "Streams never started.");
            assertEquals(KafkaStreams.State.RUNNING, streams.state());
            waitForCondition(
                () -> streamsStateListener.numChanges == 2,
                "Streams never started.");
            assertEquals(KafkaStreams.State.RUNNING, streams.state());

            for (final StreamThread thread : streams.threads) {
                threadStateListenerCapture.getValue().onChange(
                    thread,
                    StreamThread.State.PARTITIONS_REVOKED,
                    StreamThread.State.RUNNING);
            }

            assertEquals(3, streamsStateListener.numChanges);
            assertEquals(KafkaStreams.State.REBALANCING, streams.state());

            for (final StreamThread thread : streams.threads) {
                threadStateListenerCapture.getValue().onChange(
                    thread,
                    StreamThread.State.PARTITIONS_ASSIGNED,
                    StreamThread.State.PARTITIONS_REVOKED);
            }

            assertEquals(3, streamsStateListener.numChanges);
            assertEquals(KafkaStreams.State.REBALANCING, streams.state());

            threadStateListenerCapture.getValue().onChange(
                streams.threads.get(NUM_THREADS - 1),
                StreamThread.State.PENDING_SHUTDOWN,
                StreamThread.State.PARTITIONS_ASSIGNED);

            threadStateListenerCapture.getValue().onChange(
                streams.threads.get(NUM_THREADS - 1),
                StreamThread.State.DEAD,
                StreamThread.State.PENDING_SHUTDOWN);

            assertEquals(3, streamsStateListener.numChanges);
            assertEquals(KafkaStreams.State.REBALANCING, streams.state());

            for (final StreamThread thread : streams.threads) {
                if (thread != streams.threads.get(NUM_THREADS - 1)) {
                    threadStateListenerCapture.getValue().onChange(
                        thread,
                        StreamThread.State.RUNNING,
                        StreamThread.State.PARTITIONS_ASSIGNED);
                }
            }

            assertEquals(4, streamsStateListener.numChanges);
            assertEquals(KafkaStreams.State.RUNNING, streams.state());

            streams.close();

            waitForCondition(
                () -> streamsStateListener.numChanges == 6,
                "Streams never closed.");
            assertEquals(KafkaStreams.State.NOT_RUNNING, streams.state());
        }
    }

    @Test
    public void shouldCleanupResourcesOnCloseWithoutPreviousStart() throws Exception {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareConsumer(streamThreadOne, state1);
        prepareConsumer(streamThreadTwo, state2);
        final StreamsBuilder builder = getBuilderWithSource();
        builder.globalTable("anyTopic");

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KafkaStreams.class);
            final KafkaStreams streams = new KafkaStreams(builder.build(), props, supplier, time)) {
            streams.close();

            waitForCondition(
                () -> streams.state() == KafkaStreams.State.NOT_RUNNING,
                "Streams never stopped.");

            assertThat(appender.getMessages(), not(hasItem(containsString("ERROR"))));
        }

        assertTrue(supplier.consumer.closed());
        assertTrue(supplier.restoreConsumer.closed());
        for (final MockProducer<byte[], byte[]> p : supplier.producers) {
            assertTrue(p.closed());
        }
    }

    @Test
    public void testStateThreadClose() throws Exception {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        prepareConsumer(streamThreadOne, state1);
        prepareConsumer(streamThreadTwo, state2);
        prepareThreadLock(streamThreadOne);
        prepareThreadLock(streamThreadTwo);
        // make sure we have the global state thread running too
        final StreamsBuilder builder = getBuilderWithSource();
        builder.globalTable("anyTopic");

        try (final KafkaStreams streams = new KafkaStreams(builder.build(), props, supplier, time)) {
            assertEquals(NUM_THREADS, streams.threads.size());
            assertEquals(streams.state(), KafkaStreams.State.CREATED);

            streams.start();
            waitForCondition(
                () -> streams.state() == KafkaStreams.State.RUNNING,
                "Streams never started.");

            for (int i = 0; i < NUM_THREADS; i++) {
                final StreamThread tmpThread = streams.threads.get(i);
                tmpThread.shutdown();
                waitForCondition(() -> tmpThread.state() == StreamThread.State.DEAD,
                    "Thread never stopped.");
                streams.threads.get(i).join();
            }
            waitForCondition(
                () -> streams.metadataForLocalThreads().stream().allMatch(t -> t.threadState().equals("DEAD")),
                "Streams never stopped"
            );
            streams.close();

            waitForCondition(
                () -> streams.state() == KafkaStreams.State.NOT_RUNNING,
                "Streams never stopped.");

            assertNull(streams.globalStreamThread);
        }
    }

    @Test
    public void testStateGlobalThreadClose() throws Exception {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        // make sure we have the global state thread running too
        final StreamsBuilder builder = getBuilderWithSource();
        builder.globalTable("anyTopic");

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KafkaStreams.class);
            final KafkaStreams streams = new KafkaStreams(builder.build(), props, supplier, time)) {
            streams.start();
            waitForCondition(
                () -> streams.state() == KafkaStreams.State.RUNNING,
                "Streams never started.");

            final GlobalStreamThread globalStreamThread = streams.globalStreamThread;
            globalStreamThread.shutdown();
            waitForCondition(
                () -> globalStreamThread.state() == GlobalStreamThread.State.DEAD,
                "Thread never stopped.");

            // shutting down the global thread from "external" will yield an error in KafkaStreams
            waitForCondition(
                () -> streams.state() == KafkaStreams.State.ERROR,
                "Thread never stopped."
            );

            streams.close();
            assertEquals(streams.state(), KafkaStreams.State.ERROR, "KafkaStreams should remain in ERROR state after close.");
            assertThat(appender.getMessages(), hasItem(containsString("State transition from RUNNING to PENDING_ERROR")));
            assertThat(appender.getMessages(), hasItem(containsString("State transition from PENDING_ERROR to ERROR")));
            assertThat(appender.getMessages(), hasItem(containsString("Streams client is already in the terminal ERROR state")));
        }
    }

    @Test
    public void testInitializesAndDestroysMetricsReporters() {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        final int oldInitCount = MockMetricsReporter.INIT_COUNT.get();

        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            final int newInitCount = MockMetricsReporter.INIT_COUNT.get();
            final int initDiff = newInitCount - oldInitCount;
            assertEquals(1, initDiff, "some reporters including MockMetricsReporter should be initialized by calling on construction");

            streams.start();
            final int oldCloseCount = MockMetricsReporter.CLOSE_COUNT.get();
            streams.close();
            assertEquals(streams.state(), KafkaStreams.State.NOT_RUNNING);
            assertEquals(oldCloseCount + initDiff, MockMetricsReporter.CLOSE_COUNT.get());
        }
    }

    @Test
    public void testCloseIsIdempotent() {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.close();
            final int closeCount = MockMetricsReporter.CLOSE_COUNT.get();

            streams.close();
            assertEquals(closeCount, MockMetricsReporter.CLOSE_COUNT.get(), "subsequent close() calls should do nothing");
        }
    }

    @Test
    public void testPauseResume() {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            streams.pause();
            assertTrue(streams.isPaused());
            streams.resume();
            assertFalse(streams.isPaused());
        }
    }

    @Test
    public void testStartingPaused() {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        // This test shows that a KafkaStreams instance can be started "paused"
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.pause();
            streams.start();
            assertTrue(streams.isPaused());
            streams.resume();
            assertFalse(streams.isPaused());
        }
    }

    @Test
    public void testShowPauseResumeAreIdempotent() {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        // This test shows that a KafkaStreams instance can be started "paused"
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            streams.pause();
            assertTrue(streams.isPaused());
            streams.pause();
            assertTrue(streams.isPaused());
            streams.resume();
            assertFalse(streams.isPaused());
            streams.resume();
            assertFalse(streams.isPaused());
        }
    }

    @Test
    public void shouldAddThreadWhenRunning() throws Exception {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            final int oldSize = streams.threads.size();
            waitForCondition(() -> streams.state() == KafkaStreams.State.RUNNING, 15L, "wait until running");
            assertThat(streams.addStreamThread(), equalTo(Optional.of("processId-StreamThread-" + 2)));
            assertThat(streams.threads.size(), equalTo(oldSize + 1));
        }
    }

    @Test
    public void shouldNotAddThreadWhenCreated() {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            final int oldSize = streams.threads.size();
            assertThat(streams.addStreamThread(), equalTo(Optional.empty()));
            assertThat(streams.threads.size(), equalTo(oldSize));
        }
    }

    @Test
    public void shouldNotAddThreadWhenClosed() {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            final int oldSize = streams.threads.size();
            streams.close();
            assertThat(streams.addStreamThread(), equalTo(Optional.empty()));
            assertThat(streams.threads.size(), equalTo(oldSize));
        }
    }

    @Test
    public void shouldNotAddThreadWhenError() {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        // make sure we have the global state thread running too
        final StreamsBuilder builder = getBuilderWithSource();
        builder.globalTable("anyTopic");
        try (final KafkaStreams streams = new KafkaStreams(builder.build(), props, supplier, time)) {
            final int oldSize = streams.threads.size();
            streams.start();
            streams.globalStreamThread.shutdown();
            assertThat(streams.addStreamThread(), equalTo(Optional.empty()));
            assertThat(streams.threads.size(), equalTo(oldSize));
        }
    }

    @Test
    public void shouldNotReturnDeadThreads() {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        prepareThreadLock(streamThreadOne);
        prepareThreadLock(streamThreadTwo);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            streamThreadOne.shutdown();
            final Set<ThreadMetadata> threads = streams.metadataForLocalThreads();
            assertThat(threads.size(), equalTo(1));
            assertThat(threads, hasItem(streamThreadTwo.threadMetadata()));
        }
    }

    @Test
    public void shouldRemoveThread() throws Exception {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        when(streamThreadOne.groupInstanceID()).thenReturn(Optional.empty());
        when(streamThreadOne.waitOnThreadState(isA(StreamThread.State.class), anyLong())).thenReturn(true);
        when(streamThreadOne.isThreadAlive()).thenReturn(true);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            final int oldSize = streams.threads.size();
            waitForCondition(() -> streams.state() == KafkaStreams.State.RUNNING, 15L,
                "Kafka Streams client did not reach state RUNNING");
            assertThat(streams.removeStreamThread(), equalTo(Optional.of("processId-StreamThread-" + 1)));
            assertThat(streams.threads.size(), equalTo(oldSize - 1));
        }
    }

    @Test
    public void shouldNotRemoveThreadWhenNotRunning() {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        try (final KafkaStreams streams =
                     new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            assertThat(streams.removeStreamThread(), equalTo(Optional.empty()));
            assertThat(streams.threads.size(), equalTo(1));
        }
    }

    @Test
    public void testCannotStartOnceClosed() {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            streams.close();
            try {
                streams.start();
                fail("Should have throw IllegalStateException");
            } catch (final IllegalStateException expected) {
                // this is ok
            }
        }
    }

    @Test
    public void shouldNotSetGlobalRestoreListenerAfterStarting() {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            try {
                streams.setGlobalStateRestoreListener(null);
                fail("Should throw an IllegalStateException");
            } catch (final IllegalStateException e) {
                // expected
            }
        }
    }

    @Test
    public void shouldThrowExceptionSettingUncaughtExceptionHandlerNotInCreateState() {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            assertThrows(IllegalStateException.class, () -> streams.setUncaughtExceptionHandler((StreamsUncaughtExceptionHandler) null));
        }
    }

    @Test
    public void shouldThrowExceptionSettingStreamsUncaughtExceptionHandlerNotInCreateState() {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            assertThrows(IllegalStateException.class, () -> streams.setUncaughtExceptionHandler((StreamsUncaughtExceptionHandler) null));
        }

    }
    @Test
    public void shouldThrowNullPointerExceptionSettingStreamsUncaughtExceptionHandlerIfNull() {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            assertThrows(NullPointerException.class, () -> streams.setUncaughtExceptionHandler((StreamsUncaughtExceptionHandler) null));
        }
    }

    @Test
    public void shouldThrowExceptionSettingStateListenerNotInCreateState() {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            try {
                streams.setStateListener(null);
                fail("Should throw IllegalStateException");
            } catch (final IllegalStateException e) {
                // expected
            }
        }
    }

    @Test
    public void shouldAllowCleanupBeforeStartAndAfterClose() {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            try {
                streams.cleanUp();
                streams.start();
            } finally {
                streams.close();
                streams.cleanUp();
            }
        }
    }

    @Test
    public void shouldThrowOnCleanupWhileRunning() throws Exception {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            waitForCondition(
                () -> streams.state() == KafkaStreams.State.RUNNING,
                "Streams never started.");

            try {
                streams.cleanUp();
                fail("Should have thrown IllegalStateException");
            } catch (final IllegalStateException expected) {
                assertEquals("Cannot clean up while running.", expected.getMessage());
            }
        }
    }

    @Test
    public void shouldThrowOnCleanupWhilePaused() throws Exception {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            waitForCondition(
                () -> streams.state() == KafkaStreams.State.RUNNING,
                "Streams never started.");

            streams.pause();
            waitForCondition(
                streams::isPaused,
                "Streams did not pause.");

            assertThrows(IllegalStateException.class, streams::cleanUp, "Cannot clean up while running.");
        }
    }

    @Test
    public void shouldThrowOnCleanupWhileShuttingDown() throws Exception {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        prepareTerminableThread(streamThreadOne);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            waitForCondition(
                () -> streams.state() == KafkaStreams.State.RUNNING,
                "Streams never started.");

            streams.close(Duration.ZERO);
            assertThat(streams.state() == State.PENDING_SHUTDOWN, equalTo(true));
            assertThrows(IllegalStateException.class, streams::cleanUp);
            assertThat(streams.state() == State.PENDING_SHUTDOWN, equalTo(true));
        }
    }

    @Test
    public void shouldThrowOnCleanupWhileShuttingDownStreamClosedWithCloseOptionLeaveGroupFalse() throws Exception {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        prepareTerminableThread(streamThreadOne);
        final MockClientSupplier mockClientSupplier = spy(MockClientSupplier.class);

        when(mockClientSupplier.getAdmin(any())).thenReturn(adminClient);

        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, mockClientSupplier, time)) {
            streams.start();
            waitForCondition(
                () -> streams.state() == KafkaStreams.State.RUNNING,
                "Streams never started.");

            final KafkaStreams.CloseOptions closeOptions = new KafkaStreams.CloseOptions();
            closeOptions.timeout(Duration.ZERO);
            closeOptions.leaveGroup(true);

            streams.close(closeOptions);
            assertThat(streams.state() == State.PENDING_SHUTDOWN, equalTo(true));
            assertThrows(IllegalStateException.class, streams::cleanUp);
            assertThat(streams.state() == State.PENDING_SHUTDOWN, equalTo(true));
        }
    }

    @Test
    public void shouldThrowOnCleanupWhileShuttingDownStreamClosedWithCloseOptionLeaveGroupTrue() throws Exception {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        prepareTerminableThread(streamThreadOne);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            waitForCondition(
                () -> streams.state() == KafkaStreams.State.RUNNING,
                "Streams never started.");

            final KafkaStreams.CloseOptions closeOptions = new KafkaStreams.CloseOptions();
            closeOptions.timeout(Duration.ZERO);

            streams.close(closeOptions);
            assertThat(streams.state() == State.PENDING_SHUTDOWN, equalTo(true));
            assertThrows(IllegalStateException.class, streams::cleanUp);
            assertThat(streams.state() == State.PENDING_SHUTDOWN, equalTo(true));
        }
    }

    @Test
    public void shouldNotGetAllTasksWhenNotRunning() throws Exception {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            assertThrows(StreamsNotStartedException.class, streams::metadataForAllStreamsClients);
            streams.start();
            waitForApplicationState(Collections.singletonList(streams), KafkaStreams.State.RUNNING, DEFAULT_DURATION);
            streams.close();
            waitForApplicationState(Collections.singletonList(streams), KafkaStreams.State.NOT_RUNNING, DEFAULT_DURATION);
            assertThrows(IllegalStateException.class, streams::metadataForAllStreamsClients);
        }
    }

    @Test
    public void shouldNotGetAllTasksWithStoreWhenNotRunning() throws Exception {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            assertThrows(StreamsNotStartedException.class, () -> streams.streamsMetadataForStore("store"));
            streams.start();
            waitForApplicationState(Collections.singletonList(streams), KafkaStreams.State.RUNNING, DEFAULT_DURATION);
            streams.close();
            waitForApplicationState(Collections.singletonList(streams), KafkaStreams.State.NOT_RUNNING, DEFAULT_DURATION);
            assertThrows(IllegalStateException.class, () -> streams.streamsMetadataForStore("store"));
        }
    }

    @Test
    public void shouldNotGetQueryMetadataWithSerializerWhenNotRunningOrRebalancing() throws Exception {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            assertThrows(StreamsNotStartedException.class, () -> streams.queryMetadataForKey("store", "key", new StringSerializer()));
            streams.start();
            waitForApplicationState(Collections.singletonList(streams), KafkaStreams.State.RUNNING, DEFAULT_DURATION);
            streams.close();
            waitForApplicationState(Collections.singletonList(streams), KafkaStreams.State.NOT_RUNNING, DEFAULT_DURATION);
            assertThrows(IllegalStateException.class, () -> streams.queryMetadataForKey("store", "key", new StringSerializer()));
        }
    }

    @Test
    public void shouldGetQueryMetadataWithSerializerWhenRunningOrRebalancing() {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);

        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            assertEquals(KeyQueryMetadata.NOT_AVAILABLE, streams.queryMetadataForKey("store", "key", new StringSerializer()));
        }
    }

    @Test
    public void shouldNotGetQueryMetadataWithPartitionerWhenNotRunningOrRebalancing() throws Exception {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            assertThrows(StreamsNotStartedException.class, () -> streams.queryMetadataForKey("store", "key", (topic, key, value, numPartitions) -> Optional.of(Collections.singleton(0))));
            streams.start();
            waitForApplicationState(Collections.singletonList(streams), KafkaStreams.State.RUNNING, DEFAULT_DURATION);
            streams.close();
            waitForApplicationState(Collections.singletonList(streams), KafkaStreams.State.NOT_RUNNING, DEFAULT_DURATION);
            assertThrows(IllegalStateException.class, () -> streams.queryMetadataForKey("store", "key", (topic, key, value, numPartitions) -> Optional.of(Collections.singleton(0))));
        }
    }

    @Test
    public void shouldThrowUnknownStateStoreExceptionWhenStoreNotExist() throws Exception {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            waitForApplicationState(Collections.singletonList(streams), KafkaStreams.State.RUNNING, DEFAULT_DURATION);
            assertThrows(UnknownStateStoreException.class, () -> streams.store(StoreQueryParameters.fromNameAndType("unknown-store", keyValueStore())));
        }
    }

    @Test
    public void shouldNotGetStoreWhenWhenNotRunningOrRebalancing() throws Exception {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            assertThrows(StreamsNotStartedException.class, () -> streams.store(StoreQueryParameters.fromNameAndType("store", keyValueStore())));
            streams.start();
            waitForApplicationState(Collections.singletonList(streams), KafkaStreams.State.RUNNING, DEFAULT_DURATION);
            streams.close();
            waitForApplicationState(Collections.singletonList(streams), KafkaStreams.State.NOT_RUNNING, DEFAULT_DURATION);
            assertThrows(IllegalStateException.class, () -> streams.store(StoreQueryParameters.fromNameAndType("store", keyValueStore())));
        }
    }

    @Test
    public void shouldReturnEmptyLocalStorePartitionLags() {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);

        // Mock all calls made to compute the offset lags,
        final KafkaFutureImpl<Map<TopicPartition, ListOffsetsResultInfo>> allFuture = new KafkaFutureImpl<>();
        allFuture.complete(Collections.emptyMap());

        final MockAdminClient mockAdminClient = spy(MockAdminClient.class);
        final MockClientSupplier mockClientSupplier = spy(MockClientSupplier.class);
        when(mockClientSupplier.getAdmin(any())).thenReturn(mockAdminClient);

        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, mockClientSupplier, time)) {
            streams.start();
            assertEquals(0, streams.allLocalStorePartitionLags().size());
        }
    }

    @Test
    public void shouldReturnFalseOnCloseWhenThreadsHaventTerminated() throws Exception {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);
        prepareTerminableThread(streamThreadOne);

        // do not use mock time so that it can really elapse
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier)) {
            assertFalse(streams.close(Duration.ofMillis(10L)));
        }
    }

    @Test
    public void shouldThrowOnNegativeTimeoutForClose() throws Exception {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);
        prepareTerminableThread(streamThreadOne);

        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            assertThrows(IllegalArgumentException.class, () -> streams.close(Duration.ofMillis(-1L)));
        }
    }

    @Test
    public void shouldNotBlockInCloseForZeroDuration() throws Exception {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);
        prepareTerminableThread(streamThreadOne);

        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            // with mock time that does not elapse, close would not return if it ever waits on the state transition
            assertFalse(streams.close(Duration.ZERO));
        }
    }

    @Test
    public void shouldReturnFalseOnCloseWithCloseOptionWithLeaveGroupFalseWhenThreadsHaventTerminated() throws Exception {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);
        prepareTerminableThread(streamThreadOne);

        final KafkaStreams.CloseOptions closeOptions = new KafkaStreams.CloseOptions();
        closeOptions.timeout(Duration.ofMillis(10L));
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier)) {
            assertFalse(streams.close(closeOptions));
        }
    }

    @Test
    public void shouldThrowOnNegativeTimeoutForCloseWithCloseOptionLeaveGroupFalse() throws Exception {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);
        prepareTerminableThread(streamThreadOne);

        final KafkaStreams.CloseOptions closeOptions = new KafkaStreams.CloseOptions();
        closeOptions.timeout(Duration.ofMillis(-1L));
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            assertThrows(IllegalArgumentException.class, () -> streams.close(closeOptions));
        }
    }

    @Test
    public void shouldNotBlockInCloseWithCloseOptionLeaveGroupFalseForZeroDuration() throws Exception {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);
        prepareTerminableThread(streamThreadOne);

        final KafkaStreams.CloseOptions closeOptions = new KafkaStreams.CloseOptions();
        closeOptions.timeout(Duration.ZERO);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier)) {
            assertFalse(streams.close(closeOptions));
        }
    }

    @Test
    public void shouldReturnFalseOnCloseWithCloseOptionWithLeaveGroupTrueWhenThreadsHaventTerminated() throws Exception {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);
        prepareTerminableThread(streamThreadOne);

        final MockClientSupplier mockClientSupplier = spy(MockClientSupplier.class);

        when(mockClientSupplier.getAdmin(any())).thenReturn(adminClient);

        final KafkaStreams.CloseOptions closeOptions = new KafkaStreams.CloseOptions();
        closeOptions.timeout(Duration.ofMillis(10L));
        closeOptions.leaveGroup(true);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, mockClientSupplier)) {
            assertFalse(streams.close(closeOptions));
        }
    }

    @Test
    public void shouldThrowOnNegativeTimeoutForCloseWithCloseOptionLeaveGroupTrue() throws Exception {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);
        prepareTerminableThread(streamThreadOne);

        final MockClientSupplier mockClientSupplier = spy(MockClientSupplier.class);
        when(mockClientSupplier.getAdmin(any())).thenReturn(adminClient);

        final KafkaStreams.CloseOptions closeOptions = new KafkaStreams.CloseOptions();
        closeOptions.timeout(Duration.ofMillis(-1L));
        closeOptions.leaveGroup(true);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, mockClientSupplier, time)) {
            assertThrows(IllegalArgumentException.class, () -> streams.close(closeOptions));
        }
    }

    @Test
    public void shouldNotBlockInCloseWithCloseOptionLeaveGroupTrueForZeroDuration() throws Exception {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);
        prepareTerminableThread(streamThreadOne);

        final MockClientSupplier mockClientSupplier = spy(MockClientSupplier.class);

        when(mockClientSupplier.getAdmin(any())).thenReturn(adminClient);

        final KafkaStreams.CloseOptions closeOptions = new KafkaStreams.CloseOptions();
        closeOptions.timeout(Duration.ZERO);
        closeOptions.leaveGroup(true);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, mockClientSupplier)) {
            assertFalse(streams.close(closeOptions));
        }
    }

    @Test
    public void shouldTriggerRecordingOfRocksDBMetricsIfRecordingLevelIsDebug() throws Exception {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);
        prepareTerminableThread(streamThreadOne);

        try (final MockedStatic<Executors> executorsMockedStatic = mockStatic(Executors.class)) {
            final ScheduledExecutorService cleanupSchedule = mock(ScheduledExecutorService.class);
            final ScheduledExecutorService rocksDBMetricsRecordingTriggerThread = mock(ScheduledExecutorService.class);

            executorsMockedStatic.when(() -> Executors.newSingleThreadScheduledExecutor(
                    any(ThreadFactory.class))).thenReturn(cleanupSchedule, rocksDBMetricsRecordingTriggerThread);

            final StreamsBuilder builder = new StreamsBuilder();
            builder.table("topic", Materialized.as("store"));
            props.setProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, RecordingLevel.DEBUG.name());

            try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
                streams.start();
            }

            executorsMockedStatic.verify(() -> Executors.newSingleThreadScheduledExecutor(any(ThreadFactory.class)),
                times(2));
            verify(rocksDBMetricsRecordingTriggerThread).scheduleAtFixedRate(any(RocksDBMetricsRecordingTrigger.class),
                eq(0L), eq(1L), eq(TimeUnit.MINUTES));
            verify(rocksDBMetricsRecordingTriggerThread).shutdownNow();
        }
    }

    @Test
    public void shouldGetClientSupplierFromConfigForConstructor() throws Exception {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);
        prepareTerminableThread(streamThreadOne);
        prepareTerminableThread(streamThreadTwo);

        final StreamsConfig config = new StreamsConfig(props);
        final StreamsConfig mockConfig = spy(config);
        when(mockConfig.getKafkaClientSupplier()).thenReturn(supplier);

        try (final KafkaStreams ignored = new KafkaStreams(getBuilderWithSource().build(), mockConfig)) {
            // no-op
        }
        // It's called once in above when mock
        verify(mockConfig, times(2)).getKafkaClientSupplier();
    }

    @Test
    public void shouldGetClientSupplierFromConfigForConstructorWithTime() throws Exception {
        prepareStreams();
        final AtomicReference<StreamThread.State> state1 = prepareStreamThread(streamThreadOne, 1);
        final AtomicReference<StreamThread.State> state2 = prepareStreamThread(streamThreadTwo, 2);
        prepareThreadState(streamThreadOne, state1);
        prepareThreadState(streamThreadTwo, state2);

        final StreamsConfig config = new StreamsConfig(props);
        final StreamsConfig mockConfig = spy(config);
        when(mockConfig.getKafkaClientSupplier()).thenReturn(supplier);

        try (final KafkaStreams ignored = new KafkaStreams(getBuilderWithSource().build(), mockConfig, time)) {
            // no-op
        }
        // It's called once in above when mock
        verify(mockConfig, times(2)).getKafkaClientSupplier();
    }

    @Test
    public void shouldUseProvidedClientSupplier() throws Exception {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);
        prepareTerminableThread(streamThreadOne);
        prepareTerminableThread(streamThreadTwo);

        final StreamsConfig config = new StreamsConfig(props);
        final StreamsConfig mockConfig = spy(config);

        try (final KafkaStreams ignored = new KafkaStreams(getBuilderWithSource().build(), mockConfig, supplier)) {
            // no-op
        }
        // It's called once in above when mock
        verify(mockConfig, times(0)).getKafkaClientSupplier();
    }

    @Test
    public void shouldNotTriggerRecordingOfRocksDBMetricsIfRecordingLevelIsInfo() {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);

        try (final MockedStatic<Executors> executorsMockedStatic = mockStatic(Executors.class)) {
            final ScheduledExecutorService cleanupSchedule = mock(ScheduledExecutorService.class);
            executorsMockedStatic.when(() ->
                    Executors.newSingleThreadScheduledExecutor(any(ThreadFactory.class))).thenReturn(cleanupSchedule);

            final StreamsBuilder builder = new StreamsBuilder();
            builder.table("topic", Materialized.as("store"));
            props.setProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, RecordingLevel.INFO.name());

            try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
                streams.start();
            }
            executorsMockedStatic.verify(() -> Executors.newSingleThreadScheduledExecutor(any(ThreadFactory.class)));
        }
    }

    @Test
    public void shouldCleanupOldStateDirs() {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);

        try (final MockedStatic<Executors> executorsMockedStatic = mockStatic(Executors.class)) {
            final ScheduledExecutorService cleanupSchedule = mock(ScheduledExecutorService.class);
            executorsMockedStatic.when(() -> Executors.newSingleThreadScheduledExecutor(
                any(ThreadFactory.class)
            )).thenReturn(cleanupSchedule);

            try (MockedConstruction<StateDirectory> ignored = mockConstruction(StateDirectory.class,
                (mock, context) -> when(mock.initializeProcessId()).thenReturn(UUID.randomUUID()))) {
                props.setProperty(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, "1");
                final StreamsBuilder builder = new StreamsBuilder();
                builder.table("topic", Materialized.as("store"));

                try (final KafkaStreams streams = new KafkaStreams(builder.build(), props, supplier, time)) {
                    streams.start();
                }
            }

            verify(cleanupSchedule).scheduleAtFixedRate(any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.MILLISECONDS));
            verify(cleanupSchedule).shutdownNow();
        }
    }

    @Test
    public void statelessTopologyShouldNotCreateStateDirectory(final TestInfo testInfo) {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);

        final String safeTestName = safeUniqueTestName(testInfo);
        final String inputTopic = safeTestName + "-input";
        final String outputTopic = safeTestName + "-output";
        final Topology topology = new Topology();
        topology.addSource("source", new StringDeserializer(), new StringDeserializer(), inputTopic)
                .addProcessor("process", () -> new Processor<String, String, String, String>() {
                    private ProcessorContext<String, String> context;

                    @Override
                    public void init(final ProcessorContext<String, String> context) {
                        this.context = context;
                    }

                    @Override
                    public void process(final Record<String, String> record) {
                        if (record.value().length() % 2 == 0) {
                            context.forward(record.withValue(record.key() + record.value()));
                        }
                    }
                }, "source")
                .addSink("sink", outputTopic, new StringSerializer(), new StringSerializer(), "process");
        startStreamsAndCheckDirExists(topology, false);
    }

    @Test
    public void inMemoryStatefulTopologyShouldNotCreateStateDirectory(final TestInfo testInfo) {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);

        final String safeTestName = safeUniqueTestName(testInfo);
        final String inputTopic = safeTestName + "-input";
        final String outputTopic = safeTestName + "-output";
        final String globalTopicName = safeTestName + "-global";
        final String storeName = safeTestName + "-counts";
        final String globalStoreName = safeTestName + "-globalStore";
        final Topology topology = getStatefulTopology(inputTopic, outputTopic, globalTopicName, storeName, globalStoreName, false);
        startStreamsAndCheckDirExists(topology, false);
    }

    @Test
    public void statefulTopologyShouldCreateStateDirectory(final TestInfo testInfo) {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);

        final String safeTestName = safeUniqueTestName(testInfo);
        final String inputTopic = safeTestName + "-input";
        final String outputTopic = safeTestName + "-output";
        final String globalTopicName = safeTestName + "-global";
        final String storeName = safeTestName + "-counts";
        final String globalStoreName = safeTestName + "-globalStore";
        final Topology topology = getStatefulTopology(inputTopic, outputTopic, globalTopicName, storeName, globalStoreName, true);
        startStreamsAndCheckDirExists(topology, true);
    }

    @Test
    public void shouldThrowTopologyExceptionOnEmptyTopology() {
        prepareStreams();
        try (final KafkaStreams ignored = new KafkaStreams(new StreamsBuilder().build(), props, supplier, time)) {
            fail("Should have thrown TopologyException");
        } catch (final TopologyException e) {
            assertThat(
                e.getMessage(),
                equalTo("Invalid topology: Topology has no stream threads and no global threads, " +
                            "must subscribe to at least one source topic or global table."));
        }
    }

    @Test
    public void shouldNotCreateStreamThreadsForGlobalOnlyTopology() {
        prepareStreams();
        final StreamsBuilder builder = new StreamsBuilder();
        builder.globalTable("anyTopic");
        try (final KafkaStreams streams = new KafkaStreams(builder.build(), props, supplier, time)) {
            assertThat(streams.threads.size(), equalTo(0));
        }
    }

    @Test
    public void shouldTransitToRunningWithGlobalOnlyTopology() throws Exception {
        prepareStreams();
        final StreamsBuilder builder = new StreamsBuilder();
        builder.globalTable("anyTopic");
        try (final KafkaStreams streams = new KafkaStreams(builder.build(), props, supplier, time)) {

            assertThat(streams.threads.size(), equalTo(0));
            assertEquals(streams.state(), KafkaStreams.State.CREATED);

            streams.start();
            waitForCondition(
                () -> streams.state() == KafkaStreams.State.RUNNING,
                "Streams never started, state is " + streams.state());

            streams.close();

            waitForCondition(
                () -> streams.state() == KafkaStreams.State.NOT_RUNNING,
                "Streams never stopped.");
        }
    }

    @Test
    public void shouldThrowOnClientInstanceIdsWithNegativeTimeout() {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);

        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> streams.clientInstanceIds(Duration.ofMillis(-1L))
            );
            assertThat(
                error.getMessage(),
                equalTo("The timeout cannot be negative.")
            );
        }
    }

    @Test
    public void shouldThrowOnClientInstanceIdsWhenNotStarted() {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);

        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            final IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> streams.clientInstanceIds(Duration.ZERO)
            );
            assertThat(
                error.getMessage(),
                equalTo("KafkaStreams has not been started, you can retry after calling start().")
            );
        }
    }

    @Test
    public void shouldThrowOnClientInstanceIdsWhenClosed() {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);

        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.close();

            final IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> streams.clientInstanceIds(Duration.ZERO)
            );
            assertThat(
                error.getMessage(),
                equalTo("KafkaStreams has been stopped (NOT_RUNNING).")
            );
        }
    }

    @Test
    public void shouldThrowStreamsExceptionWhenAdminNotInitialized() {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);

        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();

            final StreamsException error = assertThrows(
                StreamsException.class,
                () -> streams.clientInstanceIds(Duration.ZERO)
            );
            assertThat(
                error.getMessage(),
                equalTo("Could not retrieve admin client instance id.")
            );

            final Throwable cause = error.getCause();
            assertThat(cause, instanceOf(UnsupportedOperationException.class));
            assertThat(
                cause.getMessage(),
                equalTo("clientInstanceId not set")
            );
        }
    }

    @Test
    public void shouldNotCrashButThrowLaterIfAdminTelemetryDisabled() {
        prepareStreams();
        adminClient.disableTelemetry();
        // set threads to zero to simplify set setup
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 0);

        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();

            final ClientInstanceIds clientInstanceIds = streams.clientInstanceIds(Duration.ZERO);

            final IllegalStateException error = assertThrows(
                IllegalStateException.class,
                clientInstanceIds::adminInstanceId
            );
            assertThat(
                error.getMessage(),
                equalTo("Telemetry is not enabled on the admin client. Set config `enable.metrics.push` to `true`.")
            );
        }
    }

    @Test
    public void shouldThrowTimeExceptionWhenAdminTimesOut() {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);

        adminClient.setClientInstanceId(Uuid.randomUuid());
        adminClient.injectTimeoutException(1);

        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();

            assertThrows(
                TimeoutException.class,
                () -> streams.clientInstanceIds(Duration.ZERO)
            );
        }
    }

    @Test
    public void shouldReturnAdminInstanceID() {
        prepareStreams();
        final Uuid instanceId = Uuid.randomUuid();
        adminClient.setClientInstanceId(instanceId);
        // set threads to zero to simplify set setup
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 0);

        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();

            assertThat(
                streams.clientInstanceIds(Duration.ZERO).adminInstanceId(),
                equalTo(instanceId)
            );
        }
    }

    @Test
    public void shouldReturnProducerAndConsumerInstanceIds() {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        final Uuid mainConsumerInstanceId = Uuid.randomUuid();
        final Uuid producerInstanceId = Uuid.randomUuid();
        final KafkaFutureImpl<Uuid> consumerFuture = new KafkaFutureImpl<>();
        final KafkaFutureImpl<Uuid> producerFuture = new KafkaFutureImpl<>();
        consumerFuture.complete(mainConsumerInstanceId);
        producerFuture.complete(producerInstanceId);
        final Uuid adminInstanceId = Uuid.randomUuid();
        adminClient.setClientInstanceId(adminInstanceId);
        
        final Map<String, KafkaFuture<Uuid>> expectedClientIds = Map.of("main-consumer", consumerFuture, "some-thread-producer", producerFuture);
        when(streamThreadOne.clientInstanceIds(any())).thenReturn(expectedClientIds);

        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            final ClientInstanceIds clientInstanceIds = streams.clientInstanceIds(Duration.ZERO);
            assertThat(clientInstanceIds.consumerInstanceIds().size(), equalTo(1));
            assertThat(clientInstanceIds.consumerInstanceIds().get("main-consumer"), equalTo(mainConsumerInstanceId));
            assertThat(clientInstanceIds.producerInstanceIds().size(),  equalTo(1));
            assertThat(clientInstanceIds.producerInstanceIds().get("some-thread-producer"), equalTo(producerInstanceId));
            assertThat(clientInstanceIds.adminInstanceId(), equalTo(adminInstanceId));
        }
    }

    @Test
    public void shouldThrowTimeoutExceptionWhenAnyClientFutureDoesNotComplete() {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);

        when(streamThreadOne.clientInstanceIds(any()))
            .thenReturn(Collections.singletonMap("some-client", new KafkaFutureImpl<>()));
        adminClient.setClientInstanceId(Uuid.randomUuid());

        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            final TimeoutException timeoutException = assertThrows(
                TimeoutException.class,
                () -> streams.clientInstanceIds(Duration.ZERO)
            );
            assertThat(timeoutException.getMessage(), equalTo("Could not retrieve consumer/producer instance id for some-client."));
            assertThat(timeoutException.getCause(), instanceOf(java.util.concurrent.TimeoutException.class));
        }
    }

    @Test
    public void shouldThrowTimeoutExceptionWhenGlobalConsumerFutureDoesNotComplete() {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);

        adminClient.setClientInstanceId(Uuid.randomUuid());

        final StreamsBuilder builder = getBuilderWithSource();
        builder.globalTable("anyTopic");
        try (final KafkaStreams streams = new KafkaStreams(builder.build(), props, supplier, time)) {
            streams.start();

            when(globalStreamThreadMockedConstruction.constructed().get(0).globalConsumerInstanceId(any()))
                .thenReturn(new KafkaFutureImpl<>());

            final TimeoutException timeoutException = assertThrows(
                TimeoutException.class,
                () -> streams.clientInstanceIds(Duration.ZERO)
            );
            assertThat(timeoutException.getMessage(), equalTo("Could not retrieve global consumer client instance id."));
            assertThat(timeoutException.getCause(), instanceOf(java.util.concurrent.TimeoutException.class));
        }
    }

    @Test
    public void shouldCountDownTimeoutAcrossClient() {
        prepareStreams();
        prepareStreamThread(streamThreadOne, 1);
        prepareStreamThread(streamThreadTwo, 2);

        adminClient.setClientInstanceId(Uuid.randomUuid());
        adminClient.advanceTimeOnClientInstanceId(time, Duration.ofMillis(10L).toMillis());

        final Time mockTime = time;
        final AtomicLong expectedTimeout = new AtomicLong(50L);
        final AtomicBoolean didAssertThreadOne = new AtomicBoolean(false);
        final AtomicBoolean didAssertThreadTwo = new AtomicBoolean(false);
        final AtomicBoolean didAssertGlobalThread = new AtomicBoolean(false);

        when(streamThreadOne.clientInstanceIds(any()))
            .thenReturn(Collections.singletonMap("any-client-1", new KafkaFutureImpl<Uuid>() {
                @Override
                public Uuid get(final long timeout, final TimeUnit timeUnit) {
                    didAssertThreadOne.set(true);
                    assertThat(timeout, equalTo(expectedTimeout.getAndAdd(-10L)));
                    mockTime.sleep(10L);
                    return null;
                }
            }));
        when(streamThreadTwo.clientInstanceIds(any()))
            .thenReturn(Collections.singletonMap("any-client-2", new KafkaFutureImpl<Uuid>() {
                @Override
                public Uuid get(final long timeout, final TimeUnit timeUnit) {
                    didAssertThreadTwo.set(true);
                    assertThat(timeout, equalTo(expectedTimeout.getAndAdd(-5L)));
                    mockTime.sleep(5L);
                    return null;
                }
            }));

        final StreamsBuilder builder = getBuilderWithSource();
        builder.globalTable("anyTopic");

        try (final KafkaStreams streams = new KafkaStreams(builder.build(), props, supplier, time)) {
            streams.start();

            when(globalStreamThreadMockedConstruction.constructed().get(0).globalConsumerInstanceId(any()))
                .thenReturn(new KafkaFutureImpl<Uuid>() {
                    @Override
                    public Uuid get(final long timeout, final TimeUnit timeUnit) {
                        didAssertGlobalThread.set(true);
                        assertThat(timeout, equalTo(expectedTimeout.getAndAdd(-8L)));
                        mockTime.sleep(8L);
                        return null;
                    }
                });

            streams.clientInstanceIds(Duration.ofMillis(60L));
        }

        assertThat(didAssertThreadOne.get(), equalTo(true));
        assertThat(didAssertThreadTwo.get(), equalTo(true));
        assertThat(didAssertGlobalThread.get(), equalTo(true));
    }

    private Topology getStatefulTopology(final String inputTopic,
                                         final String outputTopic,
                                         final String globalTopicName,
                                         final String storeName,
                                         final String globalStoreName,
                                         final boolean isPersistentStore) {
        final StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(
            isPersistentStore ?
                Stores.persistentKeyValueStore(storeName)
                : Stores.inMemoryKeyValueStore(storeName),
            Serdes.String(),
            Serdes.Long());
        final Topology topology = new Topology();
        topology.addSource("source", new StringDeserializer(), new StringDeserializer(), inputTopic)
            .addProcessor("process", () -> new Processor<String, String, String, String>() {
                private ProcessorContext<String, String> context;

                @Override
                public void init(final ProcessorContext<String, String> context) {
                    this.context = context;
                }

                @Override
                public void process(final Record<String, String> record) {
                    final KeyValueStore<String, Long> kvStore = context.getStateStore(storeName);
                    kvStore.put(record.key(), 5L);

                    context.forward(record.withValue("5"));
                    context.commit();
                }
            }, "source")
            .addStateStore(storeBuilder, "process")
            .addSink("sink", outputTopic, new StringSerializer(), new StringSerializer(), "process");

        final StoreBuilder<KeyValueStore<String, String>> globalStoreBuilder = Stores.keyValueStoreBuilder(
            isPersistentStore ? Stores.persistentKeyValueStore(globalStoreName) : Stores.inMemoryKeyValueStore(globalStoreName),
            Serdes.String(), Serdes.String()).withLoggingDisabled();
        topology.addGlobalStore(
            globalStoreBuilder,
            "global",
            new StringDeserializer(),
            new StringDeserializer(),
            globalTopicName,
            globalTopicName + "-processor",
            new MockProcessorSupplier<>());
        return topology;
    }

    private StreamsBuilder getBuilderWithSource() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("source-topic");
        return builder;
    }

    private void startStreamsAndCheckDirExists(final Topology topology, final boolean shouldFilesExist) {
        try (MockedConstruction<StateDirectory> stateDirectoryMockedConstruction = mockConstruction(StateDirectory.class,
            (mock, context) -> {
                when(mock.initializeProcessId()).thenReturn(UUID.randomUUID());
                assertEquals(4, context.arguments().size());
                assertEquals(shouldFilesExist, context.arguments().get(2));
            })) {

            try (final KafkaStreams ignored = new KafkaStreams(topology, props, supplier, time)) {
                // verify that stateDirectory constructor was called
                assertFalse(stateDirectoryMockedConstruction.constructed().isEmpty());
            }
        }
    }
}
