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
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.errors.StreamsNotStartedException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.errors.UnknownStateStoreException;
import org.apache.kafka.streams.internals.StreamsConfigUtils;
import org.apache.kafka.streams.internals.metrics.ClientMetrics;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.GlobalStreamThread;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.StreamsMetadataState;
import org.apache.kafka.streams.processor.internals.ThreadMetadataImpl;
import org.apache.kafka.streams.processor.internals.TopologyMetadata;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecordingTrigger;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockMetricsReporter;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.After;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnitRunner;

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
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForApplicationState;
import static org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore;
import static org.apache.kafka.test.TestUtils.waitForCondition;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.withSettings;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class KafkaStreamsTest {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(600);

    private static final int NUM_THREADS = 2;
    private final static String APPLICATION_ID = "appId";
    private final static String CLIENT_ID = "test-client";
    private final static Duration DEFAULT_DURATION = Duration.ofSeconds(30);

    @Rule
    public TestName testName = new TestName();

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

    @Before
    public void before() throws Exception {
        time = new MockTime();
        adminClient = new MockAdminClient();
        supplier = new MockClientSupplier();
        supplier.setCluster(Cluster.bootstrap(singletonList(new InetSocketAddress("localhost", 9999))));
        streamsStateListener = new StateListenerStub();
        props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2018");
        props.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, NUM_THREADS);

        prepareStreams();
    }

    @After
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
    }

    @SuppressWarnings("unchecked")
    private void prepareStreams() throws Exception {
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
                anyInt(),
                any(Runnable.class),
                any()
        )).thenReturn(streamThreadOne).thenReturn(streamThreadTwo);

        streamsConfigUtils = mockStatic(StreamsConfigUtils.class);
        streamsConfigUtils.when(() -> StreamsConfigUtils.processingMode(any(StreamsConfig.class))).thenReturn(StreamsConfigUtils.ProcessingMode.AT_LEAST_ONCE);
        streamsConfigUtils.when(() -> StreamsConfigUtils.eosEnabled(any(StreamsConfig.class))).thenReturn(false);
        streamsConfigUtils.when(() -> StreamsConfigUtils.getTotalCacheSize(any(StreamsConfig.class))).thenReturn(10 * 1024 * 1024L);
        when(streamThreadOne.getId()).thenReturn(1L);
        when(streamThreadTwo.getId()).thenReturn(2L);

        prepareStreamThread(streamThreadOne, 1, true);
        prepareStreamThread(streamThreadTwo, 2, false);

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

    private void prepareStreamThread(final StreamThread thread,
                                     final int threadId,
                                     final boolean terminable) throws Exception {
        final AtomicReference<StreamThread.State> state = new AtomicReference<>(StreamThread.State.CREATED);
        when(thread.state()).thenAnswer(invocation -> state.get());
        doNothing().when(thread).setStateListener(threadStateListenerCapture.capture());
        when(thread.getStateLock()).thenReturn(new Object());

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

        when(thread.getGroupInstanceID()).thenReturn(Optional.empty());
        when(thread.threadMetadata()).thenReturn(new ThreadMetadataImpl(
                "processId-StreamThread-" + threadId,
                "DEAD",
                "",
                "",
                Collections.emptySet(),
                "",
                Collections.emptySet(),
                Collections.emptySet()
        ));
        when(thread.waitOnThreadState(isA(StreamThread.State.class), anyLong())).thenReturn(true);
        when(thread.isThreadAlive()).thenReturn(true);
        when(thread.getName()).thenReturn("processId-StreamThread-" + threadId);

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

        if (!terminable) {
            doAnswer(invocation -> {
                Thread.sleep(2000L);
                return null;
            }).when(thread).join();
        }

        when(thread.activeTasks()).thenReturn(emptyList());
        when(thread.allTasks()).thenReturn(Collections.emptyMap());
    }

    @Test
    public void testShouldTransitToNotRunningIfCloseRightAfterCreated() {
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.close();

            Assert.assertEquals(KafkaStreams.State.NOT_RUNNING, streams.state());
        }
    }

    @Test
    public void stateShouldTransitToRunningIfNonDeadThreadsBackToRunning() throws InterruptedException {
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.setStateListener(streamsStateListener);

            Assert.assertEquals(0, streamsStateListener.numChanges);
            Assert.assertEquals(KafkaStreams.State.CREATED, streams.state());

            streams.start();

            waitForCondition(
                () -> streamsStateListener.numChanges == 2,
                "Streams never started.");
            Assert.assertEquals(KafkaStreams.State.RUNNING, streams.state());
            waitForCondition(
                () -> streamsStateListener.numChanges == 2,
                "Streams never started.");
            Assert.assertEquals(KafkaStreams.State.RUNNING, streams.state());

            for (final StreamThread thread : streams.threads) {
                threadStateListenerCapture.getValue().onChange(
                    thread,
                    StreamThread.State.PARTITIONS_REVOKED,
                    StreamThread.State.RUNNING);
            }

            Assert.assertEquals(3, streamsStateListener.numChanges);
            Assert.assertEquals(KafkaStreams.State.REBALANCING, streams.state());

            for (final StreamThread thread : streams.threads) {
                threadStateListenerCapture.getValue().onChange(
                    thread,
                    StreamThread.State.PARTITIONS_ASSIGNED,
                    StreamThread.State.PARTITIONS_REVOKED);
            }

            Assert.assertEquals(3, streamsStateListener.numChanges);
            Assert.assertEquals(KafkaStreams.State.REBALANCING, streams.state());

            threadStateListenerCapture.getValue().onChange(
                streams.threads.get(NUM_THREADS - 1),
                StreamThread.State.PENDING_SHUTDOWN,
                StreamThread.State.PARTITIONS_ASSIGNED);

            threadStateListenerCapture.getValue().onChange(
                streams.threads.get(NUM_THREADS - 1),
                StreamThread.State.DEAD,
                StreamThread.State.PENDING_SHUTDOWN);

            Assert.assertEquals(3, streamsStateListener.numChanges);
            Assert.assertEquals(KafkaStreams.State.REBALANCING, streams.state());

            for (final StreamThread thread : streams.threads) {
                if (thread != streams.threads.get(NUM_THREADS - 1)) {
                    threadStateListenerCapture.getValue().onChange(
                        thread,
                        StreamThread.State.RUNNING,
                        StreamThread.State.PARTITIONS_ASSIGNED);
                }
            }

            Assert.assertEquals(4, streamsStateListener.numChanges);
            Assert.assertEquals(KafkaStreams.State.RUNNING, streams.state());

            streams.close();

            waitForCondition(
                () -> streamsStateListener.numChanges == 6,
                "Streams never closed.");
            Assert.assertEquals(KafkaStreams.State.NOT_RUNNING, streams.state());
        }
    }

    @Test
    public void shouldCleanupResourcesOnCloseWithoutPreviousStart() throws Exception {
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
            globalStreamThread.join();

            // shutting down the global thread from "external" will yield an error in KafkaStreams
            waitForCondition(
                () -> streams.state() == KafkaStreams.State.PENDING_ERROR,
                "Thread never stopped."
            );
            streams.close();

            waitForCondition(
                () -> streams.state() == KafkaStreams.State.ERROR,
                "Thread never stopped."
            );

            assertThat(appender.getMessages(), hasItem(containsString("ERROR")));
        }
    }

    @Test
    public void testInitializesAndDestroysMetricsReporters() {
        final int oldInitCount = MockMetricsReporter.INIT_COUNT.get();

        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            final int newInitCount = MockMetricsReporter.INIT_COUNT.get();
            final int initDiff = newInitCount - oldInitCount;
            assertEquals("some reporters including MockMetricsReporter should be initialized by calling on construction", 1, initDiff);

            streams.start();
            final int oldCloseCount = MockMetricsReporter.CLOSE_COUNT.get();
            streams.close();
            assertEquals(streams.state(), KafkaStreams.State.NOT_RUNNING);
            assertEquals(oldCloseCount + initDiff, MockMetricsReporter.CLOSE_COUNT.get());
        }
    }

    @Test
    public void testCloseIsIdempotent() {
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.close();
            final int closeCount = MockMetricsReporter.CLOSE_COUNT.get();

            streams.close();
            Assert.assertEquals("subsequent close() calls should do nothing",
                closeCount, MockMetricsReporter.CLOSE_COUNT.get());
        }
    }

    @Test
    public void testPauseResume() {
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            streams.pause();
            Assert.assertTrue(streams.isPaused());
            streams.resume();
            Assert.assertFalse(streams.isPaused());
        }
    }

    @Test
    public void testStartingPaused() {
        // This test shows that a KafkaStreams instance can be started "paused"
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.pause();
            streams.start();
            Assert.assertTrue(streams.isPaused());
            streams.resume();
            Assert.assertFalse(streams.isPaused());
        }
    }

    @Test
    public void testShowPauseResumeAreIdempotent() {
        // This test shows that a KafkaStreams instance can be started "paused"
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            streams.pause();
            Assert.assertTrue(streams.isPaused());
            streams.pause();
            Assert.assertTrue(streams.isPaused());
            streams.resume();
            Assert.assertFalse(streams.isPaused());
            streams.resume();
            Assert.assertFalse(streams.isPaused());
        }
    }

    @Test
    public void shouldAddThreadWhenRunning() throws InterruptedException {
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
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            final int oldSize = streams.threads.size();
            assertThat(streams.addStreamThread(), equalTo(Optional.empty()));
            assertThat(streams.threads.size(), equalTo(oldSize));
        }
    }

    @Test
    public void shouldNotAddThreadWhenClosed() {
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            final int oldSize = streams.threads.size();
            streams.close();
            assertThat(streams.addStreamThread(), equalTo(Optional.empty()));
            assertThat(streams.threads.size(), equalTo(oldSize));
        }
    }

    @Test
    public void shouldNotAddThreadWhenError() {
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
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            streamThreadOne.shutdown();
            final Set<ThreadMetadata> threads = streams.metadataForLocalThreads();
            assertThat(threads.size(), equalTo(1));
            assertThat(threads, hasItem(streamThreadTwo.threadMetadata()));
        }
    }

    @Test
    public void shouldRemoveThread() throws InterruptedException {
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
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        try (final KafkaStreams streams =
                     new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            assertThat(streams.removeStreamThread(), equalTo(Optional.empty()));
            assertThat(streams.threads.size(), equalTo(1));
        }
    }

    @Test
    public void testCannotStartOnceClosed() {
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
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            assertThrows(IllegalStateException.class, () -> streams.setUncaughtExceptionHandler((StreamsUncaughtExceptionHandler) null));
        }
    }

    @Test
    public void shouldThrowExceptionSettingStreamsUncaughtExceptionHandlerNotInCreateState() {
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            assertThrows(IllegalStateException.class, () -> streams.setUncaughtExceptionHandler((StreamsUncaughtExceptionHandler) null));
        }

    }
    @Test
    public void shouldThrowNullPointerExceptionSettingStreamsUncaughtExceptionHandlerIfNull() {
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            assertThrows(NullPointerException.class, () -> streams.setUncaughtExceptionHandler((StreamsUncaughtExceptionHandler) null));
        }
    }

    @Test
    public void shouldThrowExceptionSettingStateListenerNotInCreateState() {
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
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
        try {
            streams.cleanUp();
            streams.start();
        } finally {
            streams.close();
            streams.cleanUp();
        }
    }

    @Test
    public void shouldThrowOnCleanupWhileRunning() throws InterruptedException {
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
    public void shouldThrowOnCleanupWhilePaused() throws InterruptedException {
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            waitForCondition(
                () -> streams.state() == KafkaStreams.State.RUNNING,
                "Streams never started.");

            streams.pause();
            waitForCondition(
                streams::isPaused,
                "Streams did not pause.");

            assertThrows("Cannot clean up while running.", IllegalStateException.class,
                streams::cleanUp);
        }
    }

    @Test
    public void shouldThrowOnCleanupWhileShuttingDown() throws InterruptedException {
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
        streams.start();
        waitForCondition(
            () -> streams.state() == KafkaStreams.State.RUNNING,
            "Streams never started.");

        streams.close(Duration.ZERO);
        assertThat(streams.state() == State.PENDING_SHUTDOWN, equalTo(true));
        assertThrows(IllegalStateException.class, streams::cleanUp);
        assertThat(streams.state() == State.PENDING_SHUTDOWN, equalTo(true));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldThrowOnCleanupWhileShuttingDownStreamClosedWithCloseOptionLeaveGroupFalse() throws InterruptedException {
        final MockConsumer<byte[], byte[]> mockConsumer = mock(MockConsumer.class, withSettings().useConstructor(OffsetResetStrategy.EARLIEST));
        final MockClientSupplier mockClientSupplier = spy(MockClientSupplier.class);
        final ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
        final Optional<String> groupInstanceId = Optional.of("test-instance-id");

        when(consumerGroupMetadata.groupInstanceId()).thenReturn(groupInstanceId);
        when(mockConsumer.groupMetadata()).thenReturn(consumerGroupMetadata);
        when(mockClientSupplier.getAdmin(any())).thenReturn(adminClient);
        when(mockClientSupplier.getConsumer(any())).thenReturn(mockConsumer);

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
    public void shouldThrowOnCleanupWhileShuttingDownStreamClosedWithCloseOptionLeaveGroupTrue() throws InterruptedException {
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
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

    @Test
    public void shouldNotGetAllTasksWhenNotRunning() throws InterruptedException {
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
    public void shouldNotGetAllTasksWithStoreWhenNotRunning() throws InterruptedException {
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
    public void shouldNotGetQueryMetadataWithSerializerWhenNotRunningOrRebalancing() throws InterruptedException {
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            assertThrows(StreamsNotStartedException.class, () -> streams.queryMetadataForKey("store", "key", Serdes.String().serializer()));
            streams.start();
            waitForApplicationState(Collections.singletonList(streams), KafkaStreams.State.RUNNING, DEFAULT_DURATION);
            streams.close();
            waitForApplicationState(Collections.singletonList(streams), KafkaStreams.State.NOT_RUNNING, DEFAULT_DURATION);
            assertThrows(IllegalStateException.class, () -> streams.queryMetadataForKey("store", "key", Serdes.String().serializer()));
        }
    }

    @Test
    public void shouldGetQueryMetadataWithSerializerWhenRunningOrRebalancing() {
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            assertEquals(KeyQueryMetadata.NOT_AVAILABLE, streams.queryMetadataForKey("store", "key", Serdes.String().serializer()));
        }
    }

    @Test
    public void shouldNotGetQueryMetadataWithPartitionerWhenNotRunningOrRebalancing() throws InterruptedException {
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            assertThrows(StreamsNotStartedException.class, () -> streams.queryMetadataForKey("store", "key", (topic, key, value, numPartitions) -> 0));
            streams.start();
            waitForApplicationState(Collections.singletonList(streams), KafkaStreams.State.RUNNING, DEFAULT_DURATION);
            streams.close();
            waitForApplicationState(Collections.singletonList(streams), KafkaStreams.State.NOT_RUNNING, DEFAULT_DURATION);
            assertThrows(IllegalStateException.class, () -> streams.queryMetadataForKey("store", "key", (topic, key, value, numPartitions) -> 0));
        }
    }

    @Test
    public void shouldThrowUnknownStateStoreExceptionWhenStoreNotExist() throws InterruptedException {
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
            waitForApplicationState(Collections.singletonList(streams), KafkaStreams.State.RUNNING, DEFAULT_DURATION);
            assertThrows(UnknownStateStoreException.class, () -> streams.store(StoreQueryParameters.fromNameAndType("unknown-store", keyValueStore())));
        }
    }

    @Test
    public void shouldNotGetStoreWhenWhenNotRunningOrRebalancing() throws InterruptedException {
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
        // Mock all calls made to compute the offset lags,
        final ListOffsetsResult result = mock(ListOffsetsResult.class);
        final KafkaFutureImpl<Map<TopicPartition, ListOffsetsResultInfo>> allFuture = new KafkaFutureImpl<>();
        allFuture.complete(Collections.emptyMap());

        when(result.all()).thenReturn(allFuture);
        final MockAdminClient mockAdminClient = spy(MockAdminClient.class);
        when(mockAdminClient.listOffsets(anyMap())).thenReturn(result);
        final MockClientSupplier mockClientSupplier = spy(MockClientSupplier.class);
        when(mockClientSupplier.getAdmin(any())).thenReturn(mockAdminClient);

        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, mockClientSupplier, time)) {
            streams.start();
            assertEquals(0, streams.allLocalStorePartitionLags().size());
        }
    }

    @Test
    public void shouldReturnFalseOnCloseWhenThreadsHaventTerminated() {
        // do not use mock time so that it can really elapse
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier)) {
            assertFalse(streams.close(Duration.ofMillis(10L)));
        }
    }

    @Test
    public void shouldThrowOnNegativeTimeoutForClose() {
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            assertThrows(IllegalArgumentException.class, () -> streams.close(Duration.ofMillis(-1L)));
        }
    }

    @Test
    public void shouldNotBlockInCloseForZeroDuration() {
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            // with mock time that does not elapse, close would not return if it ever waits on the state transition
            assertFalse(streams.close(Duration.ZERO));
        }
    }

    @Test
    public void shouldReturnFalseOnCloseWithCloseOptionWithLeaveGroupFalseWhenThreadsHaventTerminated() {
        final KafkaStreams.CloseOptions closeOptions = new KafkaStreams.CloseOptions();
        closeOptions.timeout(Duration.ofMillis(10L));
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier)) {
            assertFalse(streams.close(closeOptions));
        }
    }

    @Test
    public void shouldThrowOnNegativeTimeoutForCloseWithCloseOptionLeaveGroupFalse() {
        final KafkaStreams.CloseOptions closeOptions = new KafkaStreams.CloseOptions();
        closeOptions.timeout(Duration.ofMillis(-1L));
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            assertThrows(IllegalArgumentException.class, () -> streams.close(closeOptions));
        }
    }

    @Test
    public void shouldNotBlockInCloseWithCloseOptionLeaveGroupFalseForZeroDuration() {
        final KafkaStreams.CloseOptions closeOptions = new KafkaStreams.CloseOptions();
        closeOptions.timeout(Duration.ZERO);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier)) {
            assertFalse(streams.close(closeOptions));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldReturnFalseOnCloseWithCloseOptionWithLeaveGroupTrueWhenThreadsHaventTerminated() {
        final MockConsumer<byte[], byte[]> mockConsumer = mock(MockConsumer.class, withSettings().useConstructor(OffsetResetStrategy.EARLIEST));
        final MockClientSupplier mockClientSupplier = spy(MockClientSupplier.class);
        final ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
        final Optional<String> groupInstanceId = Optional.of("test-instance-id");

        when(consumerGroupMetadata.groupInstanceId()).thenReturn(groupInstanceId);
        when(mockConsumer.groupMetadata()).thenReturn(consumerGroupMetadata);
        when(mockClientSupplier.getAdmin(any())).thenReturn(adminClient);
        when(mockClientSupplier.getConsumer(any())).thenReturn(mockConsumer);

        final KafkaStreams.CloseOptions closeOptions = new KafkaStreams.CloseOptions();
        closeOptions.timeout(Duration.ofMillis(10L));
        closeOptions.leaveGroup(true);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, mockClientSupplier)) {
            assertFalse(streams.close(closeOptions));
        }
    }

    @Test
    public void shouldThrowOnNegativeTimeoutForCloseWithCloseOptionLeaveGroupTrue() {
        final MockClientSupplier mockClientSupplier = spy(MockClientSupplier.class);
        when(mockClientSupplier.getAdmin(any())).thenReturn(adminClient);

        final KafkaStreams.CloseOptions closeOptions = new KafkaStreams.CloseOptions();
        closeOptions.timeout(Duration.ofMillis(-1L));
        closeOptions.leaveGroup(true);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, mockClientSupplier, time)) {
            assertThrows(IllegalArgumentException.class, () -> streams.close(closeOptions));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldNotBlockInCloseWithCloseOptionLeaveGroupTrueForZeroDuration() {
        final MockConsumer<byte[], byte[]> mockConsumer = mock(MockConsumer.class, withSettings().useConstructor(OffsetResetStrategy.EARLIEST));
        final MockClientSupplier mockClientSupplier = spy(MockClientSupplier.class);
        final ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
        final Optional<String> groupInstanceId = Optional.of("test-instance-id");

        when(consumerGroupMetadata.groupInstanceId()).thenReturn(groupInstanceId);
        when(mockConsumer.groupMetadata()).thenReturn(consumerGroupMetadata);
        when(mockClientSupplier.getAdmin(any())).thenReturn(adminClient);
        when(mockClientSupplier.getConsumer(any())).thenReturn(mockConsumer);

        final KafkaStreams.CloseOptions closeOptions = new KafkaStreams.CloseOptions();
        closeOptions.timeout(Duration.ZERO);
        closeOptions.leaveGroup(true);
        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, mockClientSupplier)) {
            assertFalse(streams.close(closeOptions));
        }
    }

    @Test
    public void shouldTriggerRecordingOfRocksDBMetricsIfRecordingLevelIsDebug() {
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
    public void shouldGetClientSupplierFromConfigForConstructor() {
        final StreamsConfig config = new StreamsConfig(props);
        final StreamsConfig mockConfig = spy(config);
        when(mockConfig.getKafkaClientSupplier()).thenReturn(supplier);

        new KafkaStreams(getBuilderWithSource().build(), mockConfig);
        // It's called once in above when mock
        verify(mockConfig, times(2)).getKafkaClientSupplier();
    }

    @Test
    public void shouldGetClientSupplierFromConfigForConstructorWithTime() {
        final StreamsConfig config = new StreamsConfig(props);
        final StreamsConfig mockConfig = spy(config);
        when(mockConfig.getKafkaClientSupplier()).thenReturn(supplier);

        new KafkaStreams(getBuilderWithSource().build(), mockConfig, time);
        // It's called once in above when mock
        verify(mockConfig, times(2)).getKafkaClientSupplier();
    }

    @Test
    public void shouldUseProvidedClientSupplier() {
        final StreamsConfig config = new StreamsConfig(props);
        final StreamsConfig mockConfig = spy(config);

        new KafkaStreams(getBuilderWithSource().build(), mockConfig, supplier);
        // It's called once in above when mock
        verify(mockConfig, times(0)).getKafkaClientSupplier();
    }

    @Test
    public void shouldNotTriggerRecordingOfRocksDBMetricsIfRecordingLevelIsInfo() {
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
    public void statelessTopologyShouldNotCreateStateDirectory() {
        final String safeTestName = safeUniqueTestName(getClass(), testName);
        final String inputTopic = safeTestName + "-input";
        final String outputTopic = safeTestName + "-output";
        final Topology topology = new Topology();
        topology.addSource("source", Serdes.String().deserializer(), Serdes.String().deserializer(), inputTopic)
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
    public void inMemoryStatefulTopologyShouldNotCreateStateDirectory() {
        final String safeTestName = safeUniqueTestName(getClass(), testName);
        final String inputTopic = safeTestName + "-input";
        final String outputTopic = safeTestName + "-output";
        final String globalTopicName = safeTestName + "-global";
        final String storeName = safeTestName + "-counts";
        final String globalStoreName = safeTestName + "-globalStore";
        final Topology topology = getStatefulTopology(inputTopic, outputTopic, globalTopicName, storeName, globalStoreName, false);
        startStreamsAndCheckDirExists(topology, false);
    }

    @Test
    public void statefulTopologyShouldCreateStateDirectory() {
        final String safeTestName = safeUniqueTestName(getClass(), testName);
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
        try {
            new KafkaStreams(new StreamsBuilder().build(), props, supplier, time);
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
        final StreamsBuilder builder = new StreamsBuilder();
        builder.globalTable("anyTopic");
        try (final KafkaStreams streams = new KafkaStreams(builder.build(), props, supplier, time)) {
            assertThat(streams.threads.size(), equalTo(0));
        }
    }

    @Test
    public void shouldTransitToRunningWithGlobalOnlyTopology() throws InterruptedException {
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

    @Deprecated // testing old PAPI
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
        topology.addSource("source", Serdes.String().deserializer(), Serdes.String().deserializer(), inputTopic)
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
            Serdes.String().deserializer(),
            Serdes.String().deserializer(),
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
