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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.internals.metrics.ClientMetrics;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.ThreadMetadata;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.GlobalStreamThread;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.StreamsMetadataState;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecordingTrigger;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockMetricsReporter;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockRocksDbConfigSetter;
import org.apache.kafka.test.TestUtils;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

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
import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.capture;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@PrepareForTest({KafkaStreams.class, StreamThread.class, ClientMetrics.class})
public class KafkaStreamsTest {

    private static final int NUM_THREADS = 2;
    private final static String APPLICATION_ID = "appId";
    private final static String CLIENT_ID = "test-client";

    @Rule
    public TestName testName = new TestName();

    private MockClientSupplier supplier;
    private MockTime time;

    private Properties props;
    
    @Mock
    private StateDirectory stateDirectory;
    @Mock
    private StreamThread streamThreadOne;
    @Mock
    private StreamThread streamThreadTwo;
    @Mock
    private GlobalStreamThread globalStreamThread;
    @Mock
    private Metrics metrics;
    @Mock
    private ThreadMetadata threadMetadata;

    private StateListenerStub streamsStateListener;
    private Capture<List<MetricsReporter>> metricsReportersCapture;
    private Capture<StreamThread.StateListener> threadStatelistenerCapture;

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
        supplier = new MockClientSupplier();
        supplier.setCluster(Cluster.bootstrap(singletonList(new InetSocketAddress("localhost", 9999))));
        streamsStateListener = new StateListenerStub();
        threadStatelistenerCapture = EasyMock.newCapture();
        metricsReportersCapture = EasyMock.newCapture();

        props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2018");
        props.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, NUM_THREADS);

        prepareStreams();
    }

    private void prepareStreams() throws Exception {
        // setup metrics
        PowerMock.expectNew(Metrics.class,
            anyObject(MetricConfig.class),
            capture(metricsReportersCapture),
            anyObject(Time.class),
            anyObject(MetricsContext.class)
        ).andAnswer(() -> {
            for (final MetricsReporter reporter : metricsReportersCapture.getValue()) {
                reporter.init(Collections.emptyList());
            }
            return metrics;
        }).anyTimes();
        metrics.close();
        EasyMock.expectLastCall().andAnswer(() -> {
            for (final MetricsReporter reporter : metricsReportersCapture.getValue()) {
                reporter.close();
            }
            return null;
        }).anyTimes();

        PowerMock.mockStatic(ClientMetrics.class);
        EasyMock.expect(ClientMetrics.version()).andReturn("1.56");
        EasyMock.expect(ClientMetrics.commitId()).andReturn("1a2b3c4d5e");
        ClientMetrics.addVersionMetric(anyObject(StreamsMetricsImpl.class));
        ClientMetrics.addCommitIdMetric(anyObject(StreamsMetricsImpl.class));
        ClientMetrics.addApplicationIdMetric(anyObject(StreamsMetricsImpl.class), EasyMock.eq(APPLICATION_ID));
        ClientMetrics.addTopologyDescriptionMetric(anyObject(StreamsMetricsImpl.class), anyString());
        ClientMetrics.addStateMetric(anyObject(StreamsMetricsImpl.class), anyObject());
        ClientMetrics.addNumAliveStreamThreadMetric(anyObject(StreamsMetricsImpl.class), anyObject());

        // setup stream threads
        PowerMock.mockStatic(StreamThread.class);
        EasyMock.expect(StreamThread.create(
            anyObject(InternalTopologyBuilder.class),
            anyObject(StreamsConfig.class),
            anyObject(KafkaClientSupplier.class),
            anyObject(Admin.class),
            anyObject(UUID.class),
            anyObject(String.class),
            anyObject(StreamsMetricsImpl.class),
            anyObject(Time.class),
            anyObject(StreamsMetadataState.class),
            anyLong(),
            anyObject(StateDirectory.class),
            anyObject(StateRestoreListener.class),
            anyInt(),
            anyObject(Runnable.class),
            anyObject()
        )).andReturn(streamThreadOne).andReturn(streamThreadTwo);

        EasyMock.expect(StreamThread.eosEnabled(anyObject(StreamsConfig.class))).andReturn(false).anyTimes();
        EasyMock.expect(StreamThread.processingMode(anyObject(StreamsConfig.class))).andReturn(StreamThread.ProcessingMode.AT_LEAST_ONCE).anyTimes();
        EasyMock.expect(streamThreadOne.getId()).andReturn(0L).anyTimes();
        EasyMock.expect(streamThreadTwo.getId()).andReturn(1L).anyTimes();
        prepareStreamThread(streamThreadOne, 1, true);
        prepareStreamThread(streamThreadTwo, 2, false);

        // setup global threads
        final AtomicReference<GlobalStreamThread.State> globalThreadState = new AtomicReference<>(GlobalStreamThread.State.CREATED);
        PowerMock.expectNew(GlobalStreamThread.class,
            anyObject(ProcessorTopology.class),
            anyObject(StreamsConfig.class),
            anyObject(Consumer.class),
            anyObject(StateDirectory.class),
            anyLong(),
            anyObject(StreamsMetricsImpl.class),
            anyObject(Time.class),
            anyString(),
            anyObject(StateRestoreListener.class),
            anyObject(StreamsUncaughtExceptionHandler.class)
        ).andReturn(globalStreamThread).anyTimes();
        EasyMock.expect(globalStreamThread.state()).andAnswer(globalThreadState::get).anyTimes();
        globalStreamThread.setStateListener(capture(threadStatelistenerCapture));
        EasyMock.expectLastCall().anyTimes();

        globalStreamThread.start();
        EasyMock.expectLastCall().andAnswer(() -> {
            globalThreadState.set(GlobalStreamThread.State.RUNNING);
            threadStatelistenerCapture.getValue().onChange(globalStreamThread,
                GlobalStreamThread.State.RUNNING,
                GlobalStreamThread.State.CREATED);
            return null;
        }).anyTimes();
        globalStreamThread.shutdown();
        EasyMock.expectLastCall().andAnswer(() -> {
            supplier.restoreConsumer.close();

            for (final MockProducer<byte[], byte[]> producer : supplier.producers) {
                producer.close();
            }
            globalThreadState.set(GlobalStreamThread.State.DEAD);
            threadStatelistenerCapture.getValue().onChange(globalStreamThread,
                GlobalStreamThread.State.PENDING_SHUTDOWN,
                GlobalStreamThread.State.RUNNING);
            threadStatelistenerCapture.getValue().onChange(globalStreamThread,
                GlobalStreamThread.State.DEAD,
                GlobalStreamThread.State.PENDING_SHUTDOWN);
            return null;
        }).anyTimes();
        EasyMock.expect(globalStreamThread.stillRunning()).andReturn(globalThreadState.get() == GlobalStreamThread.State.RUNNING).anyTimes();
        globalStreamThread.join();
        EasyMock.expectLastCall().anyTimes();

        PowerMock.replay(
            StreamThread.class,
            Metrics.class,
            metrics,
            ClientMetrics.class,
            streamThreadOne,
            streamThreadTwo,
            GlobalStreamThread.class,
            globalStreamThread
        );
    }

    private void prepareStreamThread(final StreamThread thread, final int threadId, final boolean terminable) throws Exception {
        final AtomicReference<StreamThread.State> state = new AtomicReference<>(StreamThread.State.CREATED);
        EasyMock.expect(thread.state()).andAnswer(state::get).anyTimes();

        thread.setStateListener(capture(threadStatelistenerCapture));
        EasyMock.expectLastCall().anyTimes();

        EasyMock.expect(thread.getStateLock()).andReturn(new Object()).anyTimes();

        thread.start();
        EasyMock.expectLastCall().andAnswer(() -> {
            state.set(StreamThread.State.STARTING);
            threadStatelistenerCapture.getValue().onChange(thread,
                StreamThread.State.STARTING,
                StreamThread.State.CREATED);
            threadStatelistenerCapture.getValue().onChange(thread,
                StreamThread.State.PARTITIONS_REVOKED,
                StreamThread.State.STARTING);
            threadStatelistenerCapture.getValue().onChange(thread,
                StreamThread.State.PARTITIONS_ASSIGNED,
                StreamThread.State.PARTITIONS_REVOKED);
            threadStatelistenerCapture.getValue().onChange(thread,
                StreamThread.State.RUNNING,
                StreamThread.State.PARTITIONS_ASSIGNED);
            return null;
        }).anyTimes();
        EasyMock.expect(thread.getGroupInstanceID()).andStubReturn(Optional.empty());
        EasyMock.expect(thread.threadMetadata()).andReturn(new ThreadMetadata(
                "processId-StreamThread-" + threadId,
                "DEAD",
                "",
                "",
                Collections.emptySet(),
                "",
                Collections.emptySet(),
                Collections.emptySet()
            )
        ).anyTimes();
        EasyMock.expect(thread.waitOnThreadState(EasyMock.isA(StreamThread.State.class), anyLong())).andStubReturn(true);
        EasyMock.expect(thread.isAlive()).andReturn(true).times(0, 1);
        thread.resizeCache(EasyMock.anyLong());
        EasyMock.expectLastCall().anyTimes();
        thread.requestLeaveGroupDuringShutdown();
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(thread.getName()).andStubReturn("processId-StreamThread-" + threadId);
        thread.shutdown();
        EasyMock.expectLastCall().andAnswer(() -> {
            supplier.consumer.close();
            supplier.restoreConsumer.close();
            for (final MockProducer<byte[], byte[]> producer : supplier.producers) {
                producer.close();
            }
            state.set(StreamThread.State.DEAD);
            threadStatelistenerCapture.getValue().onChange(thread, StreamThread.State.PENDING_SHUTDOWN, StreamThread.State.RUNNING);
            threadStatelistenerCapture.getValue().onChange(thread, StreamThread.State.DEAD, StreamThread.State.PENDING_SHUTDOWN);
            return null;
        }).anyTimes();
        EasyMock.expect(thread.isRunning()).andReturn(state.get() == StreamThread.State.RUNNING).anyTimes();
        thread.join();
        if (terminable)
            EasyMock.expectLastCall().anyTimes();
        else
            EasyMock.expectLastCall().andAnswer(() -> {
                Thread.sleep(50L);
                return null;
            }).anyTimes();

        EasyMock.expect(thread.activeTasks()).andStubReturn(emptyList());
        EasyMock.expect(thread.allTasks()).andStubReturn(Collections.emptyMap());
    }

    @Test
    public void testShouldTransitToNotRunningIfCloseRightAfterCreated() {
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
        streams.close();

        Assert.assertEquals(KafkaStreams.State.NOT_RUNNING, streams.state());
    }

    @Test
    public void stateShouldTransitToRunningIfNonDeadThreadsBackToRunning() throws InterruptedException {
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
        streams.setStateListener(streamsStateListener);

        Assert.assertEquals(0, streamsStateListener.numChanges);
        Assert.assertEquals(KafkaStreams.State.CREATED, streams.state());

        streams.start();

        TestUtils.waitForCondition(
            () -> streamsStateListener.numChanges == 2,
            "Streams never started.");
        Assert.assertEquals(KafkaStreams.State.RUNNING, streams.state());

        for (final StreamThread thread: streams.threads) {
            threadStatelistenerCapture.getValue().onChange(
                thread,
                StreamThread.State.PARTITIONS_REVOKED,
                StreamThread.State.RUNNING);
        }

        Assert.assertEquals(3, streamsStateListener.numChanges);
        Assert.assertEquals(KafkaStreams.State.REBALANCING, streams.state());

        for (final StreamThread thread : streams.threads) {
            threadStatelistenerCapture.getValue().onChange(
                thread,
                StreamThread.State.PARTITIONS_ASSIGNED,
                StreamThread.State.PARTITIONS_REVOKED);
        }

        Assert.assertEquals(3, streamsStateListener.numChanges);
        Assert.assertEquals(KafkaStreams.State.REBALANCING, streams.state());

        threadStatelistenerCapture.getValue().onChange(
            streams.threads.get(NUM_THREADS - 1),
            StreamThread.State.PENDING_SHUTDOWN,
            StreamThread.State.PARTITIONS_ASSIGNED);

        threadStatelistenerCapture.getValue().onChange(
            streams.threads.get(NUM_THREADS - 1),
            StreamThread.State.DEAD,
            StreamThread.State.PENDING_SHUTDOWN);

        Assert.assertEquals(3, streamsStateListener.numChanges);
        Assert.assertEquals(KafkaStreams.State.REBALANCING, streams.state());

        for (final StreamThread thread : streams.threads) {
            if (thread != streams.threads.get(NUM_THREADS - 1)) {
                threadStatelistenerCapture.getValue().onChange(
                    thread,
                    StreamThread.State.RUNNING,
                    StreamThread.State.PARTITIONS_ASSIGNED);
            }
        }

        Assert.assertEquals(4, streamsStateListener.numChanges);
        Assert.assertEquals(KafkaStreams.State.RUNNING, streams.state());

        streams.close();

        TestUtils.waitForCondition(
            () -> streamsStateListener.numChanges == 6,
            "Streams never closed.");
        Assert.assertEquals(KafkaStreams.State.NOT_RUNNING, streams.state());
    }

    @Test
    public void shouldCleanupResourcesOnCloseWithoutPreviousStart() throws Exception {
        final StreamsBuilder builder = getBuilderWithSource();
        builder.globalTable("anyTopic");

        final KafkaStreams streams = new KafkaStreams(builder.build(), props, supplier, time);
        streams.close();

        TestUtils.waitForCondition(
            () -> streams.state() == KafkaStreams.State.NOT_RUNNING,
            "Streams never stopped.");

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
        final KafkaStreams streams = new KafkaStreams(builder.build(), props, supplier, time);

        try {
            assertEquals(NUM_THREADS, streams.threads.size());
            assertEquals(streams.state(), KafkaStreams.State.CREATED);

            streams.start();
            TestUtils.waitForCondition(
                () -> streams.state() == KafkaStreams.State.RUNNING,
                "Streams never started.");

            for (int i = 0; i < NUM_THREADS; i++) {
                final StreamThread tmpThread = streams.threads.get(i);
                tmpThread.shutdown();
                TestUtils.waitForCondition(() -> tmpThread.state() == StreamThread.State.DEAD,
                    "Thread never stopped.");
                streams.threads.get(i).join();
            }
            TestUtils.waitForCondition(
                () -> streams.localThreadsMetadata().stream().allMatch(t -> t.threadState().equals("DEAD")),
                "Streams never stopped"
            );
        } finally {
            streams.close();
        }

        TestUtils.waitForCondition(
            () -> streams.state() == KafkaStreams.State.NOT_RUNNING,
            "Streams never stopped.");

        assertNull(streams.globalStreamThread);
    }

    @Test
    public void testStateGlobalThreadClose() throws Exception {
        // make sure we have the global state thread running too
        final StreamsBuilder builder = getBuilderWithSource();
        builder.globalTable("anyTopic");
        final KafkaStreams streams = new KafkaStreams(builder.build(), props, supplier, time);

        try {
            streams.start();
            TestUtils.waitForCondition(
                () -> streams.state() == KafkaStreams.State.RUNNING,
                "Streams never started.");

            final GlobalStreamThread globalStreamThread = streams.globalStreamThread;
            globalStreamThread.shutdown();
            TestUtils.waitForCondition(
                () -> globalStreamThread.state() == GlobalStreamThread.State.DEAD,
                "Thread never stopped.");
            globalStreamThread.join();
            TestUtils.waitForCondition(
                () -> streams.state() == KafkaStreams.State.PENDING_ERROR,
                "Thread never stopped."
            );
        } finally {
            streams.close();
        }

        TestUtils.waitForCondition(
            () -> streams.state() == KafkaStreams.State.ERROR,
            "Thread never stopped."
        );
    }

    @Test
    public void testInitializesAndDestroysMetricsReporters() {
        final int oldInitCount = MockMetricsReporter.INIT_COUNT.get();

        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            final int newInitCount = MockMetricsReporter.INIT_COUNT.get();
            final int initDiff = newInitCount - oldInitCount;
            assertTrue("some reporters should be initialized by calling on construction", initDiff > 0);

            streams.start();
            final int oldCloseCount = MockMetricsReporter.CLOSE_COUNT.get();
            streams.close();
            assertEquals(oldCloseCount + initDiff, MockMetricsReporter.CLOSE_COUNT.get());
        }
    }

    @Test
    public void testCloseIsIdempotent() {
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
        streams.close();
        final int closeCount = MockMetricsReporter.CLOSE_COUNT.get();

        streams.close();
        Assert.assertEquals("subsequent close() calls should do nothing",
            closeCount, MockMetricsReporter.CLOSE_COUNT.get());
    }

    @Test
    public void shouldAddThreadWhenRunning() throws InterruptedException {
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
        streams.start();
        final int oldSize = streams.threads.size();
        TestUtils.waitForCondition(() -> streams.state() == KafkaStreams.State.RUNNING, 15L, "wait until running");
        assertThat(streams.addStreamThread(), equalTo(Optional.of("processId-StreamThread-" + 2)));
        assertThat(streams.threads.size(), equalTo(oldSize + 1));
    }

    @Test
    public void shouldNotAddThreadWhenCreated() {
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
        final int oldSize = streams.threads.size();
        assertThat(streams.addStreamThread(), equalTo(Optional.empty()));
        assertThat(streams.threads.size(), equalTo(oldSize));
    }

    @Test
    public void shouldNotAddThreadWhenClosed() {
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
        final int oldSize = streams.threads.size();
        streams.close();
        assertThat(streams.addStreamThread(), equalTo(Optional.empty()));
        assertThat(streams.threads.size(), equalTo(oldSize));
    }

    @Test
    public void shouldNotAddThreadWhenError() {
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
        final int oldSize = streams.threads.size();
        streams.start();
        globalStreamThread.shutdown();
        assertThat(streams.addStreamThread(), equalTo(Optional.empty()));
        assertThat(streams.threads.size(), equalTo(oldSize));
    }

    @Test
    public void shouldNotReturnDeadThreads() {
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
        streams.start();
        streamThreadOne.shutdown();
        final Set<ThreadMetadata> threads = streams.localThreadsMetadata();
        assertThat(threads.size(), equalTo(1));
        assertThat(threads, hasItem(streamThreadTwo.threadMetadata()));
    }

    @Test
    public void shouldRemoveThread() throws InterruptedException {
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
        streams.start();
        final int oldSize = streams.threads.size();
        TestUtils.waitForCondition(() -> streams.state() == KafkaStreams.State.RUNNING, 15L,
            "Kafka Streams client did not reach state RUNNING");
        assertThat(streams.removeStreamThread(), equalTo(Optional.of("processId-StreamThread-" + 1)));
        assertThat(streams.threads.size(), equalTo(oldSize - 1));
    }

    @Test
    public void shouldNotRemoveThreadWhenNotRunning() {
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
        assertThat(streams.removeStreamThread(), equalTo(Optional.empty()));
        assertThat(streams.threads.size(), equalTo(1));
    }

    @Test
    public void testCannotStartOnceClosed() {
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
        streams.start();
        streams.close();
        try {
            streams.start();
            fail("Should have throw IllegalStateException");
        } catch (final IllegalStateException expected) {
            // this is ok
        } finally {
            streams.close();
        }
    }

    @Test
    public void shouldNotSetGlobalRestoreListenerAfterStarting() {
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
        streams.start();
        try {
            streams.setGlobalStateRestoreListener(null);
            fail("Should throw an IllegalStateException");
        } catch (final IllegalStateException e) {
            // expected
        } finally {
            streams.close();
        }
    }

    @Test
    public void shouldThrowExceptionSettingUncaughtExceptionHandlerNotInCreateState() {
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
        streams.start();
        assertThrows(IllegalStateException.class, () -> streams.setUncaughtExceptionHandler((StreamsUncaughtExceptionHandler) null));
    }

    @Test
    public void shouldThrowExceptionSettingStreamsUncaughtExceptionHandlerNotInCreateState() {
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
        streams.start();
        assertThrows(IllegalStateException.class, () -> streams.setUncaughtExceptionHandler((StreamsUncaughtExceptionHandler) null));

    }
    @Test
    public void shouldThrowNullPointerExceptionSettingStreamsUncaughtExceptionHandlerIfNull() {
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
        assertThrows(NullPointerException.class, () -> streams.setUncaughtExceptionHandler((StreamsUncaughtExceptionHandler) null));
    }

    @Test
    public void shouldThrowExceptionSettingStateListenerNotInCreateState() {
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
        streams.start();
        try {
            streams.setStateListener(null);
            fail("Should throw IllegalStateException");
        } catch (final IllegalStateException e) {
            // expected
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
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
        streams.start();
        TestUtils.waitForCondition(
            () -> streams.state() == KafkaStreams.State.RUNNING,
            "Streams never started.");

        try {
            streams.cleanUp();
            fail("Should have thrown IllegalStateException");
        } catch (final IllegalStateException expected) {
            assertEquals("Cannot clean up while running.", expected.getMessage());
        }
    }

    @Test
    public void shouldNotGetAllTasksWhenNotRunning() {
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
        assertThrows(IllegalStateException.class, streams::allMetadata);
    }

    @Test
    public void shouldNotGetAllTasksWithStoreWhenNotRunning() {
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
        assertThrows(IllegalStateException.class, () -> streams.allMetadataForStore("store"));
    }

    @Test
    public void shouldNotGetQueryMetadataWithSerializerWhenNotRunningOrRebalancing() {
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
        assertThrows(IllegalStateException.class, () -> streams.queryMetadataForKey("store", "key", Serdes.String().serializer()));
    }

    @Test
    public void shouldGetQueryMetadataWithSerializerWhenRunningOrRebalancing() {
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
        streams.start();
        assertEquals(KeyQueryMetadata.NOT_AVAILABLE, streams.queryMetadataForKey("store", "key", Serdes.String().serializer()));
    }

    @Test
    public void shouldNotGetQueryMetadataWithPartitionerWhenNotRunningOrRebalancing() {
        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);
        assertThrows(IllegalStateException.class, () -> streams.queryMetadataForKey("store", "key", (topic, key, value, numPartitions) -> 0));
    }

    @Test
    public void shouldReturnEmptyLocalStorePartitionLags() {
        // Mock all calls made to compute the offset lags,
        final ListOffsetsResult result = EasyMock.mock(ListOffsetsResult.class);
        final KafkaFutureImpl<Map<TopicPartition, ListOffsetsResultInfo>> allFuture = new KafkaFutureImpl<>();
        allFuture.complete(Collections.emptyMap());

        EasyMock.expect(result.all()).andReturn(allFuture);
        final MockAdminClient mockAdminClient = EasyMock.partialMockBuilder(MockAdminClient.class)
            .addMockedMethod("listOffsets", Map.class).createMock();
        EasyMock.expect(mockAdminClient.listOffsets(anyObject())).andStubReturn(result);
        final MockClientSupplier mockClientSupplier = EasyMock.partialMockBuilder(MockClientSupplier.class)
            .addMockedMethod("getAdmin").createMock();
        EasyMock.expect(mockClientSupplier.getAdmin(anyObject())).andReturn(mockAdminClient);
        EasyMock.replay(result, mockAdminClient, mockClientSupplier);

        final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, mockClientSupplier, time);
        streams.start();
        assertEquals(0, streams.allLocalStorePartitionLags().size());
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
    public void shouldTriggerRecordingOfRocksDBMetricsIfRecordingLevelIsDebug() {
        PowerMock.mockStatic(Executors.class);
        final ScheduledExecutorService cleanupSchedule = EasyMock.niceMock(ScheduledExecutorService.class);
        final ScheduledExecutorService rocksDBMetricsRecordingTriggerThread =
            EasyMock.mock(ScheduledExecutorService.class);
        EasyMock.expect(Executors.newSingleThreadScheduledExecutor(
            anyObject(ThreadFactory.class)
        )).andReturn(cleanupSchedule);
        EasyMock.expect(Executors.newSingleThreadScheduledExecutor(
            anyObject(ThreadFactory.class)
        )).andReturn(rocksDBMetricsRecordingTriggerThread);
        EasyMock.expect(rocksDBMetricsRecordingTriggerThread.scheduleAtFixedRate(
            EasyMock.anyObject(RocksDBMetricsRecordingTrigger.class),
            EasyMock.eq(0L),
            EasyMock.eq(1L),
            EasyMock.eq(TimeUnit.MINUTES)
        )).andReturn(null);
        EasyMock.expect(rocksDBMetricsRecordingTriggerThread.shutdownNow()).andReturn(null);
        PowerMock.replay(Executors.class);
        PowerMock.replay(rocksDBMetricsRecordingTriggerThread);
        PowerMock.replay(cleanupSchedule);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.table("topic", Materialized.as("store"));
        props.setProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, RecordingLevel.DEBUG.name());

        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
        }

        PowerMock.verify(Executors.class);
        PowerMock.verify(rocksDBMetricsRecordingTriggerThread);
    }

    @Test
    public void shouldNotTriggerRecordingOfRocksDBMetricsIfRecordingLevelIsInfo() {
        PowerMock.mockStatic(Executors.class);
        final ScheduledExecutorService cleanupSchedule = EasyMock.niceMock(ScheduledExecutorService.class);
        final ScheduledExecutorService rocksDBMetricsRecordingTriggerThread =
            EasyMock.mock(ScheduledExecutorService.class);
        EasyMock.expect(Executors.newSingleThreadScheduledExecutor(
            anyObject(ThreadFactory.class)
        )).andReturn(cleanupSchedule);
        PowerMock.replay(Executors.class, rocksDBMetricsRecordingTriggerThread, cleanupSchedule);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.table("topic", Materialized.as("store"));
        props.setProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, RecordingLevel.INFO.name());

        try (final KafkaStreams streams = new KafkaStreams(getBuilderWithSource().build(), props, supplier, time)) {
            streams.start();
        }

        PowerMock.verify(Executors.class, rocksDBMetricsRecordingTriggerThread);
    }

    @Test
    public void shouldWarnAboutRocksDBConfigSetterIsNotGuaranteedToBeBackwardsCompatible() {
        props.setProperty(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, MockRocksDbConfigSetter.class.getName());

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister()) {
            new KafkaStreams(getBuilderWithSource().build(), props, supplier, time);

            assertThat(appender.getMessages(), hasItem("stream-client [" + CLIENT_ID + "] "
                + "RocksDB's version will be bumped to version 6+ via KAFKA-8897 in a future release. "
                + "If you use `org.rocksdb.CompactionOptionsFIFO#setTtl(long)` or `#ttl()` you will need to rewrite "
                + "your code after KAFKA-8897 is resolved and set TTL via `org.rocksdb.Options` "
                + "(or `org.rocksdb.ColumnFamilyOptions`)."));
        }
    }

    @Test
    public void shouldCleanupOldStateDirs() throws Exception {
        PowerMock.mockStatic(Executors.class);
        final ScheduledExecutorService cleanupSchedule = EasyMock.mock(ScheduledExecutorService.class);
        EasyMock.expect(Executors.newSingleThreadScheduledExecutor(
            anyObject(ThreadFactory.class)
        )).andReturn(cleanupSchedule).anyTimes();
        EasyMock.expect(cleanupSchedule.scheduleAtFixedRate(
            EasyMock.anyObject(Runnable.class),
            EasyMock.eq(1L),
            EasyMock.eq(1L),
            EasyMock.eq(TimeUnit.MILLISECONDS)
        )).andReturn(null);
        EasyMock.expect(cleanupSchedule.shutdownNow()).andReturn(null);
        PowerMock.expectNew(StateDirectory.class,
            anyObject(StreamsConfig.class),
            anyObject(Time.class),
            EasyMock.eq(true)
        ).andReturn(stateDirectory);
        EasyMock.expect(stateDirectory.initializeProcessId()).andReturn(UUID.randomUUID());
        stateDirectory.close();
        PowerMock.replayAll(Executors.class, cleanupSchedule, stateDirectory);

        props.setProperty(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, "1");
        final StreamsBuilder builder = new StreamsBuilder();
        builder.table("topic", Materialized.as("store"));

        try (final KafkaStreams streams = new KafkaStreams(builder.build(), props, supplier, time)) {
            streams.start();
        }

        PowerMock.verify(Executors.class, cleanupSchedule);
    }

    @Test
    public void statelessTopologyShouldNotCreateStateDirectory() throws Exception {
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
    public void inMemoryStatefulTopologyShouldNotCreateStateDirectory() throws Exception {
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
    public void statefulTopologyShouldCreateStateDirectory() throws Exception {
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
        final KafkaStreams streams = new KafkaStreams(builder.build(), props, supplier, time);

        assertThat(streams.threads.size(), equalTo(0));
    }

    @Test
    public void shouldTransitToRunningWithGlobalOnlyTopology() throws InterruptedException {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.globalTable("anyTopic");
        final KafkaStreams streams = new KafkaStreams(builder.build(), props, supplier, time);

        assertThat(streams.threads.size(), equalTo(0));
        assertEquals(streams.state(), KafkaStreams.State.CREATED);

        streams.start();
        TestUtils.waitForCondition(
            () -> streams.state() == KafkaStreams.State.RUNNING,
            "Streams never started, state is " + streams.state());

        streams.close();

        TestUtils.waitForCondition(
            () -> streams.state() == KafkaStreams.State.NOT_RUNNING,
            "Streams never stopped.");
    }

    @SuppressWarnings("unchecked")
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

    private void startStreamsAndCheckDirExists(final Topology topology,
                                               final boolean shouldFilesExist) throws Exception {
        PowerMock.expectNew(StateDirectory.class,
            anyObject(StreamsConfig.class),
            anyObject(Time.class),
            EasyMock.eq(shouldFilesExist)
        ).andReturn(stateDirectory);
        EasyMock.expect(stateDirectory.initializeProcessId()).andReturn(UUID.randomUUID());

        PowerMock.replayAll();

        new KafkaStreams(topology, props, supplier, time);

        PowerMock.verifyAll();
    }
}
