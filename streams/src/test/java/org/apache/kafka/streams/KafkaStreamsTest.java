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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.internals.GlobalStreamThread;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.StreamsMetadataState;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockMetricsReporter;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.TestUtils;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singletonList;
import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({IntegrationTest.class})
@RunWith(PowerMockRunner.class)
@PrepareForTest({KafkaStreams.class, StreamThread.class})
public class KafkaStreamsTest {

    private static final int NUM_THREADS = 2;

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

    private StateListenerStub streamsStateListener;
    private Capture<StreamThread.StateListener> threadStatelistenerCapture;


    @Mock
    private Metrics metrics;

    @Before
    public void before() throws Exception {
        time = new MockTime();
        supplier = new MockClientSupplier();
        supplier.setClusterForAdminClient(Cluster.bootstrap(singletonList(new InetSocketAddress("localhost", 9999))));
        streamsStateListener = new StateListenerStub();
        threadStatelistenerCapture = EasyMock.newCapture();

        props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "clientId");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2018");
        props.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, NUM_THREADS);

        prepareStreams();
    }

    /*
    // We need this to avoid the KafkaConsumer hanging on poll
    // (this may occur if the test doesn't complete quickly enough)
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final StreamsBuilder builder = new StreamsBuilder();
    private KafkaStreams globalStreams;

    @Before
    public void before() {
        props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "clientId");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, NUM_THREADS);
        globalStreams = new KafkaStreams(builder.build(), props);
    }

    @After
    public void cleanup() {
        if (globalStreams != null) {
            globalStreams.close();
        }
    }
    */

    private void prepareStreams() throws Exception {
        // setup metrics
        PowerMock.expectNew(Metrics.class,
            anyObject(MetricConfig.class),
            anyObject(List.class),
            EasyMock.eq(time)
        ).andReturn(metrics);

        // setup stream threads
        PowerMock.mockStatic(StreamThread.class);
        EasyMock.expect(StreamThread.create(
            anyObject(InternalTopologyBuilder.class),
            anyObject(StreamsConfig.class),
            anyObject(KafkaClientSupplier.class),
            anyObject(Admin.class),
            anyObject(UUID.class),
            anyObject(String.class),
            anyObject(Metrics.class),
            anyObject(Time.class),
            anyObject(StreamsMetadataState.class),
            anyLong(),
            anyObject(StateDirectory.class),
            anyObject(StateRestoreListener.class),
            anyInt()
        )).andReturn(streamThreadOne).andReturn(streamThreadTwo);
        EasyMock.expect(StreamThread.getSharedAdminClientId(
            anyString()
        )).andReturn("admin");

        EasyMock.expect(streamThreadOne.getId()).andReturn(0L).anyTimes();
        EasyMock.expect(streamThreadTwo.getId()).andReturn(1L).anyTimes();
        prepareStreamThread(streamThreadOne);
        prepareStreamThread(streamThreadTwo);

        // setup global threads
        final AtomicReference<GlobalStreamThread.State> globalThreadState = new AtomicReference<>(GlobalStreamThread.State.CREATED);
        PowerMock.expectNew(GlobalStreamThread.class,
            anyObject(ProcessorTopology.class),
            anyObject(StreamsConfig.class),
            anyObject(Consumer.class),
            anyObject(StateDirectory.class),
            anyLong(),
            anyObject(Metrics.class),
            anyObject(Time.class),
            anyString(),
            anyObject(StateRestoreListener.class)
        ).andReturn(globalStreamThread);
        EasyMock.expect(globalStreamThread.state()).andAnswer(globalThreadState::get).anyTimes();
        globalStreamThread.setStateListener(EasyMock.capture(threadStatelistenerCapture));
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
            for (final MockProducer producer : supplier.producers) {
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

        // setup metrics
        metrics.close();
        EasyMock.expectLastCall().anyTimes();

        PowerMock.replayAll();
    }

    private void prepareStreamThread(StreamThread thread) throws Exception {
        final AtomicReference<StreamThread.State> state = new AtomicReference<>(StreamThread.State.CREATED);
        EasyMock.expect(thread.state()).andAnswer(state::get).anyTimes();

        thread.setStateListener(EasyMock.capture(threadStatelistenerCapture));
        EasyMock.expectLastCall().anyTimes();

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
        thread.shutdown();
        EasyMock.expectLastCall().andAnswer(() -> {
            supplier.consumer.close();
            supplier.restoreConsumer.close();
            for (final MockProducer producer : supplier.producers) {
                producer.close();
            }
            state.set(StreamThread.State.DEAD);
            threadStatelistenerCapture.getValue().onChange(thread, StreamThread.State.PENDING_SHUTDOWN, StreamThread.State.RUNNING);
            threadStatelistenerCapture.getValue().onChange(thread, StreamThread.State.DEAD, StreamThread.State.PENDING_SHUTDOWN);
            return null;
        }).anyTimes();
        EasyMock.expect(thread.isRunning()).andReturn(state.get() == StreamThread.State.RUNNING).anyTimes();
        thread.join();
        EasyMock.expectLastCall().anyTimes();

    }

    @Test
    public void testShouldTransitToNotRunningIfCloseRightAfterCreated() {
        KafkaStreams streams = new KafkaStreams(new StreamsBuilder().build(), props, supplier, time);
        streams.close();

        Assert.assertEquals(KafkaStreams.State.NOT_RUNNING, streams.state());
    }

    @Test
    public void stateShouldTransitToRunningIfNonDeadThreadsBackToRunning() throws InterruptedException {
        KafkaStreams streams = new KafkaStreams(new StreamsBuilder().build(), props, supplier, time);
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
            streams.threads[NUM_THREADS - 1],
            StreamThread.State.PENDING_SHUTDOWN,
            StreamThread.State.PARTITIONS_ASSIGNED);

        threadStatelistenerCapture.getValue().onChange(
            streams.threads[NUM_THREADS - 1],
            StreamThread.State.DEAD,
            StreamThread.State.PENDING_SHUTDOWN);

        Assert.assertEquals(3, streamsStateListener.numChanges);
        Assert.assertEquals(KafkaStreams.State.REBALANCING, streams.state());

        for (final StreamThread thread : streams.threads) {
            if (thread != streams.threads[NUM_THREADS - 1]) {
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
    public void stateShouldTransitToErrorIfAllThreadsDead() throws InterruptedException {
        KafkaStreams streams = new KafkaStreams(new StreamsBuilder().build(), props, supplier, time);
        streams.setStateListener(streamsStateListener);

        Assert.assertEquals(0, streamsStateListener.numChanges);
        Assert.assertEquals(KafkaStreams.State.CREATED, streams.state());

        streams.start();

        TestUtils.waitForCondition(
            () -> streamsStateListener.numChanges == 2,
            "Streams never started.");
        Assert.assertEquals(KafkaStreams.State.RUNNING, streams.state());

        for (final StreamThread thread : streams.threads) {
            threadStatelistenerCapture.getValue().onChange(
                thread,
                StreamThread.State.PARTITIONS_REVOKED,
                StreamThread.State.RUNNING);
        }

        Assert.assertEquals(3, streamsStateListener.numChanges);
        Assert.assertEquals(KafkaStreams.State.REBALANCING, streams.state());

        threadStatelistenerCapture.getValue().onChange(
            streams.threads[NUM_THREADS - 1],
            StreamThread.State.PENDING_SHUTDOWN,
            StreamThread.State.PARTITIONS_REVOKED);

        threadStatelistenerCapture.getValue().onChange(
            streams.threads[NUM_THREADS - 1],
            StreamThread.State.DEAD,
            StreamThread.State.PENDING_SHUTDOWN);

        Assert.assertEquals(3, streamsStateListener.numChanges);
        Assert.assertEquals(KafkaStreams.State.REBALANCING, streams.state());

        for (final StreamThread thread : streams.threads) {
            if (thread != streams.threads[NUM_THREADS - 1]) {
                threadStatelistenerCapture.getValue().onChange(
                    thread,
                    StreamThread.State.PENDING_SHUTDOWN,
                    StreamThread.State.PARTITIONS_REVOKED);

                threadStatelistenerCapture.getValue().onChange(
                    thread,
                    StreamThread.State.DEAD,
                    StreamThread.State.PENDING_SHUTDOWN);
            }
        }

        Assert.assertEquals(4, streamsStateListener.numChanges);
        Assert.assertEquals(KafkaStreams.State.ERROR, streams.state());

        streams.close();

        // the state should not stuck with ERROR, but transit to NOT_RUNNING in the end
        TestUtils.waitForCondition(
            () -> streamsStateListener.numChanges == 6,
            "Streams never closed.");
        Assert.assertEquals(KafkaStreams.State.NOT_RUNNING, streams.state());
    }

    @Test
    public void shouldCleanupResourcesOnCloseWithoutPreviousStart() throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.globalTable("anyTopic");

        final KafkaStreams streams = new KafkaStreams(builder.build(), props, supplier, time);
        streams.close();

        TestUtils.waitForCondition(
            () -> streams.state() == KafkaStreams.State.NOT_RUNNING,
            "Streams never stopped.");

        assertTrue(supplier.consumer.closed());
        assertTrue(supplier.restoreConsumer.closed());
        for (final MockProducer p : supplier.producers) {
            assertTrue(p.closed());
        }
    }

    @Test
    public void testStateThreadClose() throws Exception {
        // make sure we have the global state thread running too
        final StreamsBuilder builder = new StreamsBuilder();
        builder.globalTable("anyTopic");
        final KafkaStreams streams = new KafkaStreams(builder.build(), props, supplier, time);

        try {
            assertEquals(NUM_THREADS, streams.threads.length);
            assertEquals(streams.state(), KafkaStreams.State.CREATED);

            streams.start();
            TestUtils.waitForCondition(
                () -> streams.state() == KafkaStreams.State.RUNNING,
                "Streams never started.");

            for (int i = 0; i < NUM_THREADS; i++) {
                final StreamThread tmpThread = streams.threads[i];
                tmpThread.shutdown();
                TestUtils.waitForCondition(() -> tmpThread.state() == StreamThread.State.DEAD,
                    "Thread never stopped.");
                streams.threads[i].join();
            }
            TestUtils.waitForCondition(
                () -> streams.state() == KafkaStreams.State.ERROR,
                "Streams never stopped.");
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
        final StreamsBuilder builder = new StreamsBuilder();
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
            assertEquals(streams.state(), KafkaStreams.State.ERROR);
        } finally {
            streams.close();
        }

        assertEquals(streams.state(), KafkaStreams.State.NOT_RUNNING);
    }

    @Test
    public void globalThreadShouldTimeoutWhenBrokerConnectionCannotBeEstablished() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
        props.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, NUM_THREADS);

        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 200);

        // make sure we have the global state thread running too
        final StreamsBuilder builder = new StreamsBuilder();
        builder.globalTable("anyTopic");
        try (final KafkaStreams streams = new KafkaStreams(builder.build(), props, supplier, time)) {
            streams.start();
            fail("expected start() to time out and throw an exception.");
        } catch (final StreamsException expected) {
            // This is a result of not being able to connect to the broker.
            expected.printStackTrace();
        }
        // There's nothing to assert... We're testing that this operation actually completes.
    }

    /*
    @Test
    public void testLocalThreadCloseWithoutConnectingToBroker() {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
        props.setProperty(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, NUM_THREADS);

        // make sure we have the global state thread running too
        builder.table("anyTopic");
        try (final KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
            streams.start();
        }
        // There's nothing to assert... We're testing that this operation actually completes.
    }


    @Test
    public void testInitializesAndDestroysMetricsReporters() {
        final int oldInitCount = MockMetricsReporter.INIT_COUNT.get();

        try (final KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
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
        globalStreams.close();
        final int closeCount = MockMetricsReporter.CLOSE_COUNT.get();

        globalStreams.close();
        Assert.assertEquals("subsequent close() calls should do nothing",
            closeCount, MockMetricsReporter.CLOSE_COUNT.get());
    }

    @Test
    public void testCannotStartOnceClosed() {
        globalStreams.start();
        globalStreams.close();
        try {
            globalStreams.start();
            fail("Should have throw IllegalStateException");
        } catch (final IllegalStateException expected) {
            // this is ok
        } finally {
            globalStreams.close();
        }
    }

    @Test
    public void testCannotStartTwice() {
        globalStreams.start();

        try {
            globalStreams.start();
            fail("Should throw an IllegalStateException");
        } catch (final IllegalStateException e) {
            // this is ok
        } finally {
            globalStreams.close();
        }
    }

    @Test
    public void shouldNotSetGlobalRestoreListenerAfterStarting() {
        globalStreams.start();
        try {
            globalStreams.setGlobalStateRestoreListener(new MockStateRestoreListener());
            fail("Should throw an IllegalStateException");
        } catch (final IllegalStateException e) {
            // expected
        } finally {
            globalStreams.close();
        }
    }

    @Test
    public void shouldThrowExceptionSettingUncaughtExceptionHandlerNotInCreateState() {
        globalStreams.start();
        try {
            globalStreams.setUncaughtExceptionHandler(null);
            fail("Should throw IllegalStateException");
        } catch (final IllegalStateException e) {
            // expected
        }
    }

    @Test
    public void shouldThrowExceptionSettingStateListenerNotInCreateState() {
        globalStreams.start();
        try {
            globalStreams.setStateListener(null);
            fail("Should throw IllegalStateException");
        } catch (final IllegalStateException e) {
            // expected
        }
    }

    @Test
    public void testIllegalMetricsConfig() {
        props.setProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "illegalConfig");

        try {
            new KafkaStreams(builder.build(), props);
            fail("Should have throw ConfigException");
        } catch (final ConfigException expected) {}  // expected
    }*/

    /*
    @Test
    public void testLegalMetricsConfig() {
        props.setProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.INFO.toString());
        new KafkaStreams(builder.build(), props).close();

        props.setProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.DEBUG.toString());
        new KafkaStreams(builder.build(), props).close();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotGetAllTasksWhenNotRunning() {
        globalStreams.allMetadata();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotGetAllTasksWithStoreWhenNotRunning() {
        globalStreams.allMetadataForStore("store");
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotGetTaskWithKeyAndSerializerWhenNotRunning() {
        globalStreams.metadataForKey("store", "key", Serdes.String().serializer());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotGetTaskWithKeyAndPartitionerWhenNotRunning() {
        globalStreams.metadataForKey("store", "key", (topic, key, value, numPartitions) -> 0);
    }

    @Test
    public void shouldReturnFalseOnCloseWhenThreadsHaventTerminated() throws Exception {
        final AtomicBoolean keepRunning = new AtomicBoolean(true);
        KafkaStreams streams = null;
        try {
            final StreamsBuilder builder = new StreamsBuilder();
            final CountDownLatch latch = new CountDownLatch(1);
            final String topic = "input";
            CLUSTER.createTopics(topic);

            builder.stream(topic, Consumed.with(Serdes.String(), Serdes.String()))
                    .foreach((key, value) -> {
                        try {
                            latch.countDown();
                            while (keepRunning.get()) {
                                Thread.sleep(10);
                            }
                        } catch (final InterruptedException e) {
                            // no-op
                        }
                    });
            streams = new KafkaStreams(builder.build(), props);
            streams.start();
            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(topic,
                Collections.singletonList(new KeyValue<>("A", "A")),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    StringSerializer.class,
                    StringSerializer.class,
                    new Properties()),
                System.currentTimeMillis());

            assertTrue("Timed out waiting to receive single message", latch.await(30, TimeUnit.SECONDS));
            assertFalse(streams.close(Duration.ofMillis(10)));
        } finally {
            // stop the thread so we don't interfere with other tests etc
            keepRunning.set(false);
            if (streams != null) {
                streams.close();
            }
        }
    }

    @Test
    public void shouldReturnThreadMetadata() {
        globalStreams.start();
        final Set<ThreadMetadata> threadMetadata = globalStreams.localThreadsMetadata();
        assertNotNull(threadMetadata);
        assertEquals(2, threadMetadata.size());
        for (final ThreadMetadata metadata : threadMetadata) {
            assertTrue("#threadState() was: " + metadata.threadState() + "; expected either RUNNING, STARTING, PARTITIONS_REVOKED, PARTITIONS_ASSIGNED, or CREATED",
                asList("RUNNING", "STARTING", "PARTITIONS_REVOKED", "PARTITIONS_ASSIGNED", "CREATED").contains(metadata.threadState()));
            assertEquals(0, metadata.standbyTasks().size());
            assertEquals(0, metadata.activeTasks().size());
            final String threadName = metadata.threadName();
            assertTrue(threadName.startsWith("clientId-StreamThread-"));
            assertEquals(threadName + "-consumer", metadata.consumerClientId());
            assertEquals(threadName + "-restore-consumer", metadata.restoreConsumerClientId());
            assertEquals(Collections.singleton(threadName + "-producer"), metadata.producerClientIds());
            assertEquals("clientId-admin", metadata.adminClientId());
        }
    }

    @Test
    public void shouldAllowCleanupBeforeStartAndAfterClose() {
        try {
            globalStreams.cleanUp();
            globalStreams.start();
        } finally {
            globalStreams.close();
        }
        globalStreams.cleanUp();
    }

    @Test
    public void shouldThrowOnCleanupWhileRunning() throws InterruptedException {
        globalStreams.start();
        TestUtils.waitForCondition(
            () -> globalStreams.state() == KafkaStreams.State.RUNNING,
            "Streams never started.");

        try {
            globalStreams.cleanUp();
            fail("Should have thrown IllegalStateException");
        } catch (final IllegalStateException expected) {
            assertEquals("Cannot clean up while running.", expected.getMessage());
        }
    }

    @Test
    public void shouldCleanupOldStateDirs() throws InterruptedException {
        props.setProperty(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, "1");

        final String topic = "topic";
        CLUSTER.createTopic(topic);
        final StreamsBuilder builder = new StreamsBuilder();

        builder.table(topic, Materialized.as("store"));

        try (final KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
            final CountDownLatch latch = new CountDownLatch(1);
            streams.setStateListener((newState, oldState) -> {
                if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
                    latch.countDown();
                }
            });
            final String appDir = props.getProperty(StreamsConfig.STATE_DIR_CONFIG) + File.separator + props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
            final File oldTaskDir = new File(appDir, "10_1");
            assertTrue(oldTaskDir.mkdirs());

            streams.start();
            latch.await(30, TimeUnit.SECONDS);
            verifyCleanupStateDir(appDir, oldTaskDir);
            assertTrue(oldTaskDir.mkdirs());
            verifyCleanupStateDir(appDir, oldTaskDir);
        }
    }

    @Test
    public void shouldThrowOnNegativeTimeoutForClose() {
        try (final KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
            streams.close(Duration.ofMillis(-1L));
            fail("should not accept negative close parameter");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void shouldNotBlockInCloseForZeroDuration() throws InterruptedException {
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final Thread th = new Thread(() -> streams.close(Duration.ofMillis(0L)));

        th.start();

        try {
            th.join(30_000L);
            assertFalse(th.isAlive());
        } finally {
            streams.close();
        }
    }

    @Test
    public void statelessTopologyShouldNotCreateStateDirectory() throws Exception {
        final String inputTopic = testName.getMethodName() + "-input";
        final String outputTopic = testName.getMethodName() + "-output";
        final Topology topology = new Topology();
        topology.addSource("source", Serdes.String().deserializer(), Serdes.String().deserializer(), inputTopic)
                .addProcessor("process", () -> new AbstractProcessor<String, String>() {
                    @Override
                    public void process(final String key, final String value) {
                        if (value.length() % 2 == 0) {
                            context().forward(key, key + value);
                        }
                    }
                }, "source")
                .addSink("sink", outputTopic, new StringSerializer(), new StringSerializer(), "process");
        startStreamsAndCheckDirExists(topology, false);
    }

    @Test
    public void inMemoryStatefulTopologyShouldNotCreateStateDirectory() throws Exception {
        final String inputTopic = testName.getMethodName() + "-input";
        final String outputTopic = testName.getMethodName() + "-output";
        final String globalTopicName = testName.getMethodName() + "-global";
        final String storeName = testName.getMethodName() + "-counts";
        final String globalStoreName = testName.getMethodName() + "-globalStore";
        final Topology topology = getStatefulTopology(inputTopic, outputTopic, globalTopicName, storeName, globalStoreName, false);
        startStreamsAndCheckDirExists(topology, false);
    }

    @Test
    public void statefulTopologyShouldCreateStateDirectory() throws Exception {
        final String inputTopic = testName.getMethodName() + "-input";
        final String outputTopic = testName.getMethodName() + "-output";
        final String globalTopicName = testName.getMethodName() + "-global";
        final String storeName = testName.getMethodName() + "-counts";
        final String globalStoreName = testName.getMethodName() + "-globalStore";
        final Topology topology = getStatefulTopology(inputTopic, outputTopic, globalTopicName, storeName, globalStoreName, true);
        startStreamsAndCheckDirExists(topology, true);
    }

    @SuppressWarnings("unchecked")
    private Topology getStatefulTopology(final String inputTopic,
                                         final String outputTopic,
                                         final String globalTopicName,
                                         final String storeName,
                                         final String globalStoreName,
                                         final boolean isPersistentStore) throws Exception {
        CLUSTER.createTopics(inputTopic, outputTopic, globalTopicName);
        final StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(
            isPersistentStore ?
                Stores.persistentKeyValueStore(storeName)
                : Stores.inMemoryKeyValueStore(storeName),
            Serdes.String(),
            Serdes.Long());
        final Topology topology = new Topology();
        topology.addSource("source", Serdes.String().deserializer(), Serdes.String().deserializer(), inputTopic)
                .addProcessor("process", () -> new AbstractProcessor<String, String>() {
                    @Override
                    public void process(final String key, final String value) {
                        final KeyValueStore<String, Long> kvStore =
                                (KeyValueStore<String, Long>) context().getStateStore(storeName);
                        kvStore.put(key, 5L);

                        context().forward(key, "5");
                        context().commit();
                    }
                }, "source")
                .addStateStore(storeBuilder, "process")
                .addSink("sink", outputTopic, new StringSerializer(), new StringSerializer(), "process");

        final StoreBuilder<KeyValueStore<String, String>> globalStoreBuilder = Stores.keyValueStoreBuilder(
                isPersistentStore ? Stores.persistentKeyValueStore(globalStoreName) : Stores.inMemoryKeyValueStore(globalStoreName),
                Serdes.String(), Serdes.String()).withLoggingDisabled();
        topology.addGlobalStore(globalStoreBuilder,
                "global",
                Serdes.String().deserializer(),
                Serdes.String().deserializer(),
                globalTopicName,
                globalTopicName + "-processor",
                new MockProcessorSupplier());
        return topology;
    }

    private void startStreamsAndCheckDirExists(final Topology topology,
                                               final boolean shouldFilesExist) throws Exception {
        PowerMock.expectNew(StateDirectory.class,
            anyObject(StreamsConfig.class),
            anyObject(Time.class),
            EasyMock.eq(shouldFilesExist)
        );

        final Properties localProps = new Properties();
        localProps.putAll(props);
        final KafkaStreams streams = new KafkaStreams(topology, localProps);
        streams.start();
    }

    private void verifyCleanupStateDir(final String appDir,
                                       final File oldTaskDir) throws InterruptedException {
        final File taskDir = new File(appDir, "0_0");
        TestUtils.waitForCondition(
            () -> !oldTaskDir.exists() && taskDir.exists(),
            "cleanup has not successfully run");
        assertTrue(taskDir.exists());
    }

*/
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

    @Test
    public void inMemoryStatefulTopologyShouldNotCreateStateDirectory() throws Exception {
        final String inputTopic = testName.getMethodName() + "-input";
        final String outputTopic = testName.getMethodName() + "-output";
        final String globalTopicName = testName.getMethodName() + "-global";
        final String storeName = testName.getMethodName() + "-counts";
        final String globalStoreName = testName.getMethodName() + "-globalStore";
        final Topology topology = getStatefulTopology(inputTopic, outputTopic, globalTopicName, storeName, globalStoreName, false);
        startStreamsAndCheckDirExists(topology, false);
    }

    @Test
    public void statefulTopologyShouldCreateStateDirectory() throws Exception {
        final String inputTopic = testName.getMethodName() + "-input";
        final String outputTopic = testName.getMethodName() + "-output";
        final String globalTopicName = testName.getMethodName() + "-global";
        final String storeName = testName.getMethodName() + "-counts";
        final String globalStoreName = testName.getMethodName() + "-globalStore";
        final Topology topology = getStatefulTopology(inputTopic, outputTopic, globalTopicName, storeName, globalStoreName, true);
        startStreamsAndCheckDirExists(topology, true);
    }

    @SuppressWarnings("unchecked")
    private Topology getStatefulTopology(final String inputTopic,
                                         final String outputTopic,
                                         final String globalTopicName,
                                         final String storeName,
                                         final String globalStoreName,
                                         final boolean isPersistentStore) throws Exception {
        final StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(
            isPersistentStore ?
                Stores.persistentKeyValueStore(storeName)
                : Stores.inMemoryKeyValueStore(storeName),
            Serdes.String(),
            Serdes.Long());
        final Topology topology = new Topology();
        topology.addSource("source", Serdes.String().deserializer(), Serdes.String().deserializer(), inputTopic)
            .addProcessor("process", () -> new AbstractProcessor<String, String>() {
                @Override
                public void process(final String key, final String value) {
                    final KeyValueStore<String, Long> kvStore =
                        (KeyValueStore<String, Long>) context().getStateStore(storeName);
                    kvStore.put(key, 5L);

                    context().forward(key, "5");
                    context().commit();
                }
            }, "source")
            .addStateStore(storeBuilder, "process")
            .addSink("sink", outputTopic, new StringSerializer(), new StringSerializer(), "process");

        final StoreBuilder<KeyValueStore<String, String>> globalStoreBuilder = Stores.keyValueStoreBuilder(
            isPersistentStore ? Stores.persistentKeyValueStore(globalStoreName) : Stores.inMemoryKeyValueStore(globalStoreName),
            Serdes.String(), Serdes.String()).withLoggingDisabled();
        topology.addGlobalStore(globalStoreBuilder,
            "global",
            Serdes.String().deserializer(),
            Serdes.String().deserializer(),
            globalTopicName,
            globalTopicName + "-processor",
            new MockProcessorSupplier());
        return topology;
    }

    private void startStreamsAndCheckDirExists(final Topology topology,
                                               final boolean shouldFilesExist) throws Exception {
        PowerMock.expectNew(StateDirectory.class,
            anyObject(StreamsConfig.class),
            anyObject(Time.class),
            EasyMock.eq(shouldFilesExist)
        ).andReturn(stateDirectory);

        PowerMock.replayAll();

        new KafkaStreams(topology, props, supplier, time);

        PowerMock.verifyAll();
    }
}
