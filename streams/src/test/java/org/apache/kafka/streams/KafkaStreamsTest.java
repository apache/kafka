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

import java.time.Duration;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.ThreadMetadata;
import org.apache.kafka.streams.processor.internals.GlobalStreamThread;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockMetricsReporter;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({IntegrationTest.class})
public class KafkaStreamsTest {

    private static final int NUM_BROKERS = 1;
    private static final int NUM_THREADS = 2;
    // We need this to avoid the KafkaConsumer hanging on poll
    // (this may occur if the test doesn't complete quickly enough)
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final StreamsBuilder builder = new StreamsBuilder();
    private KafkaStreams globalStreams;
    private Properties props;

    @Before
    public void before() {
        props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, NUM_THREADS);
        props.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
        globalStreams = new KafkaStreams(builder.build(), props);
    }

    @After
    public void cleanup() {
        if (globalStreams != null) {
            globalStreams.close();
        }
    }

    @Test
    public void testOsDefaultSocketBufferSizes() {
        props.put(CommonClientConfigs.SEND_BUFFER_CONFIG, Selectable.USE_DEFAULT_BUFFER_SIZE);
        props.put(CommonClientConfigs.RECEIVE_BUFFER_CONFIG, Selectable.USE_DEFAULT_BUFFER_SIZE);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.close();
    }

    @Test(expected = KafkaException.class)
    public void testInvalidSocketSendBufferSize() {
        props.put(CommonClientConfigs.SEND_BUFFER_CONFIG, -2);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.close();
    }

    @Test(expected = KafkaException.class)
    public void testInvalidSocketReceiveBufferSize() {
        props.put(CommonClientConfigs.RECEIVE_BUFFER_CONFIG, -2);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.close();
    }

    @Test
    public void testStateChanges() throws InterruptedException {
        final StateListenerStub stateListener = new StateListenerStub();
        globalStreams.setStateListener(stateListener);

        Assert.assertEquals(globalStreams.state(), KafkaStreams.State.CREATED);
        Assert.assertEquals(stateListener.numChanges, 0);

        globalStreams.start();
        TestUtils.waitForCondition(
            () -> globalStreams.state() == KafkaStreams.State.RUNNING,
            10 * 1000,
            "Streams never started.");

        globalStreams.close();

        Assert.assertEquals(globalStreams.state(), KafkaStreams.State.NOT_RUNNING);
    }

    @Test
    public void testStateCloseAfterCreate() {
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);

        try {
            final StateListenerStub stateListener = new StateListenerStub();
            streams.setStateListener(stateListener);
        } finally {
            streams.close();
        }

        Assert.assertEquals(KafkaStreams.State.NOT_RUNNING, streams.state());
    }

    @Test
    public void shouldCleanupResourcesOnCloseWithoutPreviousStart() throws Exception {
        builder.globalTable("anyTopic");
        final List<Node> nodes = Arrays.asList(new Node(0, "localhost", 8121));
        final Cluster cluster = new Cluster("mockClusterId", nodes,
                                            Collections.emptySet(), Collections.<String>emptySet(),
                                            Collections.emptySet(), nodes.get(0));
        final MockClientSupplier clientSupplier = new MockClientSupplier();
        clientSupplier.setClusterForAdminClient(cluster);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props, clientSupplier);
        streams.close();
        TestUtils.waitForCondition(
            () -> streams.state() == KafkaStreams.State.NOT_RUNNING,
            10 * 1000,
            "Streams never stopped.");

        // Ensure that any created clients are closed
        assertTrue(clientSupplier.consumer.closed());
        assertTrue(clientSupplier.restoreConsumer.closed());
        for (final MockProducer p : clientSupplier.producers) {
            assertTrue(p.closed());
        }
    }

    @Test
    public void testStateThreadClose() throws Exception {
        // make sure we have the global state thread running too
        builder.globalTable("anyTopic");
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);

        try {
            final java.lang.reflect.Field threadsField = streams.getClass().getDeclaredField("threads");
            threadsField.setAccessible(true);
            final StreamThread[] threads = (StreamThread[]) threadsField.get(streams);

            assertEquals(NUM_THREADS, threads.length);
            assertEquals(streams.state(), KafkaStreams.State.CREATED);

            streams.start();
            TestUtils.waitForCondition(
                () -> streams.state() == KafkaStreams.State.RUNNING,
                10 * 1000,
                "Streams never started.");

            for (int i = 0; i < NUM_THREADS; i++) {
                final StreamThread tmpThread = threads[i];
                tmpThread.shutdown();
                TestUtils.waitForCondition(
                    () -> tmpThread.state() == StreamThread.State.DEAD,
                    10 * 1000,
                    "Thread never stopped.");
                threads[i].join();
            }
            TestUtils.waitForCondition(
                () -> streams.state() == KafkaStreams.State.ERROR,
                10 * 1000,
                "Streams never stopped.");
        } finally {
            streams.close();
        }

        TestUtils.waitForCondition(
            () -> streams.state() == KafkaStreams.State.NOT_RUNNING,
            10 * 1000,
            "Streams never stopped.");

        final java.lang.reflect.Field globalThreadField = streams.getClass().getDeclaredField("globalStreamThread");
        globalThreadField.setAccessible(true);
        final GlobalStreamThread globalStreamThread = (GlobalStreamThread) globalThreadField.get(streams);
        assertNull(globalStreamThread);
    }

    @Test
    public void testStateGlobalThreadClose() throws Exception {
        // make sure we have the global state thread running too
        builder.globalTable("anyTopic");
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);

        try {
            streams.start();
            TestUtils.waitForCondition(
                () -> streams.state() == KafkaStreams.State.RUNNING,
                10 * 1000,
                "Streams never started.");
            final java.lang.reflect.Field globalThreadField = streams.getClass().getDeclaredField("globalStreamThread");
            globalThreadField.setAccessible(true);
            final GlobalStreamThread globalStreamThread = (GlobalStreamThread) globalThreadField.get(streams);
            globalStreamThread.shutdown();
            TestUtils.waitForCondition(
                () -> globalStreamThread.state() == GlobalStreamThread.State.DEAD,
                10 * 1000,
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
        builder.globalTable("anyTopic");
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        try {
            streams.start();
            fail("expected start() to time out and throw an exception.");
        } catch (final StreamsException expected) {
            // This is a result of not being able to connect to the broker.
        } finally {
            streams.close();
        }
        // There's nothing to assert... We're testing that this operation actually completes.
    }

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
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        try {
            streams.start();
        } finally {
            streams.close();
        }
        // There's nothing to assert... We're testing that this operation actually completes.
    }


    @Test
    public void testInitializesAndDestroysMetricsReporters() {
        final int oldInitCount = MockMetricsReporter.INIT_COUNT.get();
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);

        try {
            final int newInitCount = MockMetricsReporter.INIT_COUNT.get();
            final int initDiff = newInitCount - oldInitCount;
            assertTrue("some reporters should be initialized by calling on construction", initDiff > 0);

            streams.start();
            final int oldCloseCount = MockMetricsReporter.CLOSE_COUNT.get();
            streams.close();
            assertEquals(oldCloseCount + initDiff, MockMetricsReporter.CLOSE_COUNT.get());
        } finally {
            streams.close();
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
        } catch (final ConfigException expected) { /* expected */ }
    }

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
            assertTrue("#threadState() was: " + metadata.threadState() + "; expected either RUNNING, PARTITIONS_REVOKED, PARTITIONS_ASSIGNED, or CREATED",
                Utils.mkList("RUNNING", "PARTITIONS_REVOKED", "PARTITIONS_ASSIGNED", "CREATED").contains(metadata.threadState()));
            assertEquals(0, metadata.standbyTasks().size());
            assertEquals(0, metadata.activeTasks().size());
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
            10 * 1000,
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

        final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        builder.table(topic, consumed);

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        try {
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
        } finally {
            streams.close();
        }
    }

    @Test
    public void shouldThrowOnNegativeTimeoutForClose() {
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        try {
            streams.close(Duration.ofMillis(-1L));
            fail("should not accept negative close parameter");
        } catch (final IllegalArgumentException e) {
            // expected
        } finally {
            streams.close();
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

    private void verifyCleanupStateDir(final String appDir, final File oldTaskDir) throws InterruptedException {
        final File taskDir = new File(appDir, "0_0");
        TestUtils.waitForCondition(
            () -> !oldTaskDir.exists() && taskDir.exists(),
            30000,
            "cleanup has not successfully run");
        assertTrue(taskDir.exists());
    }

    public static class StateListenerStub implements KafkaStreams.StateListener {
        int numChanges = 0;
        KafkaStreams.State oldState;
        KafkaStreams.State newState;
        public Map<KafkaStreams.State, Long> mapStates = new HashMap<>();

        @Override
        public void onChange(final KafkaStreams.State newState, final KafkaStreams.State oldState) {
            final long prevCount = mapStates.containsKey(newState) ? mapStates.get(newState) : 0;
            numChanges++;
            this.oldState = oldState;
            this.newState = newState;
            mapStates.put(newState, prevCount + 1);
        }
    }

}
