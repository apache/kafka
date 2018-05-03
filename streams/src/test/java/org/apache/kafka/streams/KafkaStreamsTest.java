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

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.ThreadMetadata;
import org.apache.kafka.streams.processor.internals.GlobalStreamThread;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockMetricsReporter;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({IntegrationTest.class})
public class KafkaStreamsTest {

    private static final int NUM_BROKERS = 1;
    private static final int NUM_THREADS = 2;
    // We need this to avoid the KafkaConsumer hanging on poll (this may occur if the test doesn't complete
    // quick enough)
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final StreamsBuilder builder = new StreamsBuilder();
    private KafkaStreams streams;
    private Properties props;

    @Before
    public void before() {
        props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.setProperty(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, NUM_THREADS);
        streams = new KafkaStreams(builder.build(), props);
    }

    @Test
    public void testStateChanges() throws InterruptedException {
        final StreamsBuilder builder = new StreamsBuilder();
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);

        final StateListenerStub stateListener = new StateListenerStub();
        streams.setStateListener(stateListener);
        Assert.assertEquals(streams.state(), KafkaStreams.State.CREATED);
        Assert.assertEquals(stateListener.numChanges, 0);

        streams.start();
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return streams.state() == KafkaStreams.State.RUNNING;
            }
        }, 10 * 1000, "Streams never started.");
        streams.close();
        Assert.assertEquals(streams.state(), KafkaStreams.State.NOT_RUNNING);
    }

    @Test
    public void testStateCloseAfterCreate() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);

        final StateListenerStub stateListener = new StateListenerStub();
        streams.setStateListener(stateListener);
        streams.close();
        Assert.assertEquals(KafkaStreams.State.NOT_RUNNING, streams.state());
    }

    @Test
    public void shouldCleanupResourcesOnCloseWithoutPreviousStart() throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.globalTable("anyTopic");
        List<Node> nodes = Arrays.asList(new Node(0, "localhost", 8121));
        Cluster cluster = new Cluster("mockClusterId", nodes,
            Collections.<PartitionInfo>emptySet(), Collections.<String>emptySet(),
            Collections.<String>emptySet(), nodes.get(0));
        MockClientSupplier clientSupplier = new MockClientSupplier();
        clientSupplier.setClusterForAdminClient(cluster);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props, clientSupplier);
        streams.close();
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return streams.state() == KafkaStreams.State.NOT_RUNNING;
            }
        }, 10 * 1000, "Streams never stopped.");

        // Ensure that any created clients are closed
        assertTrue(clientSupplier.consumer.closed());
        assertTrue(clientSupplier.restoreConsumer.closed());
        for (MockProducer p : clientSupplier.producers) {
            assertTrue(p.closed());
        }
    }

    @Test
    public void testStateThreadClose() throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        // make sure we have the global state thread running too
        builder.globalTable("anyTopic");
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);



        final java.lang.reflect.Field threadsField = streams.getClass().getDeclaredField("threads");
        threadsField.setAccessible(true);
        final StreamThread[] threads = (StreamThread[]) threadsField.get(streams);

        assertEquals(NUM_THREADS, threads.length);
        assertEquals(streams.state(), KafkaStreams.State.CREATED);

        streams.start();
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return streams.state() == KafkaStreams.State.RUNNING;
            }
        }, 10 * 1000, "Streams never started.");

        for (int i = 0; i < NUM_THREADS; i++) {
            final StreamThread tmpThread = threads[i];
            tmpThread.shutdown();
            TestUtils.waitForCondition(new TestCondition() {
                @Override
                public boolean conditionMet() {
                    return tmpThread.state() == StreamThread.State.DEAD;
                }
            }, 10 * 1000, "Thread never stopped.");
            threads[i].join();
        }
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return streams.state() == KafkaStreams.State.ERROR;
            }
        }, 10 * 1000, "Streams never stopped.");
        streams.close();
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return streams.state() == KafkaStreams.State.NOT_RUNNING;
            }
        }, 10 * 1000, "Streams never stopped.");

        final java.lang.reflect.Field globalThreadField = streams.getClass().getDeclaredField("globalStreamThread");
        globalThreadField.setAccessible(true);
        final GlobalStreamThread globalStreamThread = (GlobalStreamThread) globalThreadField.get(streams);
        assertEquals(globalStreamThread, null);
    }

    @Test
    public void testStateGlobalThreadClose() throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        // make sure we have the global state thread running too
        builder.globalTable("anyTopic");
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);


        streams.start();
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return streams.state() == KafkaStreams.State.RUNNING;
            }
        }, 10 * 1000, "Streams never started.");
        final java.lang.reflect.Field globalThreadField = streams.getClass().getDeclaredField("globalStreamThread");
        globalThreadField.setAccessible(true);
        final GlobalStreamThread globalStreamThread = (GlobalStreamThread) globalThreadField.get(streams);
        globalStreamThread.shutdown();
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return globalStreamThread.state() == GlobalStreamThread.State.DEAD;
            }
        }, 10 * 1000, "Thread never stopped.");
        globalStreamThread.join();
        assertEquals(streams.state(), KafkaStreams.State.ERROR);

        streams.close();
        assertEquals(streams.state(), KafkaStreams.State.NOT_RUNNING);
    }

    @Ignore // this test cannot pass as long as GST blocks KS.start()
    @Test
    public void testGlobalThreadCloseWithoutConnectingToBroker() {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
        props.setProperty(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, NUM_THREADS);

        final StreamsBuilder builder = new StreamsBuilder();
        // make sure we have the global state thread running too
        builder.globalTable("anyTopic");
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        streams.close();
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

        final StreamsBuilder builder = new StreamsBuilder();
        // make sure we have the global state thread running too
        builder.table("anyTopic");
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        streams.close();
        // There's nothing to assert... We're testing that this operation actually completes.
    }


    @Test
    public void testInitializesAndDestroysMetricsReporters() {
        final int oldInitCount = MockMetricsReporter.INIT_COUNT.get();
        final StreamsBuilder builder = new StreamsBuilder();
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final int newInitCount = MockMetricsReporter.INIT_COUNT.get();
        final int initDiff = newInitCount - oldInitCount;
        assertTrue("some reporters should be initialized by calling on construction", initDiff > 0);

        streams.start();
        final int oldCloseCount = MockMetricsReporter.CLOSE_COUNT.get();
        streams.close();
        assertEquals(oldCloseCount + initDiff, MockMetricsReporter.CLOSE_COUNT.get());
    }

    @Test
    public void testCloseIsIdempotent() {
        streams.close();
        final int closeCount = MockMetricsReporter.CLOSE_COUNT.get();

        streams.close();
        Assert.assertEquals("subsequent close() calls should do nothing",
            closeCount, MockMetricsReporter.CLOSE_COUNT.get());
    }

    @Test
    public void testCannotStartOnceClosed() {
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
    public void testCannotStartTwice() {
        streams.start();

        try {
            streams.start();
        } catch (final IllegalStateException e) {
            // this is ok
        } finally {
            streams.close();
        }
    }

    @Test
    public void shouldNotSetGlobalRestoreListenerAfterStarting() {
        streams.start();
        try {
            streams.setGlobalStateRestoreListener(new MockStateRestoreListener());
            fail("Should throw an IllegalStateException");
        } catch (final IllegalStateException e) {
            // expected
        } finally {
            streams.close();
        }
    }

    @Test
    public void shouldThrowExceptionSettingUncaughtExceptionHandlerNotInCreateState() {
        streams.start();
        try {
            streams.setUncaughtExceptionHandler(null);
            fail("Should throw IllegalStateException");
        } catch (final IllegalStateException e) {
            // expected
        } finally {
            streams.close();
        }
    }

    @Test
    public void shouldThrowExceptionSettingStateListenerNotInCreateState() {
        streams.start();
        try {
            streams.setStateListener(null);
            fail("Should throw IllegalStateException");
        } catch (final IllegalStateException e) {
            // expected
        } finally {
            streams.close();
        }
    }

    @Test
    public void testIllegalMetricsConfig() {
        props.setProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "illegalConfig");
        final StreamsBuilder builder = new StreamsBuilder();

        try {
            new KafkaStreams(builder.build(), props);
            fail("Should have throw ConfigException");
        } catch (final ConfigException expected) { /* expected */ }
    }

    @Test
    public void testLegalMetricsConfig() {
        props.setProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.INFO.toString());
        final StreamsBuilder builder1 = new StreamsBuilder();
        final KafkaStreams streams1 = new KafkaStreams(builder1.build(), props);
        streams1.close();

        props.setProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.DEBUG.toString());
        final StreamsBuilder builder2 = new StreamsBuilder();
        new KafkaStreams(builder2.build(), props);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotGetAllTasksWhenNotRunning() {
        streams.allMetadata();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotGetAllTasksWithStoreWhenNotRunning() {
        streams.allMetadataForStore("store");
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotGetTaskWithKeyAndSerializerWhenNotRunning() {
        streams.metadataForKey("store", "key", Serdes.String().serializer());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotGetTaskWithKeyAndPartitionerWhenNotRunning() {
        streams.metadataForKey("store", "key", new StreamPartitioner<String, Object>() {
            @Override
            public Integer partition(final String key, final Object value, final int numPartitions) {
                return 0;
            }
        });
    }

    @Test
    public void shouldReturnFalseOnCloseWhenThreadsHaventTerminated() throws Exception {
        final AtomicBoolean keepRunning = new AtomicBoolean(true);
        try {
            final StreamsBuilder builder = new StreamsBuilder();
            final CountDownLatch latch = new CountDownLatch(1);
            final String topic = "input";
            CLUSTER.createTopic(topic);

            builder.stream(topic, Consumed.with(Serdes.String(), Serdes.String()))
                    .foreach(new ForeachAction<String, String>() {
                        @Override
                        public void apply(final String key, final String value) {
                            try {
                                latch.countDown();
                                while (keepRunning.get()) {
                                    Thread.sleep(10);
                                }
                            } catch (final InterruptedException e) {
                                // no-op
                            }
                        }
                    });
            final KafkaStreams streams = new KafkaStreams(builder.build(), props);
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
            assertFalse(streams.close(10, TimeUnit.MILLISECONDS));
        } finally {
            // stop the thread so we don't interfere with other tests etc
            keepRunning.set(false);
        }
    }

    @Test
    public void shouldReturnThreadMetadata() {
        streams.start();
        Set<ThreadMetadata> threadMetadata = streams.localThreadsMetadata();
        assertNotNull(threadMetadata);
        assertEquals(2, threadMetadata.size());
        for (ThreadMetadata metadata : threadMetadata) {
            assertTrue("#threadState() was: " + metadata.threadState() + "; expected either RUNNING, PARTITIONS_REVOKED, PARTITIONS_ASSIGNED, or CREATED",
                Utils.mkList("RUNNING", "PARTITIONS_REVOKED", "PARTITIONS_ASSIGNED", "CREATED").contains(metadata.threadState()));
            assertEquals(0, metadata.standbyTasks().size());
            assertEquals(0, metadata.activeTasks().size());
        }
        streams.close();
    }

    @Test
    public void testCleanup() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.cleanUp();
        streams.start();
        streams.close();
        streams.cleanUp();
    }

    @Test
    public void testCannotCleanupWhileRunning() throws InterruptedException {
        final StreamsBuilder builder = new StreamsBuilder();
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.start();
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return streams.state() == KafkaStreams.State.RUNNING;
            }
        }, 10 * 1000, "Streams never started.");
        try {
            streams.cleanUp();
            fail("Should have thrown IllegalStateException");
        } catch (final IllegalStateException expected) {
            assertEquals("Cannot clean up while running.", expected.getMessage());
        } finally {
            streams.close();
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testToString() {
        streams.start();
        final String streamString = streams.toString();
        streams.close();
        final String appId = streamString.split("\\n")[1].split(":")[1].trim();
        Assert.assertNotEquals("streamString should not be empty", "", streamString);
        Assert.assertNotNull("streamString should not be null", streamString);
        Assert.assertNotEquals("streamString contains non-empty appId", "", appId);
        Assert.assertNotNull("streamString contains non-null appId", appId);
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
        final CountDownLatch latch = new CountDownLatch(1);
        streams.setStateListener(new KafkaStreams.StateListener() {
            @Override
            public void onChange(final KafkaStreams.State newState, final KafkaStreams.State oldState) {
                if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
                    latch.countDown();
                }
            }
        });
        final String appDir = props.getProperty(StreamsConfig.STATE_DIR_CONFIG) + File.separator + props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
        final File oldTaskDir = new File(appDir, "10_1");
        assertTrue(oldTaskDir.mkdirs());
        try {
            streams.start();
            latch.await(30, TimeUnit.SECONDS);
            verifyCleanupStateDir(appDir, oldTaskDir);
            assertTrue(oldTaskDir.mkdirs());
            verifyCleanupStateDir(appDir, oldTaskDir);
        } finally {
            streams.close();
        }
    }

    private void verifyCleanupStateDir(final String appDir, final File oldTaskDir) throws InterruptedException {
        final File taskDir = new File(appDir, "0_0");
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return !oldTaskDir.exists() && taskDir.exists();
            }
        }, 30000, "cleanup has not successfully run");
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
