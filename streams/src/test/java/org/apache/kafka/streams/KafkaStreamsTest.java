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

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.internals.GlobalStreamThread;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockMetricsReporter;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

    private final KStreamBuilder builder = new KStreamBuilder();
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
        streams = new KafkaStreams(builder, props);
    }

    @Test
    public void testStateChanges() throws Exception {
        StateListenerStub stateListener = new StateListenerStub();
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
    public void testStateCloseAfterCreate() throws Exception {
        StateListenerStub stateListener = new StateListenerStub();
        streams.setStateListener(stateListener);
        streams.close();
        Assert.assertEquals(streams.state(), KafkaStreams.State.NOT_RUNNING);
    }

    private void testStateThreadCloseHelper(final int numThreads) throws Exception {
        final java.lang.reflect.Field threadsField = streams.getClass().getDeclaredField("threads");
        threadsField.setAccessible(true);
        final StreamThread[] threads = (StreamThread[]) threadsField.get(streams);

        assertEquals(numThreads, threads.length);
        assertEquals(streams.state(), KafkaStreams.State.CREATED);

        streams.start();
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return streams.state() == KafkaStreams.State.RUNNING;
            }
        }, 10 * 1000, "Streams never started.");

        for (int i = 0; i < numThreads; i++) {
            final StreamThread tmpThread = threads[i];
            tmpThread.close();
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
        GlobalStreamThread globalStreamThread = (GlobalStreamThread) globalThreadField.get(streams);
        assertEquals(globalStreamThread, null);
    }

    @Test
    public void testStateThreadClose() throws Exception {
        // make sure we have the global state thread running too
        builder.globalTable("anyTopic");

        streams = new KafkaStreams(new KStreamBuilder(), props);

        testStateThreadCloseHelper(NUM_THREADS);
    }
    
    @Test
    public void testStateGlobalThreadClose() throws Exception {
        final KStreamBuilder builder = new KStreamBuilder();
        // make sure we have the global state thread running too
        builder.globalTable("anyTopic");
        streams = new KafkaStreams(builder, props);

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
        globalStreamThread.close();
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

    @Test
    public void testInitializesAndDestroysMetricsReporters() throws Exception {
        final int oldInitCount = MockMetricsReporter.INIT_COUNT.get();
        final KStreamBuilder builder = new KStreamBuilder();
        final KafkaStreams streams = new KafkaStreams(builder, props);
        final int newInitCount = MockMetricsReporter.INIT_COUNT.get();
        final int initDiff = newInitCount - oldInitCount;
        assertTrue("some reporters should be initialized by calling on construction", initDiff > 0);

        streams.start();
        final int oldCloseCount = MockMetricsReporter.CLOSE_COUNT.get();
        streams.close();
        assertEquals(oldCloseCount + initDiff, MockMetricsReporter.CLOSE_COUNT.get());
    }


    @Test
    public void testCloseIsIdempotent() throws Exception {
        streams.close();
        final int closeCount = MockMetricsReporter.CLOSE_COUNT.get();

        streams.close();
        Assert.assertEquals("subsequent close() calls should do nothing",
            closeCount, MockMetricsReporter.CLOSE_COUNT.get());
    }

    @Test
    public void testCannotStartOnceClosed() throws Exception {
        streams.start();
        streams.close();
        try {
            streams.start();
            fail("Should have thrown IllegalStateException.");
        } catch (final IllegalStateException expected) {
            // ignore
        } finally {
            streams.close();
        }
    }

    @Test
    public void testCannotStartTwice() throws Exception {
        streams.start();

        try {
            streams.start();
            fail("Should have thrown IllegalStateException.");
        } catch (final IllegalStateException expected) {
            // ignore
        } finally {
            streams.close();
        }
    }

    @Test
    public void testNumberDefaultMetrics() {
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        streams = new KafkaStreams(builder, props);
        final Map<MetricName, ? extends Metric> metrics = streams.metrics();
        // all 15 default StreamThread metrics + 1 metric that keeps track of number of metrics
        assertEquals(metrics.size(), 16);
    }

    @Test
    public void testIllegalMetricsConfig() {
        props.setProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "illegalConfig");

        try {
            new KafkaStreams(builder, props);
            fail("Should have thrown ConfigException.");
        } catch (final ConfigException expected) { /* ignore */ }
    }

    @Test
    public void testLegalMetricsConfig() {
        props.setProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.INFO.toString());
        final KafkaStreams streams1 = new KafkaStreams(builder, props);
        streams1.close();

        props.setProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.DEBUG.toString());
        new KafkaStreams(builder, props);

    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotGetAllTasksWhenNotRunning() throws Exception {
        streams.allMetadata();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotGetAllTasksWithStoreWhenNotRunning() throws Exception {
        streams.allMetadataForStore("store");
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotGetTaskWithKeyAndSerializerWhenNotRunning() throws Exception {
        streams.metadataForKey("store", "key", Serdes.String().serializer());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotGetTaskWithKeyAndPartitionerWhenNotRunning() throws Exception {
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
            final KStreamBuilder builder = new KStreamBuilder();
            final CountDownLatch latch = new CountDownLatch(1);
            final String topic = "input";
            CLUSTER.createTopic(topic);

            builder.stream(Serdes.String(), Serdes.String(), topic)
                    .foreach(new ForeachAction<String, String>() {
                        @Override
                        public void apply(final String key, final String value) {
                            try {
                                latch.countDown();
                                while (keepRunning.get()) {
                                    Thread.sleep(10);
                                }
                            } catch (InterruptedException e) {
                                // no-op
                            }
                        }
                    });

            streams = new KafkaStreams(builder, props);
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
    public void testCleanup() throws Exception {
        streams.cleanUp();
        streams.start();
        streams.close();
        streams.cleanUp();
    }

    @Test
    public void testCannotCleanupWhileRunning() throws Exception {
        streams.start();
        try {
            streams.cleanUp();
            fail("Should have thrown IllegalStateException.");
        } catch (final IllegalStateException expected) {
            // ignore
        } finally {
            streams.close();
        }
    }

    @Test
    public void testToString() {
        streams.start();
        String streamString = streams.toString();
        streams.close();
        String appId = streamString.split("\\n")[1].split(":")[1].trim();
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

        final KStreamBuilder builder = new KStreamBuilder();
        builder.table(Serdes.String(), Serdes.String(), topic, topic);

        streams = new KafkaStreams(builder, props);
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
        public int numChanges = 0;
        public KafkaStreams.State oldState;
        public KafkaStreams.State newState;
        public Map<KafkaStreams.State, Long> mapStates = new HashMap<>();
        private final boolean closeOnChange;
        private final KafkaStreams streams;

        public StateListenerStub() {
            this.closeOnChange = false;
            this.streams = null;
        }

        /**
         * For testing only, we might want to test closing streams on a transition change
         * @param closeOnChange
         * @param streams
         */
        public StateListenerStub(final boolean closeOnChange, final KafkaStreams streams) {
            this.closeOnChange = closeOnChange;
            this.streams = streams;
        }

        @Override
        public void onChange(final KafkaStreams.State newState, final KafkaStreams.State oldState) {
            long prevCount = this.mapStates.containsKey(newState) ? this.mapStates.get(newState) : 0;
            this.numChanges++;
            this.oldState = oldState;
            this.newState = newState;
            this.mapStates.put(newState, prevCount + 1);
            if (this.closeOnChange &&
                    (newState == KafkaStreams.State.NOT_RUNNING || newState == KafkaStreams.State.ERROR)) {
                streams.close();
            }
        }

    }
}
