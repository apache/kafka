/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
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
import org.apache.kafka.test.MockMetricsReporter;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

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
        final KStreamBuilder builder = new KStreamBuilder();
        final KafkaStreams streams = new KafkaStreams(builder, props);

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
    public void testStateCloseAfterCreate() {
        final KStreamBuilder builder = new KStreamBuilder();
        final KafkaStreams streams = new KafkaStreams(builder, props);

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
        final int numThreads = 2;
        final KStreamBuilder builder = new KStreamBuilder();
        // make sure we have the global state thread running too
        builder.globalTable("anyTopic", "anyStore");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numThreads);
        new KafkaStreams(builder, props);

        testStateThreadCloseHelper(numThreads);
    }
    
    @Test
    public void testStateGlobalThreadClose() throws Exception {
        final int numThreads = 2;
        final KStreamBuilder builder = new KStreamBuilder();
        // make sure we have the global state thread running too
        builder.globalTable("anyTopic", "anyStoreName");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numThreads);
        final KafkaStreams streams = new KafkaStreams(builder, props);


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
    public void testInitializesAndDestroysMetricsReporters() {
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
    public void testCloseIsIdempotent() {
        streams.close();
        final int closeCount = MockMetricsReporter.CLOSE_COUNT.get();

        streams.close();
        Assert.assertEquals("subsequent close() calls should do nothing",
            closeCount, MockMetricsReporter.CLOSE_COUNT.get());
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotStartOnceClosed() {
        streams.start();
        streams.close();
        try {
            streams.start();
        } catch (final IllegalStateException e) {
            Assert.assertEquals("Cannot start again.", e.getMessage());
            throw e;
        } finally {
            streams.close();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotStartTwice() {
        streams.start();

        try {
            streams.start();
        } catch (final IllegalStateException e) {
            Assert.assertEquals("Cannot start again.", e.getMessage());
            throw e;
        } finally {
            streams.close();
        }
    }

    @Test
    public void testNumberDefaultMetrics() {
        final KafkaStreams streams = createKafkaStreams();
        final Map<MetricName, ? extends Metric> metrics = streams.metrics();
        // all 15 default StreamThread metrics + 1 metric that keeps track of number of metrics
        assertEquals(metrics.size(), 16);
    }

    @Test(expected = ConfigException.class)
    public void testIllegalMetricsConfig() {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.setProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "illegalConfig");
        final KStreamBuilder builder = new KStreamBuilder();
        new KafkaStreams(builder, props);
    }

    @Test
    public void testLegalMetricsConfig() {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.setProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.INFO.toString());
        final KStreamBuilder builder1 = new KStreamBuilder();
        final KafkaStreams streams1 = new KafkaStreams(builder1, props);
        streams1.close();

        props.setProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.DEBUG.toString());
        final KStreamBuilder builder2 = new KStreamBuilder();
        new KafkaStreams(builder2, props);
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
            final Properties props = new Properties();
            props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
            props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
            props.setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

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
            final KafkaStreams streams = new KafkaStreams(builder, props);
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


    private KafkaStreams createKafkaStreams() {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());

        final KStreamBuilder builder = new KStreamBuilder();
        return new KafkaStreams(builder, props);
    }

    @Test
    public void testCleanup() {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "testLocalCleanup");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());

        final KStreamBuilder builder = new KStreamBuilder();
        final KafkaStreams streams = new KafkaStreams(builder, props);

        streams.cleanUp();
        streams.start();
        streams.close();
        streams.cleanUp();
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotCleanupWhileRunning() throws Exception {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "testCannotCleanupWhileRunning");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());

        final KStreamBuilder builder = new KStreamBuilder();
        final KafkaStreams streams = new KafkaStreams(builder, props);

        streams.start();
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return streams.state() == KafkaStreams.State.RUNNING;
            }
        }, 10 * 1000, "Streams never started.");
        try {
            streams.cleanUp();
        } catch (final IllegalStateException e) {
            Assert.assertEquals("Cannot clean up while running.", e.getMessage());
            throw e;
        } finally {
            streams.close();
        }
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
