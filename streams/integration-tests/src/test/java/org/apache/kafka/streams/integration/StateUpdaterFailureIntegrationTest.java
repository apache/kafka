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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.AbstractStoreBuilder;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockKeyValueStore;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StateUpdaterFailureIntegrationTest {

    private static final int NUM_BROKERS = 1;
    protected static final String INPUT_TOPIC_NAME = "input-topic";
    private static final int NUM_PARTITIONS = 6;

    private final EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster(NUM_BROKERS);

    private Properties streamsConfiguration;
    private final MockTime mockTime = cluster.time;
    private KafkaStreams streams;

    @BeforeEach
    public void before(final TestInfo testInfo) throws InterruptedException, IOException {
        cluster.start();
        cluster.createTopic(INPUT_TOPIC_NAME, NUM_PARTITIONS, 1);
        streamsConfiguration = new Properties();
        final String safeTestName = safeUniqueTestName(testInfo);
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);

    }

    @AfterEach
    public void after() {
        cluster.stop();
        if (streams != null) {
            streams.close(Duration.ofSeconds(30));
        }
    }

    /**
     * The conditions that we need to meet:
     * <p><ul>
     * <li>We have an unhandled task in {@link org.apache.kafka.streams.processor.internals.DefaultStateUpdater}</li>
     * <li>StreamThread is not running, so {@link org.apache.kafka.streams.processor.internals.TaskManager#handleExceptionsFromStateUpdater} is not called anymore</li>
     * <li>The task throws exception in {@link org.apache.kafka.streams.processor.internals.Task#maybeCheckpoint(boolean)} while being processed by {@code DefaultStateUpdater}</li>
     * <li>{@link org.apache.kafka.streams.processor.internals.TaskManager#shutdownStateUpdater} tries to clean up all tasks that are left in the {@code DefaultStateUpdater}</li>
     * </ul><p>
     * If all conditions are met, {@code TaskManager} needs to be able to handle the failed task from the {@code DefaultStateUpdater} correctly and not hang.
     */
    @Test
    public void correctlyHandleFlushErrorsDuringRebalance() throws Exception {
        final AtomicInteger numberOfStoreInits = new AtomicInteger();
        final CountDownLatch pendingShutdownLatch = new CountDownLatch(1);

        final StoreBuilder<KeyValueStore<Object, Object>> storeBuilder = new AbstractStoreBuilder<>("testStateStore", Serdes.Integer(), Serdes.ByteArray(), new MockTime()) {

            @Override
            public KeyValueStore<Object, Object> build() {
                return new MockKeyValueStore(name, false) {

                    @Override
                    public void init(final StateStoreContext stateStoreContext, final StateStore root) {
                        super.init(stateStoreContext, root);
                        numberOfStoreInits.incrementAndGet();
                    }

                    @Override
                    public void flush() {
                        // we want to throw the ProcessorStateException here only when the rebalance finished(we reassigned the 3 tasks from the removed thread to the existing thread)
                        // we use waitForCondition to wait until the current state is PENDING_SHUTDOWN to make sure the Stream Thread will not handle the exception and we can get to in TaskManager#shutdownStateUpdater
                        if (numberOfStoreInits.get() == 9) {
                            try {
                                pendingShutdownLatch.await();
                            } catch (final InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            throw new ProcessorStateException("flush");
                        }
                    }
                };
            }
        };

        final TopologyWrapper topology = new TopologyWrapper();
        topology.addSource("ingest", INPUT_TOPIC_NAME);
        topology.addProcessor("my-processor", new MockApiProcessorSupplier<>(), "ingest");
        topology.addStateStore(storeBuilder, "my-processor");

        streams = new KafkaStreams(topology, streamsConfiguration);
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.PENDING_SHUTDOWN) {
                pendingShutdownLatch.countDown();
            }
        });
        streams.start();

        TestUtils.waitForCondition(() -> streams.state() == KafkaStreams.State.RUNNING, "Streams never reached RUNNING state");

        streams.removeStreamThread();

        // Before shutting down, we want the tasks to be reassigned
        TestUtils.waitForCondition(() -> numberOfStoreInits.get() == 9, "Streams never reinitialized the store enough times");

        assertTrue(streams.close(Duration.ofSeconds(60)));
    }
}
