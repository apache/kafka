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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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

    @Test
    public void correctlyHandleFlushErrorsDuringRebalance() throws InterruptedException {
        final AtomicInteger numberOfStoreInits = new AtomicInteger();
        final AtomicReference<KafkaStreams.State> currentState = new AtomicReference<>();

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
                        if (numberOfStoreInits.get() == NUM_PARTITIONS * 1.5) {
                            try {
                                TestUtils.waitForCondition(() -> currentState.get() == KafkaStreams.State.PENDING_SHUTDOWN, "Streams never reached PENDING_SHUTDOWN state");
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
        streams.setStateListener((newState, oldState) -> currentState.set(newState));
        streams.start();

        TestUtils.waitForCondition(() -> currentState.get() == KafkaStreams.State.RUNNING, "Streams never reached RUNNING state");

        streams.removeStreamThread();

        TestUtils.waitForCondition(() -> numberOfStoreInits.get() == NUM_PARTITIONS * 1.5, "Streams never reinitialized the store enough times");

        assertTrue(streams.close(Duration.ofSeconds(60)));
    }
}
