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

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.internals.InMemorySessionStore;
import org.apache.kafka.streams.state.internals.InMemoryWindowStore;
import org.apache.kafka.test.MockApiProcessorSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.fail;

@Timeout(120)
@Tag("integration")
public class InMemoryStoreMetricsIntegrationTest {
    private static final int NUM_BROKERS = 1;
    private static final String INPUT_TOPIC = "in-memory-num-keys-input";

    private final EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster(NUM_BROKERS);
    private String safeTestName;

    @BeforeEach
    public void startCluster(final TestInfo testInfo) throws InterruptedException {
        cluster.start();
        cluster.createTopic(INPUT_TOPIC, 1, 1);
        safeTestName = safeUniqueTestName(testInfo);
    }

    @AfterEach
    public void closeCluster() {
        cluster.stop();
    }

    @Test
    public void keyValueStoreMetricValueShouldNotThrowIfStoreIsNotInitialized() throws Exception {
        final CountDownLatch initLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);

        final StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(
                new KeyValueBytesStoreSupplier() {
                    @Override
                    public String name() {
                        return "store";
                    }

                    @Override
                    public KeyValueStore<Bytes, byte[]> get() {
                        return new InMemoryKeyValueStore(name()) {
                            @Override
                            public void init(final StateStoreContext stateStoreContext, final StateStore root) {
                                initLatch.countDown();
                                try {
                                    finishLatch.await();
                                } catch (final InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                                super.init(stateStoreContext, root);
                            }
                        };
                    }

                    @Override
                    public String metricsScope() {
                        return "in-memory";
                    }
                },
                Serdes.String(),
                Serdes.String())
            .withCachingEnabled()
            .withLoggingEnabled(Collections.emptyMap());

        test(storeBuilder, initLatch, finishLatch);
    }

    @Test
    public void sessionStoreMetricValueShouldNotThrowIfStoreIsNotInitialized() throws Exception {
        final CountDownLatch initLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);

        final long retentionMs = 60_000L;

        final StoreBuilder<SessionStore<String, String>> storeBuilder = Stores.sessionStoreBuilder(
                new SessionBytesStoreSupplier() {
                    @Override
                    public String name() {
                        return "store";
                    }

                    @Override
                    public SessionStore<Bytes, byte[]> get() {
                        return new InMemorySessionStore(name(), retentionMs, metricsScope()) {
                            @Override
                            public void init(final StateStoreContext stateStoreContext, final StateStore root) {
                                initLatch.countDown();
                                try {
                                    finishLatch.await();
                                } catch (final InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                                super.init(stateStoreContext, root);
                            }
                        };
                    }

                    @Override
                    public String metricsScope() {
                        return "in-memory-session";
                    }

                    @Override
                    public long segmentIntervalMs() {
                        return 1L;
                    }

                    @Override
                    public long retentionPeriod() {
                        return retentionMs;
                    }
                },
                Serdes.String(),
                Serdes.String())
            .withCachingEnabled()
            .withLoggingEnabled(Collections.emptyMap());

        test(storeBuilder, initLatch, finishLatch);
    }

    @Test
    public void windowStoreMetricValueShouldNotThrowIfStoreIsNotInitialized() throws Exception {
        final CountDownLatch initLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);

        final long retentionMs = 60_000L;
        final long windowMs = 1_000L;

        final StoreBuilder<WindowStore<String, String>> storeBuilder = Stores.windowStoreBuilder(
                new WindowBytesStoreSupplier() {
                    @Override
                    public String name() {
                        return "store";
                    }

                    @Override
                    public WindowStore<Bytes, byte[]> get() {
                        return new InMemoryWindowStore(name(), retentionMs, windowMs, false, metricsScope()) {
                            @Override
                            public void init(final StateStoreContext stateStoreContext, final StateStore root) {
                                initLatch.countDown();
                                try {
                                    finishLatch.await();
                                } catch (final InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                                super.init(stateStoreContext, root);
                            }
                        };
                    }

                    @Override
                    public String metricsScope() {
                        return "in-memory-window";
                    }

                    @Override
                    public long segmentIntervalMs() {
                        return 1L;
                    }

                    @Override
                    public long windowSize() {
                        return windowMs;
                    }

                    @Override
                    public boolean retainDuplicates() {
                        return false;
                    }

                    @Override
                    public long retentionPeriod() {
                        return retentionMs;
                    }
                },
                Serdes.String(),
                Serdes.String())
            .withCachingEnabled()
            .withLoggingEnabled(Collections.emptyMap());

        test(storeBuilder, initLatch, finishLatch);
    }

    private void test(final StoreBuilder<?> storeBuilder,
                      final CountDownLatch initLatch,
                      final CountDownLatch finishLatch) throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(storeBuilder);
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .process(new MockApiProcessorSupplier<>(), "store");

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, safeTestName);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.STATE_DIR_CONFIG, org.apache.kafka.test.TestUtils.tempDirectory().getAbsolutePath());

        try (KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
            streams.start();

            initLatch.await();

            try {
                for (final Map.Entry<MetricName, ? extends Metric> entry : streams.metrics().entrySet()) {
                    entry.getValue().metricValue();
                }
            } catch (final Exception e) {
                fail("Getting metric values on an uninitialized store shouldn't throw exceptions", e);
            } finally {
                finishLatch.countDown();
            }
        }
    }
}
