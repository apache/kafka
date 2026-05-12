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
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
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
    public void metricValueShouldNotThrowIfStoreIsNotInitialized() throws Exception {
        final CountDownLatch initLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(
            Stores.keyValueStoreBuilder(
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
            .withLoggingEnabled(Collections.emptyMap()));

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
            } finally {
                finishLatch.countDown();
            }
        }
    }
}
