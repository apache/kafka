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
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;

/**
 * Regression test for KAFKA-13009: removing a subtopology while a global store remains, then restarting
 * with the same application id and state directory, must not crash.
 */
@Timeout(600)
@Tag("integration")
public class GlobalStoreTopologyChangeIntegrationTest {
    private static final int NUM_BROKERS = 1;
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private static final String COUNT_STORE = "count-store";
    private static final String COUNT_STORE_2 = "count-store-2";
    private static final String GLOBAL_STORE = "global-store";

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    private final MockTime mockTime = CLUSTER.time;
    private String inputTopic;
    private String inputTopic2;
    private String globalTopic;
    private Properties config;

    @BeforeEach
    public void before(final TestInfo testInfo) throws Exception {
        final String safeTestName = safeUniqueTestName(testInfo);
        inputTopic = "input-" + safeTestName;
        inputTopic2 = "input2-" + safeTestName;
        globalTopic = "global-" + safeTestName;
        CLUSTER.createTopic(inputTopic, 2, 1);
        CLUSTER.createTopic(inputTopic2, 2, 1);
        CLUSTER.createTopic(globalTopic, 2, 1);

        config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
    }

    @AfterEach
    public void after() throws Exception {
        IntegrationTestUtils.purgeLocalStreamsState(config);
    }

    @Test
    public void shouldNotCrashWhenSubtopologyRemovedWhileGlobalStoreRemains() throws Exception {
        produceInput(inputTopic, "a");
        produceInput(inputTopic2, "x");
        produceGlobal();

        // v1: two regular subtopologies + a global table, materialising local state on disk.
        final KafkaStreams v1 = new KafkaStreams(twoRegularPlusGlobal().build(), config);
        try {
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(v1);
            final ReadOnlyKeyValueStore<String, Long> countStore =
                IntegrationTestUtils.getStore(COUNT_STORE, v1, QueryableStoreTypes.keyValueStore());
            TestUtils.waitForCondition(
                () -> countStore.get("a") != null,
                30_000L,
                "First subtopology's state was not materialised");
        } finally {
            v1.close(Duration.ofSeconds(60));
        }

        // v2: first subtopology removed, global table stays, same app id + state dir (stale on-disk
        // directories remain). Before the fix start() threw from RocksDBMetricsRecorder.init.
        produceInput(inputTopic2, "x");
        final KafkaStreams v2 = new KafkaStreams(secondRegularOnlyPlusGlobal().build(), config);
        try {
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(v2);
        } finally {
            v2.close(Duration.ofSeconds(60));
        }
    }

    private StreamsBuilder twoRegularPlusGlobal() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.Long()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
            .count(Materialized.as(COUNT_STORE));
        builder.stream(inputTopic2, Consumed.with(Serdes.String(), Serdes.Long()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
            .count(Materialized.as(COUNT_STORE_2));
        builder.globalTable(globalTopic, Consumed.with(Serdes.Long(), Serdes.String()),
            Materialized.as(GLOBAL_STORE));
        return builder;
    }

    private StreamsBuilder secondRegularOnlyPlusGlobal() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic2, Consumed.with(Serdes.String(), Serdes.Long()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
            .count(Materialized.as(COUNT_STORE_2));
        builder.globalTable(globalTopic, Consumed.with(Serdes.Long(), Serdes.String()),
            Materialized.as(GLOBAL_STORE));
        return builder;
    }

    private void produceInput(final String topic, final String key) {
        IntegrationTestUtils.produceKeyValuesSynchronously(
            topic,
            Arrays.asList(new KeyValue<>(key, 1L), new KeyValue<>(key, 2L)),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, LongSerializer.class, new Properties()),
            mockTime);
    }

    private void produceGlobal() {
        IntegrationTestUtils.produceKeyValuesSynchronously(
            globalTopic,
            Arrays.asList(new KeyValue<>(1L, "A"), new KeyValue<>(2L, "B")),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class, StringSerializer.class, new Properties()),
            mockTime);
    }
}
