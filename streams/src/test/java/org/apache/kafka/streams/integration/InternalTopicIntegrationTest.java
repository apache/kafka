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

import java.time.Duration;
import kafka.log.LogConfig;
import kafka.utils.MockTime;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.Collections.singletonList;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForCompletion;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests related to internal topics in streams
 */
@Category({IntegrationTest.class})
public class InternalTopicIntegrationTest {
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    @BeforeClass
    public static void startCluster() throws IOException, InterruptedException {
        CLUSTER.start();
        CLUSTER.createTopics(DEFAULT_INPUT_TOPIC, DEFAULT_INPUT_TABLE_TOPIC);
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }


    private static final String APP_ID = "internal-topics-integration-test";
    private static final String DEFAULT_INPUT_TOPIC = "inputTopic";
    private static final String DEFAULT_INPUT_TABLE_TOPIC = "inputTable";

    private final MockTime mockTime = CLUSTER.time;

    private Properties streamsProp;

    @Before
    public void before() {
        streamsProp = new Properties();
        streamsProp.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsProp.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProp.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProp.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsProp.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsProp.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsProp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @After
    public void after() throws IOException {
        // Remove any state from previous test runs
        IntegrationTestUtils.purgeLocalStreamsState(streamsProp);
    }

    private void produceData(final List<String> inputValues) {
        final Properties producerProp = new Properties();
        producerProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerProp.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        IntegrationTestUtils.produceValuesSynchronously(DEFAULT_INPUT_TOPIC, inputValues, producerProp, mockTime);
    }

    private Properties getTopicProperties(final String changelog) {
        try (final Admin adminClient = createAdminClient()) {
            final ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, changelog);
            try {
                final Config config = adminClient.describeConfigs(Collections.singletonList(configResource)).values().get(configResource).get();
                final Properties properties = new Properties();
                for (final ConfigEntry configEntry : config.entries()) {
                    if (configEntry.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG) {
                        properties.put(configEntry.name(), configEntry.value());
                    }
                }
                return properties;
            } catch (final InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Admin createAdminClient() {
        final Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        return Admin.create(adminClientConfig);
    }

    /*
     * This test just ensures that that the assignor does not get stuck during partition number resolution
     * for internal repartition topics. See KAFKA-10689
     */
    @Test
    public void shouldGetToRunningWithWindowedTableInFKJ() throws Exception {
        final String appID = APP_ID + "-windowed-FKJ";
        streamsProp.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KStream<String, String> inputTopic = streamsBuilder.stream(DEFAULT_INPUT_TOPIC);
        final KTable<String, String> inputTable = streamsBuilder.table(DEFAULT_INPUT_TABLE_TOPIC);
        inputTopic
            .groupBy(
                (k, v) -> k,
                Grouped.with("GroupName", Serdes.String(), Serdes.String())
            )
            .windowedBy(TimeWindows.of(Duration.ofMinutes(10)))
            .aggregate(
                () -> "",
                (k, v, a) -> a + k)
            .leftJoin(
                inputTable,
                v -> v,
                (x, y) -> x + y
            );

        final KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), streamsProp);
        startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(60));
    }

    @Test
    public void shouldCompactTopicsForKeyValueStoreChangelogs() {
        final String appID = APP_ID + "-compact";
        streamsProp.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        //
        // Step 1: Configure and start a simple word count topology
        //
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> textLines = builder.stream(DEFAULT_INPUT_TOPIC);

        textLines.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy(MockMapper.selectValueMapper())
            .count(Materialized.as("Counts"));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsProp);
        streams.start();

        //
        // Step 2: Produce some input data to the input topic.
        //
        produceData(Arrays.asList("hello", "world", "world", "hello world"));

        //
        // Step 3: Verify the state changelog topics are compact
        //
        waitForCompletion(streams, 2, 30000L);
        streams.close();

        final Properties changelogProps = getTopicProperties(ProcessorStateManager.storeChangelogTopic(appID, "Counts"));
        assertEquals(LogConfig.Compact(), changelogProps.getProperty(LogConfig.CleanupPolicyProp()));

        final Properties repartitionProps = getTopicProperties(appID + "-Counts-repartition");
        assertEquals(LogConfig.Delete(), repartitionProps.getProperty(LogConfig.CleanupPolicyProp()));
        assertEquals(4, repartitionProps.size());
    }

    @Test
    public void shouldCompactAndDeleteTopicsForWindowStoreChangelogs() {
        final String appID = APP_ID + "-compact-delete";
        streamsProp.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        //
        // Step 1: Configure and start a simple word count topology
        //
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> textLines = builder.stream(DEFAULT_INPUT_TOPIC);

        final int durationMs = 2000;

        textLines.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy(MockMapper.selectValueMapper())
            .windowedBy(TimeWindows.of(ofSeconds(1L)).grace(ofMillis(0L)))
            .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("CountWindows").withRetention(ofSeconds(2L)));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsProp);
        streams.start();

        //
        // Step 2: Produce some input data to the input topic.
        //
        produceData(Arrays.asList("hello", "world", "world", "hello world"));

        //
        // Step 3: Verify the state changelog topics are compact
        //
        waitForCompletion(streams, 2, 30000L);
        streams.close();
        final Properties properties = getTopicProperties(ProcessorStateManager.storeChangelogTopic(appID, "CountWindows"));
        final List<String> policies = Arrays.asList(properties.getProperty(LogConfig.CleanupPolicyProp()).split(","));
        assertEquals(2, policies.size());
        assertTrue(policies.contains(LogConfig.Compact()));
        assertTrue(policies.contains(LogConfig.Delete()));
        // retention should be 1 day + the window duration
        final long retention = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS) + durationMs;
        assertEquals(retention, Long.parseLong(properties.getProperty(LogConfig.RetentionMsProp())));

        final Properties repartitionProps = getTopicProperties(appID + "-CountWindows-repartition");
        assertEquals(LogConfig.Delete(), repartitionProps.getProperty(LogConfig.CleanupPolicyProp()));
        assertEquals(4, repartitionProps.size());
    }
}
