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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.UnknownStateStoreException;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.streams.StoreQueryParameters.fromNameAndType;
import static org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("deprecation")
@Tag("integration")
@Timeout(600)
public class JoinStoreIntegrationTest {

    private static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
        BASE_STREAMS_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        BASE_STREAMS_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        BASE_STREAMS_CONFIG.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        BASE_STREAMS_CONFIG.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        BASE_STREAMS_CONFIG.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL);

        ADMIN_CONFIG.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    private static final String APP_ID = "join-store-integration-test";
    private static final Long COMMIT_INTERVAL = 100L;
    private static final Properties BASE_STREAMS_CONFIG = new Properties();
    static final String INPUT_TOPIC_RIGHT = "inputTopicRight";
    static final String INPUT_TOPIC_LEFT = "inputTopicLeft";
    static final String OUTPUT_TOPIC = "outputTopic";
    static final Properties ADMIN_CONFIG = new Properties();
    private Properties streamsConfig;
    private String appId;

    @BeforeEach
    public void prepareTopology(final TestInfo testInfo) throws InterruptedException {
        CLUSTER.createTopics(INPUT_TOPIC_LEFT, INPUT_TOPIC_RIGHT, OUTPUT_TOPIC);
        appId = APP_ID + "-" + safeUniqueTestName(testInfo);
        streamsConfig = new Properties();
        streamsConfig.putAll(BASE_STREAMS_CONFIG);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
    }

    @AfterEach
    public void cleanup() throws InterruptedException, IOException {
        CLUSTER.deleteAllTopics();
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfig);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void providingAJoinStoreNameShouldNotMakeTheJoinResultQueryable(final boolean withHeaders) throws InterruptedException {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Integer> left = builder.stream(INPUT_TOPIC_LEFT, Consumed.with(Serdes.String(), Serdes.Integer()));
        final KStream<String, Integer> right = builder.stream(INPUT_TOPIC_RIGHT, Consumed.with(Serdes.String(), Serdes.Integer()));
        final CountDownLatch latch = new CountDownLatch(1);

        left.join(
            right,
            Integer::sum,
            JoinWindows.of(ofMillis(100)),
            StreamJoined.with(Serdes.String(), Serdes.Integer(), Serdes.Integer()).withStoreName("join-store"));

        StreamsTestUtils.maybeSetDslStoreFormatHeaders(streamsConfig, withHeaders);
        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig)) {
            kafkaStreams.setStateListener((newState, oldState) -> {
                if (newState == KafkaStreams.State.RUNNING) {
                    latch.countDown();
                }
            });

            kafkaStreams.start();
            latch.await();
            final UnknownStateStoreException exception =
                assertThrows(
                    UnknownStateStoreException.class,
                    () -> kafkaStreams.store(fromNameAndType("join-store", keyValueStore()))
                );
            assertEquals(
                "Cannot get state store join-store because no such store is registered in the topology.",
                exception.getMessage());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void streamJoinChangelogTopicShouldBeConfiguredWithDeleteOnlyCleanupPolicy(final boolean withHeaders) throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Integer> left = builder.stream(INPUT_TOPIC_LEFT, Consumed.with(Serdes.String(), Serdes.Integer()));
        final KStream<String, Integer> right = builder.stream(INPUT_TOPIC_RIGHT, Consumed.with(Serdes.String(), Serdes.Integer()));
        final CountDownLatch latch = new CountDownLatch(1);

        left.join(
            right,
            Integer::sum,
            JoinWindows.of(ofMillis(100)),
            StreamJoined.with(Serdes.String(), Serdes.Integer(), Serdes.Integer()).withStoreName("join-store"));

        StreamsTestUtils.maybeSetDslStoreFormatHeaders(streamsConfig, withHeaders);

        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
            final Admin admin = Admin.create(ADMIN_CONFIG)) {
            kafkaStreams.setStateListener((newState, oldState) -> {
                if (newState == KafkaStreams.State.RUNNING) {
                    latch.countDown();
                }
            });

            kafkaStreams.start();
            latch.await();

            final Collection<ConfigResource> changelogTopics = Stream.of(
                    appId + "-join-store-this-join-store-changelog",
                    appId + "-join-store-other-join-store-changelog"
                )
                .map(name -> new ConfigResource(Type.TOPIC, name))
                .collect(Collectors.toList());

            final Map<ConfigResource, org.apache.kafka.clients.admin.Config> topicConfig
                = admin.describeConfigs(changelogTopics).all().get();
            topicConfig.values().forEach(tc -> assertEquals("delete", tc.get("cleanup.policy").value()));
        }
    }
}
