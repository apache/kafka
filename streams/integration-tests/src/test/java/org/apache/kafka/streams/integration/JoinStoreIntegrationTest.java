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
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("deprecation")
@Tag("integration")
@Timeout(600)
public class JoinStoreIntegrationTest {

    private static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
        STREAMS_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        STREAMS_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        STREAMS_CONFIG.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL);

        ADMIN_CONFIG.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    private static final String APP_ID = "join-store-integration-test";
    private static final Long COMMIT_INTERVAL = 100L;
    static final Properties STREAMS_CONFIG = new Properties();
    static final String INPUT_TOPIC_RIGHT = "inputTopicRight";
    static final String INPUT_TOPIC_LEFT = "inputTopicLeft";
    static final String OUTPUT_TOPIC = "outputTopic";
    static final Properties ADMIN_CONFIG = new Properties();

    @BeforeEach
    public void prepareTopology() throws InterruptedException {
        CLUSTER.createTopics(INPUT_TOPIC_LEFT, INPUT_TOPIC_RIGHT, OUTPUT_TOPIC);
        STREAMS_CONFIG.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
    }

    @AfterEach
    public void cleanup() throws InterruptedException, IOException {
        CLUSTER.deleteAllTopics();
        IntegrationTestUtils.purgeLocalStreamsState(STREAMS_CONFIG);
    }

    @Test
    public void providingAJoinStoreNameShouldNotMakeTheJoinResultQueryable() throws InterruptedException {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-no-store-access");
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Integer> left = builder.stream(INPUT_TOPIC_LEFT, Consumed.with(Serdes.String(), Serdes.Integer()));
        final KStream<String, Integer> right = builder.stream(INPUT_TOPIC_RIGHT, Consumed.with(Serdes.String(), Serdes.Integer()));
        final CountDownLatch latch = new CountDownLatch(1);

        left.join(
            right,
            Integer::sum,
            JoinWindows.of(ofMillis(100)),
            StreamJoined.with(Serdes.String(), Serdes.Integer(), Serdes.Integer()).withStoreName("join-store"));

        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), STREAMS_CONFIG)) {
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
            assertThat(
                exception.getMessage(),
                is("Cannot get state store join-store because no such store is registered in the topology.")
            );
        }
    }

    @Test
    public void streamJoinChangelogTopicShouldBeConfiguredWithDeleteOnlyCleanupPolicy() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-changelog-cleanup-policy");
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Integer> left = builder.stream(INPUT_TOPIC_LEFT, Consumed.with(Serdes.String(), Serdes.Integer()));
        final KStream<String, Integer> right = builder.stream(INPUT_TOPIC_RIGHT, Consumed.with(Serdes.String(), Serdes.Integer()));
        final CountDownLatch latch = new CountDownLatch(1);

        left.join(
            right,
            Integer::sum,
            JoinWindows.of(ofMillis(100)),
            StreamJoined.with(Serdes.String(), Serdes.Integer(), Serdes.Integer()).withStoreName("join-store"));

        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), STREAMS_CONFIG);
            final Admin admin = Admin.create(ADMIN_CONFIG)) {
            kafkaStreams.setStateListener((newState, oldState) -> {
                if (newState == KafkaStreams.State.RUNNING) {
                    latch.countDown();
                }
            });

            kafkaStreams.start();
            latch.await();

            final Collection<ConfigResource> changelogTopics = Stream.of(
                    "join-store-integration-test-changelog-cleanup-policy-join-store-this-join-store-changelog",
                    "join-store-integration-test-changelog-cleanup-policy-join-store-other-join-store-changelog"
                )
                .map(name -> new ConfigResource(Type.TOPIC, name))
                .collect(Collectors.toList());

            final Map<ConfigResource, org.apache.kafka.clients.admin.Config> topicConfig
                = admin.describeConfigs(changelogTopics).all().get();
            topicConfig.values().forEach(
                tc -> assertThat(
                    tc.get("cleanup.policy").value(),
                    is("delete")
                )
            );
        }
    }
}
