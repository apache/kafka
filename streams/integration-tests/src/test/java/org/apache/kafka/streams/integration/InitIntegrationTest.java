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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.MissingInternalTopicsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
@Timeout(600)
public class InitIntegrationTest {

    private static final String INPUT_TOPIC = "input-topic";

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    @BeforeAll
    public static void startCluster() throws IOException, InterruptedException {
        CLUSTER.start();
        CLUSTER.createTopics(INPUT_TOPIC);
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    private Properties streamsConfig;

    @BeforeEach
    public void before(final TestInfo testInfo) {
        streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "init-test-" + safeUniqueTestName(testInfo));
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @AfterEach
    public void after() throws IOException {
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfig);
    }

    @Test
    public void shouldInitAndStartInManualMode() throws Exception {
        streamsConfig.put(StreamsConfig.INTERNAL_TOPIC_SETUP_CONFIG, StreamsConfig.INTERNAL_TOPIC_SETUP_MANUAL);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC).groupByKey().count();

        try (final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig)) {
            streams.init();

            // Verify internal topics were created on the broker
            try (final Admin admin = Admin.create(adminConfig())) {
                final Set<String> topics = admin.listTopics().names().get();
                final String appId = streamsConfig.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
                assertTrue(topics.stream().anyMatch(t -> t.contains(appId) && t.contains("changelog")),
                    "Expected changelog topic to exist after init(), found: " + topics);
            }

            // App should reach RUNNING after init
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(streams);
        }
    }

    @Test
    public void shouldThrowWhenInternalTopicDeletedBetweenRestartsInManualMode() throws Exception {
        streamsConfig.put(StreamsConfig.INTERNAL_TOPIC_SETUP_CONFIG, StreamsConfig.INTERNAL_TOPIC_SETUP_MANUAL);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC).groupByKey().count();

        // First run: init + start + stop normally
        try (final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig)) {
            streams.init();
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(streams);
        }

        // Delete a changelog topic while the app is stopped
        final String appId = streamsConfig.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
        try (final Admin admin = Admin.create(adminConfig())) {
            final Set<String> topics = admin.listTopics().names().get();
            final String changelogTopic = topics.stream()
                .filter(t -> t.contains(appId) && t.contains("changelog"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No changelog topic found in: " + topics));

            admin.deleteTopics(Set.of(changelogTopic)).all().get();
        }

        // Second run: start without init() — rebalance calls makeReady() for changelog
        // topics, which tries to recreate the missing topic. Guard throws in manual mode.
        final AtomicReference<Throwable> caughtException = new AtomicReference<>();

        try (final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig)) {
            streams.setUncaughtExceptionHandler(exception -> {
                caughtException.set(exception);
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });

            streams.start();

            TestUtils.waitForCondition(
                () -> streams.state() == State.ERROR || streams.state() == State.NOT_RUNNING,
                60000,
                () -> "Streams did not reach ERROR state. State: " + streams.state()
                    + ", exception: " + caughtException.get()
            );

            assertTrue(caughtException.get() instanceof MissingInternalTopicsException,
                "Expected MissingInternalTopicsException but got: " + caughtException.get());
        }
    }

    @Test
    public void shouldAutoCreateInternalTopicsWithoutInit() throws Exception {
        // Default config is automatic mode — no init() call needed
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC).groupByKey().count();

        try (final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig)) {
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(streams);

            // Verify internal topics were auto-created during rebalance
            try (final Admin admin = Admin.create(adminConfig())) {
                final Set<String> topics = admin.listTopics().names().get();
                final String appId = streamsConfig.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
                assertTrue(topics.stream().anyMatch(t -> t.contains(appId) && t.contains("changelog")),
                    "Expected changelog topic to be auto-created, found: " + topics);
            }
        }
    }

    private Properties adminConfig() {
        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        return props;
    }
}
