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

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
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
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;


@Timeout(600)
@Tag("integration")
public class CustomHandlerIntegrationTest {
    private static final int NUM_BROKERS = 1;
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS,
            Utils.mkProperties(Collections.singletonMap("auto.create.topics.enable", "false")));

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    private final long timeoutMs = 60_000;

    // topic name
    private static final String STREAM_INPUT = "STREAM_INPUT";
    private static final String NON_EXISTING_TOPIC = "non_existing_topic";

    private final AtomicReference<Throwable> caughtException = new AtomicReference<>();

    private KafkaStreams kafkaStreams;
    private Topology topology;
    private String appId;

    @BeforeEach
    public void before(final TestInfo testInfo) throws InterruptedException {
        final StreamsBuilder builder = new StreamsBuilder();
        CLUSTER.createTopics(STREAM_INPUT);
        final String safeTestName = safeUniqueTestName(testInfo);
        appId = "app-" + safeTestName;

        builder.stream(STREAM_INPUT, Consumed.with(Serdes.Integer(), Serdes.String()))
            .to(NON_EXISTING_TOPIC, Produced.with(Serdes.Integer(), Serdes.String()));
        produceRecords();
        topology = builder.build();
    }

    @AfterEach
    public void after() throws InterruptedException {
        CLUSTER.deleteTopics(STREAM_INPUT);
        if (kafkaStreams != null) {
            kafkaStreams.close();
            kafkaStreams.cleanUp();
        }
    }

    private void produceRecords() {
        final Properties props = TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                StringSerializer.class,
                new Properties());
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                STREAM_INPUT,
                Collections.singletonList(new KeyValue<>(1, "A")),
                props,
                CLUSTER.time.milliseconds() + 2
        );
    }

    private Properties getCommonProperties() {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);
        return streamsConfiguration;
    }

    private void closeApplication(final Properties streamsConfiguration) throws Exception {
        kafkaStreams.close();
        kafkaStreams.cleanUp();
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @Test
    public void shouldThrowStreamsExceptionWithMissingTopicAndDefaultExceptionHandler() throws Exception {
        final Properties streamsConfiguration = getCommonProperties();
        kafkaStreams = new KafkaStreams(topology, streamsConfiguration);
        kafkaStreams.setUncaughtExceptionHandler(e -> {
            caughtException.set(e);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        kafkaStreams.start();
        TestUtils.waitForCondition(
            () -> kafkaStreams.state() == State.RUNNING,
            timeoutMs,
            () -> "Kafka Streams application did not reach state RUNNING in " + timeoutMs + " ms"
        );

        TestUtils.waitForCondition(
            this::receivedUnknownTopicOrPartitionException,
            timeoutMs,
            () -> "Did not receive UnknownTopicOrPartitionException"
        );

        TestUtils.waitForCondition(
            () -> kafkaStreams.state() == State.ERROR,
            timeoutMs,
            () -> "Kafka Streams application did not reach state ERROR in " + timeoutMs + " ms"
        );
        closeApplication(streamsConfiguration);
    }

    private boolean receivedUnknownTopicOrPartitionException() {
        if (caughtException.get() == null) {
            return false;
        }

        assertInstanceOf(StreamsException.class, caughtException.get());
        assertInstanceOf(TimeoutException.class, caughtException.get().getCause());
        assertInstanceOf(UnknownTopicOrPartitionException.class, caughtException.get().getCause().getCause());

        return true;
    }
}