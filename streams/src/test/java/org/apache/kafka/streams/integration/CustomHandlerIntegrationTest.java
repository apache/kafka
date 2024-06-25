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

import org.apache.kafka.common.metrics.Sensor;
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

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;


@Timeout(600)
@Tag("integration")
@SuppressWarnings("deprecation")
public class CustomHandlerIntegrationTest {
    private static final int NUM_BROKERS = 1;
    private static final int NUM_THREADS = 2;

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

    private final long timeout = 60000;

    // topic name
    private static final String STREAM_INPUT = "STREAM_INPUT";
    private static final String NON_EXISTING_TOPIC = "non_existing_topic";

    private StreamsBuilder builder;
    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;

    private String appId;

    @BeforeEach
    public void before(final TestInfo testInfo) throws InterruptedException {
        builder = new StreamsBuilder();
        CLUSTER.createTopics(STREAM_INPUT);

        final String safeTestName = safeUniqueTestName(testInfo);
        appId = "app-" + safeTestName;

        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.DEBUG.name);
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, NUM_THREADS);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
    }

    @AfterEach
    public void after() throws InterruptedException {
        CLUSTER.deleteTopics(STREAM_INPUT);
    }

    private void startApplication() throws InterruptedException {
        final Topology topology = builder.build();
        kafkaStreams = new KafkaStreams(topology, streamsConfiguration);
        kafkaStreams.start();
        TestUtils.waitForCondition(
                () -> kafkaStreams.state() == State.RUNNING,
                timeout,
                () -> "Kafka Streams application did not reach state RUNNING in " + timeout + " ms");
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

    private void closeApplication() throws Exception {
        kafkaStreams.close();
        kafkaStreams.cleanUp();
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
        final long timeout = 60000;
        TestUtils.waitForCondition(
                () -> kafkaStreams.state() == State.NOT_RUNNING,
                timeout,
                () -> "Kafka Streams application did not reach state NOT_RUNNING in " + timeout + " ms");
    }


    private void writeToNonExistingTopic() throws Exception {
        builder.stream(STREAM_INPUT, Consumed.with(Serdes.Integer(), Serdes.String()))
                .to(NON_EXISTING_TOPIC, Produced.with(Serdes.Integer(), Serdes.String()));
        produceRecords();
        startApplication();
        closeApplication();
    }

    @Test
    void shouldNotThrowCorruptedTaskException() {
        assertDoesNotThrow(this::writeToNonExistingTopic);
    }
}