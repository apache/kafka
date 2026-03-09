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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Collections.singletonList;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForEmptyConsumerGroup;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@Tag("integration")
public class RebalanceProtocolMigrationIntegrationTest {

    public static final String INPUT_TOPIC = "migration-input";
    public static final String OUTPUT_TOPIC = "migration-output";
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    private String inputTopic;
    private String outputTopic;
    private KafkaStreams kafkaStreams;
    private String safeTestName;

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @BeforeEach
    public void createTopics(final TestInfo testInfo) throws Exception {
        safeTestName = safeUniqueTestName(testInfo);
        inputTopic = INPUT_TOPIC + safeTestName;
        outputTopic = OUTPUT_TOPIC + safeTestName;
        CLUSTER.createTopic(inputTopic);
        CLUSTER.createTopic(outputTopic);
    }

    private Properties props() {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 500);
        streamsConfiguration.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        return streamsConfiguration;
    }

    @AfterEach
    public void shutdown() {
        if (kafkaStreams != null) {
            kafkaStreams.close(Duration.ofSeconds(30L));
            kafkaStreams.cleanUp();
        }
    }


    @Test
    public void shouldMigrateToAndFromStreamsRebalanceProtocol() throws Exception {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KStream<String, String> input = streamsBuilder.stream(
            inputTopic, Consumed.with(Serdes.String(), Serdes.String()));
        input.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        final Properties props = props();
        props.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name());
        processExactlyOneRecord(streamsBuilder, props, "1", "A");

        // Wait for session to time out
        try (final Admin adminClient = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()))) {
            waitForEmptyConsumerGroup(adminClient, props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG), 1000);
        }

        props.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name());
        processExactlyOneRecord(streamsBuilder, props, "2", "B");

        props.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name());
        processExactlyOneRecord(streamsBuilder, props, "3", "C");
    }

    @Test
    public void shouldMigrateFromAndToStreamsRebalanceProtocol() throws Exception {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KStream<String, String> input = streamsBuilder.stream(
            inputTopic, Consumed.with(Serdes.String(), Serdes.String()));
        input.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        final Properties props = props();
        props.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name());
        processExactlyOneRecord(streamsBuilder, props, "1", "A");

        props.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name());
        processExactlyOneRecord(streamsBuilder, props, "2", "B");

        // Wait for session to time out
        try (final Admin adminClient = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()))) {
            waitForEmptyConsumerGroup(adminClient, props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG), 1000);
        }

        props.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name());
        processExactlyOneRecord(streamsBuilder, props, "3", "C");
    }

    private void processExactlyOneRecord(
        final StreamsBuilder streamsBuilder,
        final Properties props,
        final String key,
        final String value)
        throws Exception {
        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        final long currentTimeNew = CLUSTER.time.milliseconds();

        processKeyValueAndVerify(
            key,
            value,
            currentTimeNew,
            List.of(
                new KeyValueTimestamp<>(key, value, currentTimeNew)
            )
        );

        kafkaStreams.close();
        kafkaStreams = null;
    }


    private <K, V> void processKeyValueAndVerify(
        final K key,
        final V value,
        final long timestamp,
        final List<KeyValueTimestamp<K, V>> expected)
        throws Exception {

        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputTopic,
            singletonList(KeyValue.pair(key, value)),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class),
            timestamp);


        final Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-" + safeTestName);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());


        final List<KeyValueTimestamp<K, V>> actual =
            IntegrationTestUtils.waitUntilMinKeyValueWithTimestampRecordsReceived(
                consumerProperties,
                outputTopic,
                expected.size(),
                60 * 1000);

        assertThat(actual, is(expected));

    }
}
