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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Deduplicated;
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
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration")
@Timeout(600)
public class KStreamDeduplicateIntegrationTest {

    private static final int NUM_BROKERS = 1;
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    private String inputTopic;
    private String outputTopic;
    private String applicationId;
    private Properties streamsConfig;
    private KafkaStreams kafkaStreams;

    @BeforeEach
    public void before(final TestInfo testInfo) throws InterruptedException {
        final String safeTestName = safeUniqueTestName(testInfo);
        inputTopic = "input-" + safeTestName;
        outputTopic = "output-" + safeTestName;
        applicationId = "app-" + safeTestName;
        CLUSTER.createTopic(inputTopic, 1, 1);
        CLUSTER.createTopic(outputTopic, 1, 1);
        streamsConfig = buildStreamsConfig();
    }

    @AfterEach
    public void after() throws IOException {
        if (kafkaStreams != null) {
            kafkaStreams.close(Duration.ofSeconds(60));
            kafkaStreams = null;
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfig);
    }

    @Test
    public void shouldDeduplicateByKeyWithinInterval() throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(inputTopic)
               .deduplicateByKey(Duration.ofSeconds(10))
               .to(outputTopic);
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        startApplicationAndWaitUntilRunning(kafkaStreams);

        produce(Collections.singletonList(new KeyValue<>("k1", "v1")), 0L);
        produce(Collections.singletonList(new KeyValue<>("k1", "v2")), 5_000L);
        produce(Collections.singletonList(new KeyValue<>("k1", "v3")), 9_999L);
        produce(Collections.singletonList(new KeyValue<>("k2", "end")), 9_999L);

        final List<KeyValue<String, String>> output = waitUntilMinKeyValueRecordsReceived(
            buildConsumerConfig(), outputTopic, 2);

        assertEquals(2, output.size());
        assertEquals(new KeyValue<>("k1", "v1"), output.get(0));
        assertEquals(new KeyValue<>("k2", "end"), output.get(1));
    }

    @Test
    public void shouldForwardSameKeyAfterIntervalExpiry() throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(inputTopic)
               .deduplicateByKey(Duration.ofSeconds(10))
               .to(outputTopic);
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        startApplicationAndWaitUntilRunning(kafkaStreams);

        // k1 window opens at t=0.
        produce(Collections.singletonList(new KeyValue<>("k1", "v1")), 0L);
        // Advance stream time to 11000ms. Punctuator fires; k1 entry (t=0) is evicted
        produce(Collections.singletonList(new KeyValue<>("k2", "advance")), 11_000L);
        produce(Collections.singletonList(new KeyValue<>("k1", "v_new")), 11_000L);

        final List<KeyValue<String, String>> output = waitUntilMinKeyValueRecordsReceived(
            buildConsumerConfig(), outputTopic, 3);

        assertEquals(3, output.size());
        assertEquals(new KeyValue<>("k1", "v1"), output.get(0));
        assertEquals(new KeyValue<>("k2", "advance"), output.get(1));
        assertEquals(new KeyValue<>("k1", "v_new"), output.get(2));
    }

    @Test
    public void shouldDeduplicateByKeyValueOnKeyAndId() throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(inputTopic)
               .deduplicateByKeyValue((k, v) -> k + "|" + v, Duration.ofSeconds(10), Deduplicated.idSerde(Serdes.String()))
               .to(outputTopic);
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        startApplicationAndWaitUntilRunning(kafkaStreams);

        produce(Collections.singletonList(new KeyValue<>("k1", "id-A")), 0L);
        produce(Collections.singletonList(new KeyValue<>("k2", "id-A")), 0L);
        produce(Collections.singletonList(new KeyValue<>("k1", "id-A")), 5_000L);

        final List<KeyValue<String, String>> output = waitUntilMinKeyValueRecordsReceived(
            buildConsumerConfig(), outputTopic, 2);

        assertEquals(2, output.size());
        assertEquals(new KeyValue<>("k1", "id-A"), output.get(0));
        assertEquals(new KeyValue<>("k2", "id-A"), output.get(1));
    }

    private void produce(final List<KeyValue<String, String>> records, final long timestamp) {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputTopic, records, buildProducerConfig(), timestamp);
    }

    private Properties buildStreamsConfig() {
        final Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        p.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        return p;
    }

    private Properties buildProducerConfig() {
        return TestUtils.producerConfig(
            CLUSTER.bootstrapServers(),
            StringSerializer.class,
            StringSerializer.class,
            new Properties()
        );
    }

    private Properties buildConsumerConfig() {
        final Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-" + applicationId);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return p;
    }
}
