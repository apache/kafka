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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.cleanStateBeforeTest;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.getStartedStreams;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.quietlyCleanStateAfterTest;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag("integration")
@Timeout(600)
public class ResetPartitionTimeIntegrationTest {
    private static final int NUM_BROKERS = 1;
    private static final Properties BROKER_CONFIG;
    private static final long NOW = Instant.now().toEpochMilli();

    static {
        BROKER_CONFIG = new Properties();
        BROKER_CONFIG.put("transaction.state.log.replication.factor", (short) 1);
        BROKER_CONFIG.put("transaction.state.log.min.isr", 1);
    }
    public static final EmbeddedKafkaCluster CLUSTER =
        new EmbeddedKafkaCluster(NUM_BROKERS, BROKER_CONFIG);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    private static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();
    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final int DEFAULT_TIMEOUT = 100;
    private static long lastRecordedTimestamp = -2L;

    @ParameterizedTest
    @ValueSource(strings = {StreamsConfig.AT_LEAST_ONCE, StreamsConfig.EXACTLY_ONCE_V2})
    public void shouldPreservePartitionTimeOnKafkaStreamRestart(final String processingGuarantee, final TestInfo testInfo) {
        final String appId = "app-" + safeUniqueTestName(testInfo);
        final String input = "input";
        final String outputRaw = "output-raw";

        cleanStateBeforeTest(CLUSTER, 2, input, outputRaw);

        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream(input, Consumed.with(STRING_SERDE, STRING_SERDE))
            .to(outputRaw);

        final Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MaxTimestampExtractor.class);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfig.put(StreamsConfig.POLL_MS_CONFIG, Integer.toString(DEFAULT_TIMEOUT));
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, (long) DEFAULT_TIMEOUT);
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee);
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());

        KafkaStreams kafkaStreams = getStartedStreams(streamsConfig, builder, true);
        try {
            // start sending some records to have partition time committed 
            produceSynchronouslyToPartitionZero(
                input,
                Collections.singletonList(
                    new KeyValueTimestamp<>("k3", "v3", NOW + 5000)
                )
            );
            verifyOutput(
                outputRaw,
                Collections.singletonList(
                    new KeyValueTimestamp<>("k3", "v3", NOW + 5000)
                )
            );
            assertThat(lastRecordedTimestamp, is(-1L));
            lastRecordedTimestamp = -2L;

            kafkaStreams.close();
            assertThat(kafkaStreams.state(), is(KafkaStreams.State.NOT_RUNNING));

            kafkaStreams = getStartedStreams(streamsConfig, builder, true);

            // resend some records and retrieve the last committed timestamp
            produceSynchronouslyToPartitionZero(
                input,
                Collections.singletonList(
                    new KeyValueTimestamp<>("k5", "v5", NOW + 4999)
                )
            );
            verifyOutput(
                outputRaw,
                Collections.singletonList(
                    new KeyValueTimestamp<>("k5", "v5", NOW + 4999)
                )
            );
            assertThat(lastRecordedTimestamp, is(NOW + 5000L));
        } finally {
            kafkaStreams.close();
            quietlyCleanStateAfterTest(CLUSTER, kafkaStreams);
        }
    }

    public static final class MaxTimestampExtractor implements TimestampExtractor {
        @Override
        public long extract(final ConsumerRecord<Object, Object> record, final long partitionTime) {
            lastRecordedTimestamp = partitionTime;
            return record.timestamp();
        }
    }

    private void verifyOutput(final String topic, final List<KeyValueTimestamp<String, String>> keyValueTimestamps) {
        final Properties properties = mkProperties(
            mkMap(
                mkEntry(ConsumerConfig.GROUP_ID_CONFIG, "test-group"),
                mkEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ((Deserializer<String>) STRING_DESERIALIZER).getClass().getName()),
                mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ((Deserializer<String>) STRING_DESERIALIZER).getClass().getName())
            )
        );
        IntegrationTestUtils.verifyKeyValueTimestamps(properties, topic, keyValueTimestamps);
    }

    private static void produceSynchronouslyToPartitionZero(final String topic, final List<KeyValueTimestamp<String, String>> toProduce) {
        final Properties producerConfig = mkProperties(mkMap(
            mkEntry(ProducerConfig.CLIENT_ID_CONFIG, "anything"),
            mkEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ((Serializer<String>) STRING_SERIALIZER).getClass().getName()),
            mkEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ((Serializer<String>) STRING_SERIALIZER).getClass().getName()),
            mkEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
        ));
        IntegrationTestUtils.produceSynchronously(producerConfig, false, topic, Optional.of(0), toProduce);
    }
}
