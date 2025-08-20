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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.test.MockMapper;
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
import java.util.List;
import java.util.Properties;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofMinutes;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;

/**
 * Similar to KStreamAggregationIntegrationTest but with dedupping enabled
 * by virtue of having a large commit interval
 */
@Timeout(600)
@Tag("integration")
public class KStreamAggregationDedupIntegrationTest {
    private static final int NUM_BROKERS = 1;
    private static final long COMMIT_INTERVAL_MS = 300L;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }


    private final MockTime mockTime = CLUSTER.time;
    private StreamsBuilder builder;
    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;
    private String streamOneInput;
    private String outputTopic;
    private KGroupedStream<String, String> groupedStream;
    private Reducer<String> reducer;
    private KStream<Integer, String> stream;

    @BeforeEach
    public void before(final TestInfo testInfo) throws InterruptedException {
        builder = new StreamsBuilder();
        final String safeTestName = safeUniqueTestName(testInfo);
        createTopics(safeTestName);
        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL_MS);
        streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 10 * 1024 * 1024L);

        final KeyValueMapper<Integer, String, String> mapper = MockMapper.selectValueMapper();
        stream = builder.stream(streamOneInput, Consumed.with(Serdes.Integer(), Serdes.String()));
        groupedStream = stream.groupBy(mapper, Grouped.with(Serdes.String(), Serdes.String()));

        reducer = (value1, value2) -> value1 + ":" + value2;
    }

    @AfterEach
    public void whenShuttingDown() throws IOException {
        if (kafkaStreams != null) {
            kafkaStreams.close(Duration.ofSeconds(60));
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }


    @Test
    public void shouldReduce(final TestInfo testInfo) throws Exception {
        produceMessages(System.currentTimeMillis());
        groupedStream
                .reduce(reducer, Materialized.as("reduce-by-key"))
                .toStream()
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        startStreams();

        final long timestamp = System.currentTimeMillis();
        produceMessages(timestamp);

        validateReceivedMessages(
                new StringDeserializer(),
                new StringDeserializer(),
                Arrays.asList(
                    new KeyValueTimestamp<>("A", "A:A", timestamp),
                    new KeyValueTimestamp<>("B", "B:B", timestamp),
                    new KeyValueTimestamp<>("C", "C:C", timestamp),
                    new KeyValueTimestamp<>("D", "D:D", timestamp),
                    new KeyValueTimestamp<>("E", "E:E", timestamp)),
                testInfo);
    }

    @Test
    public void shouldReduceWindowed(final TestInfo testInfo) throws Exception {
        final long firstBatchTimestamp = System.currentTimeMillis() - 1000;
        produceMessages(firstBatchTimestamp);
        final long secondBatchTimestamp = System.currentTimeMillis();
        produceMessages(secondBatchTimestamp);
        produceMessages(secondBatchTimestamp);

        groupedStream
            .windowedBy(TimeWindows.ofSizeAndGrace(ofMillis(500L), ofMinutes(1L)))
            .reduce(reducer, Materialized.as("reduce-time-windows"))
            .toStream((windowedKey, value) -> windowedKey.key() + "@" + windowedKey.window().start())
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        startStreams();

        final long firstBatchWindow = firstBatchTimestamp / 500 * 500;
        final long secondBatchWindow = secondBatchTimestamp / 500 * 500;

        validateReceivedMessages(
                new StringDeserializer(),
                new StringDeserializer(),
                Arrays.asList(
                    new KeyValueTimestamp<>("A@" + firstBatchWindow, "A", firstBatchTimestamp),
                    new KeyValueTimestamp<>("A@" + secondBatchWindow, "A:A", secondBatchTimestamp),
                    new KeyValueTimestamp<>("B@" + firstBatchWindow, "B", firstBatchTimestamp),
                    new KeyValueTimestamp<>("B@" + secondBatchWindow, "B:B", secondBatchTimestamp),
                    new KeyValueTimestamp<>("C@" + firstBatchWindow, "C", firstBatchTimestamp),
                    new KeyValueTimestamp<>("C@" + secondBatchWindow, "C:C", secondBatchTimestamp),
                    new KeyValueTimestamp<>("D@" + firstBatchWindow, "D", firstBatchTimestamp),
                    new KeyValueTimestamp<>("D@" + secondBatchWindow, "D:D", secondBatchTimestamp),
                    new KeyValueTimestamp<>("E@" + firstBatchWindow, "E", firstBatchTimestamp),
                    new KeyValueTimestamp<>("E@" + secondBatchWindow, "E:E", secondBatchTimestamp)
                ),
                testInfo
        );
    }

    @Test
    public void shouldGroupByKey(final TestInfo testInfo) throws Exception {
        final long timestamp = mockTime.milliseconds();
        produceMessages(timestamp);
        produceMessages(timestamp);

        stream.groupByKey(Grouped.with(Serdes.Integer(), Serdes.String()))
            .windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(500L)))
            .count(Materialized.as("count-windows"))
            .toStream((windowedKey, value) -> windowedKey.key() + "@" + windowedKey.window().start())
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        startStreams();

        final long window = timestamp / 500 * 500;

        validateReceivedMessages(
                new StringDeserializer(),
                new LongDeserializer(),
                Arrays.asList(
                    new KeyValueTimestamp<>("1@" + window, 2L, timestamp),
                    new KeyValueTimestamp<>("2@" + window, 2L, timestamp),
                    new KeyValueTimestamp<>("3@" + window, 2L, timestamp),
                    new KeyValueTimestamp<>("4@" + window, 2L, timestamp),
                    new KeyValueTimestamp<>("5@" + window, 2L, timestamp)
                ),
                testInfo
        );
    }


    private void produceMessages(final long timestamp) throws Exception {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            streamOneInput,
            Arrays.asList(
                new KeyValue<>(1, "A"),
                new KeyValue<>(2, "B"),
                new KeyValue<>(3, "C"),
                new KeyValue<>(4, "D"),
                new KeyValue<>(5, "E")),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                StringSerializer.class,
                new Properties()),
            timestamp);
    }


    private void createTopics(final String safeTestName) throws InterruptedException {
        streamOneInput = "stream-one-" + safeTestName;
        outputTopic = "output-" + safeTestName;
        CLUSTER.createTopic(streamOneInput, 3, 1);
        CLUSTER.createTopic(outputTopic);
    }

    private void startStreams() {
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();
    }


    private <K, V> void validateReceivedMessages(final Deserializer<K> keyDeserializer,
                                                 final Deserializer<V> valueDeserializer,
                                                 final List<KeyValueTimestamp<K, V>> expectedRecords,
                                                 final TestInfo testInfo)
            throws Exception {

        final String safeTestName = safeUniqueTestName(testInfo);
        final Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-" + safeTestName);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass().getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass().getName());

        IntegrationTestUtils.waitUntilFinalKeyValueTimestampRecordsReceived(
            consumerProperties,
            outputTopic,
            expectedRecords);
    }

}
