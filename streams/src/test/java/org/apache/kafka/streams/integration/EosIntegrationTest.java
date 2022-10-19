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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForEmptyConsumerGroup;
import static org.apache.kafka.test.StreamsTestUtils.startKafkaStreamsAndWaitForRunningState;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(Parameterized.class)
@Category({IntegrationTest.class})
public class EosIntegrationTest {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(600);
    private static final int NUM_BROKERS = 3;
    private static final int MAX_POLL_INTERVAL_MS = 5 * 1000;
    private static final int MAX_WAIT_TIME_MS = 60 * 1000;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(
        NUM_BROKERS,
        Utils.mkProperties(Collections.singletonMap("auto.create.topics.enable", "true"))
    );

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }


    private String applicationId;
    private final static int NUM_TOPIC_PARTITIONS = 2;
    private final static String CONSUMER_GROUP_ID = "readCommitted";
    private final static String SINGLE_PARTITION_INPUT_TOPIC = "singlePartitionInputTopic";
    private final static String SINGLE_PARTITION_THROUGH_TOPIC = "singlePartitionThroughTopic";
    private final static String SINGLE_PARTITION_OUTPUT_TOPIC = "singlePartitionOutputTopic";
    private final static String MULTI_PARTITION_INPUT_TOPIC = "multiPartitionInputTopic";
    private final static String MULTI_PARTITION_THROUGH_TOPIC = "multiPartitionThroughTopic";
    private final static String MULTI_PARTITION_OUTPUT_TOPIC = "multiPartitionOutputTopic";

    private static final AtomicInteger TEST_NUMBER = new AtomicInteger(0);

    @SuppressWarnings("deprecation")
    @Parameters(name = "{0}")
    public static Collection<String[]> data() {
        return Arrays.asList(new String[][]{
            {StreamsConfig.AT_LEAST_ONCE},
            {StreamsConfig.EXACTLY_ONCE},
            {StreamsConfig.EXACTLY_ONCE_V2}
        });
    }

    @Parameter
    public String eosConfig;

    @Before
    public void createTopics() throws Exception {
        applicationId = "appId-" + TEST_NUMBER.getAndIncrement();
        CLUSTER.deleteTopicsAndWait(
            SINGLE_PARTITION_INPUT_TOPIC, MULTI_PARTITION_INPUT_TOPIC,
            SINGLE_PARTITION_THROUGH_TOPIC, MULTI_PARTITION_THROUGH_TOPIC,
            SINGLE_PARTITION_OUTPUT_TOPIC, MULTI_PARTITION_OUTPUT_TOPIC);

        CLUSTER.createTopics(SINGLE_PARTITION_INPUT_TOPIC, SINGLE_PARTITION_THROUGH_TOPIC, SINGLE_PARTITION_OUTPUT_TOPIC);
        CLUSTER.createTopic(MULTI_PARTITION_INPUT_TOPIC, NUM_TOPIC_PARTITIONS, 1);
        CLUSTER.createTopic(MULTI_PARTITION_THROUGH_TOPIC, NUM_TOPIC_PARTITIONS, 1);
        CLUSTER.createTopic(MULTI_PARTITION_OUTPUT_TOPIC, NUM_TOPIC_PARTITIONS, 1);
    }

    @Test
    public void shouldBeAbleToRunWithEosEnabled() throws Exception {
        runSimpleCopyTest(1, SINGLE_PARTITION_INPUT_TOPIC, null, SINGLE_PARTITION_OUTPUT_TOPIC, false, eosConfig);
    }

    @Test
    public void shouldCommitCorrectOffsetIfInputTopicIsTransactional() throws Exception {
        runSimpleCopyTest(1, SINGLE_PARTITION_INPUT_TOPIC, null, SINGLE_PARTITION_OUTPUT_TOPIC, true, eosConfig);

        try (final Admin adminClient = Admin.create(mkMap(mkEntry(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())));
             final Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(mkMap(
                 mkEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                 mkEntry(ConsumerConfig.GROUP_ID_CONFIG, applicationId),
                 mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class),
                 mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class)))) {

            waitForEmptyConsumerGroup(adminClient, applicationId, 5 * MAX_POLL_INTERVAL_MS);

            final TopicPartition topicPartition = new TopicPartition(SINGLE_PARTITION_INPUT_TOPIC, 0);
            final Collection<TopicPartition> topicPartitions = Collections.singleton(topicPartition);

            final long committedOffset = adminClient.listConsumerGroupOffsets(applicationId).partitionsToOffsetAndMetadata().get().get(topicPartition).offset();

            consumer.assign(topicPartitions);
            final long consumerPosition = consumer.position(topicPartition);
            final long endOffset = consumer.endOffsets(topicPartitions).get(topicPartition);

            assertThat(committedOffset, equalTo(consumerPosition));
            assertThat(committedOffset, equalTo(endOffset));
        }
    }

    @Test
    public void shouldBeAbleToRestartAfterClose() throws Exception {
        runSimpleCopyTest(2, SINGLE_PARTITION_INPUT_TOPIC, null, SINGLE_PARTITION_OUTPUT_TOPIC, false, eosConfig);
    }

    @Test
    public void shouldBeAbleToCommitToMultiplePartitions() throws Exception {
        runSimpleCopyTest(1, SINGLE_PARTITION_INPUT_TOPIC, null, MULTI_PARTITION_OUTPUT_TOPIC, false, eosConfig);
    }

    @Test
    public void shouldBeAbleToCommitMultiplePartitionOffsets() throws Exception {
        runSimpleCopyTest(1, MULTI_PARTITION_INPUT_TOPIC, null, SINGLE_PARTITION_OUTPUT_TOPIC, false, eosConfig);
    }

    @Test
    public void shouldBeAbleToRunWithTwoSubtopologies() throws Exception {
        runSimpleCopyTest(1, SINGLE_PARTITION_INPUT_TOPIC, SINGLE_PARTITION_THROUGH_TOPIC, SINGLE_PARTITION_OUTPUT_TOPIC, false, eosConfig);
    }

    @Test
    public void shouldBeAbleToRunWithTwoSubtopologiesAndMultiplePartitions() throws Exception {
        runSimpleCopyTest(1, MULTI_PARTITION_INPUT_TOPIC, MULTI_PARTITION_THROUGH_TOPIC, MULTI_PARTITION_OUTPUT_TOPIC, false, eosConfig);
    }

    private void runSimpleCopyTest(final int numberOfRestarts,
                                   final String inputTopic,
                                   final String throughTopic,
                                   final String outputTopic,
                                   final boolean inputTopicTransactional,
                                   final String eosConfig) throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Long, Long> input = builder.stream(inputTopic);
        KStream<Long, Long> output = input;
        if (throughTopic != null) {
            input.to(throughTopic);
            output = builder.stream(throughTopic);
        }
        output.to(outputTopic);

        final Properties properties = new Properties();
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, eosConfig);
        properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), 1);
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), "1000");
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), MAX_POLL_INTERVAL_MS - 1);
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), MAX_POLL_INTERVAL_MS);

        for (int i = 0; i < numberOfRestarts; ++i) {
            final Properties config = StreamsTestUtils.getStreamsConfig(
                applicationId,
                CLUSTER.bootstrapServers(),
                Serdes.LongSerde.class.getName(),
                Serdes.LongSerde.class.getName(),
                properties);

            final List<KeyValue<Long, Long>> inputData = prepareData(i * 100, i * 100 + 10L, 0L, 1L);

            final Properties producerConfigs = new Properties();
            if (inputTopicTransactional) {
                producerConfigs.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, applicationId + "-input-producer");
            }

            IntegrationTestUtils.produceKeyValuesSynchronously(
                inputTopic,
                inputData,
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class, LongSerializer.class, producerConfigs),
                CLUSTER.time,
                inputTopicTransactional
            );

            try (final KafkaStreams streams = new KafkaStreams(builder.build(), config)) {
                startKafkaStreamsAndWaitForRunningState(streams, MAX_WAIT_TIME_MS);

                final List<KeyValue<Long, Long>> committedRecords = readResult(outputTopic, inputData.size(), CONSUMER_GROUP_ID);
                checkResultPerKey(committedRecords, inputData, "The committed records do not match what expected");
            }
        }
    }

    private void checkResultPerKey(final List<KeyValue<Long, Long>> result,
                                   final List<KeyValue<Long, Long>> expectedResult,
                                   final String reason) {
        final Set<Long> allKeys = new HashSet<>();
        addAllKeys(allKeys, result);
        addAllKeys(allKeys, expectedResult);

        for (final Long key : allKeys) {
            assertThat(reason, getAllRecordPerKey(key, result), equalTo(getAllRecordPerKey(key, expectedResult)));
        }
    }

    private void addAllKeys(final Set<Long> allKeys, final List<KeyValue<Long, Long>> records) {
        for (final KeyValue<Long, Long> record : records) {
            allKeys.add(record.key);
        }
    }

    private List<KeyValue<Long, Long>> getAllRecordPerKey(final Long key, final List<KeyValue<Long, Long>> records) {
        final List<KeyValue<Long, Long>> recordsPerKey = new ArrayList<>(records.size());

        for (final KeyValue<Long, Long> record : records) {
            if (record.key.equals(key)) {
                recordsPerKey.add(record);
            }
        }

        return recordsPerKey;
    }

    @Test
    public void shouldBeAbleToPerformMultipleTransactions() throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(SINGLE_PARTITION_INPUT_TOPIC).to(SINGLE_PARTITION_OUTPUT_TOPIC);

        final Properties properties = new Properties();
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, eosConfig);
        properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        properties.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Properties config = StreamsTestUtils.getStreamsConfig(
            applicationId,
            CLUSTER.bootstrapServers(),
            Serdes.LongSerde.class.getName(),
            Serdes.LongSerde.class.getName(),
            properties);

        try (final KafkaStreams streams = new KafkaStreams(builder.build(), config)) {
            startKafkaStreamsAndWaitForRunningState(streams, MAX_WAIT_TIME_MS);

            final List<KeyValue<Long, Long>> firstBurstOfData = prepareData(0L, 5L, 0L);
            final List<KeyValue<Long, Long>> secondBurstOfData = prepareData(5L, 8L, 0L);

            IntegrationTestUtils.produceKeyValuesSynchronously(
                SINGLE_PARTITION_INPUT_TOPIC,
                firstBurstOfData,
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class,
                    LongSerializer.class),
                CLUSTER.time
            );

            final List<KeyValue<Long, Long>> firstCommittedRecords = readResult(
                SINGLE_PARTITION_OUTPUT_TOPIC, firstBurstOfData.size(), CONSUMER_GROUP_ID);
            assertThat(firstCommittedRecords, equalTo(firstBurstOfData));

            IntegrationTestUtils.produceKeyValuesSynchronously(
                SINGLE_PARTITION_INPUT_TOPIC,
                secondBurstOfData,
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class,
                    LongSerializer.class),
                CLUSTER.time
            );

            final List<KeyValue<Long, Long>> secondCommittedRecords = readResult(
                SINGLE_PARTITION_OUTPUT_TOPIC, secondBurstOfData.size(), CONSUMER_GROUP_ID);
            assertThat(secondCommittedRecords, equalTo(secondBurstOfData));
        }
    }

    private List<KeyValue<Long, Long>> prepareData(final long fromInclusive,
                                                   final long toExclusive,
                                                   final Long... keys) {
        final long dataSize = keys.length * (toExclusive - fromInclusive);
        final List<KeyValue<Long, Long>> data = new ArrayList<>((int) dataSize);

        for (final Long k : keys) {
            for (long v = fromInclusive; v < toExclusive; ++v) {
                data.add(new KeyValue<>(k, v));
            }
        }

        return data;
    }

    private List<KeyValue<Long, Long>> readResult(final String topic,
                                                  final int numberOfRecords,
                                                  final String groupId) throws Exception {
        if (groupId != null) {
            return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                TestUtils.consumerConfig(
                    CLUSTER.bootstrapServers(),
                    groupId,
                    LongDeserializer.class,
                    LongDeserializer.class,
                    Utils.mkProperties(Collections.singletonMap(
                        ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                        IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT)))),
                topic,
                numberOfRecords
            );
        }

        // read uncommitted
        return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            TestUtils.consumerConfig(CLUSTER.bootstrapServers(), LongDeserializer.class, LongDeserializer.class),
            topic,
            numberOfRecords
        );
    }
}
