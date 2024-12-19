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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.DEFAULT_TIMEOUT;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.purgeLocalStreamsState;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForEmptyConsumerGroup;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinRecordsReceived;
import static org.apache.kafka.streams.query.StateQueryRequest.inStore;
import static org.apache.kafka.streams.utils.TestUtils.waitForApplicationState;
import static org.apache.kafka.test.TestUtils.consumerConfig;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
@Timeout(600)
public class EosIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(EosIntegrationTest.class);
    private static final int NUM_BROKERS = 3;
    private static final int MAX_POLL_INTERVAL_MS = 30_000;
    private static final int MAX_WAIT_TIME_MS = 120_000;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(
        NUM_BROKERS,
        Utils.mkProperties(Map.of("auto.create.topics.enable", "true",
                GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, "classic,consumer,streams",
                ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, "true"))
    );

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }


    private String applicationId;
    private static final int NUM_TOPIC_PARTITIONS = 2;
    private static final String CONSUMER_GROUP_ID = "readCommitted";
    private static final String SINGLE_PARTITION_INPUT_TOPIC = "singlePartitionInputTopic";
    private static final String SINGLE_PARTITION_THROUGH_TOPIC = "singlePartitionThroughTopic";
    private static final String SINGLE_PARTITION_OUTPUT_TOPIC = "singlePartitionOutputTopic";
    private static final String MULTI_PARTITION_INPUT_TOPIC = "multiPartitionInputTopic";
    private static final String MULTI_PARTITION_THROUGH_TOPIC = "multiPartitionThroughTopic";
    private static final String MULTI_PARTITION_OUTPUT_TOPIC = "multiPartitionOutputTopic";
    private final String storeName = "store";

    private AtomicBoolean errorInjected;
    private AtomicBoolean stallInjected;
    private AtomicReference<String> stallingHost;
    private volatile boolean doStall = true;
    private AtomicInteger commitRequested;
    private Throwable uncaughtException;

    private static final AtomicInteger TEST_NUMBER = new AtomicInteger(0);

    private volatile boolean hasUnexpectedError = false;

    private String stateTmpDir;

    @BeforeEach
    public void createTopics() throws Exception {
        applicationId = "appId-" + TEST_NUMBER.getAndIncrement();
        CLUSTER.deleteTopics(
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
        runSimpleCopyTest(1, SINGLE_PARTITION_INPUT_TOPIC, null, SINGLE_PARTITION_OUTPUT_TOPIC, false);
    }

    @Test
    public void shouldCommitCorrectOffsetIfInputTopicIsTransactional() throws Exception {
        runSimpleCopyTest(1, SINGLE_PARTITION_INPUT_TOPIC, null, SINGLE_PARTITION_OUTPUT_TOPIC, true);

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
        runSimpleCopyTest(2, SINGLE_PARTITION_INPUT_TOPIC, null, SINGLE_PARTITION_OUTPUT_TOPIC, false);
    }

    @Test
    public void shouldBeAbleToCommitToMultiplePartitions() throws Exception {
        runSimpleCopyTest(1, SINGLE_PARTITION_INPUT_TOPIC, null, MULTI_PARTITION_OUTPUT_TOPIC, false);
    }

    @Test
    public void shouldBeAbleToCommitMultiplePartitionOffsets() throws Exception {
        runSimpleCopyTest(1, MULTI_PARTITION_INPUT_TOPIC, null, SINGLE_PARTITION_OUTPUT_TOPIC, false);
    }

    @Test
    public void shouldBeAbleToRunWithTwoSubtopologies() throws Exception {
        runSimpleCopyTest(1, SINGLE_PARTITION_INPUT_TOPIC, SINGLE_PARTITION_THROUGH_TOPIC, SINGLE_PARTITION_OUTPUT_TOPIC, false);
    }

    @Test
    public void shouldBeAbleToRunWithTwoSubtopologiesAndMultiplePartitions() throws Exception {
        runSimpleCopyTest(1, MULTI_PARTITION_INPUT_TOPIC, MULTI_PARTITION_THROUGH_TOPIC, MULTI_PARTITION_OUTPUT_TOPIC, false);
    }

    private void runSimpleCopyTest(final int numberOfRestarts,
                                   final String inputTopic,
                                   final String throughTopic,
                                   final String outputTopic,
                                   final boolean inputTopicTransactional) throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Long, Long> input = builder.stream(inputTopic);
        KStream<Long, Long> output = input;
        if (throughTopic != null) {
            input.to(throughTopic);
            output = builder.stream(throughTopic);
        }
        output.to(outputTopic);

        final Properties properties = new Properties();
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        properties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:7777");
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
                startApplicationAndWaitUntilRunning(streams);

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
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
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
            startApplicationAndWaitUntilRunning(streams);

            final List<KeyValue<Long, Long>> firstBurstOfData = prepareData(0L, 5L, 0L);
            final List<KeyValue<Long, Long>> secondBurstOfData = prepareData(5L, 8L, 0L);

            IntegrationTestUtils.produceKeyValuesSynchronously(
                SINGLE_PARTITION_INPUT_TOPIC,
                firstBurstOfData,
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class, LongSerializer.class),
                CLUSTER.time
            );

            final List<KeyValue<Long, Long>> firstCommittedRecords = readResult(SINGLE_PARTITION_OUTPUT_TOPIC, firstBurstOfData.size(), CONSUMER_GROUP_ID);
            assertThat(firstCommittedRecords, equalTo(firstBurstOfData));

            IntegrationTestUtils.produceKeyValuesSynchronously(
                SINGLE_PARTITION_INPUT_TOPIC,
                secondBurstOfData,
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class, LongSerializer.class),
                CLUSTER.time
            );

            final List<KeyValue<Long, Long>> secondCommittedRecords = readResult(SINGLE_PARTITION_OUTPUT_TOPIC, secondBurstOfData.size(), CONSUMER_GROUP_ID);
            assertThat(secondCommittedRecords, equalTo(secondBurstOfData));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldNotViolateEosIfOneTaskFails(final boolean processingThreadsEnabled) throws Exception {

        // this test writes 10 + 5 + 5 records per partition (running with 2 partitions)
        // the app is supposed to copy all 40 records into the output topic
        //
        // the app first commits after each 10 records per partition(total 20 records), and thus will have 2 * 5 uncommitted writes
        //
        // the failure gets inject after 20 committed and 30 uncommitted records got received
        // -> the failure only kills one thread
        // after fail over, we should read 40 committed records (even if 50 record got written)

        try (final KafkaStreams streams = getKafkaStreams("dummy", false, "appDir", 2, processingThreadsEnabled)) {
            startApplicationAndWaitUntilRunning(streams);

            final List<KeyValue<Long, Long>> committedDataBeforeFailure = prepareData(0L, 10L, 0L, 1L);
            final List<KeyValue<Long, Long>> uncommittedDataBeforeFailure = prepareData(10L, 15L, 0L, 1L);

            final List<KeyValue<Long, Long>> dataBeforeFailure = new ArrayList<>(
                committedDataBeforeFailure.size() + uncommittedDataBeforeFailure.size());
            dataBeforeFailure.addAll(committedDataBeforeFailure);
            dataBeforeFailure.addAll(uncommittedDataBeforeFailure);

            final List<KeyValue<Long, Long>> dataAfterFailure = prepareData(15L, 20L, 0L, 1L);

            writeInputData(committedDataBeforeFailure);

            waitForCondition(
                () -> commitRequested.get() == 2, MAX_WAIT_TIME_MS,
                "StreamsTasks did not request commit.");

            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // p-0: ---> 10 rec + C
            // p-1: ---> 10 rec + C

            final List<KeyValue<Long, Long>> committedRecords = readResult(SINGLE_PARTITION_OUTPUT_TOPIC, committedDataBeforeFailure.size(), CONSUMER_GROUP_ID);
            checkResultPerKey(
                committedRecords,
                committedDataBeforeFailure,
                "The committed records before failure do not match what expected");

            writeInputData(uncommittedDataBeforeFailure);

            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // p-0: ---> 10 rec + C  + 5 rec (pending)
            // p-1: ---> 10 rec + C  + 5 rec (pending)

            final List<KeyValue<Long, Long>> uncommittedRecords = readResult(SINGLE_PARTITION_OUTPUT_TOPIC, dataBeforeFailure.size(), null);
            checkResultPerKey(
                uncommittedRecords,
                dataBeforeFailure,
                "The uncommitted records before failure do not match what expected");
            errorInjected.set(true);
            writeInputData(dataAfterFailure);

            waitForCondition(
                () -> uncaughtException != null, MAX_WAIT_TIME_MS,
                "Should receive uncaught exception from one StreamThread.");

            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // p-0: ---> 10 rec + C  + 5 rec + C    + 5 rec + C
            // p-1: ---> 10 rec + C  + 5 rec + C    + 5 rec + C

            final List<KeyValue<Long, Long>> allCommittedRecords = readResult(
                SINGLE_PARTITION_OUTPUT_TOPIC,
                committedDataBeforeFailure.size() + uncommittedDataBeforeFailure.size() + dataAfterFailure.size(),
                CONSUMER_GROUP_ID + "_ALL");

            final List<KeyValue<Long, Long>> committedRecordsAfterFailure = readResult(
                SINGLE_PARTITION_OUTPUT_TOPIC,
                uncommittedDataBeforeFailure.size() + dataAfterFailure.size(),
                CONSUMER_GROUP_ID);

            final int allCommittedRecordsAfterRecoverySize = committedDataBeforeFailure.size() +
                uncommittedDataBeforeFailure.size() + dataAfterFailure.size();
            final List<KeyValue<Long, Long>> allExpectedCommittedRecordsAfterRecovery = new ArrayList<>(allCommittedRecordsAfterRecoverySize);
            allExpectedCommittedRecordsAfterRecovery.addAll(committedDataBeforeFailure);
            allExpectedCommittedRecordsAfterRecovery.addAll(uncommittedDataBeforeFailure);
            allExpectedCommittedRecordsAfterRecovery.addAll(dataAfterFailure);

            final int committedRecordsAfterRecoverySize = uncommittedDataBeforeFailure.size() + dataAfterFailure.size();
            final List<KeyValue<Long, Long>> expectedCommittedRecordsAfterRecovery = new ArrayList<>(committedRecordsAfterRecoverySize);
            expectedCommittedRecordsAfterRecovery.addAll(uncommittedDataBeforeFailure);
            expectedCommittedRecordsAfterRecovery.addAll(dataAfterFailure);

            checkResultPerKey(
                allCommittedRecords,
                allExpectedCommittedRecordsAfterRecovery,
                "The all committed records after recovery do not match what expected");
            checkResultPerKey(
                committedRecordsAfterFailure,
                expectedCommittedRecordsAfterRecovery,
                "The committed records after recovery do not match what expected");

            assertThat("Should only get one uncaught exception from Streams.", hasUnexpectedError, is(false));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldNotViolateEosIfOneTaskFailsWithState(final boolean processingThreadsEnabled) throws Exception {

        // this test updates a store with 10 + 5 + 5 records per partition (running with 2 partitions)
        // the app is supposed to emit all 40 update records into the output topic
        //
        // the app first commits after each 10 records per partition (total 20 records), and thus will have 2 * 5 uncommitted writes
        // and store updates (ie, another 5 uncommitted writes to a changelog topic per partition)
        // in the uncommitted batch, sending some data for the new key to validate that upon resuming they will not be shown up in the store
        //
        // the failure gets inject after 20 committed and 30 uncommitted records got received
        // -> the failure only kills one thread
        // after fail over, we should read 40 committed records and the state stores should contain the correct sums
        // per key (even if some records got processed twice)

        // We need more processing time under "with state" situation, so increasing the max.poll.interval.ms
        // to avoid unexpected rebalance during test, which will cause unexpected fail over triggered
        try (final KafkaStreams streams = getKafkaStreams("dummy", true, "appDir", 2, processingThreadsEnabled)) {
            startApplicationAndWaitUntilRunning(streams);

            final List<KeyValue<Long, Long>> committedDataBeforeFailure = prepareData(0L, 10L, 0L, 1L);
            final List<KeyValue<Long, Long>> uncommittedDataBeforeFailure = prepareData(10L, 15L, 0L, 1L, 2L, 3L);

            final List<KeyValue<Long, Long>> dataBeforeFailure = new ArrayList<>(
                committedDataBeforeFailure.size() + uncommittedDataBeforeFailure.size());
            dataBeforeFailure.addAll(committedDataBeforeFailure);
            dataBeforeFailure.addAll(uncommittedDataBeforeFailure);

            final List<KeyValue<Long, Long>> dataAfterFailure = prepareData(15L, 20L, 0L, 1L);

            writeInputData(committedDataBeforeFailure);

            waitForCondition(
                () -> commitRequested.get() == 2, MAX_WAIT_TIME_MS,
                "StreamsTasks did not request commit.");

            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // p-0: ---> 10 rec + C
            // p-1: ---> 10 rec + C

            final List<KeyValue<Long, Long>> committedRecords = readResult(SINGLE_PARTITION_OUTPUT_TOPIC, committedDataBeforeFailure.size(), CONSUMER_GROUP_ID);
            checkResultPerKey(
                committedRecords,
                computeExpectedResult(committedDataBeforeFailure),
                "The committed records before failure do not match what expected");

            writeInputData(uncommittedDataBeforeFailure);

            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // p-0: ---> 10 rec + C  + 5 rec (pending)
            // p-1: ---> 10 rec + C  + 5 rec (pending)

            final List<KeyValue<Long, Long>> uncommittedRecords = readResult(SINGLE_PARTITION_OUTPUT_TOPIC, dataBeforeFailure.size(), null);
            final List<KeyValue<Long, Long>> expectedResultBeforeFailure = computeExpectedResult(dataBeforeFailure);


            checkResultPerKey(
                uncommittedRecords,
                expectedResultBeforeFailure,
                "The uncommitted records before failure do not match what expected");
            verifyStateStore(
                streams,
                getMaxPerKey(expectedResultBeforeFailure),
                "The state store content before failure do not match what expected");

            errorInjected.set(true);
            writeInputData(dataAfterFailure);

            waitForCondition(
                () -> uncaughtException != null, MAX_WAIT_TIME_MS,
                "Should receive uncaught exception from one StreamThread.");

            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // p-0: ---> 10 rec + C  + 5 rec + C    + 5 rec + C
            // p-1: ---> 10 rec + C  + 5 rec + C    + 5 rec + C

            final List<KeyValue<Long, Long>> allCommittedRecords = readResult(
                SINGLE_PARTITION_OUTPUT_TOPIC,
                committedDataBeforeFailure.size() + uncommittedDataBeforeFailure.size() + dataAfterFailure.size(),
                CONSUMER_GROUP_ID + "_ALL");

            final List<KeyValue<Long, Long>> committedRecordsAfterFailure = readResult(
                SINGLE_PARTITION_OUTPUT_TOPIC,
                uncommittedDataBeforeFailure.size() + dataAfterFailure.size(),
                CONSUMER_GROUP_ID);

            final int allCommittedRecordsAfterRecoverySize = committedDataBeforeFailure.size() +
                uncommittedDataBeforeFailure.size() + dataAfterFailure.size();
            final List<KeyValue<Long, Long>> allExpectedCommittedRecordsAfterRecovery = new ArrayList<>(allCommittedRecordsAfterRecoverySize);
            allExpectedCommittedRecordsAfterRecovery.addAll(committedDataBeforeFailure);
            allExpectedCommittedRecordsAfterRecovery.addAll(uncommittedDataBeforeFailure);
            allExpectedCommittedRecordsAfterRecovery.addAll(dataAfterFailure);

            final List<KeyValue<Long, Long>> expectedResult = computeExpectedResult(allExpectedCommittedRecordsAfterRecovery);

            checkResultPerKey(
                allCommittedRecords,
                expectedResult,
                "The all committed records after recovery do not match what expected");

            checkResultPerKey(
                committedRecordsAfterFailure,
                expectedResult.subList(committedDataBeforeFailure.size(), expectedResult.size()),
                "The committed records after recovery do not match what expected");

            verifyStateStore(
                streams,
                getMaxPerKey(expectedResult),
                "The state store content after recovery do not match what expected");

            assertThat("Should only get one uncaught exception from Streams.", hasUnexpectedError, is(false));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldNotViolateEosIfOneTaskGetsFencedUsingIsolatedAppInstances(final boolean processingThreadsEnabled) throws Exception {
        // this test writes 10 + 5 + 5 + 10 records per partition (running with 2 partitions)
        // the app is supposed to copy all 60 records into the output topic
        //
        // the app first commits after each 10 records per partition, and thus will have 2 * 5 uncommitted writes
        //
        // Then, a stall gets injected after 20 committed and 30 uncommitted records got received
        // -> the stall only affects one thread and should trigger a rebalance
        // after rebalancing, we should read 40 committed records (even if 50 record got written)
        //
        // afterward, the "stalling" thread resumes, and another rebalance should get triggered
        // we write the remaining 20 records and verify to read 60 result records

        try (
            final KafkaStreams streams1 = getKafkaStreams("streams1", false, "appDir1", 1, processingThreadsEnabled);
            final KafkaStreams streams2 = getKafkaStreams("streams2", false, "appDir2", 1, processingThreadsEnabled)
        ) {
            startApplicationAndWaitUntilRunning(streams1);
            startApplicationAndWaitUntilRunning(streams2);

            final List<KeyValue<Long, Long>> committedDataBeforeStall = prepareData(0L, 10L, 0L, 1L);
            final List<KeyValue<Long, Long>> uncommittedDataBeforeStall = prepareData(10L, 15L, 0L, 1L);

            final List<KeyValue<Long, Long>> dataBeforeStall = new ArrayList<>(
                committedDataBeforeStall.size() + uncommittedDataBeforeStall.size());
            dataBeforeStall.addAll(committedDataBeforeStall);
            dataBeforeStall.addAll(uncommittedDataBeforeStall);

            final List<KeyValue<Long, Long>> dataToTriggerFirstRebalance = prepareData(15L, 20L, 0L, 1L);

            final List<KeyValue<Long, Long>> dataAfterSecondRebalance = prepareData(20L, 30L, 0L, 1L);

            writeInputData(committedDataBeforeStall);

            waitForCondition(
                () -> commitRequested.get() == 2, MAX_WAIT_TIME_MS,
                "StreamsTasks did not request commit.");

            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // p-0: ---> 10 rec + C
            // p-1: ---> 10 rec + C

            final List<KeyValue<Long, Long>> committedRecords = readResult(SINGLE_PARTITION_OUTPUT_TOPIC, committedDataBeforeStall.size(), CONSUMER_GROUP_ID);
            checkResultPerKey(
                committedRecords,
                committedDataBeforeStall,
                "The committed records before stall do not match what expected");

            writeInputData(uncommittedDataBeforeStall);

            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // p-0: ---> 10 rec + C  + 5 rec (pending)
            // p-1: ---> 10 rec + C  + 5 rec (pending)

            final List<KeyValue<Long, Long>> uncommittedRecords = readResult(SINGLE_PARTITION_OUTPUT_TOPIC, dataBeforeStall.size(), null);
            checkResultPerKey(
                uncommittedRecords,
                dataBeforeStall,
                "The uncommitted records before stall do not match what expected");

            LOG.info("Injecting Stall");
            stallInjected.set(true);
            writeInputData(dataToTriggerFirstRebalance);
            LOG.info("Input Data Written");
            waitForCondition(
                () -> stallingHost.get() != null,
                MAX_WAIT_TIME_MS,
                "Expected a host to start stalling"
            );
            final String observedStallingHost = stallingHost.get();
            final KafkaStreams stallingInstance;
            final KafkaStreams remainingInstance;
            if ("streams1".equals(observedStallingHost)) {
                stallingInstance = streams1;
                remainingInstance = streams2;
            } else if ("streams2".equals(observedStallingHost)) {
                stallingInstance = streams2;
                remainingInstance = streams1;
            } else {
                throw new IllegalArgumentException("unexpected host name: " + observedStallingHost);
            }

            // the stalling instance won't have an updated view, and it doesn't matter what it thinks
            // the assignment is. We only really care that the remaining instance only sees one host
            // that owns both partitions.
            waitForCondition(
                () -> stallingInstance.metadataForAllStreamsClients().size() == 2
                    && remainingInstance.metadataForAllStreamsClients().size() == 1
                    && remainingInstance.metadataForAllStreamsClients().iterator().next().topicPartitions().size() == 2,
                MAX_WAIT_TIME_MS,
                () -> "Should have rebalanced.\n" +
                    "Streams1[" + streams1.metadataForAllStreamsClients() + "]\n" +
                    "Streams2[" + streams2.metadataForAllStreamsClients() + "]");

            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // p-0: ---> 10 rec + C  + 5 rec + C    + 5 rec + C
            // p-1: ---> 10 rec + C  + 5 rec + C    + 5 rec + C

            final List<KeyValue<Long, Long>> committedRecordsAfterRebalance = readResult(
                SINGLE_PARTITION_OUTPUT_TOPIC,
                uncommittedDataBeforeStall.size() + dataToTriggerFirstRebalance.size(),
                CONSUMER_GROUP_ID);

            final List<KeyValue<Long, Long>> expectedCommittedRecordsAfterRebalance = new ArrayList<>(
                uncommittedDataBeforeStall.size() + dataToTriggerFirstRebalance.size());
            expectedCommittedRecordsAfterRebalance.addAll(uncommittedDataBeforeStall);
            expectedCommittedRecordsAfterRebalance.addAll(dataToTriggerFirstRebalance);

            checkResultPerKey(
                committedRecordsAfterRebalance,
                expectedCommittedRecordsAfterRebalance,
                "The all committed records after rebalance do not match what expected");

            LOG.info("Releasing Stall");
            doStall = false;
            // Once the stalling host rejoins the group, we expect both instances to see both instances.
            // It doesn't really matter what the assignment is, but we might as well also assert that they
            // both see both partitions assigned exactly once
            waitForCondition(
                () -> streams1.metadataForAllStreamsClients().size() == 2
                    && streams2.metadataForAllStreamsClients().size() == 2
                    && streams1.metadataForAllStreamsClients().stream().mapToLong(meta -> meta.topicPartitions().size()).sum() == 2
                    && streams2.metadataForAllStreamsClients().stream().mapToLong(meta -> meta.topicPartitions().size()).sum() == 2,
                MAX_WAIT_TIME_MS,
                () -> "Should have rebalanced.\n" +
                    "Streams1[" + streams1.metadataForAllStreamsClients() + "]\n" +
                    "Streams2[" + streams2.metadataForAllStreamsClients() + "]");

            writeInputData(dataAfterSecondRebalance);

            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // p-0: ---> 10 rec + C  + 5 rec + C    + 5 rec + C   + 10 rec + C
            // p-1: ---> 10 rec + C  + 5 rec + C    + 5 rec + C   + 10 rec + C

            final List<KeyValue<Long, Long>> allCommittedRecords = readResult(
                SINGLE_PARTITION_OUTPUT_TOPIC,
                committedDataBeforeStall.size() + uncommittedDataBeforeStall.size()
                + dataToTriggerFirstRebalance.size() + dataAfterSecondRebalance.size(),
                CONSUMER_GROUP_ID + "_ALL");

            final int allCommittedRecordsAfterRecoverySize = committedDataBeforeStall.size() +
                uncommittedDataBeforeStall.size() + dataToTriggerFirstRebalance.size() + dataAfterSecondRebalance.size();
            final List<KeyValue<Long, Long>> allExpectedCommittedRecordsAfterRecovery = new ArrayList<>(allCommittedRecordsAfterRecoverySize);
            allExpectedCommittedRecordsAfterRecovery.addAll(committedDataBeforeStall);
            allExpectedCommittedRecordsAfterRecovery.addAll(uncommittedDataBeforeStall);
            allExpectedCommittedRecordsAfterRecovery.addAll(dataToTriggerFirstRebalance);
            allExpectedCommittedRecordsAfterRecovery.addAll(dataAfterSecondRebalance);

            checkResultPerKey(
                allCommittedRecords,
                allExpectedCommittedRecordsAfterRecovery,
                "The all committed records after recovery do not match what expected");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldWriteLatestOffsetsToCheckpointOnShutdown(final boolean processingThreadsEnabled) throws Exception {
        final List<KeyValue<Long, Long>> writtenData = prepareData(0L, 10, 0L, 1L);
        final List<KeyValue<Long, Long>> expectedResult = computeExpectedResult(writtenData);

        try (final KafkaStreams streams = getKafkaStreams("streams", true, "appDir", 1, processingThreadsEnabled)) {
            writeInputData(writtenData);

            startApplicationAndWaitUntilRunning(streams);

            waitForCondition(
                    () -> commitRequested.get() == 2, MAX_WAIT_TIME_MS,
                    "StreamsTasks did not request commit.");

            final List<KeyValue<Long, Long>> committedRecords = readResult(SINGLE_PARTITION_OUTPUT_TOPIC, writtenData.size(), CONSUMER_GROUP_ID);

            checkResultPerKey(
                    committedRecords,
                    expectedResult,
                    "The committed records do not match what expected");

            verifyStateStore(
                    streams,
                    getMaxPerKey(expectedResult),
                    "The state store content do not match what expected");
        }

        verifyOffsetsAreInCheckpoint(0);
        verifyOffsetsAreInCheckpoint(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldCheckpointRestoredOffsetsWhenClosingCleanDuringRestoringStateUpdaterEnabled(
            final boolean processingThreadsEnabled) throws Exception {
        shouldCheckpointRestoredOffsetsWhenClosingCleanDuringRestoring(processingThreadsEnabled, true);
    }

    @Test
    public void shouldCheckpointRestoredOffsetsWhenClosingCleanDuringRestoringStateUpdaterDisabled() throws Exception {
        shouldCheckpointRestoredOffsetsWhenClosingCleanDuringRestoring(false, false);
    }

    private void shouldCheckpointRestoredOffsetsWhenClosingCleanDuringRestoring(
            final boolean processingThreadsEnabled,
            final boolean stateUpdaterEnabled) throws Exception {

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(applicationId).getPath());
        streamsConfiguration.put(InternalConfig.PROCESSING_THREADS_ENABLED, processingThreadsEnabled);
        streamsConfiguration.put(InternalConfig.STATE_UPDATER_ENABLED, stateUpdaterEnabled);
        streamsConfiguration.put(StreamsConfig.restoreConsumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), 100);
        final String stateStoreName = "stateStore";

        purgeLocalStreamsState(streamsConfiguration);

        final int startKey = 1;
        final int endKey = 30001;
        final List<KeyValue<Integer, Integer>> recordBatch1 = IntStream.range(startKey, endKey - 1000).mapToObj(i -> KeyValue.pair(i, 0)).collect(Collectors.toList());
        IntegrationTestUtils.produceKeyValuesSynchronously(MULTI_PARTITION_INPUT_TOPIC,
            recordBatch1,
            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                IntegerSerializer.class),
            CLUSTER.time);

        final StoreBuilder<KeyValueStore<Integer, String>> stateStore = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(stateStoreName),
            Serdes.Integer(),
            Serdes.String()).withCachingEnabled();

        final int partitionToVerify = 0;
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean throwException = new AtomicBoolean(false);
        final TaskId task00 = new TaskId(0, partitionToVerify);
        final AtomicLong restoredOffsetsForPartition0 = new AtomicLong(0);
        final Topology topology = new Topology();
        topology
            .addSource("source", MULTI_PARTITION_INPUT_TOPIC)
            .addProcessor("processor", () -> new Processor<Integer, String, Integer, String>() {
                KeyValueStore<Integer, String> stateStore;
                ProcessorContext<Integer, String> context;

                @Override
                public void init(final ProcessorContext<Integer, String> context) {
                    Processor.super.init(context);
                    this.context = context;
                    stateStore = context.getStateStore(stateStoreName);
                }

                @Override
                public void process(final Record<Integer, String> record) {
                    context.recordMetadata().ifPresent(recordMetadata -> {
                        if (recordMetadata.partition() == partitionToVerify) {
                            if (throwException.compareAndSet(true, false)) {
                                throw new TaskCorruptedException(Collections.singleton(task00));
                            }
                            stateStore.put(record.key(), record.value());
                        } else {
                            stateStore.put(record.key(), record.value());
                        }
                    });
                }

                @Override
                public void close() {
                    Processor.super.close();
                }
            }, "source")
            .addStateStore(stateStore, "processor");

        final KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfiguration);
        kafkaStreams.setGlobalStateRestoreListener(new StateRestoreListener() {
            @Override
            public void onRestoreStart(final TopicPartition topicPartition,
                                       final String storeName,
                                       final long startingOffset,
                                       final long endingOffset) {}
            @Override
            public void onBatchRestored(final TopicPartition topicPartition,
                                        final String storeName,
                                        final long batchEndOffset,
                                        final long numRestored) {
                if (topicPartition.partition() == 0) {
                    restoredOffsetsForPartition0.set(batchEndOffset);
                    if (batchEndOffset > 100) {
                        latch.countDown();
                    }
                }
            }
            @Override
            public void onRestoreEnd(final TopicPartition topicPartition,
                                     final String storeName,
                                     final long totalRestored) {}
        });
        startApplicationAndWaitUntilRunning(Collections.singletonList(kafkaStreams), Duration.ofSeconds(60));
        ensureCommittedRecordsInTopicPartition(
            applicationId + "-" + stateStoreName + "-changelog",
            partitionToVerify,
            2000,
            IntegerDeserializer.class,
            IntegerDeserializer.class
        );
        throwException.set(true);
        final List<KeyValue<Integer, Integer>> recordBatch2 = IntStream.range(endKey - 1000, endKey).mapToObj(i -> KeyValue.pair(i, 0)).collect(Collectors.toList());
        IntegrationTestUtils.produceKeyValuesSynchronously(MULTI_PARTITION_INPUT_TOPIC,
            recordBatch2,
            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                IntegerSerializer.class),
            CLUSTER.time);
        latch.await();
        kafkaStreams.close();
        waitForApplicationState(Collections.singletonList(kafkaStreams), KafkaStreams.State.NOT_RUNNING, Duration.ofSeconds(60));

        final File checkpointFile = Paths.get(
            streamsConfiguration.getProperty(StreamsConfig.STATE_DIR_CONFIG),
            streamsConfiguration.getProperty(StreamsConfig.APPLICATION_ID_CONFIG),
            task00.toString(),
            ".checkpoint"
        ).toFile();
        assertTrue(checkpointFile.exists());
        final Map<TopicPartition, Long> checkpoints = new OffsetCheckpoint(checkpointFile).read();
        assertEquals(
            Long.valueOf(restoredOffsetsForPartition0.get()),
            new ArrayList<>(checkpoints.values()).get(0)
        );
    }

    private void verifyOffsetsAreInCheckpoint(final int partition) throws IOException {
        final String stateStoreDir = stateTmpDir + File.separator + "appDir" + File.separator + applicationId + File.separator + "0_" + partition + File.separator;

        // Verify that the checkpointed offsets match exactly with max offset of the records in the changelog
        final OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(stateStoreDir + ".checkpoint"));
        final Map<TopicPartition, Long> checkpointedOffsets = checkpoint.read();
        checkpointedOffsets.forEach(this::verifyChangelogMaxRecordOffsetMatchesCheckpointedOffset);
    }

    private void verifyChangelogMaxRecordOffsetMatchesCheckpointedOffset(final TopicPartition tp, final long checkpointedOffset) {
        try (
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                consumerConfig(
                    CLUSTER.bootstrapServers(),
                    Serdes.ByteArray().deserializer().getClass(),
                    Serdes.ByteArray().deserializer().getClass()
                )
            )
        ) {
            final List<TopicPartition> partitions = Collections.singletonList(tp);
            consumer.assign(partitions);
            consumer.seekToEnd(partitions);
            final long topicEndOffset = consumer.position(tp);

            assertTrue(topicEndOffset >= checkpointedOffset,
                "changelog topic end " + topicEndOffset + " is less than checkpointed offset " + checkpointedOffset);

            consumer.seekToBeginning(partitions);

            Long maxRecordOffset = null;
            while (consumer.position(tp) != topicEndOffset) {
                final List<ConsumerRecord<String, String>> records = consumer.poll(Duration.ofMillis(0)).records(tp);
                if (!records.isEmpty()) {
                    maxRecordOffset = records.get(records.size() - 1).offset();
                }
            }

            assertEquals(maxRecordOffset, (Long) checkpointedOffset,
                "Checkpointed offset does not match end of changelog");
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

    @SuppressWarnings("deprecation")
    // the threads should no longer fail one thread one at a time
    private KafkaStreams getKafkaStreams(final String dummyHostName,
                                         final boolean withState,
                                         final String appDir,
                                         final int numberOfStreamsThreads,
                                         final boolean processingThreadsEnabled) {
        commitRequested = new AtomicInteger(0);
        errorInjected = new AtomicBoolean(false);
        stallInjected = new AtomicBoolean(false);
        stallingHost = new AtomicReference<>();
        final StreamsBuilder builder = new StreamsBuilder();

        String[] storeNames = new String[0];
        if (withState) {
            storeNames = new String[] {storeName};
            final StoreBuilder<KeyValueStore<Long, Long>> storeBuilder = Stores
                .keyValueStoreBuilder(Stores.persistentKeyValueStore(storeName), Serdes.Long(), Serdes.Long())
                .withCachingEnabled();

            builder.addStateStore(storeBuilder);
        }

        final KStream<Long, Long> input = builder.stream(MULTI_PARTITION_INPUT_TOPIC);
        input.process(() -> new Processor<Long, Long, Long, Long>() {
                ProcessorContext<Long, Long> context;
                KeyValueStore<Long, Long> state = null;

                @Override
                public void init(final ProcessorContext<Long, Long> context) {
                    this.context = context;

                    if (withState) {
                        state = context.getStateStore(storeName);
                    }
                }

                @Override
                public void process(final Record<Long, Long> record) {
                    if (stallInjected.compareAndSet(true, false)) {
                        LOG.info(dummyHostName + " is executing the injected stall");
                        stallingHost.set(dummyHostName);
                        while (doStall) {
                            final Thread thread = Thread.currentThread();
                            if (thread.isInterrupted()) {
                                throw new RuntimeException("Detected we've been interrupted.");
                            }
                            if (!processingThreadsEnabled) {
                                if (!((StreamThread) thread).isRunning()) {
                                    throw new RuntimeException("Detected we've been interrupted.");
                                }
                            }
                            try {
                                Thread.sleep(100);
                            } catch (final InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }

                    final long key = record.key();
                    final long value = record.value();

                    if ((value + 1) % 10 == 0) {
                        context.commit();
                        commitRequested.incrementAndGet();
                    }

                    if (state != null) {
                        Long sum = state.get(key);

                        if (sum == null) {
                            sum = value;
                        } else {
                            sum += value;
                        }
                        state.put(key, sum);
                        state.flush();
                    }


                    if (errorInjected.compareAndSet(true, false)) {
                        // only tries to fail once on one of the task
                        throw new RuntimeException("Injected test exception.");
                    }

                    if (state != null) {
                        context.forward(record.withValue(state.get(key)));
                    } else {
                        context.forward(record);
                    }
                }
            }, storeNames)
            .to(SINGLE_PARTITION_OUTPUT_TOPIC);

        stateTmpDir = TestUtils.tempDirectory().getPath() + File.separator;

        final Properties properties = new Properties();
        // Set commit interval to a larger value to avoid affection of controlled stream commit,
        // but not too large as we need to have a relatively low transaction timeout such
        // that it should help trigger the timed out transaction in time.
        final long commitIntervalMs = 20_000L;
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numberOfStreamsThreads);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalMs);
        properties.put(StreamsConfig.producerPrefix(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG), (int) commitIntervalMs);
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), "1000");
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), MAX_POLL_INTERVAL_MS - 1);
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), MAX_POLL_INTERVAL_MS);
        properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        properties.put(StreamsConfig.STATE_DIR_CONFIG, stateTmpDir + appDir);
        properties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, dummyHostName + ":2142");
        properties.put(InternalConfig.STATE_UPDATER_ENABLED, processingThreadsEnabled);
        properties.put(InternalConfig.PROCESSING_THREADS_ENABLED, processingThreadsEnabled);

        final Properties config = StreamsTestUtils.getStreamsConfig(
            applicationId,
            CLUSTER.bootstrapServers(),
            Serdes.LongSerde.class.getName(),
            Serdes.LongSerde.class.getName(),
            properties);

        final KafkaStreams streams = new KafkaStreams(builder.build(), config);

        streams.setUncaughtExceptionHandler(e -> {
            if (uncaughtException != null ||
                !(e instanceof StreamsException) ||
                !e.getCause().getMessage().equals("Injected test exception.")) {
                e.printStackTrace(System.err);
                hasUnexpectedError = true;
            }
            uncaughtException = e;
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        return streams;
    }

    private void writeInputData(final List<KeyValue<Long, Long>> records) {
        IntegrationTestUtils.produceKeyValuesSynchronously(
            MULTI_PARTITION_INPUT_TOPIC,
            records,
            TestUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class, LongSerializer.class),
            CLUSTER.time
        );
    }

    private List<KeyValue<Long, Long>> readResult(final String topic,
                                                  final int numberOfRecords,
                                                  final String groupId) throws Exception {
        return readResult(topic, numberOfRecords, LongDeserializer.class, LongDeserializer.class, groupId);
    }

    private <K, V> List<KeyValue<K, V>> readResult(final String topic,
                                                   final int numberOfRecords,
                                                   final Class<? extends Deserializer<K>> keyDeserializer,
                                                   final Class<? extends Deserializer<V>> valueDeserializer,
                                                   final String groupId) throws Exception {
        if (groupId != null) {
            return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                TestUtils.consumerConfig(
                    CLUSTER.bootstrapServers(),
                    groupId,
                    keyDeserializer,
                    valueDeserializer,
                    Utils.mkProperties(Collections.singletonMap(
                        ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                        IsolationLevel.READ_COMMITTED.toString()))),
                topic,
                numberOfRecords
            );
        }

        // read uncommitted
        return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            TestUtils.consumerConfig(CLUSTER.bootstrapServers(), keyDeserializer, valueDeserializer),
            topic,
            numberOfRecords
        );
    }

    private <K, V> void ensureCommittedRecordsInTopicPartition(final String topic,
                                                               final int partition,
                                                               final int numberOfRecords,
                                                               final Class<? extends Deserializer<K>> keyDeserializer,
                                                               final Class<? extends Deserializer<V>> valueDeserializer) throws Exception {
        final long timeoutMs = 2 * DEFAULT_TIMEOUT;
        final int maxTries = 10;
        final long deadline = System.currentTimeMillis() + timeoutMs;
        int tries = 0;
        while (true) {
            final List<ConsumerRecord<K, V>> consumerRecords = waitUntilMinRecordsReceived(
                TestUtils.consumerConfig(
                    CLUSTER.bootstrapServers(),
                    CONSUMER_GROUP_ID,
                    keyDeserializer,
                    valueDeserializer,
                    Utils.mkProperties(Collections.singletonMap(
                        ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                        IsolationLevel.READ_COMMITTED.toString())
                    )
                ),
                topic,
                numberOfRecords,
                timeoutMs
            );
            ++tries;
            if (consumerRecords.stream().anyMatch(record -> record.partition() == partition)) {
                return;
            }
            if (tries >= maxTries) {
                throw new AssertionError("No committed records in topic " + topic
                    + ", partition " + partition + " after " + maxTries + " retries.");
            }
            final long now = System.currentTimeMillis();
            if (now > deadline) {
                throw new AssertionError("No committed records in topic " + topic
                    + ", partition " + partition + " after " + timeoutMs + " ms.");
            }
        }
    }

    private List<KeyValue<Long, Long>> computeExpectedResult(final List<KeyValue<Long, Long>> input) {
        final List<KeyValue<Long, Long>> expectedResult = new ArrayList<>(input.size());

        final HashMap<Long, Long> sums = new HashMap<>();

        for (final KeyValue<Long, Long> record : input) {
            Long sum = sums.get(record.key);
            if (sum == null) {
                sum = record.value;
            } else {
                sum += record.value;
            }
            sums.put(record.key, sum);
            expectedResult.add(new KeyValue<>(record.key, sum));
        }

        return expectedResult;
    }

    private Set<KeyValue<Long, Long>> getMaxPerKey(final List<KeyValue<Long, Long>> input) {
        final Set<KeyValue<Long, Long>> expectedResult = new HashSet<>(input.size());

        final HashMap<Long, Long> maxPerKey = new HashMap<>();

        for (final KeyValue<Long, Long> record : input) {
            final Long max = maxPerKey.get(record.key);
            if (max == null || record.value > max) {
                maxPerKey.put(record.key, record.value);
            }

        }

        for (final Map.Entry<Long, Long> max : maxPerKey.entrySet()) {
            expectedResult.add(new KeyValue<>(max.getKey(), max.getValue()));
        }

        return expectedResult;
    }

    private void verifyStateStore(final KafkaStreams streams,
                                  final Set<KeyValue<Long, Long>> expectedStoreContent,
                                  final String reason) {
        final StateQueryRequest<KeyValueIterator<Long, Long>> request =
                inStore(storeName).withQuery(RangeQuery.withNoBounds());

        final StateQueryResult<KeyValueIterator<Long, Long>> result =
                IntegrationTestUtils.iqv2WaitForResult(streams, request);

        for (final QueryResult<KeyValueIterator<Long, Long>> partitionResult: result.getPartitionResults().values()) {
            try (final KeyValueIterator<Long, Long> it = partitionResult.getResult()) {
                while (it.hasNext()) {
                    assertTrue(expectedStoreContent.remove(it.next()), reason);
                }
            }
        }

        assertTrue(expectedStoreContent.isEmpty(), reason);
    }
}
