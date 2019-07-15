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
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({IntegrationTest.class})
public class EosIntegrationTest {
    private static final int NUM_BROKERS = 3;
    private static final int MAX_POLL_INTERVAL_MS = 5 * 1000;
    private static final int MAX_WAIT_TIME_MS = 60 * 1000;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(
        NUM_BROKERS,
        Utils.mkProperties(Collections.singletonMap("auto.create.topics.enable", "false"))
    );

    private static String applicationId;
    private final static int NUM_TOPIC_PARTITIONS = 2;
    private final static String CONSUMER_GROUP_ID = "readCommitted";
    private final static String SINGLE_PARTITION_INPUT_TOPIC = "singlePartitionInputTopic";
    private final static String SINGLE_PARTITION_THROUGH_TOPIC = "singlePartitionThroughTopic";
    private final static String SINGLE_PARTITION_OUTPUT_TOPIC = "singlePartitionOutputTopic";
    private final static String MULTI_PARTITION_INPUT_TOPIC = "multiPartitionInputTopic";
    private final static String MULTI_PARTITION_THROUGH_TOPIC = "multiPartitionThroughTopic";
    private final static String MULTI_PARTITION_OUTPUT_TOPIC = "multiPartitionOutputTopic";
    private final String storeName = "store";

    private AtomicBoolean errorInjected;
    private AtomicBoolean gcInjected;
    private volatile boolean doGC = true;
    private AtomicInteger commitRequested;
    private Throwable uncaughtException;

    private int testNumber = 0;

    @Before
    public void createTopics() throws Exception {
        applicationId = "appId-" + ++testNumber;
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
        runSimpleCopyTest(1, SINGLE_PARTITION_INPUT_TOPIC, null, SINGLE_PARTITION_OUTPUT_TOPIC);
    }

    @Test
    public void shouldBeAbleToRestartAfterClose() throws Exception {
        runSimpleCopyTest(2, SINGLE_PARTITION_INPUT_TOPIC, null, SINGLE_PARTITION_OUTPUT_TOPIC);
    }

    @Test
    public void shouldBeAbleToCommitToMultiplePartitions() throws Exception {
        runSimpleCopyTest(1, SINGLE_PARTITION_INPUT_TOPIC, null, MULTI_PARTITION_OUTPUT_TOPIC);
    }

    @Test
    public void shouldBeAbleToCommitMultiplePartitionOffsets() throws Exception {
        runSimpleCopyTest(1, MULTI_PARTITION_INPUT_TOPIC, null, SINGLE_PARTITION_OUTPUT_TOPIC);
    }

    @Test
    public void shouldBeAbleToRunWithTwoSubtopologies() throws Exception {
        runSimpleCopyTest(1, SINGLE_PARTITION_INPUT_TOPIC, SINGLE_PARTITION_THROUGH_TOPIC, SINGLE_PARTITION_OUTPUT_TOPIC);
    }

    @Test
    public void shouldBeAbleToRunWithTwoSubtopologiesAndMultiplePartitions() throws Exception {
        runSimpleCopyTest(1, MULTI_PARTITION_INPUT_TOPIC, MULTI_PARTITION_THROUGH_TOPIC, MULTI_PARTITION_OUTPUT_TOPIC);
    }

    private void runSimpleCopyTest(final int numberOfRestarts,
                                   final String inputTopic,
                                   final String throughTopic,
                                   final String outputTopic) throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Long, Long> input = builder.stream(inputTopic);
        KStream<Long, Long> output = input;
        if (throughTopic != null) {
            output = input.through(throughTopic);
        }
        output.to(outputTopic);

        final Properties properties = new Properties();
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), 1);
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), "1000");
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

        for (int i = 0; i < numberOfRestarts; ++i) {
            final Properties config = StreamsTestUtils.getStreamsConfig(
                applicationId,
                CLUSTER.bootstrapServers(),
                Serdes.LongSerde.class.getName(),
                Serdes.LongSerde.class.getName(),
                properties);

            try (final KafkaStreams streams = new KafkaStreams(builder.build(), config)) {
                streams.start();

                final List<KeyValue<Long, Long>> inputData = prepareData(i * 100, i * 100 + 10L, 0L, 1L);

                IntegrationTestUtils.produceKeyValuesSynchronously(
                    inputTopic,
                    inputData,
                    TestUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class, LongSerializer.class),
                    CLUSTER.time
                );

                final List<KeyValue<Long, Long>> committedRecords =
                    IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                        TestUtils.consumerConfig(
                            CLUSTER.bootstrapServers(),
                            CONSUMER_GROUP_ID,
                            LongDeserializer.class,
                            LongDeserializer.class,
                            Utils.mkProperties(Collections.singletonMap(
                                ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                                IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT)))
                            ),
                        outputTopic,
                        inputData.size()
                    );

                checkResultPerKey(committedRecords, inputData);
            }
        }
    }

    private void checkResultPerKey(final List<KeyValue<Long, Long>> result,
                                   final List<KeyValue<Long, Long>> expectedResult) {
        final Set<Long> allKeys = new HashSet<>();
        addAllKeys(allKeys, result);
        addAllKeys(allKeys, expectedResult);

        for (final Long key : allKeys) {
            assertThat(getAllRecordPerKey(key, result), equalTo(getAllRecordPerKey(key, expectedResult)));
        }

    }

    private void addAllKeys(final Set<Long> allKeys,
                            final List<KeyValue<Long, Long>> records) {
        for (final KeyValue<Long, Long> record : records) {
            allKeys.add(record.key);
        }
    }

    private List<KeyValue<Long, Long>> getAllRecordPerKey(final Long key,
                                                          final List<KeyValue<Long, Long>> records) {
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
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        properties.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Properties config = StreamsTestUtils.getStreamsConfig(
            applicationId,
            CLUSTER.bootstrapServers(),
            Serdes.LongSerde.class.getName(),
            Serdes.LongSerde.class.getName(),
            properties);

        try (final KafkaStreams streams = new KafkaStreams(builder.build(), config)) {
            streams.start();

            final List<KeyValue<Long, Long>> firstBurstOfData = prepareData(0L, 5L, 0L);
            final List<KeyValue<Long, Long>> secondBurstOfData = prepareData(5L, 8L, 0L);

            IntegrationTestUtils.produceKeyValuesSynchronously(
                SINGLE_PARTITION_INPUT_TOPIC,
                firstBurstOfData,
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class, LongSerializer.class),
                CLUSTER.time
            );

            final List<KeyValue<Long, Long>> firstCommittedRecords =
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                    TestUtils.consumerConfig(
                        CLUSTER.bootstrapServers(),
                        CONSUMER_GROUP_ID,
                        LongDeserializer.class,
                        LongDeserializer.class,
                        Utils.mkProperties(Collections.singletonMap(
                            ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                            IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT)))
                        ),
                    SINGLE_PARTITION_OUTPUT_TOPIC,
                    firstBurstOfData.size()
                );

            assertThat(firstCommittedRecords, equalTo(firstBurstOfData));

            IntegrationTestUtils.produceKeyValuesSynchronously(
                SINGLE_PARTITION_INPUT_TOPIC,
                secondBurstOfData,
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class, LongSerializer.class),
                CLUSTER.time
            );

            final List<KeyValue<Long, Long>> secondCommittedRecords =
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                    TestUtils.consumerConfig(
                        CLUSTER.bootstrapServers(),
                        CONSUMER_GROUP_ID,
                        LongDeserializer.class,
                        LongDeserializer.class,
                        Utils.mkProperties(Collections.singletonMap(
                            ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                            IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT)))
                        ),
                    SINGLE_PARTITION_OUTPUT_TOPIC,
                    secondBurstOfData.size()
                );

            assertThat(secondCommittedRecords, equalTo(secondBurstOfData));
        }
    }

    @Test
    public void shouldNotViolateEosIfOneTaskFails() throws Exception {
        // this test writes 10 + 5 + 5 records per partition (running with 2 partitions)
        // the app is supposed to copy all 40 records into the output topic
        // the app commits after each 10 records per partition, and thus will have 2*5 uncommitted writes
        //
        // the failure gets inject after 20 committed and 30 uncommitted records got received
        // -> the failure only kills one thread
        // after fail over, we should read 40 committed records (even if 50 record got written)

        try (final KafkaStreams streams = getKafkaStreams(false, "appDir", 2)) {
            streams.start();

            final List<KeyValue<Long, Long>> committedDataBeforeFailure = prepareData(0L, 10L, 0L, 1L);
            final List<KeyValue<Long, Long>> uncommittedDataBeforeFailure = prepareData(10L, 15L, 0L, 1L);

            final List<KeyValue<Long, Long>> dataBeforeFailure = new ArrayList<>();
            dataBeforeFailure.addAll(committedDataBeforeFailure);
            dataBeforeFailure.addAll(uncommittedDataBeforeFailure);

            final List<KeyValue<Long, Long>> dataAfterFailure = prepareData(15L, 20L, 0L, 1L);

            writeInputData(committedDataBeforeFailure);

            TestUtils.waitForCondition(
                () -> commitRequested.get() == 2, MAX_WAIT_TIME_MS,
                "SteamsTasks did not request commit.");

            writeInputData(uncommittedDataBeforeFailure);

            final List<KeyValue<Long, Long>> uncommittedRecords = readResult(dataBeforeFailure.size(), null);
            final List<KeyValue<Long, Long>> committedRecords = readResult(committedDataBeforeFailure.size(), CONSUMER_GROUP_ID);

            checkResultPerKey(committedRecords, committedDataBeforeFailure);
            checkResultPerKey(uncommittedRecords, dataBeforeFailure);

            errorInjected.set(true);
            writeInputData(dataAfterFailure);

            TestUtils.waitForCondition(
                () -> uncaughtException != null, MAX_WAIT_TIME_MS,
                "Should receive uncaught exception from one StreamThread.");

            final List<KeyValue<Long, Long>> allCommittedRecords = readResult(
                committedDataBeforeFailure.size() + uncommittedDataBeforeFailure.size() + dataAfterFailure.size(),
                CONSUMER_GROUP_ID + "_ALL");

            final List<KeyValue<Long, Long>> committedRecordsAfterFailure = readResult(
                uncommittedDataBeforeFailure.size() + dataAfterFailure.size(),
                CONSUMER_GROUP_ID);

            final List<KeyValue<Long, Long>> allExpectedCommittedRecordsAfterRecovery = new ArrayList<>();
            allExpectedCommittedRecordsAfterRecovery.addAll(committedDataBeforeFailure);
            allExpectedCommittedRecordsAfterRecovery.addAll(uncommittedDataBeforeFailure);
            allExpectedCommittedRecordsAfterRecovery.addAll(dataAfterFailure);

            final List<KeyValue<Long, Long>> expectedCommittedRecordsAfterRecovery = new ArrayList<>();
            expectedCommittedRecordsAfterRecovery.addAll(uncommittedDataBeforeFailure);
            expectedCommittedRecordsAfterRecovery.addAll(dataAfterFailure);

            checkResultPerKey(allCommittedRecords, allExpectedCommittedRecordsAfterRecovery);
            checkResultPerKey(committedRecordsAfterFailure, expectedCommittedRecordsAfterRecovery);
        }
    }

    @Test
    public void shouldNotViolateEosIfOneTaskFailsWithState() throws Exception {
        // this test updates a store with 10 + 5 + 5 records per partition (running with 2 partitions)
        // the app is supposed to emit all 40 update records into the output topic
        // the app commits after each 10 records per partition, and thus will have 2*5 uncommitted writes
        // and store updates (ie, another 5 uncommitted writes to a changelog topic per partition)
        // in the uncommitted batch sending some data for the new key to validate that upon resuming they will not be shown up in the store
        //
        // the failure gets inject after 20 committed and 10 uncommitted records got received
        // -> the failure only kills one thread
        // after fail over, we should read 40 committed records and the state stores should contain the correct sums
        // per key (even if some records got processed twice)

        try (final KafkaStreams streams = getKafkaStreams(true, "appDir", 2)) {
            streams.start();

            final List<KeyValue<Long, Long>> committedDataBeforeFailure = prepareData(0L, 10L, 0L, 1L);
            final List<KeyValue<Long, Long>> uncommittedDataBeforeFailure = prepareData(10L, 15L, 0L, 1L, 2L, 3L);

            final List<KeyValue<Long, Long>> dataBeforeFailure = new ArrayList<>();
            dataBeforeFailure.addAll(committedDataBeforeFailure);
            dataBeforeFailure.addAll(uncommittedDataBeforeFailure);

            final List<KeyValue<Long, Long>> dataAfterFailure = prepareData(15L, 20L, 0L, 1L);

            writeInputData(committedDataBeforeFailure);

            TestUtils.waitForCondition(
                () -> commitRequested.get() == 2, MAX_WAIT_TIME_MS,
                "SteamsTasks did not request commit.");

            writeInputData(uncommittedDataBeforeFailure);

            final List<KeyValue<Long, Long>> uncommittedRecords = readResult(dataBeforeFailure.size(), null);
            final List<KeyValue<Long, Long>> committedRecords = readResult(committedDataBeforeFailure.size(), CONSUMER_GROUP_ID);

            final List<KeyValue<Long, Long>> expectedResultBeforeFailure = computeExpectedResult(dataBeforeFailure);
            checkResultPerKey(committedRecords, computeExpectedResult(committedDataBeforeFailure));
            checkResultPerKey(uncommittedRecords, expectedResultBeforeFailure);
            verifyStateStore(streams, getMaxPerKey(expectedResultBeforeFailure));

            errorInjected.set(true);
            writeInputData(dataAfterFailure);

            TestUtils.waitForCondition(
                () -> uncaughtException != null, MAX_WAIT_TIME_MS,
                "Should receive uncaught exception from one StreamThread.");

            final List<KeyValue<Long, Long>> allCommittedRecords = readResult(
                committedDataBeforeFailure.size() + uncommittedDataBeforeFailure.size() + dataAfterFailure.size(),
                CONSUMER_GROUP_ID + "_ALL");

            final List<KeyValue<Long, Long>> committedRecordsAfterFailure = readResult(
                uncommittedDataBeforeFailure.size() + dataAfterFailure.size(),
                CONSUMER_GROUP_ID);

            final List<KeyValue<Long, Long>> allExpectedCommittedRecordsAfterRecovery = new ArrayList<>();
            allExpectedCommittedRecordsAfterRecovery.addAll(committedDataBeforeFailure);
            allExpectedCommittedRecordsAfterRecovery.addAll(uncommittedDataBeforeFailure);
            allExpectedCommittedRecordsAfterRecovery.addAll(dataAfterFailure);

            final List<KeyValue<Long, Long>> expectedResult = computeExpectedResult(allExpectedCommittedRecordsAfterRecovery);

            checkResultPerKey(allCommittedRecords, expectedResult);
            checkResultPerKey(
                committedRecordsAfterFailure,
                expectedResult.subList(committedDataBeforeFailure.size(), expectedResult.size()));

            verifyStateStore(streams, getMaxPerKey(expectedResult));
        }
    }

    @Test
    public void shouldNotViolateEosIfOneTaskGetsFencedUsingIsolatedAppInstances() throws Exception {
        // this test writes 10 + 5 + 5 + 10 records per partition (running with 2 partitions)
        // the app is supposed to copy all 60 records into the output topic
        // the app commits after each 10 records per partition, and thus will have 2*5 uncommitted writes
        //
        // a GC pause gets inject after 20 committed and 30 uncommitted records got received
        // -> the GC pause only affects one thread and should trigger a rebalance
        // after rebalancing, we should read 40 committed records (even if 50 record got written)
        //
        // afterwards, the "stalling" thread resumes, and another rebalance should get triggered
        // we write the remaining 20 records and verify to read 60 result records

        try (
            final KafkaStreams streams1 = getKafkaStreams(false, "appDir1", 1);
            final KafkaStreams streams2 = getKafkaStreams(false, "appDir2", 1)
        ) {
            streams1.start();
            streams2.start();

            final List<KeyValue<Long, Long>> committedDataBeforeGC = prepareData(0L, 10L, 0L, 1L);
            final List<KeyValue<Long, Long>> uncommittedDataBeforeGC = prepareData(10L, 15L, 0L, 1L);

            final List<KeyValue<Long, Long>> dataBeforeGC = new ArrayList<>();
            dataBeforeGC.addAll(committedDataBeforeGC);
            dataBeforeGC.addAll(uncommittedDataBeforeGC);

            final List<KeyValue<Long, Long>> dataToTriggerFirstRebalance = prepareData(15L, 20L, 0L, 1L);

            final List<KeyValue<Long, Long>> dataAfterSecondRebalance = prepareData(20L, 30L, 0L, 1L);

            writeInputData(committedDataBeforeGC);

            TestUtils.waitForCondition(
                () -> commitRequested.get() == 2, MAX_WAIT_TIME_MS,
                "SteamsTasks did not request commit.");

            writeInputData(uncommittedDataBeforeGC);

            final List<KeyValue<Long, Long>> uncommittedRecords = readResult(dataBeforeGC.size(), null);
            final List<KeyValue<Long, Long>> committedRecords = readResult(committedDataBeforeGC.size(), CONSUMER_GROUP_ID);

            checkResultPerKey(committedRecords, committedDataBeforeGC);
            checkResultPerKey(uncommittedRecords, dataBeforeGC);

            gcInjected.set(true);
            writeInputData(dataToTriggerFirstRebalance);

            TestUtils.waitForCondition(
                () -> streams1.allMetadata().size() == 1
                    && streams2.allMetadata().size() == 1
                    && (streams1.allMetadata().iterator().next().topicPartitions().size() == 2
                        || streams2.allMetadata().iterator().next().topicPartitions().size() == 2),
                MAX_WAIT_TIME_MS, "Should have rebalanced.");

            final List<KeyValue<Long, Long>> committedRecordsAfterRebalance = readResult(
                uncommittedDataBeforeGC.size() + dataToTriggerFirstRebalance.size(),
                CONSUMER_GROUP_ID);

            final List<KeyValue<Long, Long>> expectedCommittedRecordsAfterRebalance = new ArrayList<>();
            expectedCommittedRecordsAfterRebalance.addAll(uncommittedDataBeforeGC);
            expectedCommittedRecordsAfterRebalance.addAll(dataToTriggerFirstRebalance);

            checkResultPerKey(committedRecordsAfterRebalance, expectedCommittedRecordsAfterRebalance);

            doGC = false;
            TestUtils.waitForCondition(
                () -> streams1.allMetadata().size() == 1
                    && streams2.allMetadata().size() == 1
                    && streams1.allMetadata().iterator().next().topicPartitions().size() == 1
                    && streams2.allMetadata().iterator().next().topicPartitions().size() == 1,
                MAX_WAIT_TIME_MS,
                "Should have rebalanced.");

            writeInputData(dataAfterSecondRebalance);

            final List<KeyValue<Long, Long>> allCommittedRecords = readResult(
                committedDataBeforeGC.size() + uncommittedDataBeforeGC.size()
                + dataToTriggerFirstRebalance.size() + dataAfterSecondRebalance.size(),
                CONSUMER_GROUP_ID + "_ALL");

            final List<KeyValue<Long, Long>> allExpectedCommittedRecordsAfterRecovery = new ArrayList<>();
            allExpectedCommittedRecordsAfterRecovery.addAll(committedDataBeforeGC);
            allExpectedCommittedRecordsAfterRecovery.addAll(uncommittedDataBeforeGC);
            allExpectedCommittedRecordsAfterRecovery.addAll(dataToTriggerFirstRebalance);
            allExpectedCommittedRecordsAfterRecovery.addAll(dataAfterSecondRebalance);

            checkResultPerKey(allCommittedRecords, allExpectedCommittedRecordsAfterRecovery);
        }
    }

    private List<KeyValue<Long, Long>> prepareData(final long fromInclusive,
                                                   final long toExclusive,
                                                   final Long... keys) {
        final List<KeyValue<Long, Long>> data = new ArrayList<>();

        for (final Long k : keys) {
            for (long v = fromInclusive; v < toExclusive; ++v) {
                data.add(new KeyValue<>(k, v));
            }
        }

        return data;
    }

    private KafkaStreams getKafkaStreams(final boolean withState,
                                         final String appDir,
                                         final int numberOfStreamsThreads) {
        commitRequested = new AtomicInteger(0);
        errorInjected = new AtomicBoolean(false);
        gcInjected = new AtomicBoolean(false);
        final StreamsBuilder builder = new StreamsBuilder();

        String[] storeNames = null;
        if (withState) {
            storeNames = new String[] {storeName};
            final StoreBuilder<KeyValueStore<Long, Long>> storeBuilder = Stores
                .keyValueStoreBuilder(Stores.persistentKeyValueStore(storeName), Serdes.Long(), Serdes.Long())
                .withCachingEnabled();

            builder.addStateStore(storeBuilder);
        }

        final KStream<Long, Long> input = builder.stream(MULTI_PARTITION_INPUT_TOPIC);
        input.transform(new TransformerSupplier<Long, Long, KeyValue<Long, Long>>() {
            @SuppressWarnings("unchecked")
            @Override
            public Transformer<Long, Long, KeyValue<Long, Long>> get() {
                return new Transformer<Long, Long, KeyValue<Long, Long>>() {
                    ProcessorContext context;
                    KeyValueStore<Long, Long> state = null;

                    @Override
                    public void init(final ProcessorContext context) {
                        this.context = context;

                        if (withState) {
                            state = (KeyValueStore<Long, Long>) context.getStateStore(storeName);
                        }
                    }

                    @Override
                    public KeyValue<Long, Long> transform(final Long key,
                                                          final Long value) {
                        if (gcInjected.compareAndSet(true, false)) {
                            while (doGC) {
                                try {
                                    Thread.sleep(100);
                                } catch (final InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }

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
                            return new KeyValue<>(key, state.get(key));
                        } else {
                            return new KeyValue<>(key, value);
                        }
                    }

                    @Override
                    public void close() { }
                };
            } }, storeNames)
            .to(SINGLE_PARTITION_OUTPUT_TOPIC);

        final Properties properties = new Properties();
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numberOfStreamsThreads);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Long.MAX_VALUE);
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), "1000");
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), 5 * 1000);
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 5 * 1000 - 1);
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), MAX_POLL_INTERVAL_MS);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        properties.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath() + File.separator + appDir);
        properties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "dummy:2142");

        final Properties config = StreamsTestUtils.getStreamsConfig(
            applicationId,
            CLUSTER.bootstrapServers(),
            Serdes.LongSerde.class.getName(),
            Serdes.LongSerde.class.getName(),
            properties);

        final KafkaStreams streams = new KafkaStreams(builder.build(), config);

        streams.setUncaughtExceptionHandler((t, e) -> {
            if (uncaughtException != null) {
                e.printStackTrace(System.err);
                fail("Should only get one uncaught exception from Streams.");
            }
            uncaughtException = e;
        });

        return streams;
    }

    private void writeInputData(final List<KeyValue<Long, Long>> records) throws Exception {
        IntegrationTestUtils.produceKeyValuesSynchronously(
            MULTI_PARTITION_INPUT_TOPIC,
            records,
            TestUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class, LongSerializer.class),
            CLUSTER.time
        );
    }

    private List<KeyValue<Long, Long>> readResult(final int numberOfRecords,
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
                SINGLE_PARTITION_OUTPUT_TOPIC,
                numberOfRecords
            );
        }

        // read uncommitted
        return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            TestUtils.consumerConfig(CLUSTER.bootstrapServers(), LongDeserializer.class, LongDeserializer.class),
            SINGLE_PARTITION_OUTPUT_TOPIC,
            numberOfRecords
        );
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
                                  final Set<KeyValue<Long, Long>> expectedStoreContent) {
        ReadOnlyKeyValueStore<Long, Long> store = null;

        final long maxWaitingTime = System.currentTimeMillis() + 300000L;
        while (System.currentTimeMillis() < maxWaitingTime) {
            try {
                store = streams.store(storeName, QueryableStoreTypes.keyValueStore());
                break;
            } catch (final InvalidStateStoreException okJustRetry) {
                try {
                    Thread.sleep(5000L);
                } catch (final Exception ignore) { }
            }
        }

        assertNotNull(store);

        final KeyValueIterator<Long, Long> it = store.all();
        while (it.hasNext()) {
            assertTrue(expectedStoreContent.remove(it.next()));
        }

        assertTrue(expectedStoreContent.isEmpty());
    }

}
