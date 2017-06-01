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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({IntegrationTest.class})
public class EosIntegrationTest {
    private static final int NUM_BROKERS = 3;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private static String applicationId;
    private final static String CONSUMER_GROUP_ID = "readCommitted";
    private final static String SINGLE_PARTITION_INPUT_TOPIC = "singlePartitionInputTopic";
    private final static String MULTI_PARTITION_INPUT_TOPIC = "inputTopic";
    private final static int NUM_TOPIC_PARTITIONS = 2;
    private final static String SINGLE_PARTITION_OUTPUT_TOPIC = "outputTopic";
    private final static String MULTI_PARTITION_OUTPUT_TOPIC = "multiPartitionOutputTopic";
    private final String storeName = "store";

    private final Map<Integer, Integer> maxPartitionNumberSeen = Collections.synchronizedMap(new HashMap<Integer, Integer>());
    private boolean injectError = false;
    private AtomicInteger commitRequested;
    private Throwable uncaughtException;

    private int testNumber = 0;

    @Before
    public void createTopics() throws Exception {
        applicationId = "appId-" + ++testNumber;
        CLUSTER.deleteTopicsAndWait(SINGLE_PARTITION_INPUT_TOPIC, MULTI_PARTITION_INPUT_TOPIC, SINGLE_PARTITION_OUTPUT_TOPIC, MULTI_PARTITION_OUTPUT_TOPIC);

        CLUSTER.createTopics(SINGLE_PARTITION_INPUT_TOPIC, SINGLE_PARTITION_OUTPUT_TOPIC);
        CLUSTER.createTopic(MULTI_PARTITION_INPUT_TOPIC, NUM_TOPIC_PARTITIONS, 1);
    }

    @Test
    public void shouldBeAbleToRunWithEosEnabled() throws Exception {
        runSimpleCopyTest(1, SINGLE_PARTITION_INPUT_TOPIC, SINGLE_PARTITION_OUTPUT_TOPIC);
    }

    @Test
    public void shouldBeAbleToRestartAfterClose() throws Exception {
        runSimpleCopyTest(2, SINGLE_PARTITION_INPUT_TOPIC, SINGLE_PARTITION_OUTPUT_TOPIC);
    }

    @Test
    public void shouldBeAbleToCommitToMultiplePartitions() throws Exception {
        runSimpleCopyTest(1, SINGLE_PARTITION_INPUT_TOPIC, MULTI_PARTITION_OUTPUT_TOPIC);
    }

    @Test
    public void shouldBeAbleToCommitMultiplePartitionOffsets() throws Exception {
        runSimpleCopyTest(1, MULTI_PARTITION_INPUT_TOPIC, SINGLE_PARTITION_OUTPUT_TOPIC);
    }

    private void runSimpleCopyTest(final int numberOfRestarts,
                                   final String inputTopic,
                                   final String outputTopic) throws Exception {
        final KStreamBuilder builder = new KStreamBuilder();
        final KStream<Long, Long> input = builder.stream(inputTopic);
        input
            .mapValues(new ValueMapper<Long, Long>() {
                @Override
                public Long apply(final Long value) {
                    return value;
                }
            })
            .to(outputTopic);

        for (int i = 0; i < numberOfRestarts; ++i) {
            final long factor = i;
            final KafkaStreams streams = new KafkaStreams(
                builder,
                StreamsTestUtils.getStreamsConfig(
                    applicationId,
                    CLUSTER.bootstrapServers(),
                    Serdes.LongSerde.class.getName(),
                    Serdes.LongSerde.class.getName(),
                    new Properties() {
                        {
                            put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
                        }
                    }));

            try {
                streams.start();

                final List<KeyValue<Long, Long>> inputData = new ArrayList<KeyValue<Long, Long>>() {
                    {
                        add(new KeyValue<>(0L, factor * 100));
                        add(new KeyValue<>(0L, factor * 100 + 1L));
                        add(new KeyValue<>(0L, factor * 100 + 2L));
                        add(new KeyValue<>(1L, factor * 100));
                        add(new KeyValue<>(1L, factor * 100 + 1L));
                        add(new KeyValue<>(1L, factor * 100 + 2L));
                    }
                };

                IntegrationTestUtils.produceKeyValuesSynchronously(
                    inputTopic,
                    inputData,
                    TestUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class, LongSerializer.class),
                    CLUSTER.time
                );

                final List<KeyValue<Long, Long>> committedRecords
                    = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                    TestUtils.consumerConfig(
                        CLUSTER.bootstrapServers(),
                        CONSUMER_GROUP_ID,
                        LongDeserializer.class,
                        LongDeserializer.class,
                        new Properties() {
                            {
                                put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT));
                            }
                        }),
                    inputTopic,
                    inputData.size()
                );

                checkResultPerKey(committedRecords, inputData);
            } finally {
                streams.close();
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
        final KStreamBuilder builder = new KStreamBuilder();
        builder.stream(SINGLE_PARTITION_INPUT_TOPIC).to(SINGLE_PARTITION_OUTPUT_TOPIC);

        final KafkaStreams streams = new KafkaStreams(
            builder,
            StreamsTestUtils.getStreamsConfig(
                applicationId,
                CLUSTER.bootstrapServers(),
                Serdes.LongSerde.class.getName(),
                Serdes.LongSerde.class.getName(),
                new Properties() {
                    {
                        put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
                    }
                }));

        try {
            streams.start();

            final List<KeyValue<Long, Long>> firstBurstOfData = new ArrayList<KeyValue<Long, Long>>() {
                {
                    add(new KeyValue<>(0L, 0L));
                    add(new KeyValue<>(0L, 1L));
                    add(new KeyValue<>(0L, 2L));
                    add(new KeyValue<>(0L, 3L));
                    add(new KeyValue<>(0L, 4L));
                }
            };
            final List<KeyValue<Long, Long>> secondBurstOfData = new ArrayList<KeyValue<Long, Long>>() {
                {
                    add(new KeyValue<>(0L, 5L));
                    add(new KeyValue<>(0L, 6L));
                    add(new KeyValue<>(0L, 7L));
                }
            };

            IntegrationTestUtils.produceKeyValuesSynchronously(
                SINGLE_PARTITION_INPUT_TOPIC,
                firstBurstOfData,
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class, LongSerializer.class),
                CLUSTER.time
            );

            final List<KeyValue<Long, Long>> firstCommittedRecords
                = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                TestUtils.consumerConfig(
                    CLUSTER.bootstrapServers(),
                    CONSUMER_GROUP_ID,
                    LongDeserializer.class,
                    LongDeserializer.class,
                    new Properties() {
                        {
                            put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT));
                        }
                    }),
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

            final List<KeyValue<Long, Long>> secondCommittedRecords
                = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                TestUtils.consumerConfig(
                    CLUSTER.bootstrapServers(),
                    CONSUMER_GROUP_ID,
                    LongDeserializer.class,
                    LongDeserializer.class,
                    new Properties() {
                        {
                            put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT));
                        }
                    }),
                SINGLE_PARTITION_OUTPUT_TOPIC,
                secondBurstOfData.size()
            );

            assertThat(secondCommittedRecords, equalTo(secondBurstOfData));
        } finally {
            streams.close();
        }
    }

    @Ignore
    @Test
    public void shouldNotViolateEosIfOneTaskFails() throws Exception {
        // this test writes 10 + 5 + 5 records per partition (running with 2 parttions)
        // the app is supposed to copy all 40 records into the output topic
        // the app commits after each 10 records per partition, and thus will have 2*5 uncommitted writes
        //
        // the failure gets inject after 20 committed and 30 uncommitted records got received
        // -> the failure only kills one thread
        // after fail over, we should read 40 committed records (even if 50 record got written)

        final KafkaStreams streams = getKafkaStreams(false);
        try {
            streams.start();

            final List<KeyValue<Long, Long>> committedDataBeforeFailure = new ArrayList<KeyValue<Long, Long>>() {
                {
                    add(new KeyValue<>(0L, 0L));
                    add(new KeyValue<>(0L, 1L));
                    add(new KeyValue<>(0L, 2L));
                    add(new KeyValue<>(0L, 3L));
                    add(new KeyValue<>(0L, 4L));
                    add(new KeyValue<>(0L, 5L));
                    add(new KeyValue<>(0L, 6L));
                    add(new KeyValue<>(0L, 7L));
                    add(new KeyValue<>(0L, 8L));
                    add(new KeyValue<>(0L, 9L));
                    add(new KeyValue<>(1L, 0L));
                    add(new KeyValue<>(1L, 1L));
                    add(new KeyValue<>(1L, 2L));
                    add(new KeyValue<>(1L, 3L));
                    add(new KeyValue<>(1L, 4L));
                    add(new KeyValue<>(1L, 5L));
                    add(new KeyValue<>(1L, 6L));
                    add(new KeyValue<>(1L, 7L));
                    add(new KeyValue<>(1L, 8L));
                    add(new KeyValue<>(1L, 9L));
                }
            };
            final List<KeyValue<Long, Long>> uncommittedDataBeforeFailure = new ArrayList<KeyValue<Long, Long>>() {
                {
                    add(new KeyValue<>(0L, 10L));
                    add(new KeyValue<>(0L, 11L));
                    add(new KeyValue<>(0L, 12L));
                    add(new KeyValue<>(0L, 13L));
                    add(new KeyValue<>(0L, 14L));
                    add(new KeyValue<>(1L, 10L));
                    add(new KeyValue<>(1L, 11L));
                    add(new KeyValue<>(1L, 12L));
                    add(new KeyValue<>(1L, 13L));
                    add(new KeyValue<>(1L, 14L));
                }
            };

            final List<KeyValue<Long, Long>> dataBeforeFailure = new ArrayList<>();
            dataBeforeFailure.addAll(committedDataBeforeFailure);
            dataBeforeFailure.addAll(uncommittedDataBeforeFailure);

            final List<KeyValue<Long, Long>> dataAfterFailure = new ArrayList<KeyValue<Long, Long>>() {
                {
                    add(new KeyValue<>(0L, 15L));
                    add(new KeyValue<>(0L, 16L));
                    add(new KeyValue<>(0L, 17L));
                    add(new KeyValue<>(0L, 18L));
                    add(new KeyValue<>(0L, 19L));
                    add(new KeyValue<>(1L, 15L));
                    add(new KeyValue<>(1L, 16L));
                    add(new KeyValue<>(1L, 17L));
                    add(new KeyValue<>(1L, 18L));
                    add(new KeyValue<>(1L, 19L));
                }
            };

            writeInputData(committedDataBeforeFailure);

            TestUtils.waitForCondition(new TestCondition() {
                @Override
                public boolean conditionMet() {
                    return commitRequested.get() == 2;
                }
            }, 60000, "SteamsTasks did not request commit.");

            writeInputData(uncommittedDataBeforeFailure);

            final List<KeyValue<Long, Long>> uncommittedRecords = readResult(dataBeforeFailure.size(), null);
            final List<KeyValue<Long, Long>> committedRecords = readResult(committedDataBeforeFailure.size(), CONSUMER_GROUP_ID);

            checkResultPerKey(committedRecords, committedDataBeforeFailure);
            checkResultPerKey(uncommittedRecords, dataBeforeFailure);

            injectError = true;
            writeInputData(dataAfterFailure);

            TestUtils.waitForCondition(new TestCondition() {
                @Override
                public boolean conditionMet() {
                    return uncaughtException != null;
                }
            }, 60000, "Should receive uncaught exception from one StreamThread.");

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
        } finally {
            streams.close();
        }
    }

    private KafkaStreams getKafkaStreams(final boolean withState) {
        commitRequested = new AtomicInteger(0);
        final KStreamBuilder builder = new KStreamBuilder();

        String[] storeNames = null;
        if (withState) {
            storeNames = new String[] {storeName};
            final StateStoreSupplier storeSupplier = Stores.create(storeName)
                .withLongKeys()
                .withLongValues()
                .persistent()
                .build();

            builder.addStateStore(storeSupplier);
        }
        final KStream<Long, Long> input = builder.stream(MULTI_PARTITION_INPUT_TOPIC);
        input.transform(new TransformerSupplier<Long, Long, KeyValue<Long, Long>>() {
            @SuppressWarnings("unchecked")
            @Override
            public Transformer<Long, Long, KeyValue<Long, Long>> get() {
                return new Transformer<Long, Long, KeyValue<Long, Long>>() {
                    ProcessorContext context;
                    int processedRecords = 0;
                    KeyValueStore<Long, Long> state = null;

                    @Override
                    public void init(final ProcessorContext context) {
                        final Integer hashCode = hashCode();
                        if (!maxPartitionNumberSeen.containsKey(hashCode)) {
                            if (maxPartitionNumberSeen.size() < 2) {
                                // initial startup case
                                maxPartitionNumberSeen.put(hashCode, -1);
                            } else {
                                // recovery case -- we need to "protect" the new instance of Transformer
                                // to throw the injected exception again
                                maxPartitionNumberSeen.put(hashCode, Integer.MAX_VALUE);
                            }
                        }
                        this.context = context;

                        if (withState) {
                            state = (KeyValueStore<Long, Long>) context.getStateStore(storeName);
                        }
                    }

                    @Override
                    public KeyValue<Long, Long> transform(final Long key, final Long value) {
                        final Integer hashCode = hashCode();
                        int maxPartitionNumber = maxPartitionNumberSeen.get(hashCode);
                        maxPartitionNumber = Math.max(maxPartitionNumber, context.partition());
                        maxPartitionNumberSeen.put(hashCode, maxPartitionNumber);
                        if (maxPartitionNumber == 0 && injectError) {
                            throw new RuntimeException("Injected test exception.");
                        }

                        if (++processedRecords % 10 == 0) {
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
                            context.forward(key, sum);
                            return null;
                        }
                        return new KeyValue<>(key, value);
                    }

                    @Override
                    public KeyValue<Long, Long> punctuate(final long timestamp) {
                        return null;
                    }

                    @Override
                    public void close() { }
                };
            } }, storeNames)
            .to(SINGLE_PARTITION_OUTPUT_TOPIC);

        final KafkaStreams streams = new KafkaStreams(
            builder,
            StreamsTestUtils.getStreamsConfig(
                applicationId,
                CLUSTER.bootstrapServers(),
                Serdes.LongSerde.class.getName(),
                Serdes.LongSerde.class.getName(),
                new Properties() {
                    {
                        put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
                        put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
                        put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, -1);
                        put(StreamsConfig.consumerPrefix(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), 5 * 1000);
                        put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 5 * 1000 - 1);
                        put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
                    }
                }));

        streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(final Thread t, final Throwable e) {
                if (uncaughtException != null) {
                    e.printStackTrace(System.err);
                    fail("Should only get one uncaught exception from Streams.");
                }
                uncaughtException = e;
            }
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
                    new Properties() {
                        {
                            put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT));
                        }
                    }),
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

    @Ignore
    @Test
    public void shouldNotViolateEosIfOneTaskFailsWithState() throws Exception {
        // this test updates a store with 10 + 5 + 5 records per partition (running with 2 partitions)
        // the app is supposed to emit all 40 update records into the output topic
        // the app commits after each 10 records per partition, and thus will have 2*5 uncommitted writes
        // and store updates (ie, another 5 uncommitted writes to a changelog topic per partition)
        //
        // the failure gets inject after 20 committed and 30 uncommitted records got received
        // -> the failure only kills one thread
        // after fail over, we should read 40 committed records and the state stores should contain the correct sums
        // per key (even if some recrods got processed twice)

        final KafkaStreams streams = getKafkaStreams(true);
        try {
            streams.start();

            final List<KeyValue<Long, Long>> committedDataBeforeFailure = new ArrayList<KeyValue<Long, Long>>() {
                {
                    add(new KeyValue<>(0L, 0L));
                    add(new KeyValue<>(0L, 1L));
                    add(new KeyValue<>(0L, 2L));
                    add(new KeyValue<>(0L, 3L));
                    add(new KeyValue<>(0L, 4L));
                    add(new KeyValue<>(0L, 5L));
                    add(new KeyValue<>(0L, 6L));
                    add(new KeyValue<>(0L, 7L));
                    add(new KeyValue<>(0L, 8L));
                    add(new KeyValue<>(0L, 9L));
                    add(new KeyValue<>(1L, 0L));
                    add(new KeyValue<>(1L, 1L));
                    add(new KeyValue<>(1L, 2L));
                    add(new KeyValue<>(1L, 3L));
                    add(new KeyValue<>(1L, 4L));
                    add(new KeyValue<>(1L, 5L));
                    add(new KeyValue<>(1L, 6L));
                    add(new KeyValue<>(1L, 7L));
                    add(new KeyValue<>(1L, 8L));
                    add(new KeyValue<>(1L, 9L));
                }
            };
            final List<KeyValue<Long, Long>> uncommittedDataBeforeFailure = new ArrayList<KeyValue<Long, Long>>() {
                {
                    add(new KeyValue<>(0L, 10L));
                    add(new KeyValue<>(0L, 11L));
                    add(new KeyValue<>(0L, 12L));
                    add(new KeyValue<>(0L, 13L));
                    add(new KeyValue<>(0L, 14L));
                    add(new KeyValue<>(1L, 10L));
                    add(new KeyValue<>(1L, 11L));
                    add(new KeyValue<>(1L, 12L));
                    add(new KeyValue<>(1L, 13L));
                    add(new KeyValue<>(1L, 14L));
                }
            };

            final List<KeyValue<Long, Long>> dataBeforeFailure = new ArrayList<>();
            dataBeforeFailure.addAll(committedDataBeforeFailure);
            dataBeforeFailure.addAll(uncommittedDataBeforeFailure);

            final List<KeyValue<Long, Long>> dataAfterFailure = new ArrayList<KeyValue<Long, Long>>() {
                {
                    add(new KeyValue<>(0L, 15L));
                    add(new KeyValue<>(0L, 16L));
                    add(new KeyValue<>(0L, 17L));
                    add(new KeyValue<>(0L, 18L));
                    add(new KeyValue<>(0L, 19L));
                    add(new KeyValue<>(1L, 15L));
                    add(new KeyValue<>(1L, 16L));
                    add(new KeyValue<>(1L, 17L));
                    add(new KeyValue<>(1L, 18L));
                    add(new KeyValue<>(1L, 19L));
                }
            };

            writeInputData(committedDataBeforeFailure);

            TestUtils.waitForCondition(new TestCondition() {
                @Override
                public boolean conditionMet() {
                    return commitRequested.get() == 2;
                }
            }, 60000, "SteamsTasks did not request commit.");

            writeInputData(uncommittedDataBeforeFailure);

            final List<KeyValue<Long, Long>> uncommittedRecords = readResult(dataBeforeFailure.size(), null);
            final List<KeyValue<Long, Long>> committedRecords = readResult(committedDataBeforeFailure.size(), CONSUMER_GROUP_ID);

            final List<KeyValue<Long, Long>> expectedResultBeforeFailure = computeExpectedResult(dataBeforeFailure);
            checkResultPerKey(committedRecords, computeExpectedResult(committedDataBeforeFailure));
            checkResultPerKey(uncommittedRecords, expectedResultBeforeFailure);
            verifyStateStore(streams, getMaxPerKey(expectedResultBeforeFailure));

            injectError = true;
            writeInputData(dataAfterFailure);

            TestUtils.waitForCondition(new TestCondition() {
                @Override
                public boolean conditionMet() {
                    return uncaughtException != null;
                }
            }, 60000, "Should receive uncaught exception from one StreamThread.");

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
            checkResultPerKey(committedRecordsAfterFailure, expectedResult.subList(committedDataBeforeFailure.size(), expectedResult.size()));

            verifyStateStore(streams, getMaxPerKey(expectedResult));
        } finally {
            streams.close();
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

    private void verifyStateStore(final KafkaStreams streams, final Set<KeyValue<Long, Long>> expectedStoreContent) {
        ReadOnlyKeyValueStore<Long, Long> store = null;

        final long maxWaitingTime = System.currentTimeMillis() + 300000L;
        while (System.currentTimeMillis() < maxWaitingTime) {
            try {
                store = streams.store(storeName, QueryableStoreTypes.<Long, Long>keyValueStore());
                break;
            } catch (final InvalidStateStoreException okJustRetry) {
                try {
                    Thread.sleep(5000L);
                } catch (final Exception ignore) { }
            }
        }

        final KeyValueIterator<Long, Long> it = store.all();
        while (it.hasNext()) {
            assertTrue(expectedStoreContent.remove(it.next()));
        }

        assertTrue(expectedStoreContent.isEmpty());
    }

}
