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

import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

@Category({IntegrationTest.class})
public class GlobalKTableIntegrationTest {
    private static final int NUM_BROKERS = 1;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private static volatile AtomicInteger testNo = new AtomicInteger(0);
    private final MockTime mockTime = CLUSTER.time;
    private final KeyValueMapper<String, Long, Long> keyMapper = (key, value) -> value;
    private final ValueJoiner<Long, String, String> joiner = (value1, value2) -> value1 + "+" + value2;
    private final String globalStore = "globalStore";
    private StreamsBuilder builder;
    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;
    private String globalTableTopic;
    private String streamTopic;
    private GlobalKTable<Long, String> globalTable;
    private KStream<String, Long> stream;
    private MockProcessorSupplier<String, String> supplier;

    @Before
    public void before() throws Exception {
        builder = new StreamsBuilder();
        createTopics();
        streamsConfiguration = new Properties();
        final String applicationId = "globalTableTopic-table-test-" + testNo.incrementAndGet();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        globalTable = builder.globalTable(globalTableTopic, Consumed.with(Serdes.Long(), Serdes.String()),
                                          Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as(globalStore)
                                                  .withKeySerde(Serdes.Long())
                                                  .withValueSerde(Serdes.String()));
        final Consumed<String, Long> stringLongConsumed = Consumed.with(Serdes.String(), Serdes.Long());
        stream = builder.stream(streamTopic, stringLongConsumed);
        supplier = new MockProcessorSupplier<>();
    }

    @After
    public void whenShuttingDown() throws Exception {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @Test
    public void shouldKStreamGlobalKTableLeftJoin() throws Exception {
        final KStream<String, String> streamTableJoin = stream.leftJoin(globalTable, keyMapper, joiner);
        streamTableJoin.process(supplier);
        produceInitialGlobalTableValues();
        startStreams();
        long firstTimestamp = mockTime.milliseconds();
        produceTopicValues(streamTopic);

        final Map<String, ValueAndTimestamp<String>> expected = new HashMap<>();
        expected.put("a", ValueAndTimestamp.make("1+A", firstTimestamp));
        expected.put("b", ValueAndTimestamp.make("2+B", firstTimestamp + 1L));
        expected.put("c", ValueAndTimestamp.make("3+C", firstTimestamp + 2L));
        expected.put("d", ValueAndTimestamp.make("4+D", firstTimestamp + 3L));
        expected.put("e", ValueAndTimestamp.make("5+null", firstTimestamp + 4L));

        TestUtils.waitForCondition(
            () -> {
                if (supplier.capturedProcessorsCount() < 2) {
                    return false;
                }
                final Map<String, ValueAndTimestamp<String>> result = new HashMap<>();
                result.putAll(supplier.capturedProcessors(2).get(0).lastValueAndTimestampPerKey);
                result.putAll(supplier.capturedProcessors(2).get(1).lastValueAndTimestampPerKey);
                return result.equals(expected);
            },
            30000L,
            "waiting for initial values");


        firstTimestamp = mockTime.milliseconds();
        produceGlobalTableValues();

        final ReadOnlyKeyValueStore<Long, String> replicatedStore =
            kafkaStreams.store(globalStore, QueryableStoreTypes.keyValueStore());

        TestUtils.waitForCondition(
            () -> "J".equals(replicatedStore.get(5L)),
            30000,
            "waiting for data in replicated store");

        final ReadOnlyKeyValueStore<Long, ValueAndTimestamp<String>> replicatedStoreWithTimestamp =
            kafkaStreams.store(globalStore, QueryableStoreTypes.timestampedKeyValueStore());
        assertThat(replicatedStoreWithTimestamp.get(5L), equalTo(ValueAndTimestamp.make("J", firstTimestamp + 4L)));

        firstTimestamp = mockTime.milliseconds();
        produceTopicValues(streamTopic);

        expected.put("a", ValueAndTimestamp.make("1+F", firstTimestamp));
        expected.put("b", ValueAndTimestamp.make("2+G", firstTimestamp + 1L));
        expected.put("c", ValueAndTimestamp.make("3+H", firstTimestamp + 2L));
        expected.put("d", ValueAndTimestamp.make("4+I", firstTimestamp + 3L));
        expected.put("e", ValueAndTimestamp.make("5+J", firstTimestamp + 4L));

        TestUtils.waitForCondition(
            () -> {
                if (supplier.capturedProcessorsCount() < 2) {
                    return false;
                }
                final Map<String, ValueAndTimestamp<String>> result = new HashMap<>();
                result.putAll(supplier.capturedProcessors(2).get(0).lastValueAndTimestampPerKey);
                result.putAll(supplier.capturedProcessors(2).get(1).lastValueAndTimestampPerKey);
                return result.equals(expected);
            },
            30000L,
            "waiting for final values");
    }

    @Test
    public void shouldKStreamGlobalKTableJoin() throws Exception {
        final KStream<String, String> streamTableJoin = stream.join(globalTable, keyMapper, joiner);
        streamTableJoin.process(supplier);
        produceInitialGlobalTableValues();
        startStreams();
        long firstTimestamp = mockTime.milliseconds();
        produceTopicValues(streamTopic);

        final Map<String, ValueAndTimestamp<String>> expected = new HashMap<>();
        expected.put("a", ValueAndTimestamp.make("1+A", firstTimestamp));
        expected.put("b", ValueAndTimestamp.make("2+B", firstTimestamp + 1L));
        expected.put("c", ValueAndTimestamp.make("3+C", firstTimestamp + 2L));
        expected.put("d", ValueAndTimestamp.make("4+D", firstTimestamp + 3L));

        TestUtils.waitForCondition(
            () -> {
                if (supplier.capturedProcessorsCount() < 2) {
                    return false;
                }
                final Map<String, ValueAndTimestamp<String>> result = new HashMap<>();
                result.putAll(supplier.capturedProcessors(2).get(0).lastValueAndTimestampPerKey);
                result.putAll(supplier.capturedProcessors(2).get(1).lastValueAndTimestampPerKey);
                return result.equals(expected);
            },
            30000L,
            "waiting for initial values");


        firstTimestamp = mockTime.milliseconds();
        produceGlobalTableValues();

        final ReadOnlyKeyValueStore<Long, String> replicatedStore =
            kafkaStreams.store(globalStore, QueryableStoreTypes.keyValueStore());

        TestUtils.waitForCondition(
            () -> "J".equals(replicatedStore.get(5L)),
            30000,
            "waiting for data in replicated store");

        final ReadOnlyKeyValueStore<Long, ValueAndTimestamp<String>> replicatedStoreWithTimestamp =
            kafkaStreams.store(globalStore, QueryableStoreTypes.timestampedKeyValueStore());
        assertThat(replicatedStoreWithTimestamp.get(5L), equalTo(ValueAndTimestamp.make("J", firstTimestamp + 4L)));

        firstTimestamp = mockTime.milliseconds();
        produceTopicValues(streamTopic);

        expected.put("a", ValueAndTimestamp.make("1+F", firstTimestamp));
        expected.put("b", ValueAndTimestamp.make("2+G", firstTimestamp + 1L));
        expected.put("c", ValueAndTimestamp.make("3+H", firstTimestamp + 2L));
        expected.put("d", ValueAndTimestamp.make("4+I", firstTimestamp + 3L));
        expected.put("e", ValueAndTimestamp.make("5+J", firstTimestamp + 4L));

        TestUtils.waitForCondition(
            () -> {
                if (supplier.capturedProcessorsCount() < 2) {
                    return false;
                }
                final Map<String, ValueAndTimestamp<String>> result = new HashMap<>();
                result.putAll(supplier.capturedProcessors(2).get(0).lastValueAndTimestampPerKey);
                result.putAll(supplier.capturedProcessors(2).get(1).lastValueAndTimestampPerKey);
                return result.equals(expected);
            },
            30000L,
            "waiting for final values");
    }

    @Test
    public void shouldRestoreGlobalInMemoryKTableOnRestart() throws Exception {
        builder = new StreamsBuilder();
        globalTable = builder.globalTable(
            globalTableTopic,
            Consumed.with(Serdes.Long(), Serdes.String()),
            Materialized.as(Stores.inMemoryKeyValueStore(globalStore)));

        produceInitialGlobalTableValues();

        startStreams();
        ReadOnlyKeyValueStore<Long, String> store = kafkaStreams.store(globalStore, QueryableStoreTypes.keyValueStore());
        assertThat(store.approximateNumEntries(), equalTo(4L));
        ReadOnlyKeyValueStore<Long, ValueAndTimestamp<String>> timestampedStore =
            kafkaStreams.store(globalStore, QueryableStoreTypes.timestampedKeyValueStore());
        assertThat(timestampedStore.approximateNumEntries(), equalTo(4L));
        kafkaStreams.close();

        startStreams();
        store = kafkaStreams.store(globalStore, QueryableStoreTypes.keyValueStore());
        assertThat(store.approximateNumEntries(), equalTo(4L));
        timestampedStore = kafkaStreams.store(globalStore, QueryableStoreTypes.timestampedKeyValueStore());
        assertThat(timestampedStore.approximateNumEntries(), equalTo(4L));
    }

    private void createTopics() throws Exception {
        streamTopic = "stream-" + testNo;
        globalTableTopic = "globalTable-" + testNo;
        CLUSTER.createTopics(streamTopic);
        CLUSTER.createTopic(globalTableTopic, 2, 1);
    }
    
    private void startStreams() {
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();
    }

    private void produceTopicValues(final String topic) throws Exception {
        IntegrationTestUtils.produceKeyValuesSynchronously(
                topic,
                Arrays.asList(
                        new KeyValue<>("a", 1L),
                        new KeyValue<>("b", 2L),
                        new KeyValue<>("c", 3L),
                        new KeyValue<>("d", 4L),
                        new KeyValue<>("e", 5L)),
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        StringSerializer.class,
                        LongSerializer.class,
                        new Properties()),
                mockTime);
    }

    private void produceInitialGlobalTableValues() throws Exception {
        IntegrationTestUtils.produceKeyValuesSynchronously(
                globalTableTopic,
                Arrays.asList(
                        new KeyValue<>(1L, "A"),
                        new KeyValue<>(2L, "B"),
                        new KeyValue<>(3L, "C"),
                        new KeyValue<>(4L, "D")
                        ),
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        LongSerializer.class,
                        StringSerializer.class
                        ),
                mockTime);
    }

    private void produceGlobalTableValues() throws Exception {
        IntegrationTestUtils.produceKeyValuesSynchronously(
                globalTableTopic,
                Arrays.asList(
                        new KeyValue<>(1L, "F"),
                        new KeyValue<>(2L, "G"),
                        new KeyValue<>(3L, "H"),
                        new KeyValue<>(4L, "I"),
                        new KeyValue<>(5L, "J")),
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        LongSerializer.class,
                        StringSerializer.class,
                        new Properties()),
                mockTime);
    }
}
