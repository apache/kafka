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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

@Category({IntegrationTest.class})
public class GlobalKTableEOSIntegrationTest {
    private static final int NUM_BROKERS = 1;
    private static final Properties BROKER_CONFIG;
    static {
        BROKER_CONFIG = new Properties();
        BROKER_CONFIG.put("transaction.state.log.replication.factor", (short) 1);
        BROKER_CONFIG.put("transaction.state.log.min.isr", 1);
    }

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER =
            new EmbeddedKafkaCluster(NUM_BROKERS, BROKER_CONFIG);

    private static volatile int testNo = 0;
    private final MockTime mockTime = CLUSTER.time;
    private final KeyValueMapper<String, Long, Long> keyMapper = (key, value) -> value;
    private final ValueJoiner<Long, String, String> joiner = (value1, value2) -> value1 + "+" + value2;
    private final String globalStore = "globalStore";
    private final Map<String, String> results = new HashMap<>();
    private StreamsBuilder builder;
    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;
    private String globalTableTopic;
    private String streamTopic;
    private GlobalKTable<Long, String> globalTable;
    private KStream<String, Long> stream;
    private ForeachAction<String, String> foreachAction;

    @Before
    public void before() throws Exception {
        testNo++;
        builder = new StreamsBuilder();
        createTopics();
        streamsConfiguration = new Properties();
        final String applicationId = "globalTableTopic-table-eos-test-" + testNo;
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
        globalTable = builder.globalTable(globalTableTopic, Consumed.with(Serdes.Long(), Serdes.String()),
                                          Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as(globalStore)
                                                  .withKeySerde(Serdes.Long())
                                                  .withValueSerde(Serdes.String()));
        final Consumed<String, Long> stringLongConsumed = Consumed.with(Serdes.String(), Serdes.Long());
        stream = builder.stream(streamTopic, stringLongConsumed);
        foreachAction = results::put;
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
        streamTableJoin.foreach(foreachAction);
        produceInitialGlobalTableValues();
        startStreams();
        produceTopicValues(streamTopic);

        final Map<String, String> expected = new HashMap<>();
        expected.put("a", "1+A");
        expected.put("b", "2+B");
        expected.put("c", "3+C");
        expected.put("d", "4+D");
        expected.put("e", "5+null");

        TestUtils.waitForCondition(
            () -> results.equals(expected),
            30000L,
            "waiting for initial values");


        produceGlobalTableValues();

        final ReadOnlyKeyValueStore<Long, String> replicatedStore =
            kafkaStreams.store(globalStore, QueryableStoreTypes.keyValueStore());

        TestUtils.waitForCondition(
            () -> "J".equals(replicatedStore.get(5L)),
            30000,
            "waiting for data in replicated store");

        produceTopicValues(streamTopic);

        expected.put("a", "1+F");
        expected.put("b", "2+G");
        expected.put("c", "3+H");
        expected.put("d", "4+I");
        expected.put("e", "5+J");

        TestUtils.waitForCondition(
            () -> results.equals(expected),
            30000L,
            "waiting for final values");
    }

    @Test
    public void shouldKStreamGlobalKTableJoin() throws Exception {
        final KStream<String, String> streamTableJoin = stream.join(globalTable, keyMapper, joiner);
        streamTableJoin.foreach(foreachAction);
        produceInitialGlobalTableValues();
        startStreams();
        produceTopicValues(streamTopic);

        final Map<String, String> expected = new HashMap<>();
        expected.put("a", "1+A");
        expected.put("b", "2+B");
        expected.put("c", "3+C");
        expected.put("d", "4+D");

        TestUtils.waitForCondition(
            () -> results.equals(expected),
            30000L,
            "waiting for initial values");


        produceGlobalTableValues();

        final ReadOnlyKeyValueStore<Long, String> replicatedStore =
            kafkaStreams.store(globalStore, QueryableStoreTypes.keyValueStore());

        TestUtils.waitForCondition(
            () -> "J".equals(replicatedStore.get(5L)),
            30000,
            "waiting for data in replicated store");

        produceTopicValues(streamTopic);

        expected.put("a", "1+F");
        expected.put("b", "2+G");
        expected.put("c", "3+H");
        expected.put("d", "4+I");
        expected.put("e", "5+J");

        TestUtils.waitForCondition(
            () -> results.equals(expected),
            30000L,
            "waiting for final values");
    }

    @Test
    public void shouldRestoreTransactionalMessages() throws Exception {
        produceInitialGlobalTableValues();

        startStreams();

        final Map<Long, String> expected = new HashMap<>();
        expected.put(1L, "A");
        expected.put(2L, "B");
        expected.put(3L, "C");
        expected.put(4L, "D");

        TestUtils.waitForCondition(
            () -> {
                final ReadOnlyKeyValueStore<Long, String> store;
                try {
                    store = kafkaStreams.store(globalStore, QueryableStoreTypes.keyValueStore());
                } catch (final InvalidStateStoreException ex) {
                    return false;
                }
                final Map<Long, String> result = new HashMap<>();
                final Iterator<KeyValue<Long, String>> it = store.all();
                while (it.hasNext()) {
                    final KeyValue<Long, String> kv = it.next();
                    result.put(kv.key, kv.value);
                }
                return result.equals(expected);
            },
            30000L,
            "waiting for initial values");
    }
    
    @Test
    public void shouldNotRestoreAbortedMessages() throws Exception {
        produceAbortedMessages();
        produceInitialGlobalTableValues();
        produceAbortedMessages();

        startStreams();
        
        final Map<Long, String> expected = new HashMap<>();
        expected.put(1L, "A");
        expected.put(2L, "B");
        expected.put(3L, "C");
        expected.put(4L, "D");

        TestUtils.waitForCondition(
            () -> {
                final ReadOnlyKeyValueStore<Long, String> store;
                try {
                    store = kafkaStreams.store(globalStore, QueryableStoreTypes.keyValueStore());
                } catch (final InvalidStateStoreException ex) {
                    return false;
                }
                final Map<Long, String> result = new HashMap<>();
                final Iterator<KeyValue<Long, String>> it = store.all();
                while (it.hasNext()) {
                    final KeyValue<Long, String> kv = it.next();
                    result.put(kv.key, kv.value);
                }
                return result.equals(expected);
            },
            30000L,
            "waiting for initial values");
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

    private void produceAbortedMessages() throws Exception {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "someid");
        properties.put(ProducerConfig.RETRIES_CONFIG, 1);
        IntegrationTestUtils.produceAbortedKeyValuesSynchronouslyWithTimestamp(
                globalTableTopic, Arrays.asList(
                        new KeyValue<>(1L, "A"),
                        new KeyValue<>(2L, "B"),
                        new KeyValue<>(3L, "C"),
                        new KeyValue<>(4L, "D")
                        ), 
                TestUtils.producerConfig(
                                CLUSTER.bootstrapServers(),
                                LongSerializer.class,
                                StringSerializer.class,
                                properties),
                mockTime.milliseconds());
    }

    private void produceInitialGlobalTableValues() throws Exception {
        produceInitialGlobalTableValues(true);
    }

    private void produceInitialGlobalTableValues(final boolean enableTransactions) throws Exception {
        final Properties properties = new Properties();
        if (enableTransactions) {
            properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "someid");
            properties.put(ProducerConfig.RETRIES_CONFIG, 1);
        }
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
                        StringSerializer.class,
                        properties),
                mockTime,
                enableTransactions);
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
