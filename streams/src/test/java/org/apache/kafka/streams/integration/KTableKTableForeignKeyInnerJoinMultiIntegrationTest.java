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
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.utils.UniqueTopicSerdeScope;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

import static java.time.Duration.ofSeconds;
import static java.util.Arrays.asList;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.junit.Assert.assertEquals;

@Category({IntegrationTest.class})
public class KTableKTableForeignKeyInnerJoinMultiIntegrationTest {
    private final static int NUM_BROKERS = 1;

    public final static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final static MockTime MOCK_TIME = CLUSTER.time;
    private final static String TABLE_1 = "table1";
    private final static String TABLE_2 = "table2";
    private final static String TABLE_3 = "table3";
    private final static String OUTPUT = "output-";
    private final Properties streamsConfig = getStreamsConfig();
    private final Properties streamsConfigTwo = getStreamsConfig();
    private final Properties streamsConfigThree = getStreamsConfig();
    private KafkaStreams streams;
    private KafkaStreams streamsTwo;
    private KafkaStreams streamsThree;
    private final static Properties CONSUMER_CONFIG = new Properties();

    private final static Properties PRODUCER_CONFIG_1 = new Properties();
    private final static Properties PRODUCER_CONFIG_2 = new Properties();
    private final static Properties PRODUCER_CONFIG_3 = new Properties();

    @BeforeClass
    public static void startCluster() throws IOException, InterruptedException {
        CLUSTER.start();
        //Use multiple partitions to ensure distribution of keys.

        CLUSTER.createTopic(TABLE_1, 3, 1);
        CLUSTER.createTopic(TABLE_2, 5, 1);
        CLUSTER.createTopic(TABLE_3, 7, 1);
        CLUSTER.createTopic(OUTPUT, 11, 1);

        PRODUCER_CONFIG_1.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        PRODUCER_CONFIG_1.put(ProducerConfig.ACKS_CONFIG, "all");
        PRODUCER_CONFIG_1.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        PRODUCER_CONFIG_1.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, FloatSerializer.class);

        PRODUCER_CONFIG_2.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        PRODUCER_CONFIG_2.put(ProducerConfig.ACKS_CONFIG, "all");
        PRODUCER_CONFIG_2.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        PRODUCER_CONFIG_2.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);

        PRODUCER_CONFIG_3.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        PRODUCER_CONFIG_3.put(ProducerConfig.ACKS_CONFIG, "all");
        PRODUCER_CONFIG_3.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        PRODUCER_CONFIG_3.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        final List<KeyValue<Integer, Float>> table1 = asList(
            new KeyValue<>(1, 1.33f),
            new KeyValue<>(2, 2.22f),
            new KeyValue<>(3, -1.22f), //Won't be joined in yet.
            new KeyValue<>(4, -2.22f)  //Won't be joined in at all.
        );

        //Partitions pre-computed using the default Murmur2 hash, just to ensure that all 3 partitions will be exercised.
        final List<KeyValue<String, Long>> table2 = asList(
            new KeyValue<>("0", 0L),  //partition 2
            new KeyValue<>("1", 10L), //partition 0
            new KeyValue<>("2", 20L), //partition 2
            new KeyValue<>("3", 30L), //partition 2
            new KeyValue<>("4", 40L), //partition 1
            new KeyValue<>("5", 50L), //partition 0
            new KeyValue<>("6", 60L), //partition 1
            new KeyValue<>("7", 70L), //partition 0
            new KeyValue<>("8", 80L), //partition 0
            new KeyValue<>("9", 90L)  //partition 2
        );

        //Partitions pre-computed using the default Murmur2 hash, just to ensure that all 3 partitions will be exercised.
        final List<KeyValue<Integer, String>> table3 = Collections.singletonList(
                new KeyValue<>(10, "waffle")
        );

        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_1, table1, PRODUCER_CONFIG_1, MOCK_TIME);
        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_2, table2, PRODUCER_CONFIG_2, MOCK_TIME);
        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_3, table3, PRODUCER_CONFIG_3, MOCK_TIME);

        CONSUMER_CONFIG.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        CONSUMER_CONFIG.put(ConsumerConfig.GROUP_ID_CONFIG, "ktable-ktable-consumer");
        CONSUMER_CONFIG.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        CONSUMER_CONFIG.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @Before
    public void before() throws IOException {
        final String stateDirBasePath = TestUtils.tempDirectory().getPath();
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, stateDirBasePath + "-1");
        streamsConfigTwo.put(StreamsConfig.STATE_DIR_CONFIG, stateDirBasePath + "-2");
        streamsConfigThree.put(StreamsConfig.STATE_DIR_CONFIG, stateDirBasePath + "-3");
    }

    @After
    public void after() throws IOException {
        if (streams != null) {
            streams.close();
            streams = null;
        }
        if (streamsTwo != null) {
            streamsTwo.close();
            streamsTwo = null;
        }
        if (streamsThree != null) {
            streamsThree.close();
            streamsThree = null;
        }
        IntegrationTestUtils.purgeLocalStreamsState(asList(streamsConfig, streamsConfigTwo, streamsConfigThree));
    }

    private String innerJoinType = "INNER";

    @Test
    public void shouldInnerJoinMultiPartitionQueryable() throws Exception {
        final Set<KeyValue<Integer, String>> expectedOne = new HashSet<>();
        expectedOne.add(new KeyValue<>(1, "value1=1.33,value2=10,value3=waffle"));

        verifyKTableKTableJoin(expectedOne);
    }

    private void verifyKTableKTableJoin(final Set<KeyValue<Integer, String>> expectedResult) throws Exception {
        final String queryableName = innerJoinType + "-store1";
        final String queryableNameTwo = innerJoinType + "-store2";

        streams = prepareTopology(queryableName, queryableNameTwo, streamsConfig);
        streamsTwo = prepareTopology(queryableName, queryableNameTwo, streamsConfigTwo);
        streamsThree = prepareTopology(queryableName, queryableNameTwo, streamsConfigThree);

        final List<KafkaStreams> kafkaStreamsList = asList(streams, streamsTwo, streamsThree);
        startApplicationAndWaitUntilRunning(kafkaStreamsList, ofSeconds(60));

        final Set<KeyValue<Integer, String>> result = new HashSet<>(IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            CONSUMER_CONFIG,
            OUTPUT,
            expectedResult.size()));

        assertEquals(expectedResult, result);
    }

    private static Properties getStreamsConfig() {
        final Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "KTable-FKJ-Multi");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        return streamsConfig;
    }

    private static KafkaStreams prepareTopology(final String queryableName,
                                                final String queryableNameTwo,
                                                final Properties streamsConfig) {

        final UniqueTopicSerdeScope serdeScope = new UniqueTopicSerdeScope();
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<Integer, Float> table1 = builder.table(
            TABLE_1,
            Consumed.with(serdeScope.decorateSerde(Serdes.Integer(), streamsConfig, true),
                          serdeScope.decorateSerde(Serdes.Float(), streamsConfig, false))
        );
        final KTable<String, Long> table2 = builder.table(
            TABLE_2,
            Consumed.with(serdeScope.decorateSerde(Serdes.String(), streamsConfig, true),
                          serdeScope.decorateSerde(Serdes.Long(), streamsConfig, false))
        );
        final KTable<Integer, String> table3 = builder.table(
            TABLE_3,
            Consumed.with(serdeScope.decorateSerde(Serdes.Integer(), streamsConfig, true),
                          serdeScope.decorateSerde(Serdes.String(), streamsConfig, false))
        );

        final Materialized<Integer, String, KeyValueStore<Bytes, byte[]>> materialized;
        if (queryableName != null) {
            materialized = Materialized.<Integer, String, KeyValueStore<Bytes, byte[]>>as(queryableName)
                    .withKeySerde(serdeScope.decorateSerde(Serdes.Integer(), streamsConfig, true))
                    .withValueSerde(serdeScope.decorateSerde(Serdes.String(), streamsConfig, false))
                    .withCachingDisabled();
        } else {
            throw new RuntimeException("Current implementation of joinOnForeignKey requires a materialized store");
        }

        final Materialized<Integer, String, KeyValueStore<Bytes, byte[]>> materializedTwo;
        if (queryableNameTwo != null) {
            materializedTwo = Materialized.<Integer, String, KeyValueStore<Bytes, byte[]>>as(queryableNameTwo)
                    .withKeySerde(serdeScope.decorateSerde(Serdes.Integer(), streamsConfig, true))
                    .withValueSerde(serdeScope.decorateSerde(Serdes.String(), streamsConfig, false))
                    .withCachingDisabled();
        } else {
            throw new RuntimeException("Current implementation of joinOnForeignKey requires a materialized store");
        }

        final Function<Float, String> tableOneKeyExtractor = value -> Integer.toString((int) value.floatValue());
        final Function<String, Integer> joinedTableKeyExtractor = value -> {
            //Hardwired to return the desired foreign key as a test shortcut
            if (value.contains("value2=10"))
                return 10;
            else
                return 0;
        };

        final ValueJoiner<Float, Long, String> joiner = (value1, value2) -> "value1=" + value1 + ",value2=" + value2;
        final ValueJoiner<String, String, String> joinerTwo = (value1, value2) -> value1 + ",value3=" + value2;

        table1.join(table2, tableOneKeyExtractor, joiner, materialized)
            .join(table3, joinedTableKeyExtractor, joinerTwo, materializedTwo)
            .toStream()
            .to(OUTPUT,
                Produced.with(serdeScope.decorateSerde(Serdes.Integer(), streamsConfig, true),
                              serdeScope.decorateSerde(Serdes.String(), streamsConfig, false)));

        return new KafkaStreams(builder.build(streamsConfig), streamsConfig);
    }
}
