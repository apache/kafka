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
import org.apache.kafka.common.utils.Murmur3;
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
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

@Category({IntegrationTest.class})
public class KTableKTableForeignKeyInnerJoinMultiIntegrationTest {
    private final static int NUM_BROKERS = 1;

    @ClassRule
    public final static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final static MockTime MOCK_TIME = CLUSTER.time;
    private final static String TABLE_1 = "table1";
    private final static String TABLE_2 = "table2";
    private final static String TABLE_3 = "table3";
    private final static String OUTPUT = "output-";
    private static Properties streamsConfig;
    private KafkaStreams streams;
    private KafkaStreams streamsTwo;
    private KafkaStreams streamsThree;
    private final static Properties CONSUMER_CONFIG = new Properties();

    private final static Properties producerConfigOne = new Properties();
    private final static Properties producerConfigTwo = new Properties();
    private final static Properties producerConfigThree = new Properties();

    @BeforeClass
    public static void beforeTest() throws Exception {
        //TODO - This fails about half the time! Not all of the tasks seem to get created or assigned, and it crashes.
        /**
         * Exception in thread "INNER-ktable-ktable-joinOnForeignKeyINNER-ktable-ktable-joinOnForeignKey-query-665f82ce-37a7-4176-b572-5244ed20e13f-StreamThread-2" java.lang.NullPointerException: Task was unexpectedly missing for partition table1-1
         * 	at org.apache.kafka.streams.processor.internals.StreamThread.addRecordsToTasks(StreamThread.java:989)
         * 	at org.apache.kafka.streams.processor.internals.StreamThread.runOnce(StreamThread.java:834)
         * 	at org.apache.kafka.streams.processor.internals.StreamThread.runLoop(StreamThread.java:778)
         * 	at org.apache.kafka.streams.processor.internals.StreamThread.run(StreamThread.java:748)
         */
        //Use multiple partitions to ensure distribution of keys.
        CLUSTER.createTopic(TABLE_1, 11, 1);
        CLUSTER.createTopic(TABLE_2, 2, 1);
        CLUSTER.createTopic(TABLE_3, 7, 1);
        CLUSTER.createTopic(OUTPUT, 13, 1);

        producerConfigOne.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfigOne.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfigOne.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfigOne.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerConfigOne.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, FloatSerializer.class);

        producerConfigTwo.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfigTwo.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfigTwo.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfigTwo.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfigTwo.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);

        producerConfigThree.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfigThree.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfigThree.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfigThree.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerConfigThree.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

        final List<KeyValue<Integer, Float>> table1 = Arrays.asList(
            new KeyValue<>(1, 1.33f),
            new KeyValue<>(2, 2.22f),
            new KeyValue<>(3, -1.22f), //Won't be joined in yet.
            new KeyValue<>(4, -2.22f)  //Won't be joined in at all.
                //,
            //new KeyValue<>(5, 5.55f)   //Will have foreign key be deleted
        );

        //Partitions pre-computed using the default Murmur2 hash, just to ensure that all 3 partitions will be exercised.
        final List<KeyValue<String, Long>> table2 = Arrays.asList(
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
        final List<KeyValue<Integer, String>> table3 = Arrays.asList(
                new KeyValue<>(10, "waffle")
        );


        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_1, table1, producerConfigOne, MOCK_TIME);
        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_2, table2, producerConfigTwo, MOCK_TIME);
        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_3, table3, producerConfigThree, MOCK_TIME);

        CONSUMER_CONFIG.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        CONSUMER_CONFIG.put(ConsumerConfig.GROUP_ID_CONFIG, "ktable-ktable-consumer");
        CONSUMER_CONFIG.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        CONSUMER_CONFIG.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    @Before
    public void before() throws IOException {
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfig);
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
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfig);
    }

    private enum JoinType {
        INNER
    }

    @Test
    public void shouldInnerJoinMultiPartitionQueryable() throws Exception {
        Set<KeyValue<Integer, String>> expectedOne = new HashSet<>();
        expectedOne.add(new KeyValue<>(1, "value1=1.33,value2=10,value3=waffle"));

        verifyKTableKTableJoin(JoinType.INNER, expectedOne, true);

        assert(false); //Manually failing because of Task exception.
    }

    private void verifyKTableKTableJoin(final JoinType joinType,
                                        final Set<KeyValue<Integer, String>> expectedResult,
                                        boolean verifyQueryableState) throws Exception {
        final String queryableName = verifyQueryableState ? joinType + "-ktable-ktable-joinOnForeignKey-query" : null;
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, joinType + "-ktable-ktable-joinOnForeignKey" + queryableName);

        streams = prepareTopology(queryableName);
        streamsTwo = prepareTopology(queryableName);
        streamsThree = prepareTopology(queryableName);
        streams.start();
        streamsTwo.start();
        streamsThree.start();

        final Set<KeyValue<Integer, String>> result = new HashSet<>(IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                CONSUMER_CONFIG,
                OUTPUT,
                expectedResult.size()));

        assertThat(result, equalTo(expectedResult));
    }

    private KafkaStreams prepareTopology(final String queryableName) {
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<Integer, Float> table1 = builder.table(TABLE_1, Consumed.with(Serdes.Integer(), Serdes.Float()));
        final KTable<String, Long> table2 = builder.table(TABLE_2, Consumed.with(Serdes.String(), Serdes.Long()));
        final KTable<Integer, String> table3 = builder.table(TABLE_3, Consumed.with(Serdes.Integer(), Serdes.String()));

        Materialized<Integer, String, KeyValueStore<Bytes, byte[]>> materialized;
        if (queryableName != null) {
            materialized = Materialized.<Integer, String, KeyValueStore<Bytes, byte[]>>as(queryableName)
                    .withKeySerde(Serdes.Integer())
                    .withValueSerde(Serdes.String())
                    .withCachingDisabled();
        } else {
            throw new RuntimeException("Current implementation of joinOnForeignKey requires a materialized store");
        }

        Materialized<Integer, String, KeyValueStore<Bytes, byte[]>> materializedTwo;
        if (queryableName != null) {
            materializedTwo = Materialized.<Integer, String, KeyValueStore<Bytes, byte[]>>as(queryableName + "wafflehouse")
                    .withKeySerde(Serdes.Integer())
                    .withValueSerde(Serdes.String())
                    .withCachingDisabled();
        } else {
            throw new RuntimeException("Current implementation of joinOnForeignKey requires a materialized store");
        }


        ValueMapper<Float, String> tableOneKeyExtractor = (value) -> Integer.toString((int)value.floatValue());
        ValueMapper<String, Integer> joinedTableKeyExtractor = (value) -> {
            if (value.contains("value2=10"))
                return 10;
            else
                return 0;
        }; //Hardwired to get the waffle FK.

        ValueJoiner<Float, Long, String> joiner = (value1, value2) -> "value1=" + value1 + ",value2=" + value2;
        ValueJoiner<String, String, String> joinerTwo = (value1, value2) -> value1 + ",value3=" + value2;

        table1.join(table2, tableOneKeyExtractor, joiner, materialized)
              .join(table3, joinedTableKeyExtractor, joinerTwo, materializedTwo)
            .toStream()
            .to(OUTPUT, Produced.with(Serdes.Integer(), Serdes.String()));

        return new KafkaStreams(builder.build(), streamsConfig);
    }
}
