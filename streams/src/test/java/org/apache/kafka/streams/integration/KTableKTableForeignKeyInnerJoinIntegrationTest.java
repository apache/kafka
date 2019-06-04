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
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionResponseWrapper;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionResponseWrapperSerde;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapperSerde;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
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
public class KTableKTableForeignKeyInnerJoinIntegrationTest {
    private final static int NUM_BROKERS = 1;

    @ClassRule
    public final static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final static MockTime MOCK_TIME = CLUSTER.time;
    private final static String TABLE_1 = "table1";
    private final static String TABLE_2 = "table2";
    private final static String OUTPUT = "output-";
    private static Properties streamsConfig;
    private KafkaStreams streams;
    private KafkaStreams streamsTwo;
    private KafkaStreams streamsThree;
    private final static Properties CONSUMER_CONFIG = new Properties();

    static final Properties producerConfigOne = new Properties();
    static final Properties producerConfigTwo = new Properties();

    @BeforeClass
    public static void beforeTest() throws Exception {
        //Use multiple partitions to ensure distribution of keys.
        CLUSTER.createTopic(TABLE_1, 17, 1);
        CLUSTER.createTopic(TABLE_2, 7, 1);
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

        streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        //streamsConfig.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
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

        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_1, table1, producerConfigOne, MOCK_TIME);
        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_2, table2, producerConfigTwo, MOCK_TIME);

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
    public void shouldInnerInnerJoinQueryable() throws Exception {
        Set<KeyValue<Integer, String>> expectedOne = new HashSet<>();
        expectedOne.add(new KeyValue<>(1, "value1=1.33,value2=10"));
        expectedOne.add(new KeyValue<>(2, "value1=2.22,value2=20"));
        //expectedOne.add(new KeyValue<>(5, "value1=5.55,value2=50"));

        List<KeyValue<Integer, String>> expectedTwo = new LinkedList<>();
        expectedTwo.add(new KeyValue<>(3, "value1=1.11,value2=10"));
        verifyKTableKTableJoin(JoinType.INNER, expectedOne, expectedTwo, true);
    }

    private void verifyKTableKTableJoin(final JoinType joinType,
                                        final Set<KeyValue<Integer, String>> expectedResult,
                                        final List<KeyValue<Integer, String>> expectedResultTwo,
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

        //Rapidly reassign the foreignKey to exercise out-of-order resolution.
        final List<KeyValue<Integer, Float>> table1ForeignKeyChange = Arrays.asList(
                new KeyValue<>(3, 2.22f), //Partition 2
                new KeyValue<>(3, 3.33f), //Partition 2
                new KeyValue<>(3, 4.44f), //Partition 1
                new KeyValue<>(3, 5.55f), //Partition 0
                new KeyValue<>(3, 9.99f), //Partition 2
                new KeyValue<>(3, 8.88f), //Partition 0
                new KeyValue<>(3, 0.23f), //Partition 2
                new KeyValue<>(3, 7.77f), //Partition 0
                new KeyValue<>(3, 6.66f), //Partition 1
                new KeyValue<>(3, 1.11f)  //Partition 0 - This will be the final result.
        );

        final List<KeyValue<String, Long>> table2KeyChange = Arrays.asList(
                new KeyValue<>("5", null)
        );

        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_1, table1ForeignKeyChange, producerConfigOne, MOCK_TIME);
        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_2, table2KeyChange, producerConfigTwo, MOCK_TIME);

        //Worst case scenario is that every event update gets propagated to the output.
        //Realistically we just need to wait until every update is sent, and so a conservative 15s timeout has been
        //selected in the case of slower systems.
        final List<KeyValue<Integer, String>> resultTwo = IntegrationTestUtils.readKeyValues(OUTPUT, CONSUMER_CONFIG, 15*1000L, Integer.MAX_VALUE);

        assertThat(resultTwo.get(resultTwo.size()-1), equalTo(expectedResultTwo.get(0)));

        if (verifyQueryableState) {
            Set<KeyValue<Integer, String>> totalResults = new HashSet<>(expectedResult);
            totalResults.add(expectedResultTwo.get(expectedResultTwo.size()-1));
            verifyKTableKTableJoinQueryableState(joinType, totalResults);
        }
    }

    private void verifyKTableKTableJoinQueryableState(final JoinType joinType,
                                                      final Set<KeyValue<Integer, String>> expectedResult) {
        final String queryableName = joinType + "-ktable-ktable-joinOnForeignKey-query";
        final ReadOnlyKeyValueStore<Integer, String> myJoinStoreOne = streams.store(queryableName,
                QueryableStoreTypes.keyValueStore());

        final ReadOnlyKeyValueStore<Integer, String> myJoinStoreTwo = streamsTwo.store(queryableName,
                QueryableStoreTypes.keyValueStore());

        final ReadOnlyKeyValueStore<Integer, String> myJoinStoreThree = streamsThree.store(queryableName,
                QueryableStoreTypes.keyValueStore());

        // store only keeps last set of values, not entire stream of value changes
        final Map<Integer, String> expectedInStore = new HashMap<>();
        for (KeyValue<Integer, String> expected : expectedResult) {
            expectedInStore.put(expected.key, expected.value);
        }

        // depending on partition assignment, the values will be in one of the three stream clients.
        for (Map.Entry<Integer, String> expected : expectedInStore.entrySet()) {
            String one = myJoinStoreOne.get(expected.getKey());
            String two = myJoinStoreTwo.get(expected.getKey());
            String three = myJoinStoreThree.get(expected.getKey());

            String result;
            if (one != null)
                result = one;
            else if (two != null)
                result = two;
            else if (three != null)
                result = three;
            else
                throw new RuntimeException("Cannot find key " + expected.getKey() + " in any of the state stores");
            assertEquals(expected.getValue(), result);
        }

        //Merge all the iterators together to ensure that their sum contains the total set of expected elements.
        final KeyValueIterator<Integer, String> allOne = myJoinStoreOne.all();
        final KeyValueIterator<Integer, String> allTwo = myJoinStoreTwo.all();
        final KeyValueIterator<Integer, String> allThree = myJoinStoreThree.all();

        List<KeyValue<Integer, String>> all = new LinkedList<>();

        while (allOne.hasNext()) {
            all.add(allOne.next());
        }
        while (allTwo.hasNext()) {
            all.add(allTwo.next());
        }
        while (allThree.hasNext()) {
            all.add(allThree.next());
        }
        allOne.close();
        allTwo.close();
        allThree.close();

        for (KeyValue<Integer, String> elem : all) {
            assertTrue(expectedResult.contains(elem));
        }
    }

    private KafkaStreams prepareTopology(final String queryableName) {
        final StreamsBuilder builder = new StreamsBuilder();

        //TODO - bellemare - this appears to only work when I materialize the state... because I cannot access auto-materialized stateSupplier in internalStreamsBuilder!
        Materialized<Integer, Float, KeyValueStore<Bytes, byte[]>> table1Mat = Materialized.as("table-1-test");
            table1Mat.withKeySerde(Serdes.Integer()).withValueSerde(Serdes.Float());

        final KTable<Integer, Float> table1 = builder.table(TABLE_1,
                Consumed.with(Serdes.Integer(), Serdes.Float()));
//                ,
//                table1Mat);

        Materialized<String, Long, KeyValueStore<Bytes, byte[]>> table2Mat = Materialized.as("table-2-test");
        table2Mat.withKeySerde(Serdes.String()).withValueSerde(Serdes.Long());
        final KTable<String, Long> table2 = builder.table(TABLE_2, Consumed.with(Serdes.String(), Serdes.Long()), table2Mat);

        Materialized<Integer, String, KeyValueStore<Bytes, byte[]>> materialized = null;
        if (queryableName != null) {
            materialized = Materialized.<Integer, String, KeyValueStore<Bytes, byte[]>>as(queryableName)
                    .withKeySerde(Serdes.Integer())
                    .withValueSerde(Serdes.String())
                    .withCachingDisabled();
        } else {
            throw new RuntimeException("Current implementation of joinOnForeignKey requires a materialized store");
        }

        ValueMapper<Float, String> tableOneKeyExtractor = (value) -> Integer.toString((int)value.floatValue());
        ValueJoiner<Float, Long, String> joiner = (value1, value2) -> "value1=" + value1 + ",value2=" + value2;

        table1.join(table2, tableOneKeyExtractor, joiner, materialized)
            .toStream()
            .to(OUTPUT, Produced.with(Serdes.Integer(), Serdes.String()));

        return new KafkaStreams(builder.build(), streamsConfig);
    }
}
