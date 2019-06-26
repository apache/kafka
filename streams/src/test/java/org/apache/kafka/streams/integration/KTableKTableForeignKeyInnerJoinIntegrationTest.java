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
import org.apache.kafka.streams.kstream.ValueMapper;
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
    private static Properties STREAMS_CONFIG;
    private KafkaStreams streams;
    private KafkaStreams streamsTwo;
    private KafkaStreams streamsThree;
    private final static Properties CONSUMER_CONFIG = new Properties();
    private static final Properties PRODUCER_CONFIG_1 = new Properties();
    private static final Properties PRODUCER_CONFIG_2 = new Properties();

    @BeforeClass
    public static void beforeTest() throws Exception {
        //Use multiple partitions to ensure distribution of keys.
        PRODUCER_CONFIG_1.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        PRODUCER_CONFIG_1.put(ProducerConfig.ACKS_CONFIG, "all");
        PRODUCER_CONFIG_1.put(ProducerConfig.RETRIES_CONFIG, 0);
        PRODUCER_CONFIG_1.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        PRODUCER_CONFIG_1.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, FloatSerializer.class);

        PRODUCER_CONFIG_2.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        PRODUCER_CONFIG_2.put(ProducerConfig.ACKS_CONFIG, "all");
        PRODUCER_CONFIG_2.put(ProducerConfig.RETRIES_CONFIG, 0);
        PRODUCER_CONFIG_2.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        PRODUCER_CONFIG_2.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);

        STREAMS_CONFIG = new Properties();
        STREAMS_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        STREAMS_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        STREAMS_CONFIG.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        STREAMS_CONFIG.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        STREAMS_CONFIG.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

        CONSUMER_CONFIG.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        CONSUMER_CONFIG.put(ConsumerConfig.GROUP_ID_CONFIG, "ktable-ktable-consumer");
        CONSUMER_CONFIG.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        CONSUMER_CONFIG.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    @Before
    public void before() throws IOException, InterruptedException {
        CLUSTER.deleteTopicsAndWait(TABLE_1);
        CLUSTER.deleteTopicsAndWait(TABLE_2);
        CLUSTER.deleteTopicsAndWait(OUTPUT);

        CLUSTER.createTopic(TABLE_1, 3, 1);
        CLUSTER.createTopic(TABLE_2, 3, 1);
        CLUSTER.createTopic(OUTPUT, 3, 1);

        IntegrationTestUtils.purgeLocalStreamsState(STREAMS_CONFIG);
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
        IntegrationTestUtils.purgeLocalStreamsState(STREAMS_CONFIG);
    }

    private enum JoinType {
        INNER
    }

    @Test
    public void happyPath() throws Exception {
        final List<KeyValue<Integer, Float>> table1 = Arrays.asList(new KeyValue<>(1, 1.33f));
        final List<KeyValue<String, Long>> table2 = Arrays.asList(new KeyValue<>("1", 10L)); //partition 0
        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_1, table1, PRODUCER_CONFIG_1, MOCK_TIME);
        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_2, table2, PRODUCER_CONFIG_2, MOCK_TIME);

        Set<KeyValue<Integer, String>> expected = new HashSet<>();
        expected.add(new KeyValue<>(1, "value1=1.33,value2=10"));

        String currentMethodName = new Object(){}
                .getClass()
                .getEnclosingMethod()
                .getName();
        createAndStartStreamsApplication(currentMethodName);

        final Set<KeyValue<Integer, String>> result = new HashSet<>(IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                CONSUMER_CONFIG,
                OUTPUT,
                expected.size()));

        assertEquals(result, expected);
    }

    @Test
    public void shouldProduceNullsWhenNonMatchingForeignKeyInnerJoin() throws Exception {
        //There is no matching extracted foreign-key of 8 anywhere. Should not produce any output for INNER JOIN.
        List<KeyValue<Integer, Float>> table1 = Arrays.asList(new KeyValue<>(1, 8.33f));
        List<KeyValue<String, Long>> table2 = Arrays.asList(new KeyValue<>("1", 10L)); //partition 0
        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_1, table1, PRODUCER_CONFIG_1, MOCK_TIME);
        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_2, table2, PRODUCER_CONFIG_2, MOCK_TIME);

        String currentMethodName = new Object(){}
                .getClass()
                .getEnclosingMethod()
                .getName();
        createAndStartStreamsApplication(currentMethodName);

        //There is also no matching extracted foreign-key for 18 anywhere. This WILL produce a null output for INNER JOIN,
        //since we cannot remember (maintain state) that the FK=8 also produced a null result.
        table1 = Arrays.asList(new KeyValue<>(1, 18.00f));
        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_1, table1, PRODUCER_CONFIG_1, MOCK_TIME);

        List<KeyValue<Integer, String>> expected = new LinkedList<>();
        expected.add(new KeyValue<>(1, null));

        final List<KeyValue<Integer, String>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                CONSUMER_CONFIG,
                OUTPUT,
                expected.size());
        assertEquals(result, expected);

        //Another change to FK that has no match on the RHS will result in another null
        table1 = Arrays.asList(new KeyValue<>(1, 100.00f));
        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_1, table1, PRODUCER_CONFIG_1, MOCK_TIME);
        //Consume the next event - note that we are using the same consumerGroupId, so this will consume a new event.
        final List<KeyValue<Integer, String>> result2 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                CONSUMER_CONFIG,
                OUTPUT,
                expected.size());
        assertEquals(result2, expected);

        //Now set the LHS event FK to match the table2 key-value.
        table1 = Arrays.asList(new KeyValue<>(1, 1.11f));

        List<KeyValue<Integer, String>> expected3 = new LinkedList<>();
        expected3.add(new KeyValue<>(1, "value1=1.11,value2=10"));

        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_1, table1, PRODUCER_CONFIG_1, MOCK_TIME);
        final List<KeyValue<Integer, String>> result3 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                CONSUMER_CONFIG,
                OUTPUT,
                expected3.size());
        assertEquals(result3, expected3);
    }

    @Test
    public void shouldInnerInnerJoinQueryable() throws Exception {
        final List<KeyValue<Integer, Float>> table1 = Arrays.asList(
                new KeyValue<>(1, 1.33f),
                new KeyValue<>(2, 2.22f),
                new KeyValue<>(3, -1.22f), //Won't be joined in yet.
                new KeyValue<>(4, -2.22f),  //Won't be joined in at all.
                new KeyValue<>(5, 2.22f)
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

        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_1, table1, PRODUCER_CONFIG_1, MOCK_TIME);
        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_2, table2, PRODUCER_CONFIG_2, MOCK_TIME);

        Set<KeyValue<Integer, String>> expectedOne = new HashSet<>();
        expectedOne.add(new KeyValue<>(1, "value1=1.33,value2=10"));
        expectedOne.add(new KeyValue<>(2, "value1=2.22,value2=20"));
        expectedOne.add(new KeyValue<>(5, "value1=2.22,value2=20"));

        String currentMethodName = new Object(){}
                .getClass()
                .getEnclosingMethod()
                .getName();
        createAndStartStreamsApplication(currentMethodName);

        final Set<KeyValue<Integer, String>> result = new HashSet<>(IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                CONSUMER_CONFIG,
                OUTPUT,
                expectedOne.size()));

        assertThat(result, equalTo(expectedOne));

        List<KeyValue<Integer, String>> expectedTwo = new LinkedList<>();
        expectedTwo.add(new KeyValue<>(3, "value1=1.11,value2=10"));
        expectedTwo.add(new KeyValue<>(5, null));


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
                new KeyValue<>(3, 1.11f),  //Partition 0 - This will be the final result.
                new KeyValue<>(5, null)   //Validate that null is propagated when INNER join has one side deleted.
        );

        final List<KeyValue<String, Long>> table2KeyChange = Arrays.asList(
                new KeyValue<>("5", null)
        );

        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_1, table1ForeignKeyChange, PRODUCER_CONFIG_1, MOCK_TIME);
        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_2, table2KeyChange, PRODUCER_CONFIG_2, MOCK_TIME);

        final List<KeyValue<Integer, String>> resultTwo = IntegrationTestUtils.readKeyValues(OUTPUT, CONSUMER_CONFIG, 15*1000L, Integer.MAX_VALUE);

        assertArrayEquals(resultTwo.toArray(), expectedTwo.toArray());

        //verifyKTableKTableJoin(JoinType.INNER, expectedOne, expectedTwo, true);
    }

    private void createAndStartStreamsApplication(String queryableStoreName) {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-ktable-joinOnForeignKey-" + queryableStoreName);
        streams = prepareTopology(queryableStoreName);
        streamsTwo = prepareTopology(queryableStoreName);
        streamsThree = prepareTopology(queryableStoreName);
        streams.start();
        streamsTwo.start();
        streamsThree.start();
    }



    private void verifyKTableKTableJoin(final JoinType joinType,
                                        final Set<KeyValue<Integer, String>> expectedResult,
                                        final List<KeyValue<Integer, String>> expectedResultTwo,
                                        boolean verifyQueryableState) throws Exception {
        final String queryableName = verifyQueryableState ? joinType + "-ktable-ktable-joinOnForeignKey-query" : null;
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, joinType + "-ktable-ktable-joinOnForeignKey" + queryableName);

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
                new KeyValue<>(3, 1.11f),  //Partition 0 - This will be the final result.
                new KeyValue<>(5, null)   //Validate that null is propagated when INNER join has one side deleted.
        );

        final List<KeyValue<String, Long>> table2KeyChange = Arrays.asList(
                new KeyValue<>("5", null)
        );

        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_1, table1ForeignKeyChange, PRODUCER_CONFIG_1, MOCK_TIME);
        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_2, table2KeyChange, PRODUCER_CONFIG_2, MOCK_TIME);
        //Thread.sleep(15000);

        //Worst case scenario is that every event update gets propagated to the output.
        //Realistically we just need to wait until every update is sent, and so a conservative 15s timeout has been
        //selected in the case of slower systems.
        final List<KeyValue<Integer, String>> resultTwo = IntegrationTestUtils.readKeyValues(OUTPUT, CONSUMER_CONFIG, 15*1000L, Integer.MAX_VALUE);

        assertArrayEquals(resultTwo.toArray(), expectedResultTwo.toArray());

//        if (verifyQueryableState) {
//            Set<KeyValue<Integer, String>> totalResults = new HashSet<>(expectedResult);
//            totalResults.add(expectedResultTwo.get(expectedResultTwo.size()-1));
//            verifyKTableKTableJoinQueryableState(joinType, totalResults);
//        }
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

    private KafkaStreams prepareTopology(final String queryableStoreName) {
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<Integer, Float> table1 = builder.table(TABLE_1, Consumed.with(Serdes.Integer(), Serdes.Float()));
        final KTable<String, Long> table2 = builder.table(TABLE_2, Consumed.with(Serdes.String(), Serdes.Long()));

        Materialized<Integer, String, KeyValueStore<Bytes, byte[]>> materialized;
        if (queryableStoreName != null) {
            materialized = Materialized.<Integer, String, KeyValueStore<Bytes, byte[]>>as(queryableStoreName)
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

        return new KafkaStreams(builder.build(), STREAMS_CONFIG);
    }
}
