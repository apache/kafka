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
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
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
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;


@Category({IntegrationTest.class})
public class KTableKTableForeignKeyJoinIntegrationTest {
    private final static int NUM_BROKERS = 1;

    @ClassRule
    public final static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final static MockTime MOCK_TIME = CLUSTER.time;
    private final static String LEFT_TABLE = "left_table";
    private final static String RIGHT_TABLE = "right_table";
    private final static String OUTPUT = "output-topic";
    private static Properties streamsConfig;
    private KafkaStreams streams;
    private KafkaStreams streamsTwo;
    private KafkaStreams streamsThree;
    private static final  Properties CONSUMER_CONFIG = new Properties();
    private static final Properties LEFT_PROD_CONF = new Properties();
    private static final Properties RIGHT_PROD_CONF = new Properties();

    @BeforeClass
    public static void beforeTest() {
        //Use multiple partitions to ensure distribution of keys.
        LEFT_PROD_CONF.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        LEFT_PROD_CONF.put(ProducerConfig.ACKS_CONFIG, "all");
        LEFT_PROD_CONF.put(ProducerConfig.RETRIES_CONFIG, 0);
        LEFT_PROD_CONF.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        LEFT_PROD_CONF.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, FloatSerializer.class);

        RIGHT_PROD_CONF.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        RIGHT_PROD_CONF.put(ProducerConfig.ACKS_CONFIG, "all");
        RIGHT_PROD_CONF.put(ProducerConfig.RETRIES_CONFIG, 0);
        RIGHT_PROD_CONF.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        RIGHT_PROD_CONF.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);

        streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);

        CONSUMER_CONFIG.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        CONSUMER_CONFIG.put(ConsumerConfig.GROUP_ID_CONFIG, "ktable-ktable-consumer");
        CONSUMER_CONFIG.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        CONSUMER_CONFIG.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    @Before
    public void before() throws IOException, InterruptedException {
        CLUSTER.deleteTopicsAndWait(LEFT_TABLE);
        CLUSTER.deleteTopicsAndWait(RIGHT_TABLE);
        CLUSTER.deleteTopicsAndWait(OUTPUT);

        CLUSTER.createTopic(LEFT_TABLE, 3, 1);
        CLUSTER.createTopic(RIGHT_TABLE, 3, 1);
        CLUSTER.createTopic(OUTPUT, 3, 1);

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

    @Test
    public void doInnerJoinFromLeftThenDeleteLeftEntity() throws Exception {
        final List<KeyValue<String, Long>> rightTableEvents = Arrays.asList(new KeyValue<>("1", 10L), new KeyValue<>("2", 20L)); //partition 0
        IntegrationTestUtils.produceKeyValuesSynchronously(RIGHT_TABLE, rightTableEvents, RIGHT_PROD_CONF, MOCK_TIME);

        final String currentMethodName = new Object() { }
                .getClass()
                .getEnclosingMethod()
                .getName();
        createAndStartStreamsApplication(currentMethodName, false);

        final List<KeyValue<Integer, Float>> leftTableEvents = Arrays.asList(new KeyValue<>(1, 1.33f), new KeyValue<>(2, 2.77f));
        IntegrationTestUtils.produceKeyValuesSynchronously(LEFT_TABLE, leftTableEvents, LEFT_PROD_CONF, MOCK_TIME);

        final Set<KeyValue<Integer, String>> expected = new HashSet<>();
        expected.add(new KeyValue<>(1, "value1=1.33,value2=10"));
        expected.add(new KeyValue<>(2, "value1=2.77,value2=20"));

        final Set<KeyValue<Integer, String>> result = new HashSet<>(IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                CONSUMER_CONFIG,
                OUTPUT,
                expected.size()));
        assertEquals(expected, result);

        //Now delete one LHS entity such that one delete is propagated down to the output.
        final Set<KeyValue<Integer, String>> expectedDeleted = new HashSet<>();
        expectedDeleted.add(new KeyValue<>(1, null));

        final List<KeyValue<Integer, Float>> rightTableDeleteEvents = Arrays.asList(new KeyValue<>(1, null));
        IntegrationTestUtils.produceKeyValuesSynchronously(LEFT_TABLE, rightTableDeleteEvents, LEFT_PROD_CONF, MOCK_TIME);
        final Set<KeyValue<Integer, String>> resultDeleted = new HashSet<>(IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                CONSUMER_CONFIG,
                OUTPUT,
                expectedDeleted.size()));
        assertEquals(expectedDeleted, resultDeleted);

        //Ensure the state stores have the correct values within:
        final Set<KeyValue<Integer, String>> expMatResults = new HashSet<>();
        expMatResults.add(new KeyValue<>(2, "value1=2.77,value2=20"));
        validateQueryableStoresContainExpectedKeyValues(expMatResults, currentMethodName);
    }

    @Test
    public void doLeftJoinFromLeftThenDeleteLeftEntity() throws Exception {
        final List<KeyValue<String, Long>> rightTableEvents = Arrays.asList(new KeyValue<>("1", 10L), new KeyValue<>("2", 20L)); //partition 0
        IntegrationTestUtils.produceKeyValuesSynchronously(RIGHT_TABLE, rightTableEvents, RIGHT_PROD_CONF, MOCK_TIME);

        final String currentMethodName = new Object() { }
                .getClass()
                .getEnclosingMethod()
                .getName();
        createAndStartStreamsApplication(currentMethodName, true);

        final List<KeyValue<Integer, Float>> leftTableEvents = Arrays.asList(new KeyValue<>(1, 1.33f), new KeyValue<>(2, 2.77f));
        IntegrationTestUtils.produceKeyValuesSynchronously(LEFT_TABLE, leftTableEvents, LEFT_PROD_CONF, MOCK_TIME);

        final Set<KeyValue<Integer, String>> expected = new HashSet<>();
        expected.add(new KeyValue<>(1, "value1=1.33,value2=10"));
        expected.add(new KeyValue<>(2, "value1=2.77,value2=20"));

        final Set<KeyValue<Integer, String>> result = new HashSet<>(IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                CONSUMER_CONFIG,
                OUTPUT,
                expected.size()));
        assertEquals(expected, result);

        //Now delete one LHS entity such that one delete is propagated down to the output.
        final Set<KeyValue<Integer, String>> expectedDeleted = new HashSet<>();
        expectedDeleted.add(new KeyValue<>(1, null));

        final List<KeyValue<Integer, Float>> rightTableDeleteEvents = Arrays.asList(new KeyValue<>(1, null));
        IntegrationTestUtils.produceKeyValuesSynchronously(LEFT_TABLE, rightTableDeleteEvents, LEFT_PROD_CONF, MOCK_TIME);
        final Set<KeyValue<Integer, String>> resultDeleted = new HashSet<>(IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                CONSUMER_CONFIG,
                OUTPUT,
                expectedDeleted.size()));
        assertEquals(expectedDeleted, resultDeleted);

        //Ensure the state stores have the correct values within:
        final Set<KeyValue<Integer, String>> expMatResults = new HashSet<>();
        expMatResults.add(new KeyValue<>(2, "value1=2.77,value2=20"));
        validateQueryableStoresContainExpectedKeyValues(expMatResults, currentMethodName);
    }

    @Test
    public void doInnerJoinFromRightThenDeleteRightEntity() throws Exception {
        final List<KeyValue<Integer, Float>> leftTableEvents = Arrays.asList(
                new KeyValue<>(1, 1.33f),
                new KeyValue<>(2, 1.77f),
                new KeyValue<>(3, 3.77f));
        IntegrationTestUtils.produceKeyValuesSynchronously(LEFT_TABLE, leftTableEvents, LEFT_PROD_CONF, MOCK_TIME);
        final String currentMethodName = new Object() { }
                .getClass()
                .getEnclosingMethod()
                .getName();
        createAndStartStreamsApplication(currentMethodName, false);

        final List<KeyValue<String, Long>> rightTableEvents = Arrays.asList(
                new KeyValue<>("1", 10L),  //partition 0
                new KeyValue<>("2", 20L),  //partition 2
                new KeyValue<>("3", 30L)); //partition 2
        IntegrationTestUtils.produceKeyValuesSynchronously(RIGHT_TABLE, rightTableEvents, RIGHT_PROD_CONF, MOCK_TIME);

        //Ensure that the joined values exist in the output
        final Set<KeyValue<Integer, String>> expected = new HashSet<>();
        expected.add(new KeyValue<>(1, "value1=1.33,value2=10"));   //Will be deleted.
        expected.add(new KeyValue<>(2, "value1=1.77,value2=10"));   //Will be deleted.
        expected.add(new KeyValue<>(3, "value1=3.77,value2=30"));   //Will not be deleted.

        final Set<KeyValue<Integer, String>> result = new HashSet<>(IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                CONSUMER_CONFIG,
                OUTPUT,
                expected.size()));
        assertEquals(expected, result);

        //Now delete the RHS entity such that all matching keys have deletes propagated.
        final Set<KeyValue<Integer, String>> expectedDeleted = new HashSet<>();
        expectedDeleted.add(new KeyValue<>(1, null));
        expectedDeleted.add(new KeyValue<>(2, null));

        final List<KeyValue<String, Long>> rightTableDeleteEvents = Arrays.asList(new KeyValue<>("1", null));
        IntegrationTestUtils.produceKeyValuesSynchronously(RIGHT_TABLE, rightTableDeleteEvents, RIGHT_PROD_CONF, MOCK_TIME);
        final Set<KeyValue<Integer, String>> resultDeleted = new HashSet<>(IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                CONSUMER_CONFIG,
                OUTPUT,
                expectedDeleted.size()));
        assertEquals(expectedDeleted, resultDeleted);

        //Ensure the state stores have the correct values within:
        final Set<KeyValue<Integer, String>> expMatResults = new HashSet<>();
        expMatResults.add(new KeyValue<>(3, "value1=3.77,value2=30"));
        validateQueryableStoresContainExpectedKeyValues(expMatResults, currentMethodName);
    }

    @Test
    public void doLeftJoinFromRightThenDeleteRightEntity() throws Exception {
        final List<KeyValue<Integer, Float>> leftTableEvents = Arrays.asList(
                new KeyValue<>(1, 1.33f),
                new KeyValue<>(2, 1.77f),
                new KeyValue<>(3, 3.77f));
        IntegrationTestUtils.produceKeyValuesSynchronously(LEFT_TABLE, leftTableEvents, LEFT_PROD_CONF, MOCK_TIME);
        final String currentMethodName = new Object() { }
                .getClass()
                .getEnclosingMethod()
                .getName();
        createAndStartStreamsApplication(currentMethodName, true);

        final List<KeyValue<String, Long>> rightTableEvents = Arrays.asList(
                new KeyValue<>("1", 10L),  //partition 0
                new KeyValue<>("2", 20L),  //partition 2
                new KeyValue<>("3", 30L)); //partition 2
        IntegrationTestUtils.produceKeyValuesSynchronously(RIGHT_TABLE, rightTableEvents, RIGHT_PROD_CONF, MOCK_TIME);

        //Ensure that the joined values exist in the output
        final Set<KeyValue<Integer, String>> expected = new HashSet<>();
        expected.add(new KeyValue<>(1, "value1=1.33,value2=10"));   //Will be deleted.
        expected.add(new KeyValue<>(2, "value1=1.77,value2=10"));   //Will be deleted.
        expected.add(new KeyValue<>(3, "value1=3.77,value2=30"));   //Will not be deleted.
        //final HashSet<KeyValue<Integer, String>> expected = new HashSet<>(buildExpectedResults(leftTableEvents, rightTableEvents, false));

        final Set<KeyValue<Integer, String>> result = new HashSet<>(IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                CONSUMER_CONFIG,
                OUTPUT,
                expected.size()));
        assertEquals(expected, result);

        //Now delete the RHS entity such that all matching keys have deletes propagated.
        //This will exercise the joiner with the RHS value == null.
        final Set<KeyValue<Integer, String>> expectedDeleted = new HashSet<>();
        expectedDeleted.add(new KeyValue<>(1, "value1=1.33,value2=null"));
        expectedDeleted.add(new KeyValue<>(2, "value1=1.77,value2=null"));

        final List<KeyValue<String, Long>> rightTableDeleteEvents = Arrays.asList(new KeyValue<>("1", null));
        IntegrationTestUtils.produceKeyValuesSynchronously(RIGHT_TABLE, rightTableDeleteEvents, RIGHT_PROD_CONF, MOCK_TIME);
        final Set<KeyValue<Integer, String>> resultDeleted = new HashSet<>(IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                CONSUMER_CONFIG,
                OUTPUT,
                expectedDeleted.size()));
        assertEquals(expectedDeleted, resultDeleted);

        //Ensure the state stores have the correct values within:
        final Set<KeyValue<Integer, String>> expMatResults = new HashSet<>();
        expMatResults.add(new KeyValue<>(1, "value1=1.33,value2=null"));
        expMatResults.add(new KeyValue<>(2, "value1=1.77,value2=null"));
        expMatResults.add(new KeyValue<>(3, "value1=3.77,value2=30"));
        validateQueryableStoresContainExpectedKeyValues(expMatResults, currentMethodName);
    }

    @Test
    public void doInnerJoinProduceNullsWhenValueHasNonMatchingForeignKey() throws Exception {
        //There is no matching extracted foreign-key of 8 anywhere. Should not produce any output for INNER JOIN, only
        //because the state is transitioning from oldValue=null -> newValue=8.33.
        List<KeyValue<Integer, Float>> leftTableEvents = Arrays.asList(new KeyValue<>(1, 8.33f));
        final List<KeyValue<String, Long>> rightTableEvents = Arrays.asList(new KeyValue<>("1", 10L)); //partition 0
        IntegrationTestUtils.produceKeyValuesSynchronously(LEFT_TABLE, leftTableEvents, LEFT_PROD_CONF, MOCK_TIME);
        IntegrationTestUtils.produceKeyValuesSynchronously(RIGHT_TABLE, rightTableEvents, RIGHT_PROD_CONF, MOCK_TIME);

        final String currentMethodName = new Object() { }
                .getClass()
                .getEnclosingMethod()
                .getName();
        createAndStartStreamsApplication(currentMethodName, false);

        //There is also no matching extracted foreign-key for 18 anywhere. This WILL produce a null output for INNER JOIN,
        //since we cannot remember (maintain state) that the FK=8 also produced a null result.
        leftTableEvents = Arrays.asList(new KeyValue<>(1, 18.00f));
        IntegrationTestUtils.produceKeyValuesSynchronously(LEFT_TABLE, leftTableEvents, LEFT_PROD_CONF, MOCK_TIME);

        final List<KeyValue<Integer, String>> expected = new LinkedList<>();
        expected.add(new KeyValue<>(1, null));

        final List<KeyValue<Integer, String>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                CONSUMER_CONFIG,
                OUTPUT,
                expected.size());
        assertEquals(result, expected);

        //Another change to FK that has no match on the RHS will result in another null
        leftTableEvents = Arrays.asList(new KeyValue<>(1, 100.00f));
        IntegrationTestUtils.produceKeyValuesSynchronously(LEFT_TABLE, leftTableEvents, LEFT_PROD_CONF, MOCK_TIME);
        //Consume the next event - note that we are using the same consumerGroupId, so this will consume a new event.
        final List<KeyValue<Integer, String>> result2 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                CONSUMER_CONFIG,
                OUTPUT,
                expected.size());
        assertEquals(result2, expected);

        //Now set the LHS event FK to match the rightTableEvents key-value.
        leftTableEvents = Arrays.asList(new KeyValue<>(1, 1.11f));

        final List<KeyValue<Integer, String>> expected3 = new LinkedList<>();
        expected3.add(new KeyValue<>(1, "value1=1.11,value2=10"));

        IntegrationTestUtils.produceKeyValuesSynchronously(LEFT_TABLE, leftTableEvents, LEFT_PROD_CONF, MOCK_TIME);
        final List<KeyValue<Integer, String>> result3 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                CONSUMER_CONFIG,
                OUTPUT,
                expected3.size());
        assertEquals(result3, expected3);

        //Ensure the state stores have the correct values within:
        final Set<KeyValue<Integer, String>> expMatResults = new HashSet<>();
        expMatResults.add(new KeyValue<>(1, "value1=1.11,value2=10"));
        validateQueryableStoresContainExpectedKeyValues(expMatResults, currentMethodName);
    }

    @Test
    public void doLeftJoinProduceJoinedResultsWhenValueHasNonMatchingForeignKey() throws Exception {
        //There is no matching extracted foreign-key of 8 anywhere.
        //However, it will still run the join function since this is LEFT join.
        List<KeyValue<Integer, Float>> leftTableEvents = Arrays.asList(new KeyValue<>(1, 8.33f));
        final List<KeyValue<String, Long>> rightTableEvents = Arrays.asList(new KeyValue<>("1", 10L)); //partition 0
        IntegrationTestUtils.produceKeyValuesSynchronously(LEFT_TABLE, leftTableEvents, LEFT_PROD_CONF, MOCK_TIME);
        IntegrationTestUtils.produceKeyValuesSynchronously(RIGHT_TABLE, rightTableEvents, RIGHT_PROD_CONF, MOCK_TIME);

        final String currentMethodName = new Object() { }
                .getClass()
                .getEnclosingMethod()
                .getName();
        createAndStartStreamsApplication(currentMethodName, true);

        final List<KeyValue<Integer, String>> expected = new LinkedList<>();
        expected.add(new KeyValue<>(1, "value1=8.33,value2=null"));
        final List<KeyValue<Integer, String>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                CONSUMER_CONFIG,
                OUTPUT,
                expected.size());
        assertEquals(expected, result);

        //There is also no matching extracted foreign-key for 18 anywhere.
        //However, it will still run the join function since this if LEFT join.
        leftTableEvents = Arrays.asList(new KeyValue<>(1, 18.0f));
        IntegrationTestUtils.produceKeyValuesSynchronously(LEFT_TABLE, leftTableEvents, LEFT_PROD_CONF, MOCK_TIME);

        final List<KeyValue<Integer, String>> expected2 = new LinkedList<>();
        expected2.add(new KeyValue<>(1, "value1=18.0,value2=null"));
        final List<KeyValue<Integer, String>> result2 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                CONSUMER_CONFIG,
                OUTPUT,
                expected2.size());
        assertEquals(expected2, result2);


        leftTableEvents = Arrays.asList(new KeyValue<>(1, 1.11f));
        IntegrationTestUtils.produceKeyValuesSynchronously(LEFT_TABLE, leftTableEvents, LEFT_PROD_CONF, MOCK_TIME);

        final List<KeyValue<Integer, String>> expected3 = new LinkedList<>();
        expected3.add(new KeyValue<>(1, "value1=1.11,value2=10"));
        final List<KeyValue<Integer, String>> result3 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                CONSUMER_CONFIG,
                OUTPUT,
                expected3.size());
        assertEquals(expected3, result3);

        //Ensure the state stores have the correct values within:
        final Set<KeyValue<Integer, String>> expMatResults = new HashSet<>();
        expMatResults.add(new KeyValue<>(1, "value1=1.11,value2=10"));
        validateQueryableStoresContainExpectedKeyValues(expMatResults, currentMethodName);
    }

    @Test
    public void doInnerJoinFilterOutRapidlyChangingForeignKeyValues() throws Exception {
        final List<KeyValue<Integer, Float>> leftTableEvents = Arrays.asList(
                new KeyValue<>(1, 1.33f),
                new KeyValue<>(2, 2.22f),
                new KeyValue<>(3, -1.22f), //Won't be joined in
                new KeyValue<>(4, -2.22f), //Won't be joined in
                new KeyValue<>(5, 2.22f)
        );

        //Partitions pre-computed using the default Murmur2 hash, just to ensure that all 3 partitions will be exercised.
        final List<KeyValue<String, Long>> rightTableEvents = Arrays.asList(
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

        IntegrationTestUtils.produceKeyValuesSynchronously(LEFT_TABLE, leftTableEvents, LEFT_PROD_CONF, MOCK_TIME);
        IntegrationTestUtils.produceKeyValuesSynchronously(RIGHT_TABLE, rightTableEvents, RIGHT_PROD_CONF, MOCK_TIME);

        final Set<KeyValue<Integer, String>> expected = new HashSet<>();
        expected.add(new KeyValue<>(1, "value1=1.33,value2=10"));
        expected.add(new KeyValue<>(2, "value1=2.22,value2=20"));
        expected.add(new KeyValue<>(5, "value1=2.22,value2=20"));

        final String currentMethodName = new Object() { }
                .getClass()
                .getEnclosingMethod()
                .getName();
        createAndStartStreamsApplication(currentMethodName, false);

        final Set<KeyValue<Integer, String>> result = new HashSet<>(IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                CONSUMER_CONFIG,
                OUTPUT,
                expected.size()));

        assertEquals(result, expected);

        //Rapidly change the foreign key, to validate that the hashing prevents incorrect results from being output,
        //and that eventually the correct value is output.
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

        IntegrationTestUtils.produceKeyValuesSynchronously(LEFT_TABLE, table1ForeignKeyChange, LEFT_PROD_CONF, MOCK_TIME);
        final List<KeyValue<Integer, String>> resultTwo = IntegrationTestUtils.readKeyValues(OUTPUT, CONSUMER_CONFIG, 15 * 1000L, Integer.MAX_VALUE);

        final List<KeyValue<Integer, String>> expectedTwo = new LinkedList<>();
        expectedTwo.add(new KeyValue<>(3, "value1=1.11,value2=10"));
        assertArrayEquals(resultTwo.toArray(), expectedTwo.toArray());

        //Ensure the state stores have the correct values within:
        final Set<KeyValue<Integer, String>> expMatResults = new HashSet<>();
        expMatResults.addAll(expected);
        expMatResults.addAll(expectedTwo);
        validateQueryableStoresContainExpectedKeyValues(expMatResults, currentMethodName);
    }

    @Test
    public void doLeftJoinFilterOutRapidlyChangingForeignKeyValues() throws Exception {
        final List<KeyValue<Integer, Float>> leftTableEvents = Arrays.asList(
                new KeyValue<>(1, 1.33f),
                new KeyValue<>(2, 2.22f)
        );

        //Partitions pre-computed using the default Murmur2 hash, just to ensure that all 3 partitions will be exercised.
        final List<KeyValue<String, Long>> rightTableEvents = Arrays.asList(
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

        IntegrationTestUtils.produceKeyValuesSynchronously(LEFT_TABLE, leftTableEvents, LEFT_PROD_CONF, MOCK_TIME);
        IntegrationTestUtils.produceKeyValuesSynchronously(RIGHT_TABLE, rightTableEvents, RIGHT_PROD_CONF, MOCK_TIME);

        final Set<KeyValue<Integer, String>> expected = new HashSet<>();
        expected.add(new KeyValue<>(1, "value1=1.33,value2=10"));
        expected.add(new KeyValue<>(2, "value1=2.22,value2=20"));

        final String currentMethodName = new Object() { }
                .getClass()
                .getEnclosingMethod()
                .getName();
        createAndStartStreamsApplication(currentMethodName, false);

        final Set<KeyValue<Integer, String>> result = new HashSet<>(IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                CONSUMER_CONFIG,
                OUTPUT,
                expected.size()));

        assertEquals(result, expected);

        //Rapidly change the foreign key, to validate that the hashing prevents incorrect results from being output,
        //and that eventually the correct value is output.
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

        IntegrationTestUtils.produceKeyValuesSynchronously(LEFT_TABLE, table1ForeignKeyChange, LEFT_PROD_CONF, MOCK_TIME);
        final List<KeyValue<Integer, String>> resultTwo = IntegrationTestUtils.readKeyValues(OUTPUT, CONSUMER_CONFIG, 15 * 1000L, Integer.MAX_VALUE);

        final List<KeyValue<Integer, String>> expectedTwo = new LinkedList<>();
        expectedTwo.add(new KeyValue<>(3, "value1=1.11,value2=10"));

        assertArrayEquals(resultTwo.toArray(), expectedTwo.toArray());

        //Ensure the state stores have the correct values within:
        final Set<KeyValue<Integer, String>> expMatResults = new HashSet<>();
        expMatResults.addAll(expected);
        expMatResults.addAll(expectedTwo);
        validateQueryableStoresContainExpectedKeyValues(expMatResults, currentMethodName);
    }

    private void createAndStartStreamsApplication(final String queryableStoreName, final boolean leftJoin) {
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-ktable-joinOnForeignKey-" + queryableStoreName);
        streams = prepareTopology(queryableStoreName, leftJoin);
        streamsTwo = prepareTopology(queryableStoreName, leftJoin);
        streamsThree = prepareTopology(queryableStoreName, leftJoin);
        streams.start();
        streamsTwo.start();
        streamsThree.start();
    }

    // These are hardwired into the test logic for readability sake.
    // Do not change unless you want to change all the test results as well.
    private ValueJoiner<Float, Long, String> joiner = (value1, value2) -> "value1=" + value1 + ",value2=" + value2;
    //Do not change. See above comment.
    private Function<Float, String> tableOneKeyExtractor = value -> Integer.toString((int) value.floatValue());

    private void validateQueryableStoresContainExpectedKeyValues(final Set<KeyValue<Integer, String>> expectedResult,
                                                                 final String queryableStoreName) {
        final ReadOnlyKeyValueStore<Integer, String> myJoinStoreOne = streams.store(queryableStoreName,
                QueryableStoreTypes.keyValueStore());

        final ReadOnlyKeyValueStore<Integer, String> myJoinStoreTwo = streamsTwo.store(queryableStoreName,
                QueryableStoreTypes.keyValueStore());

        final ReadOnlyKeyValueStore<Integer, String> myJoinStoreThree = streamsThree.store(queryableStoreName,
                QueryableStoreTypes.keyValueStore());

        // store only keeps last set of values, not entire stream of value changes
        final Map<Integer, String> expectedInStore = new HashMap<>();
        for (final KeyValue<Integer, String> expected : expectedResult) {
            expectedInStore.put(expected.key, expected.value);
        }

        // depending on partition assignment, the values will be in one of the three stream clients.
        for (final Map.Entry<Integer, String> expected : expectedInStore.entrySet()) {
            final String one = myJoinStoreOne.get(expected.getKey());
            final String two = myJoinStoreTwo.get(expected.getKey());
            final String three = myJoinStoreThree.get(expected.getKey());

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

        //Merge all the iterators together to ensure that their sum equals the total set of expected elements.
        final KeyValueIterator<Integer, String> allOne = myJoinStoreOne.all();
        final KeyValueIterator<Integer, String> allTwo = myJoinStoreTwo.all();
        final KeyValueIterator<Integer, String> allThree = myJoinStoreThree.all();

        final List<KeyValue<Integer, String>> all = new LinkedList<>();

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

        for (final KeyValue<Integer, String> elem : all) {
            assertTrue(expectedResult.contains(elem));
        }
    }

    private KafkaStreams prepareTopology(final String queryableStoreName, final boolean leftJoin) {
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<Integer, Float> left = builder.table(LEFT_TABLE, Consumed.with(Serdes.Integer(), Serdes.Float()));
        final KTable<String, Long> right = builder.table(RIGHT_TABLE, Consumed.with(Serdes.String(), Serdes.Long()));

        final Materialized<Integer, String, KeyValueStore<Bytes, byte[]>> materialized;
        if (queryableStoreName != null) {
            materialized = Materialized.<Integer, String, KeyValueStore<Bytes, byte[]>>as(queryableStoreName)
                    .withKeySerde(Serdes.Integer())
                    .withValueSerde(Serdes.String())
                    .withCachingDisabled();
        } else {
            throw new RuntimeException("Current implementation of join on foreign key requires a materialized store");
        }

        if (leftJoin)
            left.leftJoin(right, tableOneKeyExtractor, joiner, Named.as("customName"), materialized)
                .toStream()
                .to(OUTPUT, Produced.with(Serdes.Integer(), Serdes.String()));
        else
            left.join(right, tableOneKeyExtractor, joiner, materialized)
                .toStream()
                .to(OUTPUT, Produced.with(Serdes.Integer(), Serdes.String()));

        final Topology topology = builder.build(streamsConfig);

        return new KafkaStreams(topology, streamsConfig);
    }
}
