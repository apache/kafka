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
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@Category({IntegrationTest.class})
public class KTableKTableJoinIntegrationTest {
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
    private final static Properties CONSUMER_CONFIG = new Properties();

    @BeforeClass
    public static void beforeTest() throws Exception {
        CLUSTER.createTopics(TABLE_1, TABLE_2, TABLE_3, OUTPUT);

        streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfig.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        final List<KeyValue<String, String>> table1 = Arrays.asList(
            new KeyValue<>("a", "A1"),
            new KeyValue<>("b", "B1")
        );

        final List<KeyValue<String, String>> table2 = Arrays.asList(
            new KeyValue<>("b", "B2"),
            new KeyValue<>("c", "C2")
        );

        final List<KeyValue<String, String>> table3 = Arrays.asList(
            new KeyValue<>("a", "A3"),
            new KeyValue<>("b", "B3"),
            new KeyValue<>("c", "C3")
        );

        // put table 3 first, to make sure data is there when joining T1 with T2
        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_3, table3, producerConfig, MOCK_TIME);
        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_1, table1, producerConfig, MOCK_TIME);
        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_2, table2, producerConfig, MOCK_TIME);

        CONSUMER_CONFIG.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        CONSUMER_CONFIG.put(ConsumerConfig.GROUP_ID_CONFIG, "ktable-ktable-consumer");
        CONSUMER_CONFIG.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
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
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfig);
    }

    private enum JoinType {
        INNER, LEFT, OUTER
    }


    @Test
    public void shouldInnerInnerJoin() throws InterruptedException {
        verifyKTableKTableJoin(JoinType.INNER, JoinType.INNER, Collections.singletonList(new KeyValue<>("b", "B1-B2-B3")), false);
    }

    @Test
    public void shouldInnerInnerJoinQueryable() throws InterruptedException {
        verifyKTableKTableJoin(JoinType.INNER, JoinType.INNER, Collections.singletonList(new KeyValue<>("b", "B1-B2-B3")), true);
    }

    @Test
    public void shouldInnerLeftJoin() throws InterruptedException {
        verifyKTableKTableJoin(JoinType.INNER, JoinType.LEFT, Collections.singletonList(new KeyValue<>("b", "B1-B2-B3")), false);
    }

    @Test
    public void shouldInnerLeftJoinQueryable() throws InterruptedException {
        verifyKTableKTableJoin(JoinType.INNER, JoinType.LEFT, Collections.singletonList(new KeyValue<>("b", "B1-B2-B3")), true);
    }

    @Test
    public void shouldInnerOuterJoin() throws InterruptedException {
        verifyKTableKTableJoin(JoinType.INNER, JoinType.OUTER, Arrays.asList(
                new KeyValue<>("a", "null-A3"),
                new KeyValue<>("b", "null-B3"),
                new KeyValue<>("c", "null-C3"),
                new KeyValue<>("b", "B1-B2-B3")), false);
    }

    @Test
    public void shouldInnerOuterJoinQueryable() throws InterruptedException {
        verifyKTableKTableJoin(JoinType.INNER, JoinType.OUTER, Arrays.asList(
            new KeyValue<>("a", "null-A3"),
            new KeyValue<>("b", "null-B3"),
            new KeyValue<>("c", "null-C3"),
            new KeyValue<>("b", "B1-B2-B3")), true);
    }

    @Test
    public void shouldLeftInnerJoin() throws InterruptedException {
        verifyKTableKTableJoin(JoinType.LEFT, JoinType.INNER, Arrays.asList(
                new KeyValue<>("a", "A1-null-A3"),
                new KeyValue<>("b", "B1-null-B3"),
                new KeyValue<>("b", "B1-B2-B3")), false);
    }

    @Test
    public void shouldLeftInnerJoinQueryable() throws InterruptedException {
        verifyKTableKTableJoin(JoinType.LEFT, JoinType.INNER, Arrays.asList(
            new KeyValue<>("a", "A1-null-A3"),
            new KeyValue<>("b", "B1-null-B3"),
            new KeyValue<>("b", "B1-B2-B3")), true);
    }

    @Test
    public void shouldLeftLeftJoin() throws InterruptedException {
        verifyKTableKTableJoin(JoinType.LEFT, JoinType.LEFT, Arrays.asList(
                new KeyValue<>("a", "A1-null-A3"),
                new KeyValue<>("b", "B1-null-B3"),
                new KeyValue<>("b", "B1-B2-B3")), false);
    }

    @Test
    public void shouldLeftLeftJoinQueryable() throws InterruptedException {
        verifyKTableKTableJoin(JoinType.LEFT, JoinType.LEFT, Arrays.asList(
            new KeyValue<>("a", "A1-null-A3"),
            new KeyValue<>("b", "B1-null-B3"),
            new KeyValue<>("b", "B1-B2-B3")), true);
    }

    @Test
    public void shouldLeftOuterJoin() throws InterruptedException {
        verifyKTableKTableJoin(JoinType.LEFT, JoinType.OUTER, Arrays.asList(
                new KeyValue<>("a", "null-A3"),
                new KeyValue<>("b", "null-B3"),
                new KeyValue<>("c", "null-C3"),
                new KeyValue<>("a", "A1-null-A3"),
                new KeyValue<>("b", "B1-null-B3"),
                new KeyValue<>("b", "B1-B2-B3")), false);
    }

    @Test
    public void shouldLeftOuterJoinQueryable() throws InterruptedException {
        verifyKTableKTableJoin(JoinType.LEFT, JoinType.OUTER, Arrays.asList(
            new KeyValue<>("a", "null-A3"),
            new KeyValue<>("b", "null-B3"),
            new KeyValue<>("c", "null-C3"),
            new KeyValue<>("a", "A1-null-A3"),
            new KeyValue<>("b", "B1-null-B3"),
            new KeyValue<>("b", "B1-B2-B3")), true);
    }

    @Test
    public void shouldOuterInnerJoin() throws InterruptedException {
        verifyKTableKTableJoin(JoinType.OUTER, JoinType.INNER, Arrays.asList(
                new KeyValue<>("a", "A1-null-A3"),
                new KeyValue<>("b", "B1-null-B3"),
                new KeyValue<>("b", "B1-B2-B3"),
                new KeyValue<>("c", "null-C2-C3")), false);
    }

    @Test
    public void shouldOuterInnerJoinQueryable() throws InterruptedException {
        verifyKTableKTableJoin(JoinType.OUTER, JoinType.INNER, Arrays.asList(
            new KeyValue<>("a", "A1-null-A3"),
            new KeyValue<>("b", "B1-null-B3"),
            new KeyValue<>("b", "B1-B2-B3"),
            new KeyValue<>("c", "null-C2-C3")), true);
    }

    @Test
    public void shouldOuterLeftJoin() throws InterruptedException {
        verifyKTableKTableJoin(JoinType.OUTER, JoinType.LEFT,  Arrays.asList(
                new KeyValue<>("a", "A1-null-A3"),
                new KeyValue<>("b", "B1-null-B3"),
                new KeyValue<>("b", "B1-B2-B3"),
                new KeyValue<>("c", "null-C2-C3")), false);
    }

    @Test
    public void shouldOuterLeftJoinQueryable() throws InterruptedException {
        verifyKTableKTableJoin(JoinType.OUTER, JoinType.LEFT,  Arrays.asList(
            new KeyValue<>("a", "A1-null-A3"),
            new KeyValue<>("b", "B1-null-B3"),
            new KeyValue<>("b", "B1-B2-B3"),
            new KeyValue<>("c", "null-C2-C3")), true);
    }

    @Test
    public void shouldOuterOuterJoin() throws InterruptedException {
        verifyKTableKTableJoin(JoinType.OUTER, JoinType.OUTER, Arrays.asList(
                new KeyValue<>("a", "null-A3"),
                new KeyValue<>("b", "null-B3"),
                new KeyValue<>("c", "null-C3"),
                new KeyValue<>("a", "A1-null-A3"),
                new KeyValue<>("b", "B1-null-B3"),
                new KeyValue<>("b", "B1-B2-B3"),
                new KeyValue<>("c", "null-C2-C3")), false);
    }

    @Test
    public void shouldOuterOuterJoinQueryable() throws InterruptedException {
        verifyKTableKTableJoin(JoinType.OUTER, JoinType.OUTER, Arrays.asList(
            new KeyValue<>("a", "null-A3"),
            new KeyValue<>("b", "null-B3"),
            new KeyValue<>("c", "null-C3"),
            new KeyValue<>("a", "A1-null-A3"),
            new KeyValue<>("b", "B1-null-B3"),
            new KeyValue<>("b", "B1-B2-B3"),
            new KeyValue<>("c", "null-C2-C3")), true);
    }


    private void verifyKTableKTableJoin(final JoinType joinType1,
                                        final JoinType joinType2,
                                        final List<KeyValue<String, String>> expectedResult,
                                        boolean verifyQueryableState) throws InterruptedException {
        final String queryableName = verifyQueryableState ? joinType1 + "-" + joinType2 + "-ktable-ktable-join-query" : null;
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, joinType1 + "-" + joinType2 + "-ktable-ktable-join" + queryableName);

        streams = prepareTopology(joinType1, joinType2, queryableName);
        streams.start();

        final List<KeyValue<String, String>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                CONSUMER_CONFIG,
                OUTPUT,
                expectedResult.size());

        assertThat(result, equalTo(expectedResult));

        if (verifyQueryableState) {
            verifyKTableKTableJoinQueryableState(joinType1, joinType2, expectedResult);
        }
    }

    private void verifyKTableKTableJoinQueryableState(final JoinType joinType1,
                                                      final JoinType joinType2,
                                                      final List<KeyValue<String, String>> expectedResult) {
        final String queryableName = joinType1 + "-" + joinType2 + "-ktable-ktable-join-query";
        final ReadOnlyKeyValueStore<String, String> myJoinStore = streams.store(queryableName,
            QueryableStoreTypes.<String, String>keyValueStore());

        // store only keeps last set of values, not entire stream of value changes
        final Map<String, String> expectedInStore = new HashMap<>();
        for (KeyValue<String, String> expected : expectedResult) {
            expectedInStore.put(expected.key, expected.value);
        }

        for (Map.Entry<String, String> expected : expectedInStore.entrySet()) {
            assertEquals(expected.getValue(), myJoinStore.get(expected.getKey()));
        }
        final KeyValueIterator<String, String> all = myJoinStore.all();
        while (all.hasNext()) {
            KeyValue<String, String> storeEntry = all.next();
            assertTrue(expectedResult.contains(storeEntry));
        }
        all.close();

    }

    private KafkaStreams prepareTopology(final JoinType joinType1, final JoinType joinType2, final String queryableName) {
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String, String> table1 = builder.table(TABLE_1);
        final KTable<String, String> table2 = builder.table(TABLE_2);
        final KTable<String, String> table3 = builder.table(TABLE_3);

        Materialized<String, String, KeyValueStore<Bytes, byte[]>> materialized = null;
        if (queryableName != null) {
            materialized = Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(queryableName)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .withCachingDisabled();
        }
        join(join(table1, table2, joinType1, null /* no need to query intermediate result */), table3,
            joinType2, materialized).to(OUTPUT);

        return new KafkaStreams(builder.build(), new StreamsConfig(streamsConfig));
    }

    private KTable<String, String> join(final KTable<String, String> first,
                                        final KTable<String, String> second,
                                        final JoinType joinType,
                                        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> materialized) {
        final ValueJoiner<String, String, String> joiner = new ValueJoiner<String, String, String>() {
            @Override
            public String apply(final String value1, final String value2) {
                return value1 + "-" + value2;
            }
        };


        switch (joinType) {
            case INNER:
                if (materialized != null) {
                    return first.join(second, joiner, materialized);
                } else {
                    return first.join(second, joiner);
                }
            case LEFT:
                if (materialized != null) {
                    return first.leftJoin(second, joiner, materialized);
                } else {
                    return first.leftJoin(second, joiner);
                }
            case OUTER:
                if (materialized != null) {
                    return first.outerJoin(second, joiner, materialized);
                } else {
                    return first.outerJoin(second, joiner);
                }
        }

        throw new RuntimeException("Unknown join type.");
    }

}
