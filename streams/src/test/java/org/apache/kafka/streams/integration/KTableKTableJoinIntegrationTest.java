/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.kafka.streams.integration;

import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
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

    @Parameterized.Parameter(value = 0)
    public JoinType joinType1;
    @Parameterized.Parameter(value = 1)
    public JoinType joinType2;
    @Parameterized.Parameter(value = 2)
    public List<KeyValue<String, String>> expectedResult;

    //Single parameter, use Object[]
    @Parameterized.Parameters
    public static Object[] parameters() {
        return new Object[][]{
            {JoinType.INNER, JoinType.INNER, Arrays.asList(
                new KeyValue<>("b", "B1-B2-B3")//,
            )},
            {JoinType.INNER, JoinType.LEFT, Arrays.asList(
                new KeyValue<>("b", "B1-B2-B3")//,
            )},
            {JoinType.INNER, JoinType.OUTER, Arrays.asList(
                new KeyValue<>("a", "null-A3"),
                new KeyValue<>("b", "null-B3"),
                new KeyValue<>("c", "null-C3"),
                new KeyValue<>("b", "B1-B2-B3")//,
            )},
            {JoinType.LEFT, JoinType.INNER, Arrays.asList(
                new KeyValue<>("a", "A1-null-A3"),
                new KeyValue<>("b", "B1-null-B3"),
                new KeyValue<>("b", "B1-B2-B3")//,
            )},
            {JoinType.LEFT, JoinType.LEFT, Arrays.asList(
                new KeyValue<>("a", "A1-null-A3"),
                new KeyValue<>("b", "B1-null-B3"),
                new KeyValue<>("b", "B1-B2-B3")//,
            )},
            {JoinType.LEFT, JoinType.OUTER, Arrays.asList(
                new KeyValue<>("a", "null-A3"),
                new KeyValue<>("b", "null-B3"),
                new KeyValue<>("c", "null-C3"),
                new KeyValue<>("a", "A1-null-A3"),
                new KeyValue<>("b", "B1-null-B3"),
                new KeyValue<>("b", "B1-B2-B3")//,
            )},
            {JoinType.OUTER, JoinType.INNER, Arrays.asList(
                new KeyValue<>("a", "A1-null-A3"),
                new KeyValue<>("b", "B1-null-B3"),
                new KeyValue<>("b", "B1-B2-B3"),
                new KeyValue<>("c", "null-C2-C3")
            )},
            {JoinType.OUTER, JoinType.LEFT, Arrays.asList(
                new KeyValue<>("a", "A1-null-A3"),
                new KeyValue<>("b", "B1-null-B3"),
                new KeyValue<>("b", "B1-B2-B3"),
                new KeyValue<>("c", "null-C2-C3")
            )},
            {JoinType.OUTER, JoinType.OUTER, Arrays.asList(
                new KeyValue<>("a", "null-A3"),
                new KeyValue<>("b", "null-B3"),
                new KeyValue<>("c", "null-C3"),
                new KeyValue<>("a", "A1-null-A3"),
                new KeyValue<>("b", "B1-null-B3"),
                new KeyValue<>("b", "B1-B2-B3"),
                new KeyValue<>("c", "null-C2-C3")
            )}
        };
    }

    @BeforeClass
    public static void beforeTest() throws Exception {
        CLUSTER.createTopic(TABLE_1);
        CLUSTER.createTopic(TABLE_2);
        CLUSTER.createTopic(TABLE_3);
        CLUSTER.createTopic(OUTPUT);

        streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfig.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

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
    public void before() throws Exception {
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfig);
    }

    @After
    public void after() throws Exception {
        if (streams != null) {
            streams.close();
            streams = null;
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfig);
    }

    private enum JoinType {
        INNER, LEFT, OUTER
    }

    private KafkaStreams prepareTopology() {
        final KStreamBuilder builder = new KStreamBuilder();

        final KTable<String, String> table1 = builder.table(TABLE_1, TABLE_1);
        final KTable<String, String> table2 = builder.table(TABLE_2, TABLE_2);
        final KTable<String, String> table3 = builder.table(TABLE_3, TABLE_3);

        join(join(table1, table2, joinType1), table3, joinType2).to(OUTPUT);

        return new KafkaStreams(builder, new StreamsConfig(streamsConfig));
    }

    private KTable<String, String> join(KTable<String, String> first, KTable<String, String> second, JoinType joinType) {
        final ValueJoiner<String, String, String> joiner = new ValueJoiner<String, String, String>() {
            @Override
            public String apply(final String value1, final String value2) {
                return value1 + "-" + value2;
            }
        };

        switch (joinType) {
            case INNER:
                return first.join(second, joiner);
            case LEFT:
                return first.leftJoin(second, joiner);
            case OUTER:
                return first.outerJoin(second, joiner);
        }

        throw new RuntimeException("Unknown join type.");
    }

    @Test
    public void KTableKTableJoin() throws Exception {
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, joinType1 + "-" + joinType2 + "-ktable-ktable-join");

        streams = prepareTopology();
        streams.start();


        final List<KeyValue<String, String>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            CONSUMER_CONFIG,
            OUTPUT,
            expectedResult.size());

        assertThat(result, equalTo(expectedResult));
    }

}
