/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at <p> http://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class KStreamRepartitionJoinTest {

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER =
        new EmbeddedSingleNodeKafkaCluster();

    private static volatile int testNo = 0;

    private KStreamBuilder builder;
    private Properties streamsConfiguration;
    private KStream<Long, Integer> streamOne;
    private KStream<Integer, String> streamTwo;
    private ValueJoiner<Integer, String, String> valueJoiner;
    private KeyValueMapper<Long, Integer, KeyValue<Integer, Integer>>
        keyMapper;

    private final List<String>
        expectedStreamOneTwoJoin = Arrays.asList("1:A", "2:B", "3:C", "4:D", "5:E");
    private KafkaStreams kafkaStreams;
    private String streamOneInput;
    private String streamTwoInput;
    private String tableInput;
    private String outputTopic;
    private String streamThreeInput;
    private KStream<Integer, Integer> streamThree;
    private KTable<Integer, String> kTable;


    @Before
    public void before() {
        testNo++;
        builder = new KStreamBuilder();
        createTopics();
        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG,
                                 "kstream-repartition-join-test" + testNo);
        streamsConfiguration
            .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zKConnectString());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kstream-repartition-test");

        streamOne = builder.stream(Serdes.Long(), Serdes.Integer(), streamOneInput);
        streamTwo = builder.stream(Serdes.Integer(), Serdes.String(), streamTwoInput);
        streamThree = builder.stream(Serdes.Integer(), Serdes.Integer(), streamThreeInput);

        kTable = builder.table(Serdes.Integer(), Serdes.String(), tableInput);

        valueJoiner = new ValueJoiner<Integer, String, String>() {
            @Override
            public String apply(final Integer value1, final String value2) {
                return value1 + ":" + value2;
            }
        };

        keyMapper = new KeyValueMapper<Long, Integer, KeyValue<Integer, Integer>>() {
            @Override
            public KeyValue<Integer, Integer> apply(final Long key, final Integer value) {
                return new KeyValue<>(value, value);
            }
        };
    }

    @After
    public void whenShuttingDown() throws IOException {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @Test
    public void shouldMapLhsAndJoin() throws ExecutionException, InterruptedException {
        produceMessages();
        doJoin(streamOne.map(keyMapper), streamTwo);
        startStreams();
        verifyCorrectOutput(expectedStreamOneTwoJoin);
    }

    @Test
    public void shouldMapBothStreamsAndJoin() throws Exception {
        produceMessages();

        final KStream<Integer, Integer>
            map1 =
            streamOne.map(keyMapper);

        final KStream<Integer, String> map2 = streamTwo.map(
            new KeyValueMapper<Integer, String, KeyValue<Integer, String>>() {
                @Override
                public KeyValue<Integer, String> apply(Integer key,
                                                       String value) {
                    return new KeyValue<>(key, value);
                }
            });

        doJoin(map1, map2);
        startStreams();
        verifyCorrectOutput(expectedStreamOneTwoJoin);

    }

    @Test
    public void shouldMapMapJoin() throws Exception {
        produceMessages();

        final KStream<Integer, Integer> mapMapStream = streamOne.map(
            new KeyValueMapper<Long, Integer, KeyValue<Long, Integer>>() {
                @Override
                public KeyValue<Long, Integer> apply(Long key, Integer value) {
                    return new KeyValue<>(key + value, value);
                }
            }).map(keyMapper);

        doJoin(mapMapStream, streamTwo);
        startStreams();
        verifyCorrectOutput(expectedStreamOneTwoJoin);
    }


    @Test
    public void shouldSelectKeyAndJoin() throws ExecutionException, InterruptedException {
        produceMessages();

        final KStream<Integer, Integer>
            keySelected =
            streamOne.selectKey(new KeyValueMapper<Long, Integer, Integer>() {
                @Override
                public Integer apply(final Long key, final Integer value) {
                    return value;
                }
            });

        doJoin(keySelected, streamTwo);
        startStreams();
        verifyCorrectOutput(expectedStreamOneTwoJoin);
    }


    @Test
    public void shouldFlatMapJoin() throws Exception {
        produceMessages();

        final KStream<Integer, Integer> flatMapped = streamOne.flatMap(
            new KeyValueMapper<Long, Integer, Iterable<KeyValue<Integer, Integer>>>() {
                @Override
                public Iterable<KeyValue<Integer, Integer>> apply(Long key,
                                                                  Integer value) {
                    return Collections.singletonList(new KeyValue<>(value, value));
                }
            });

        doJoin(flatMapped, streamTwo);
        startStreams();
        verifyCorrectOutput(expectedStreamOneTwoJoin);
    }

    @Test
    public void shouldJoinTwoStreamsPartitionedTheSame() throws Exception {
        produceMessages();
        doJoin(streamThree, streamTwo);
        startStreams();
        verifyCorrectOutput(Arrays.asList("10:A", "20:B", "30:C", "40:D", "50:E"));
    }

    @Test
    public void shouldJoinWithRhsStreamMapped() throws Exception {
        produceMessages();

        ValueJoiner<String, Integer, String> joiner = new ValueJoiner<String, Integer, String>() {
            @Override
            public String apply(String value1, Integer value2) {
                return value1 + ":" + value2;
            }
        };
        streamTwo
            .join(streamOne.map(keyMapper),
                  joiner,
                  JoinWindows.of("the-join").within(60 * 1000),
                  Serdes.Integer(),
                  Serdes.String(),
                  Serdes.Integer())
            .to(Serdes.Integer(), Serdes.String(), outputTopic);

        startStreams();
        verifyCorrectOutput(Arrays.asList("A:1", "B:2", "C:3", "D:4", "E:5"));
    }

    @Test
    public void shouldLeftJoinTwoStreamsPartitionedTheSame() throws Exception {
        produceMessages();
        doLeftJoin(streamThree, streamTwo);
        startStreams();
        verifyCorrectOutput(Arrays.asList("10:A", "20:B", "30:C", "40:D", "50:E"));
    }

    @Test
    public void shouldMapLhsAndLeftJoin() throws ExecutionException, InterruptedException {
        produceMessages();
        doLeftJoin(streamOne.map(keyMapper), streamTwo);
        startStreams();
        verifyCorrectOutput(expectedStreamOneTwoJoin);
    }

    @Test
    public void shouldMapBothStreamsAndLeftJoin() throws Exception {
        produceMessages();

        final KStream<Integer, Integer>
            map1 =
            streamOne.map(keyMapper);

        final KStream<Integer, String> map2 = streamTwo.map(
            new KeyValueMapper<Integer, String, KeyValue<Integer, String>>() {
                @Override
                public KeyValue<Integer, String> apply(Integer key,
                                                       String value) {
                    return new KeyValue<>(key, value);
                }
            });

        doLeftJoin(map1, map2);
        startStreams();

        List<String> received = receiveMessages(new StringDeserializer(), 5);

        if (!received.equals(expectedStreamOneTwoJoin)) {
            produceToStreamOne();
            verifyCorrectOutput(expectedStreamOneTwoJoin);
        }

    }

    @Test
    public void shouldLeftJoinWithRhsStreamMapped() throws Exception {
        produceMessages();

        ValueJoiner<String, Integer, String> joiner = new ValueJoiner<String, Integer, String>() {
            @Override
            public String apply(String value1, Integer value2) {
                return value1 + ":" + value2;
            }
        };
        streamTwo
            .leftJoin(streamOne.map(keyMapper),
                      joiner,
                      JoinWindows.of("the-join").within(60 * 1000),
                      Serdes.Integer(),
                      null,
                      Serdes.Integer())
            .to(Serdes.Integer(), Serdes.String(), outputTopic);

        startStreams();
        List<String> received = receiveMessages(new StringDeserializer(), 5);

        List<String> expectedMessages = Arrays.asList("A:1", "B:2", "C:3", "D:4", "E:5");
        if (!received.equals(expectedMessages)) {
            produceStreamTwoInputTo(streamTwoInput);
            verifyCorrectOutput(expectedMessages);
        }
    }

    @Test
    public void shouldLeftJoinWithKTableAfterMap() throws Exception {
        produceMessages();
        streamOne.map(keyMapper)
            .leftJoin(kTable, valueJoiner, Serdes.Integer(), Serdes.Integer())
            .to(Serdes.Integer(), Serdes.String(), outputTopic);

        startStreams();

        List<String> received = receiveMessages(new StringDeserializer(), 5);
        assertThat(received, is(expectedStreamOneTwoJoin));
    }

    @Test
    public void shouldLeftJoinWithTableProducedFromGroupBy() throws Exception {
        produceMessages();
        KTable<Integer, String> aggTable =
            streamOne.map(keyMapper)
                .groupByKey(Serdes.Integer(), Serdes.Integer())
                .aggregate(new Initializer<String>() {
                    @Override
                    public String apply() {
                        return "";
                    }
                }, new Aggregator<Integer, Integer, String>() {
                    @Override
                    public String apply(final Integer aggKey, final Integer value,
                                        final String aggregate) {
                        return aggregate + ":" + value;
                    }
                }, Serdes.String(), "agg-by-key");

        streamTwo.leftJoin(aggTable, new ValueJoiner<String, String, String>() {
            @Override
            public String apply(final String value1, final String value2) {
                return value1 + "@" + value2;
            }
        }, Serdes.Integer(), Serdes.String())
            .to(Serdes.Integer(), Serdes.String(), outputTopic);

        startStreams();

        receiveMessages(new StringDeserializer(), 5);
        produceStreamTwoInputTo(streamTwoInput);
        List<String> received = receiveMessages(new StringDeserializer(), 5);

        assertThat(received, is(Arrays.asList("A@:1", "B@:2", "C@:3", "D@:4", "E@:5")));

    }

    private void produceMessages()
        throws ExecutionException, InterruptedException {
        produceToStreamOne();
        produceStreamTwoInputTo(streamTwoInput);
        produceToStreamThree();
        produceStreamTwoInputTo(tableInput);
    }

    private void produceToStreamThree()
        throws ExecutionException, InterruptedException {
        IntegrationTestUtils.produceKeyValuesSynchronously(
            streamThreeInput,
            Arrays.asList(
                new KeyValue<>(1, 10),
                new KeyValue<>(2, 20),
                new KeyValue<>(3, 30),
                new KeyValue<>(4, 40),
                new KeyValue<>(5, 50)),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                IntegerSerializer.class,
                new Properties()));
    }

    private void produceStreamTwoInputTo(final String streamTwoInput)
        throws ExecutionException, InterruptedException {
        IntegrationTestUtils.produceKeyValuesSynchronously(
            streamTwoInput,
            Arrays.asList(
                new KeyValue<>(1, "A"),
                new KeyValue<>(2, "B"),
                new KeyValue<>(3, "C"),
                new KeyValue<>(4, "D"),
                new KeyValue<>(5, "E")),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                StringSerializer.class,
                new Properties()));
    }

    private void produceToStreamOne()
        throws ExecutionException, InterruptedException {
        IntegrationTestUtils.produceKeyValuesSynchronously(
            streamOneInput,
            Arrays.asList(
                new KeyValue<>(10L, 1),
                new KeyValue<>(5L, 2),
                new KeyValue<>(12L, 3),
                new KeyValue<>(15L, 4),
                new KeyValue<>(20L, 5)),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                LongSerializer.class,
                IntegerSerializer.class,
                new Properties()));
    }

    private void createTopics() {
        streamOneInput = "stream-one-" + testNo;
        streamTwoInput = "stream-two-" + testNo;
        streamThreeInput = "stream-three-" + testNo;
        tableInput = "table-stream-two-" + testNo;
        outputTopic = "output-" + testNo;
        CLUSTER.createTopic(streamOneInput, 2, 1);
        CLUSTER.createTopic(streamTwoInput, 2, 1);
        CLUSTER.createTopic(streamThreeInput, 2, 1);
        CLUSTER.createTopic(tableInput, 2, 1);
        CLUSTER.createTopic(outputTopic);
    }


    private void startStreams() {
        kafkaStreams = new KafkaStreams(builder, streamsConfiguration);
        kafkaStreams.start();
    }


    private List<String> receiveMessages(final Deserializer<?> valueDeserializer,
                                         final int numMessages) throws InterruptedException {

        final Properties config = new Properties();

        config
            .setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kstream-test-" + testNo);
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                                       IntegerDeserializer.class.getName());
        config.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                       valueDeserializer.getClass().getName());
        List<String> received = IntegrationTestUtils.waitUntilMinValuesRecordsReceived(config,
                                                                                      outputTopic,
                                                                                      numMessages,
                                                                                      5 * 60 *
                                                                                      1000);
        Collections.sort(received);
        return received;
    }

    private void verifyCorrectOutput(List<String> expectedMessages) throws InterruptedException {
        assertThat(receiveMessages(new StringDeserializer(), expectedMessages.size()),
                   is(expectedMessages));
    }

    private void doJoin(KStream<Integer, Integer> lhs,
                        KStream<Integer, String> rhs) {
        lhs.join(rhs,
                 valueJoiner,
                 JoinWindows.of("the-join").within(60 * 1000),
                 Serdes.Integer(),
                 Serdes.Integer(),
                 Serdes.String())
            .to(Serdes.Integer(), Serdes.String(), outputTopic);
    }

    private void doLeftJoin(KStream<Integer, Integer> lhs,
                            KStream<Integer, String> rhs) {
        lhs.leftJoin(rhs,
                     valueJoiner,
                     JoinWindows.of("the-join").within(60 * 1000),
                     Serdes.Integer(),
                     Serdes.Integer(),
                     Serdes.String())
            .to(Serdes.Integer(), Serdes.String(), outputTopic);
    }

}
