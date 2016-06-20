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
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.StateTestUtils;
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

    private KStreamBuilder builder;
    private Properties streamsConfiguration;
    private KStream<Long, Integer> streamOne;
    private KStream<Integer, String> streamTwo;
    private KStream<Integer, String> streamFour;
    private ValueJoiner<Integer, String, String> valueJoiner;
    private KeyValueMapper<Long, Integer, KeyValue<Integer, Integer>>
        keyMapper;

    private final List<String>
        expectedStreamOneTwoJoin = Arrays.asList("1:A", "2:B", "3:C", "4:D", "5:E");
    private KafkaStreams kafkaStreams;
    private String streamOneInput;
    private String streamTwoInput;
    private String streamFourInput;



    @Before
    public void before() {
        String applicationId = "kstream-repartition-join-test";
        builder = new KStreamBuilder();
        createTopics();
        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG,
                                 applicationId);
        streamsConfiguration
            .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zKConnectString());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, StateTestUtils.tempDir().getPath());
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);


        streamOne = builder.stream(Serdes.Long(), Serdes.Integer(), streamOneInput);
        streamTwo = builder.stream(Serdes.Integer(), Serdes.String(), streamTwoInput);
        streamFour = builder.stream(Serdes.Integer(), Serdes.String(), streamFourInput);

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
    public void shouldCorrectlyRepartitionOnJoinOperations() throws Exception {
        produceMessages();

        final ExpectedOutputOnTopic mapOne = mapStreamOneAndJoin();
        final ExpectedOutputOnTopic mapBoth = mapBothStreamsAndJoin();
        final ExpectedOutputOnTopic mapMapJoin = mapMapJoin();
        final ExpectedOutputOnTopic selectKeyJoin = selectKeyAndJoin();
        final ExpectedOutputOnTopic flatMapJoin = flatMapJoin();
        final ExpectedOutputOnTopic mapRhs = joinMappedRhsStream();
        final ExpectedOutputOnTopic mapJoinJoin = joinTwoMappedStreamsOneThatHasBeenPreviouslyJoined();
        final ExpectedOutputOnTopic leftJoin = mapBothStreamsAndLeftJoin();

        startStreams();

        verifyCorrectOutput(mapOne);
        verifyCorrectOutput(mapBoth);
        verifyCorrectOutput(mapMapJoin);
        verifyCorrectOutput(selectKeyJoin);
        verifyCorrectOutput(flatMapJoin);
        verifyCorrectOutput(mapRhs);
        verifyCorrectOutput(mapJoinJoin);
        verifyLeftJoin(leftJoin);
    }

    private ExpectedOutputOnTopic mapStreamOneAndJoin() {
        String mapOneStreamAndJoinOutput = "map-one-join-output";
        doJoin(streamOne.map(keyMapper), streamTwo, mapOneStreamAndJoinOutput, "map-one-join");
        return new ExpectedOutputOnTopic(expectedStreamOneTwoJoin, mapOneStreamAndJoinOutput);
    }

    private ExpectedOutputOnTopic mapBothStreamsAndJoin() throws Exception {

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

        doJoin(map1, map2, "map-both-streams-and-join", "map-both-join");
        return new ExpectedOutputOnTopic(expectedStreamOneTwoJoin, "map-both-streams-and-join");
    }


    private ExpectedOutputOnTopic mapMapJoin() throws Exception {
        final KStream<Integer, Integer> mapMapStream = streamOne.map(
            new KeyValueMapper<Long, Integer, KeyValue<Long, Integer>>() {
                @Override
                public KeyValue<Long, Integer> apply(Long key, Integer value) {
                    if (value == null) {
                        return new KeyValue<>(null, null);
                    }
                    return new KeyValue<>(key + value, value);
                }
            }).map(keyMapper);

        String outputTopic = "map-map-join";
        doJoin(mapMapStream, streamTwo, outputTopic, outputTopic);
        return new ExpectedOutputOnTopic(expectedStreamOneTwoJoin, outputTopic);
    }


    public ExpectedOutputOnTopic selectKeyAndJoin() throws ExecutionException, InterruptedException {

        final KStream<Integer, Integer>
            keySelected =
            streamOne.selectKey(new KeyValueMapper<Long, Integer, Integer>() {
                @Override
                public Integer apply(final Long key, final Integer value) {
                    return value;
                }
            });

        String outputTopic = "select-key-join";
        doJoin(keySelected, streamTwo, outputTopic, outputTopic);
        return new ExpectedOutputOnTopic(expectedStreamOneTwoJoin, outputTopic);
    }


    private ExpectedOutputOnTopic flatMapJoin() throws Exception {
        final KStream<Integer, Integer> flatMapped = streamOne.flatMap(
            new KeyValueMapper<Long, Integer, Iterable<KeyValue<Integer, Integer>>>() {
                @Override
                public Iterable<KeyValue<Integer, Integer>> apply(Long key,
                                                                  Integer value) {
                    return Collections.singletonList(new KeyValue<>(value, value));
                }
            });

        String outputTopic = "flat-map-join";
        doJoin(flatMapped, streamTwo, outputTopic, outputTopic);

        return new ExpectedOutputOnTopic(expectedStreamOneTwoJoin, outputTopic);
    }

    private ExpectedOutputOnTopic joinMappedRhsStream() throws Exception {

        ValueJoiner<String, Integer, String> joiner = new ValueJoiner<String, Integer, String>() {
            @Override
            public String apply(String value1, Integer value2) {
                return value1 + ":" + value2;
            }
        };
        String output = "join-rhs-stream-mapped";
        streamTwo
            .join(streamOne.map(keyMapper),
                  joiner,
                  JoinWindows.of(output).within(60 * 1000),
                  Serdes.Integer(),
                  Serdes.String(),
                  Serdes.Integer())
            .to(Serdes.Integer(), Serdes.String(), output);

        return new ExpectedOutputOnTopic(Arrays.asList("A:1", "B:2", "C:3", "D:4", "E:5"),
                            output);
    }

    public ExpectedOutputOnTopic mapBothStreamsAndLeftJoin() throws Exception {
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

        String outputTopic = "left-join";
        map1.leftJoin(map2,
                      valueJoiner,
                      JoinWindows.of("the-left-join").within(60 * 1000),
                      Serdes.Integer(),
                      Serdes.Integer(),
                      Serdes.String())
            .to(Serdes.Integer(), Serdes.String(), outputTopic);

        return new ExpectedOutputOnTopic(expectedStreamOneTwoJoin, outputTopic);
    }

    private ExpectedOutputOnTopic joinTwoMappedStreamsOneThatHasBeenPreviouslyJoined() throws
                                                                                   Exception {
        final KStream<Integer, Integer>
            map1 =
            streamOne.map(keyMapper);

        final KeyValueMapper<Integer, String, KeyValue<Integer, String>>
            kvMapper =
            new KeyValueMapper<Integer, String, KeyValue<Integer, String>>() {
                @Override
                public KeyValue<Integer, String> apply(Integer key,
                                                       String value) {
                    return new KeyValue<>(key, value);
                }
            };

        final KStream<Integer, String> map2 = streamTwo.map(kvMapper);

        final KStream<Integer, String> join = map1.join(map2,
                                                        valueJoiner,
                                                        JoinWindows.of("join-one")
                                                            .within(60 * 1000),
                                                        Serdes.Integer(),
                                                        Serdes.Integer(),
                                                        Serdes.String());

        ValueJoiner<String, String, String> joiner = new ValueJoiner<String, String, String>() {
            @Override
            public String apply(final String value1, final String value2) {
                return value1 + ":" + value2;
            }
        };
        String topic = "map-join-join";
        join.map(kvMapper)
            .join(streamFour.map(kvMapper),
                  joiner,
                  JoinWindows.of("the-other-join").within(60 * 1000),
                  Serdes.Integer(),
                  Serdes.String(),
                  Serdes.String())
            .to(Serdes.Integer(), Serdes.String(), topic);


        return new ExpectedOutputOnTopic(Arrays.asList("1:A:A", "2:B:B", "3:C:C", "4:D:D", "5:E:E"),
                            topic);
    }


    private class ExpectedOutputOnTopic {
        private final List<String> expectedOutput;
        private final String outputTopic;

        ExpectedOutputOnTopic(final List<String> expectedOutput, final String outputTopic) {
            this.expectedOutput = expectedOutput;
            this.outputTopic = outputTopic;
        }
    }


    private void verifyCorrectOutput(final ExpectedOutputOnTopic expectedOutputOnTopic)
        throws InterruptedException {
        assertThat(receiveMessages(new StringDeserializer(),
                                   expectedOutputOnTopic.expectedOutput.size(),
                                   expectedOutputOnTopic.outputTopic),
                   is(expectedOutputOnTopic.expectedOutput));
    }
    private void verifyLeftJoin(ExpectedOutputOnTopic expectedOutputOnTopic)
        throws InterruptedException, ExecutionException {
        List<String> received = receiveMessages(new StringDeserializer(), expectedOutputOnTopic
            .expectedOutput.size(), expectedOutputOnTopic.outputTopic);
        if (!received.equals(expectedOutputOnTopic.expectedOutput)) {
            produceToStreamOne();
            verifyCorrectOutput(expectedOutputOnTopic.expectedOutput, expectedOutputOnTopic.outputTopic);
        }
    }

    private void produceMessages()
        throws ExecutionException, InterruptedException {
        produceToStreamOne();
        produceStreamTwoInputTo(streamTwoInput);
        produceStreamTwoInputTo(streamFourInput);

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
                new KeyValue<>(20L, 5),
                new KeyValue<Long, Integer>(70L, null)), // nulls should be filtered
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                LongSerializer.class,
                IntegerSerializer.class,
                new Properties()));
    }

    private void createTopics() {
        streamOneInput = "stream-one";
        streamTwoInput = "stream-two";
        streamFourInput = "stream-four";
        CLUSTER.createTopic(streamOneInput);
        CLUSTER.createTopic(streamTwoInput, 2, 1);
        CLUSTER.createTopic(streamFourInput);
    }


    private void startStreams() {
        kafkaStreams = new KafkaStreams(builder, streamsConfiguration);
        kafkaStreams.start();
    }


    private List<String> receiveMessages(final Deserializer<?> valueDeserializer,
                                         final int numMessages, final String topic) throws InterruptedException {

        final Properties config = new Properties();

        config
            .setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kstream-test");
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                                       IntegerDeserializer.class.getName());
        config.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                       valueDeserializer.getClass().getName());
        List<String> received = IntegrationTestUtils.waitUntilMinValuesRecordsReceived(config,
                                                                                      topic,
                                                                                      numMessages,
                                                                                      60 *
                                                                                      1000);
        Collections.sort(received);
        return received;
    }

    private void verifyCorrectOutput(List<String> expectedMessages,
                                     final String topic) throws InterruptedException {
        assertThat(receiveMessages(new StringDeserializer(), expectedMessages.size(), topic),
                   is(expectedMessages));
    }

    private void doJoin(KStream<Integer, Integer> lhs,
                        KStream<Integer, String> rhs,
                        String outputTopic,
                        final String joinName) {
        CLUSTER.createTopic(outputTopic);
        lhs.join(rhs,
                 valueJoiner,
                 JoinWindows.of(joinName).within(60 * 1000),
                 Serdes.Integer(),
                 Serdes.Integer(),
                 Serdes.String())
            .to(Serdes.Integer(), Serdes.String(), outputTopic);
    }

}
