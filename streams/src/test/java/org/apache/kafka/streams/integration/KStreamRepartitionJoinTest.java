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
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.test.MockKeyValueMapper;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@RunWith(Parameterized.class)
public class KStreamRepartitionJoinTest {
    private static final int NUM_BROKERS = 1;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final MockTime mockTime = CLUSTER.time;
    private static final long WINDOW_SIZE = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);

    private KStreamBuilder builder;
    private Properties streamsConfiguration;
    private KStream<Long, Integer> streamOne;
    private KStream<Integer, String> streamTwo;
    private KStream<Integer, String> streamFour;
    private ValueJoiner<Integer, String, String> valueJoiner;
    private KeyValueMapper<Long, Integer, KeyValue<Integer, Integer>> keyMapper;

    private final List<String>
        expectedStreamOneTwoJoin = Arrays.asList("1:A", "2:B", "3:C", "4:D", "5:E");
    private KafkaStreams kafkaStreams;
    private String streamOneInput;
    private String streamTwoInput;
    private String streamFourInput;
    private static volatile int testNo = 0;

    @Parameter
    public long cacheSizeBytes;

    //Single parameter, use Object[]
    @Parameters
    public static Object[] data() {
        return new Object[] {0, 10 * 1024 * 1024L};
    }

    @Before
    public void before() {
        testNo++;
        String applicationId = "kstream-repartition-join-test-" + testNo;
        builder = new KStreamBuilder();
        createTopics();
        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zKConnectString());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1);
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, cacheSizeBytes);

        streamOne = builder.stream(Serdes.Long(), Serdes.Integer(), streamOneInput);
        streamTwo = builder.stream(Serdes.Integer(), Serdes.String(), streamTwoInput);
        streamFour = builder.stream(Serdes.Integer(), Serdes.String(), streamFourInput);

        valueJoiner = new ValueJoiner<Integer, String, String>() {
            @Override
            public String apply(final Integer value1, final String value2) {
                return value1 + ":" + value2;
            }
        };

        keyMapper = MockKeyValueMapper.SelectValueKeyValueMapper();
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
        String mapOneStreamAndJoinOutput = "map-one-join-output-" + testNo;
        doJoin(streamOne.map(keyMapper), streamTwo, mapOneStreamAndJoinOutput);
        return new ExpectedOutputOnTopic(expectedStreamOneTwoJoin, mapOneStreamAndJoinOutput);
    }

    private ExpectedOutputOnTopic mapBothStreamsAndJoin() throws Exception {
        final KStream<Integer, Integer> map1 = streamOne.map(keyMapper);
        final KStream<Integer, String> map2 = streamTwo.map(MockKeyValueMapper.<Integer, String>NoOpKeyValueMapper());

        doJoin(map1, map2, "map-both-streams-and-join-" + testNo);
        return new ExpectedOutputOnTopic(expectedStreamOneTwoJoin, "map-both-streams-and-join-" + testNo);
    }


    private ExpectedOutputOnTopic mapMapJoin() throws Exception {
        final KStream<Integer, Integer> mapMapStream = streamOne.map(
            new KeyValueMapper<Long, Integer, KeyValue<Long, Integer>>() {
                @Override
                public KeyValue<Long, Integer> apply(final Long key, final Integer value) {
                    if (value == null) {
                        return new KeyValue<>(null, null);
                    }
                    return new KeyValue<>(key + value, value);
                }
            }).map(keyMapper);

        final String outputTopic = "map-map-join-" + testNo;
        doJoin(mapMapStream, streamTwo, outputTopic);
        return new ExpectedOutputOnTopic(expectedStreamOneTwoJoin, outputTopic);
    }


    public ExpectedOutputOnTopic selectKeyAndJoin() throws ExecutionException, InterruptedException {

        final KStream<Integer, Integer> keySelected =
            streamOne.selectKey(MockKeyValueMapper.<Long, Integer>SelectValueMapper());

        final String outputTopic = "select-key-join-" + testNo;
        doJoin(keySelected, streamTwo, outputTopic);
        return new ExpectedOutputOnTopic(expectedStreamOneTwoJoin, outputTopic);
    }


    private ExpectedOutputOnTopic flatMapJoin() throws Exception {
        final KStream<Integer, Integer> flatMapped = streamOne.flatMap(
            new KeyValueMapper<Long, Integer, Iterable<KeyValue<Integer, Integer>>>() {
                @Override
                public Iterable<KeyValue<Integer, Integer>> apply(final Long key, final Integer value) {
                    return Collections.singletonList(new KeyValue<>(value, value));
                }
            });

        final String outputTopic = "flat-map-join-" + testNo;
        doJoin(flatMapped, streamTwo, outputTopic);

        return new ExpectedOutputOnTopic(expectedStreamOneTwoJoin, outputTopic);
    }

    private ExpectedOutputOnTopic joinMappedRhsStream() throws Exception {

        final ValueJoiner<String, Integer, String> joiner = new ValueJoiner<String, Integer, String>() {
            @Override
            public String apply(final String value1, final Integer value2) {
                return value1 + ":" + value2;
            }
        };

        final String output = "join-rhs-stream-mapped-" + testNo;
        CLUSTER.createTopic(output);
        streamTwo
            .join(streamOne.map(keyMapper),
                joiner,
                getJoinWindow(),
                Serdes.Integer(),
                Serdes.String(),
                Serdes.Integer())
            .to(Serdes.Integer(), Serdes.String(), output);

        return new ExpectedOutputOnTopic(Arrays.asList("A:1", "B:2", "C:3", "D:4", "E:5"), output);
    }

    public ExpectedOutputOnTopic mapBothStreamsAndLeftJoin() throws Exception {
        final KStream<Integer, Integer> map1 = streamOne.map(keyMapper);

        final KStream<Integer, String> map2 = streamTwo.map(MockKeyValueMapper.<Integer, String>NoOpKeyValueMapper());


        final String outputTopic = "left-join-" + testNo;
        CLUSTER.createTopic(outputTopic);
        map1.leftJoin(map2,
            valueJoiner,
            getJoinWindow(),
            Serdes.Integer(),
            Serdes.Integer(),
            Serdes.String())
            .to(Serdes.Integer(), Serdes.String(), outputTopic);

        return new ExpectedOutputOnTopic(expectedStreamOneTwoJoin, outputTopic);
    }

    private ExpectedOutputOnTopic joinTwoMappedStreamsOneThatHasBeenPreviouslyJoined() throws Exception {
        final KStream<Integer, Integer> map1 = streamOne.map(keyMapper);

        final KeyValueMapper<Integer, String, KeyValue<Integer, String>>
            kvMapper = MockKeyValueMapper.NoOpKeyValueMapper();

        final KStream<Integer, String> map2 = streamTwo.map(kvMapper);

        final KStream<Integer, String> join = map1.join(map2,
            valueJoiner,
            getJoinWindow(),
            Serdes.Integer(),
            Serdes.Integer(),
            Serdes.String());

        final ValueJoiner<String, String, String> joiner = new ValueJoiner<String, String, String>() {
            @Override
            public String apply(final String value1, final String value2) {
                return value1 + ":" + value2;
            }
        };

        final String topic = "map-join-join-" + testNo;
        CLUSTER.createTopic(topic);
        join.map(kvMapper)
            .join(streamFour.map(kvMapper),
                joiner,
                getJoinWindow(),
                Serdes.Integer(),
                Serdes.String(),
                Serdes.String())
            .to(Serdes.Integer(), Serdes.String(), topic);


        return new ExpectedOutputOnTopic(Arrays.asList("1:A:A", "2:B:B", "3:C:C", "4:D:D", "5:E:E"), topic);
    }

    private JoinWindows getJoinWindow() {
        return (JoinWindows) JoinWindows.of(WINDOW_SIZE).until(3 * WINDOW_SIZE);
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

    private void verifyLeftJoin(final ExpectedOutputOnTopic expectedOutputOnTopic)
        throws InterruptedException, ExecutionException {
        final List<String> received = receiveMessages(new StringDeserializer(), expectedOutputOnTopic
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
                new Properties()),
            mockTime);
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
                new Properties()),
            mockTime);
    }

    private void createTopics() {
        streamOneInput = "stream-one-" + testNo;
        streamTwoInput = "stream-two-" + testNo;
        streamFourInput = "stream-four-" + testNo;
        CLUSTER.createTopic(streamOneInput);
        CLUSTER.createTopic(streamTwoInput);
        CLUSTER.createTopic(streamFourInput);
    }


    private void startStreams() {
        kafkaStreams = new KafkaStreams(builder, streamsConfiguration);
        kafkaStreams.start();
    }


    private List<String> receiveMessages(final Deserializer<?> valueDeserializer,
                                         final int numMessages, final String topic) throws InterruptedException {

        final Properties config = new Properties();

        config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kstream-test");
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            IntegerDeserializer.class.getName());
        config.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            valueDeserializer.getClass().getName());
        final List<String> received = IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
            config,
            topic,
            numMessages,
            60 * 1000);
        Collections.sort(received);
        return received;
    }

    private void verifyCorrectOutput(final List<String> expectedMessages,
                                     final String topic) throws InterruptedException {
        assertThat(receiveMessages(new StringDeserializer(), expectedMessages.size(), topic),
            is(expectedMessages));
    }

    private void doJoin(final KStream<Integer, Integer> lhs,
                        final KStream<Integer, String> rhs,
                        final String outputTopic) {
        CLUSTER.createTopic(outputTopic);
        lhs.join(rhs,
            valueJoiner,
            getJoinWindow(),
            Serdes.Integer(),
            Serdes.Integer(),
            Serdes.String())
            .to(Serdes.Integer(), Serdes.String(), outputTopic);
    }

}
