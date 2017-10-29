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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * Tests all available joins of Kafka Streams DSL.
 */
@Category({IntegrationTest.class})
public class JoinIntegrationTest {
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    private static final String APP_ID = "join-integration-test";
    private static final String INPUT_TOPIC_1 = "inputTopicLeft";
    private static final String INPUT_TOPIC_2 = "inputTopicRight";
    private static final String OUTPUT_TOPIC = "outputTopic";

    private final static Properties PRODUCER_CONFIG = new Properties();
    private final static Properties RESULT_CONSUMER_CONFIG = new Properties();
    private final static Properties STREAMS_CONFIG = new Properties();

    private StreamsBuilder builder;
    private KStream<Long, String> leftStream;
    private KStream<Long, String> rightStream;
    private KTable<Long, String> leftTable;
    private KTable<Long, String> rightTable;

    private final List<Input<String>> input = Arrays.asList(
        new Input<>(INPUT_TOPIC_1, (String) null),
        new Input<>(INPUT_TOPIC_2, (String) null),
        new Input<>(INPUT_TOPIC_1, "A"),
        new Input<>(INPUT_TOPIC_2, "a"),
        new Input<>(INPUT_TOPIC_1, "B"),
        new Input<>(INPUT_TOPIC_2, "b"),
        new Input<>(INPUT_TOPIC_1, (String) null),
        new Input<>(INPUT_TOPIC_2, (String) null),
        new Input<>(INPUT_TOPIC_1, "C"),
        new Input<>(INPUT_TOPIC_2, "c"),
        new Input<>(INPUT_TOPIC_2, (String) null),
        new Input<>(INPUT_TOPIC_1, (String) null),
        new Input<>(INPUT_TOPIC_2, (String) null),
        new Input<>(INPUT_TOPIC_2, "d"),
        new Input<>(INPUT_TOPIC_1, "D")
    );

    private final ValueJoiner<String, String, String> valueJoiner = new ValueJoiner<String, String, String>() {
        @Override
        public String apply(final String value1, final String value2) {
            return value1 + "-" + value2;
        }
    };

    @BeforeClass
    public static void setupConfigsAndUtils() {
        PRODUCER_CONFIG.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        PRODUCER_CONFIG.put(ProducerConfig.ACKS_CONFIG, "all");
        PRODUCER_CONFIG.put(ProducerConfig.RETRIES_CONFIG, 0);
        PRODUCER_CONFIG.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        PRODUCER_CONFIG.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        RESULT_CONSUMER_CONFIG.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        RESULT_CONSUMER_CONFIG.put(ConsumerConfig.GROUP_ID_CONFIG, APP_ID + "-result-consumer");
        RESULT_CONSUMER_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        RESULT_CONSUMER_CONFIG.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        RESULT_CONSUMER_CONFIG.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        STREAMS_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        STREAMS_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        STREAMS_CONFIG.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        STREAMS_CONFIG.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        STREAMS_CONFIG.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
        STREAMS_CONFIG.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    }

    @Before
    public void prepareTopology() throws InterruptedException {
        CLUSTER.createTopics(INPUT_TOPIC_1, INPUT_TOPIC_2, OUTPUT_TOPIC);

        builder = new StreamsBuilder();
        leftTable = builder.table(INPUT_TOPIC_1);
        rightTable = builder.table(INPUT_TOPIC_2);
        leftStream = leftTable.toStream();
        rightStream = rightTable.toStream();
    }

    @After
    public void cleanup() throws InterruptedException {
        CLUSTER.deleteTopicsAndWait(120000, INPUT_TOPIC_1, INPUT_TOPIC_2, OUTPUT_TOPIC);
    }

    private void checkResult(final String outputTopic, final List<String> expectedResult) throws InterruptedException {
        if (expectedResult != null) {
            final List<String> result = IntegrationTestUtils.waitUntilMinValuesRecordsReceived(RESULT_CONSUMER_CONFIG, outputTopic, expectedResult.size(), 30 * 1000L);
            assertThat(result, is(expectedResult));
        }
    }

    /*
     * Runs the actual test. Checks the result after each input record to ensure fixed processing order.
     * If an input tuple does not trigger any result, "expectedResult" should contain a "null" entry
     */
    private void runTest(final List<List<String>> expectedResult) throws Exception {
        assert expectedResult.size() == input.size();

        IntegrationTestUtils.purgeLocalStreamsState(STREAMS_CONFIG);
        final KafkaStreams streams = new KafkaStreams(builder.build(), STREAMS_CONFIG);
        try {
            streams.start();

            long ts = System.currentTimeMillis();

            final Iterator<List<String>> resultIterator = expectedResult.iterator();
            for (final Input<String> singleInput : input) {
                IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(singleInput.topic, Collections.singleton(singleInput.record), PRODUCER_CONFIG, ++ts);
                checkResult(OUTPUT_TOPIC, resultIterator.next());
            }
        } finally {
            streams.close();
        }
    }

    @Test
    public void testInnerKStreamKStream() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-KStream-KStream");

        final List<List<String>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            Collections.singletonList("A-a"),
            Collections.singletonList("B-a"),
            Arrays.asList("A-b", "B-b"),
            null,
            null,
            Arrays.asList("C-a", "C-b"),
            Arrays.asList("A-c", "B-c", "C-c"),
            null,
            null,
            null,
            Arrays.asList("A-d", "B-d", "C-d"),
            Arrays.asList("D-a", "D-b", "D-c", "D-d")
        );

        leftStream.join(rightStream, valueJoiner, JoinWindows.of(10000)).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testLeftKStreamKStream() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-left-KStream-KStream");

        final List<List<String>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList("A-null"),
            Collections.singletonList("A-a"),
            Collections.singletonList("B-a"),
            Arrays.asList("A-b", "B-b"),
            null,
            null,
            Arrays.asList("C-a", "C-b"),
            Arrays.asList("A-c", "B-c", "C-c"),
            null,
            null,
            null,
            Arrays.asList("A-d", "B-d", "C-d"),
            Arrays.asList("D-a", "D-b", "D-c", "D-d")
        );

        leftStream.leftJoin(rightStream, valueJoiner, JoinWindows.of(10000)).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testOuterKStreamKStream() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-outer-KStream-KStream");

        final List<List<String>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList("A-null"),
            Collections.singletonList("A-a"),
            Collections.singletonList("B-a"),
            Arrays.asList("A-b", "B-b"),
            null,
            null,
            Arrays.asList("C-a", "C-b"),
            Arrays.asList("A-c", "B-c", "C-c"),
            null,
            null,
            null,
            Arrays.asList("A-d", "B-d", "C-d"),
            Arrays.asList("D-a", "D-b", "D-c", "D-d")
        );

        leftStream.outerJoin(rightStream, valueJoiner, JoinWindows.of(10000)).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testInnerKStreamKTable() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-KStream-KTable");

        final List<List<String>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            null,
            Collections.singletonList("B-a"),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Collections.singletonList("D-d")
        );

        leftStream.join(rightTable, valueJoiner).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testLeftKStreamKTable() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-left-KStream-KTable");

        final List<List<String>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList("A-null"),
            null,
            Collections.singletonList("B-a"),
            null,
            null,
            null,
            Collections.singletonList("C-null"),
            null,
            null,
            null,
            null,
            null,
            Collections.singletonList("D-d")
        );

        leftStream.leftJoin(rightTable, valueJoiner).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testInnerKTableKTable() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-KTable-KTable");

        final List<List<String>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            Collections.singletonList("A-a"),
            Collections.singletonList("B-a"),
            Collections.singletonList("B-b"),
            Collections.singletonList((String) null),
            null,
            null,
            Collections.singletonList("C-c"),
            Collections.singletonList((String) null),
            null,
            null,
            null,
            Collections.singletonList("D-d")
        );

        leftTable.join(rightTable, valueJoiner).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testLeftKTableKTable() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-left-KTable-KTable");

        final List<List<String>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList("A-null"),
            Collections.singletonList("A-a"),
            Collections.singletonList("B-a"),
            Collections.singletonList("B-b"),
            Collections.singletonList((String) null),
            null,
            Collections.singletonList("C-null"),
            Collections.singletonList("C-c"),
            Collections.singletonList("C-null"),
            Collections.singletonList((String) null),
            null,
            null,
            Collections.singletonList("D-d")
        );

        leftTable.leftJoin(rightTable, valueJoiner).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testOuterKTableKTable() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-outer-KTable-KTable");

        final List<List<String>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList("A-null"),
            Collections.singletonList("A-a"),
            Collections.singletonList("B-a"),
            Collections.singletonList("B-b"),
            Collections.singletonList("null-b"),
            Collections.singletonList((String) null),
            Collections.singletonList("C-null"),
            Collections.singletonList("C-c"),
            Collections.singletonList("C-null"),
            Collections.singletonList((String) null),
            null,
            Collections.singletonList("null-d"),
            Collections.singletonList("D-d")
        );

        leftTable.outerJoin(rightTable, valueJoiner).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    private final class Input<V> {
        String topic;
        KeyValue<Long, V> record;

        private final long anyUniqueKey = 0L;

        Input(final String topic, final V value) {
            this.topic = topic;
            record = KeyValue.pair(anyUniqueKey, value);
        }
    }
}
