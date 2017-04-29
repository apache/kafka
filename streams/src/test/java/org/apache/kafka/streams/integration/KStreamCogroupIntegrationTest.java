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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({IntegrationTest.class})
public class KStreamCogroupIntegrationTest {
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);
    private static final String APP_ID = "cogroup-integration-test";
    private static final String COGROUP_STORE_NAME = "cogroup";
    private static final String TABLE_STORE_NAME = "table";
    private static final String INPUT_TOPIC_1 = "input-topic-1-";
    private static final String INPUT_TOPIC_2 = "input-topic-2-";
    private static final String INPUT_TOPIC_3 = "input-topic-3-";
    private static final String OUTPUT_TOPIC = "output-topic-";
    private static final AtomicInteger TEST_NUMBER = new AtomicInteger();
    private static final Initializer<String> INITIALIZER = new Initializer<String>() {
        @Override
        public String apply() {
            return "";
        }
    };
    private static final Aggregator<Long, String, String> AGGREGATOR_1 = new Aggregator<Long, String, String>() {
            @Override
            public String apply(Long key, String value, String aggregate) {
                return aggregate + "1" + value;
            }
        };
    private static final Aggregator<Long, String, String> AGGREGATOR_2 = new Aggregator<Long, String, String>() {
            @Override
            public String apply(Long key, String value, String aggregate) {
                return aggregate + "2" + value;
            }
        };
    private static final Aggregator<Long, String, String> AGGREGATOR_3 = new Aggregator<Long, String, String>() {
            @Override
            public String apply(Long key, String value, String aggregate) {
                return aggregate + "3" + value;
            }
        };
    private static final KeyValueMapper<Long, String, Long> GROUP_BY = new KeyValueMapper<Long, String, Long>() {
            @Override
            public Long apply(Long key, String value) {
                return key * 3;
            }
        };
    private static final ValueJoiner<String, String, String> JOINER = new ValueJoiner<String, String, String>() {
            @Override
            public String apply(String value1, String value2) {
                return value1 + "+" + value2;
            }
        };
    private static final Properties PRODUCER_CONFIG = new Properties();
    private static final Properties CONSUMER_CONFIG = new Properties();
    private static final Properties STREAMS_CONFIG = new Properties();
    private static final List<Input<Long, String>> INPUTS = Arrays.asList(
            new Input<>(INPUT_TOPIC_1, new KeyValue<>(1L, "a")),
            new Input<>(INPUT_TOPIC_2, new KeyValue<>(2L, "a")),
            new Input<>(INPUT_TOPIC_3, new KeyValue<>(1L, "a")),
            new Input<>(INPUT_TOPIC_1, new KeyValue<>(1L, "b")),
            new Input<>(INPUT_TOPIC_2, new KeyValue<>(2L, "b")),
            new Input<>(INPUT_TOPIC_3, new KeyValue<>(1L, "b")),
            new Input<>(INPUT_TOPIC_1, new KeyValue<>(2L, "c")),
            new Input<>(INPUT_TOPIC_2, new KeyValue<>(1L, "c")),
            new Input<>(INPUT_TOPIC_3, new KeyValue<>(2L, "c")),
            new Input<>(INPUT_TOPIC_1, new KeyValue<>(2L, "a")),
            new Input<>(INPUT_TOPIC_2, new KeyValue<>(1L, "a")),
            new Input<>(INPUT_TOPIC_3, new KeyValue<>(2L, "a")),
            new Input<>(INPUT_TOPIC_1, new KeyValue<>(2L, "b")),
            new Input<>(INPUT_TOPIC_2, new KeyValue<>(1L, "b")),
            new Input<>(INPUT_TOPIC_3, new KeyValue<>(2L, "b")),
            new Input<>(INPUT_TOPIC_1, new KeyValue<>(1L, "c")),
            new Input<>(INPUT_TOPIC_2, new KeyValue<>(2L, "c")),
            new Input<>(INPUT_TOPIC_3, new KeyValue<>(1L, "c"))
        );

    @BeforeClass
    public static void setupConfigs() throws Exception {
        PRODUCER_CONFIG.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        PRODUCER_CONFIG.put(ProducerConfig.ACKS_CONFIG, "all");
        PRODUCER_CONFIG.put(ProducerConfig.RETRIES_CONFIG, 0);
        PRODUCER_CONFIG.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        PRODUCER_CONFIG.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        CONSUMER_CONFIG.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        CONSUMER_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        CONSUMER_CONFIG.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        CONSUMER_CONFIG.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        STREAMS_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        STREAMS_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        STREAMS_CONFIG.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        STREAMS_CONFIG.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        STREAMS_CONFIG.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        STREAMS_CONFIG.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        STREAMS_CONFIG.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
    }

    @Test
    public void testCogroup() throws InterruptedException, ExecutionException {
        final int testNumber = TEST_NUMBER.getAndIncrement();
        CLUSTER.createTopic(INPUT_TOPIC_1 + testNumber, 2, 1);
        CLUSTER.createTopic(INPUT_TOPIC_2 + testNumber, 2, 1);
        CLUSTER.createTopic(INPUT_TOPIC_3 + testNumber, 2, 1);
        CLUSTER.createTopic(OUTPUT_TOPIC + testNumber);

        final KStreamBuilder builder = new KStreamBuilder();
        KGroupedStream<Long, String> stream1 = builder.<Long, String>stream(INPUT_TOPIC_1 + testNumber).groupByKey();
        KGroupedStream<Long, String> stream2 = builder.<Long, String>stream(INPUT_TOPIC_2 + testNumber).groupByKey();
        KGroupedStream<Long, String> stream3 = builder.<Long, String>stream(INPUT_TOPIC_3 + testNumber).groupByKey();
        stream1.cogroup(INITIALIZER, AGGREGATOR_1, null, COGROUP_STORE_NAME)
                .cogroup(stream2, AGGREGATOR_2)
                .cogroup(stream3, AGGREGATOR_3)
                .aggregate()
                .to(OUTPUT_TOPIC + testNumber);

        final KafkaStreams streams = new KafkaStreams(builder, streamsConfig(APP_ID + testNumber));
        
        final List<String> outputs = Arrays.asList(
                "1a", // Cogroup 1
                "2a", // Cogroup 2
                "1a3a", // Cogroup 1
                "1a3a1b", // Cogroup 1
                "2a2b", // Cogroup 2
                "1a3a1b3b", // Cogroup 1
                "2a2b1c", // Cogroup 2
                "1a3a1b3b2c", // Cogroup 1
                "2a2b1c3c", // Cogroup 2
                "2a2b1c3c1a", // Cogroup 2
                "1a3a1b3b2c2a", // Cogroup 1
                "2a2b1c3c1a3a", // Cogroup 2
                "2a2b1c3c1a3a1b", // Cogroup 2
                "1a3a1b3b2c2a2b", // Cogroup 1
                "2a2b1c3c1a3a1b3b", // Cogroup 2
                "1a3a1b3b2c2a2b1c", // Cogroup 1
                "2a2b1c3c1a3a1b3b2c", // Cogroup 2
                "1a3a1b3b2c2a2b1c3c" // Cogroup 1
            );

        try {
            streams.start();

            Iterator<String> outputIterator = outputs.iterator();
            for (final Input<Long, String> input : INPUTS) {
                IntegrationTestUtils.produceKeyValuesSynchronously(input.topic + testNumber, Collections.singleton(input.keyValue), producerConfig(), CLUSTER.time);
                List<KeyValue<Long, String>> output = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig("consumer-" + testNumber), OUTPUT_TOPIC + testNumber, 1);
                assertThat(output.get(0).value, equalTo(outputIterator.next()));
            }
        } finally {
            streams.close();
        }
    }

    @Test
    public void testCogroupRepartition() throws InterruptedException, ExecutionException {
        final int testNumber = TEST_NUMBER.getAndIncrement();
        CLUSTER.createTopic(INPUT_TOPIC_1 + testNumber, 2, 1);
        CLUSTER.createTopic(INPUT_TOPIC_2 + testNumber, 2, 1);
        CLUSTER.createTopic(INPUT_TOPIC_3 + testNumber, 2, 1);
        CLUSTER.createTopic(OUTPUT_TOPIC + testNumber);

        final KStreamBuilder builder = new KStreamBuilder();
        KGroupedStream<Long, String> stream1 = builder.<Long, String>stream(INPUT_TOPIC_1 + testNumber).groupBy(GROUP_BY);
        KGroupedStream<Long, String> stream2 = builder.<Long, String>stream(INPUT_TOPIC_2 + testNumber).groupBy(GROUP_BY);
        KGroupedStream<Long, String> stream3 = builder.<Long, String>stream(INPUT_TOPIC_3 + testNumber).groupBy(GROUP_BY);
        stream1.cogroup(INITIALIZER, AGGREGATOR_1, null, COGROUP_STORE_NAME)
                .cogroup(stream2, AGGREGATOR_2)
                .cogroup(stream3, AGGREGATOR_3)
                .aggregate()
                .to(OUTPUT_TOPIC + testNumber);

        final KafkaStreams streams = new KafkaStreams(builder, streamsConfig(APP_ID + testNumber));

        final List<String> outputs = Arrays.asList(
                "1a", // Cogroup 3
                "2a", // Cogroup 6
                "1a3a", // Cogroup 3
                "1a3a1b", // Cogroup 3
                "2a2b", // Cogroup 6
                "1a3a1b3b", // Cogroup 3
                "2a2b1c", // Cogroup 6
                "1a3a1b3b2c", // Cogroup 3
                "2a2b1c3c", // Cogroup 6
                "2a2b1c3c1a", // Cogroup 6
                "1a3a1b3b2c2a", // Cogroup 3
                "2a2b1c3c1a3a", // Cogroup 6
                "2a2b1c3c1a3a1b", // Cogroup 6
                "1a3a1b3b2c2a2b", // Cogroup 3
                "2a2b1c3c1a3a1b3b", // Cogroup 6
                "1a3a1b3b2c2a2b1c", // Cogroup 3
                "2a2b1c3c1a3a1b3b2c", // Cogroup 6
                "1a3a1b3b2c2a2b1c3c" // Cogroup 3
            );

        try {
            streams.start();

            Iterator<String> outputIterator = outputs.iterator();
            for (final Input<Long, String> input : INPUTS) {
                IntegrationTestUtils.produceKeyValuesSynchronously(input.topic + testNumber, Collections.singleton(input.keyValue), producerConfig(), CLUSTER.time);
                List<KeyValue<Long, String>> output = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig("consumer-" + testNumber), OUTPUT_TOPIC + testNumber, 1);
                assertThat(output.get(0).value, equalTo(outputIterator.next()));
            }
        } finally {
            streams.close();
        }
    }

    @Test
    public void testCogroupEnableSendingOldValuesAndView() throws InterruptedException, ExecutionException {
        final int testNumber = TEST_NUMBER.getAndIncrement();
        CLUSTER.createTopic(INPUT_TOPIC_1 + testNumber, 2, 1);
        CLUSTER.createTopic(INPUT_TOPIC_2 + testNumber, 2, 1);
        CLUSTER.createTopic(INPUT_TOPIC_3 + testNumber, 2, 1);
        CLUSTER.createTopic(OUTPUT_TOPIC + testNumber);

        final KStreamBuilder builder = new KStreamBuilder();
        KGroupedStream<Long, String> stream1 = builder.<Long, String>stream(INPUT_TOPIC_1 + testNumber).groupByKey();
        KGroupedStream<Long, String> stream2 = builder.<Long, String>stream(INPUT_TOPIC_2 + testNumber).groupByKey();
        KTable<Long, String> table = builder.table(INPUT_TOPIC_3 + testNumber, TABLE_STORE_NAME);
        stream1.cogroup(INITIALIZER, AGGREGATOR_1, null, COGROUP_STORE_NAME)
                .cogroup(stream2, AGGREGATOR_2)
                .aggregate()
                .outerJoin(table, JOINER)
                .to(OUTPUT_TOPIC + testNumber);

        final KafkaStreams streams = new KafkaStreams(builder, streamsConfig(APP_ID + testNumber));

        final List<String> outputs = Arrays.asList(
                "1a+null", // Cogroup 3
                "2a+null", // Cogroup 6
                "1a+a", // Cogroup 3
                "1a1b+a", // Cogroup 3
                "2a2b+null", // Cogroup 6
                "1a1b+b", // Cogroup 3
                "2a2b1c+null", // Cogroup 6
                "1a1b2c+b", // Cogroup 3
                "2a2b1c+c", // Cogroup 6
                "2a2b1c1a+c", // Cogroup 6
                "1a1b2c2a+b", // Cogroup 3
                "2a2b1c1a+a", // Cogroup 6
                "2a2b1c1a1b+a", // Cogroup 6
                "1a1b2c2a2b+b", // Cogroup 3
                "2a2b1c1a1b+b", // Cogroup 6
                "1a1b2c2a2b1c+b", // Cogroup 3
                "2a2b1c1a1b2c+b", // Cogroup 6
                "1a1b2c2a2b1c+c" // Cogroup 3
            );

        try {
            streams.start();

            Iterator<String> outputIterator = outputs.iterator();
            for (final Input<Long, String> input : INPUTS) {
                IntegrationTestUtils.produceKeyValuesSynchronously(input.topic + testNumber, Collections.singleton(input.keyValue), producerConfig(), CLUSTER.time);
                List<KeyValue<Long, String>> output = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig("consumer-" + testNumber), OUTPUT_TOPIC + testNumber, 1);
                assertThat(output.get(0).value, equalTo(outputIterator.next()));
            }
        } finally {
            streams.close();
        }
    }

    private static final Properties producerConfig() {
        return PRODUCER_CONFIG;
    }

    private static final Properties consumerConfig(final String groupId) {
        final Properties consumerConfig = new Properties();
        consumerConfig.putAll(CONSUMER_CONFIG);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return consumerConfig;
    }

    private static final Properties streamsConfig(final String applicationId) {
        final Properties streamsConfig = new Properties();
        streamsConfig.putAll(STREAMS_CONFIG);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        return streamsConfig;
    }

    private static final class Input<K, V> {
        String topic;
        KeyValue<K, V> keyValue;

        Input(final String topic, final KeyValue<K, V> keyValue) {
            this.topic = topic;
            this.keyValue = keyValue;
        }
    }
}
