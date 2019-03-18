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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreamsTest.MyStringSerde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.time.Duration.ofMillis;
import static org.junit.Assert.assertTrue;

/**
 * Similar to KStreamAggregationIntegrationTest but with dedupping enabled
 * by virtue of having a large commit interval
 */
@Category({IntegrationTest.class})
public class KStreamAggregationDedupIntegrationTest {
    private static final int NUM_BROKERS = 1;
    private static final long COMMIT_INTERVAL_MS = 300L;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER =
        new EmbeddedKafkaCluster(NUM_BROKERS);

    private final MockTime mockTime = CLUSTER.time;
    private static volatile int testNo = 0;
    private StreamsBuilder builder;
    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;
    private String streamOneInput;
    private String outputTopic;
    private KGroupedStream<String, String> groupedStream;
    private Reducer<String> reducer;
    private KStream<Integer, String> stream;

    @Before
    public void before() throws InterruptedException {
        testNo++;
        builder = new StreamsBuilder();
        createTopics();
        streamsConfiguration = new Properties();
        final String applicationId = "kgrouped-stream-test-" + testNo;
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL_MS);
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
        streamsConfiguration.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);

        final KeyValueMapper<Integer, String, String> mapper = MockMapper.selectValueMapper();
        stream = builder.stream(streamOneInput, Consumed.with(Serdes.Integer(), Serdes.String()));
        groupedStream = stream.groupBy(mapper, Grouped.with(Serdes.String(), Serdes.String()));

        reducer = (value1, value2) -> value1 + ":" + value2;
    }

    @After
    public void whenShuttingDown() throws IOException {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }


    @Test
    @SuppressWarnings("unchecked")
    public void shouldReduce() throws Exception {
        produceMessages(System.currentTimeMillis());
        final MyStringSerde keyTestSerde = new MyStringSerde();
        final MyStringSerde valueTestSerde = new MyStringSerde();
        groupedStream
                .reduce(reducer, Materialized.as("reduce-by-key"))
                .toStream()
                .to(outputTopic, Produced.with(keyTestSerde, valueTestSerde));

        startStreams();

        produceMessages(System.currentTimeMillis());

        assertTrue(keyTestSerde.configured());
        assertTrue(valueTestSerde.configured());
        validateReceivedMessages(
                new StringDeserializer(),
                new StringDeserializer(),
                Arrays.asList(
                        KeyValue.pair("A", "A:A"),
                        KeyValue.pair("B", "B:B"),
                        KeyValue.pair("C", "C:C"),
                        KeyValue.pair("D", "D:D"),
                        KeyValue.pair("E", "E:E")));
    }

    @Test
    public void shouldReduceWindowed() throws Exception {
        final long firstBatchTimestamp = System.currentTimeMillis() - 1000;
        produceMessages(firstBatchTimestamp);
        final long secondBatchTimestamp = System.currentTimeMillis();
        produceMessages(secondBatchTimestamp);
        produceMessages(secondBatchTimestamp);

        groupedStream
            .windowedBy(TimeWindows.of(ofMillis(500L)))
            .reduce(reducer, Materialized.as("reduce-time-windows"))
            .toStream((windowedKey, value) -> windowedKey.key() + "@" + windowedKey.window().start())
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        startStreams();

        final long firstBatchWindow = firstBatchTimestamp / 500 * 500;
        final long secondBatchWindow = secondBatchTimestamp / 500 * 500;

        validateReceivedMessages(
                new StringDeserializer(),
                new StringDeserializer(),
                Arrays.asList(
                        new KeyValue<>("A@" + firstBatchWindow, "A"),
                        new KeyValue<>("A@" + secondBatchWindow, "A:A"),
                        new KeyValue<>("B@" + firstBatchWindow, "B"),
                        new KeyValue<>("B@" + secondBatchWindow, "B:B"),
                        new KeyValue<>("C@" + firstBatchWindow, "C"),
                        new KeyValue<>("C@" + secondBatchWindow, "C:C"),
                        new KeyValue<>("D@" + firstBatchWindow, "D"),
                        new KeyValue<>("D@" + secondBatchWindow, "D:D"),
                        new KeyValue<>("E@" + firstBatchWindow, "E"),
                        new KeyValue<>("E@" + secondBatchWindow, "E:E")
                )
        );
    }

    static class MyIntSerializer extends IntegerSerializer {
        boolean configured = false;
        boolean called = false;

        MyIntSerializer() {
            super();
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            super.configure(configs, isKey);
            configured = true;
        }

        @Override
        public byte[] serialize(final String topic, final Integer data) {
            called = true;
            return super.serialize(topic, data);
        }

        boolean configured() {
            return !called || configured;
        }
    }
    static class MyIntDeserializer extends IntegerDeserializer {
        boolean configured = false;
        boolean called = false;

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            super.configure(configs, isKey);
            configured = true;
        }
        @Override
        public Integer deserialize(final String topic, final byte[] data) {
            called = true;
            return super.deserialize(topic, data);
        }

        boolean configured() {
            return !called || configured;
        }
    }

    class MyIntegerSerde<K> extends Serdes.WrapperSerde<Integer> {
        public MyIntegerSerde() {
            super(new MyIntSerializer(), new MyIntDeserializer());
        }
        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            super.configure(configs, isKey);
        }

        public boolean configured() {
            if (!((MyIntSerializer) this.serializer()).configured()) return false;
            if (!((MyIntDeserializer) this.deserializer()).configured()) return false;
            return true;
        }
    }
    @Test
    @SuppressWarnings("unchecked")
    public void shouldGroupByKey() throws Exception {
        final long timestamp = mockTime.milliseconds();
        produceMessages(timestamp);
        produceMessages(timestamp);

        final MyIntegerSerde keyTestSerde = new MyIntegerSerde();
        final MyStringSerde valueTestSerde = new MyStringSerde();
        stream.groupByKey(Grouped.with(keyTestSerde, valueTestSerde))
            .windowedBy(TimeWindows.of(ofMillis(500L)))
            .count(Materialized.as("count-windows"))
            .toStream((windowedKey, value) -> ((Windowed<Integer>) windowedKey).key() + "@" +
                    ((Windowed<Integer>) windowedKey).window().start())
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        startStreams();

        final long window = timestamp / 500 * 500;

        validateReceivedMessages(
                new StringDeserializer(),
                new LongDeserializer(),
                Arrays.asList(
                        KeyValue.pair("1@" + window, 2L),
                        KeyValue.pair("2@" + window, 2L),
                        KeyValue.pair("3@" + window, 2L),
                        KeyValue.pair("4@" + window, 2L),
                        KeyValue.pair("5@" + window, 2L)
                )
        );
        assertTrue(keyTestSerde.configured());
        assertTrue(valueTestSerde.configured());
    }


    private void produceMessages(final long timestamp) throws Exception {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            streamOneInput,
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
            timestamp);
    }


    private void createTopics() throws InterruptedException {
        streamOneInput = "stream-one-" + testNo;
        outputTopic = "output-" + testNo;
        CLUSTER.createTopic(streamOneInput, 3, 1);
        CLUSTER.createTopic(outputTopic);
    }

    private void startStreams() {
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();
    }


    private <K, V> void validateReceivedMessages(final Deserializer<K> keyDeserializer,
                                                 final Deserializer<V> valueDeserializer,
                                                 final List<KeyValue<K, V>> expectedRecords)
        throws InterruptedException {
        final Properties consumerProperties = new Properties();
        consumerProperties
            .setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kgroupedstream-test-" +
            testNo);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            keyDeserializer.getClass().getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            valueDeserializer.getClass().getName());

        IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(
            consumerProperties,
            outputTopic,
            expectedRecords);
    }

}
