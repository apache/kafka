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
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import static java.time.Duration.ofMillis;
import static org.junit.Assert.assertTrue;

/**
 * Tests SerDes are properly configured
 *
 * Similar to KStreamAggregationIntegrationTest but with dedupping enabled
 * by virtue of having a large commit interval
 */
@Category({IntegrationTest.class})
public class KStreamSerDesIntegrationTest {
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
    final MyIntegerSerde keyIntSerde = new MyIntegerSerde();
    final MyStringSerde valueStringSerde = new MyStringSerde();

    private static final String INPUT_TOPIC = "input";
    private static final String COUNT_TOPIC = "outputTopic_0";
    private static final String AGGREGATION_TOPIC = "outputTopic_1";
    private static final String REDUCE_TOPIC = "outputTopic_2";
    private static final String JOINED_TOPIC = "joinedOutputTopic";

    @Before
    @SuppressWarnings("unchecked")
    public void before() throws Exception {
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
        stream = builder.stream(streamOneInput, Consumed.with(keyIntSerde, valueStringSerde));
        groupedStream = stream.groupBy(mapper, Grouped.with(Serdes.String(), Serdes.String()));

        reducer = (value1, value2) -> value1 + ":" + value2;

        final Properties props = new Properties();
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 1024 * 10);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000);
        props.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);

        streamsConfiguration = StreamsTestUtils.getStreamsConfig(
                "maybe-optimized-test-app",
                CLUSTER.bootstrapServers(),
                Serdes.String().getClass().getName(),
                Serdes.String().getClass().getName(),
                props);
        CLUSTER.createTopics(INPUT_TOPIC,
                COUNT_TOPIC,
                AGGREGATION_TOPIC,
                REDUCE_TOPIC,
                JOINED_TOPIC);

        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @After
    public void whenShuttingDown() throws Exception {
        assertTrue(keyIntSerde.configured());
        assertTrue(valueStringSerde.configured());
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        CLUSTER.deleteAllTopicsAndWait(30_000L);
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    static class MyStringSerializer extends StringSerializer {
        boolean configured = false;
        boolean called = false;

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            super.configure(configs, isKey);
            configured = true;
        }

        @Override
        public byte[] serialize(final String topic, final String data) {
            called = true;
            return super.serialize(topic, data);
        }

        boolean configured() {
            return !called || configured;
        }
    }

    static class MyStringDeserializer extends StringDeserializer {
        boolean configured = false;
        boolean called = false;

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            super.configure(configs, isKey);
            configured = true;
        }

        @Override
        public String deserialize(final String topic, final byte[] data) {
            called = true;
            return super.deserialize(topic, data);
        }

        boolean configured() {
            return !called || configured;
        }
    }

    public static class MyStringSerde<K> extends Serdes.WrapperSerde<String> {
        public MyStringSerde() {
            super(new MyStringSerializer(), new MyStringDeserializer());
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            super.configure(configs, isKey);
        }

        public boolean configured() {
            if (!((MyStringSerializer) this.serializer()).configured()) return false;
            if (!((MyStringDeserializer) this.deserializer()).configured()) return false;
            return true;
        }
    }

    private static class SimpleProcessor extends AbstractProcessor<String, String> {

        final List<String> valueList;

        SimpleProcessor(final List<String> valueList) {
            this.valueList = valueList;
        }

        @Override
        public void process(final String key, final String value) {
            valueList.add(value);
        }
    }
    @Test
    @SuppressWarnings("unchecked")
    public void shouldInitializeForMaterialized() throws Exception {
        final Initializer<Integer> initializer = () -> 0;
        final Aggregator<String, String, Integer> aggregator = (k, v, agg) -> agg + v.length();

        final Reducer<String> reducer = (v1, v2) -> v1 + ":" + v2;

        final List<String> processorValueCollector = new ArrayList<>();

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> sourceStream = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        final KStream<String, String> mappedStream = sourceStream.map((k, v) -> KeyValue.pair(k.toUpperCase(Locale.getDefault()), v));

        mappedStream.filter((k, v) -> k.equals("B")).mapValues(v -> v.toUpperCase(Locale.getDefault()))
                .process(() -> new SimpleProcessor(processorValueCollector));

        final KStream<String, Long> countStream = mappedStream.groupByKey().count(Materialized.with(Serdes.String(), Serdes.Long())).toStream();

        countStream.to(COUNT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        final MyIntegerSerde valueTestSerde = new MyIntegerSerde();
        final MyStringSerde keyTestSerde = new MyStringSerde();
        mappedStream.groupByKey().aggregate(initializer,
                aggregator,
                Materialized.with(keyTestSerde, valueTestSerde))
                .toStream().to(AGGREGATION_TOPIC, Produced.with(Serdes.String(), Serdes.Integer()));

        assertTrue(keyTestSerde.configured());
        assertTrue(valueTestSerde.configured());
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

    static class MyIntegerSerde<K> extends Serdes.WrapperSerde<Integer> {
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
    public void shouldInitializeForProducedGrouped() throws Exception {
        final long timestamp = mockTime.milliseconds();
        produceMessages(timestamp);
        produceMessages(timestamp);

        final MyIntegerSerde keySerdeGrouped = new MyIntegerSerde();
        final MyStringSerde valueSerdeGrouped = new MyStringSerde();
        final MyStringSerde keySerdeProduced = new MyStringSerde();
        final MyIntegerSerde valueSerdeProduced = new MyIntegerSerde();

        stream.groupByKey(Grouped.with(keySerdeGrouped, valueSerdeGrouped))
            .windowedBy(TimeWindows.of(ofMillis(500L)))
            .count(Materialized.as("count-windows"))
            .toStream((windowedKey, value) -> ((Windowed<Integer>) windowedKey).key() + "@" +
                    ((Windowed<Integer>) windowedKey).window().start())
            .to(outputTopic, Produced.with(keySerdeProduced, valueSerdeProduced));

        startStreams();

        assertTrue(keySerdeGrouped.configured());
        assertTrue(valueSerdeGrouped.configured());

        assertTrue(keySerdeProduced.configured());
        assertTrue(valueSerdeProduced.configured());
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
}
