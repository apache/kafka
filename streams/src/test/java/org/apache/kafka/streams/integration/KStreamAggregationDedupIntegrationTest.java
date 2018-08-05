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
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

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
        String applicationId = "kgrouped-stream-test-" +
            testNo;
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration
            .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL_MS);
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
        streamsConfiguration.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);

        KeyValueMapper<Integer, String, String> mapper = MockMapper.selectValueMapper();
        stream = builder.stream(streamOneInput, Consumed.with(Serdes.Integer(), Serdes.String()));
        groupedStream = stream
            .groupBy(
                mapper,
                Serialized.with(Serdes.String(), Serdes.String()));

        reducer = new Reducer<String>() {
            @Override
            public String apply(String value1, String value2) {
                return value1 + ":" + value2;
            }
        };
    }

    @Test
    public void shouldTest() throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();

        StoreBuilder<WindowStore<String, String>> storeBuilder = Stores.windowStoreBuilder(
                Stores.persistentWindowStore("store-name", 3600000, 3, 60000, false),
                Serdes.String(),
                Serdes.String())
                .withCachingEnabled();

        builder.addStateStore(storeBuilder);

        builder.stream(streamOneInput, Consumed.with(Serdes.String(), Serdes.String()))
                .transform(() -> new Transformer<String, String, KeyValue<String, String>>() {

                    private WindowStore<String, String> store;
                    @Override
                    public void init(ProcessorContext processorContext) {
                        this.store = (WindowStore<String, String>) processorContext.getStateStore("store-name");
                        int count = 0;

                        KeyValueIterator<Windowed<String>, String> all = store.all();
                        while (all.hasNext()) {
                            count++;
                            all.next();
                        }

                        System.out.println("In init: number of items in store is: " + count);
                    }

                    @Override
                    public KeyValue<String, String> transform(String key, String value) {
                        int count = 0;

                        KeyValueIterator<Windowed<String>, String> all = store.all();
                        while (all.hasNext()) {
                            count++;
                            all.next();
                        }
                        System.out.println("Number of items in store is: " + count);

                        store.put(value, value);
                        return new KeyValue<>(key, value);
                    }

                    @Override
                    public void close() {

                    }
                }, "store-name");

        final String bootstrapServers = CLUSTER.bootstrapServers();
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "duplicate-example");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "duplicate-example-client");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        streams.cleanUp();
        streams.start();

        Thread.sleep(6000);

        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                streamOneInput,
                Arrays.asList(
                        new KeyValue<>(UUID.randomUUID().toString(), UUID.randomUUID().toString()),
                        new KeyValue<>(UUID.randomUUID().toString(), UUID.randomUUID().toString()),
                        new KeyValue<>(UUID.randomUUID().toString(), UUID.randomUUID().toString()),
                        new KeyValue<>(UUID.randomUUID().toString(), UUID.randomUUID().toString()),
                        new KeyValue<>(UUID.randomUUID().toString(), UUID.randomUUID().toString())),
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        StringSerializer.class,
                        StringSerializer.class,
                        new Properties()),
                null);

        Thread.sleep(7000);

        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                streamOneInput,
                Arrays.asList(
                        new KeyValue<>(UUID.randomUUID().toString(), UUID.randomUUID().toString()),
                        new KeyValue<>(UUID.randomUUID().toString(), UUID.randomUUID().toString()),
                        new KeyValue<>(UUID.randomUUID().toString(), UUID.randomUUID().toString()),
                        new KeyValue<>(UUID.randomUUID().toString(), UUID.randomUUID().toString()),
                        new KeyValue<>(UUID.randomUUID().toString(), UUID.randomUUID().toString())),
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        StringSerializer.class,
                        StringSerializer.class,
                        new Properties()),
                null);

        Thread.sleep(10000);

        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                streamOneInput,
                Arrays.asList(
                        new KeyValue<>(UUID.randomUUID().toString(), UUID.randomUUID().toString()),
                        new KeyValue<>(UUID.randomUUID().toString(), UUID.randomUUID().toString()),
                        new KeyValue<>(UUID.randomUUID().toString(), UUID.randomUUID().toString()),
                        new KeyValue<>(UUID.randomUUID().toString(), UUID.randomUUID().toString()),
                        new KeyValue<>(UUID.randomUUID().toString(), UUID.randomUUID().toString())),
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        StringSerializer.class,
                        StringSerializer.class,
                        new Properties()),
                null);

        Thread.sleep(8000);


        streams.close();
    }

    @After
    public void whenShuttingDown() throws IOException {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }


    @Test
    public void shouldReduce() throws Exception {
        produceMessages(System.currentTimeMillis());
        groupedStream
                .reduce(reducer, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("reduce-by-key"))
                .toStream()
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        startStreams();

        produceMessages(System.currentTimeMillis());

        List<KeyValue<String, String>> results = receiveMessages(
            new StringDeserializer(),
            new StringDeserializer(),
            5);

        Collections.sort(results, new Comparator<KeyValue<String, String>>() {
            @Override
            public int compare(KeyValue<String, String> o1, KeyValue<String, String> o2) {
                return KStreamAggregationDedupIntegrationTest.compare(o1, o2);
            }
        });

        assertThat(results, is(Arrays.asList(
            KeyValue.pair("A", "A:A"),
            KeyValue.pair("B", "B:B"),
            KeyValue.pair("C", "C:C"),
            KeyValue.pair("D", "D:D"),
            KeyValue.pair("E", "E:E"))));
    }

    @SuppressWarnings("unchecked")
    private static <K extends Comparable, V extends Comparable> int compare(final KeyValue<K, V> o1,
                                                                            final KeyValue<K, V> o2) {
        final int keyComparison = o1.key.compareTo(o2.key);
        if (keyComparison == 0) {
            return o1.value.compareTo(o2.value);
        }
        return keyComparison;
    }

    @Test
    public void shouldReduceWindowed() throws Exception {
        long firstBatchTimestamp = System.currentTimeMillis() - 1000;
        produceMessages(firstBatchTimestamp);
        long secondBatchTimestamp = System.currentTimeMillis();
        produceMessages(secondBatchTimestamp);
        produceMessages(secondBatchTimestamp);

        groupedStream
            .windowedBy(TimeWindows.of(500L))
            .reduce(reducer, Materialized.<String, String, WindowStore<Bytes, byte[]>>as("reduce-time-windows"))
            .toStream(new KeyValueMapper<Windowed<String>, String, String>() {
                @Override
                public String apply(Windowed<String> windowedKey, String value) {
                    return windowedKey.key() + "@" + windowedKey.window().start();
                }
            })
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        startStreams();

        List<KeyValue<String, String>> windowedOutput = receiveMessages(
            new StringDeserializer(),
            new StringDeserializer(),
            10);

        Comparator<KeyValue<String, String>>
            comparator =
            new Comparator<KeyValue<String, String>>() {
                @Override
                public int compare(final KeyValue<String, String> o1,
                                   final KeyValue<String, String> o2) {
                    return KStreamAggregationDedupIntegrationTest.compare(o1, o2);
                }
            };

        Collections.sort(windowedOutput, comparator);
        long firstBatchWindow = firstBatchTimestamp / 500 * 500;
        long secondBatchWindow = secondBatchTimestamp / 500 * 500;

        assertThat(windowedOutput, is(
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
        ));
    }

    @Test
    public void shouldGroupByKey() throws Exception {
        final long timestamp = mockTime.milliseconds();
        produceMessages(timestamp);
        produceMessages(timestamp);

        stream.groupByKey(Serialized.with(Serdes.Integer(), Serdes.String()))
            .windowedBy(TimeWindows.of(500L))
            .count(Materialized.<Integer, Long, WindowStore<Bytes, byte[]>>as("count-windows"))
            .toStream(new KeyValueMapper<Windowed<Integer>, Long, String>() {
                @Override
                public String apply(final Windowed<Integer> windowedKey, final Long value) {
                    return windowedKey.key() + "@" + windowedKey.window().start();
                }
            }).to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        startStreams();

        final List<KeyValue<String, Long>> results = receiveMessages(
            new StringDeserializer(),
            new LongDeserializer(),
            5);
        Collections.sort(results, new Comparator<KeyValue<String, Long>>() {
            @Override
            public int compare(final KeyValue<String, Long> o1, final KeyValue<String, Long> o2) {
                return KStreamAggregationDedupIntegrationTest.compare(o1, o2);
            }
        });

        final long window = timestamp / 500 * 500;
        assertThat(results, is(Arrays.asList(
            KeyValue.pair("1@" + window, 2L),
            KeyValue.pair("2@" + window, 2L),
            KeyValue.pair("3@" + window, 2L),
            KeyValue.pair("4@" + window, 2L),
            KeyValue.pair("5@" + window, 2L)
        )));

    }


    private void produceMessages(long timestamp) throws Exception {
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
        CLUSTER.createTopic(streamOneInput, 1, 1);
        CLUSTER.createTopic(outputTopic);
    }

    private void startStreams() {
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();
    }


    private <K, V> List<KeyValue<K, V>> receiveMessages(final Deserializer<K>
                                                            keyDeserializer,
                                                        final Deserializer<V>
                                                            valueDeserializer,
                                                        final int numMessages)
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
        return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerProperties,
            outputTopic,
            numMessages,
            60 * 1000);

    }

}
