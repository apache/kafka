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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@Category({IntegrationTest.class})
public class KStreamTransformIntegrationTest {
    private static final int NUM_BROKERS = 1;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER =
        new EmbeddedKafkaCluster(NUM_BROKERS);

    private static volatile int testNo = 0;
    private final MockTime mockTime = CLUSTER.time;
    private StreamsBuilder builder;
    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;
    private String streamOneInput;
    private String userSessionsStream = "user-sessions";
    private String outputTopic;
    private KStream<Integer, Integer> stream;
    private StoreBuilder<KeyValueStore<Integer, Integer>> storeBuilder;

    @Before
    public void before() throws InterruptedException {
        testNo++;
        builder = new StreamsBuilder();
        createTopics();
        streamsConfiguration = new Properties();
        final String applicationId = "kgrouped-stream-test-" + testNo;
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration
            .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        final StoreBuilder<KeyValueStore<Integer, Integer>> keyValueStoreBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("myTransformState"),
                                            Serdes.Integer(),
                                            Serdes.Integer());
        builder.addStateStore(keyValueStoreBuilder);
    }

    @After
    public void whenShuttingDown() throws IOException {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @Test
    public void shouldFlatTransform() throws Exception {

        stream = builder.stream(streamOneInput, Consumed.with(Serdes.Integer(), Serdes.Integer()));
        stream
            .flatTransform(new TransformerSupplier<Integer, Integer, List<KeyValue<Integer, Integer>>>() {
                @Override
                public Transformer<Integer, Integer, List<KeyValue<Integer, Integer>>> get() {
                    return new Transformer<Integer, Integer, List<KeyValue<Integer, Integer>>>() {
                        private KeyValueStore<Integer, Integer> state;

                        @Override
                        public void init(final ProcessorContext context) {
                            state = (KeyValueStore<Integer, Integer>) context.getStateStore("myTransformState");
                        }

                        @Override
                        public List<KeyValue<Integer, Integer>> transform(final Integer key, final Integer value) {
                            final List<KeyValue<Integer, Integer>> result = new ArrayList<>();
                            state.putIfAbsent(key, 0);
                            final Integer storedValue = state.get(key);
                            int outputValue = storedValue.intValue();
                            for (int i = 0; i < 3; i++) {
                                result.add(new KeyValue<Integer, Integer>(key + i, value + outputValue++));
                            }
                            state.put(key, new Integer(outputValue));
                            return result;
                        }

                        @Override
                        public void close() {
                        }
                    };
                }
            }, "myTransformState")
            .to(outputTopic, Produced.with(Serdes.Integer(), Serdes.Integer()));
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();

        produceMessages(mockTime.milliseconds());
        final List<KeyValue<Integer, Integer>> results = receiveMessages(
            new IntegerDeserializer(),
            new IntegerDeserializer(),
            18);

        Collections.sort(results, new Comparator<KeyValue<Integer, Integer>>() {
            @Override
            public int compare(final KeyValue<Integer, Integer> o1, final KeyValue<Integer, Integer> o2) {
                return KStreamTransformIntegrationTest.compare(o1, o2);
            }
        });

        assertThat(results, is(Arrays.asList(
            KeyValue.pair(1, 1),
            KeyValue.pair(1, 7),
            KeyValue.pair(2, 2),
            KeyValue.pair(2, 2),
            KeyValue.pair(2, 8),
            KeyValue.pair(2, 8),
            KeyValue.pair(3, 3),
            KeyValue.pair(3, 3),
            KeyValue.pair(3, 3),
            KeyValue.pair(3, 9),
            KeyValue.pair(3, 9),
            KeyValue.pair(3, 9),
            KeyValue.pair(4, 4),
            KeyValue.pair(4, 4),
            KeyValue.pair(4, 10),
            KeyValue.pair(4, 10),
            KeyValue.pair(5, 5),
            KeyValue.pair(5, 11))));
    }

    @Test
    public void shouldTransform() throws Exception {

        stream = builder.stream(streamOneInput, Consumed.with(Serdes.Integer(), Serdes.Integer()));
        stream
            .transform(new TransformerSupplier<Integer, Integer, KeyValue<Integer, Integer>>() {
                @Override
                public Transformer<Integer, Integer, KeyValue<Integer, Integer>> get() {
                    return new Transformer<Integer, Integer, KeyValue<Integer, Integer>>() {
                        private KeyValueStore<Integer, Integer> state;

                        @Override
                        public void init(final ProcessorContext context) {
                            state = (KeyValueStore<Integer, Integer>) context.getStateStore("myTransformState");
                        }

                        @Override
                        public KeyValue<Integer, Integer> transform(final Integer key, final Integer value) {
                            state.putIfAbsent(key, 0);
                            final Integer storedValue = state.get(key);
                            int outputValue = storedValue.intValue();
                            final KeyValue<Integer, Integer> result = new KeyValue<>(key + 1, value + outputValue++);
                            state.put(key, outputValue);
                            return result;
                        }

                        @Override
                        public void close() {
                        }
                    };
                }
            }, "myTransformState")
            .to(outputTopic, Produced.with(Serdes.Integer(), Serdes.Integer()));
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();

        produceMessages(mockTime.milliseconds());
        final List<KeyValue<Integer, Integer>> results = receiveMessages(
            new IntegerDeserializer(),
            new IntegerDeserializer(),
            6);

        Collections.sort(results, new Comparator<KeyValue<Integer, Integer>>() {
            @Override
            public int compare(final KeyValue<Integer, Integer> o1, final KeyValue<Integer, Integer> o2) {
                return KStreamTransformIntegrationTest.compare(o1, o2);
            }
        });

        assertThat(results, is(Arrays.asList(
            KeyValue.pair(2, 1),
            KeyValue.pair(2, 5),
            KeyValue.pair(3, 2),
            KeyValue.pair(3, 6),
            KeyValue.pair(4, 3),
            KeyValue.pair(4, 7))));
    }

    private static <K extends Comparable, V extends Comparable> int compare(final KeyValue<K, V> o1,
                                                                            final KeyValue<K, V> o2) {
        final int keyComparison = o1.key.compareTo(o2.key);
        if (keyComparison == 0) {
            return o1.value.compareTo(o2.value);
        }
        return keyComparison;
    }

    private void produceMessages(final long timestamp) throws Exception {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            streamOneInput,
            Arrays.asList(
                new KeyValue<>(1, 1),
                new KeyValue<>(2, 2),
                new KeyValue<>(3, 3),
                new KeyValue<>(1, 4),
                new KeyValue<>(2, 5),
                new KeyValue<>(3, 6)),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                IntegerSerializer.class,
                new Properties()),
            timestamp);
    }


    private void createTopics() throws InterruptedException {
        streamOneInput = "stream-one-" + testNo;
        outputTopic = "output-" + testNo;
        userSessionsStream = userSessionsStream + "-" + testNo;
        CLUSTER.createTopic(streamOneInput, 3, 1);
        CLUSTER.createTopics(userSessionsStream, outputTopic);
    }

    private <K, V> List<KeyValue<K, V>> receiveMessages(final Deserializer<K> keyDeserializer,
                                                        final Deserializer<V> valueDeserializer,
                                                        final int numMessages)
        throws InterruptedException {
        return receiveMessages(keyDeserializer, valueDeserializer, null, numMessages);
    }

    private <K, V> List<KeyValue<K, V>> receiveMessages(final Deserializer<K> keyDeserializer,
                                                        final Deserializer<V> valueDeserializer,
                                                        final Class innerClass,
                                                        final int numMessages) throws InterruptedException {
        final Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kgroupedstream-test-" + testNo);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass().getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass().getName());
        if (keyDeserializer instanceof TimeWindowedDeserializer || keyDeserializer instanceof SessionWindowedDeserializer) {
            consumerProperties.setProperty(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS,
                    Serdes.serdeFrom(innerClass).getClass().getName());
        }
        return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                consumerProperties,
                outputTopic,
                numMessages,
                60 * 1000);
    }
}
