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


import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.utils.MockTime;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

@Category({IntegrationTest.class})
public class RepartitionOptimizingIntegrationTest {

    private static final int NUM_BROKERS = 1;
    private static final String INPUT_TOPIC = "input";
    private static final String COUNT_TOPIC = "outputTopic_0";
    private static final String AGGREGATION_TOPIC = "outputTopic_1";
    private static final String REDUCE_TOPIC = "outputTopic_2";
    private static final String BYTE_ARRAY_SERDES_CLASS_NAME = Serdes.ByteArray().getClass().getName();

    private Properties streamsConfiguration;


    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final MockTime mockTime = CLUSTER.time;

    @Before
    public void setUp() throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 1024 * 10);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000);

        streamsConfiguration = StreamsTestUtils.getStreamsConfig(
            "repartitionOptimizationTest",
            CLUSTER.bootstrapServers(),
            BYTE_ARRAY_SERDES_CLASS_NAME,
            BYTE_ARRAY_SERDES_CLASS_NAME,
            props);

        deleteAndCreateTopicsAndCleanStreamsState();
    }


    @Test
    public void shouldSendCorrectRecordsWithOptimization() throws Exception {

        Initializer<Integer> initializer = () -> 0;
        Aggregator<String, String, Integer> aggregator = (k, v, agg) -> agg + v.length();

        Reducer<String> reducer = (v1, v2) -> v1 + ":" + v2;

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> sourceStream = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> mappedStream = sourceStream.map((k, v) -> KeyValue.pair(k.toUpperCase(Locale.getDefault()), v));

        mappedStream.groupByKey().count(Materialized.with(Serdes.String(), Serdes.Long())).toStream().to(COUNT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
        mappedStream.groupByKey().aggregate(initializer, aggregator, Materialized.with(Serdes.String(), Serdes.Integer())).toStream().to(AGGREGATION_TOPIC, Produced.with(Serdes.String(), Serdes.Integer()));
        mappedStream.groupByKey().reduce(reducer, Materialized.with(Serdes.String(), Serdes.String())).toStream().to(REDUCE_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        streamsConfiguration.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);

        final Properties producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC, getKeyValues(), producerConfig, mockTime);

        final Properties consumerConfig1 = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, LongDeserializer.class);
        final Properties consumerConfig2 = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, IntegerDeserializer.class);
        final Properties consumerConfig3 = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, StringDeserializer.class);

        Topology topology = builder.build(streamsConfiguration);
        System.out.println(topology.describe());
        final KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
        streams.start();

        final List<KeyValue<String, Long>> expectedCountKeyValues = Arrays.asList(KeyValue.pair("A", 3L), KeyValue.pair("B", 3L), KeyValue.pair("C", 3L));
        final List<KeyValue<String, Long>>
            receivedCountKeyValues =
            IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig1, COUNT_TOPIC, expectedCountKeyValues.size());

        final List<KeyValue<String, Integer>> expectedAggKeyValues = Arrays.asList(KeyValue.pair("A", 9), KeyValue.pair("B", 9), KeyValue.pair("C", 9));
        final List<KeyValue<String, Integer>>
            receivedAggKeyValues =
            IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig2, AGGREGATION_TOPIC, expectedAggKeyValues.size());

        final List<KeyValue<String, String>>
            expectedReduceKeyValues =
            Arrays.asList(KeyValue.pair("A", "foo:bar:baz"), KeyValue.pair("B", "foo:bar:baz"), KeyValue.pair("C", "foo:bar:baz"));
        final List<KeyValue<String, Integer>>
            receivedReduceKeyValues =
            IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig3, REDUCE_TOPIC, expectedAggKeyValues.size());

        assertThat(receivedCountKeyValues, equalTo(expectedCountKeyValues));
        assertThat(receivedAggKeyValues, equalTo(expectedAggKeyValues));
        assertThat(receivedReduceKeyValues, equalTo(expectedReduceKeyValues));

        streams.close(5, TimeUnit.SECONDS);
    }


    private void deleteAndCreateTopicsAndCleanStreamsState() throws Exception {
        CLUSTER.deleteAndRecreateTopics(INPUT_TOPIC,
                                        COUNT_TOPIC,
                                        AGGREGATION_TOPIC,
                                        REDUCE_TOPIC);

        // Remove any state from previous test runs
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    private List<KeyValue<String, String>> getKeyValues() {
        List<KeyValue<String, String>> keyValueList = new ArrayList<>();
        String[] keys = new String[]{"a", "b", "c"};
        String[] values = new String[]{"foo", "bar", "baz"};
        for (String key : keys) {
            for (String value : values) {
                keyValueList.add(KeyValue.pair(key, value));
            }
        }
        return keyValueList;
    }


    private static class MaybeOptimizedIntegrationTestRusults {

        final List<KeyValue<String, Long>> receivedCountKeyValues;
        final List<KeyValue<String, Integer>> receivedAggKeyValues;
        final List<KeyValue<String, Integer>> receivedReduceKeyValues;
        final String topology;

        public MaybeOptimizedIntegrationTestRusults(final List<KeyValue<String, Long>> receivedCountKeyValues,
                                                    final List<KeyValue<String, Integer>> receivedAggKeyValues,
                                                    final List<KeyValue<String, Integer>> receivedReduceKeyValues,
                                                    final String topology) {

            this.receivedCountKeyValues = new ArrayList<>(receivedCountKeyValues);
            this.receivedAggKeyValues = new ArrayList<>(receivedAggKeyValues);
            this.receivedReduceKeyValues = new ArrayList<>(receivedReduceKeyValues);
            this.topology = topology;
        }
    }

}
