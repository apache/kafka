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
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import kafka.utils.MockTime;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@Category({IntegrationTest.class})
public class RepartitionOptimizingIntegrationTest {

    private static final int NUM_BROKERS = 1;
    private static final String INPUT_TOPIC = "input";
    private static final String COUNT_TOPIC = "outputTopic_0";
    private static final String AGGREGATION_TOPIC = "outputTopic_1";
    private static final String REDUCE_TOPIC = "outputTopic_2";
    private static final String JOINED_TOPIC = "joinedOutputTopic";

    private static final int ONE_REPARTITION_TOPIC = 1;
    private static final int FOUR_REPARTITION_TOPICS = 4;

    private final Pattern repartitionTopicPattern = Pattern.compile("Sink: .*-repartition");

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
            "replace-me",
            CLUSTER.bootstrapServers(),
            Serdes.String().getClass().getName(),
            Serdes.String().getClass().getName(),
            props);

        CLUSTER.deleteAndRecreateTopics(INPUT_TOPIC,
                                        COUNT_TOPIC,
                                        AGGREGATION_TOPIC,
                                        REDUCE_TOPIC,
                                        JOINED_TOPIC);


        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @Test
    public void shouldSendCorrectRecords_OPTIMIZED() throws Exception {
        runIntegrationTest(StreamsConfig.OPTIMIZE,
                           "optimized-test-app",
                           ONE_REPARTITION_TOPIC);
    }

    @Test
    public void shouldSendCorrcetResults_NO_OPTIMIZATION() throws Exception {
        runIntegrationTest(StreamsConfig.NO_OPTIMIZATION,
                           "non-optimized-test-app",
                           FOUR_REPARTITION_TOPICS);
    }


    private void runIntegrationTest(final String optimizationConfig,
                                    final String appId,
                                    final int expectedNumberRepartitionTopics) throws Exception {

        Initializer<Integer> initializer = () -> 0;
        Aggregator<String, String, Integer> aggregator = (k, v, agg) -> agg + v.length();

        Reducer<String> reducer = (v1, v2) -> v1 + ":" + v2;

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> sourceStream = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> mappedStream = sourceStream.map((k, v) -> KeyValue.pair(k.toUpperCase(Locale.getDefault()), v));


        KStream<String, Long> countStream = mappedStream.groupByKey().count(Materialized.with(Serdes.String(), Serdes.Long())).toStream();
        countStream.to(COUNT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));


        mappedStream.groupByKey().aggregate(initializer, aggregator, Materialized.with(Serdes.String(), Serdes.Integer())).toStream().to(AGGREGATION_TOPIC, Produced.with(Serdes.String(), Serdes.Integer()));
        mappedStream.groupByKey().reduce(reducer, Materialized.with(Serdes.String(), Serdes.String())).toStream().to(REDUCE_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        mappedStream.filter((k, v) -> k.equals("A"))
            .join(countStream, (v1, v2) -> v1 + ":" + v2.toString(), JoinWindows.of(5000), Joined.with(Serdes.String(), Serdes.String(), Serdes.Long()))
            .to(JOINED_TOPIC);


        streamsConfiguration.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION, optimizationConfig);
        streamsConfiguration.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, appId);

        final Properties producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC, getKeyValues(), producerConfig, mockTime);

        final Properties consumerConfig1 = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, LongDeserializer.class);
        final Properties consumerConfig2 = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, IntegerDeserializer.class);
        final Properties consumerConfig3 = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, StringDeserializer.class);

        Topology topology = builder.build(streamsConfiguration);
        String topologyString = topology.describe().toString();

        assertEquals(expectedNumberRepartitionTopics, getCountOfRepartitionTopicsFound(topologyString));

        final KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
        streams.start();

        final List<KeyValue<String, Long>> expectedCountKeyValues = Arrays.asList(KeyValue.pair("A", 3L), KeyValue.pair("B", 3L), KeyValue.pair("C", 3L));
        final List<KeyValue<String, Long>> receivedCountKeyValues = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig1, COUNT_TOPIC, expectedCountKeyValues.size());

        final List<KeyValue<String, Integer>> expectedAggKeyValues = Arrays.asList(KeyValue.pair("A", 9), KeyValue.pair("B", 9), KeyValue.pair("C", 9));
        final List<KeyValue<String, Integer>> receivedAggKeyValues = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig2, AGGREGATION_TOPIC, expectedAggKeyValues.size());

        final List<KeyValue<String, String>> expectedReduceKeyValues = Arrays.asList(KeyValue.pair("A", "foo:bar:baz"), KeyValue.pair("B", "foo:bar:baz"), KeyValue.pair("C", "foo:bar:baz"));
        final List<KeyValue<String, Integer>> receivedReduceKeyValues = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig3, REDUCE_TOPIC, expectedAggKeyValues.size());

        final List<KeyValue<String, String>> expectedJoinKeyValues = Arrays.asList(KeyValue.pair("A", "foo:3"), KeyValue.pair("A", "bar:3"), KeyValue.pair("A", "baz:3"));
        final List<KeyValue<String, Integer>> receivedJoinKeyValues = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig3, JOINED_TOPIC, expectedJoinKeyValues.size());


        assertThat(receivedCountKeyValues, equalTo(expectedCountKeyValues));
        assertThat(receivedAggKeyValues, equalTo(expectedAggKeyValues));
        assertThat(receivedReduceKeyValues, equalTo(expectedReduceKeyValues));
        assertThat(receivedJoinKeyValues, equalTo(expectedJoinKeyValues));

        streams.close(5, TimeUnit.SECONDS);
    }


    private int getCountOfRepartitionTopicsFound(String topologyString) {
        final Matcher matcher = repartitionTopicPattern.matcher(topologyString);
        final List<String> repartitionTopicsFound = new ArrayList<>();
        while (matcher.find()) {
            repartitionTopicsFound.add(matcher.group());
        }
        return repartitionTopicsFound.size();
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

}
