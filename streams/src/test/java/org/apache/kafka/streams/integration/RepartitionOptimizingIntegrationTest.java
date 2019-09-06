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
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.test.IntegrationTest;
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
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

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
        final Properties props = new Properties();
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 1024 * 10);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000);

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
    public void tearDown() throws Exception {
        CLUSTER.deleteAllTopicsAndWait(30_000L);
    }

    @Test
    public void shouldSendCorrectRecords_OPTIMIZED() throws Exception {
        runIntegrationTest(StreamsConfig.OPTIMIZE,
                           ONE_REPARTITION_TOPIC);
    }

    @Test
    public void shouldSendCorrectResults_NO_OPTIMIZATION() throws Exception {
        runIntegrationTest(StreamsConfig.NO_OPTIMIZATION,
                           FOUR_REPARTITION_TOPICS);
    }


    private void runIntegrationTest(final String optimizationConfig,
                                    final int expectedNumberRepartitionTopics) throws Exception {

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

        mappedStream.groupByKey().aggregate(initializer,
                                            aggregator,
                                            Materialized.with(Serdes.String(), Serdes.Integer()))
            .toStream().to(AGGREGATION_TOPIC, Produced.with(Serdes.String(), Serdes.Integer()));

        // adding operators for case where the repartition node is further downstream
        mappedStream.filter((k, v) -> true).peek((k, v) -> System.out.println(k + ":" + v)).groupByKey()
            .reduce(reducer, Materialized.with(Serdes.String(), Serdes.String()))
            .toStream().to(REDUCE_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        mappedStream.filter((k, v) -> k.equals("A"))
            .join(countStream, (v1, v2) -> v1 + ":" + v2.toString(),
                  JoinWindows.of(ofMillis(5000)),
                  Joined.with(Serdes.String(), Serdes.String(), Serdes.Long()))
            .to(JOINED_TOPIC);

        streamsConfiguration.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION, optimizationConfig);

        final Properties producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC, getKeyValues(), producerConfig, mockTime);

        final Properties consumerConfig1 = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, LongDeserializer.class);
        final Properties consumerConfig2 = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, IntegerDeserializer.class);
        final Properties consumerConfig3 = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, StringDeserializer.class);

        final Topology topology = builder.build(streamsConfiguration);
        final String topologyString = topology.describe().toString();

        if (optimizationConfig.equals(StreamsConfig.OPTIMIZE)) {
            assertEquals(EXPECTED_OPTIMIZED_TOPOLOGY, topologyString);
        } else {
            assertEquals(EXPECTED_UNOPTIMIZED_TOPOLOGY, topologyString);
        }


        /*
           confirming number of expected repartition topics here
         */
        assertEquals(expectedNumberRepartitionTopics, getCountOfRepartitionTopicsFound(topologyString));

        final KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
        streams.start();

        final List<KeyValue<String, Long>> expectedCountKeyValues = Arrays.asList(KeyValue.pair("A", 3L), KeyValue.pair("B", 3L), KeyValue.pair("C", 3L));
        IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(consumerConfig1, COUNT_TOPIC, expectedCountKeyValues);

        final List<KeyValue<String, Integer>> expectedAggKeyValues = Arrays.asList(KeyValue.pair("A", 9), KeyValue.pair("B", 9), KeyValue.pair("C", 9));
        IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(consumerConfig2, AGGREGATION_TOPIC, expectedAggKeyValues);

        final List<KeyValue<String, String>> expectedReduceKeyValues = Arrays.asList(KeyValue.pair("A", "foo:bar:baz"), KeyValue.pair("B", "foo:bar:baz"), KeyValue.pair("C", "foo:bar:baz"));
        IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(consumerConfig3, REDUCE_TOPIC, expectedReduceKeyValues);

        final List<KeyValue<String, String>> expectedJoinKeyValues = Arrays.asList(KeyValue.pair("A", "foo:3"), KeyValue.pair("A", "bar:3"), KeyValue.pair("A", "baz:3"));
        IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(consumerConfig3, JOINED_TOPIC, expectedJoinKeyValues);


        final List<String> expectedCollectedProcessorValues = Arrays.asList("FOO", "BAR", "BAZ");

        assertThat(3, equalTo(processorValueCollector.size()));
        assertThat(processorValueCollector, equalTo(expectedCollectedProcessorValues));

        streams.close(ofSeconds(5));
    }


    private int getCountOfRepartitionTopicsFound(final String topologyString) {
        final Matcher matcher = repartitionTopicPattern.matcher(topologyString);
        final List<String> repartitionTopicsFound = new ArrayList<>();
        while (matcher.find()) {
            repartitionTopicsFound.add(matcher.group());
        }
        return repartitionTopicsFound.size();
    }


    private List<KeyValue<String, String>> getKeyValues() {
        final List<KeyValue<String, String>> keyValueList = new ArrayList<>();
        final String[] keys = new String[]{"a", "b", "c"};
        final String[] values = new String[]{"foo", "bar", "baz"};
        for (final String key : keys) {
            for (final String value : values) {
                keyValueList.add(KeyValue.pair(key, value));
            }
        }
        return keyValueList;
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


    private static final String EXPECTED_OPTIMIZED_TOPOLOGY = "Topologies:\n"
                                                              + "   Sub-topology: 0\n"
                                                              + "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n"
                                                              + "      --> KSTREAM-MAP-0000000001\n"
                                                              + "    Processor: KSTREAM-MAP-0000000001 (stores: [])\n"
                                                              + "      --> KSTREAM-FILTER-0000000002, KSTREAM-FILTER-0000000040\n"
                                                              + "      <-- KSTREAM-SOURCE-0000000000\n"
                                                              + "    Processor: KSTREAM-FILTER-0000000002 (stores: [])\n"
                                                              + "      --> KSTREAM-MAPVALUES-0000000003\n"
                                                              + "      <-- KSTREAM-MAP-0000000001\n"
                                                              + "    Processor: KSTREAM-FILTER-0000000040 (stores: [])\n"
                                                              + "      --> KSTREAM-SINK-0000000039\n"
                                                              + "      <-- KSTREAM-MAP-0000000001\n"
                                                              + "    Processor: KSTREAM-MAPVALUES-0000000003 (stores: [])\n"
                                                              + "      --> KSTREAM-PROCESSOR-0000000004\n"
                                                              + "      <-- KSTREAM-FILTER-0000000002\n"
                                                              + "    Processor: KSTREAM-PROCESSOR-0000000004 (stores: [])\n"
                                                              + "      --> none\n"
                                                              + "      <-- KSTREAM-MAPVALUES-0000000003\n"
                                                              + "    Sink: KSTREAM-SINK-0000000039 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000006-repartition)\n"
                                                              + "      <-- KSTREAM-FILTER-0000000040\n"
                                                              + "\n"
                                                              + "  Sub-topology: 1\n"
                                                              + "    Source: KSTREAM-SOURCE-0000000041 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000006-repartition])\n"
                                                              + "      --> KSTREAM-FILTER-0000000020, KSTREAM-AGGREGATE-0000000007, KSTREAM-AGGREGATE-0000000014, KSTREAM-FILTER-0000000029\n"
                                                              + "    Processor: KSTREAM-AGGREGATE-0000000007 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000006])\n"
                                                              + "      --> KTABLE-TOSTREAM-0000000011\n"
                                                              + "      <-- KSTREAM-SOURCE-0000000041\n"
                                                              + "    Processor: KTABLE-TOSTREAM-0000000011 (stores: [])\n"
                                                              + "      --> KSTREAM-SINK-0000000012, KSTREAM-WINDOWED-0000000034\n"
                                                              + "      <-- KSTREAM-AGGREGATE-0000000007\n"
                                                              + "    Processor: KSTREAM-FILTER-0000000020 (stores: [])\n"
                                                              + "      --> KSTREAM-PEEK-0000000021\n"
                                                              + "      <-- KSTREAM-SOURCE-0000000041\n"
                                                              + "    Processor: KSTREAM-FILTER-0000000029 (stores: [])\n"
                                                              + "      --> KSTREAM-WINDOWED-0000000033\n"
                                                              + "      <-- KSTREAM-SOURCE-0000000041\n"
                                                              + "    Processor: KSTREAM-PEEK-0000000021 (stores: [])\n"
                                                              + "      --> KSTREAM-REDUCE-0000000023\n"
                                                              + "      <-- KSTREAM-FILTER-0000000020\n"
                                                              + "    Processor: KSTREAM-WINDOWED-0000000033 (stores: [KSTREAM-JOINTHIS-0000000035-store])\n"
                                                              + "      --> KSTREAM-JOINTHIS-0000000035\n"
                                                              + "      <-- KSTREAM-FILTER-0000000029\n"
                                                              + "    Processor: KSTREAM-WINDOWED-0000000034 (stores: [KSTREAM-JOINOTHER-0000000036-store])\n"
                                                              + "      --> KSTREAM-JOINOTHER-0000000036\n"
                                                              + "      <-- KTABLE-TOSTREAM-0000000011\n"
                                                              + "    Processor: KSTREAM-AGGREGATE-0000000014 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000013])\n"
                                                              + "      --> KTABLE-TOSTREAM-0000000018\n"
                                                              + "      <-- KSTREAM-SOURCE-0000000041\n"
                                                              + "    Processor: KSTREAM-JOINOTHER-0000000036 (stores: [KSTREAM-JOINTHIS-0000000035-store])\n"
                                                              + "      --> KSTREAM-MERGE-0000000037\n"
                                                              + "      <-- KSTREAM-WINDOWED-0000000034\n"
                                                              + "    Processor: KSTREAM-JOINTHIS-0000000035 (stores: [KSTREAM-JOINOTHER-0000000036-store])\n"
                                                              + "      --> KSTREAM-MERGE-0000000037\n"
                                                              + "      <-- KSTREAM-WINDOWED-0000000033\n"
                                                              + "    Processor: KSTREAM-REDUCE-0000000023 (stores: [KSTREAM-REDUCE-STATE-STORE-0000000022])\n"
                                                              + "      --> KTABLE-TOSTREAM-0000000027\n"
                                                              + "      <-- KSTREAM-PEEK-0000000021\n"
                                                              + "    Processor: KSTREAM-MERGE-0000000037 (stores: [])\n"
                                                              + "      --> KSTREAM-SINK-0000000038\n"
                                                              + "      <-- KSTREAM-JOINTHIS-0000000035, KSTREAM-JOINOTHER-0000000036\n"
                                                              + "    Processor: KTABLE-TOSTREAM-0000000018 (stores: [])\n"
                                                              + "      --> KSTREAM-SINK-0000000019\n"
                                                              + "      <-- KSTREAM-AGGREGATE-0000000014\n"
                                                              + "    Processor: KTABLE-TOSTREAM-0000000027 (stores: [])\n"
                                                              + "      --> KSTREAM-SINK-0000000028\n"
                                                              + "      <-- KSTREAM-REDUCE-0000000023\n"
                                                              + "    Sink: KSTREAM-SINK-0000000012 (topic: outputTopic_0)\n"
                                                              + "      <-- KTABLE-TOSTREAM-0000000011\n"
                                                              + "    Sink: KSTREAM-SINK-0000000019 (topic: outputTopic_1)\n"
                                                              + "      <-- KTABLE-TOSTREAM-0000000018\n"
                                                              + "    Sink: KSTREAM-SINK-0000000028 (topic: outputTopic_2)\n"
                                                              + "      <-- KTABLE-TOSTREAM-0000000027\n"
                                                              + "    Sink: KSTREAM-SINK-0000000038 (topic: joinedOutputTopic)\n"
                                                              + "      <-- KSTREAM-MERGE-0000000037\n\n";


    private static final String EXPECTED_UNOPTIMIZED_TOPOLOGY = "Topologies:\n"
                                                                + "   Sub-topology: 0\n"
                                                                + "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n"
                                                                + "      --> KSTREAM-MAP-0000000001\n"
                                                                + "    Processor: KSTREAM-MAP-0000000001 (stores: [])\n"
                                                                + "      --> KSTREAM-FILTER-0000000020, KSTREAM-FILTER-0000000002, KSTREAM-FILTER-0000000009, KSTREAM-FILTER-0000000016, KSTREAM-FILTER-0000000029\n"
                                                                + "      <-- KSTREAM-SOURCE-0000000000\n"
                                                                + "    Processor: KSTREAM-FILTER-0000000020 (stores: [])\n"
                                                                + "      --> KSTREAM-PEEK-0000000021\n"
                                                                + "      <-- KSTREAM-MAP-0000000001\n"
                                                                + "    Processor: KSTREAM-FILTER-0000000002 (stores: [])\n"
                                                                + "      --> KSTREAM-MAPVALUES-0000000003\n"
                                                                + "      <-- KSTREAM-MAP-0000000001\n"
                                                                + "    Processor: KSTREAM-FILTER-0000000029 (stores: [])\n"
                                                                + "      --> KSTREAM-FILTER-0000000031\n"
                                                                + "      <-- KSTREAM-MAP-0000000001\n"
                                                                + "    Processor: KSTREAM-PEEK-0000000021 (stores: [])\n"
                                                                + "      --> KSTREAM-FILTER-0000000025\n"
                                                                + "      <-- KSTREAM-FILTER-0000000020\n"
                                                                + "    Processor: KSTREAM-FILTER-0000000009 (stores: [])\n"
                                                                + "      --> KSTREAM-SINK-0000000008\n"
                                                                + "      <-- KSTREAM-MAP-0000000001\n"
                                                                + "    Processor: KSTREAM-FILTER-0000000016 (stores: [])\n"
                                                                + "      --> KSTREAM-SINK-0000000015\n"
                                                                + "      <-- KSTREAM-MAP-0000000001\n"
                                                                + "    Processor: KSTREAM-FILTER-0000000025 (stores: [])\n"
                                                                + "      --> KSTREAM-SINK-0000000024\n"
                                                                + "      <-- KSTREAM-PEEK-0000000021\n"
                                                                + "    Processor: KSTREAM-FILTER-0000000031 (stores: [])\n"
                                                                + "      --> KSTREAM-SINK-0000000030\n"
                                                                + "      <-- KSTREAM-FILTER-0000000029\n"
                                                                + "    Processor: KSTREAM-MAPVALUES-0000000003 (stores: [])\n"
                                                                + "      --> KSTREAM-PROCESSOR-0000000004\n"
                                                                + "      <-- KSTREAM-FILTER-0000000002\n"
                                                                + "    Processor: KSTREAM-PROCESSOR-0000000004 (stores: [])\n"
                                                                + "      --> none\n"
                                                                + "      <-- KSTREAM-MAPVALUES-0000000003\n"
                                                                + "    Sink: KSTREAM-SINK-0000000008 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000006-repartition)\n"
                                                                + "      <-- KSTREAM-FILTER-0000000009\n"
                                                                + "    Sink: KSTREAM-SINK-0000000015 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000013-repartition)\n"
                                                                + "      <-- KSTREAM-FILTER-0000000016\n"
                                                                + "    Sink: KSTREAM-SINK-0000000024 (topic: KSTREAM-REDUCE-STATE-STORE-0000000022-repartition)\n"
                                                                + "      <-- KSTREAM-FILTER-0000000025\n"
                                                                + "    Sink: KSTREAM-SINK-0000000030 (topic: KSTREAM-FILTER-0000000029-repartition)\n"
                                                                + "      <-- KSTREAM-FILTER-0000000031\n"
                                                                + "\n"
                                                                + "  Sub-topology: 1\n"
                                                                + "    Source: KSTREAM-SOURCE-0000000010 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000006-repartition])\n"
                                                                + "      --> KSTREAM-AGGREGATE-0000000007\n"
                                                                + "    Processor: KSTREAM-AGGREGATE-0000000007 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000006])\n"
                                                                + "      --> KTABLE-TOSTREAM-0000000011\n"
                                                                + "      <-- KSTREAM-SOURCE-0000000010\n"
                                                                + "    Processor: KTABLE-TOSTREAM-0000000011 (stores: [])\n"
                                                                + "      --> KSTREAM-SINK-0000000012, KSTREAM-WINDOWED-0000000034\n"
                                                                + "      <-- KSTREAM-AGGREGATE-0000000007\n"
                                                                + "    Source: KSTREAM-SOURCE-0000000032 (topics: [KSTREAM-FILTER-0000000029-repartition])\n"
                                                                + "      --> KSTREAM-WINDOWED-0000000033\n"
                                                                + "    Processor: KSTREAM-WINDOWED-0000000033 (stores: [KSTREAM-JOINTHIS-0000000035-store])\n"
                                                                + "      --> KSTREAM-JOINTHIS-0000000035\n"
                                                                + "      <-- KSTREAM-SOURCE-0000000032\n"
                                                                + "    Processor: KSTREAM-WINDOWED-0000000034 (stores: [KSTREAM-JOINOTHER-0000000036-store])\n"
                                                                + "      --> KSTREAM-JOINOTHER-0000000036\n"
                                                                + "      <-- KTABLE-TOSTREAM-0000000011\n"
                                                                + "    Processor: KSTREAM-JOINOTHER-0000000036 (stores: [KSTREAM-JOINTHIS-0000000035-store])\n"
                                                                + "      --> KSTREAM-MERGE-0000000037\n"
                                                                + "      <-- KSTREAM-WINDOWED-0000000034\n"
                                                                + "    Processor: KSTREAM-JOINTHIS-0000000035 (stores: [KSTREAM-JOINOTHER-0000000036-store])\n"
                                                                + "      --> KSTREAM-MERGE-0000000037\n"
                                                                + "      <-- KSTREAM-WINDOWED-0000000033\n"
                                                                + "    Processor: KSTREAM-MERGE-0000000037 (stores: [])\n"
                                                                + "      --> KSTREAM-SINK-0000000038\n"
                                                                + "      <-- KSTREAM-JOINTHIS-0000000035, KSTREAM-JOINOTHER-0000000036\n"
                                                                + "    Sink: KSTREAM-SINK-0000000012 (topic: outputTopic_0)\n"
                                                                + "      <-- KTABLE-TOSTREAM-0000000011\n"
                                                                + "    Sink: KSTREAM-SINK-0000000038 (topic: joinedOutputTopic)\n"
                                                                + "      <-- KSTREAM-MERGE-0000000037\n"
                                                                + "\n"
                                                                + "  Sub-topology: 2\n"
                                                                + "    Source: KSTREAM-SOURCE-0000000017 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000013-repartition])\n"
                                                                + "      --> KSTREAM-AGGREGATE-0000000014\n"
                                                                + "    Processor: KSTREAM-AGGREGATE-0000000014 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000013])\n"
                                                                + "      --> KTABLE-TOSTREAM-0000000018\n"
                                                                + "      <-- KSTREAM-SOURCE-0000000017\n"
                                                                + "    Processor: KTABLE-TOSTREAM-0000000018 (stores: [])\n"
                                                                + "      --> KSTREAM-SINK-0000000019\n"
                                                                + "      <-- KSTREAM-AGGREGATE-0000000014\n"
                                                                + "    Sink: KSTREAM-SINK-0000000019 (topic: outputTopic_1)\n"
                                                                + "      <-- KTABLE-TOSTREAM-0000000018\n"
                                                                + "\n"
                                                                + "  Sub-topology: 3\n"
                                                                + "    Source: KSTREAM-SOURCE-0000000026 (topics: [KSTREAM-REDUCE-STATE-STORE-0000000022-repartition])\n"
                                                                + "      --> KSTREAM-REDUCE-0000000023\n"
                                                                + "    Processor: KSTREAM-REDUCE-0000000023 (stores: [KSTREAM-REDUCE-STATE-STORE-0000000022])\n"
                                                                + "      --> KTABLE-TOSTREAM-0000000027\n"
                                                                + "      <-- KSTREAM-SOURCE-0000000026\n"
                                                                + "    Processor: KTABLE-TOSTREAM-0000000027 (stores: [])\n"
                                                                + "      --> KSTREAM-SINK-0000000028\n"
                                                                + "      <-- KSTREAM-REDUCE-0000000023\n"
                                                                + "    Sink: KSTREAM-SINK-0000000028 (topic: outputTopic_2)\n"
                                                                + "      <-- KTABLE-TOSTREAM-0000000027\n\n";

}
