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

package org.apache.kafka.streams.processor.internals;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.time.Duration.ofDays;
import static java.time.Duration.ofMillis;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class RepartitionOptimizingTest {

    private final Logger log = LoggerFactory.getLogger(RepartitionOptimizingTest.class);

    private static final String INPUT_TOPIC = "input";
    private static final String COUNT_TOPIC = "outputTopic_0";
    private static final String AGGREGATION_TOPIC = "outputTopic_1";
    private static final String REDUCE_TOPIC = "outputTopic_2";
    private static final String JOINED_TOPIC = "joinedOutputTopic";

    private static final int ONE_REPARTITION_TOPIC = 1;
    private static final int FOUR_REPARTITION_TOPICS = 4;

    private final Serializer<String> stringSerializer = new StringSerializer();
    private final Deserializer<String> stringDeserializer = new StringDeserializer();

    private final Pattern repartitionTopicPattern = Pattern.compile("Sink: .*-repartition");

    private Properties streamsConfiguration;
    private TopologyTestDriver topologyTestDriver;

    private final Initializer<Integer> initializer = () -> 0;
    private final Aggregator<String, String, Integer> aggregator = (k, v, agg) -> agg + v.length();
    private final Reducer<String> reducer = (v1, v2) -> v1 + ":" + v2;

    private final List<String> processorValueCollector = new ArrayList<>();

    private final List<KeyValue<String, Long>> expectedCountKeyValues =
        Arrays.asList(KeyValue.pair("A", 3L), KeyValue.pair("B", 3L), KeyValue.pair("C", 3L));
    private final List<KeyValue<String, Integer>> expectedAggKeyValues =
        Arrays.asList(KeyValue.pair("A", 9), KeyValue.pair("B", 9), KeyValue.pair("C", 9));
    private final List<KeyValue<String, String>> expectedReduceKeyValues =
        Arrays.asList(KeyValue.pair("A", "foo:bar:baz"), KeyValue.pair("B", "foo:bar:baz"), KeyValue.pair("C", "foo:bar:baz"));
    private final List<KeyValue<String, String>> expectedJoinKeyValues =
        Arrays.asList(KeyValue.pair("A", "foo:3"), KeyValue.pair("A", "bar:3"), KeyValue.pair("A", "baz:3"));
    private final List<String> expectedCollectedProcessorValues =
        Arrays.asList("FOO", "BAR", "BAZ");

    @Before
    public void setUp() {
        streamsConfiguration = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
        streamsConfiguration.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, Integer.toString(1024 * 10));
        streamsConfiguration.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Integer.toString(5000));

        processorValueCollector.clear();
    }

    @After
    public void tearDown() {
        try {
            topologyTestDriver.close();
        } catch (final RuntimeException e) {
            log.warn("The following exception was thrown while trying to close the TopologyTestDriver (note that " +
                "KAFKA-6647 causes this when running on Windows):", e);
        }
    }

    @Test
    public void shouldSendCorrectRecords_OPTIMIZED() {
        runTest(StreamsConfig.OPTIMIZE, ONE_REPARTITION_TOPIC);
    }

    @Test
    public void shouldSendCorrectResults_NO_OPTIMIZATION() {
        runTest(StreamsConfig.NO_OPTIMIZATION, FOUR_REPARTITION_TOPICS);
    }


    private void runTest(final String optimizationConfig, final int expectedNumberRepartitionTopics) {

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> sourceStream =
            builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("sourceStream"));

        final KStream<String, String> mappedStream = sourceStream
            .map((k, v) -> KeyValue.pair(k.toUpperCase(Locale.getDefault()), v), Named.as("source-map"));

        mappedStream
            .filter((k, v) -> k.equals("B"), Named.as("process-filter"))
            .mapValues(v -> v.toUpperCase(Locale.getDefault()), Named.as("process-mapValues"))
            .process(() -> new SimpleProcessor(processorValueCollector), Named.as("process"));

        final KStream<String, Long> countStream = mappedStream
            .groupByKey(Grouped.as("count-groupByKey"))
            .count(Named.as("count"), Materialized.<String, Long>as(Stores.inMemoryKeyValueStore("count-store"))
                                                                .withKeySerde(Serdes.String())
                                                                .withValueSerde(Serdes.Long()))
            .toStream(Named.as("count-toStream"));

        countStream.to(COUNT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()).withName("count-to"));

        mappedStream
            .groupByKey(Grouped.as("aggregate-groupByKey"))
            .aggregate(initializer,
                       aggregator,
                       Named.as("aggregate"),
                       Materialized.<String, Integer>as(Stores.inMemoryKeyValueStore("aggregate-store"))
                                                    .withKeySerde(Serdes.String())
                                                    .withValueSerde(Serdes.Integer()))
            .toStream(Named.as("aggregate-toStream"))
            .to(AGGREGATION_TOPIC, Produced.with(Serdes.String(), Serdes.Integer()).withName("reduce-to"));

        // adding operators for case where the repartition node is further downstream
        mappedStream
            .filter((k, v) -> true, Named.as("reduce-filter"))
            .peek((k, v) -> System.out.println(k + ":" + v), Named.as("reduce-peek"))
            .groupByKey(Grouped.as("reduce-groupByKey"))
            .reduce(reducer,
                    Named.as("reducer"),
                    Materialized.as(Stores.inMemoryKeyValueStore("reduce-store")))
            .toStream(Named.as("reduce-toStream"))
            .to(REDUCE_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        mappedStream
            .filter((k, v) -> k.equals("A"), Named.as("join-filter"))
            .join(countStream, (v1, v2) -> v1 + ":" + v2.toString(),
                  JoinWindows.of(ofMillis(5000)),
                  StreamJoined.<String, String, Long>with(Stores.inMemoryWindowStore("join-store", ofDays(1), ofMillis(10000), true),
                                                          Stores.inMemoryWindowStore("other-join-store",  ofDays(1), ofMillis(10000), true))
                                                    .withName("join")
                                                    .withKeySerde(Serdes.String())
                                                    .withValueSerde(Serdes.String())
                                                    .withOtherValueSerde(Serdes.Long()))
            .to(JOINED_TOPIC, Produced.as("join-to"));

        streamsConfiguration.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, optimizationConfig);
        final Topology topology = builder.build(streamsConfiguration);

        topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration);

        final TestInputTopic<String, String> inputTopicA = topologyTestDriver.createInputTopic(INPUT_TOPIC, stringSerializer, stringSerializer);
        final TestOutputTopic<String, Long> countOutputTopic = topologyTestDriver.createOutputTopic(COUNT_TOPIC, stringDeserializer, new LongDeserializer());
        final TestOutputTopic<String, Integer> aggregationOutputTopic = topologyTestDriver.createOutputTopic(AGGREGATION_TOPIC, stringDeserializer, new IntegerDeserializer());
        final TestOutputTopic<String, String> reduceOutputTopic = topologyTestDriver.createOutputTopic(REDUCE_TOPIC, stringDeserializer, stringDeserializer);
        final TestOutputTopic<String, String> joinedOutputTopic = topologyTestDriver.createOutputTopic(JOINED_TOPIC, stringDeserializer, stringDeserializer);

        inputTopicA.pipeKeyValueList(getKeyValues());

        // Verify the topology
        final String topologyString = topology.describe().toString();
        if (optimizationConfig.equals(StreamsConfig.OPTIMIZE)) {
            assertEquals(EXPECTED_OPTIMIZED_TOPOLOGY, topologyString);
        } else {
            assertEquals(EXPECTED_UNOPTIMIZED_TOPOLOGY, topologyString);
        }

        // Verify the number of repartition topics
        assertEquals(expectedNumberRepartitionTopics, getCountOfRepartitionTopicsFound(topologyString));

        // Verify the values collected by the processor
        assertThat(3, equalTo(processorValueCollector.size()));
        assertThat(processorValueCollector, equalTo(expectedCollectedProcessorValues));

        // Verify the expected output
        assertThat(countOutputTopic.readKeyValuesToMap(), equalTo(keyValueListToMap(expectedCountKeyValues)));
        assertThat(aggregationOutputTopic.readKeyValuesToMap(), equalTo(keyValueListToMap(expectedAggKeyValues)));
        assertThat(reduceOutputTopic.readKeyValuesToMap(), equalTo(keyValueListToMap(expectedReduceKeyValues)));
        assertThat(joinedOutputTopic.readKeyValuesToMap(), equalTo(keyValueListToMap(expectedJoinKeyValues)));
    }

    private <K, V> Map<K, V> keyValueListToMap(final List<KeyValue<K, V>> keyValuePairs) {
        final Map<K, V> map = new HashMap<>();
        for (final KeyValue<K, V> pair : keyValuePairs) {
            map.put(pair.key, pair.value);
        }
        return map;
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
                                                                  + "    Source: sourceStream (topics: [input])\n"
                                                                  + "      --> source-map\n"
                                                                  + "    Processor: source-map (stores: [])\n"
                                                                  + "      --> process-filter, count-groupByKey-repartition-filter\n"
                                                                  + "      <-- sourceStream\n"
                                                                  + "    Processor: process-filter (stores: [])\n"
                                                                  + "      --> process-mapValues\n"
                                                                  + "      <-- source-map\n"
                                                                  + "    Processor: count-groupByKey-repartition-filter (stores: [])\n"
                                                                  + "      --> count-groupByKey-repartition-sink\n"
                                                                  + "      <-- source-map\n"
                                                                  + "    Processor: process-mapValues (stores: [])\n"
                                                                  + "      --> process\n"
                                                                  + "      <-- process-filter\n"
                                                                  + "    Sink: count-groupByKey-repartition-sink (topic: count-groupByKey-repartition)\n"
                                                                  + "      <-- count-groupByKey-repartition-filter\n"
                                                                  + "    Processor: process (stores: [])\n"
                                                                  + "      --> none\n"
                                                                  + "      <-- process-mapValues\n"
                                                                  + "\n"
                                                                  + "  Sub-topology: 1\n"
                                                                  + "    Source: count-groupByKey-repartition-source (topics: [count-groupByKey-repartition])\n"
                                                                  + "      --> aggregate, count, join-filter, reduce-filter\n"
                                                                  + "    Processor: count (stores: [count-store])\n"
                                                                  + "      --> count-toStream\n"
                                                                  + "      <-- count-groupByKey-repartition-source\n"
                                                                  + "    Processor: count-toStream (stores: [])\n"
                                                                  + "      --> join-other-windowed, count-to\n"
                                                                  + "      <-- count\n"
                                                                  + "    Processor: join-filter (stores: [])\n"
                                                                  + "      --> join-this-windowed\n"
                                                                  + "      <-- count-groupByKey-repartition-source\n"
                                                                  + "    Processor: reduce-filter (stores: [])\n"
                                                                  + "      --> reduce-peek\n"
                                                                  + "      <-- count-groupByKey-repartition-source\n"
                                                                  + "    Processor: join-other-windowed (stores: [other-join-store])\n"
                                                                  + "      --> join-other-join\n"
                                                                  + "      <-- count-toStream\n"
                                                                  + "    Processor: join-this-windowed (stores: [join-store])\n"
                                                                  + "      --> join-this-join\n"
                                                                  + "      <-- join-filter\n"
                                                                  + "    Processor: reduce-peek (stores: [])\n"
                                                                  + "      --> reducer\n"
                                                                  + "      <-- reduce-filter\n"
                                                                  + "    Processor: aggregate (stores: [aggregate-store])\n"
                                                                  + "      --> aggregate-toStream\n"
                                                                  + "      <-- count-groupByKey-repartition-source\n"
                                                                  + "    Processor: join-other-join (stores: [join-store])\n"
                                                                  + "      --> join-merge\n"
                                                                  + "      <-- join-other-windowed\n"
                                                                  + "    Processor: join-this-join (stores: [other-join-store])\n"
                                                                  + "      --> join-merge\n"
                                                                  + "      <-- join-this-windowed\n"
                                                                  + "    Processor: reducer (stores: [reduce-store])\n"
                                                                  + "      --> reduce-toStream\n"
                                                                  + "      <-- reduce-peek\n"
                                                                  + "    Processor: aggregate-toStream (stores: [])\n"
                                                                  + "      --> reduce-to\n"
                                                                  + "      <-- aggregate\n"
                                                                  + "    Processor: join-merge (stores: [])\n"
                                                                  + "      --> join-to\n"
                                                                  + "      <-- join-this-join, join-other-join\n"
                                                                  + "    Processor: reduce-toStream (stores: [])\n"
                                                                  + "      --> KSTREAM-SINK-0000000023\n"
                                                                  + "      <-- reducer\n"
                                                                  + "    Sink: KSTREAM-SINK-0000000023 (topic: outputTopic_2)\n"
                                                                  + "      <-- reduce-toStream\n"
                                                                  + "    Sink: count-to (topic: outputTopic_0)\n"
                                                                  + "      <-- count-toStream\n"
                                                                  + "    Sink: join-to (topic: joinedOutputTopic)\n"
                                                                  + "      <-- join-merge\n"
                                                                  + "    Sink: reduce-to (topic: outputTopic_1)\n"
                                                                  + "      <-- aggregate-toStream\n\n";




    private static final String EXPECTED_UNOPTIMIZED_TOPOLOGY = "Topologies:\n" +
        "   Sub-topology: 0\n" +
        "    Source: sourceStream (topics: [input])\n" +
        "      --> source-map\n" +
        "    Processor: source-map (stores: [])\n" +
        "      --> reduce-filter, process-filter, aggregate-groupByKey-repartition-filter, count-groupByKey-repartition-filter, join-filter\n" +
        "      <-- sourceStream\n" +
        "    Processor: reduce-filter (stores: [])\n" +
        "      --> reduce-peek\n" +
        "      <-- source-map\n" +
        "    Processor: join-filter (stores: [])\n" +
        "      --> join-left-repartition-filter\n" +
        "      <-- source-map\n" +
        "    Processor: process-filter (stores: [])\n" +
        "      --> process-mapValues\n" +
        "      <-- source-map\n" +
        "    Processor: reduce-peek (stores: [])\n" +
        "      --> reduce-groupByKey-repartition-filter\n" +
        "      <-- reduce-filter\n" +
        "    Processor: aggregate-groupByKey-repartition-filter (stores: [])\n" +
        "      --> aggregate-groupByKey-repartition-sink\n" +
        "      <-- source-map\n" +
        "    Processor: count-groupByKey-repartition-filter (stores: [])\n" +
        "      --> count-groupByKey-repartition-sink\n" +
        "      <-- source-map\n" +
        "    Processor: join-left-repartition-filter (stores: [])\n" +
        "      --> join-left-repartition-sink\n" +
        "      <-- join-filter\n" +
        "    Processor: process-mapValues (stores: [])\n" +
        "      --> process\n" +
        "      <-- process-filter\n" +
        "    Processor: reduce-groupByKey-repartition-filter (stores: [])\n" +
        "      --> reduce-groupByKey-repartition-sink\n" +
        "      <-- reduce-peek\n" +
        "    Sink: aggregate-groupByKey-repartition-sink (topic: aggregate-groupByKey-repartition)\n" +
        "      <-- aggregate-groupByKey-repartition-filter\n" +
        "    Sink: count-groupByKey-repartition-sink (topic: count-groupByKey-repartition)\n" +
        "      <-- count-groupByKey-repartition-filter\n" +
        "    Sink: join-left-repartition-sink (topic: join-left-repartition)\n" +
        "      <-- join-left-repartition-filter\n" +
        "    Processor: process (stores: [])\n" +
        "      --> none\n" +
        "      <-- process-mapValues\n" +
        "    Sink: reduce-groupByKey-repartition-sink (topic: reduce-groupByKey-repartition)\n" +
        "      <-- reduce-groupByKey-repartition-filter\n" +
        "\n" +
        "  Sub-topology: 1\n" +
        "    Source: count-groupByKey-repartition-source (topics: [count-groupByKey-repartition])\n" +
        "      --> count\n" +
        "    Processor: count (stores: [count-store])\n" +
        "      --> count-toStream\n" +
        "      <-- count-groupByKey-repartition-source\n" +
        "    Processor: count-toStream (stores: [])\n" +
        "      --> join-other-windowed, count-to\n" +
        "      <-- count\n" +
        "    Source: join-left-repartition-source (topics: [join-left-repartition])\n" +
        "      --> join-this-windowed\n" +
        "    Processor: join-other-windowed (stores: [other-join-store])\n" +
        "      --> join-other-join\n" +
        "      <-- count-toStream\n" +
        "    Processor: join-this-windowed (stores: [join-store])\n" +
        "      --> join-this-join\n" +
        "      <-- join-left-repartition-source\n" +
        "    Processor: join-other-join (stores: [join-store])\n" +
        "      --> join-merge\n" +
        "      <-- join-other-windowed\n" +
        "    Processor: join-this-join (stores: [other-join-store])\n" +
        "      --> join-merge\n" +
        "      <-- join-this-windowed\n" +
        "    Processor: join-merge (stores: [])\n" +
        "      --> join-to\n" +
        "      <-- join-this-join, join-other-join\n" +
        "    Sink: count-to (topic: outputTopic_0)\n" +
        "      <-- count-toStream\n" +
        "    Sink: join-to (topic: joinedOutputTopic)\n" +
        "      <-- join-merge\n" +
        "\n" +
        "  Sub-topology: 2\n" +
        "    Source: aggregate-groupByKey-repartition-source (topics: [aggregate-groupByKey-repartition])\n" +
        "      --> aggregate\n" +
        "    Processor: aggregate (stores: [aggregate-store])\n" +
        "      --> aggregate-toStream\n" +
        "      <-- aggregate-groupByKey-repartition-source\n" +
        "    Processor: aggregate-toStream (stores: [])\n" +
        "      --> reduce-to\n" +
        "      <-- aggregate\n" +
        "    Sink: reduce-to (topic: outputTopic_1)\n" +
        "      <-- aggregate-toStream\n" +
        "\n" +
        "  Sub-topology: 3\n" +
        "    Source: reduce-groupByKey-repartition-source (topics: [reduce-groupByKey-repartition])\n" +
        "      --> reducer\n" +
        "    Processor: reducer (stores: [reduce-store])\n" +
        "      --> reduce-toStream\n" +
        "      <-- reduce-groupByKey-repartition-source\n" +
        "    Processor: reduce-toStream (stores: [])\n" +
        "      --> KSTREAM-SINK-0000000023\n" +
        "      <-- reducer\n" +
        "    Sink: KSTREAM-SINK-0000000023 (topic: outputTopic_2)\n" +
        "      <-- reduce-toStream\n\n";

}
