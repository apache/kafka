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
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.StreamsTestUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class RepartitionWithMergeOptimizingTest {

    private final Logger log = LoggerFactory.getLogger(RepartitionWithMergeOptimizingTest.class);

    private static final String INPUT_A_TOPIC = "inputA";
    private static final String INPUT_B_TOPIC = "inputB";
    private static final String COUNT_TOPIC = "outputTopic_0";
    private static final String STRING_COUNT_TOPIC = "outputTopic_1";

    private static final int ONE_REPARTITION_TOPIC = 1;
    private static final int TWO_REPARTITION_TOPICS = 2;

    private final Serializer<String> stringSerializer = new StringSerializer();
    private final Deserializer<String> stringDeserializer = new StringDeserializer();

    private final Pattern repartitionTopicPattern = Pattern.compile("Sink: .*-repartition");

    private Properties streamsConfiguration;
    private TopologyTestDriver topologyTestDriver;

    private final List<KeyValue<String, Long>> expectedCountKeyValues =
        Arrays.asList(KeyValue.pair("A", 6L), KeyValue.pair("B", 6L), KeyValue.pair("C", 6L));
    private final List<KeyValue<String, String>> expectedStringCountKeyValues =
        Arrays.asList(KeyValue.pair("A", "6"), KeyValue.pair("B", "6"), KeyValue.pair("C", "6"));

    @Before
    public void setUp() {
        final Properties props = new Properties();
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 1024 * 10);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000);

        streamsConfiguration = StreamsTestUtils.getStreamsConfig(
            "maybe-optimized-with-merge-test-app",
            "dummy-bootstrap-servers-config",
            Serdes.String().getClass().getName(),
            Serdes.String().getClass().getName(),
            props);
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
        runTest(StreamsConfig.NO_OPTIMIZATION, TWO_REPARTITION_TOPICS);
    }


    private void runTest(final String optimizationConfig, final int expectedNumberRepartitionTopics) {

        streamsConfiguration.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION, optimizationConfig);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> sourceAStream =
            builder.stream(INPUT_A_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("sourceAStream"));

        final KStream<String, String> sourceBStream =
            builder.stream(INPUT_B_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("sourceBStream"));

        final KStream<String, String> mappedAStream =
            sourceAStream.map((k, v) -> KeyValue.pair(v.split(":")[0], v), Named.as("mappedAStream"));
        final KStream<String, String> mappedBStream =
            sourceBStream.map((k, v) -> KeyValue.pair(v.split(":")[0], v), Named.as("mappedBStream"));

        final KStream<String, String> mergedStream = mappedAStream.merge(mappedBStream, Named.as("mergedStream"));

        mergedStream
            .groupByKey(Grouped.as("long-groupByKey"))
            .count(Named.as("long-count"), Materialized.as(Stores.inMemoryKeyValueStore("long-store")))
            .toStream(Named.as("long-toStream"))
            .to(COUNT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()).withName("long-to"));

        mergedStream
            .groupByKey(Grouped.as("string-groupByKey"))
            .count(Named.as("string-count"), Materialized.as(Stores.inMemoryKeyValueStore("string-store")))
            .toStream(Named.as("string-toStream"))
            .mapValues(v -> v.toString(), Named.as("string-mapValues"))
            .to(STRING_COUNT_TOPIC, Produced.with(Serdes.String(), Serdes.String()).withName("string-to"));

        final Topology topology = builder.build(streamsConfiguration);

        topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration);

        final TestInputTopic<String, String> inputTopicA = topologyTestDriver.createInputTopic(INPUT_A_TOPIC, stringSerializer, stringSerializer);
        final TestInputTopic<String, String> inputTopicB = topologyTestDriver.createInputTopic(INPUT_B_TOPIC, stringSerializer, stringSerializer);

        final TestOutputTopic<String, Long> countOutputTopic = topologyTestDriver.createOutputTopic(COUNT_TOPIC, stringDeserializer, new LongDeserializer());
        final TestOutputTopic<String, String> stringCountOutputTopic = topologyTestDriver.createOutputTopic(STRING_COUNT_TOPIC, stringDeserializer, stringDeserializer);

        inputTopicA.pipeKeyValueList(getKeyValues());
        inputTopicB.pipeKeyValueList(getKeyValues());

        final String topologyString = topology.describe().toString();

        // Verify the topology
        if (optimizationConfig.equals(StreamsConfig.OPTIMIZE)) {
            assertEquals(EXPECTED_OPTIMIZED_TOPOLOGY, topologyString);
        } else {
            assertEquals(EXPECTED_UNOPTIMIZED_TOPOLOGY, topologyString);
        }

        // Verify the number of repartition topics
        assertEquals(expectedNumberRepartitionTopics, getCountOfRepartitionTopicsFound(topologyString));

        // Verify the expected output
        assertThat(countOutputTopic.readKeyValuesToMap(), equalTo(keyValueListToMap(expectedCountKeyValues)));
        assertThat(stringCountOutputTopic.readKeyValuesToMap(), equalTo(keyValueListToMap(expectedStringCountKeyValues)));
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
        final String[] keys = new String[]{"X", "Y", "Z"};
        final String[] values = new String[]{"A:foo", "B:foo", "C:foo"};
        for (final String key : keys) {
            for (final String value : values) {
                keyValueList.add(KeyValue.pair(key, value));
            }
        }
        return keyValueList;
    }

    private static final String EXPECTED_OPTIMIZED_TOPOLOGY = "Topologies:\n"
                                                              + "   Sub-topology: 0\n"
                                                              + "    Source: KSTREAM-SOURCE-0000000000 (topics: [inputA], keySerde: StringDeserializer, valueSerde: StringDeserializer)\n"
                                                              + "      --> KSTREAM-MAP-0000000002\n"
                                                              + "    Source: KSTREAM-SOURCE-0000000001 (topics: [inputB], keySerde: StringDeserializer, valueSerde: StringDeserializer)\n"
                                                              + "      --> KSTREAM-MAP-0000000003\n"
                                                              + "    Processor: KSTREAM-MAP-0000000002 (stores: [])\n"
                                                              + "      --> KSTREAM-MERGE-0000000004\n"
                                                              + "      <-- KSTREAM-SOURCE-0000000000\n"
                                                              + "    Processor: KSTREAM-MAP-0000000003 (stores: [])\n"
                                                              + "      --> KSTREAM-MERGE-0000000004\n"
                                                              + "      <-- KSTREAM-SOURCE-0000000001\n"
                                                              + "    Processor: KSTREAM-MERGE-0000000004 (stores: [])\n"
                                                              + "      --> KSTREAM-FILTER-0000000021\n"
                                                              + "      <-- KSTREAM-MAP-0000000002, KSTREAM-MAP-0000000003\n"
                                                              + "    Processor: KSTREAM-FILTER-0000000021 (stores: [])\n"
                                                              + "      --> KSTREAM-SINK-0000000020\n"
                                                              + "      <-- KSTREAM-MERGE-0000000004\n"
                                                              + "    Sink: KSTREAM-SINK-0000000020 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition, keySerde: null, valueSerde: null)\n"
                                                              + "      <-- KSTREAM-FILTER-0000000021\n"
                                                              + "\n"
                                                              + "  Sub-topology: 1\n"
                                                              + "    Source: KSTREAM-SOURCE-0000000022 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition], keySerde: null, valueSerde: null)\n"
                                                              + "      --> KSTREAM-AGGREGATE-0000000006, KSTREAM-AGGREGATE-0000000013\n"
                                                              + "    Processor: KSTREAM-AGGREGATE-0000000013 (stores: [(KSTREAM-AGGREGATE-STATE-STORE-0000000012, serdes: [null, LongSerde])])\n"
                                                              + "      --> KTABLE-TOSTREAM-0000000017\n"
                                                              + "      <-- KSTREAM-SOURCE-0000000022\n"
                                                              + "    Processor: KSTREAM-AGGREGATE-0000000006 (stores: [(KSTREAM-AGGREGATE-STATE-STORE-0000000005, serdes: [null, LongSerde])])\n"
                                                              + "      --> KTABLE-TOSTREAM-0000000010\n"
                                                              + "      <-- KSTREAM-SOURCE-0000000022\n"
                                                              + "    Processor: KTABLE-TOSTREAM-0000000017 (stores: [])\n"
                                                              + "      --> KSTREAM-MAPVALUES-0000000018\n"
                                                              + "      <-- KSTREAM-AGGREGATE-0000000013\n"
                                                              + "    Processor: KSTREAM-MAPVALUES-0000000018 (stores: [])\n"
                                                              + "      --> KSTREAM-SINK-0000000019\n"
                                                              + "      <-- KTABLE-TOSTREAM-0000000017\n"
                                                              + "    Processor: KTABLE-TOSTREAM-0000000010 (stores: [])\n"
                                                              + "      --> KSTREAM-SINK-0000000011\n"
                                                              + "      <-- KSTREAM-AGGREGATE-0000000006\n"
                                                              + "    Sink: KSTREAM-SINK-0000000011 (topic: outputTopic_0, keySerde: StringSerializer, valueSerde: LongSerializer)\n"
                                                              + "      <-- KTABLE-TOSTREAM-0000000010\n"
                                                              + "    Sink: KSTREAM-SINK-0000000019 (topic: outputTopic_1, keySerde: StringSerializer, valueSerde: StringSerializer)\n"
                                                              + "      <-- KSTREAM-MAPVALUES-0000000018\n\n";

    private static final String EXPECTED_UNOPTIMIZED_TOPOLOGY = "Topologies:\n"
                                                                + "   Sub-topology: 0\n"
                                                                + "    Source: KSTREAM-SOURCE-0000000000 (topics: [inputA], keySerde: StringDeserializer, valueSerde: StringDeserializer)\n"
                                                                + "      --> KSTREAM-MAP-0000000002\n"
                                                                + "    Source: KSTREAM-SOURCE-0000000001 (topics: [inputB], keySerde: StringDeserializer, valueSerde: StringDeserializer)\n"
                                                                + "      --> KSTREAM-MAP-0000000003\n"
                                                                + "    Processor: KSTREAM-MAP-0000000002 (stores: [])\n"
                                                                + "      --> KSTREAM-MERGE-0000000004\n"
                                                                + "      <-- KSTREAM-SOURCE-0000000000\n"
                                                                + "    Processor: KSTREAM-MAP-0000000003 (stores: [])\n"
                                                                + "      --> KSTREAM-MERGE-0000000004\n"
                                                                + "      <-- KSTREAM-SOURCE-0000000001\n"
                                                                + "    Processor: KSTREAM-MERGE-0000000004 (stores: [])\n"
                                                                + "      --> KSTREAM-FILTER-0000000008, KSTREAM-FILTER-0000000015\n"
                                                                + "      <-- KSTREAM-MAP-0000000002, KSTREAM-MAP-0000000003\n"
                                                                + "    Processor: KSTREAM-FILTER-0000000008 (stores: [])\n"
                                                                + "      --> KSTREAM-SINK-0000000007\n"
                                                                + "      <-- KSTREAM-MERGE-0000000004\n"
                                                                + "    Processor: KSTREAM-FILTER-0000000015 (stores: [])\n"
                                                                + "      --> KSTREAM-SINK-0000000014\n"
                                                                + "      <-- KSTREAM-MERGE-0000000004\n"
                                                                + "    Sink: KSTREAM-SINK-0000000007 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition, keySerde: null, valueSerde: null)\n"
                                                                + "      <-- KSTREAM-FILTER-0000000008\n"
                                                                + "    Sink: KSTREAM-SINK-0000000014 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000012-repartition, keySerde: null, valueSerde: null)\n"
                                                                + "      <-- KSTREAM-FILTER-0000000015\n"
                                                                + "\n"
                                                                + "  Sub-topology: 1\n"
                                                                + "    Source: KSTREAM-SOURCE-0000000009 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition], keySerde: null, valueSerde: null)\n"
                                                                + "      --> KSTREAM-AGGREGATE-0000000006\n"
                                                                + "    Processor: KSTREAM-AGGREGATE-0000000006 (stores: [(KSTREAM-AGGREGATE-STATE-STORE-0000000005, serdes: [null, LongSerde])])\n"
                                                                + "      --> KTABLE-TOSTREAM-0000000010\n"
                                                                + "      <-- KSTREAM-SOURCE-0000000009\n"
                                                                + "    Processor: KTABLE-TOSTREAM-0000000010 (stores: [])\n"
                                                                + "      --> KSTREAM-SINK-0000000011\n"
                                                                + "      <-- KSTREAM-AGGREGATE-0000000006\n"
                                                                + "    Sink: KSTREAM-SINK-0000000011 (topic: outputTopic_0, keySerde: StringSerializer, valueSerde: LongSerializer)\n"
                                                                + "      <-- KTABLE-TOSTREAM-0000000010\n"
                                                                + "\n"
                                                                + "  Sub-topology: 2\n"
                                                                + "    Source: KSTREAM-SOURCE-0000000016 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000012-repartition], keySerde: null, valueSerde: null)\n"
                                                                + "      --> KSTREAM-AGGREGATE-0000000013\n"
                                                                + "    Processor: KSTREAM-AGGREGATE-0000000013 (stores: [(KSTREAM-AGGREGATE-STATE-STORE-0000000012, serdes: [null, LongSerde])])\n"
                                                                + "      --> KTABLE-TOSTREAM-0000000017\n"
                                                                + "      <-- KSTREAM-SOURCE-0000000016\n"
                                                                + "    Processor: KTABLE-TOSTREAM-0000000017 (stores: [])\n"
                                                                + "      --> KSTREAM-MAPVALUES-0000000018\n"
                                                                + "      <-- KSTREAM-AGGREGATE-0000000013\n"
                                                                + "    Processor: KSTREAM-MAPVALUES-0000000018 (stores: [])\n"
                                                                + "      --> KSTREAM-SINK-0000000019\n"
                                                                + "      <-- KTABLE-TOSTREAM-0000000017\n"
                                                                + "    Sink: KSTREAM-SINK-0000000019 (topic: outputTopic_1, keySerde: StringSerializer, valueSerde: StringSerializer)\n"
                                                                + "      <-- KSTREAM-MAPVALUES-0000000018\n\n";
}
