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
        streamsConfiguration = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
        streamsConfiguration.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, Integer.toString(1024 * 10));
        streamsConfiguration.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Integer.toString(5000));
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

        streamsConfiguration.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, optimizationConfig);

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
                                                              + "    Source: sourceAStream (topics: [inputA])\n"
                                                              + "      --> mappedAStream\n"
                                                              + "    Source: sourceBStream (topics: [inputB])\n"
                                                              + "      --> mappedBStream\n"
                                                              + "    Processor: mappedAStream (stores: [])\n"
                                                              + "      --> mergedStream\n"
                                                              + "      <-- sourceAStream\n"
                                                              + "    Processor: mappedBStream (stores: [])\n"
                                                              + "      --> mergedStream\n"
                                                              + "      <-- sourceBStream\n"
                                                              + "    Processor: mergedStream (stores: [])\n"
                                                              + "      --> long-groupByKey-repartition-filter\n"
                                                              + "      <-- mappedAStream, mappedBStream\n"
                                                              + "    Processor: long-groupByKey-repartition-filter (stores: [])\n"
                                                              + "      --> long-groupByKey-repartition-sink\n"
                                                              + "      <-- mergedStream\n"
                                                              + "    Sink: long-groupByKey-repartition-sink (topic: long-groupByKey-repartition)\n"
                                                              + "      <-- long-groupByKey-repartition-filter\n"
                                                              + "\n"
                                                              + "  Sub-topology: 1\n"
                                                              + "    Source: long-groupByKey-repartition-source (topics: [long-groupByKey-repartition])\n"
                                                              + "      --> long-count, string-count\n"
                                                              + "    Processor: string-count (stores: [string-store])\n"
                                                              + "      --> string-toStream\n"
                                                              + "      <-- long-groupByKey-repartition-source\n"
                                                              + "    Processor: long-count (stores: [long-store])\n"
                                                              + "      --> long-toStream\n"
                                                              + "      <-- long-groupByKey-repartition-source\n"
                                                              + "    Processor: string-toStream (stores: [])\n"
                                                              + "      --> string-mapValues\n"
                                                              + "      <-- string-count\n"
                                                              + "    Processor: long-toStream (stores: [])\n"
                                                              + "      --> long-to\n"
                                                              + "      <-- long-count\n"
                                                              + "    Processor: string-mapValues (stores: [])\n"
                                                              + "      --> string-to\n"
                                                              + "      <-- string-toStream\n"
                                                              + "    Sink: long-to (topic: outputTopic_0)\n"
                                                              + "      <-- long-toStream\n"
                                                              + "    Sink: string-to (topic: outputTopic_1)\n"
                                                              + "      <-- string-mapValues\n\n";


    private static final String EXPECTED_UNOPTIMIZED_TOPOLOGY = "Topologies:\n"
                                                                    + "   Sub-topology: 0\n"
                                                                    + "    Source: sourceAStream (topics: [inputA])\n"
                                                                    + "      --> mappedAStream\n"
                                                                    + "    Source: sourceBStream (topics: [inputB])\n"
                                                                    + "      --> mappedBStream\n"
                                                                    + "    Processor: mappedAStream (stores: [])\n"
                                                                    + "      --> mergedStream\n"
                                                                    + "      <-- sourceAStream\n"
                                                                    + "    Processor: mappedBStream (stores: [])\n"
                                                                    + "      --> mergedStream\n"
                                                                    + "      <-- sourceBStream\n"
                                                                    + "    Processor: mergedStream (stores: [])\n"
                                                                    + "      --> long-groupByKey-repartition-filter, string-groupByKey-repartition-filter\n"
                                                                    + "      <-- mappedAStream, mappedBStream\n"
                                                                    + "    Processor: long-groupByKey-repartition-filter (stores: [])\n"
                                                                    + "      --> long-groupByKey-repartition-sink\n"
                                                                    + "      <-- mergedStream\n"
                                                                    + "    Processor: string-groupByKey-repartition-filter (stores: [])\n"
                                                                    + "      --> string-groupByKey-repartition-sink\n"
                                                                    + "      <-- mergedStream\n"
                                                                    + "    Sink: long-groupByKey-repartition-sink (topic: long-groupByKey-repartition)\n"
                                                                    + "      <-- long-groupByKey-repartition-filter\n"
                                                                    + "    Sink: string-groupByKey-repartition-sink (topic: string-groupByKey-repartition)\n"
                                                                    + "      <-- string-groupByKey-repartition-filter\n"
                                                                    + "\n"
                                                                    + "  Sub-topology: 1\n"
                                                                    + "    Source: long-groupByKey-repartition-source (topics: [long-groupByKey-repartition])\n"
                                                                    + "      --> long-count\n"
                                                                    + "    Processor: long-count (stores: [long-store])\n"
                                                                    + "      --> long-toStream\n"
                                                                    + "      <-- long-groupByKey-repartition-source\n"
                                                                    + "    Processor: long-toStream (stores: [])\n"
                                                                    + "      --> long-to\n"
                                                                    + "      <-- long-count\n"
                                                                    + "    Sink: long-to (topic: outputTopic_0)\n"
                                                                    + "      <-- long-toStream\n"
                                                                    + "\n"
                                                                    + "  Sub-topology: 2\n"
                                                                    + "    Source: string-groupByKey-repartition-source (topics: [string-groupByKey-repartition])\n"
                                                                    + "      --> string-count\n"
                                                                    + "    Processor: string-count (stores: [string-store])\n"
                                                                    + "      --> string-toStream\n"
                                                                    + "      <-- string-groupByKey-repartition-source\n"
                                                                    + "    Processor: string-toStream (stores: [])\n"
                                                                    + "      --> string-mapValues\n"
                                                                    + "      <-- string-count\n"
                                                                    + "    Processor: string-mapValues (stores: [])\n"
                                                                    + "      --> string-to\n"
                                                                    + "      <-- string-toStream\n"
                                                                    + "    Sink: string-to (topic: outputTopic_1)\n"
                                                                    + "      <-- string-mapValues\n\n";

}
