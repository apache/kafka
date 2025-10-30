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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
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
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.StreamsTestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test demonstrating the current limitation of merge.repartition.topic optimization
 * when value-changing operations are present.
 *
 * CURRENT BEHAVIOR:
 * - Optimization stops at value-changing operations (mapValues, flatMapValues, etc.)
 * - Cannot merge repartition topics that are separated by value-changing operations
 * - Results in multiple repartition topics even when they could be merged
 *
 * PROPOSED ENHANCEMENT (KAFKA-7138):
 * - Track input/output serdes at each node
 * - Allow pushing repartition upstream past value-changing operations
 * - Switch to upstream serdes when merging repartitions
 * - Would enable merging repartitions across value-changing boundaries
 */
public class RepartitionMergeAcrossValueChangingOperationTest {

    /// key-changing operation -> repartition
    /// value-changing operation -> cannot reorder repartition past this point

    /// Critical locations to debug:
    /// - InternalStreamBuilder.getKeyChangingParentNode() - The decision point
    /// - InternalStreamBuilder.mergeRepartitionTopics() - Entry point for optimization
    /// - isValueChangingOperation() - Flag checking
    /// - setValueChangingOperation() - Where flags are set


    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC_1 = "output1";
    private static final String OUTPUT_TOPIC_2 = "output2";

    private final Serializer<String> stringSerializer = new StringSerializer();
    private final Deserializer<String> stringDeserializer = new StringDeserializer();
    private final Deserializer<Integer> integerDeserializer = new IntegerDeserializer();

    private final Pattern repartitionTopicPattern = Pattern.compile("Sink: .*-repartition");

    private Properties streamsConfiguration;
    private TopologyTestDriver topologyTestDriver;

    @BeforeEach
    public void before() {
        streamsConfiguration = StreamsTestUtils.getStreamsConfig(
            Serdes.String(),
            Serdes.String()
        );
    }

    @AfterEach
    public void after() {
        if (topologyTestDriver != null) {
            topologyTestDriver.close();
        }
    }

    /**
     * Test Case 1: Demonstrates current limitation
     *
     * Topology:
     * source[String, Integer]
     *   → map(key-changing: lowercase key)
     *     → mapValues(int → string) [VALUE-CHANGING]
     *       → groupByKey → count → output1
     *       → filter → groupByKey → count → output2
     *
     * CURRENT BEHAVIOR:
     * - Creates 2 repartition topics (both with String value serde)
     * - Optimization blocked by mapValues operation
     *
     * DESIRED BEHAVIOR (with enhancement):
     * - Could create 1 repartition topic (with Integer value serde from before mapValues)
     * - Push repartition above mapValues
     * - Both branches read from same repartition and apply mapValues afterward
     */
    @Test
    public void shouldCreateTwoRepartitionTopicsDueToValueChangingOperation() {
        streamsConfiguration.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        final StreamsBuilder builder = new StreamsBuilder();

        // Source produces <String, Integer>
        final KStream<String, Integer> source = builder.stream(
            INPUT_TOPIC,
            Consumed.with(Serdes.String(), Serdes.Integer())
        );

        // KEY-CHANGING operation to trigger repartitioning
        final KStream<String, Integer> rekeyed = source.map(
            (k, v) -> KeyValue.pair(k.toLowerCase(Locale.getDefault()), v)
        );

        // mapValues is VALUE-CHANGING: Integer → String
        final KStream<String, String> mapped = rekeyed.mapValues(
            value -> "value-" + value
        );

        // Branch 1: groupByKey → count
        // Creates repartition-1 because key was changed above
        mapped
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .count(Materialized.as(Stores.inMemoryKeyValueStore("count-store-1")))
            .toStream()
            .to(OUTPUT_TOPIC_1, Produced.with(Serdes.String(), Serdes.Long()));

        // Branch 2: filter → groupByKey → count
        // Creates repartition-2 (cannot merge with repartition-1 due to mapValues)
        mapped
            .filter((k, v) -> v.length() > 5)
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .count(Materialized.as(Stores.inMemoryKeyValueStore("count-store-2")))
            .toStream()
            .to(OUTPUT_TOPIC_2, Produced.with(Serdes.String(), Serdes.Long()));

        final Topology topology = builder.build(streamsConfiguration);
        final String topologyString = topology.describe().toString();

        // Print topology for inspection
        System.out.println("=== TOPOLOGY WITH VALUE-CHANGING OPERATION ===");
        System.out.println(topologyString);
        System.out.println("==============================================");

        // Count repartition topics
        final int repartitionCount = countRepartitionTopics(topologyString);

        // CURRENT BEHAVIOR: 2 repartition topics created
        // This is because optimization cannot push repartition above mapValues
        assertEquals(2, repartitionCount,
            "Current behavior: 2 repartition topics created because optimization " +
            "is blocked by value-changing operation (mapValues)");

        // Verify topology still works correctly
        topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration);

        final TestInputTopic<String, Integer> inputTopic =
            topologyTestDriver.createInputTopic(INPUT_TOPIC, stringSerializer, Serdes.Integer().serializer());
        final TestOutputTopic<String, Long> outputTopic1 =
            topologyTestDriver.createOutputTopic(OUTPUT_TOPIC_1, stringDeserializer, Serdes.Long().deserializer());
        final TestOutputTopic<String, Long> outputTopic2 =
            topologyTestDriver.createOutputTopic(OUTPUT_TOPIC_2, stringDeserializer, Serdes.Long().deserializer());

        // Input data (uppercase keys will be lowercased by map())
        inputTopic.pipeKeyValueList(Arrays.asList(
            KeyValue.pair("A", 1),
            KeyValue.pair("B", 2),
            KeyValue.pair("A", 3),
            KeyValue.pair("B", 4)
        ));

        // Verify outputs (keys are now lowercase: "a", "b")
        final Map<String, Long> output1 = outputTopic1.readKeyValuesToMap();
        final Map<String, Long> output2 = outputTopic2.readKeyValuesToMap();

        // Branch 1: counts all records (a=2, b=2)
        assertThat(output1.get("a"), equalTo(2L));
        assertThat(output1.get("b"), equalTo(2L));

        // Branch 2: counts only records with value.length() > 5
        // "value-1" = 7 chars, "value-2" = 7 chars, "value-3" = 7 chars, "value-4" = 7 chars
        // All pass the filter
        assertThat(output2.get("a"), equalTo(2L));
        assertThat(output2.get("b"), equalTo(2L));
    }

    /**
     * Test Case 2: Shows optimization works WITHOUT value-changing operation
     *
     * Topology:
     * source[String, Integer]
     *   → map(key-changing: lowercase key)
     *     → groupByKey → count → output1
     *     → filter → groupByKey → count → output2
     *
     * CURRENT BEHAVIOR:
     * - Creates 1 repartition topic (optimization successful)
     * - Both branches share the same repartition topic
     */
    @Test
    public void shouldMergeRepartitionTopicsWithoutValueChangingOperation() {
        streamsConfiguration.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Integer> source = builder.stream(
            INPUT_TOPIC,
            Consumed.with(Serdes.String(), Serdes.Integer())
        );

        // KEY-CHANGING operation to trigger repartitioning
        final KStream<String, Integer> rekeyed = source.map(
            (k, v) -> KeyValue.pair(k.toLowerCase(Locale.getDefault()), v)
        );

        // NO value-changing operation here

        // Branch 1: groupByKey → count
        rekeyed
            .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
            .count(Materialized.as(Stores.inMemoryKeyValueStore("count-store-1")))
            .toStream()
            .to(OUTPUT_TOPIC_1, Produced.with(Serdes.String(), Serdes.Long()));

        // Branch 2: filter → groupByKey → count
        rekeyed
            .filter((k, v) -> v > 2)
            .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
            .count(Materialized.as(Stores.inMemoryKeyValueStore("count-store-2")))
            .toStream()
            .to(OUTPUT_TOPIC_2, Produced.with(Serdes.String(), Serdes.Long()));

        final Topology topology = builder.build(streamsConfiguration);
        final String topologyString = topology.describe().toString();

        System.out.println("=== TOPOLOGY WITHOUT VALUE-CHANGING OPERATION ===");
        System.out.println(topologyString);
        System.out.println("==================================================");

        final int repartitionCount = countRepartitionTopics(topologyString);

        // CURRENT BEHAVIOR: 1 repartition topic (optimization successful!)
        assertEquals(1, repartitionCount,
            "Optimization works: 1 merged repartition topic when no value-changing operation present");

        // Verify topology works correctly
        topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration);

        final TestInputTopic<String, Integer> inputTopic =
            topologyTestDriver.createInputTopic(INPUT_TOPIC, stringSerializer, Serdes.Integer().serializer());
        final TestOutputTopic<String, Long> outputTopic1 =
            topologyTestDriver.createOutputTopic(OUTPUT_TOPIC_1, stringDeserializer, Serdes.Long().deserializer());
        final TestOutputTopic<String, Long> outputTopic2 =
            topologyTestDriver.createOutputTopic(OUTPUT_TOPIC_2, stringDeserializer, Serdes.Long().deserializer());

        inputTopic.pipeKeyValueList(Arrays.asList(
            KeyValue.pair("A", 1),
            KeyValue.pair("B", 2),
            KeyValue.pair("A", 3),
            KeyValue.pair("B", 4)
        ));

        final Map<String, Long> output1 = outputTopic1.readKeyValuesToMap();
        final Map<String, Long> output2 = outputTopic2.readKeyValuesToMap();

        // Branch 1: counts all (a=2, b=2)
        assertThat(output1.get("a"), equalTo(2L));
        assertThat(output1.get("b"), equalTo(2L));

        // Branch 2: counts only > 2 (a:3, b:4 = a=1, b=1)
        assertThat(output2.get("a"), equalTo(1L));
        assertThat(output2.get("b"), equalTo(1L));
    }

    /**
     * Test Case 3: Multiple value-changing operations in sequence
     *
     * Demonstrates even more complex scenario where multiple value transformations
     * block the optimization.
     */
    @Test
    public void shouldCreateTwoRepartitionTopicsWithMultipleValueChangingOperations() {
        streamsConfiguration.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Integer> source = builder.stream(
            INPUT_TOPIC,
            Consumed.with(Serdes.String(), Serdes.Integer())
        );

        // KEY-CHANGING operation to trigger repartitioning
        final KStream<String, Integer> rekeyed = source.map(
            (k, v) -> KeyValue.pair(k.toLowerCase(Locale.getDefault()), v)
        );

        // Chain of value-changing operations: Integer → String → String
        final KStream<String, String> transformed = rekeyed
            .mapValues(value -> "step1-" + value)      // Integer → String
            .mapValues(value -> value.toUpperCase(Locale.getDefault()));   // String → String

        // Two branches that could share repartition if pushed above transformations
        transformed
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .count(Materialized.as(Stores.inMemoryKeyValueStore("count-store-1")))
            .toStream()
            .to(OUTPUT_TOPIC_1, Produced.with(Serdes.String(), Serdes.Long()));

        transformed
            .filter((k, v) -> true)
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .count(Materialized.as(Stores.inMemoryKeyValueStore("count-store-2")))
            .toStream()
            .to(OUTPUT_TOPIC_2, Produced.with(Serdes.String(), Serdes.Long()));

        final Topology topology = builder.build(streamsConfiguration);
        final String topologyString = topology.describe().toString();

        System.out.println("=== TOPOLOGY WITH CHAINED VALUE-CHANGING OPERATIONS ===");
        System.out.println(topologyString);
        System.out.println("========================================================");

        final int repartitionCount = countRepartitionTopics(topologyString);

        // CURRENT: 2 repartition topics
        // DESIRED: Could push repartition all the way to source with Integer serde
        assertEquals(2, repartitionCount,
            "Current: 2 repartition topics due to value-changing operations chain");

        topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration);

        final TestInputTopic<String, Integer> inputTopic =
            topologyTestDriver.createInputTopic(INPUT_TOPIC, stringSerializer, Serdes.Integer().serializer());
        final TestOutputTopic<String, Long> outputTopic1 =
            topologyTestDriver.createOutputTopic(OUTPUT_TOPIC_1, stringDeserializer, Serdes.Long().deserializer());
        final TestOutputTopic<String, Long> outputTopic2 =
            topologyTestDriver.createOutputTopic(OUTPUT_TOPIC_2, stringDeserializer, Serdes.Long().deserializer());

        inputTopic.pipeKeyValueList(Arrays.asList(
            KeyValue.pair("A", 1),
            KeyValue.pair("B", 2)
        ));

        final Map<String, Long> output1 = outputTopic1.readKeyValuesToMap();
        final Map<String, Long> output2 = outputTopic2.readKeyValuesToMap();

        assertThat(output1.get("a"), equalTo(1L));
        assertThat(output1.get("b"), equalTo(1L));
        assertThat(output2.get("a"), equalTo(1L));
        assertThat(output2.get("b"), equalTo(1L));
    }

    /**
     * Helper method to count repartition topics in the topology
     */
    private int countRepartitionTopics(final String topologyString) {
        final Matcher matcher = repartitionTopicPattern.matcher(topologyString);
        final List<String> repartitionTopics = new ArrayList<>();
        while (matcher.find()) {
            repartitionTopics.add(matcher.group());
        }
        System.out.println("Repartition topics found: " + repartitionTopics.size());
        for (final String topic : repartitionTopics) {
            System.out.println("  - " + topic);
        }
        return repartitionTopics.size();
    }

    /**
     * Helper to convert KeyValue list to Map
     */
    private <K, V> Map<K, V> keyValueListToMap(final List<KeyValue<K, V>> keyValuePairs) {
        final Map<K, V> map = new HashMap<>();
        for (final KeyValue<K, V> pair : keyValuePairs) {
            map.put(pair.key, pair.value);
        }
        return map;
    }
}
