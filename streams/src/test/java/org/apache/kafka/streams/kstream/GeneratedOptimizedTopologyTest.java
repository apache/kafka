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

package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Locale;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class GeneratedOptimizedTopologyTest {

    private StreamsBuilder builder;
    private Properties streamsConfiguration;

    private static final String INPUT_TOPIC = "input";
    private static final String COUNT_TOPIC = "outputTopic_0";
    private static final String AGGREGATION_TOPIC = "outputTopic_1";
    private static final String REDUCE_TOPIC = "outputTopic_2";

    @Before
    public void setUp() {
        final Initializer<Integer> initializer = () -> 0;
        final Aggregator<String, String, Integer> aggregator = (k, v, agg) -> agg + v.length();

        final Reducer<String> reducer = (v1, v2) -> v1 + ":" + v2;

        builder = new StreamsBuilder();

        final KStream<String, String> sourceStream = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        final KStream<String, String> mappedStream = sourceStream.map((k, v) -> KeyValue.pair(k.toUpperCase(Locale.getDefault()), v));

        mappedStream.groupByKey().count(Materialized.with(Serdes.String(), Serdes.Long())).toStream().to(COUNT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
        mappedStream.groupByKey().aggregate(initializer, aggregator, Materialized.with(Serdes.String(), Serdes.Integer())).toStream().to(AGGREGATION_TOPIC, Produced.with(Serdes.String(), Serdes.Integer()));
        mappedStream.groupByKey().reduce(reducer, Materialized.with(Serdes.String(), Serdes.String())).toStream().to(REDUCE_TOPIC, Produced.with(Serdes.String(), Serdes.String()));


        streamsConfiguration = StreamsTestUtils.minimalStreamsConfig();

    }


    @Test
    public void testGeneratesCorrectOptimizedTopology() {
        streamsConfiguration.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
        final Topology topology = builder.build(streamsConfiguration);
        assertEquals(getExpectedOptimizedTopology(), topology.describe().toString());
    }


    @Test
    public void testGeneratesCorrectNonOptimizedTopology() {
        streamsConfiguration.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.NO_OPTIMIZATION);
        final Topology topology = builder.build(streamsConfiguration);
        assertEquals(getExpectedNonOptimizedToplogy(), topology.describe().toString());
    }


    @Test
    public void testGeneratesCorrectNonOptimizedTopologyWhenNoPropertiesSupplied() {
        Topology topology = builder.build();
        assertEquals(getExpectedNonOptimizedToplogy(), topology.describe().toString());
    }


    private static String getExpectedNonOptimizedToplogy() {
        return  "Topologies:\n"
                + "   Sub-topology: 0\n"
                + "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n"
                + "      --> KSTREAM-MAP-0000000001\n"
                + "    Processor: KSTREAM-MAP-0000000001 (stores: [])\n"
                + "      --> KSTREAM-FILTER-0000000006, KSTREAM-FILTER-0000000013, KSTREAM-FILTER-0000000020\n"
                + "      <-- KSTREAM-SOURCE-0000000000\n"
                + "    Processor: KSTREAM-FILTER-0000000006 (stores: [])\n"
                + "      --> KSTREAM-SINK-0000000005\n"
                + "      <-- KSTREAM-MAP-0000000001\n"
                + "    Processor: KSTREAM-FILTER-0000000013 (stores: [])\n"
                + "      --> KSTREAM-SINK-0000000012\n"
                + "      <-- KSTREAM-MAP-0000000001\n"
                + "    Processor: KSTREAM-FILTER-0000000020 (stores: [])\n"
                + "      --> KSTREAM-SINK-0000000019\n"
                + "      <-- KSTREAM-MAP-0000000001\n"
                + "    Sink: KSTREAM-SINK-0000000005 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition)\n"
                + "      <-- KSTREAM-FILTER-0000000006\n"
                + "    Sink: KSTREAM-SINK-0000000012 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000010-repartition)\n"
                + "      <-- KSTREAM-FILTER-0000000013\n"
                + "    Sink: KSTREAM-SINK-0000000019 (topic: KSTREAM-REDUCE-STATE-STORE-0000000017-repartition)\n"
                + "      <-- KSTREAM-FILTER-0000000020\n"
                + "\n"
                + "  Sub-topology: 1\n"
                + "    Source: KSTREAM-SOURCE-0000000007 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition])\n"
                + "      --> KSTREAM-AGGREGATE-0000000004\n"
                + "    Processor: KSTREAM-AGGREGATE-0000000004 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000003])\n"
                + "      --> KTABLE-TOSTREAM-0000000008\n"
                + "      <-- KSTREAM-SOURCE-0000000007\n"
                + "    Processor: KTABLE-TOSTREAM-0000000008 (stores: [])\n"
                + "      --> KSTREAM-SINK-0000000009\n"
                + "      <-- KSTREAM-AGGREGATE-0000000004\n"
                + "    Sink: KSTREAM-SINK-0000000009 (topic: outputTopic_0)\n"
                + "      <-- KTABLE-TOSTREAM-0000000008\n"
                + "\n"
                + "  Sub-topology: 2\n"
                + "    Source: KSTREAM-SOURCE-0000000014 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000010-repartition])\n"
                + "      --> KSTREAM-AGGREGATE-0000000011\n"
                + "    Processor: KSTREAM-AGGREGATE-0000000011 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000010])\n"
                + "      --> KTABLE-TOSTREAM-0000000015\n"
                + "      <-- KSTREAM-SOURCE-0000000014\n"
                + "    Processor: KTABLE-TOSTREAM-0000000015 (stores: [])\n"
                + "      --> KSTREAM-SINK-0000000016\n"
                + "      <-- KSTREAM-AGGREGATE-0000000011\n"
                + "    Sink: KSTREAM-SINK-0000000016 (topic: outputTopic_1)\n"
                + "      <-- KTABLE-TOSTREAM-0000000015\n"
                + "\n"
                + "  Sub-topology: 3\n"
                + "    Source: KSTREAM-SOURCE-0000000021 (topics: [KSTREAM-REDUCE-STATE-STORE-0000000017-repartition])\n"
                + "      --> KSTREAM-REDUCE-0000000018\n"
                + "    Processor: KSTREAM-REDUCE-0000000018 (stores: [KSTREAM-REDUCE-STATE-STORE-0000000017])\n"
                + "      --> KTABLE-TOSTREAM-0000000022\n"
                + "      <-- KSTREAM-SOURCE-0000000021\n"
                + "    Processor: KTABLE-TOSTREAM-0000000022 (stores: [])\n"
                + "      --> KSTREAM-SINK-0000000023\n"
                + "      <-- KSTREAM-REDUCE-0000000018\n"
                + "    Sink: KSTREAM-SINK-0000000023 (topic: outputTopic_2)\n"
                + "      <-- KTABLE-TOSTREAM-0000000022\n" +
                "\n";
    }


    private static String getExpectedOptimizedTopology() {
        return "Topologies:\n"
               + "   Sub-topology: 0\n"
               + "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n"
               + "      --> KSTREAM-MAP-0000000001\n"
               + "    Processor: KSTREAM-MAP-0000000001 (stores: [])\n"
               + "      --> KSTREAM-FILTER-0000000025\n"
               + "      <-- KSTREAM-SOURCE-0000000000\n"
               + "    Processor: KSTREAM-FILTER-0000000025 (stores: [])\n"
               + "      --> KSTREAM-SINK-0000000024\n"
               + "      <-- KSTREAM-MAP-0000000001\n"
               + "    Sink: KSTREAM-SINK-0000000024 (topic: KSTREAM-MAP-0000000001-optimized-repartition)\n"
               + "      <-- KSTREAM-FILTER-0000000025\n"
               + "\n"
               + "  Sub-topology: 1\n"
               + "    Source: KSTREAM-SOURCE-0000000026 (topics: [KSTREAM-MAP-0000000001-optimized-repartition])\n"
               + "      --> KSTREAM-AGGREGATE-0000000004, KSTREAM-AGGREGATE-0000000011, KSTREAM-REDUCE-0000000018\n"
               + "    Processor: KSTREAM-AGGREGATE-0000000004 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000003])\n"
               + "      --> KTABLE-TOSTREAM-0000000008\n"
               + "      <-- KSTREAM-SOURCE-0000000026\n"
               + "    Processor: KSTREAM-AGGREGATE-0000000011 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000010])\n"
               + "      --> KTABLE-TOSTREAM-0000000015\n"
               + "      <-- KSTREAM-SOURCE-0000000026\n"
               + "    Processor: KSTREAM-REDUCE-0000000018 (stores: [KSTREAM-REDUCE-STATE-STORE-0000000017])\n"
               + "      --> KTABLE-TOSTREAM-0000000022\n"
               + "      <-- KSTREAM-SOURCE-0000000026\n"
               + "    Processor: KTABLE-TOSTREAM-0000000008 (stores: [])\n"
               + "      --> KSTREAM-SINK-0000000009\n"
               + "      <-- KSTREAM-AGGREGATE-0000000004\n"
               + "    Processor: KTABLE-TOSTREAM-0000000015 (stores: [])\n"
               + "      --> KSTREAM-SINK-0000000016\n"
               + "      <-- KSTREAM-AGGREGATE-0000000011\n"
               + "    Processor: KTABLE-TOSTREAM-0000000022 (stores: [])\n"
               + "      --> KSTREAM-SINK-0000000023\n"
               + "      <-- KSTREAM-REDUCE-0000000018\n"
               + "    Sink: KSTREAM-SINK-0000000009 (topic: outputTopic_0)\n"
               + "      <-- KTABLE-TOSTREAM-0000000008\n"
               + "    Sink: KSTREAM-SINK-0000000016 (topic: outputTopic_1)\n"
               + "      <-- KTABLE-TOSTREAM-0000000015\n"
               + "    Sink: KSTREAM-SINK-0000000023 (topic: outputTopic_2)\n"
               + "      <-- KTABLE-TOSTREAM-0000000022\n" +
               "\n";
    }

}
