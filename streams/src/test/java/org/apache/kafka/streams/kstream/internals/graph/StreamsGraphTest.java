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

package org.apache.kafka.streams.kstream.internals.graph;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.time.Duration.ofMillis;
import static org.junit.Assert.assertEquals;

public class StreamsGraphTest {

    private final Pattern repartitionTopicPattern = Pattern.compile("Sink: .*-repartition");

    // Test builds topology in succesive manner but only graph node not yet processed written to topology

    @Test
    public void shouldBeAbleToBuildTopologyIncrementally() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> stream = builder.stream("topic");
        final KStream<String, String> streamII = builder.stream("other-topic");
        final ValueJoiner<String, String, String> valueJoiner = (v, v2) -> v + v2;


        final KStream<String, String> joinedStream = stream.join(streamII, valueJoiner, JoinWindows.of(ofMillis(5000)));

        // build step one
        assertEquals(expectedJoinedTopology, builder.build().describe().toString());

        final KStream<String, String> filteredJoinStream = joinedStream.filter((k, v) -> v.equals("foo"));
        // build step two
        assertEquals(expectedJoinedFilteredTopology, builder.build().describe().toString());

        filteredJoinStream.mapValues(v -> v + "some value").to("output-topic");
        // build step three
        assertEquals(expectedFullTopology, builder.build().describe().toString());

    }

    @Test
    public void shouldBeAbleToProcessNestedMultipleKeyChangingNodes() {
        final Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test-application");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> inputStream = builder.stream("inputTopic");

        final KStream<String, String> changedKeyStream = inputStream.selectKey((k, v) -> v.substring(0, 5));

        // first repartition
        changedKeyStream.groupByKey(Grouped.as("count-repartition"))
            .count(Materialized.as("count-store"))
            .toStream().to("count-topic", Produced.with(Serdes.String(), Serdes.Long()));

        // second repartition
        changedKeyStream.groupByKey(Grouped.as("windowed-repartition"))
            .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
            .count(Materialized.as("windowed-count-store"))
            .toStream()
            .map((k, v) -> KeyValue.pair(k.key(), v)).to("windowed-count", Produced.with(Serdes.String(), Serdes.Long()));

        builder.build(properties);
    }

    @Test
    public void shouldNotOptimizeWithValueOrKeyChangingOperatorsAfterInitialKeyChange() {

        final Topology attemptedOptimize = getTopologyWithChangingValuesAfterChangingKey(StreamsConfig.OPTIMIZE);
        final Topology noOptimization = getTopologyWithChangingValuesAfterChangingKey(StreamsConfig.NO_OPTIMIZATION);

        assertEquals(attemptedOptimize.describe().toString(), noOptimization.describe().toString());
        assertEquals(2, getCountOfRepartitionTopicsFound(attemptedOptimize.describe().toString()));
        assertEquals(2, getCountOfRepartitionTopicsFound(noOptimization.describe().toString()));
    }

    // no need to optimize as user has already performed the repartitioning manually
    @Test
    public void shouldNotOptimizeWhenAThroughOperationIsDone() {

        final Topology attemptedOptimize = getTopologyWithThroughOperation(StreamsConfig.OPTIMIZE);
        final Topology noOptimziation = getTopologyWithThroughOperation(StreamsConfig.NO_OPTIMIZATION);

        assertEquals(attemptedOptimize.describe().toString(), noOptimziation.describe().toString());
        assertEquals(0, getCountOfRepartitionTopicsFound(attemptedOptimize.describe().toString()));
        assertEquals(0, getCountOfRepartitionTopicsFound(noOptimziation.describe().toString()));

    }

    private Topology getTopologyWithChangingValuesAfterChangingKey(final String optimizeConfig) {

        final StreamsBuilder builder = new StreamsBuilder();
        final Properties properties = new Properties();
        properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, optimizeConfig);

        final KStream<String, String> inputStream = builder.stream("input");
        final KStream<String, String> mappedKeyStream = inputStream.selectKey((k, v) -> k + v);

        mappedKeyStream.mapValues(v -> v.toUpperCase(Locale.getDefault())).groupByKey().count().toStream().to("output");
        mappedKeyStream.flatMapValues(v -> Arrays.asList(v.split("\\s"))).groupByKey().windowedBy(TimeWindows.of(ofMillis(5000))).count().toStream().to("windowed-output");

        return builder.build(properties);

    }

    private Topology getTopologyWithThroughOperation(final String optimizeConfig) {

        final StreamsBuilder builder = new StreamsBuilder();
        final Properties properties = new Properties();
        properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, optimizeConfig);

        final KStream<String, String> inputStream = builder.stream("input");
        final KStream<String, String> mappedKeyStream = inputStream.selectKey((k, v) -> k + v).through("through-topic");

        mappedKeyStream.groupByKey().count().toStream().to("output");
        mappedKeyStream.groupByKey().windowedBy(TimeWindows.of(ofMillis(5000))).count().toStream().to("windowed-output");

        return builder.build(properties);

    }

    private int getCountOfRepartitionTopicsFound(final String topologyString) {
        final Matcher matcher = repartitionTopicPattern.matcher(topologyString);
        final List<String> repartitionTopicsFound = new ArrayList<>();
        while (matcher.find()) {
            repartitionTopicsFound.add(matcher.group());
        }
        return repartitionTopicsFound.size();
    }

    private String expectedJoinedTopology = "Topologies:\n"
                                            + "   Sub-topology: 0\n"
                                            + "    Source: KSTREAM-SOURCE-0000000000 (topics: [topic])\n"
                                            + "      --> KSTREAM-WINDOWED-0000000002\n"
                                            + "    Source: KSTREAM-SOURCE-0000000001 (topics: [other-topic])\n"
                                            + "      --> KSTREAM-WINDOWED-0000000003\n"
                                            + "    Processor: KSTREAM-WINDOWED-0000000002 (stores: [KSTREAM-JOINTHIS-0000000004-store])\n"
                                            + "      --> KSTREAM-JOINTHIS-0000000004\n"
                                            + "      <-- KSTREAM-SOURCE-0000000000\n"
                                            + "    Processor: KSTREAM-WINDOWED-0000000003 (stores: [KSTREAM-JOINOTHER-0000000005-store])\n"
                                            + "      --> KSTREAM-JOINOTHER-0000000005\n"
                                            + "      <-- KSTREAM-SOURCE-0000000001\n"
                                            + "    Processor: KSTREAM-JOINOTHER-0000000005 (stores: [KSTREAM-JOINTHIS-0000000004-store])\n"
                                            + "      --> KSTREAM-MERGE-0000000006\n"
                                            + "      <-- KSTREAM-WINDOWED-0000000003\n"
                                            + "    Processor: KSTREAM-JOINTHIS-0000000004 (stores: [KSTREAM-JOINOTHER-0000000005-store])\n"
                                            + "      --> KSTREAM-MERGE-0000000006\n"
                                            + "      <-- KSTREAM-WINDOWED-0000000002\n"
                                            + "    Processor: KSTREAM-MERGE-0000000006 (stores: [])\n"
                                            + "      --> none\n"
                                            + "      <-- KSTREAM-JOINTHIS-0000000004, KSTREAM-JOINOTHER-0000000005\n\n";

    private String expectedJoinedFilteredTopology = "Topologies:\n"
                                                    + "   Sub-topology: 0\n"
                                                    + "    Source: KSTREAM-SOURCE-0000000000 (topics: [topic])\n"
                                                    + "      --> KSTREAM-WINDOWED-0000000002\n"
                                                    + "    Source: KSTREAM-SOURCE-0000000001 (topics: [other-topic])\n"
                                                    + "      --> KSTREAM-WINDOWED-0000000003\n"
                                                    + "    Processor: KSTREAM-WINDOWED-0000000002 (stores: [KSTREAM-JOINTHIS-0000000004-store])\n"
                                                    + "      --> KSTREAM-JOINTHIS-0000000004\n"
                                                    + "      <-- KSTREAM-SOURCE-0000000000\n"
                                                    + "    Processor: KSTREAM-WINDOWED-0000000003 (stores: [KSTREAM-JOINOTHER-0000000005-store])\n"
                                                    + "      --> KSTREAM-JOINOTHER-0000000005\n"
                                                    + "      <-- KSTREAM-SOURCE-0000000001\n"
                                                    + "    Processor: KSTREAM-JOINOTHER-0000000005 (stores: [KSTREAM-JOINTHIS-0000000004-store])\n"
                                                    + "      --> KSTREAM-MERGE-0000000006\n"
                                                    + "      <-- KSTREAM-WINDOWED-0000000003\n"
                                                    + "    Processor: KSTREAM-JOINTHIS-0000000004 (stores: [KSTREAM-JOINOTHER-0000000005-store])\n"
                                                    + "      --> KSTREAM-MERGE-0000000006\n"
                                                    + "      <-- KSTREAM-WINDOWED-0000000002\n"
                                                    + "    Processor: KSTREAM-MERGE-0000000006 (stores: [])\n"
                                                    + "      --> KSTREAM-FILTER-0000000007\n"
                                                    + "      <-- KSTREAM-JOINTHIS-0000000004, KSTREAM-JOINOTHER-0000000005\n"
                                                    + "    Processor: KSTREAM-FILTER-0000000007 (stores: [])\n"
                                                    + "      --> none\n"
                                                    + "      <-- KSTREAM-MERGE-0000000006\n\n";

    private String expectedFullTopology = "Topologies:\n"
                                          + "   Sub-topology: 0\n"
                                          + "    Source: KSTREAM-SOURCE-0000000000 (topics: [topic])\n"
                                          + "      --> KSTREAM-WINDOWED-0000000002\n"
                                          + "    Source: KSTREAM-SOURCE-0000000001 (topics: [other-topic])\n"
                                          + "      --> KSTREAM-WINDOWED-0000000003\n"
                                          + "    Processor: KSTREAM-WINDOWED-0000000002 (stores: [KSTREAM-JOINTHIS-0000000004-store])\n"
                                          + "      --> KSTREAM-JOINTHIS-0000000004\n"
                                          + "      <-- KSTREAM-SOURCE-0000000000\n"
                                          + "    Processor: KSTREAM-WINDOWED-0000000003 (stores: [KSTREAM-JOINOTHER-0000000005-store])\n"
                                          + "      --> KSTREAM-JOINOTHER-0000000005\n"
                                          + "      <-- KSTREAM-SOURCE-0000000001\n"
                                          + "    Processor: KSTREAM-JOINOTHER-0000000005 (stores: [KSTREAM-JOINTHIS-0000000004-store])\n"
                                          + "      --> KSTREAM-MERGE-0000000006\n"
                                          + "      <-- KSTREAM-WINDOWED-0000000003\n"
                                          + "    Processor: KSTREAM-JOINTHIS-0000000004 (stores: [KSTREAM-JOINOTHER-0000000005-store])\n"
                                          + "      --> KSTREAM-MERGE-0000000006\n"
                                          + "      <-- KSTREAM-WINDOWED-0000000002\n"
                                          + "    Processor: KSTREAM-MERGE-0000000006 (stores: [])\n"
                                          + "      --> KSTREAM-FILTER-0000000007\n"
                                          + "      <-- KSTREAM-JOINTHIS-0000000004, KSTREAM-JOINOTHER-0000000005\n"
                                          + "    Processor: KSTREAM-FILTER-0000000007 (stores: [])\n"
                                          + "      --> KSTREAM-MAPVALUES-0000000008\n"
                                          + "      <-- KSTREAM-MERGE-0000000006\n"
                                          + "    Processor: KSTREAM-MAPVALUES-0000000008 (stores: [])\n"
                                          + "      --> KSTREAM-SINK-0000000009\n"
                                          + "      <-- KSTREAM-FILTER-0000000007\n"
                                          + "    Sink: KSTREAM-SINK-0000000009 (topic: output-topic)\n"
                                          + "      <-- KSTREAM-MAPVALUES-0000000008\n\n";

}
