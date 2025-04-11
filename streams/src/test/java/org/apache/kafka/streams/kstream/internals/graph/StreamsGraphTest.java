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
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.time.Duration.ofMillis;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class StreamsGraphTest {

    private final Pattern repartitionTopicPattern = Pattern.compile("Sink: .*-repartition");
    private Initializer<String> initializer;
    private Aggregator<String, String, String> aggregator;

    // Test builds topology in successive manner but only graph node not yet processed written to topology

    @SuppressWarnings("deprecation")
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
        properties.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> inputStream = builder.stream("inputTopic");

        final KStream<String, String> changedKeyStream = inputStream.selectKey((k, v) -> v.substring(0, 5));

        // first repartition
        changedKeyStream.groupByKey(Grouped.as("count-repartition"))
            .count(Materialized.as("count-store"))
            .toStream().to("count-topic", Produced.with(Serdes.String(), Serdes.Long()));

        // second repartition
        changedKeyStream.groupByKey(Grouped.as("windowed-repartition"))
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5)))
            .count(Materialized.as("windowed-count-store"))
            .toStream()
            .map((k, v) -> KeyValue.pair(k.key(), v)).to("windowed-count", Produced.with(Serdes.String(), Serdes.Long()));

        builder.build(properties);
    }

    @Test
    // Topology in this test from https://issues.apache.org/jira/browse/KAFKA-9739
    public void shouldNotThrowNPEWithMergeNodes() {
        final Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test-application");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        final StreamsBuilder builder = new StreamsBuilder();
        initializer = () -> "";
        aggregator = (aggKey, value, aggregate) -> aggregate + value.length();
        final ProcessorSupplier<String, String, String, String> processorSupplier =
            () -> new Processor<String, String, String, String>() {
                private ProcessorContext<String, String> context;

                @Override
                public void init(final ProcessorContext<String, String> context) {
                    this.context = context;
                }

                @Override
                public void process(final Record<String, String> record) {
                    context.forward(record);
                }
            };

        final KStream<String, String> retryStream = builder.stream("retryTopic", Consumed.with(Serdes.String(), Serdes.String()))
                .process(processorSupplier)
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(initializer,
                        aggregator,
                        Materialized.with(Serdes.String(), Serdes.String()))
                .suppress(Suppressed.untilTimeLimit(Duration.ofSeconds(500), Suppressed.BufferConfig.maxBytes(64_000_000)))
                .toStream()
                .flatMap((k, v) -> new ArrayList<>());

        final KTable<String, String> idTable = builder.stream("id-table-topic", Consumed.with(Serdes.String(), Serdes.String()))
                .flatMap((k, v) -> new ArrayList<KeyValue<String, String>>())
                .peek((subscriptionId, recipientId) -> System.out.println("data " + subscriptionId + " " + recipientId))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(initializer,
                        aggregator,
                        Materialized.with(Serdes.String(), Serdes.String()));

        final KStream<String, String> joinStream = builder.stream("internal-topic-command", Consumed.with(Serdes.String(), Serdes.String()))
                .peek((subscriptionId, command) -> System.out.println("stdoutput"))
                .mapValues((k, v) -> v)
                .merge(retryStream)
                .leftJoin(idTable, (v1, v2) -> v1 + v2,
                        Joined.with(Serdes.String(), Serdes.String(), Serdes.String()));


        joinStream.split()
                .branch((k, v) -> v.equals("some-value"), Branched.withConsumer(ks -> ks.map(KeyValue::pair)
                                .peek((recipientId, command) -> System.out.println("printing out"))
                                .to("external-command", Produced.with(Serdes.String(), Serdes.String()))
                ))
                .defaultBranch(Branched.withConsumer(ks -> {
                    ks.filter((k, v) -> v != null)
                            .peek((subscriptionId, wrapper) -> System.out.println("Printing output"))
                            .mapValues((k, v) -> v)
                            .to("dlq-topic", Produced.with(Serdes.String(), Serdes.String()));
                    ks.map(KeyValue::pair).to("retryTopic", Produced.with(Serdes.String(), Serdes.String()));
                }));

        final Topology topology = builder.build(properties);
        assertEquals(expectedComplexMergeOptimizeTopology, topology.describe().toString());
    }

    @Test
    public void shouldNotOptimizeWithValueOrKeyChangingOperatorsAfterInitialKeyChange() {

        final Topology attemptedOptimize = getTopologyWithChangingValuesAfterChangingKey(StreamsConfig.OPTIMIZE);
        final Topology noOptimization = getTopologyWithChangingValuesAfterChangingKey(StreamsConfig.NO_OPTIMIZATION);

        assertEquals(attemptedOptimize.describe().toString(), noOptimization.describe().toString());
        assertEquals(2, getCountOfRepartitionTopicsFound(attemptedOptimize.describe().toString()));
        assertEquals(2, getCountOfRepartitionTopicsFound(noOptimization.describe().toString()));
    }

    @Test
    public void shouldOptimizeSeveralMergeNodesWithCommonKeyChangingParent() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KStream<Integer, Integer> parentStream = streamsBuilder.stream("input_topic", Consumed.with(Serdes.Integer(), Serdes.Integer()))
            .selectKey(Integer::sum);

        final KStream<Integer, Integer> childStream1 = parentStream.mapValues(v -> v + 1);
        final KStream<Integer, Integer> childStream2 = parentStream.mapValues(v -> v + 2);
        final KStream<Integer, Integer> childStream3 = parentStream.mapValues(v -> v + 3);

        childStream1
            .merge(childStream2)
            .merge(childStream3)
            .to("output_topic");

        final Properties properties = new Properties();
        properties.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final Topology topology = streamsBuilder.build(properties);

        assertEquals(expectedMergeOptimizedTopology, topology.describe().toString());
    }

    @Test
    public void shouldNotOptimizeWhenRepartitionOperationIsDone() {
        final Topology attemptedOptimize = getTopologyWithRepartitionOperation(StreamsConfig.OPTIMIZE);
        final Topology noOptimization = getTopologyWithRepartitionOperation(StreamsConfig.NO_OPTIMIZATION);

        assertEquals(attemptedOptimize.describe().toString(), noOptimization.describe().toString());
        assertEquals(2, getCountOfRepartitionTopicsFound(attemptedOptimize.describe().toString()));
        assertEquals(2, getCountOfRepartitionTopicsFound(noOptimization.describe().toString()));
    }

    private Topology getTopologyWithChangingValuesAfterChangingKey(final String optimizeConfig) {

        final StreamsBuilder builder = new StreamsBuilder();
        final Properties properties = new Properties();
        properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, optimizeConfig);

        final KStream<String, String> inputStream = builder.stream("input");
        final KStream<String, String> mappedKeyStream = inputStream.selectKey((k, v) -> k + v);

        mappedKeyStream.mapValues(v -> v.toUpperCase(Locale.getDefault())).groupByKey().count().toStream().to("output");
        mappedKeyStream.flatMapValues(v -> Arrays.asList(v.split("\\s"))).groupByKey().windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(5000))).count().toStream().to("windowed-output");

        return builder.build(properties);

    }

    private Topology getTopologyWithRepartitionOperation(final String optimizeConfig) {
        final StreamsBuilder builder = new StreamsBuilder();
        final Properties properties = new Properties();
        properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, optimizeConfig);

        final KStream<String, String> inputStream = builder.<String, String>stream("input").selectKey((k, v) -> k + v);

        inputStream
            .repartition()
            .groupByKey()
            .count()
            .toStream()
            .to("output");

        inputStream
            .repartition()
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(5000)))
            .count()
            .toStream()
            .to("windowed-output");

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

    private final String expectedJoinedTopology = "Topologies:\n"
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

    private final String expectedJoinedFilteredTopology = "Topologies:\n"
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

    private final String expectedFullTopology = "Topologies:\n"
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


    private final String expectedMergeOptimizedTopology = "Topologies:\n" +
        "   Sub-topology: 0\n" +
        "    Source: KSTREAM-SOURCE-0000000000 (topics: [input_topic])\n" +
        "      --> KSTREAM-KEY-SELECT-0000000001\n" +
        "    Processor: KSTREAM-KEY-SELECT-0000000001 (stores: [])\n" +
        "      --> KSTREAM-MAPVALUES-0000000002, KSTREAM-MAPVALUES-0000000003, KSTREAM-MAPVALUES-0000000004\n" +
        "      <-- KSTREAM-SOURCE-0000000000\n" +
        "    Processor: KSTREAM-MAPVALUES-0000000002 (stores: [])\n" +
        "      --> KSTREAM-MERGE-0000000005\n" +
        "      <-- KSTREAM-KEY-SELECT-0000000001\n" +
        "    Processor: KSTREAM-MAPVALUES-0000000003 (stores: [])\n" +
        "      --> KSTREAM-MERGE-0000000005\n" +
        "      <-- KSTREAM-KEY-SELECT-0000000001\n" +
        "    Processor: KSTREAM-MAPVALUES-0000000004 (stores: [])\n" +
        "      --> KSTREAM-MERGE-0000000006\n" +
        "      <-- KSTREAM-KEY-SELECT-0000000001\n" +
        "    Processor: KSTREAM-MERGE-0000000005 (stores: [])\n" +
        "      --> KSTREAM-MERGE-0000000006\n" +
        "      <-- KSTREAM-MAPVALUES-0000000002, KSTREAM-MAPVALUES-0000000003\n" +
        "    Processor: KSTREAM-MERGE-0000000006 (stores: [])\n" +
        "      --> KSTREAM-SINK-0000000007\n" +
        "      <-- KSTREAM-MERGE-0000000005, KSTREAM-MAPVALUES-0000000004\n" +
        "    Sink: KSTREAM-SINK-0000000007 (topic: output_topic)\n" +
        "      <-- KSTREAM-MERGE-0000000006\n\n";


    private final String expectedComplexMergeOptimizeTopology = "Topologies:\n" +
            "   Sub-topology: 0\n" +
            "    Source: KSTREAM-SOURCE-0000000000 (topics: [retryTopic])\n" +
            "      --> KSTREAM-PROCESSOR-0000000001\n" +
            "    Processor: KSTREAM-PROCESSOR-0000000001 (stores: [])\n" +
            "      --> KSTREAM-FILTER-0000000005\n" +
            "      <-- KSTREAM-SOURCE-0000000000\n" +
            "    Processor: KSTREAM-FILTER-0000000005 (stores: [])\n" +
            "      --> KSTREAM-SINK-0000000004\n" +
            "      <-- KSTREAM-PROCESSOR-0000000001\n" +
            "    Sink: KSTREAM-SINK-0000000004 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000002-repartition)\n" +
            "      <-- KSTREAM-FILTER-0000000005\n" +
            "\n" +
            "  Sub-topology: 1\n" +
            "    Source: KSTREAM-SOURCE-0000000006 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000002-repartition])\n" +
            "      --> KSTREAM-AGGREGATE-0000000003\n" +
            "    Processor: KSTREAM-AGGREGATE-0000000003 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000002])\n" +
            "      --> KTABLE-SUPPRESS-0000000007\n" +
            "      <-- KSTREAM-SOURCE-0000000006\n" +
            "    Source: KSTREAM-SOURCE-0000000019 (topics: [internal-topic-command])\n" +
            "      --> KSTREAM-PEEK-0000000020\n" +
            "    Processor: KTABLE-SUPPRESS-0000000007 (stores: [KTABLE-SUPPRESS-STATE-STORE-0000000008])\n" +
            "      --> KTABLE-TOSTREAM-0000000009\n" +
            "      <-- KSTREAM-AGGREGATE-0000000003\n" +
            "    Processor: KSTREAM-PEEK-0000000020 (stores: [])\n" +
            "      --> KSTREAM-MAPVALUES-0000000021\n" +
            "      <-- KSTREAM-SOURCE-0000000019\n" +
            "    Processor: KTABLE-TOSTREAM-0000000009 (stores: [])\n" +
            "      --> KSTREAM-FLATMAP-0000000010\n" +
            "      <-- KTABLE-SUPPRESS-0000000007\n" +
            "    Processor: KSTREAM-FLATMAP-0000000010 (stores: [])\n" +
            "      --> KSTREAM-MERGE-0000000022\n" +
            "      <-- KTABLE-TOSTREAM-0000000009\n" +
            "    Processor: KSTREAM-MAPVALUES-0000000021 (stores: [])\n" +
            "      --> KSTREAM-MERGE-0000000022\n" +
            "      <-- KSTREAM-PEEK-0000000020\n" +
            "    Processor: KSTREAM-MERGE-0000000022 (stores: [])\n" +
            "      --> KSTREAM-FILTER-0000000024\n" +
            "      <-- KSTREAM-MAPVALUES-0000000021, KSTREAM-FLATMAP-0000000010\n" +
            "    Processor: KSTREAM-FILTER-0000000024 (stores: [])\n" +
            "      --> KSTREAM-SINK-0000000023\n" +
            "      <-- KSTREAM-MERGE-0000000022\n" +
            "    Sink: KSTREAM-SINK-0000000023 (topic: KSTREAM-MERGE-0000000022-repartition)\n" +
            "      <-- KSTREAM-FILTER-0000000024\n" +
            "\n" +
            "  Sub-topology: 2\n" +
            "    Source: KSTREAM-SOURCE-0000000011 (topics: [id-table-topic])\n" +
            "      --> KSTREAM-FLATMAP-0000000012\n" +
            "    Processor: KSTREAM-FLATMAP-0000000012 (stores: [])\n" +
            "      --> KSTREAM-AGGREGATE-STATE-STORE-0000000014-repartition-filter\n" +
            "      <-- KSTREAM-SOURCE-0000000011\n" +
            "    Processor: KSTREAM-AGGREGATE-STATE-STORE-0000000014-repartition-filter (stores: [])\n" +
            "      --> KSTREAM-AGGREGATE-STATE-STORE-0000000014-repartition-sink\n" +
            "      <-- KSTREAM-FLATMAP-0000000012\n" +
            "    Sink: KSTREAM-AGGREGATE-STATE-STORE-0000000014-repartition-sink (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000014-repartition)\n" +
            "      <-- KSTREAM-AGGREGATE-STATE-STORE-0000000014-repartition-filter\n" +
            "\n" +
            "  Sub-topology: 3\n" +
            "    Source: KSTREAM-SOURCE-0000000025 (topics: [KSTREAM-MERGE-0000000022-repartition])\n" +
            "      --> KSTREAM-LEFTJOIN-0000000026\n" +
            "    Processor: KSTREAM-LEFTJOIN-0000000026 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000014])\n" +
            "      --> KSTREAM-BRANCH-0000000027\n" +
            "      <-- KSTREAM-SOURCE-0000000025\n" +
            "    Processor: KSTREAM-BRANCH-0000000027 (stores: [])\n" +
            "      --> KSTREAM-BRANCH-00000000270, KSTREAM-BRANCH-00000000271\n" +
            "      <-- KSTREAM-LEFTJOIN-0000000026\n" +
            "    Processor: KSTREAM-BRANCH-00000000270 (stores: [])\n" +
            "      --> KSTREAM-FILTER-0000000033, KSTREAM-MAP-0000000037\n" +
            "      <-- KSTREAM-BRANCH-0000000027\n" +
            "    Processor: KSTREAM-BRANCH-00000000271 (stores: [])\n" +
            "      --> KSTREAM-MAP-0000000029\n" +
            "      <-- KSTREAM-BRANCH-0000000027\n" +
            "    Processor: KSTREAM-FILTER-0000000033 (stores: [])\n" +
            "      --> KSTREAM-PEEK-0000000034\n" +
            "      <-- KSTREAM-BRANCH-00000000270\n" +
            "    Source: KSTREAM-AGGREGATE-STATE-STORE-0000000014-repartition-source (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000014-repartition])\n" +
            "      --> KSTREAM-PEEK-0000000013\n" +
            "    Processor: KSTREAM-MAP-0000000029 (stores: [])\n" +
            "      --> KSTREAM-PEEK-0000000030\n" +
            "      <-- KSTREAM-BRANCH-00000000271\n" +
            "    Processor: KSTREAM-PEEK-0000000034 (stores: [])\n" +
            "      --> KSTREAM-MAPVALUES-0000000035\n" +
            "      <-- KSTREAM-FILTER-0000000033\n" +
            "    Processor: KSTREAM-MAP-0000000037 (stores: [])\n" +
            "      --> KSTREAM-SINK-0000000038\n" +
            "      <-- KSTREAM-BRANCH-00000000270\n" +
            "    Processor: KSTREAM-MAPVALUES-0000000035 (stores: [])\n" +
            "      --> KSTREAM-SINK-0000000036\n" +
            "      <-- KSTREAM-PEEK-0000000034\n" +
            "    Processor: KSTREAM-PEEK-0000000013 (stores: [])\n" +
            "      --> KSTREAM-AGGREGATE-0000000015\n" +
            "      <-- KSTREAM-AGGREGATE-STATE-STORE-0000000014-repartition-source\n" +
            "    Processor: KSTREAM-PEEK-0000000030 (stores: [])\n" +
            "      --> KSTREAM-SINK-0000000031\n" +
            "      <-- KSTREAM-MAP-0000000029\n" +
            "    Processor: KSTREAM-AGGREGATE-0000000015 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000014])\n" +
            "      --> none\n" +
            "      <-- KSTREAM-PEEK-0000000013\n" +
            "    Sink: KSTREAM-SINK-0000000031 (topic: external-command)\n" +
            "      <-- KSTREAM-PEEK-0000000030\n" +
            "    Sink: KSTREAM-SINK-0000000036 (topic: dlq-topic)\n" +
            "      <-- KSTREAM-MAPVALUES-0000000035\n" +
            "    Sink: KSTREAM-SINK-0000000038 (topic: retryTopic)\n" +
            "      <-- KSTREAM-MAP-0000000037\n\n";
}
