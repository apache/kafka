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
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("deprecation")
public class RepartitionTopicNamingTest {

    private final KeyValueMapper<String, String, String> kvMapper = (k, v) -> k + v;
    private static final String INPUT_TOPIC = "input";
    private static final String COUNT_TOPIC = "outputTopic_0";
    private static final String AGGREGATION_TOPIC = "outputTopic_1";
    private static final String REDUCE_TOPIC = "outputTopic_2";
    private static final String JOINED_TOPIC = "outputTopicForJoin";

    private final String firstRepartitionTopicName = "count-stream";
    private final String secondRepartitionTopicName = "aggregate-stream";
    private final String thirdRepartitionTopicName = "reduced-stream";
    private final String fourthRepartitionTopicName = "joined-stream";
    private final Pattern repartitionTopicPattern = Pattern.compile("Sink: .*-repartition");


    @Test
    public void shouldReuseFirstRepartitionTopicNameWhenOptimizing() {

        final String optimizedTopology = buildTopology(StreamsConfig.OPTIMIZE).describe().toString();
        final String unOptimizedTopology = buildTopology(StreamsConfig.NO_OPTIMIZATION).describe().toString();

        assertThat(optimizedTopology, is(EXPECTED_OPTIMIZED_TOPOLOGY));
        // only one repartition topic
        assertThat(1, is(getCountOfRepartitionTopicsFound(optimizedTopology, repartitionTopicPattern)));
        // the first named repartition topic
        assertTrue(optimizedTopology.contains(firstRepartitionTopicName + "-repartition"));


        assertThat(unOptimizedTopology, is(EXPECTED_UNOPTIMIZED_TOPOLOGY));
        // now 4 repartition topic
        assertThat(4, is(getCountOfRepartitionTopicsFound(unOptimizedTopology, repartitionTopicPattern)));
        // all 4 named repartition topics present
        assertTrue(unOptimizedTopology.contains(firstRepartitionTopicName + "-repartition"));
        assertTrue(unOptimizedTopology.contains(secondRepartitionTopicName + "-repartition"));
        assertTrue(unOptimizedTopology.contains(thirdRepartitionTopicName + "-repartition"));
        assertTrue(unOptimizedTopology.contains(fourthRepartitionTopicName + "-left-repartition"));

    }

    // can't use same repartition topic name
    @Test
    public void shouldFailWithSameRepartitionTopicName() {
        try {
            final StreamsBuilder builder = new StreamsBuilder();
            builder.<String, String>stream("topic").selectKey((k, v) -> k)
                                            .groupByKey(Grouped.as("grouping"))
                                            .count().toStream();

            builder.<String, String>stream("topicII").selectKey((k, v) -> k)
                                              .groupByKey(Grouped.as("grouping"))
                                              .count().toStream();
            builder.build();
            fail("Should not build re-using repartition topic name");
        } catch (final TopologyException te) {
              // ok
        }
    }

    @Test
    public void shouldNotFailWithSameRepartitionTopicNameUsingSameKGroupedStream() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KGroupedStream<String, String> kGroupedStream = builder.<String, String>stream("topic")
                                                                     .selectKey((k, v) -> k)
                                                                     .groupByKey(Grouped.as("grouping"));

        kGroupedStream.windowedBy(TimeWindows.of(Duration.ofMillis(10L))).count().toStream().to("output-one");
        kGroupedStream.windowedBy(TimeWindows.of(Duration.ofMillis(30L))).count().toStream().to("output-two");

        final String topologyString = builder.build().describe().toString();
        assertThat(1, is(getCountOfRepartitionTopicsFound(topologyString, repartitionTopicPattern)));
        assertTrue(topologyString.contains("grouping-repartition"));
    }

    @Test
    public void shouldNotFailWithSameRepartitionTopicNameUsingSameTimeWindowStream() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KGroupedStream<String, String> kGroupedStream = builder.<String, String>stream("topic")
                                                                     .selectKey((k, v) -> k)
                                                                     .groupByKey(Grouped.as("grouping"));

        final TimeWindowedKStream<String, String> timeWindowedKStream = kGroupedStream.windowedBy(TimeWindows.of(Duration.ofMillis(10L)));

        timeWindowedKStream.count().toStream().to("output-one");
        timeWindowedKStream.reduce((v, v2) -> v + v2).toStream().to("output-two");
        kGroupedStream.windowedBy(TimeWindows.of(Duration.ofMillis(30L))).count().toStream().to("output-two");

        final String topologyString = builder.build().describe().toString();
        assertThat(1, is(getCountOfRepartitionTopicsFound(topologyString, repartitionTopicPattern)));
        assertTrue(topologyString.contains("grouping-repartition"));
    }

    @Test
    public void shouldNotFailWithSameRepartitionTopicNameUsingSameSessionWindowStream() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KGroupedStream<String, String> kGroupedStream = builder.<String, String>stream("topic")
                                                                     .selectKey((k, v) -> k)
                                                                     .groupByKey(Grouped.as("grouping"));

        final SessionWindowedKStream<String, String> sessionWindowedKStream = kGroupedStream.windowedBy(SessionWindows.with(Duration.ofMillis(10L)));

        sessionWindowedKStream.count().toStream().to("output-one");
        sessionWindowedKStream.reduce((v, v2) -> v + v2).toStream().to("output-two");
        kGroupedStream.windowedBy(TimeWindows.of(Duration.ofMillis(30L))).count().toStream().to("output-two");

        final String topologyString = builder.build().describe().toString();
        assertThat(1, is(getCountOfRepartitionTopicsFound(topologyString, repartitionTopicPattern)));
        assertTrue(topologyString.contains("grouping-repartition"));
    }

    @Test
    public void shouldNotFailWithSameRepartitionTopicNameUsingSameKGroupedTable() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KGroupedTable<String, String> kGroupedTable = builder.<String, String>table("topic")
                                                                   .groupBy(KeyValue::pair, Grouped.as("grouping"));
        kGroupedTable.count().toStream().to("output-count");
        kGroupedTable.reduce((v, v2) -> v2, (v, v2) -> v2).toStream().to("output-reduce");
        final String topologyString = builder.build().describe().toString();
        assertThat(1, is(getCountOfRepartitionTopicsFound(topologyString, repartitionTopicPattern)));
        assertTrue(topologyString.contains("grouping-repartition"));
    }

    @Test
    public void shouldNotReuseRepartitionNodeWithUnamedRepartitionTopics() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KGroupedStream<String, String> kGroupedStream = builder.<String, String>stream("topic")
                                                                     .selectKey((k, v) -> k)
                                                                     .groupByKey();
        kGroupedStream.windowedBy(TimeWindows.of(Duration.ofMillis(10L))).count().toStream().to("output-one");
        kGroupedStream.windowedBy(TimeWindows.of(Duration.ofMillis(30L))).count().toStream().to("output-two");
        final String topologyString = builder.build().describe().toString();
        assertThat(2, is(getCountOfRepartitionTopicsFound(topologyString, repartitionTopicPattern)));
    }

    @Test
    public void shouldNotReuseRepartitionNodeWithUnamedRepartitionTopicsKGroupedTable() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KGroupedTable<String, String> kGroupedTable = builder.<String, String>table("topic").groupBy(KeyValue::pair);
        kGroupedTable.count().toStream().to("output-count");
        kGroupedTable.reduce((v, v2) -> v2, (v, v2) -> v2).toStream().to("output-reduce");
        final String topologyString = builder.build().describe().toString();
        assertThat(2, is(getCountOfRepartitionTopicsFound(topologyString, repartitionTopicPattern)));
    }

    @Test
    public void shouldNotFailWithSameRepartitionTopicNameUsingSameKGroupedStreamOptimizationsOn() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KGroupedStream<String, String> kGroupedStream = builder.<String, String>stream("topic")
                                                                     .selectKey((k, v) -> k)
                                                                     .groupByKey(Grouped.as("grouping"));
        kGroupedStream.windowedBy(TimeWindows.of(Duration.ofMillis(10L))).count();
        kGroupedStream.windowedBy(TimeWindows.of(Duration.ofMillis(30L))).count();
        final Properties properties = new Properties();
        properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
        final Topology topology = builder.build(properties);
        assertThat(getCountOfRepartitionTopicsFound(topology.describe().toString(), repartitionTopicPattern), is(1));
    }


    // can't use same repartition topic name in joins
    @Test
    public void shouldFailWithSameRepartitionTopicNameInJoin() {
        try {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> stream1 = builder.<String, String>stream("topic").selectKey((k, v) -> k);
            final KStream<String, String> stream2 = builder.<String, String>stream("topic2").selectKey((k, v) -> k);
            final KStream<String, String> stream3 = builder.<String, String>stream("topic3").selectKey((k, v) -> k);

            final KStream<String, String> joined = stream1.join(stream2, (v1, v2) -> v1 + v2,
                                                                JoinWindows.of(Duration.ofMillis(30L)),
                                                                Joined.named("join-repartition"));

            joined.join(stream3, (v1, v2) -> v1 + v2, JoinWindows.of(Duration.ofMillis(30L)),
                                                      Joined.named("join-repartition"));
            builder.build();
            fail("Should not build re-using repartition topic name");
        } catch (final TopologyException te) {
            // ok
        }
    }

    @Test
    public void shouldPassWithSameRepartitionTopicNameUsingSameKGroupedStreamOptimized() {
        final StreamsBuilder builder = new StreamsBuilder();
        final Properties properties = new Properties();
        properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
        final KGroupedStream<String, String> kGroupedStream = builder.<String, String>stream("topic")
                                                                     .selectKey((k, v) -> k)
                                                                     .groupByKey(Grouped.as("grouping"));
        kGroupedStream.windowedBy(TimeWindows.of(Duration.ofMillis(10L))).count();
        kGroupedStream.windowedBy(TimeWindows.of(Duration.ofMillis(30L))).count();
        builder.build(properties);
    }


    @Test
    public void shouldKeepRepartitionTopicNameForJoins() {

        final String expectedLeftRepartitionTopic = "(topic: my-join-left-repartition)";
        final String expectedRightRepartitionTopic = "(topic: my-join-right-repartition)";


        final String joinTopologyFirst = buildStreamJoin(false);

        assertTrue(joinTopologyFirst.contains(expectedLeftRepartitionTopic));
        assertTrue(joinTopologyFirst.contains(expectedRightRepartitionTopic));

        final String joinTopologyUpdated = buildStreamJoin(true);

        assertTrue(joinTopologyUpdated.contains(expectedLeftRepartitionTopic));
        assertTrue(joinTopologyUpdated.contains(expectedRightRepartitionTopic));
    }

    @Test
    public void shouldKeepRepartitionTopicNameForGroupByKeyTimeWindows() {

        final String expectedTimeWindowRepartitionTopic = "(topic: time-window-grouping-repartition)";

        final String timeWindowGroupingRepartitionTopology = buildStreamGroupByKeyTimeWindows(false, true);
        assertTrue(timeWindowGroupingRepartitionTopology.contains(expectedTimeWindowRepartitionTopic));

        final String timeWindowGroupingUpdatedTopology = buildStreamGroupByKeyTimeWindows(true, true);
        assertTrue(timeWindowGroupingUpdatedTopology.contains(expectedTimeWindowRepartitionTopic));
    }

    @Test
    public void shouldKeepRepartitionTopicNameForGroupByTimeWindows() {

        final String expectedTimeWindowRepartitionTopic = "(topic: time-window-grouping-repartition)";

        final String timeWindowGroupingRepartitionTopology = buildStreamGroupByKeyTimeWindows(false, false);
        assertTrue(timeWindowGroupingRepartitionTopology.contains(expectedTimeWindowRepartitionTopic));

        final String timeWindowGroupingUpdatedTopology = buildStreamGroupByKeyTimeWindows(true, false);
        assertTrue(timeWindowGroupingUpdatedTopology.contains(expectedTimeWindowRepartitionTopic));
    }


    @Test
    public void shouldKeepRepartitionTopicNameForGroupByKeyNoWindows() {

        final String expectedNoWindowRepartitionTopic = "(topic: kstream-grouping-repartition)";

        final String noWindowGroupingRepartitionTopology = buildStreamGroupByKeyNoWindows(false, true);
        assertTrue(noWindowGroupingRepartitionTopology.contains(expectedNoWindowRepartitionTopic));

        final String noWindowGroupingUpdatedTopology = buildStreamGroupByKeyNoWindows(true, true);
        assertTrue(noWindowGroupingUpdatedTopology.contains(expectedNoWindowRepartitionTopic));
    }

    @Test
    public void shouldKeepRepartitionTopicNameForGroupByNoWindows() {

        final String expectedNoWindowRepartitionTopic = "(topic: kstream-grouping-repartition)";

        final String noWindowGroupingRepartitionTopology = buildStreamGroupByKeyNoWindows(false, false);
        assertTrue(noWindowGroupingRepartitionTopology.contains(expectedNoWindowRepartitionTopic));

        final String noWindowGroupingUpdatedTopology = buildStreamGroupByKeyNoWindows(true, false);
        assertTrue(noWindowGroupingUpdatedTopology.contains(expectedNoWindowRepartitionTopic));
    }


    @Test
    public void shouldKeepRepartitionTopicNameForGroupByKeySessionWindows() {

        final String expectedSessionWindowRepartitionTopic = "(topic: session-window-grouping-repartition)";

        final String sessionWindowGroupingRepartitionTopology = buildStreamGroupByKeySessionWindows(false, true);
        assertTrue(sessionWindowGroupingRepartitionTopology.contains(expectedSessionWindowRepartitionTopic));

        final String sessionWindowGroupingUpdatedTopology = buildStreamGroupByKeySessionWindows(true, true);
        assertTrue(sessionWindowGroupingUpdatedTopology.contains(expectedSessionWindowRepartitionTopic));
    }

    @Test
    public void shouldKeepRepartitionTopicNameForGroupBySessionWindows() {

        final String expectedSessionWindowRepartitionTopic = "(topic: session-window-grouping-repartition)";

        final String sessionWindowGroupingRepartitionTopology = buildStreamGroupByKeySessionWindows(false, false);
        assertTrue(sessionWindowGroupingRepartitionTopology.contains(expectedSessionWindowRepartitionTopic));

        final String sessionWindowGroupingUpdatedTopology = buildStreamGroupByKeySessionWindows(true, false);
        assertTrue(sessionWindowGroupingUpdatedTopology.contains(expectedSessionWindowRepartitionTopic));
    }

    @Test
    public void shouldKeepRepartitionNameForGroupByKTable() {
        final String expectedKTableGroupByRepartitionTopic = "(topic: ktable-group-by-repartition)";

        final String ktableGroupByTopology = buildKTableGroupBy(false);
        assertTrue(ktableGroupByTopology.contains(expectedKTableGroupByRepartitionTopic));

        final String ktableUpdatedGroupByTopology = buildKTableGroupBy(true);
        assertTrue(ktableUpdatedGroupByTopology.contains(expectedKTableGroupByRepartitionTopic));
    }


    private String buildKTableGroupBy(final boolean otherOperations) {
        final String ktableGroupByTopicName = "ktable-group-by";
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String, String> ktable = builder.table("topic");

        if (otherOperations) {
            ktable.filter((k, v) -> true).groupBy(KeyValue::pair, Grouped.as(ktableGroupByTopicName)).count();
        } else {
            ktable.groupBy(KeyValue::pair, Grouped.as(ktableGroupByTopicName)).count();
        }

        return builder.build().describe().toString();
    }

    private String buildStreamGroupByKeyTimeWindows(final boolean otherOperations, final boolean isGroupByKey) {

        final String groupedTimeWindowRepartitionTopicName = "time-window-grouping";
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> selectKeyStream = builder.<String, String>stream("topic").selectKey((k, v) -> k + v);


        if (isGroupByKey) {
            if (otherOperations) {
                selectKeyStream.filter((k, v) -> true).mapValues(v -> v).groupByKey(Grouped.as(groupedTimeWindowRepartitionTopicName)).windowedBy(TimeWindows.of(Duration.ofMillis(10L))).count();
            } else {
                selectKeyStream.groupByKey(Grouped.as(groupedTimeWindowRepartitionTopicName)).windowedBy(TimeWindows.of(Duration.ofMillis(10L))).count();
            }
        } else {
            if (otherOperations) {
                selectKeyStream.filter((k, v) -> true).mapValues(v -> v).groupBy(kvMapper, Grouped.as(groupedTimeWindowRepartitionTopicName)).count();
            } else {
                selectKeyStream.groupBy(kvMapper, Grouped.as(groupedTimeWindowRepartitionTopicName)).count();
            }
        }

        return builder.build().describe().toString();
    }


    private String buildStreamGroupByKeySessionWindows(final boolean otherOperations, final boolean isGroupByKey) {

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> selectKeyStream = builder.<String, String>stream("topic").selectKey((k, v) -> k + v);

        final String groupedSessionWindowRepartitionTopicName = "session-window-grouping";
        if (isGroupByKey) {
            if (otherOperations) {
                selectKeyStream.filter((k, v) -> true).mapValues(v -> v).groupByKey(Grouped.as(groupedSessionWindowRepartitionTopicName)).windowedBy(SessionWindows.with(Duration.ofMillis(10L))).count();
            } else {
                selectKeyStream.groupByKey(Grouped.as(groupedSessionWindowRepartitionTopicName)).windowedBy(SessionWindows.with(Duration.ofMillis(10L))).count();
            }
        } else {
            if (otherOperations) {
                selectKeyStream.filter((k, v) -> true).mapValues(v -> v).groupBy(kvMapper, Grouped.as(groupedSessionWindowRepartitionTopicName)).windowedBy(SessionWindows.with(Duration.ofMillis(10L))).count();
            } else {
                selectKeyStream.groupBy(kvMapper, Grouped.as(groupedSessionWindowRepartitionTopicName)).windowedBy(SessionWindows.with(Duration.ofMillis(10L))).count();
            }
        }

        return builder.build().describe().toString();
    }


    private String buildStreamGroupByKeyNoWindows(final boolean otherOperations, final boolean isGroupByKey) {

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> selectKeyStream = builder.<String, String>stream("topic").selectKey((k, v) -> k + v);

        final String groupByAndCountRepartitionTopicName = "kstream-grouping";
        if (isGroupByKey) {
            if (otherOperations) {
                selectKeyStream.filter((k, v) -> true).mapValues(v -> v).groupByKey(Grouped.as(groupByAndCountRepartitionTopicName)).count();
            } else {
                selectKeyStream.groupByKey(Grouped.as(groupByAndCountRepartitionTopicName)).count();
            }
        } else {
            if (otherOperations) {
                selectKeyStream.filter((k, v) -> true).mapValues(v -> v).groupBy(kvMapper, Grouped.as(groupByAndCountRepartitionTopicName)).count();
            } else {
                selectKeyStream.groupBy(kvMapper, Grouped.as(groupByAndCountRepartitionTopicName)).count();
            }
        }

        return builder.build().describe().toString();
    }

    private String buildStreamJoin(final boolean includeOtherOperations) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> initialStreamOne = builder.stream("topic-one");
        final KStream<String, String> initialStreamTwo = builder.stream("topic-two");

        final KStream<String, String> updatedStreamOne;
        final KStream<String, String> updatedStreamTwo;

        if (includeOtherOperations) {
            // without naming the join, the repartition topic name would change due to operator changing before join performed
            updatedStreamOne = initialStreamOne.selectKey((k, v) -> k + v).filter((k, v) -> true).peek((k, v) -> System.out.println(k + v));
            updatedStreamTwo = initialStreamTwo.selectKey((k, v) -> k + v).filter((k, v) -> true).peek((k, v) -> System.out.println(k + v));
        } else {
            updatedStreamOne = initialStreamOne.selectKey((k, v) -> k + v);
            updatedStreamTwo = initialStreamTwo.selectKey((k, v) -> k + v);
        }

        final String joinRepartitionTopicName = "my-join";
        updatedStreamOne.join(updatedStreamTwo, (v1, v2) -> v1 + v2,
                JoinWindows.of(Duration.ofMillis(1000L)), Joined.with(Serdes.String(), Serdes.String(), Serdes.String(), joinRepartitionTopicName));

        return builder.build().describe().toString();
    }


    private int getCountOfRepartitionTopicsFound(final String topologyString, final Pattern repartitionTopicPattern) {
        final Matcher matcher = repartitionTopicPattern.matcher(topologyString);
        final List<String> repartitionTopicsFound = new ArrayList<>();
        while (matcher.find()) {
            repartitionTopicsFound.add(matcher.group());
        }
        return repartitionTopicsFound.size();
    }


    private Topology buildTopology(final String optimizationConfig) {
        final Initializer<Integer> initializer = () -> 0;
        final Aggregator<String, String, Integer> aggregator = (k, v, agg) -> agg + v.length();
        final Reducer<String> reducer = (v1, v2) -> v1 + ":" + v2;
        final List<String> processorValueCollector = new ArrayList<>();

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> sourceStream = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        final KStream<String, String> mappedStream = sourceStream.map((k, v) -> KeyValue.pair(k.toUpperCase(Locale.getDefault()), v));

        mappedStream.filter((k, v) -> k.equals("B")).mapValues(v -> v.toUpperCase(Locale.getDefault()))
                .process(() -> new SimpleProcessor(processorValueCollector));

        final KStream<String, Long> countStream = mappedStream.groupByKey(Grouped.as(firstRepartitionTopicName)).count(Materialized.with(Serdes.String(), Serdes.Long())).toStream();

        countStream.to(COUNT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        mappedStream.groupByKey(Grouped.as(secondRepartitionTopicName)).aggregate(initializer,
                aggregator,
                Materialized.with(Serdes.String(), Serdes.Integer()))
                .toStream().to(AGGREGATION_TOPIC, Produced.with(Serdes.String(), Serdes.Integer()));

        // adding operators for case where the repartition node is further downstream
        mappedStream.filter((k, v) -> true).peek((k, v) -> System.out.println(k + ":" + v)).groupByKey(Grouped.as(thirdRepartitionTopicName))
                .reduce(reducer, Materialized.with(Serdes.String(), Serdes.String()))
                .toStream().to(REDUCE_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        mappedStream.filter((k, v) -> k.equals("A"))
                .join(countStream, (v1, v2) -> v1 + ":" + v2.toString(),
                        JoinWindows.of(Duration.ofMillis(5000L)),
                        Joined.with(Serdes.String(), Serdes.String(), Serdes.Long(), fourthRepartitionTopicName))
                .to(JOINED_TOPIC);

        final Properties properties = new Properties();

        properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, optimizationConfig);
        return builder.build(properties);
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


    private static final String EXPECTED_OPTIMIZED_TOPOLOGY = "Topologies:\n" +
            "   Sub-topology: 0\n" +
            "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n" +
            "      --> KSTREAM-MAP-0000000001\n" +
            "    Processor: KSTREAM-MAP-0000000001 (stores: [])\n" +
            "      --> KSTREAM-FILTER-0000000002, KSTREAM-FILTER-0000000040\n" +
            "      <-- KSTREAM-SOURCE-0000000000\n" +
            "    Processor: KSTREAM-FILTER-0000000002 (stores: [])\n" +
            "      --> KSTREAM-MAPVALUES-0000000003\n" +
            "      <-- KSTREAM-MAP-0000000001\n" +
            "    Processor: KSTREAM-FILTER-0000000040 (stores: [])\n" +
            "      --> KSTREAM-SINK-0000000039\n" +
            "      <-- KSTREAM-MAP-0000000001\n" +
            "    Processor: KSTREAM-MAPVALUES-0000000003 (stores: [])\n" +
            "      --> KSTREAM-PROCESSOR-0000000004\n" +
            "      <-- KSTREAM-FILTER-0000000002\n" +
            "    Processor: KSTREAM-PROCESSOR-0000000004 (stores: [])\n" +
            "      --> none\n" +
            "      <-- KSTREAM-MAPVALUES-0000000003\n" +
            "    Sink: KSTREAM-SINK-0000000039 (topic: count-stream-repartition)\n" +
            "      <-- KSTREAM-FILTER-0000000040\n" +
            "\n" +
            "  Sub-topology: 1\n" +
            "    Source: KSTREAM-SOURCE-0000000041 (topics: [count-stream-repartition])\n" +
            "      --> KSTREAM-FILTER-0000000020, KSTREAM-AGGREGATE-0000000007, KSTREAM-AGGREGATE-0000000014, KSTREAM-FILTER-0000000029\n" +
            "    Processor: KSTREAM-AGGREGATE-0000000007 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000006])\n" +
            "      --> KTABLE-TOSTREAM-0000000011\n" +
            "      <-- KSTREAM-SOURCE-0000000041\n" +
            "    Processor: KTABLE-TOSTREAM-0000000011 (stores: [])\n" +
            "      --> KSTREAM-SINK-0000000012, KSTREAM-WINDOWED-0000000034\n" +
            "      <-- KSTREAM-AGGREGATE-0000000007\n" +
            "    Processor: KSTREAM-FILTER-0000000020 (stores: [])\n" +
            "      --> KSTREAM-PEEK-0000000021\n" +
            "      <-- KSTREAM-SOURCE-0000000041\n" +
            "    Processor: KSTREAM-FILTER-0000000029 (stores: [])\n" +
            "      --> KSTREAM-WINDOWED-0000000033\n" +
            "      <-- KSTREAM-SOURCE-0000000041\n" +
            "    Processor: KSTREAM-PEEK-0000000021 (stores: [])\n" +
            "      --> KSTREAM-REDUCE-0000000023\n" +
            "      <-- KSTREAM-FILTER-0000000020\n" +
            "    Processor: KSTREAM-WINDOWED-0000000033 (stores: [KSTREAM-JOINTHIS-0000000035-store])\n" +
            "      --> KSTREAM-JOINTHIS-0000000035\n" +
            "      <-- KSTREAM-FILTER-0000000029\n" +
            "    Processor: KSTREAM-WINDOWED-0000000034 (stores: [KSTREAM-JOINOTHER-0000000036-store])\n" +
            "      --> KSTREAM-JOINOTHER-0000000036\n" +
            "      <-- KTABLE-TOSTREAM-0000000011\n" +
            "    Processor: KSTREAM-AGGREGATE-0000000014 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000013])\n" +
            "      --> KTABLE-TOSTREAM-0000000018\n" +
            "      <-- KSTREAM-SOURCE-0000000041\n" +
            "    Processor: KSTREAM-JOINOTHER-0000000036 (stores: [KSTREAM-JOINTHIS-0000000035-store])\n" +
            "      --> KSTREAM-MERGE-0000000037\n" +
            "      <-- KSTREAM-WINDOWED-0000000034\n" +
            "    Processor: KSTREAM-JOINTHIS-0000000035 (stores: [KSTREAM-JOINOTHER-0000000036-store])\n" +
            "      --> KSTREAM-MERGE-0000000037\n" +
            "      <-- KSTREAM-WINDOWED-0000000033\n" +
            "    Processor: KSTREAM-REDUCE-0000000023 (stores: [KSTREAM-REDUCE-STATE-STORE-0000000022])\n" +
            "      --> KTABLE-TOSTREAM-0000000027\n" +
            "      <-- KSTREAM-PEEK-0000000021\n" +
            "    Processor: KSTREAM-MERGE-0000000037 (stores: [])\n" +
            "      --> KSTREAM-SINK-0000000038\n" +
            "      <-- KSTREAM-JOINTHIS-0000000035, KSTREAM-JOINOTHER-0000000036\n" +
            "    Processor: KTABLE-TOSTREAM-0000000018 (stores: [])\n" +
            "      --> KSTREAM-SINK-0000000019\n" +
            "      <-- KSTREAM-AGGREGATE-0000000014\n" +
            "    Processor: KTABLE-TOSTREAM-0000000027 (stores: [])\n" +
            "      --> KSTREAM-SINK-0000000028\n" +
            "      <-- KSTREAM-REDUCE-0000000023\n" +
            "    Sink: KSTREAM-SINK-0000000012 (topic: outputTopic_0)\n" +
            "      <-- KTABLE-TOSTREAM-0000000011\n" +
            "    Sink: KSTREAM-SINK-0000000019 (topic: outputTopic_1)\n" +
            "      <-- KTABLE-TOSTREAM-0000000018\n" +
            "    Sink: KSTREAM-SINK-0000000028 (topic: outputTopic_2)\n" +
            "      <-- KTABLE-TOSTREAM-0000000027\n" +
            "    Sink: KSTREAM-SINK-0000000038 (topic: outputTopicForJoin)\n" +
            "      <-- KSTREAM-MERGE-0000000037\n\n";


    private static final String EXPECTED_UNOPTIMIZED_TOPOLOGY = "Topologies:\n" +
            "   Sub-topology: 0\n" +
            "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n" +
            "      --> KSTREAM-MAP-0000000001\n" +
            "    Processor: KSTREAM-MAP-0000000001 (stores: [])\n" +
            "      --> KSTREAM-FILTER-0000000020, KSTREAM-FILTER-0000000002, KSTREAM-FILTER-0000000009, KSTREAM-FILTER-0000000016, KSTREAM-FILTER-0000000029\n" +
            "      <-- KSTREAM-SOURCE-0000000000\n" +
            "    Processor: KSTREAM-FILTER-0000000020 (stores: [])\n" +
            "      --> KSTREAM-PEEK-0000000021\n" +
            "      <-- KSTREAM-MAP-0000000001\n" +
            "    Processor: KSTREAM-FILTER-0000000002 (stores: [])\n" +
            "      --> KSTREAM-MAPVALUES-0000000003\n" +
            "      <-- KSTREAM-MAP-0000000001\n" +
            "    Processor: KSTREAM-FILTER-0000000029 (stores: [])\n" +
            "      --> KSTREAM-FILTER-0000000031\n" +
            "      <-- KSTREAM-MAP-0000000001\n" +
            "    Processor: KSTREAM-PEEK-0000000021 (stores: [])\n" +
            "      --> KSTREAM-FILTER-0000000025\n" +
            "      <-- KSTREAM-FILTER-0000000020\n" +
            "    Processor: KSTREAM-FILTER-0000000009 (stores: [])\n" +
            "      --> KSTREAM-SINK-0000000008\n" +
            "      <-- KSTREAM-MAP-0000000001\n" +
            "    Processor: KSTREAM-FILTER-0000000016 (stores: [])\n" +
            "      --> KSTREAM-SINK-0000000015\n" +
            "      <-- KSTREAM-MAP-0000000001\n" +
            "    Processor: KSTREAM-FILTER-0000000025 (stores: [])\n" +
            "      --> KSTREAM-SINK-0000000024\n" +
            "      <-- KSTREAM-PEEK-0000000021\n" +
            "    Processor: KSTREAM-FILTER-0000000031 (stores: [])\n" +
            "      --> KSTREAM-SINK-0000000030\n" +
            "      <-- KSTREAM-FILTER-0000000029\n" +
            "    Processor: KSTREAM-MAPVALUES-0000000003 (stores: [])\n" +
            "      --> KSTREAM-PROCESSOR-0000000004\n" +
            "      <-- KSTREAM-FILTER-0000000002\n" +
            "    Processor: KSTREAM-PROCESSOR-0000000004 (stores: [])\n" +
            "      --> none\n" +
            "      <-- KSTREAM-MAPVALUES-0000000003\n" +
            "    Sink: KSTREAM-SINK-0000000008 (topic: count-stream-repartition)\n" +
            "      <-- KSTREAM-FILTER-0000000009\n" +
            "    Sink: KSTREAM-SINK-0000000015 (topic: aggregate-stream-repartition)\n" +
            "      <-- KSTREAM-FILTER-0000000016\n" +
            "    Sink: KSTREAM-SINK-0000000024 (topic: reduced-stream-repartition)\n" +
            "      <-- KSTREAM-FILTER-0000000025\n" +
            "    Sink: KSTREAM-SINK-0000000030 (topic: joined-stream-left-repartition)\n" +
            "      <-- KSTREAM-FILTER-0000000031\n" +
            "\n" +
            "  Sub-topology: 1\n" +
            "    Source: KSTREAM-SOURCE-0000000010 (topics: [count-stream-repartition])\n" +
            "      --> KSTREAM-AGGREGATE-0000000007\n" +
            "    Processor: KSTREAM-AGGREGATE-0000000007 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000006])\n" +
            "      --> KTABLE-TOSTREAM-0000000011\n" +
            "      <-- KSTREAM-SOURCE-0000000010\n" +
            "    Processor: KTABLE-TOSTREAM-0000000011 (stores: [])\n" +
            "      --> KSTREAM-SINK-0000000012, KSTREAM-WINDOWED-0000000034\n" +
            "      <-- KSTREAM-AGGREGATE-0000000007\n" +
            "    Source: KSTREAM-SOURCE-0000000032 (topics: [joined-stream-left-repartition])\n" +
            "      --> KSTREAM-WINDOWED-0000000033\n" +
            "    Processor: KSTREAM-WINDOWED-0000000033 (stores: [KSTREAM-JOINTHIS-0000000035-store])\n" +
            "      --> KSTREAM-JOINTHIS-0000000035\n" +
            "      <-- KSTREAM-SOURCE-0000000032\n" +
            "    Processor: KSTREAM-WINDOWED-0000000034 (stores: [KSTREAM-JOINOTHER-0000000036-store])\n" +
            "      --> KSTREAM-JOINOTHER-0000000036\n" +
            "      <-- KTABLE-TOSTREAM-0000000011\n" +
            "    Processor: KSTREAM-JOINOTHER-0000000036 (stores: [KSTREAM-JOINTHIS-0000000035-store])\n" +
            "      --> KSTREAM-MERGE-0000000037\n" +
            "      <-- KSTREAM-WINDOWED-0000000034\n" +
            "    Processor: KSTREAM-JOINTHIS-0000000035 (stores: [KSTREAM-JOINOTHER-0000000036-store])\n" +
            "      --> KSTREAM-MERGE-0000000037\n" +
            "      <-- KSTREAM-WINDOWED-0000000033\n" +
            "    Processor: KSTREAM-MERGE-0000000037 (stores: [])\n" +
            "      --> KSTREAM-SINK-0000000038\n" +
            "      <-- KSTREAM-JOINTHIS-0000000035, KSTREAM-JOINOTHER-0000000036\n" +
            "    Sink: KSTREAM-SINK-0000000012 (topic: outputTopic_0)\n" +
            "      <-- KTABLE-TOSTREAM-0000000011\n" +
            "    Sink: KSTREAM-SINK-0000000038 (topic: outputTopicForJoin)\n" +
            "      <-- KSTREAM-MERGE-0000000037\n" +
            "\n" +
            "  Sub-topology: 2\n" +
            "    Source: KSTREAM-SOURCE-0000000017 (topics: [aggregate-stream-repartition])\n" +
            "      --> KSTREAM-AGGREGATE-0000000014\n" +
            "    Processor: KSTREAM-AGGREGATE-0000000014 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000013])\n" +
            "      --> KTABLE-TOSTREAM-0000000018\n" +
            "      <-- KSTREAM-SOURCE-0000000017\n" +
            "    Processor: KTABLE-TOSTREAM-0000000018 (stores: [])\n" +
            "      --> KSTREAM-SINK-0000000019\n" +
            "      <-- KSTREAM-AGGREGATE-0000000014\n" +
            "    Sink: KSTREAM-SINK-0000000019 (topic: outputTopic_1)\n" +
            "      <-- KTABLE-TOSTREAM-0000000018\n" +
            "\n" +
            "  Sub-topology: 3\n" +
            "    Source: KSTREAM-SOURCE-0000000026 (topics: [reduced-stream-repartition])\n" +
            "      --> KSTREAM-REDUCE-0000000023\n" +
            "    Processor: KSTREAM-REDUCE-0000000023 (stores: [KSTREAM-REDUCE-STATE-STORE-0000000022])\n" +
            "      --> KTABLE-TOSTREAM-0000000027\n" +
            "      <-- KSTREAM-SOURCE-0000000026\n" +
            "    Processor: KTABLE-TOSTREAM-0000000027 (stores: [])\n" +
            "      --> KSTREAM-SINK-0000000028\n" +
            "      <-- KSTREAM-REDUCE-0000000023\n" +
            "    Sink: KSTREAM-SINK-0000000028 (topic: outputTopic_2)\n" +
            "      <-- KTABLE-TOSTREAM-0000000027\n\n";


}
