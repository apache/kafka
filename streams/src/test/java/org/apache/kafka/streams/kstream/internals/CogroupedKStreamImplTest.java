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
package org.apache.kafka.streams.kstream.internals;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Properties;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.CogroupedKStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

public class CogroupedKStreamImplTest {
    private final Consumed<String, String> stringConsumed = Consumed.with(Serdes.String(), Serdes.String());
    private static final String TOPIC = "topic";
    private static final String OUTPUT = "output";
    private KGroupedStream<String, String> groupedStream;
    private CogroupedKStream<String, String> cogroupedStream;

    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    private static final Aggregator<String, String, String> STRING_AGGREGATOR =
        (key, value, aggregate) -> aggregate + value;

    private static final Initializer<String> STRING_INITIALIZER = () -> "";

    private static final Aggregator<String, String, Integer> STRING_SUM_AGGREGATOR =
        (key, value, aggregate) -> aggregate + Integer.parseInt(value);

    private static final Aggregator<? super String, ? super Integer, Integer> SUM_AGGREGATOR =
        (key, value, aggregate) -> aggregate + value;

    private static final Initializer<Integer> SUM_INITIALIZER = () -> 0;


    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream = builder.stream(TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        groupedStream = stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
        cogroupedStream = groupedStream.cogroup(MockAggregator.TOSTRING_ADDER);
    }

    @Test
    public void shouldThrowNPEInCogroupIfKGroupedStreamIsNull() {
        assertThrows(NullPointerException.class, () -> cogroupedStream.cogroup(null, MockAggregator.TOSTRING_ADDER));
    }

    @Test
    public void shouldNotHaveNullAggregatorOnCogroup() {
        assertThrows(NullPointerException.class, () -> cogroupedStream.cogroup(groupedStream, null));
    }

    @Test
    public void shouldNotHaveNullInitializerOnAggregate() {
        assertThrows(NullPointerException.class, () -> cogroupedStream.aggregate(null));
    }

    @Test
    public void shouldNotHaveNullInitializerOnAggregateWitNamed() {
        assertThrows(NullPointerException.class, () -> cogroupedStream.aggregate(null, Named.as("name")));
    }

    @Test
    public void shouldNotHaveNullInitializerOnAggregateWitMaterialized() {
        assertThrows(NullPointerException.class, () -> cogroupedStream.aggregate(null, Materialized.as("store")));
    }

    @Test
    public void shouldNotHaveNullInitializerOnAggregateWitNamedAndMaterialized() {
        assertThrows(NullPointerException.class, () -> cogroupedStream.aggregate(null, Named.as("name"), Materialized.as("store")));
    }

    @Test
    public void shouldNotHaveNullNamedOnAggregate() {
        assertThrows(NullPointerException.class, () -> cogroupedStream.aggregate(STRING_INITIALIZER, (Named) null));
    }

    @Test
    public void shouldNotHaveNullMaterializedOnAggregate() {
        assertThrows(NullPointerException.class, () -> cogroupedStream.aggregate(STRING_INITIALIZER, (Materialized<String, String, KeyValueStore<Bytes, byte[]>>) null));
    }

    @Test
    public void shouldNotHaveNullNamedOnAggregateWithMateriazlied() {
        assertThrows(NullPointerException.class, () -> cogroupedStream.aggregate(STRING_INITIALIZER,  null,  Materialized.as("store")));
    }

    @Test
    public void shouldNotHaveNullMaterializedOnAggregateWithNames() {
        assertThrows(NullPointerException.class, () -> cogroupedStream.aggregate(STRING_INITIALIZER, Named.as("name"), null));
    }

    @Test
    public void shouldNotHaveNullWindowOnWindowedByTime() {
        assertThrows(NullPointerException.class, () -> cogroupedStream.windowedBy((Windows<? extends Window>) null));
    }

    @Test
    public void shouldNotHaveNullWindowOnWindowedBySession() {
        assertThrows(NullPointerException.class, () -> cogroupedStream.windowedBy((SessionWindows) null));
    }

    @Test
    public void shouldNotHaveNullWindowOnWindowedBySliding() {
        assertThrows(NullPointerException.class, () -> cogroupedStream.windowedBy((SlidingWindows) null));
    }

    @Test
    public void shouldNameProcessorsAndStoreBasedOnNamedParameter() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> stream1 = builder.stream("one", stringConsumed);
        final KStream<String, String> test2 = builder.stream("two", stringConsumed);

        final KGroupedStream<String, String> groupedOne = stream1.groupByKey();
        final KGroupedStream<String, String> groupedTwo = test2.groupByKey();

        final KTable<String, String> customers = groupedOne
            .cogroup(STRING_AGGREGATOR)
            .cogroup(groupedTwo, STRING_AGGREGATOR)
            .aggregate(STRING_INITIALIZER, Named.as("test"), Materialized.as("store"));

        customers.toStream().to(OUTPUT);

        final String topologyDescription = builder.build().describe().toString();

        assertThat(
            topologyDescription,
            equalTo("Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000000 (topics: [one])\n" +
                "      --> test-cogroup-agg-0\n" +
                "    Source: KSTREAM-SOURCE-0000000001 (topics: [two])\n" +
                "      --> test-cogroup-agg-1\n" +
                "    Processor: test-cogroup-agg-0 (stores: [store])\n" +
                "      --> test-cogroup-merge\n" +
                "      <-- KSTREAM-SOURCE-0000000000\n" +
                "    Processor: test-cogroup-agg-1 (stores: [store])\n" +
                "      --> test-cogroup-merge\n" +
                "      <-- KSTREAM-SOURCE-0000000001\n" +
                "    Processor: test-cogroup-merge (stores: [])\n" +
                "      --> KTABLE-TOSTREAM-0000000005\n" +
                "      <-- test-cogroup-agg-0, test-cogroup-agg-1\n" +
                "    Processor: KTABLE-TOSTREAM-0000000005 (stores: [])\n" +
                "      --> KSTREAM-SINK-0000000006\n" +
                "      <-- test-cogroup-merge\n" +
                "    Sink: KSTREAM-SINK-0000000006 (topic: output)\n" +
                "      <-- KTABLE-TOSTREAM-0000000005\n\n"));
    }

    @Test
    public void shouldNameRepartitionTopic() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> stream1 = builder.stream("one", stringConsumed);
        final KStream<String, String> test2 = builder.stream("two", stringConsumed);

        final KGroupedStream<String, String> groupedOne = stream1.map((k, v) -> new KeyValue<>(v, k)).groupByKey(Grouped.as("repartition-test"));
        final KGroupedStream<String, String> groupedTwo = test2.groupByKey();

        final KTable<String, String> customers = groupedOne
                .cogroup(STRING_AGGREGATOR)
                .cogroup(groupedTwo, STRING_AGGREGATOR)
                .aggregate(STRING_INITIALIZER);

        customers.toStream().to(OUTPUT);

        final String topologyDescription = builder.build().describe().toString();

        assertThat(
                topologyDescription,
                equalTo("Topologies:\n" +
                        "   Sub-topology: 0\n" +
                        "    Source: KSTREAM-SOURCE-0000000000 (topics: [one])\n" +
                        "      --> KSTREAM-MAP-0000000002\n" +
                        "    Processor: KSTREAM-MAP-0000000002 (stores: [])\n" +
                        "      --> repartition-test-repartition-filter\n" +
                        "      <-- KSTREAM-SOURCE-0000000000\n" +
                        "    Processor: repartition-test-repartition-filter (stores: [])\n" +
                        "      --> repartition-test-repartition-sink\n" +
                        "      <-- KSTREAM-MAP-0000000002\n" +
                        "    Sink: repartition-test-repartition-sink (topic: repartition-test-repartition)\n" +
                        "      <-- repartition-test-repartition-filter\n\n" +
                        "  Sub-topology: 1\n" +
                        "    Source: KSTREAM-SOURCE-0000000001 (topics: [two])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-0000000008\n" +
                        "    Source: repartition-test-repartition-source (topics: [repartition-test-repartition])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-0000000007\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000007 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000009\n" +
                        "      <-- repartition-test-repartition-source\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000008 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000009\n" +
                        "      <-- KSTREAM-SOURCE-0000000001\n" +
                        "    Processor: COGROUPKSTREAM-MERGE-0000000009 (stores: [])\n" +
                        "      --> KTABLE-TOSTREAM-0000000010\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-0000000007, COGROUPKSTREAM-AGGREGATE-0000000008\n" +
                        "    Processor: KTABLE-TOSTREAM-0000000010 (stores: [])\n" +
                        "      --> KSTREAM-SINK-0000000011\n" +
                        "      <-- COGROUPKSTREAM-MERGE-0000000009\n" +
                        "    Sink: KSTREAM-SINK-0000000011 (topic: output)\n" +
                        "      <-- KTABLE-TOSTREAM-0000000010\n\n"));
    }

    @Test
    public void shouldInsertRepartitionsTopicForUpstreamKeyModification() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream1 = builder.stream("one", stringConsumed);
        final KStream<String, String> test2 = builder.stream("two", stringConsumed);

        final KGroupedStream<String, String> groupedOne = stream1.map((k, v) -> new KeyValue<>(v, k)).groupByKey();
        final KGroupedStream<String, String> groupedTwo = test2.groupByKey();

        final KTable<String, String> customers = groupedOne
                .cogroup(STRING_AGGREGATOR)
                .cogroup(groupedTwo, STRING_AGGREGATOR)
                .aggregate(STRING_INITIALIZER, Named.as("test"), Materialized.as("store"));

        customers.toStream().to(OUTPUT);

        final String topologyDescription = builder.build().describe().toString();

        assertThat(
                topologyDescription,
                equalTo("Topologies:\n" +
                        "   Sub-topology: 0\n" +
                        "    Source: KSTREAM-SOURCE-0000000000 (topics: [one])\n" +
                        "      --> KSTREAM-MAP-0000000002\n" +
                        "    Processor: KSTREAM-MAP-0000000002 (stores: [])\n" +
                        "      --> store-repartition-filter\n" +
                        "      <-- KSTREAM-SOURCE-0000000000\n" +
                        "    Processor: store-repartition-filter (stores: [])\n" +
                        "      --> store-repartition-sink\n" +
                        "      <-- KSTREAM-MAP-0000000002\n" +
                        "    Sink: store-repartition-sink (topic: store-repartition)\n" +
                        "      <-- store-repartition-filter\n\n" +
                        "  Sub-topology: 1\n" +
                        "    Source: KSTREAM-SOURCE-0000000001 (topics: [two])\n" +
                        "      --> test-cogroup-agg-1\n" +
                        "    Source: store-repartition-source (topics: [store-repartition])\n" +
                        "      --> test-cogroup-agg-0\n" +
                        "    Processor: test-cogroup-agg-0 (stores: [store])\n" +
                        "      --> test-cogroup-merge\n" +
                        "      <-- store-repartition-source\n" +
                        "    Processor: test-cogroup-agg-1 (stores: [store])\n" +
                        "      --> test-cogroup-merge\n" +
                        "      <-- KSTREAM-SOURCE-0000000001\n" +
                        "    Processor: test-cogroup-merge (stores: [])\n" +
                        "      --> KTABLE-TOSTREAM-0000000009\n" +
                        "      <-- test-cogroup-agg-0, test-cogroup-agg-1\n" +
                        "    Processor: KTABLE-TOSTREAM-0000000009 (stores: [])\n" +
                        "      --> KSTREAM-SINK-0000000010\n" +
                        "      <-- test-cogroup-merge\n" +
                        "    Sink: KSTREAM-SINK-0000000010 (topic: output)\n" +
                        "      <-- KTABLE-TOSTREAM-0000000009\n\n"));
    }

    @Test
    public void shouldInsertRepartitionsTopicForUpstreamKeyModificationWithGroupedReusedInSameCogroups() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> stream1 = builder.stream("one", stringConsumed);
        final KStream<String, String> stream2 = builder.stream("two", stringConsumed);

        final KGroupedStream<String, String> groupedOne = stream1.map((k, v) -> new KeyValue<>(v, k)).groupByKey();
        final KGroupedStream<String, String> groupedTwo = stream2.groupByKey();

        final KTable<String, String> cogroupedTwo = groupedOne
                .cogroup(STRING_AGGREGATOR)
                .cogroup(groupedTwo, STRING_AGGREGATOR)
                .aggregate(STRING_INITIALIZER);

        final KTable<String, String> cogroupedOne = groupedOne
                .cogroup(STRING_AGGREGATOR)
                .cogroup(groupedTwo, STRING_AGGREGATOR)
                .aggregate(STRING_INITIALIZER);

        cogroupedOne.toStream().to(OUTPUT);
        cogroupedTwo.toStream().to("OUTPUT2");

        final String topologyDescription = builder.build().describe().toString();

        assertThat(
                topologyDescription,
                equalTo("Topologies:\n" +
                        "   Sub-topology: 0\n" +
                        "    Source: KSTREAM-SOURCE-0000000000 (topics: [one])\n" +
                        "      --> KSTREAM-MAP-0000000002\n" +
                        "    Processor: KSTREAM-MAP-0000000002 (stores: [])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000010-repartition-filter, COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-filter\n" +
                        "      <-- KSTREAM-SOURCE-0000000000\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-filter (stores: [])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-sink\n" +
                        "      <-- KSTREAM-MAP-0000000002\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000010-repartition-filter (stores: [])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000010-repartition-sink\n" +
                        "      <-- KSTREAM-MAP-0000000002\n" +
                        "    Sink: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-sink (topic: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition)\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-filter\n" +
                        "    Sink: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000010-repartition-sink (topic: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000010-repartition)\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000010-repartition-filter\n\n" +
                        "  Sub-topology: 1\n" +
                        "    Source: KSTREAM-SOURCE-0000000001 (topics: [two])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-0000000008, COGROUPKSTREAM-AGGREGATE-0000000015\n" +
                        "    Source: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-source (topics: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-0000000007\n" +
                        "    Source: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000010-repartition-source (topics: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000010-repartition])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-0000000014\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000007 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000009\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-source\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000008 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000009\n" +
                        "      <-- KSTREAM-SOURCE-0000000001\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000014 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000010])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000016\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000010-repartition-source\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000015 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000010])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000016\n" +
                        "      <-- KSTREAM-SOURCE-0000000001\n" +
                        "    Processor: COGROUPKSTREAM-MERGE-0000000009 (stores: [])\n" +
                        "      --> KTABLE-TOSTREAM-0000000019\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-0000000007, COGROUPKSTREAM-AGGREGATE-0000000008\n" +
                        "    Processor: COGROUPKSTREAM-MERGE-0000000016 (stores: [])\n" +
                        "      --> KTABLE-TOSTREAM-0000000017\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-0000000014, COGROUPKSTREAM-AGGREGATE-0000000015\n" +
                        "    Processor: KTABLE-TOSTREAM-0000000017 (stores: [])\n" +
                        "      --> KSTREAM-SINK-0000000018\n" +
                        "      <-- COGROUPKSTREAM-MERGE-0000000016\n" +
                        "    Processor: KTABLE-TOSTREAM-0000000019 (stores: [])\n" +
                        "      --> KSTREAM-SINK-0000000020\n" +
                        "      <-- COGROUPKSTREAM-MERGE-0000000009\n" +
                        "    Sink: KSTREAM-SINK-0000000018 (topic: output)\n" +
                        "      <-- KTABLE-TOSTREAM-0000000017\n" +
                        "    Sink: KSTREAM-SINK-0000000020 (topic: OUTPUT2)\n" +
                        "      <-- KTABLE-TOSTREAM-0000000019\n\n"));
    }

    @Test
    public void shouldInsertRepartitionsTopicForUpstreamKeyModificationWithGroupedReusedInSameCogroupsWithOptimization() {
        final Properties properties = new Properties();
        properties.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> stream1 = builder.stream("one", stringConsumed);
        final KStream<String, String> stream2 = builder.stream("two", stringConsumed);

        final KGroupedStream<String, String> groupedOne = stream1.map((k, v) -> new KeyValue<>(v, k)).groupByKey();
        final KGroupedStream<String, String> groupedTwo = stream2.groupByKey();

        final KTable<String, String> cogroupedTwo = groupedOne
                .cogroup(STRING_AGGREGATOR)
                .cogroup(groupedTwo, STRING_AGGREGATOR)
                .aggregate(STRING_INITIALIZER);

        final KTable<String, String> cogroupedOne = groupedOne
                .cogroup(STRING_AGGREGATOR)
                .cogroup(groupedTwo, STRING_AGGREGATOR)
                .aggregate(STRING_INITIALIZER);

        cogroupedOne.toStream().to(OUTPUT);
        cogroupedTwo.toStream().to("OUTPUT2");

        final String topologyDescription = builder.build(properties).describe().toString();

        assertThat(
                topologyDescription,
                equalTo("Topologies:\n" +
                        "   Sub-topology: 0\n" +
                        "    Source: KSTREAM-SOURCE-0000000000 (topics: [one])\n" +
                        "      --> KSTREAM-MAP-0000000002\n" +
                        "    Processor: KSTREAM-MAP-0000000002 (stores: [])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-filter\n" +
                        "      <-- KSTREAM-SOURCE-0000000000\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-filter (stores: [])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-sink\n" +
                        "      <-- KSTREAM-MAP-0000000002\n" +
                        "    Sink: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-sink (topic: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition)\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-filter\n\n" +
                        "  Sub-topology: 1\n" +
                        "    Source: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-source (topics: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-0000000014, COGROUPKSTREAM-AGGREGATE-0000000007\n" +
                        "    Source: KSTREAM-SOURCE-0000000001 (topics: [two])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-0000000015, COGROUPKSTREAM-AGGREGATE-0000000008\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000007 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000009\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-source\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000008 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000009\n" +
                        "      <-- KSTREAM-SOURCE-0000000001\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000014 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000010])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000016\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-source\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000015 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000010])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000016\n" +
                        "      <-- KSTREAM-SOURCE-0000000001\n" +
                        "    Processor: COGROUPKSTREAM-MERGE-0000000009 (stores: [])\n" +
                        "      --> KTABLE-TOSTREAM-0000000019\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-0000000007, COGROUPKSTREAM-AGGREGATE-0000000008\n" +
                        "    Processor: COGROUPKSTREAM-MERGE-0000000016 (stores: [])\n" +
                        "      --> KTABLE-TOSTREAM-0000000017\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-0000000014, COGROUPKSTREAM-AGGREGATE-0000000015\n" +
                        "    Processor: KTABLE-TOSTREAM-0000000017 (stores: [])\n" +
                        "      --> KSTREAM-SINK-0000000018\n" +
                        "      <-- COGROUPKSTREAM-MERGE-0000000016\n" +
                        "    Processor: KTABLE-TOSTREAM-0000000019 (stores: [])\n" +
                        "      --> KSTREAM-SINK-0000000020\n" +
                        "      <-- COGROUPKSTREAM-MERGE-0000000009\n" +
                        "    Sink: KSTREAM-SINK-0000000018 (topic: output)\n" +
                        "      <-- KTABLE-TOSTREAM-0000000017\n" +
                        "    Sink: KSTREAM-SINK-0000000020 (topic: OUTPUT2)\n" +
                        "      <-- KTABLE-TOSTREAM-0000000019\n\n"));
    }

    @Test
    public void shouldInsertRepartitionsTopicForUpstreamKeyModificationWithGroupedReusedInDifferentCogroups() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> stream1 = builder.stream("one", stringConsumed);
        final KStream<String, String> stream2 = builder.stream("two", stringConsumed);
        final KStream<String, String> stream3 = builder.stream("three", stringConsumed);

        final KGroupedStream<String, String> groupedOne = stream1.map((k, v) -> new KeyValue<>(v, k)).groupByKey();
        final KGroupedStream<String, String> groupedTwo = stream2.groupByKey();
        final KGroupedStream<String, String> groupedThree = stream3.groupByKey();

        groupedOne.cogroup(STRING_AGGREGATOR)
                .cogroup(groupedThree, STRING_AGGREGATOR)
                .aggregate(STRING_INITIALIZER);

        groupedOne.cogroup(STRING_AGGREGATOR)
                .cogroup(groupedTwo, STRING_AGGREGATOR)
                .aggregate(STRING_INITIALIZER);

        final String topologyDescription = builder.build().describe().toString();

        assertThat(
                topologyDescription,
                equalTo("Topologies:\n" +
                        "   Sub-topology: 0\n" +
                        "    Source: KSTREAM-SOURCE-0000000000 (topics: [one])\n" +
                        "      --> KSTREAM-MAP-0000000003\n" +
                        "    Processor: KSTREAM-MAP-0000000003 (stores: [])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000004-repartition-filter, COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000011-repartition-filter\n" +
                        "      <-- KSTREAM-SOURCE-0000000000\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000004-repartition-filter (stores: [])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000004-repartition-sink\n" +
                        "      <-- KSTREAM-MAP-0000000003\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000011-repartition-filter (stores: [])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000011-repartition-sink\n" +
                        "      <-- KSTREAM-MAP-0000000003\n" +
                        "    Sink: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000004-repartition-sink (topic: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000004-repartition)\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000004-repartition-filter\n" +
                        "    Sink: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000011-repartition-sink (topic: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000011-repartition)\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000011-repartition-filter\n\n" +
                        "  Sub-topology: 1\n" +
                        "    Source: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000011-repartition-source (topics: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000011-repartition])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-0000000015\n" +
                        "    Source: KSTREAM-SOURCE-0000000001 (topics: [two])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-0000000016\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000015 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000011])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000017\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000011-repartition-source\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000016 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000011])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000017\n" +
                        "      <-- KSTREAM-SOURCE-0000000001\n" +
                        "    Processor: COGROUPKSTREAM-MERGE-0000000017 (stores: [])\n" +
                        "      --> none\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-0000000015, COGROUPKSTREAM-AGGREGATE-0000000016\n\n" +
                        "  Sub-topology: 2\n" +
                        "    Source: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000004-repartition-source (topics: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000004-repartition])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-0000000008\n" +
                        "    Source: KSTREAM-SOURCE-0000000002 (topics: [three])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-0000000009\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000008 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000004])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000010\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000004-repartition-source\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000009 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000004])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000010\n" +
                        "      <-- KSTREAM-SOURCE-0000000002\n" +
                        "    Processor: COGROUPKSTREAM-MERGE-0000000010 (stores: [])\n" +
                        "      --> none\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-0000000008, COGROUPKSTREAM-AGGREGATE-0000000009\n\n"));
    }

    @Test
    public void shouldInsertRepartitionsTopicForUpstreamKeyModificationWithGroupedReusedInDifferentCogroupsWithOptimization() {
        final StreamsBuilder builder = new StreamsBuilder();

        final Properties properties = new Properties();
        properties.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        final KStream<String, String> stream1 = builder.stream("one", stringConsumed);
        final KStream<String, String> stream2 = builder.stream("two", stringConsumed);
        final KStream<String, String> stream3 = builder.stream("three", stringConsumed);

        final KGroupedStream<String, String> groupedOne = stream1.map((k, v) -> new KeyValue<>(v, k)).groupByKey();
        final KGroupedStream<String, String> groupedTwo = stream2.groupByKey();
        final KGroupedStream<String, String> groupedThree = stream3.groupByKey();

        groupedOne.cogroup(STRING_AGGREGATOR)
                .cogroup(groupedThree, STRING_AGGREGATOR)
                .aggregate(STRING_INITIALIZER);

        groupedOne.cogroup(STRING_AGGREGATOR)
                .cogroup(groupedTwo, STRING_AGGREGATOR)
                .aggregate(STRING_INITIALIZER);

        final String topologyDescription = builder.build(properties).describe().toString();

        assertThat(
                topologyDescription,
                equalTo("Topologies:\n" +
                        "   Sub-topology: 0\n" +
                        "    Source: KSTREAM-SOURCE-0000000000 (topics: [one])\n" +
                        "      --> KSTREAM-MAP-0000000003\n" +
                        "    Processor: KSTREAM-MAP-0000000003 (stores: [])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000004-repartition-filter\n" +
                        "      <-- KSTREAM-SOURCE-0000000000\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000004-repartition-filter (stores: [])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000004-repartition-sink\n" +
                        "      <-- KSTREAM-MAP-0000000003\n" +
                        "    Sink: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000004-repartition-sink (topic: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000004-repartition)\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000004-repartition-filter\n\n" +
                        "  Sub-topology: 1\n" +
                        "    Source: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000004-repartition-source (topics: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000004-repartition])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-0000000008, COGROUPKSTREAM-AGGREGATE-0000000015\n" +
                        "    Source: KSTREAM-SOURCE-0000000001 (topics: [two])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-0000000016\n" +
                        "    Source: KSTREAM-SOURCE-0000000002 (topics: [three])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-0000000009\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000008 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000004])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000010\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000004-repartition-source\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000009 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000004])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000010\n" +
                        "      <-- KSTREAM-SOURCE-0000000002\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000015 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000011])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000017\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000004-repartition-source\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000016 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000011])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000017\n" +
                        "      <-- KSTREAM-SOURCE-0000000001\n" +
                        "    Processor: COGROUPKSTREAM-MERGE-0000000010 (stores: [])\n" +
                        "      --> none\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-0000000008, COGROUPKSTREAM-AGGREGATE-0000000009\n" +
                        "    Processor: COGROUPKSTREAM-MERGE-0000000017 (stores: [])\n" +
                        "      --> none\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-0000000015, COGROUPKSTREAM-AGGREGATE-0000000016\n\n"));
    }

    @Test
    public void shouldInsertRepartitionsTopicForUpstreamKeyModificationWithGroupedReused() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> stream1 = builder.stream("one", stringConsumed);
        final KStream<String, String> stream2 = builder.stream("two", stringConsumed);

        final KGroupedStream<String, String> groupedOne = stream1.map((k, v) -> new KeyValue<>(v, k)).groupByKey();
        final KGroupedStream<String, String> groupedTwo = stream2.groupByKey();

        groupedOne.cogroup(STRING_AGGREGATOR)
                .cogroup(groupedTwo, STRING_AGGREGATOR)
                .aggregate(STRING_INITIALIZER);

        groupedOne.aggregate(STRING_INITIALIZER, STRING_AGGREGATOR);

        final String topologyDescription = builder.build().describe().toString();

        assertThat(
                topologyDescription,
                equalTo("Topologies:\n" +
                        "   Sub-topology: 0\n" +
                        "    Source: KSTREAM-SOURCE-0000000000 (topics: [one])\n" +
                        "      --> KSTREAM-MAP-0000000002\n" +
                        "    Processor: KSTREAM-MAP-0000000002 (stores: [])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-filter, KSTREAM-FILTER-0000000013\n" +
                        "      <-- KSTREAM-SOURCE-0000000000\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-filter (stores: [])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-sink\n" +
                        "      <-- KSTREAM-MAP-0000000002\n" +
                        "    Processor: KSTREAM-FILTER-0000000013 (stores: [])\n" +
                        "      --> KSTREAM-SINK-0000000012\n" +
                        "      <-- KSTREAM-MAP-0000000002\n" +
                        "    Sink: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-sink (topic: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition)\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-filter\n" +
                        "    Sink: KSTREAM-SINK-0000000012 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000010-repartition)\n" +
                        "      <-- KSTREAM-FILTER-0000000013\n\n" +
                        "  Sub-topology: 1\n" +
                        "    Source: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-source (topics: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-0000000007\n" +
                        "    Source: KSTREAM-SOURCE-0000000001 (topics: [two])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-0000000008\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000007 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000009\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-source\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000008 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000009\n" +
                        "      <-- KSTREAM-SOURCE-0000000001\n" +
                        "    Processor: COGROUPKSTREAM-MERGE-0000000009 (stores: [])\n" +
                        "      --> none\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-0000000007, COGROUPKSTREAM-AGGREGATE-0000000008\n\n" +
                        "  Sub-topology: 2\n" +
                        "    Source: KSTREAM-SOURCE-0000000014 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000010-repartition])\n" +
                        "      --> KSTREAM-AGGREGATE-0000000011\n" +
                        "    Processor: KSTREAM-AGGREGATE-0000000011 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000010])\n" +
                        "      --> none\n" +
                        "      <-- KSTREAM-SOURCE-0000000014\n\n"));
    }

    @Test
    public void shouldInsertRepartitionsTopicForUpstreamKeyModificationWithGroupedReusedWithOptimization() {
        final StreamsBuilder builder = new StreamsBuilder();

        final Properties properties = new Properties();
        properties.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        final KStream<String, String> stream1 = builder.stream("one", stringConsumed);
        final KStream<String, String> stream2 = builder.stream("two", stringConsumed);

        final KGroupedStream<String, String> groupedOne = stream1.map((k, v) -> new KeyValue<>(v, k)).groupByKey();
        final KGroupedStream<String, String> groupedTwo = stream2.groupByKey();

        groupedOne.cogroup(STRING_AGGREGATOR)
                .cogroup(groupedTwo, STRING_AGGREGATOR)
                .aggregate(STRING_INITIALIZER);

        groupedOne.aggregate(STRING_INITIALIZER, STRING_AGGREGATOR);

        final String topologyDescription = builder.build(properties).describe().toString();

        assertThat(
                topologyDescription,
                equalTo("Topologies:\n" +
                        "   Sub-topology: 0\n" +
                        "    Source: KSTREAM-SOURCE-0000000000 (topics: [one])\n" +
                        "      --> KSTREAM-MAP-0000000002\n" +
                        "    Processor: KSTREAM-MAP-0000000002 (stores: [])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-filter\n" +
                        "      <-- KSTREAM-SOURCE-0000000000\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-filter (stores: [])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-sink\n" +
                        "      <-- KSTREAM-MAP-0000000002\n" +
                        "    Sink: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-sink (topic: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition)\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-filter\n\n" +
                        "  Sub-topology: 1\n" +
                        "    Source: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-source (topics: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-0000000007, KSTREAM-AGGREGATE-0000000011\n" +
                        "    Source: KSTREAM-SOURCE-0000000001 (topics: [two])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-0000000008\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000007 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000009\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-source\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000008 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000009\n" +
                        "      <-- KSTREAM-SOURCE-0000000001\n" +
                        "    Processor: COGROUPKSTREAM-MERGE-0000000009 (stores: [])\n" +
                        "      --> none\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-0000000007, COGROUPKSTREAM-AGGREGATE-0000000008\n" +
                        "    Processor: KSTREAM-AGGREGATE-0000000011 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000010])\n" +
                        "      --> none\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition-source\n\n"));
    }

    @Test
    public void shouldInsertRepartitionsTopicForUpstreamKeyModificationWithGroupedRemadeWithOptimization() {
        final StreamsBuilder builder = new StreamsBuilder();

        final Properties properties = new Properties();
        properties.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        final KStream<String, String> stream1 = builder.stream("one", stringConsumed);
        final KStream<String, String> stream2 = builder.stream("two", stringConsumed);
        final KStream<String, String> stream3 = builder.stream("three", stringConsumed);

        final KGroupedStream<String, String> groupedOne = stream1.map((k, v) -> new KeyValue<>(v, k)).groupByKey();
        final KGroupedStream<String, String> groupedTwo = stream2.groupByKey();
        final KGroupedStream<String, String> groupedThree = stream3.groupByKey();
        final KGroupedStream<String, String> groupedFour = stream1.map((k, v) -> new KeyValue<>(v, k)).groupByKey();


        groupedOne.cogroup(STRING_AGGREGATOR)
                .cogroup(groupedTwo, STRING_AGGREGATOR)
                .aggregate(STRING_INITIALIZER);

        groupedThree.cogroup(STRING_AGGREGATOR)
                .cogroup(groupedFour, STRING_AGGREGATOR)
                .aggregate(STRING_INITIALIZER);


        final String topologyDescription = builder.build(properties).describe().toString();

        assertThat(
                topologyDescription,
                equalTo("Topologies:\n" +
                        "   Sub-topology: 0\n" +
                        "    Source: KSTREAM-SOURCE-0000000000 (topics: [one])\n" +
                        "      --> KSTREAM-MAP-0000000003, KSTREAM-MAP-0000000004\n" +
                        "    Processor: KSTREAM-MAP-0000000003 (stores: [])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition-filter\n" +
                        "      <-- KSTREAM-SOURCE-0000000000\n" +
                        "    Processor: KSTREAM-MAP-0000000004 (stores: [])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000012-repartition-filter\n" +
                        "      <-- KSTREAM-SOURCE-0000000000\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition-filter (stores: [])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition-sink\n" +
                        "      <-- KSTREAM-MAP-0000000003\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000012-repartition-filter (stores: [])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000012-repartition-sink\n" +
                        "      <-- KSTREAM-MAP-0000000004\n" +
                        "    Sink: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition-sink (topic: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition)\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition-filter\n" +
                        "    Sink: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000012-repartition-sink (topic: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000012-repartition)\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000012-repartition-filter\n\n" +
                        "  Sub-topology: 1\n" +
                        "    Source: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition-source (topics: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-0000000009\n" +
                        "    Source: KSTREAM-SOURCE-0000000001 (topics: [two])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-0000000010\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000009 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000005])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000011\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition-source\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000010 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000005])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000011\n" +
                        "      <-- KSTREAM-SOURCE-0000000001\n" +
                        "    Processor: COGROUPKSTREAM-MERGE-0000000011 (stores: [])\n" +
                        "      --> none\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-0000000009, COGROUPKSTREAM-AGGREGATE-0000000010\n\n" +
                        "  Sub-topology: 2\n" +
                        "    Source: COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000012-repartition-source (topics: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000012-repartition])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-0000000017\n" +
                        "    Source: KSTREAM-SOURCE-0000000002 (topics: [three])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-0000000016\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000016 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000012])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000018\n" +
                        "      <-- KSTREAM-SOURCE-0000000002\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000017 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000012])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000018\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000012-repartition-source\n" +
                        "    Processor: COGROUPKSTREAM-MERGE-0000000018 (stores: [])\n" +
                        "      --> none\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-0000000016, COGROUPKSTREAM-AGGREGATE-0000000017\n\n"));
    }

    @Test
    public void shouldInsertRepartitionsTopicForCogroupsUsedTwice() {
        final StreamsBuilder builder = new StreamsBuilder();

        final Properties properties = new Properties();

        final KStream<String, String> stream1 = builder.stream("one", stringConsumed);

        final KGroupedStream<String, String> groupedOne = stream1.map((k, v) -> new KeyValue<>(v, k)).groupByKey(Grouped.as("foo"));

        final CogroupedKStream<String, String> one = groupedOne.cogroup(STRING_AGGREGATOR);
        one.aggregate(STRING_INITIALIZER);
        one.aggregate(STRING_INITIALIZER);

        final String topologyDescription = builder.build(properties).describe().toString();

        assertThat(
                topologyDescription,
                equalTo("Topologies:\n" +
                        "   Sub-topology: 0\n" +
                        "    Source: KSTREAM-SOURCE-0000000000 (topics: [one])\n" +
                        "      --> KSTREAM-MAP-0000000001\n" +
                        "    Processor: KSTREAM-MAP-0000000001 (stores: [])\n" +
                        "      --> foo-repartition-filter\n" +
                        "      <-- KSTREAM-SOURCE-0000000000\n" +
                        "    Processor: foo-repartition-filter (stores: [])\n" +
                        "      --> foo-repartition-sink\n" +
                        "      <-- KSTREAM-MAP-0000000001\n" +
                        "    Sink: foo-repartition-sink (topic: foo-repartition)\n" +
                        "      <-- foo-repartition-filter\n\n" +
                        "  Sub-topology: 1\n" +
                        "    Source: foo-repartition-source (topics: [foo-repartition])\n" +
                        "      --> COGROUPKSTREAM-AGGREGATE-0000000006, COGROUPKSTREAM-AGGREGATE-0000000012\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000006 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000002])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000007\n" +
                        "      <-- foo-repartition-source\n" +
                        "    Processor: COGROUPKSTREAM-AGGREGATE-0000000012 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000008])\n" +
                        "      --> COGROUPKSTREAM-MERGE-0000000013\n" +
                        "      <-- foo-repartition-source\n" +
                        "    Processor: COGROUPKSTREAM-MERGE-0000000007 (stores: [])\n" +
                        "      --> none\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-0000000006\n" +
                        "    Processor: COGROUPKSTREAM-MERGE-0000000013 (stores: [])\n" +
                        "      --> none\n" +
                        "      <-- COGROUPKSTREAM-AGGREGATE-0000000012\n\n"));
    }

    @Test
    public void shouldCogroupAndAggregateSingleKStreams() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream1 = builder.stream("one", stringConsumed);

        final KGroupedStream<String, String> grouped1 = stream1.groupByKey();

        final KTable<String, String> customers = grouped1
            .cogroup(STRING_AGGREGATOR)
            .aggregate(STRING_INITIALIZER);

        customers.toStream().to(OUTPUT);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic =
                driver.createInputTopic("one", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> testOutputTopic =
                driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k2", "B", 0);
            testInputTopic.pipeInput("k2", "B", 0);
            testInputTopic.pipeInput("k1", "A", 0);

            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "B", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "BB", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "AA", 0);
        }
    }

    @Test
    public void testCogroupHandleNullValues() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream1 = builder.stream("one", stringConsumed);

        final KGroupedStream<String, String> grouped1 = stream1.groupByKey();

        final KTable<String, String> customers = grouped1
            .cogroup(STRING_AGGREGATOR)
            .aggregate(STRING_INITIALIZER);

        customers.toStream().to(OUTPUT);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic("one", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> testOutputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k2", "B", 0);
            testInputTopic.pipeInput("k2", null, 0);
            testInputTopic.pipeInput("k2", "B", 0);
            testInputTopic.pipeInput("k1", "A", 0);

            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "B", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "BB", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "AA", 0);
        }
    }

    @Test
    public void shouldCogroupAndAggregateTwoKStreamsWithDistinctKeys() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream1 = builder.stream("one", stringConsumed);
        final KStream<String, String> stream2 = builder.stream("two", stringConsumed);

        final KGroupedStream<String, String> grouped1 = stream1.groupByKey();
        final KGroupedStream<String, String> grouped2 = stream2.groupByKey();

        final KTable<String, String> customers = grouped1
            .cogroup(STRING_AGGREGATOR)
            .cogroup(grouped2, STRING_AGGREGATOR)
            .aggregate(STRING_INITIALIZER);

        customers.toStream().to(OUTPUT);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic =
                driver.createInputTopic("one", new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> testInputTopic2 =
                driver.createInputTopic("two", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> testOutputTopic =
                driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());

            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k1", "A", 1);
            testInputTopic.pipeInput("k1", "A", 10);
            testInputTopic.pipeInput("k1", "A", 100);
            testInputTopic2.pipeInput("k2", "B", 100L);
            testInputTopic2.pipeInput("k2", "B", 200L);
            testInputTopic2.pipeInput("k2", "B", 1L);
            testInputTopic2.pipeInput("k2", "B", 500L);
            testInputTopic2.pipeInput("k2", "B", 500L);
            testInputTopic2.pipeInput("k2", "B", 100L);

            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "AA", 1);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "AAA", 10);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "AAAA", 100);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "B", 100);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "BB", 200);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "BBB", 200);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "BBBB", 500);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "BBBBB", 500);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "BBBBBB", 500);
        }
    }

    @Test
    public void shouldCogroupAndAggregateTwoKStreamsWithSharedKeys() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream1 = builder.stream("one", stringConsumed);
        final KStream<String, String> stream2 = builder.stream("two", stringConsumed);

        final KGroupedStream<String, String> grouped1 = stream1.groupByKey();
        final KGroupedStream<String, String> grouped2 = stream2.groupByKey();

        final KTable<String, String> customers = grouped1
            .cogroup(STRING_AGGREGATOR)
            .cogroup(grouped2, STRING_AGGREGATOR)
            .aggregate(STRING_INITIALIZER);

        customers.toStream().to(OUTPUT);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic =
                driver.createInputTopic("one", new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> testInputTopic2 =
                driver.createInputTopic("two", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> testOutputTopic =
                driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());

            testInputTopic.pipeInput("k1", "A", 0L);
            testInputTopic.pipeInput("k2", "A", 1L);
            testInputTopic.pipeInput("k1", "A", 10L);
            testInputTopic.pipeInput("k2", "A", 100L);
            testInputTopic2.pipeInput("k2", "B", 100L);
            testInputTopic2.pipeInput("k2", "B", 200L);
            testInputTopic2.pipeInput("k1", "B", 1L);
            testInputTopic2.pipeInput("k2", "B", 500L);
            testInputTopic2.pipeInput("k1", "B", 500L);
            testInputTopic2.pipeInput("k2", "B", 500L);
            testInputTopic2.pipeInput("k3", "B", 500L);
            testInputTopic2.pipeInput("k2", "B", 100L);

            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "A", 1);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "AA", 10);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "AA", 100);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "AAB", 100);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "AABB", 200);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "AAB", 10);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "AABBB", 500);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "AABB", 500);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "AABBBB", 500);
        }
    }

    @Test
    public void shouldAllowDifferentOutputTypeInCoGroup() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream1 = builder.stream("one", stringConsumed);
        final KStream<String, String> stream2 = builder.stream("two", stringConsumed);

        final KGroupedStream<String, String> grouped1 = stream1.groupByKey();
        final KGroupedStream<String, String> grouped2 = stream2.groupByKey();

        final KTable<String, Integer> customers = grouped1
            .cogroup(STRING_SUM_AGGREGATOR)
            .cogroup(grouped2, STRING_SUM_AGGREGATOR)
            .aggregate(
                SUM_INITIALIZER,
                Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("store1")
                    .withValueSerde(Serdes.Integer()));

        customers.toStream().to(OUTPUT);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic =
                driver.createInputTopic("one", new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> testInputTopic2 =
                driver.createInputTopic("two", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, Integer> testOutputTopic =
                driver.createOutputTopic(OUTPUT, new StringDeserializer(), new IntegerDeserializer());

            testInputTopic.pipeInput("k1", "1", 0L);
            testInputTopic.pipeInput("k2", "1", 1L);
            testInputTopic.pipeInput("k1", "1", 10L);
            testInputTopic.pipeInput("k2", "1", 100L);
            testInputTopic2.pipeInput("k2", "2", 100L);
            testInputTopic2.pipeInput("k2", "2", 200L);
            testInputTopic2.pipeInput("k1", "2", 1L);
            testInputTopic2.pipeInput("k2", "2", 500L);
            testInputTopic2.pipeInput("k1", "2", 500L);
            testInputTopic2.pipeInput("k2", "3", 500L);
            testInputTopic2.pipeInput("k3", "2", 500L);
            testInputTopic2.pipeInput("k2", "2", 100L);

            assertOutputKeyValueTimestamp(testOutputTopic, "k1", 1, 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", 1, 1);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", 2, 10);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", 2, 100);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", 4, 100);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", 6, 200);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", 4, 10);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", 8, 500);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", 6, 500);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", 11, 500);
        }
    }

    @Test
    public void shouldCoGroupStreamsWithDifferentInputTypes() {
        final StreamsBuilder builder = new StreamsBuilder();
        final Consumed<String, Integer> integerConsumed = Consumed.with(Serdes.String(), Serdes.Integer());
        final KStream<String, String> stream1 = builder.stream("one", stringConsumed);
        final KStream<String, Integer> stream2 = builder.stream("two", integerConsumed);

        final KGroupedStream<String, String> grouped1 = stream1.groupByKey();
        final KGroupedStream<String, Integer> grouped2 = stream2.groupByKey();

        final KTable<String, Integer> customers = grouped1
            .cogroup(STRING_SUM_AGGREGATOR)
            .cogroup(grouped2, SUM_AGGREGATOR)
            .aggregate(
                SUM_INITIALIZER,
                Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("store1")
                    .withValueSerde(Serdes.Integer()));

        customers.toStream().to(OUTPUT);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic("one", new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, Integer> testInputTopic2 = driver.createInputTopic("two", new StringSerializer(), new IntegerSerializer());
            final TestOutputTopic<String, Integer> testOutputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new IntegerDeserializer());
            testInputTopic.pipeInput("k1", "1", 0L);
            testInputTopic.pipeInput("k2", "1", 1L);
            testInputTopic.pipeInput("k1", "1", 10L);
            testInputTopic.pipeInput("k2", "1", 100L);

            testInputTopic2.pipeInput("k2", 2, 100L);
            testInputTopic2.pipeInput("k2", 2, 200L);
            testInputTopic2.pipeInput("k1", 2, 1L);
            testInputTopic2.pipeInput("k2", 2, 500L);
            testInputTopic2.pipeInput("k1", 2, 500L);
            testInputTopic2.pipeInput("k2", 3, 500L);
            testInputTopic2.pipeInput("k3", 2, 500L);
            testInputTopic2.pipeInput("k2", 2, 100L);

            assertOutputKeyValueTimestamp(testOutputTopic, "k1", 1, 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", 1, 1);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", 2, 10);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", 2, 100);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", 4, 100);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", 6, 200);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", 4, 10);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", 8, 500);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", 6, 500);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", 11, 500);
            assertOutputKeyValueTimestamp(testOutputTopic, "k3", 2, 500);
        }
    }

    @Test
    public void testCogroupKeyMixedAggregators() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream1 = builder.stream("one", stringConsumed);
        final KStream<String, String> stream2 = builder.stream("two", stringConsumed);

        final KGroupedStream<String, String> grouped1 = stream1.groupByKey();
        final KGroupedStream<String, String> grouped2 = stream2.groupByKey();

        final KTable<String, String> customers = grouped1
            .cogroup(MockAggregator.TOSTRING_REMOVER)
            .cogroup(grouped2, MockAggregator.TOSTRING_ADDER)
            .aggregate(
                MockInitializer.STRING_INIT,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("store1")
                    .withValueSerde(Serdes.String()));

        customers.toStream().to(OUTPUT);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic =
                driver.createInputTopic("one", new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> testInputTopic2 =
                driver.createInputTopic("two", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> testOutputTopic =
                driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());

            testInputTopic.pipeInput("k1", "1", 0L);
            testInputTopic.pipeInput("k2", "1", 1L);
            testInputTopic.pipeInput("k1", "1", 10L);
            testInputTopic.pipeInput("k2", "1", 100L);
            testInputTopic2.pipeInput("k1", "2", 500L);
            testInputTopic2.pipeInput("k2", "2", 500L);
            testInputTopic2.pipeInput("k1", "2", 500L);
            testInputTopic2.pipeInput("k2", "2", 100L);

            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0-1", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0-1", 1);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0-1-1", 10);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0-1-1", 100);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0-1-1+2", 500L);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0-1-1+2", 500L);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0-1-1+2+2", 500L);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0-1-1+2+2", 500L);
        }
    }

    @Test
    public void testCogroupWithThreeGroupedStreams() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream1 = builder.stream("one", stringConsumed);
        final KStream<String, String> stream2 = builder.stream("two", stringConsumed);
        final KStream<String, String> stream3 = builder.stream("three", stringConsumed);

        final KGroupedStream<String, String> grouped1 = stream1.groupByKey();
        final KGroupedStream<String, String> grouped2 = stream2.groupByKey();
        final KGroupedStream<String, String> grouped3 = stream3.groupByKey();

        final KTable<String, String> customers = grouped1
            .cogroup(STRING_AGGREGATOR)
            .cogroup(grouped2, STRING_AGGREGATOR)
            .cogroup(grouped3, STRING_AGGREGATOR)
            .aggregate(STRING_INITIALIZER);

        customers.toStream().to(OUTPUT);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic =
                driver.createInputTopic("one", new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> testInputTopic2 =
                driver.createInputTopic("two", new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> testInputTopic3 =
                driver.createInputTopic("three", new StringSerializer(), new StringSerializer());

            final TestOutputTopic<String, String> testOutputTopic =
                driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());

            testInputTopic.pipeInput("k1", "A", 0L);
            testInputTopic.pipeInput("k2", "A", 1L);
            testInputTopic.pipeInput("k1", "A", 10L);
            testInputTopic.pipeInput("k2", "A", 100L);
            testInputTopic2.pipeInput("k2", "B", 100L);
            testInputTopic2.pipeInput("k2", "B", 200L);
            testInputTopic2.pipeInput("k1", "B", 1L);
            testInputTopic2.pipeInput("k2", "B", 500L);
            testInputTopic3.pipeInput("k1", "B", 500L);
            testInputTopic3.pipeInput("k2", "B", 500L);
            testInputTopic3.pipeInput("k3", "B", 500L);
            testInputTopic3.pipeInput("k2", "B", 100L);

            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "A", 1);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "AA", 10);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "AA", 100);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "AAB", 100);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "AABB", 200);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "AAB", 10);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "AABBB", 500);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "AABB", 500);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "AABBBB", 500);
            assertOutputKeyValueTimestamp(testOutputTopic, "k3", "B", 500);
        }
    }

    @Test
    public void testCogroupWithKTableKTableInnerJoin() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KGroupedStream<String, String> grouped1 = builder.stream("one", stringConsumed).groupByKey();
        final KGroupedStream<String, String> grouped2 = builder.stream("two", stringConsumed).groupByKey();

        final KTable<String, String> table1 = grouped1
            .cogroup(STRING_AGGREGATOR)
            .cogroup(grouped2, STRING_AGGREGATOR)
            .aggregate(STRING_INITIALIZER, Named.as("name"), Materialized.as("store"));

        final KTable<String, String> table2 = builder.table("three", stringConsumed);
        final KTable<String, String> joined = table1.join(table2, MockValueJoiner.TOSTRING_JOINER, Materialized.with(Serdes.String(), Serdes.String()));
        joined.toStream().to(OUTPUT);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic =
                driver.createInputTopic("one", new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> testInputTopic2 =
                driver.createInputTopic("two", new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> testInputTopic3 =
                driver.createInputTopic("three", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> testOutputTopic =
                driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());

            testInputTopic.pipeInput("k1", "A", 5L);
            testInputTopic2.pipeInput("k2", "B", 6L);

            assertTrue(testOutputTopic.isEmpty());

            testInputTopic3.pipeInput("k1", "C", 0L);
            testInputTopic3.pipeInput("k2", "D", 10L);

            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "A+C", 5L);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "B+D", 10L);
            assertTrue(testOutputTopic.isEmpty());
        }
    }

    private void assertOutputKeyValueTimestamp(final TestOutputTopic<String, String> outputTopic,
                                               final String expectedKey,
                                               final String expectedValue,
                                               final long expectedTimestamp) {
        assertThat(
            outputTopic.readRecord(),
            equalTo(new TestRecord<>(expectedKey, expectedValue, null, expectedTimestamp)));
    }

    private void assertOutputKeyValueTimestamp(final TestOutputTopic<String, Integer> outputTopic,
                                               final String expectedKey,
                                               final Integer expectedValue,
                                               final long expectedTimestamp) {
        assertThat(
            outputTopic.readRecord(),
            equalTo(new TestRecord<>(expectedKey, expectedValue, null, expectedTimestamp)));
    }
}