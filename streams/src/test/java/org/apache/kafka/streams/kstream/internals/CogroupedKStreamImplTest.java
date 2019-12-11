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

import java.util.Properties;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

public class CogroupedKStreamImplTest {
    private final Consumed<String, String> stringConsumed = Consumed.with(Serdes.String(), Serdes.String());
    private static final String TOPIC = "topic";
    private static final String OUTPUT = "output";
    private final StreamsBuilder builder = new StreamsBuilder();
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

    @Test(expected = NullPointerException.class)
    public void shouldThrowNPEInCogroupIfKGroupedStreamIsNull() {
        cogroupedStream.cogroup(null, MockAggregator.TOSTRING_ADDER);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullAggregatorOnCogroup() {
        cogroupedStream.cogroup(groupedStream, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullInitializerOnAggregate() {
        cogroupedStream.aggregate(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullInitializerOnAggregateWitNamed() {
        cogroupedStream.aggregate(null, Named.as("name"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullInitializerOnAggregateWitMaterialized() {
        cogroupedStream.aggregate(null, Materialized.as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullInitializerOnAggregateWitNamedAndMaterialized() {
        cogroupedStream.aggregate(null, Named.as("name"), Materialized.as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullNamedOnAggregate() {
        cogroupedStream.aggregate(STRING_INITIALIZER, (Named) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullMaterializedOnAggregate() {
        cogroupedStream.aggregate(STRING_INITIALIZER, (Materialized<String, String, KeyValueStore<Bytes, byte[]>>) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullNamedOnAggregateWithMateriazlied() {
        cogroupedStream.aggregate(STRING_INITIALIZER,  null,  Materialized.as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullMaterializedOnAggregateWithNames() {
        cogroupedStream.aggregate(STRING_INITIALIZER, Named.as("name"), null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullWindowOnWindowedBy() {
        cogroupedStream.windowedBy(null);
    }

    @Test
    public void shouldNameProcessorsAndStoreBasedOnNamedParameter() {
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
    public void shouldCogroupAndAggregateSingleKStreams() {
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