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
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.CogroupedKStream;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

public class CogroupedKStreamImplTest {

    private final Consumed<String, String> stringConsumed = Consumed
        .with(Serdes.String(), Serdes.String());
    private final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
    private static final String TOPIC = "topic";
    private final StreamsBuilder builder = new StreamsBuilder();
    private KGroupedStream<String, String> groupedStream;
    private CogroupedKStream<String, String> cogroupedStream;

    private final Properties props = StreamsTestUtils
        .getStreamsConfig(Serdes.String(), Serdes.String());

    private static final Aggregator<String, String, String> STRING_AGGREGATOR = (Aggregator<String, String, String>) (key, value, aggregate) ->
            aggregate + value;

    private static final Initializer<String> STRING_INITIALIZER = () -> "";

    private static final Aggregator<String, String, Integer> STRSUM_AGGREGATOR = (key, value, aggregate) ->
            aggregate + Integer.parseInt(value);

    private static final Aggregator<? super String, ? super Integer, Integer> SUM_AGGREGATOR = (key, value, aggregate) ->
            aggregate + value;

    private static final Initializer<Integer> SUM_INITIALIZER = () -> 0;


    @Before
    public void setup() {
        final KStream<String, String> stream = builder.stream(TOPIC, Consumed
            .with(Serdes.String(), Serdes.String()));

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
        cogroupedStream.aggregate(null, Materialized.as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullMaterMaterializedOnAggregate() {
        cogroupedStream.aggregate(STRING_INITIALIZER, null);
    }

    @SuppressWarnings("unchecked")
    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullWindowsOnWindowedAggregate() {
        cogroupedStream.windowedBy((Windows) null);
    }

    @Test
    public void shouldCoGroupAndAggregateSingleKStreams() {

        final KStream<String, String> test = builder.stream("one", stringConsumed);

        final KGroupedStream<String, String> groupedOne = test.groupByKey();

        final KTable<String, String> customers = groupedOne.cogroup(STRING_AGGREGATOR, Materialized.as("store"))
            .aggregate(STRING_INITIALIZER);

        customers.toStream().to("to-one", Produced.with(Serdes.String(), Serdes.String()));

        builder.stream("to-one", stringConsumed).process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic("one", new StringSerializer(), new StringSerializer());
            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k11", "A", 0);
            testInputTopic.pipeInput("k11", "A", 0);
            testInputTopic.pipeInput("k1", "A", 0);
        }

        assertThat(processorSupplier.theCapturedProcessor().processed, equalTo(Arrays.asList(
                new KeyValueTimestamp<>("k1", "A", 0),
                new KeyValueTimestamp<>("k11", "A", 0),
                new KeyValueTimestamp<>("k11", "AA", 0),
                new KeyValueTimestamp<>("k1", "AA", 0)
        )));
    }


    @Test
    public void shouldCogroupAndAggregateTwoKStreamsWithUniqueKeys() {

        final KStream<String, String> test = builder.stream("one", stringConsumed);
        final KStream<String, String> test2 = builder.stream("two", stringConsumed);

        final KGroupedStream<String, String> groupedOne = test.groupByKey();
        final KGroupedStream<String, String> groupedTwo = test2.groupByKey();

        final KTable<String, String> customers = groupedOne.cogroup(STRING_AGGREGATOR, Materialized.as("store"))
            .cogroup(groupedTwo, STRING_AGGREGATOR)
            .aggregate(STRING_INITIALIZER);

        customers.toStream().to("to-one", Produced.with(Serdes.String(), Serdes.String()));

        builder.stream("to-one", stringConsumed).process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {

            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic("one", new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> testInputTopic2 = driver.createInputTopic("two", new StringSerializer(), new StringSerializer());
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
        }

        assertThat(processorSupplier.theCapturedProcessor().processed, equalTo(Arrays.asList(
                new KeyValueTimestamp<>("k1", "A", 0),
                new KeyValueTimestamp<>("k1", "AA", 1),
                new KeyValueTimestamp<>("k1", "AAA", 10),
                new KeyValueTimestamp<>("k1", "AAAA", 100),
                new KeyValueTimestamp<>("k2", "B", 100),
                new KeyValueTimestamp<>("k2", "BB", 200),
                new KeyValueTimestamp<>("k2", "BBB", 200),
                new KeyValueTimestamp<>("k2", "BBBB", 500),
                new KeyValueTimestamp<>("k2", "BBBBB", 500),
                new KeyValueTimestamp<>("k2", "BBBBBB", 500)
        )));
    }


    @Test
    public void shouldCogroupAndAggregateTwoKStreamsWithoutUniqueKeys() {

        final KStream<String, String> test = builder.stream("one", stringConsumed);
        final KStream<String, String> test2 = builder.stream("two", stringConsumed);

        final KGroupedStream<String, String> groupedOne = test.groupByKey();
        final KGroupedStream<String, String> groupedTwo = test2.groupByKey();

        final KTable<String, String> customers = groupedOne.cogroup(STRING_AGGREGATOR, Materialized.as("store"))
            .cogroup(groupedTwo, STRING_AGGREGATOR)
            .aggregate(STRING_INITIALIZER);

        customers.toStream().to("to-one", Produced.with(Serdes.String(), Serdes.String()));

        builder.stream("to-one", stringConsumed).process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic("one", new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> testInputTopic2 = driver.createInputTopic("two", new StringSerializer(), new StringSerializer());
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
        }

        assertThat(processorSupplier.theCapturedProcessor().processed, equalTo(Arrays.asList(
                new KeyValueTimestamp<>("k1", "A", 0),
                new KeyValueTimestamp<>("k2", "A", 1),
                new KeyValueTimestamp<>("k1", "AA", 10),
                new KeyValueTimestamp<>("k2", "AA", 100),
                new KeyValueTimestamp<>("k2", "AAB", 100),
                new KeyValueTimestamp<>("k2", "AABB", 200),
                new KeyValueTimestamp<>("k1", "AAB", 10),
                new KeyValueTimestamp<>("k2", "AABBB", 500),
                new KeyValueTimestamp<>("k1", "AABB", 500),
                new KeyValueTimestamp<>("k2", "AABBBB", 500),
                new KeyValueTimestamp<>("k3", "B", 500),
                new KeyValueTimestamp<>("k2", "AABBBBB", 500)
        )));
    }

    @Test
    public void testCogroupKeyMixedInTopicsandChageTypes() {
        final Consumed<String, Integer> stringConsume = Consumed.with(Serdes.String(), Serdes.Integer());
        final MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();
        final KStream<String, String> test = builder.stream("one", stringConsumed);
        final KStream<String, String> test2 = builder.stream("two", stringConsumed);

        final KGroupedStream<String, String> groupedOne = test.groupByKey();
        final KGroupedStream<String, String> groupedTwo = test2.groupByKey();

        final KTable<String, Integer> customers = groupedOne.cogroup(STRSUM_AGGREGATOR)
            .cogroup(groupedTwo, STRSUM_AGGREGATOR)
            .aggregate(SUM_INITIALIZER, Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("store1").withValueSerde(Serdes.Integer()));

        customers.toStream().to("to-one", Produced.with(Serdes.String(), Serdes.Integer()));

        builder.stream("to-one", stringConsume).process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic("one", new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> testInputTopic2 = driver.createInputTopic("two", new StringSerializer(), new StringSerializer());
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
        }

        assertThat(supplier.theCapturedProcessor().processed, equalTo(Arrays.asList(
                new KeyValueTimestamp<>("k1", 1, 0),
                new KeyValueTimestamp<>("k2", 1, 1),
                new KeyValueTimestamp<>("k1", 2, 10),
                new KeyValueTimestamp<>("k2", 2, 100),
                new KeyValueTimestamp<>("k2", 4, 100),
                new KeyValueTimestamp<>("k2", 6, 200),
                new KeyValueTimestamp<>("k1", 4, 10),
                new KeyValueTimestamp<>("k2", 8, 500),
                new KeyValueTimestamp<>("k1", 6, 500),
                new KeyValueTimestamp<>("k2", 11, 500),
                new KeyValueTimestamp<>("k3", 2, 500),
                new KeyValueTimestamp<>("k2", 13, 500)
        )));
    }

    @Test
    public void testCogroupKeyMixedInTopicsTypesandChageTypes() {

        final Consumed<String, Integer> intergerConsumed = Consumed.with(Serdes.String(), Serdes.Integer());
        final MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();
        final KStream<String, String> test = builder.stream("one", stringConsumed);
        final KStream<String, Integer> test2 = builder.stream("two", intergerConsumed);

        final KGroupedStream<String, String> groupedOne = test.groupByKey();
        final KGroupedStream<String, Integer> groupedTwo = test2.groupByKey();

        final KTable<String, Integer> customers = groupedOne.cogroup(STRSUM_AGGREGATOR)
            .cogroup(groupedTwo, SUM_AGGREGATOR)
            .aggregate(SUM_INITIALIZER, Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("store1").withValueSerde(Serdes.Integer()));

        customers.toStream().to("to-one", Produced.with(Serdes.String(), Serdes.Integer()));

        builder.stream("to-one", intergerConsumed).process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic("one", new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, Integer> testInputTopic2 = driver.createInputTopic("two", new StringSerializer(), new IntegerSerializer());
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
        }

        assertThat(supplier.theCapturedProcessor().processed, equalTo(Arrays.asList(
            new KeyValueTimestamp<String, Integer>("k1", 1, 0),
            new KeyValueTimestamp<String, Integer>("k2", 1, 1),
            new KeyValueTimestamp<String, Integer>("k1", 2, 10),
            new KeyValueTimestamp<String, Integer>("k2", 2, 100),
            new KeyValueTimestamp<String, Integer>("k2", 4, 100),
            new KeyValueTimestamp<String, Integer>("k2", 6, 200),
            new KeyValueTimestamp<String, Integer>("k1", 4, 10),
            new KeyValueTimestamp<String, Integer>("k2", 8, 500),
            new KeyValueTimestamp<String, Integer>("k1", 6, 500),
            new KeyValueTimestamp<String, Integer>("k2", 11, 500),
            new KeyValueTimestamp<String, Integer>("k3", 2, 500),
            new KeyValueTimestamp<String, Integer>("k2", 13, 500)
        )));
    }

    @Test
    public void testCogroupKeyMixedAggregators() {

        final MockProcessorSupplier<String, String> supplier = new MockProcessorSupplier<>();
        final KStream<String, String> test = builder.stream("one", stringConsumed);
        final KStream<String, String> test2 = builder.stream("two", stringConsumed);

        final KGroupedStream<String, String> groupedOne = test.groupByKey();
        final KGroupedStream<String, String> groupedTwo = test2.groupByKey();

        final KTable<String, String> customers = groupedOne.cogroup(MockAggregator.TOSTRING_REMOVER)
            .cogroup(groupedTwo, MockAggregator.TOSTRING_ADDER)
            .aggregate(MockInitializer.STRING_INIT, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("store1").withValueSerde(Serdes.String()));

        customers.toStream().to("to-one", Produced.with(Serdes.String(), Serdes.String()));

        builder.stream("to-one", stringConsumed).process(supplier);
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic("one", new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> testInputTopic2 = driver.createInputTopic("two", new StringSerializer(), new StringSerializer());
            testInputTopic.pipeInput("k1", "1", 0L);
            testInputTopic.pipeInput("k2", "1", 1L);
            testInputTopic.pipeInput("k1", "1", 10L);
            testInputTopic.pipeInput("k2", "1", 100L);
            testInputTopic2.pipeInput("k1", "2", 500L);
            testInputTopic2.pipeInput("k2", "2", 500L);
            testInputTopic2.pipeInput("k1", "2", 500L);
            testInputTopic2.pipeInput("k2", "2", 100L);
        }

        assertThat(supplier.theCapturedProcessor().processed, equalTo(Arrays.asList(
            new KeyValueTimestamp<String, String>("k1", "0-1", 0),
            new KeyValueTimestamp<String, String>("k2", "0-1", 1),
            new KeyValueTimestamp<String, String>("k1", "0-1-1", 10),
            new KeyValueTimestamp<String, String>("k2", "0-1-1", 100),
            new KeyValueTimestamp<String, String>("k1", "0-1-1+2", 500L),
            new KeyValueTimestamp<String, String>("k2", "0-1-1+2", 500L),
            new KeyValueTimestamp<String, String>("k1", "0-1-1+2+2", 500L),
            new KeyValueTimestamp<String, String>("k2", "0-1-1+2+2", 500L)
        )));
    }
}
