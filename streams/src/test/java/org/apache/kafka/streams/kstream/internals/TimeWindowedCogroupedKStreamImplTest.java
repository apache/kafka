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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.CogroupedKStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TimeWindowedCogroupedKStream;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.StreamsTestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static java.time.Duration.ofMillis;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TimeWindowedCogroupedKStreamImplTest {

    private static final Long WINDOW_SIZE = 500L;
    private static final String TOPIC = "topic";
    private static final String TOPIC2 = "topic2";
    private static final String OUTPUT = "output";
    private final StreamsBuilder builder = new StreamsBuilder();

    private KGroupedStream<String, String> groupedStream;

    private KGroupedStream<String, String> groupedStream2;
    private CogroupedKStream<String, String> cogroupedStream;
    private TimeWindowedCogroupedKStream<String, String> windowedCogroupedStream;

    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    @BeforeEach
    public void setup() {
        final KStream<String, String> stream = builder.stream(TOPIC, Consumed
            .with(Serdes.String(), Serdes.String()));
        final KStream<String, String> stream2 = builder.stream(TOPIC2, Consumed
            .with(Serdes.String(), Serdes.String()));

        groupedStream = stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
        groupedStream2 = stream2.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
        cogroupedStream = groupedStream.cogroup(MockAggregator.TOSTRING_ADDER)
                .cogroup(groupedStream2, MockAggregator.TOSTRING_REMOVER);
        windowedCogroupedStream = cogroupedStream.windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(WINDOW_SIZE)));
    }

    @Test
    public void shouldNotHaveNullInitializerOnAggregate() {
        assertThrows(NullPointerException.class, () -> windowedCogroupedStream.aggregate(null));
    }

    @Test
    public void shouldNotHaveNullMaterializedOnTwoOptionAggregate() {
        assertThrows(NullPointerException.class, () -> windowedCogroupedStream.aggregate(MockInitializer.STRING_INIT,
            (Materialized<String, String, WindowStore<Bytes, byte[]>>) null));
    }

    @Test
    public void shouldNotHaveNullNamedTwoOptionOnAggregate() {
        assertThrows(NullPointerException.class, () -> windowedCogroupedStream.aggregate(MockInitializer.STRING_INIT, (Named) null));
    }

    @Test
    public void shouldNotHaveNullInitializerTwoOptionNamedOnAggregate() {
        assertThrows(NullPointerException.class, () -> windowedCogroupedStream.aggregate(null, Named.as("test")));
    }

    @Test
    public void shouldNotHaveNullInitializerTwoOptionMaterializedOnAggregate() {
        assertThrows(NullPointerException.class, () -> windowedCogroupedStream.aggregate(null, Materialized.as("test")));
    }

    @Test
    public void shouldNotHaveNullInitializerThreeOptionOnAggregate() {
        assertThrows(NullPointerException.class, () -> windowedCogroupedStream.aggregate(null, Named.as("test"), Materialized.as("test")));
    }

    @Test
    public void shouldNotHaveNullMaterializedOnAggregate() {
        assertThrows(NullPointerException.class, () -> windowedCogroupedStream.aggregate(MockInitializer.STRING_INIT, Named.as("Test"), null));
    }

    @Test
    public void shouldNotHaveNullNamedOnAggregate() {
        assertThrows(NullPointerException.class, () -> windowedCogroupedStream.aggregate(MockInitializer.STRING_INIT, null, Materialized.as("test")));
    }

    @Test
    public void namedParamShouldSetName() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream = builder.stream(TOPIC, Consumed
                .with(Serdes.String(), Serdes.String()));
        groupedStream = stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
        groupedStream.cogroup(MockAggregator.TOSTRING_ADDER)
                .windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(WINDOW_SIZE)))
                .aggregate(MockInitializer.STRING_INIT, Named.as("foo"));

        assertThat(builder.build().describe().toString(), equalTo(
                "Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000000 (topics: [topic])\n" +
                "      --> foo-cogroup-agg-0\n" +
                "    Processor: foo-cogroup-agg-0 (stores: [COGROUPKSTREAM-AGGREGATE-STATE-STORE-0000000001])\n" +
                "      --> foo-cogroup-merge\n" +
                "      <-- KSTREAM-SOURCE-0000000000\n" +
                "    Processor: foo-cogroup-merge (stores: [])\n" +
                "      --> none\n" +
                "      <-- foo-cogroup-agg-0\n\n"));
    }

    @Test
    public void timeWindowAggregateTestStreamsTest() {

        final KTable<Windowed<String>, String> customers = windowedCogroupedStream.aggregate(
                MockInitializer.STRING_INIT, Materialized.with(Serdes.String(), Serdes.String()));
        customers.toStream().to(OUTPUT);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic(
                    TOPIC, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<Windowed<String>, String> testOutputTopic = driver.createOutputTopic(
                    OUTPUT, new TimeWindowedDeserializer<>(new StringDeserializer(), WINDOW_SIZE), new StringDeserializer());

            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k2", "A", 0);
            testInputTopic.pipeInput("k2", "A", 1);
            testInputTopic.pipeInput("k1", "A", 2);
            testInputTopic.pipeInput("k1", "B", 3);
            testInputTopic.pipeInput("k2", "B", 3);
            testInputTopic.pipeInput("k2", "B", 4);
            testInputTopic.pipeInput("k1", "B", 4);

            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A+A", 1);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A+A", 2);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A+A+B", 3);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A+A+B", 3);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A+A+B+B", 4);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A+A+B+B", 4);
        }

    }

    @Test
    public void timeWindowMixAggregatorsTest() {

        final KTable<Windowed<String>, String> customers = windowedCogroupedStream.aggregate(
                MockInitializer.STRING_INIT, Materialized.with(Serdes.String(), Serdes.String()));
        customers.toStream().to(OUTPUT);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic(
                    TOPIC, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> testInputTopic2 = driver.createInputTopic(
                    TOPIC2, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<Windowed<String>, String> testOutputTopic = driver.createOutputTopic(
                    OUTPUT, new TimeWindowedDeserializer<>(new StringDeserializer(), WINDOW_SIZE), new StringDeserializer());

            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k2", "A", 0);
            testInputTopic.pipeInput("k2", "A", 1);
            testInputTopic.pipeInput("k1", "A", 2);
            testInputTopic2.pipeInput("k1", "B", 3);
            testInputTopic2.pipeInput("k2", "B", 3);
            testInputTopic2.pipeInput("k2", "B", 4);
            testInputTopic2.pipeInput("k1", "B", 4);

            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A+A", 1);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A+A", 2);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A+A-B", 3);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A+A-B", 3);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A+A-B-B", 4);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A+A-B-B", 4);
        }

    }

    @Test
    public void timeWindowAggregateManyWindowsTest() {

        final KTable<Windowed<String>, String> customers = groupedStream.cogroup(MockAggregator.TOSTRING_ADDER)
                .windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(500L))).aggregate(
                        MockInitializer.STRING_INIT, Materialized.with(Serdes.String(), Serdes.String()));
        customers.toStream().to(OUTPUT);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic(
                    TOPIC, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<Windowed<String>, String> testOutputTopic = driver.createOutputTopic(
                    OUTPUT, new TimeWindowedDeserializer<>(new StringDeserializer(), WINDOW_SIZE), new StringDeserializer());

            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k2", "A", 499);
            testInputTopic.pipeInput("k2", "A", 500L);
            testInputTopic.pipeInput("k1", "A", 500L);

            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A", 499);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A", 500);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A", 500);
        }
    }

    @Test
    public void timeWindowAggregateOverlappingWindowsTest() {

        final KTable<Windowed<String>, String> customers = groupedStream.cogroup(MockAggregator.TOSTRING_ADDER)
                .windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(500L)).advanceBy(ofMillis(200L))).aggregate(
                        MockInitializer.STRING_INIT, Materialized.with(Serdes.String(), Serdes.String()));
        customers.toStream().to(OUTPUT);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic(
                    TOPIC, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<Windowed<String>, String> testOutputTopic = driver.createOutputTopic(
                    OUTPUT, new TimeWindowedDeserializer<>(new StringDeserializer(), WINDOW_SIZE), new StringDeserializer());

            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k2", "A", 0);
            testInputTopic.pipeInput("k1", "B", 250);
            testInputTopic.pipeInput("k2", "B", 250);
            testInputTopic.pipeInput("k2", "A", 500L);
            testInputTopic.pipeInput("k1", "A", 500L);

            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A+B", 250);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+B", 250);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A+B", 250);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+B", 250);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+B+A", 500);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A", 500);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+B+A", 500);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A", 500);
        }
    }

    @Test
    public void timeWindowMixAggregatorsManyWindowsTest() {

        final KTable<Windowed<String>, String> customers = windowedCogroupedStream.aggregate(
                MockInitializer.STRING_INIT, Materialized.with(Serdes.String(), Serdes.String()));
        customers.toStream().to(OUTPUT);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic(
                    TOPIC, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> testInputTopic2 = driver.createInputTopic(
                    TOPIC2, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<Windowed<String>, String> testOutputTopic = driver.createOutputTopic(
                    OUTPUT, new TimeWindowedDeserializer<>(new StringDeserializer(), WINDOW_SIZE), new StringDeserializer());

            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k2", "A", 0);
            testInputTopic.pipeInput("k2", "A", 1);
            testInputTopic.pipeInput("k1", "A", 2);
            testInputTopic2.pipeInput("k1", "B", 3);
            testInputTopic2.pipeInput("k2", "B", 3);
            testInputTopic2.pipeInput("k2", "B", 501);
            testInputTopic2.pipeInput("k1", "B", 501);

            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A+A", 1);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A+A", 2);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A+A-B", 3);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A+A-B", 3);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0-B", 501);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0-B", 501);
        }
    }

    private void assertOutputKeyValueTimestamp(final TestOutputTopic<Windowed<String>, String> outputTopic,
                                               final String expectedKey,
                                               final String expectedValue,
                                               final long expectedTimestamp) {
        final TestRecord<Windowed<String>, String> realRecord = outputTopic.readRecord();
        final TestRecord<String, String> nonWindowedRecord = new TestRecord<>(
                realRecord.getKey().key(), realRecord.getValue(), null, realRecord.timestamp());
        final TestRecord<String, String> testRecord = new TestRecord<>(expectedKey, expectedValue, null, expectedTimestamp);
        assertThat(nonWindowedRecord, equalTo(testRecord));
    }
}
