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

import static java.time.Duration.ofMillis;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SessionWindowedCogroupedKStream;
import org.apache.kafka.streams.kstream.SessionWindowedDeserializer;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;


public class SessionWindowedCogroupedKStreamImplTest {

    private final StreamsBuilder builder = new StreamsBuilder();
    private static final String TOPIC = "topic";
    private static final String TOPIC2 = "topic2";
    private static final String OUTPUT = "output";

    private final Merger<String, String> sessionMerger = (aggKey, aggOne, aggTwo) -> aggOne + "+" + aggTwo;

    private KGroupedStream<String, String> groupedStream;
    private KGroupedStream<String, String> groupedStream2;
    private CogroupedKStream<String, String> cogroupedStream;
    private SessionWindowedCogroupedKStream<String, String> windowedCogroupedStream;

    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    @Before
    public void setup() {
        final KStream<String, String> stream = builder.stream(TOPIC, Consumed
            .with(Serdes.String(), Serdes.String()));
        final KStream<String, String> stream2 = builder.stream(TOPIC2, Consumed
            .with(Serdes.String(), Serdes.String()));

        groupedStream = stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
        groupedStream2 = stream2.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
        cogroupedStream = groupedStream.cogroup(MockAggregator.TOSTRING_ADDER)
                .cogroup(groupedStream2, MockAggregator.TOSTRING_REMOVER);
        windowedCogroupedStream = cogroupedStream.windowedBy(SessionWindows.with(ofMillis(100)));
    }

    @Test
    public void shouldNotHaveNullInitializerOnAggregate() {
        assertThrows(NullPointerException.class, () -> windowedCogroupedStream.aggregate(null, sessionMerger));
    }

    @Test
    public void shouldNotHaveNullSessionMergerOnAggregate() {
        assertThrows(NullPointerException.class, () -> windowedCogroupedStream.aggregate(MockInitializer.STRING_INIT, null));
    }

    @Test
    public void shouldNotHaveNullMaterializedOnAggregate() {
        assertThrows(NullPointerException.class, () -> windowedCogroupedStream.aggregate(MockInitializer.STRING_INIT,
            sessionMerger, (Named) null));
    }

    @Test
    public void shouldNotHaveNullSessionMerger2OnAggregate() {
        assertThrows(NullPointerException.class, () -> windowedCogroupedStream.aggregate(MockInitializer.STRING_INIT,
            null, Materialized.as("test")));
    }

    @Test
    public void shouldNotHaveNullInitializer2OnAggregate() {
        assertThrows(NullPointerException.class, () -> windowedCogroupedStream.aggregate(null, sessionMerger,
            Materialized.as("test")));
    }

    @Test
    public void shouldNotHaveNullMaterialized2OnAggregate() {
        assertThrows(NullPointerException.class, () -> windowedCogroupedStream.aggregate(MockInitializer.STRING_INIT,
            sessionMerger, Named.as("name"), null));
    }

    @Test
    public void shouldNotHaveNullSessionMerger3OnAggregate() {
        assertThrows(NullPointerException.class, () -> windowedCogroupedStream.aggregate(MockInitializer.STRING_INIT,
            null, Named.as("name"), Materialized.as("test")));
    }

    @Test
    public void shouldNotHaveNullNamedOnAggregate() {
        assertThrows(NullPointerException.class, () -> windowedCogroupedStream.aggregate(MockInitializer.STRING_INIT,
            sessionMerger, null, Materialized.as("test")));
    }

    @Test
    public void shouldNotHaveNullInitializer3OnAggregate() {
        assertThrows(NullPointerException.class, () -> windowedCogroupedStream.aggregate(null, sessionMerger,
            Named.as("name"), Materialized.as("test")));
    }

    @Test
    public void shouldNotHaveNullNamed2OnAggregate() {
        assertThrows(NullPointerException.class, () -> windowedCogroupedStream.aggregate(MockInitializer.STRING_INIT, sessionMerger, (Named) null));
    }

    @Test
    public void namedParamShouldSetName() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream = builder.stream(TOPIC, Consumed
                .with(Serdes.String(), Serdes.String()));
        groupedStream = stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
        groupedStream.cogroup(MockAggregator.TOSTRING_ADDER)
                .windowedBy(SessionWindows.with(ofMillis(1)))
                .aggregate(MockInitializer.STRING_INIT, sessionMerger, Named.as("foo"));

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
    public void sessionWindowAggregateTest() {
        final KTable<Windowed<String>, String> customers = groupedStream.cogroup(MockAggregator.TOSTRING_ADDER)
                .windowedBy(SessionWindows.with(ofMillis(500)))
                .aggregate(MockInitializer.STRING_INIT, sessionMerger, Materialized.with(Serdes.String(), Serdes.String()));
        customers.toStream().to(OUTPUT);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic(
                    TOPIC, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<Windowed<String>, String> testOutputTopic = driver.createOutputTopic(
                    OUTPUT, new SessionWindowedDeserializer<>(new StringDeserializer()), new StringDeserializer());
            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k2", "A", 0);
            testInputTopic.pipeInput("k1", "B", 599);
            testInputTopic.pipeInput("k2", "B", 607);

            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+B", 599);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+B", 607);
        }
    }

    @Test
    public void sessionWindowAggregate2Test() {
        final KTable<Windowed<String>, String> customers = groupedStream.cogroup(MockAggregator.TOSTRING_ADDER)
                .windowedBy(SessionWindows.with(ofMillis(500)))
                .aggregate(MockInitializer.STRING_INIT, sessionMerger, Materialized.with(Serdes.String(), Serdes.String()));
        customers.toStream().to(OUTPUT);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic(
                    TOPIC, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<Windowed<String>, String> testOutputTopic = driver.createOutputTopic(
                    OUTPUT, new SessionWindowedDeserializer<>(new StringDeserializer()), new StringDeserializer());

            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k2", "B", 599);
            testInputTopic.pipeInput("k1", "B", 607);

            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+0+A+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+B", 599);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+B", 607);
        }


    }

    @Test
    public void sessionWindowAggregateTest2StreamsTest() {
        final KTable<Windowed<String>, String> customers = windowedCogroupedStream.aggregate(
                MockInitializer.STRING_INIT, sessionMerger, Materialized.with(Serdes.String(), Serdes.String()));
        customers.toStream().to(OUTPUT);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic(
                    TOPIC, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<Windowed<String>, String> testOutputTopic = driver.createOutputTopic(
                    OUTPUT, new SessionWindowedDeserializer<>(new StringDeserializer()), new StringDeserializer());

            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k1", "A", 84);
            testInputTopic.pipeInput("k1", "A", 113);
            testInputTopic.pipeInput("k1", "A", 199);
            testInputTopic.pipeInput("k1", "B", 300);
            testInputTopic.pipeInput("k2", "B", 301);
            testInputTopic.pipeInput("k2", "B", 400);
            testInputTopic.pipeInput("k1", "B", 400);

            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", null, 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+0+A+A", 84);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", null, 84);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+0+0+A+A+A", 113);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", null, 113);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+0+0+0+A+A+A+A", 199);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+B", 300);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+B", 301);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", null, 301);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+0+B+B", 400);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", null, 300);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+0+B+B", 400);

        }
    }

    @Test
    public void sessionWindowMixAggregatorsTest() {
        final KTable<Windowed<String>, String> customers = windowedCogroupedStream.aggregate(
                MockInitializer.STRING_INIT, sessionMerger, Materialized.with(Serdes.String(), Serdes.String()));
        customers.toStream().to(OUTPUT);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic(TOPIC, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> testInputTopic2 = driver.createInputTopic(TOPIC2, new StringSerializer(), new StringSerializer());

            final TestOutputTopic<Windowed<String>, String> testOutputTopic = driver.createOutputTopic(
                    OUTPUT, new SessionWindowedDeserializer<>(new StringDeserializer()), new StringDeserializer());
            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k2", "A", 0);
            testInputTopic.pipeInput("k2", "A", 1);
            testInputTopic.pipeInput("k1", "A", 2);
            testInputTopic2.pipeInput("k1", "B", 3);
            testInputTopic2.pipeInput("k2", "B", 3);
            testInputTopic2.pipeInput("k2", "B", 444);
            testInputTopic2.pipeInput("k1", "B", 444);

            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", null, 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+0+A+A", 1);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", null, 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+0+A+A", 2);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", null, 2);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+0+0+A+A-B", 3);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", null, 1);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+0+0+A+A-B", 3);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0-B", 444);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0-B", 444);
        }

    }

    @Test
    public void sessionWindowMixAggregatorsManyWindowsTest() {
        final KTable<Windowed<String>, String> customers = windowedCogroupedStream.aggregate(
                MockInitializer.STRING_INIT, sessionMerger, Materialized.with(Serdes.String(), Serdes.String()));
        customers.toStream().to(OUTPUT);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic(TOPIC, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> testInputTopic2 = driver.createInputTopic(TOPIC2, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<Windowed<String>, String> testOutputTopic = driver.createOutputTopic(
                    OUTPUT, new SessionWindowedDeserializer<>(new StringDeserializer()), new StringDeserializer());
            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k2", "A", 0);
            testInputTopic.pipeInput("k2", "A", 1);
            testInputTopic.pipeInput("k1", "A", 2);
            testInputTopic2.pipeInput("k1", "B", 3);
            testInputTopic2.pipeInput("k2", "B", 500);
            testInputTopic2.pipeInput("k2", "B", 501);
            testInputTopic2.pipeInput("k1", "B", 501);

            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", null, 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+0+A+A", 1);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", null, 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+0+A+A", 2);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", null, 2);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+0+0+A+A-B", 3);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0-B", 500);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", null, 500);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+0-B-B", 501);
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
