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
import static org.junit.Assert.assertNotNull;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.CogroupedKStream;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.SessionWindowedCogroupedKStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;


public class SessionWindowedCogroupedKStreamImplTest {

    private final MockProcessorSupplier<Windowed<String>, String> processorSupplier = new MockProcessorSupplier<>();
    private static final String TOPIC = "topic";
    private static final String TOPIC2 = "topic2";
    private final StreamsBuilder builder = new StreamsBuilder();

    private final Merger<String, String> sessionMerger = (aggKey, aggOne, aggTwo) -> aggOne + "+" + aggTwo;
    private KGroupedStream<String, String> groupedStream;

    private KGroupedStream<String, String> groupedStream2;
    private CogroupedKStream<String, String, String> cogroupedStream;
    private SessionWindowedCogroupedKStream<String, String> windowedCogroupedStream;

    private final Properties props = StreamsTestUtils
        .getStreamsConfig(Serdes.String(), Serdes.String());

    @Before
    public void setup() {
        final KStream<String, String> stream = builder.stream(TOPIC, Consumed
            .with(Serdes.String(), Serdes.String()));
        final KStream<String, String> stream2 = builder.stream(TOPIC2, Consumed
            .with(Serdes.String(), Serdes.String()));

        groupedStream = stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
        groupedStream2 = stream2.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
        cogroupedStream = groupedStream.cogroup(MockAggregator.TOSTRING_ADDER).cogroup(groupedStream2, MockAggregator.TOSTRING_REMOVER);
        windowedCogroupedStream = cogroupedStream.windowedBy(SessionWindows.with(ofMillis(100)));
    }

    @Test
    public void sessionWindowTest() {
        assertNotNull(windowedCogroupedStream);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullInitializerOnAggregate() {
        windowedCogroupedStream.aggregate(null, sessionMerger);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullSessionMergerOnAggregate() {
        windowedCogroupedStream.aggregate(MockInitializer.STRING_INIT, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullMaterializedOnAggregate() {
        windowedCogroupedStream.aggregate(MockInitializer.STRING_INIT, sessionMerger, null);
    }

    @Test
    public void sessionWindowAggregateTest() {

        final KTable<Windowed<String>, String> customers = groupedStream.cogroup(MockAggregator.TOSTRING_ADDER).windowedBy(SessionWindows.with(ofMillis(500))).aggregate(MockInitializer.STRING_INIT, sessionMerger);
        customers.toStream().process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic(TOPIC, new StringSerializer(), new StringSerializer());

            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k11", "A", 0);
            testInputTopic.pipeInput("k11", "B", 599);
            testInputTopic.pipeInput("k1", "B", 607);
        }
        assertThat(
            processorSupplier.theCapturedProcessor().lastValueAndTimestampPerKey
                .get(new Windowed<>("k1", new SessionWindow(0, 0))),
            equalTo(ValueAndTimestamp.make("0+A", 0)));
        assertThat(
            processorSupplier.theCapturedProcessor().lastValueAndTimestampPerKey
                .get(new Windowed<>("k11", new SessionWindow(599, 599))),
            equalTo(ValueAndTimestamp.make("0+B", 599)));
    }

    @Test
    public void sessionWindowAggregate2Test() {

        final KTable<Windowed<String>, String> customers = groupedStream.cogroup(MockAggregator.TOSTRING_ADDER).windowedBy(SessionWindows.with(ofMillis(500))).aggregate(MockInitializer.STRING_INIT, sessionMerger);
        customers.toStream().process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic(TOPIC, new StringSerializer(), new StringSerializer());
            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k11", "B", 599);
            testInputTopic.pipeInput("k1", "B", 607);
        }
        assertThat(
            processorSupplier.theCapturedProcessor().lastValueAndTimestampPerKey
                .get(new Windowed<>("k1", new SessionWindow(0, 0))),
            equalTo(ValueAndTimestamp.make("0+0+A+A", 0)));
        assertThat(
            processorSupplier.theCapturedProcessor().lastValueAndTimestampPerKey
                .get(new Windowed<>("k11", new SessionWindow(599, 599))),
            equalTo(ValueAndTimestamp.make("0+B", 599)));
    }

    @Test
    public void sessionWindowAggregateTest2StreamsTest() {

        final KTable<Windowed<String>, String> customers = windowedCogroupedStream.aggregate(MockInitializer.STRING_INIT, sessionMerger);
        customers.toStream().process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic(TOPIC, new StringSerializer(), new StringSerializer());
            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k1", "A", 84);
            testInputTopic.pipeInput("k1", "A", 113);
            testInputTopic.pipeInput("k1", "A", 199);
            testInputTopic.pipeInput("k1", "B", 300);
            testInputTopic.pipeInput("k11", "B", 301);
            testInputTopic.pipeInput("k11", "B", 400);
            testInputTopic.pipeInput("k1", "B", 400);
        }
        assertThat(
            processorSupplier.theCapturedProcessor().lastValueAndTimestampPerKey
                .get(new Windowed<>("k1", new SessionWindow(0, 199))),
            equalTo(ValueAndTimestamp.make("0+0+0+0+A+A+A+A", 199)));
        assertThat(
            processorSupplier.theCapturedProcessor().lastValueAndTimestampPerKey
                .get(new Windowed<>("k1", new SessionWindow(300, 400))),
            equalTo(ValueAndTimestamp.make("0+0+B+B", 400)));
        assertThat(
            processorSupplier.theCapturedProcessor().lastValueAndTimestampPerKey
                .get(new Windowed<>("k11", new SessionWindow(301, 400))),
            equalTo(ValueAndTimestamp.make("0+0+B+B", 400)));
    }

    @Test
    public void sessionWindowMixAggregatorsTest() {

        final KTable<Windowed<String>, String> customers = windowedCogroupedStream.aggregate(MockInitializer.STRING_INIT, sessionMerger);
        customers.toStream().process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic(TOPIC, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> testInputTopic2 = driver.createInputTopic(TOPIC2, new StringSerializer(), new StringSerializer());
            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k11", "A", 0);
            testInputTopic.pipeInput("k11", "A", 1);
            testInputTopic.pipeInput("k1", "A", 2);
            testInputTopic2.pipeInput("k1", "B", 3);
            testInputTopic2.pipeInput("k11", "B", 3);
            testInputTopic2.pipeInput("k11", "B", 444);
            testInputTopic2.pipeInput("k1", "B", 444);
        }
        assertThat(
            processorSupplier.theCapturedProcessor().lastValueAndTimestampPerKey
                .get(new Windowed<>("k1", new SessionWindow(0, 3))),
            equalTo(ValueAndTimestamp.make("0+0+0+A+A-B", 3)));
        assertThat(
            processorSupplier.theCapturedProcessor().lastValueAndTimestampPerKey
                .get(new Windowed<>("k11", new SessionWindow(444, 444))),
            equalTo(ValueAndTimestamp.make("0-B", 444)));
    }



    @Test
    public void sessionWindowMixAggregatorsManyWindowsTest() {

        final KTable<Windowed<String>, String> customers = windowedCogroupedStream.aggregate(MockInitializer.STRING_INIT, sessionMerger);
        customers.toStream().process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic(TOPIC, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> testInputTopic2 = driver.createInputTopic(TOPIC2, new StringSerializer(), new StringSerializer());
            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k11", "A", 0);
            testInputTopic.pipeInput("k11", "A", 1);
            testInputTopic.pipeInput("k1", "A", 2);
            testInputTopic2.pipeInput("k1", "B", 3);
            testInputTopic2.pipeInput("k11", "B", 500);
            testInputTopic2.pipeInput("k11", "B", 501);
            testInputTopic2.pipeInput("k1", "B", 501);
        }
        assertThat(
            processorSupplier.theCapturedProcessor().lastValueAndTimestampPerKey
                .get(new Windowed<>("k1", new SessionWindow(0, 3))),
            equalTo(ValueAndTimestamp.make("0+0+0+A+A-B", 3)));
        assertThat(
            processorSupplier.theCapturedProcessor().lastValueAndTimestampPerKey
                .get(new Windowed<>("k11", new SessionWindow(500, 501))),
            equalTo(ValueAndTimestamp.make("0+0-B-B", 501)));
        assertThat(
            processorSupplier.theCapturedProcessor().lastValueAndTimestampPerKey
                .get(new Windowed<>("k1", new SessionWindow(501, 501))),
            equalTo(ValueAndTimestamp.make("0-B", 501)));


    }

}
