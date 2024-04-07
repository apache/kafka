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

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.test.StreamsTestUtils.getMetricByName;

public class KStreamKStreamWindowCloseTest {

    private static final String LEFT = "left";
    private static final String RIGHT = "right";
    private static final String OUT = "out";
    private static final Properties PROPS = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
    private static final Consumed<Integer, String> CONSUMED = Consumed.with(Serdes.Integer(), Serdes.String());
    private static final JoinWindows SYMMETRIC_WINDOW = JoinWindows.ofTimeDifferenceAndGrace(ofMillis(10), ofMillis(5));
    private static final JoinWindows ASYMMETRIC_WINDOW = JoinWindows.ofTimeDifferenceAndGrace(ofMillis(0), ofMillis(5))
        .before(Duration.ofMillis(10))
        .after(Duration.ofMillis(5));

    static List<Arguments> symmetricWindows() {
        return asList(innerJoin(SYMMETRIC_WINDOW), leftJoin(SYMMETRIC_WINDOW), outerJoin(SYMMETRIC_WINDOW));
    }

    static List<Arguments> asymmetricWindows() {
        return asList(innerJoin(ASYMMETRIC_WINDOW), leftJoin(ASYMMETRIC_WINDOW), outerJoin(ASYMMETRIC_WINDOW));
    }

    private static Arguments innerJoin(final JoinWindows window) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(LEFT, CONSUMED)
            .join(
                builder.stream(RIGHT, CONSUMED),
                MockValueJoiner.TOSTRING_JOINER,
                window,
                StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String())
            )
            .to(OUT);
        return Arguments.of(builder.build(PROPS));
    }

    private static Arguments leftJoin(final JoinWindows window) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(LEFT, CONSUMED)
            .leftJoin(
                builder.stream(RIGHT, CONSUMED),
                MockValueJoiner.TOSTRING_JOINER,
                window,
                StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String())
            )
            .to(OUT);
        return Arguments.of(builder.build(PROPS));
    }

    private static Arguments outerJoin(final JoinWindows window) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(LEFT, CONSUMED)
            .outerJoin(
                builder.stream(RIGHT, CONSUMED),
                MockValueJoiner.TOSTRING_JOINER,
                window,
                StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String())
            )
            .to(OUT);
        return Arguments.of(builder.build(PROPS));
    }

    @ParameterizedTest
    @MethodSource("symmetricWindows")
    public void shouldDropRecordsArrivingTooLate(final Topology topology) {
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, PROPS)) {
            final int key = 0;
            final TestInputTopic<Integer, String> left = driver.createInputTopic(LEFT, new IntegerSerializer(), new StringSerializer());
            final TestInputTopic<Integer, String> right = driver.createInputTopic(RIGHT, new IntegerSerializer(), new StringSerializer());
            final TestOutputTopic<Integer, String> out = driver.createOutputTopic(OUT, new IntegerDeserializer(), new StringDeserializer());

            left.pipeInput(key, "l15", 15); // l15 window {5:25,5} open, closes at 31
            assertRecordDropCount(0.0, driver.metrics());

            left.pipeInput(-1, "bump time", 30);
            right.pipeInput(key, "r4", 4); // gets dropped
            right.pipeInput(key, "r5", 5);
            right.pipeInput(key, "r25-0", 25);
            Assertions.assertEquals(
                asList(
                    new TestRecord<>(key, "l15+r5", null, 15L),
                    new TestRecord<>(key, "l15+r25-0", null, 25L)
                ),
                out.readRecordsToList()
            );
            assertRecordDropCount(1.0, driver.metrics());
            left.pipeInput(-1, "bump time", 31); // l15 is now closed

            right.pipeInput(key, "r5", 5); // gets dropped
            assertRecordDropCount(2.0, driver.metrics());
            Assertions.assertEquals(emptyList(), out.readRecordsToList());

            right.pipeInput(key, "r6", 6); // Doesn't get dropped, but all involved windows are closed.
            assertRecordDropCount(2.0, driver.metrics());
            Assertions.assertEquals(emptyList(), out.readRecordsToList());

            right.pipeInput(key, "r25-1", 25);
            // r25-1 still joins with l15 (despite l15 window closed) because r25-1 window {15:35,5} is open, closes at 41.
            Assertions.assertEquals(
                singletonList(new TestRecord<>(key, "l15+r25-1", null, 25L)),
                out.readRecordsToList()
            );

            left.pipeInput(key, "l16", 16);
            Assertions.assertEquals(
                asList(
                    new TestRecord<>(key, "l16+r6", null, 16L),
                    new TestRecord<>(key, "l16+r25-0", null, 25L),
                    new TestRecord<>(key, "l16+r25-1", null, 25L)
                ),
                out.readRecordsToList()
            );
            assertRecordDropCount(2.0, driver.metrics());
        }
    }

    @ParameterizedTest
    @MethodSource("asymmetricWindows")
    public void shouldDropRecordsArrivingTooLate_Asymmetric(final Topology topology) {
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, PROPS)) {
            final int key = 0;
            final TestInputTopic<Integer, String> left = driver.createInputTopic(LEFT, new IntegerSerializer(), new StringSerializer());
            final TestInputTopic<Integer, String> right = driver.createInputTopic(RIGHT, new IntegerSerializer(), new StringSerializer());
            final TestOutputTopic<Integer, String> out = driver.createOutputTopic(OUT, new IntegerDeserializer(), new StringDeserializer());

            left.pipeInput(key, "l15", 15); // l15 window {5:20,5} open, closes at 26
            assertRecordDropCount(0.0, driver.metrics());

            left.pipeInput(-1, "bump time", 25);
            right.pipeInput(key, "r4", 4); // gets dropped
            assertRecordDropCount(1.0, driver.metrics());
            right.pipeInput(key, "r5", 5);
            right.pipeInput(key, "r20-0", 20);
            Assertions.assertEquals(
                asList(
                    new TestRecord<>(key, "l15+r5", null, 15L),
                    new TestRecord<>(key, "l15+r20-0", null, 20L)
                ),
                out.readRecordsToList()
            );
            assertRecordDropCount(1.0, driver.metrics());
            left.pipeInput(-1, "bump time", 26); // l15 is now closed

            right.pipeInput(key, "r5", 5); // gets dropped
            assertRecordDropCount(2.0, driver.metrics());
            Assertions.assertEquals(emptyList(), out.readRecordsToList());

            right.pipeInput(key, "r6", 6); // Doesn't get dropped, but all involved windows are closed.
            assertRecordDropCount(2.0, driver.metrics());
            Assertions.assertEquals(emptyList(), out.readRecordsToList());

            right.pipeInput(key, "r20-1", 20);
            // r20-1 still joins with l15 (despite l15 window closed) because r20-1 window {10:25,5} is open, closes at 31.
            Assertions.assertEquals(
                singletonList(new TestRecord<>(key, "l15+r20-1", null, 20L)),
                out.readRecordsToList()
            );

            left.pipeInput(key, "l16", 16);
            Assertions.assertEquals(
                asList(
                    new TestRecord<>(key, "l16+r6", null, 16L),
                    new TestRecord<>(key, "l16+r20-0", null, 20L),
                    new TestRecord<>(key, "l16+r20-1", null, 20L)
                ),
                out.readRecordsToList()
            );
            assertRecordDropCount(2.0, driver.metrics());
        }
    }

    static void assertRecordDropCount(final double expected, final Map<MetricName, ? extends Metric> metrics) {
        final String metricGroup = "stream-task-metrics";
        final String metricName = "dropped-records-total";
        Assertions.assertEquals(expected, getMetricByName(metrics, metricName, metricGroup).metricValue());
    }
}
