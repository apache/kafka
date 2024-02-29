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

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.test.MockApiProcessor;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.test.StreamsTestUtils.getMetricByName;

public class KStreamKStreamWindowCloseTest {

    private static final String LEFT = "left";
    private static final String RIGHT = "right";
    private final static Properties PROPS = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
    private static final Consumed<Integer, String> CONSUMED = Consumed.with(Serdes.Integer(), Serdes.String());
    private static final JoinWindows WINDOW = JoinWindows.ofTimeDifferenceAndGrace(ofMillis(10), ofMillis(5));

    static List<Arguments> streams() {
        return Arrays.asList(
            innerJoin(),
            leftJoin(),
            outerJoin()
        );
    }

    private static Arguments innerJoin() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Integer, String> stream = builder.stream(LEFT, CONSUMED).join(
            builder.stream(RIGHT, CONSUMED),
            MockValueJoiner.TOSTRING_JOINER,
            WINDOW,
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String())
        );
        final MockApiProcessorSupplier<Integer, String, Object, Object> processorSupplier = new MockApiProcessorSupplier<>();
        stream.process(processorSupplier);
        return Arguments.of(builder.build(PROPS), processorSupplier);
    }

    private static Arguments leftJoin() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Integer, String> stream = builder.stream(LEFT, CONSUMED).leftJoin(
            builder.stream(RIGHT, CONSUMED),
            MockValueJoiner.TOSTRING_JOINER,
            WINDOW,
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String())
        );
        final MockApiProcessorSupplier<Integer, String, Object, Object> processorSupplier = new MockApiProcessorSupplier<>();
        stream.process(processorSupplier);
        return Arguments.of(builder.build(PROPS), processorSupplier);
    }

    private static Arguments outerJoin() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Integer, String> stream = builder.stream(LEFT, CONSUMED).outerJoin(
            builder.stream(RIGHT, CONSUMED),
            MockValueJoiner.TOSTRING_JOINER,
            WINDOW,
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String())
        );
        final MockApiProcessorSupplier<Integer, String, Object, Object> supplier = new MockApiProcessorSupplier<>();
        stream.process(supplier);
        return Arguments.of(builder.build(PROPS), supplier);
    }

    @ParameterizedTest
    @MethodSource("streams")
    public void recordsArrivingPostWindowCloseShouldBeDropped(
        final Topology topology,
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier) {
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, PROPS)) {
            final TestInputTopic<Integer, String> left =
                driver.createInputTopic(KStreamKStreamWindowCloseTest.LEFT, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, String> right =
                driver.createInputTopic(KStreamKStreamWindowCloseTest.RIGHT, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();

            left.pipeInput(0, "left", 15);
            right.pipeInput(-1, "bumpTime", 40);
            assertRecordDropCount(0.0, processor);

            right.pipeInput(0, "closesAt39", 24);
            assertRecordDropCount(1.0, processor);

            right.pipeInput(0, "closesAt40", 25);
            assertRecordDropCount(1.0, processor);

            right.pipeInput(-1, "bumpTime", 41);
            right.pipeInput(0, "closesAt40", 25);
            assertRecordDropCount(2.0, processor);

            right.pipeInput(1, "right", 115);
            left.pipeInput(-1, "bumpTime", 140);
            left.pipeInput(1, "closesAt139", 124);
            assertRecordDropCount(3.0, processor);
            left.pipeInput(1, "closesAt140", 125);
            assertRecordDropCount(3.0, processor);

            left.pipeInput(-1, "bumpTime", 141);
            left.pipeInput(1, "closesAt140", 125);
            assertRecordDropCount(4.0, processor);
        }
    }

    static void assertRecordDropCount(final double expected, final MockApiProcessor<Integer, String, Void, Void> processor) {
        final String metricGroup = "stream-task-metrics";
        final String metricName = "dropped-records-total";
        Assertions.assertEquals(
            expected,
            getMetricByName(processor.context().metrics().metrics(), metricName, metricGroup).metricValue()
        );
    }
}
