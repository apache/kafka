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
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.test.StreamsTestUtils.getMetricByName;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class KStreamKStreamJoinTest {
    final private String topic1 = "topic1";
    final private String topic2 = "topic2";

    private final Consumed<Integer, String> consumed = Consumed.with(Serdes.Integer(), Serdes.String());
    private final ConsumerRecordFactory<Integer, String> recordFactory =
        new ConsumerRecordFactory<>(new IntegerSerializer(), new StringSerializer(), 0L);
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    @Test
    public void shouldLogAndMeterOnSkippedRecordsWithNullValue() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Integer> left = builder.stream("left", Consumed.with(Serdes.String(), Serdes.Integer()));
        final KStream<String, Integer> right = builder.stream("right", Consumed.with(Serdes.String(), Serdes.Integer()));
        final ConsumerRecordFactory<String, Integer> recordFactory =
            new ConsumerRecordFactory<>(new StringSerializer(), new IntegerSerializer());

        left.join(
            right,
            (value1, value2) -> value1 + value2,
            JoinWindows.of(ofMillis(100)),
            Joined.with(Serdes.String(), Serdes.Integer(), Serdes.Integer())
        );

        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create("left", "A", null));
            LogCaptureAppender.unregister(appender);

            assertThat(appender.getMessages(), hasItem("Skipping record due to null key or value. key=[A] value=[null] topic=[left] partition=[0] offset=[0]"));

            assertEquals(1.0, getMetricByName(driver.metrics(), "skipped-records-total", "stream-metrics").metricValue());
        }
    }

    @Test
    public void testJoin() {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockProcessorSupplier<Integer, String> supplier = new MockProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);
        joined = stream1.join(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.of(ofMillis(100)),
            Joined.with(Serdes.Integer(), Serdes.String(), Serdes.String()));
        joined.process(supplier);

        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final MockProcessor<Integer, String> processor = supplier.theCapturedProcessor();

            // push two items to the primary stream; the other window is empty
            // w1 = {}
            // w2 = {}
            // --> w1 = { 0:X0, 1:X1 }
            //     w2 = {}
            for (int i = 0; i < 2; i++) {
                driver.pipeInput(recordFactory.create(topic1, expectedKeys[i], "X" + expectedKeys[i]));
            }
            processor.checkAndClearProcessResult();

            // push two items to the other stream; this should produce two items
            // w1 = { 0:X0, 1:X1 }
            // w2 = {}
            // --> w1 = { 0:X0, 1:X1 }
            //     w2 = { 0:Y0, 1:Y1 }
            for (int i = 0; i < 2; i++) {
                driver.pipeInput(recordFactory.create(topic2, expectedKeys[i], "Y" + expectedKeys[i]));
            }
            processor.checkAndClearProcessResult("0:X0+Y0 (ts: 0)", "1:X1+Y1 (ts: 0)");

            // push all four items to the primary stream; this should produce two items
            // w1 = { 0:X0, 1:X1 }
            // w2 = { 0:Y0, 1:Y1 }
            // --> w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3 }
            //     w2 = { 0:Y0, 1:Y1 }
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "X" + expectedKey));
            }
            processor.checkAndClearProcessResult("0:X0+Y0 (ts: 0)", "1:X1+Y1 (ts: 0)");

            // push all items to the other stream; this should produce six items
            // w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3 }
            // w2 = { 0:Y0, 1:Y1 }
            // --> w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3 }
            //     w2 = { 0:Y0, 1:Y1, 0:YY0, 1:YY1, 2:YY2, 3:YY3 }
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YY" + expectedKey));
            }
            processor.checkAndClearProcessResult("0:X0+YY0 (ts: 0)", "0:X0+YY0 (ts: 0)", "1:X1+YY1 (ts: 0)", "1:X1+YY1 (ts: 0)", "2:X2+YY2 (ts: 0)", "3:X3+YY3 (ts: 0)");

            // push all four items to the primary stream; this should produce six items
            // w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3 }
            // w2 = { 0:Y0, 1:Y1, 0:YY0, 0:YY0, 1:YY1, 2:YY2, 3:YY3 }
            // --> w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3,  0:XX0, 1:XX1, 2:XX2, 3:XX3 }
            //     w2 = { 0:Y0, 1:Y1, 0:YY0, 1:YY1, 2:YY2, 3:YY3 }
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XX" + expectedKey));
            }
            processor.checkAndClearProcessResult("0:XX0+Y0 (ts: 0)", "0:XX0+YY0 (ts: 0)", "1:XX1+Y1 (ts: 0)", "1:XX1+YY1 (ts: 0)", "2:XX2+YY2 (ts: 0)", "3:XX3+YY3 (ts: 0)");

            // push two items to the other stream; this should produce six items
            // w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3,  0:XX0, 1:XX1, 2:XX2, 3:XX3 }
            // w2 = { 0:Y0, 1:Y1, 0:YY0, 0:YY0, 1:YY1, 2:YY2, 3:YY3 }
            // --> w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3,  0:XX0, 1:XX1, 2:XX2, 3:XX3 }
            //     w2 = { 0:Y0, 1:Y1, 0:YY0, 1:YY1, 2:YY2, 3:YY3, 0:YYY0, 1:YYY1 }
            for (int i = 0; i < 2; i++) {
                driver.pipeInput(recordFactory.create(topic2, expectedKeys[i], "YYY" + expectedKeys[i]));
            }
            processor.checkAndClearProcessResult("0:X0+YYY0 (ts: 0)", "0:X0+YYY0 (ts: 0)", "0:XX0+YYY0 (ts: 0)", "1:X1+YYY1 (ts: 0)", "1:X1+YYY1 (ts: 0)", "1:XX1+YYY1 (ts: 0)");
        }
    }

    @Test
    public void testOuterJoin() {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockProcessorSupplier<Integer, String> supplier = new MockProcessorSupplier<>();

        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);
        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.of(ofMillis(100)),
            Joined.with(Serdes.Integer(), Serdes.String(), Serdes.String()));
        joined.process(supplier);
        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final MockProcessor<Integer, String> processor = supplier.theCapturedProcessor();

            // push two items to the primary stream; the other window is empty; this should produce two items
            // w1 = {}
            // w2 = {}
            // --> w1 = { 0:X0, 1:X1 }
            //     w2 = {}
            for (int i = 0; i < 2; i++) {
                driver.pipeInput(recordFactory.create(topic1, expectedKeys[i], "X" + expectedKeys[i]));
            }
            processor.checkAndClearProcessResult("0:X0+null (ts: 0)", "1:X1+null (ts: 0)");

            // push two items to the other stream; this should produce two items
            // w1 = { 0:X0, 1:X1 }
            // w2 = {}
            // --> w1 = { 0:X0, 1:X1 }
            //     w2 = { 0:Y0, 1:Y1 }
            for (int i = 0; i < 2; i++) {
                driver.pipeInput(recordFactory.create(topic2, expectedKeys[i], "Y" + expectedKeys[i]));
            }
            processor.checkAndClearProcessResult("0:X0+Y0 (ts: 0)", "1:X1+Y1 (ts: 0)");

            // push all four items to the primary stream; this should produce four items
            // w1 = { 0:X0, 1:X1 }
            // w2 = { 0:Y0, 1:Y1 }
            // --> w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3 }
            //     w2 = { 0:Y0, 1:Y1 }
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "X" + expectedKey));
            }
            processor.checkAndClearProcessResult("0:X0+Y0 (ts: 0)", "1:X1+Y1 (ts: 0)", "2:X2+null (ts: 0)", "3:X3+null (ts: 0)");

            // push all items to the other stream; this should produce six items
            // w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3 }
            // w2 = { 0:Y0, 1:Y1 }
            // --> w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3 }
            //     w2 = { 0:Y0, 1:Y1, 0:YY0, 0:YY0, 1:YY1, 2:YY2, 3:YY3 }
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YY" + expectedKey));
            }
            processor.checkAndClearProcessResult("0:X0+YY0 (ts: 0)", "0:X0+YY0 (ts: 0)", "1:X1+YY1 (ts: 0)", "1:X1+YY1 (ts: 0)", "2:X2+YY2 (ts: 0)", "3:X3+YY3 (ts: 0)");

            // push all four items to the primary stream; this should produce six items
            // w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3 }
            // w2 = { 0:Y0, 1:Y1, 0:YY0, 0:YY0, 1:YY1, 2:YY2, 3:YY3 }
            // --> w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3,  0:XX0, 1:XX1, 2:XX2, 3:XX3 }
            //     w2 = { 0:Y0, 1:Y1, 0:YY0, 0:YY0, 1:YY1, 2:YY2, 3:YY3 }
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XX" + expectedKey));
            }
            processor.checkAndClearProcessResult("0:XX0+Y0 (ts: 0)", "0:XX0+YY0 (ts: 0)", "1:XX1+Y1 (ts: 0)", "1:XX1+YY1 (ts: 0)", "2:XX2+YY2 (ts: 0)", "3:XX3+YY3 (ts: 0)");

            // push two items to the other stream; this should produce six items
            // w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3,  0:XX0, 1:XX1, 2:XX2, 3:XX3 }
            // w2 = { 0:Y0, 1:Y1, 0:YY0, 0:YY0, 1:YY1, 2:YY2, 3:YY3 }
            // --> w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3,  0:XX0, 1:XX1, 2:XX2, 3:XX3 }
            //     w2 = { 0:Y0, 1:Y1, 0:YY0, 0:YY0, 1:YY1, 2:YY2, 3:YY3, 0:YYY0, 1:YYY1 }
            for (int i = 0; i < 2; i++) {
                driver.pipeInput(recordFactory.create(topic2, expectedKeys[i], "YYY" + expectedKeys[i]));
            }
            processor.checkAndClearProcessResult("0:X0+YYY0 (ts: 0)", "0:X0+YYY0 (ts: 0)", "0:XX0+YYY0 (ts: 0)", "1:X1+YYY1 (ts: 0)", "1:X1+YYY1 (ts: 0)", "1:XX1+YYY1 (ts: 0)");
        }
    }

    @Test
    public void testWindowing() {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockProcessorSupplier<Integer, String> supplier = new MockProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);

        joined = stream1.join(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.of(ofMillis(100)),
            Joined.with(Serdes.Integer(), Serdes.String(), Serdes.String()));
        joined.process(supplier);

        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final MockProcessor<Integer, String> processor = supplier.theCapturedProcessor();
            long time = 0L;

            // push two items to the primary stream; the other window is empty; this should produce no items
            // w1 = {}
            // w2 = {}
            // --> w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0) }
            //     w2 = {}
            for (int i = 0; i < 2; i++) {
                driver.pipeInput(recordFactory.create(topic1, expectedKeys[i], "X" + expectedKeys[i], time));
            }
            processor.checkAndClearProcessResult();

            // push two items to the other stream; this should produce two items
            // w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0) }
            // w2 = {}
            // --> w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0) }
            //     w2 = { 0:Y0 (ts: 0), 1:Y1 (ts: 0) }
            for (int i = 0; i < 2; i++) {
                driver.pipeInput(recordFactory.create(topic2, expectedKeys[i], "Y" + expectedKeys[i], time));
            }
            processor.checkAndClearProcessResult("0:X0+Y0 (ts: 0)", "1:X1+Y1 (ts: 0)");

            // push four items to the primary stream with larger and increasing timestamp; this should produce no items
            // w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0) }
            // w2 = { 0:Y0 (ts: 0), 1:Y1 (ts: 0) }
            // --> w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0),
            //            0:XX0 (ts: 1000), 1:XX1 (ts: 1001), 2:XX2 (ts: 1002), 3:XX3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 0), 1:Y1 (ts: 0) }
            time = 1000L;
            for (int i = 0; i < expectedKeys.length; i++) {
                driver.pipeInput(recordFactory.create(topic1, expectedKeys[i], "XX" + expectedKeys[i], time + i));
            }
            processor.checkAndClearProcessResult();

            // push four items to the other stream with fixed larger timestamp; this should produce four items
            // w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0),
            //        0:XX0 (ts: 1000), 1:XX1 (ts: 1001), 2:XX2 (ts: 1002), 3:XX3 (ts: 1003)  }
            // w2 = { 0:Y0 (ts: 0), 1:Y1 (ts: 0) }
            // --> w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0),
            //            0:XX0 (ts: 1000), 1:XX1 (ts: 1001), 2:XX2 (ts: 1002), 3:XX3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 0), 1:Y1 (ts: 0),
            //            0:YY0 (ts: 1100), 1:YY1 (ts: 1100), 2:YY2 (ts: 1100), 3:YY3 (ts: 1100) }
            time += 100L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:XX0+YY0 (ts: 1100)", "1:XX1+YY1 (ts: 1100)", "2:XX2+YY2 (ts: 1100)", "3:XX3+YY3 (ts: 1100)");

            // push four items to the other stream with incremented timestamp; this should produce three items
            // w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0),
            //        0:XX0 (ts: 1000), 1:XX1 (ts: 1001), 2:XX2 (ts: 1002), 3:XX3 (ts: 1003)  }
            // w2 = { 0:Y0 (ts: 0), 1:Y1 (ts: 0),
            //        0:YY0 (ts: 1100), 1:YY1 (ts: 1100), 2:YY2 (ts: 1100), 3:YY3 (ts: 1100) }
            // --> w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0),
            //            0:XX0 (ts: 1000), 1:XX1 (ts: 1001), 2:XX2 (ts: 1002), 3:XX3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 0), 1:Y1 (ts: 0),
            //            0:YY0 (ts: 1100), 1:YY1 (ts: 1100), 2:YY2 (ts: 1100), 3:YY3 (ts: 1100),
            //            0:YYY0 (ts: 1101), 1:YYY1 (ts: 1101), 2:YYY2 (ts: 1101), 3:YYY3 (ts: 1101) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YYY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("1:XX1+YYY1 (ts: 1101)", "2:XX2+YYY2 (ts: 1101)", "3:XX3+YYY3 (ts: 1101)");

            // push four items to the other stream with incremented timestamp; this should produce two items
            // w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0),
            //        0:XX0 (ts: 1000), 1:XX1 (ts: 1001), 2:XX2 (ts: 1002), 3:XX3 (ts: 1003)  }
            // w2 = { 0:Y0 (ts: 0), 1:Y1 (ts: 0),
            //        0:YY0 (ts: 1100), 1:YY1 (ts: 1100), 2:YY2 (ts: 1100), 3:YY3 (ts: 1100),
            //        0:YYY0 (ts: 1101), 1:YYY1 (ts: 1101), 2:YYY2 (ts: 1101), 3:YYY3 (ts: 1101) }
            // --> w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0),
            //            0:XX0 (ts: 1000), 1:XX1 (ts: 1001), 2:XX2 (ts: 1002), 3:XX3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 0), 1:Y1 (ts: 0),
            //            0:YY0 (ts: 1100), 1:YY1 (ts: 1100), 2:YY2 (ts: 1100), 3:YY3 (ts: 1100),
            //            0:YYY0 (ts: 1101), 1:YYY1 (ts: 1101), 2:YYY2 (ts: 1101), 3:YYY3 (ts: 1101),
            //            0:YYYY0 (ts: 1102), 1:YYYY1 (ts: 1102), 2:YYYY2 (ts: 1102), 3:YYYY3 (ts: 1102) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YYYY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("2:XX2+YYYY2 (ts: 1102)", "3:XX3+YYYY3 (ts: 1102)");

            // push four items to the other stream with incremented timestamp; this should produce one item
            // w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0),
            //        0:XX0 (ts: 1000), 1:XX1 (ts: 1001), 2:XX2 (ts: 1002), 3:XX3 (ts: 1003)  }
            // w2 = { 0:Y0 (ts: 0), 1:Y1 (ts: 0),
            //        0:YY0 (ts: 1100), 1:YY1 (ts: 1100), 2:YY2 (ts: 1100), 3:YY3 (ts: 1100),
            //        0:YYY0 (ts: 1101), 1:YYY1 (ts: 1101), 2:YYY2 (ts: 1101), 3:YYY3 (ts: 1101),
            //        0:YYYY0 (ts: 1102), 1:YYYY1 (ts: 1102), 2:YYYY2 (ts: 1102), 3:YYYY3 (ts: 1102) }
            // --> w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0),
            //            0:XX0 (ts: 1000), 1:XX1 (ts: 1001), 2:XX2 (ts: 1002), 3:XX3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 0), 1:Y1 (ts: 0),
            //            0:YY0 (ts: 1100), 1:YY1 (ts: 1100), 2:YY2 (ts: 1100), 3:YY3 (ts: 1100),
            //            0:YYY0 (ts: 1101), 1:YYY1 (ts: 1101), 2:YYY2 (ts: 1101), 3:YYY3 (ts: 1101),
            //            0:YYYY0 (ts: 1102), 1:YYYY1 (ts: 1102), 2:YYYY2 (ts: 1102), 3:YYYY3 (ts: 1102),
            //            0:YYYYY0 (ts: 1103), 1:YYYYY1 (ts: 1103), 2:YYYYY2 (ts: 1103), 3:YYYYY3 (ts: 1103) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YYYY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("3:XX3+YYYY3 (ts: 1103)");

            // push four items to the other stream with incremented timestamp; this should produce no items
            // w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0),
            //        0:XX0 (ts: 1000), 1:XX1 (ts: 1001), 2:XX2 (ts: 1002), 3:XX3 (ts: 1003)  }
            // w2 = { 0:Y0 (ts: 0), 1:Y1 (ts: 0),
            //        0:YY0 (ts: 1100), 1:YY1 (ts: 1100), 2:YY2 (ts: 1100), 3:YY3 (ts: 1100),
            //        0:YYY0 (ts: 1101), 1:YYY1 (ts: 1101), 2:YYY2 (ts: 1101), 3:YYY3 (ts: 1101),
            //        0:YYYY0 (ts: 1102), 1:YYYY1 (ts: 1102), 2:YYYY2 (ts: 1102), 3:YYYY3 (ts: 1102),
            //        0:YYYYY0 (ts: 1103), 1:YYYYY1 (ts: 1103), 2:YYYYY2 (ts: 1103), 3:YYYYY3 (ts: 1103) }
            // --> w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0),
            //            0:XX0 (ts: 1000), 1:XX1 (ts: 1001), 2:XX2 (ts: 1002), 3:XX3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 0), 1:Y1 (ts: 0),
            //            0:YY0 (ts: 1100), 1:YY1 (ts: 1100), 2:YY2 (ts: 1100), 3:YY3 (ts: 1100),
            //            0:YYY0 (ts: 1101), 1:YYY1 (ts: 1101), 2:YYY2 (ts: 1101), 3:YYY3 (ts: 1101),
            //            0:YYYY0 (ts: 1102), 1:YYYY1 (ts: 1102), 2:YYYY2 (ts: 1102), 3:YYYY3 (ts: 1102),
            //            0:YYYYY0 (ts: 1103), 1:YYYYY1 (ts: 1103), 2:YYYYY2 (ts: 1103), 3:YYYYY3 (ts: 1103),
            //            0:YYYYYY0 (ts: 1104), 1:YYYYYY1 (ts: 1104), 2:YYYYYY2 (ts: 1104), 3:YYYYYY3 (ts: 1104) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YYYYYY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult();

            // push four items to the other stream with timestamp before the window bound; this should produce no items
            // w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0),
            //        0:XX0 (ts: 1000), 1:XX1 (ts: 1001), 2:XX2 (ts: 1002), 3:XX3 (ts: 1003)  }
            // w2 = { 0:Y0 (ts: 0), 1:Y1 (ts: 0),
            //        0:YY0 (ts: 1100), 1:YY1 (ts: 1100), 2:YY2 (ts: 1100), 3:YY3 (ts: 1100),
            //        0:YYY0 (ts: 1101), 1:YYY1 (ts: 1101), 2:YYY2 (ts: 1101), 3:YYY3 (ts: 1101),
            //        0:YYYY0 (ts: 1102), 1:YYYY1 (ts: 1102), 2:YYYY2 (ts: 1102), 3:YYYY3 (ts: 1102),
            //        0:YYYYY0 (ts: 1103), 1:YYYYY1 (ts: 1103), 2:YYYYY2 (ts: 1103), 3:YYYYY3 (ts: 1103),
            //        0:YYYYYY0 (ts: 1104), 1:YYYYYY1 (ts: 1104), 2:YYYYYY2 (ts: 1104), 3:YYYYYY3 (ts: 1104) }
            // --> w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0),
            //            0:XX0 (ts: 1000), 1:XX1 (ts: 1001), 2:XX2 (ts: 1002), 3:XX3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 0), 1:Y1 (ts: 0),
            //            0:YY0 (ts: 1100), 1:YY1 (ts: 1100), 2:YY2 (ts: 1100), 3:YY3 (ts: 1100),
            //            0:YYY0 (ts: 1101), 1:YYY1 (ts: 1101), 2:YYY2 (ts: 1101), 3:YYY3 (ts: 1101),
            //            0:YYYY0 (ts: 1102), 1:YYYY1 (ts: 1102), 2:YYYY2 (ts: 1102), 3:YYYY3 (ts: 1102),
            //            0:YYYYY0 (ts: 1103), 1:YYYYY1 (ts: 1103), 2:YYYYY2 (ts: 1103), 3:YYYYY3 (ts: 1103),
            //            0:YYYYYY0 (ts: 1104), 1:YYYYYY1 (ts: 1104), 2:YYYYYY2 (ts: 1104), 3:YYYYYY3 (ts: 1104),
            //            0:YYYYYYY0 (ts: 899), 1:YYYYYYY1 (ts: 899), 2:YYYYYYY2 (ts: 899), 3:YYYYYYY3 (ts: 899) }
            time = 1000L - 100L - 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YYYYYYY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult();

            // push four items to the other stream with with incremented timestamp; this should produce one item
            // w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0),
            //        0:XX0 (ts: 1000), 1:XX1 (ts: 1001), 2:XX2 (ts: 1002), 3:XX3 (ts: 1003)  }
            // w2 = { 0:Y0 (ts: 0), 1:Y1 (ts: 0),
            //        0:YY0 (ts: 1100), 1:YY1 (ts: 1100), 2:YY2 (ts: 1100), 3:YY3 (ts: 1100),
            //        0:YYY0 (ts: 1101), 1:YYY1 (ts: 1101), 2:YYY2 (ts: 1101), 3:YYY3 (ts: 1101),
            //        0:YYYY0 (ts: 1102), 1:YYYY1 (ts: 1102), 2:YYYY2 (ts: 1102), 3:YYYY3 (ts: 1102),
            //        0:YYYYY0 (ts: 1103), 1:YYYYY1 (ts: 1103), 2:YYYYY2 (ts: 1103), 3:YYYYY3 (ts: 1103),
            //        0:YYYYYY0 (ts: 1104), 1:YYYYYY1 (ts: 1104), 2:YYYYYY2 (ts: 1104), 3:YYYYYY3 (ts: 1104),
            //        0:YYYYYYY0 (ts: 899), 1:YYYYYYY1 (ts: 899), 2:YYYYYYY2 (ts: 899), 3:YYYYYYY3 (ts: 899) }
            // --> w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0),
            //            0:XX0 (ts: 1000), 1:XX1 (ts: 1001), 2:XX2 (ts: 1002), 3:XX3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 0), 1:Y1 (ts: 0),
            //            0:YY0 (ts: 1100), 1:YY1 (ts: 1100), 2:YY2 (ts: 1100), 3:YY3 (ts: 1100),
            //            0:YYY0 (ts: 1101), 1:YYY1 (ts: 1101), 2:YYY2 (ts: 1101), 3:YYY3 (ts: 1101),
            //            0:YYYY0 (ts: 1102), 1:YYYY1 (ts: 1102), 2:YYYY2 (ts: 1102), 3:YYYY3 (ts: 1102),
            //            0:YYYYY0 (ts: 1103), 1:YYYYY1 (ts: 1103), 2:YYYYY2 (ts: 1103), 3:YYYYY3 (ts: 1103),
            //            0:YYYYYY0 (ts: 1104), 1:YYYYYY1 (ts: 1104), 2:YYYYYY2 (ts: 1104), 3:YYYYYY3 (ts: 1104),
            //            0:YYYYYYY0 (ts: 899), 1:YYYYYYY1 (ts: 899), 2:YYYYYYY2 (ts: 899), 3:YYYYYYY3 (ts: 899),
            //            0:YYYYYYYY0 (ts: 900), 1:YYYYYYYY1 (ts: 900), 2:YYYYYYYY2 (ts: 900), 3:YYYYYYYY3 (ts: 900) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YYYYYYYY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:XX0+YYYYYYYY0 (ts: 1000)");

            // push four items to the other stream with with incremented timestamp; this should produce two items
            // w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0),
            //        0:XX0 (ts: 1000), 1:XX1 (ts: 1001), 2:XX2 (ts: 1002), 3:XX3 (ts: 1003)  }
            // w2 = { 0:Y0 (ts: 0), 1:Y1 (ts: 0),
            //        0:YY0 (ts: 1100), 1:YY1 (ts: 1100), 2:YY2 (ts: 1100), 3:YY3 (ts: 1100),
            //        0:YYY0 (ts: 1101), 1:YYY1 (ts: 1101), 2:YYY2 (ts: 1101), 3:YYY3 (ts: 1101),
            //        0:YYYY0 (ts: 1102), 1:YYYY1 (ts: 1102), 2:YYYY2 (ts: 1102), 3:YYYY3 (ts: 1102),
            //        0:YYYYY0 (ts: 1103), 1:YYYYY1 (ts: 1103), 2:YYYYY2 (ts: 1103), 3:YYYYY3 (ts: 1103),
            //        0:YYYYYY0 (ts: 1104), 1:YYYYYY1 (ts: 1104), 2:YYYYYY2 (ts: 1104), 3:YYYYYY3 (ts: 1104),
            //        0:YYYYYYY0 (ts: 899), 1:YYYYYYY1 (ts: 899), 2:YYYYYYY2 (ts: 899), 3:YYYYYYY3 (ts: 899),
            //        0:YYYYYYYY0 (ts: 900), 1:YYYYYYYY1 (ts: 900), 2:YYYYYYYY2 (ts: 900), 3:YYYYYYYY3 (ts: 900) }
            // --> w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0),
            //            0:XX0 (ts: 1000), 1:XX1 (ts: 1001), 2:XX2 (ts: 1002), 3:XX3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 0), 1:Y1 (ts: 0),
            //            0:YY0 (ts: 1100), 1:YY1 (ts: 1100), 2:YY2 (ts: 1100), 3:YY3 (ts: 1100),
            //            0:YYY0 (ts: 1101), 1:YYY1 (ts: 1101), 2:YYY2 (ts: 1101), 3:YYY3 (ts: 1101),
            //            0:YYYY0 (ts: 1102), 1:YYYY1 (ts: 1102), 2:YYYY2 (ts: 1102), 3:YYYY3 (ts: 1102),
            //            0:YYYYY0 (ts: 1103), 1:YYYYY1 (ts: 1103), 2:YYYYY2 (ts: 1103), 3:YYYYY3 (ts: 1103),
            //            0:YYYYYY0 (ts: 1104), 1:YYYYYY1 (ts: 1104), 2:YYYYYY2 (ts: 1104), 3:YYYYYY3 (ts: 1104),
            //            0:YYYYYYY0 (ts: 899), 1:YYYYYYY1 (ts: 899), 2:YYYYYYY2 (ts: 899), 3:YYYYYYY3 (ts: 899),
            //            0:YYYYYYYY0 (ts: 900), 1:YYYYYYYY1 (ts: 900), 2:YYYYYYYY2 (ts: 900), 3:YYYYYYYY3 (ts: 900)
            //            0:YYYYYYYYY0 (ts: 901), 1:YYYYYYYYY1 (ts: 901), 2:YYYYYYYYY2 (ts: 901), 3:YYYYYYYYY3 (ts: 901) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YYYYYYYYY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:XX0+YYYYYYYYY0 (ts: 1000)", "1:XX1+YYYYYYYYY1 (ts: 1001)");

            // push four items to the other stream with with incremented timestamp; this should produce three items
            // w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0),
            //        0:XX0 (ts: 1000), 1:XX1 (ts: 1001), 2:XX2 (ts: 1002), 3:XX3 (ts: 1003)  }
            // w2 = { 0:Y0 (ts: 0), 1:Y1 (ts: 0),
            //        0:YY0 (ts: 1100), 1:YY1 (ts: 1100), 2:YY2 (ts: 1100), 3:YY3 (ts: 1100),
            //        0:YYY0 (ts: 1101), 1:YYY1 (ts: 1101), 2:YYY2 (ts: 1101), 3:YYY3 (ts: 1101),
            //        0:YYYY0 (ts: 1102), 1:YYYY1 (ts: 1102), 2:YYYY2 (ts: 1102), 3:YYYY3 (ts: 1102),
            //        0:YYYYY0 (ts: 1103), 1:YYYYY1 (ts: 1103), 2:YYYYY2 (ts: 1103), 3:YYYYY3 (ts: 1103),
            //        0:YYYYYY0 (ts: 1104), 1:YYYYYY1 (ts: 1104), 2:YYYYYY2 (ts: 1104), 3:YYYYYY3 (ts: 1104),
            //        0:YYYYYYY0 (ts: 899), 1:YYYYYYY1 (ts: 899), 2:YYYYYYY2 (ts: 899), 3:YYYYYYY3 (ts: 899),
            //        0:YYYYYYYY0 (ts: 900), 1:YYYYYYYY1 (ts: 900), 2:YYYYYYYY2 (ts: 900), 3:YYYYYYYY3 (ts: 900),
            //        0:YYYYYYYYY0 (ts: 901), 1:YYYYYYYYY1 (ts: 901), 2:YYYYYYYYY2 (ts: 901), 3:YYYYYYYYY3 (ts: 901) }
            // --> w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0),
            //            0:XX0 (ts: 1000), 1:XX1 (ts: 1001), 2:XX2 (ts: 1002), 3:XX3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 0), 1:Y1 (ts: 0),
            //            0:YY0 (ts: 1100), 1:YY1 (ts: 1100), 2:YY2 (ts: 1100), 3:YY3 (ts: 1100),
            //            0:YYY0 (ts: 1101), 1:YYY1 (ts: 1101), 2:YYY2 (ts: 1101), 3:YYY3 (ts: 1101),
            //            0:YYYY0 (ts: 1102), 1:YYYY1 (ts: 1102), 2:YYYY2 (ts: 1102), 3:YYYY3 (ts: 1102),
            //            0:YYYYY0 (ts: 1103), 1:YYYYY1 (ts: 1103), 2:YYYYY2 (ts: 1103), 3:YYYYY3 (ts: 1103),
            //            0:YYYYYY0 (ts: 1104), 1:YYYYYY1 (ts: 1104), 2:YYYYYY2 (ts: 1104), 3:YYYYYY3 (ts: 1104),
            //            0:YYYYYYY0 (ts: 899), 1:YYYYYYY1 (ts: 899), 2:YYYYYYY2 (ts: 899), 3:YYYYYYY3 (ts: 899),
            //            0:YYYYYYYY0 (ts: 900), 1:YYYYYYYY1 (ts: 900), 2:YYYYYYYY2 (ts: 900), 3:YYYYYYYY3 (ts: 900)
            //            0:YYYYYYYYY0 (ts: 901), 1:YYYYYYYYY1 (ts: 901), 2:YYYYYYYYY2 (ts: 901), 3:YYYYYYYYY3 (ts: 901),
            //            0:YYYYYYYYYY0 (ts: 902), 1:YYYYYYYYYY1 (ts: 902), 2:YYYYYYYYYY2 (ts: 902), 3:YYYYYYYYYY3 (ts: 902) }
            time += 1;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YYYYYYYYYY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:XX0+YYYYYYYYYY0 (ts: 1000)", "1:XX1+YYYYYYYYYY1 (ts: 1001)", "2:XX2+YYYYYYYYYY2 (ts: 1002)");

            // push four items to the other stream with with incremented timestamp; this should produce four items
            // w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0),
            //        0:XX0 (ts: 1000), 1:XX1 (ts: 1001), 2:XX2 (ts: 1002), 3:XX3 (ts: 1003)  }
            // w2 = { 0:Y0 (ts: 0), 1:Y1 (ts: 0),
            //        0:YY0 (ts: 1100), 1:YY1 (ts: 1100), 2:YY2 (ts: 1100), 3:YY3 (ts: 1100),
            //        0:YYY0 (ts: 1101), 1:YYY1 (ts: 1101), 2:YYY2 (ts: 1101), 3:YYY3 (ts: 1101),
            //        0:YYYY0 (ts: 1102), 1:YYYY1 (ts: 1102), 2:YYYY2 (ts: 1102), 3:YYYY3 (ts: 1102),
            //        0:YYYYY0 (ts: 1103), 1:YYYYY1 (ts: 1103), 2:YYYYY2 (ts: 1103), 3:YYYYY3 (ts: 1103),
            //        0:YYYYYY0 (ts: 1104), 1:YYYYYY1 (ts: 1104), 2:YYYYYY2 (ts: 1104), 3:YYYYYY3 (ts: 1104),
            //        0:YYYYYYY0 (ts: 899), 1:YYYYYYY1 (ts: 899), 2:YYYYYYY2 (ts: 899), 3:YYYYYYY3 (ts: 899),
            //        0:YYYYYYYY0 (ts: 900), 1:YYYYYYYY1 (ts: 900), 2:YYYYYYYY2 (ts: 900), 3:YYYYYYYY3 (ts: 900),
            //        0:YYYYYYYYY0 (ts: 901), 1:YYYYYYYYY1 (ts: 901), 2:YYYYYYYYY2 (ts: 901), 3:YYYYYYYYY3 (ts: 901),
            //        0:YYYYYYYYYY0 (ts: 902), 1:YYYYYYYYYY1 (ts: 902), 2:YYYYYYYYYY2 (ts: 902), 3:YYYYYYYYYY3 (ts: 902) }
            // --> w1 = { 0:X0 (ts: 0), 1:X1 (ts: 0),
            //            0:XX0 (ts: 1000), 1:XX1 (ts: 1001), 2:XX2 (ts: 1002), 3:XX3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 0), 1:Y1 (ts: 0),
            //            0:YY0 (ts: 1100), 1:YY1 (ts: 1100), 2:YY2 (ts: 1100), 3:YY3 (ts: 1100),
            //            0:YYY0 (ts: 1101), 1:YYY1 (ts: 1101), 2:YYY2 (ts: 1101), 3:YYY3 (ts: 1101),
            //            0:YYYY0 (ts: 1102), 1:YYYY1 (ts: 1102), 2:YYYY2 (ts: 1102), 3:YYYY3 (ts: 1102),
            //            0:YYYYY0 (ts: 1103), 1:YYYYY1 (ts: 1103), 2:YYYYY2 (ts: 1103), 3:YYYYY3 (ts: 1103),
            //            0:YYYYYY0 (ts: 1104), 1:YYYYYY1 (ts: 1104), 2:YYYYYY2 (ts: 1104), 3:YYYYYY3 (ts: 1104),
            //            0:YYYYYYY0 (ts: 899), 1:YYYYYYY1 (ts: 899), 2:YYYYYYY2 (ts: 899), 3:YYYYYYY3 (ts: 899),
            //            0:YYYYYYYY0 (ts: 900), 1:YYYYYYYY1 (ts: 900), 2:YYYYYYYY2 (ts: 900), 3:YYYYYYYY3 (ts: 900)
            //            0:YYYYYYYYY0 (ts: 901), 1:YYYYYYYYY1 (ts: 901), 2:YYYYYYYYY2 (ts: 901), 3:YYYYYYYYY3 (ts: 901),
            //            0:YYYYYYYYYY0 (ts: 902), 1:YYYYYYYYYY1 (ts: 902), 2:YYYYYYYYYY2 (ts: 902), 3:YYYYYYYYYY3 (ts: 902),
            //            0:YYYYYYYYYYY0 (ts: 903), 1:YYYYYYYYYYY1 (ts: 903), 2:YYYYYYYYYYY2 (ts: 903), 3:YYYYYYYYYYY3 (ts: 903) }
            time += 1;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YYYYYYYYYYY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:XX0+YYYYYYYYYYY0 (ts: 1000)", "1:XX1+YYYYYYYYYYY1 (ts: 1001)", "2:XX2+YYYYYYYYYYY2 (ts: 1002)", "3:XX3+YYYYYYYYYYY3 (ts: 1003)");

            // advance time to not join with existing data
            // we omit above exiting data, even if it's still in the window
            //
            // push four items with increasing timestamps to the other stream. the primary window is empty; this should produce no items
            // w1 = {}
            // w2 = {}
            // --> w1 = {}
            //     w2 = { 0:B0 (ts: 2000), 1:B1 (ts: 2001), 2:B2 (ts: 2002), 3:B3 (ts: 2003) }
            time = 2000L;
            for (int i = 0; i < expectedKeys.length; i++) {
                driver.pipeInput(recordFactory.create(topic2, expectedKeys[i], "B" + expectedKeys[i], time + i));
            }
            processor.checkAndClearProcessResult();

            // push four items with larger timestamps to the primary stream; this should produce four items
            // w1 = {}
            // w2 = { 0:B0 (ts: 2000), 1:B1 (ts: 2001), 2:B2 (ts: 2002), 3:B3 (ts: 2003) }
            // --> w1 = { 0:A0 (ts: 2100), 1:A1 (ts: 2100), 2:A2 (ts: 2100), 3:A3 (ts: 2100) }
            //     w2 = { 0:B0 (ts: 2000), 1:B1 (ts: 2001), 2:B2 (ts: 2002), 3:B3 (ts: 2003) }
            time = 2000L + 100L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "A" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:A0+B0 (ts: 2100)", "1:A1+B1 (ts: 2100)", "2:A2+B2 (ts: 2100)", "3:A3+B3 (ts: 2100)");

            // push four items with increase timestamps to the primary stream; this should produce three items
            // w1 = { 0:A0 (ts: 2100), 1:A1 (ts: 2100), 2:A2 (ts: 2100), 3:A3 (ts: 2100) }
            // w2 = { 0:B0 (ts: 2000), 1:B1 (ts: 2001), 2:B2 (ts: 2002), 3:B3 (ts: 2003) }
            // --> w1 = { 0:A0 (ts: 2100), 1:A1 (ts: 2100), 2:A2 (ts: 2100), 3:A3 (ts: 2100),
            //            0:AA0 (ts: 2101), 1:AA1 (ts: 2101), 2:AA2 (ts: 2101), 3:AA3 (ts: 2101) }
            //     w2 = { 0:B0 (ts: 2000), 1:B1 (ts: 2001), 2:B2 (ts: 2002), 3:B3 (ts: 2003) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "AA" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("1:AA1+B1 (ts: 2101)", "2:AA2+B2 (ts: 2101)", "3:AA3+B3 (ts: 2101)");

            // push four items with increase timestamps to the primary stream; this should produce two items
            // w1 = { 0:A0 (ts: 2100), 1:A1 (ts: 2100), 2:A2 (ts: 2100), 3:A3 (ts: 2100),
            //        0:AA0 (ts: 2101), 1:AA1 (ts: 2101), 2:AA2 (ts: 2101), 3:AA3 (ts: 2101) }
            // w2 = { 0:B0 (ts: 2000), 1:B1 (ts: 2001), 2:B2 (ts: 2002), 3:B3 (ts: 2003) }
            // --> w1 = { 0:A0 (ts: 2100), 1:A1 (ts: 2100), 2:A2 (ts: 2100), 3:A3 (ts: 2100),
            //            0:AA0 (ts: 2101), 1:AA1 (ts: 2101), 2:AA2 (ts: 2101), 3:AA3 (ts: 2101),
            //            0:AAA0 (ts: 2102), 1:AAA1 (ts: 2102), 2:AAA2 (ts: 2102), 3:AAA3 (ts: 2102) }
            //     w2 = { 0:B0 (ts: 2000), 1:B1 (ts: 2001), 2:B2 (ts: 2002), 3:B3 (ts: 2003) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "AAA" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("2:AAA2+B2 (ts: 2102)", "3:AAA3+B3 (ts: 2102)");

            // push four items with increase timestamps to the primary stream; this should produce one item
            // w1 = { 0:A0 (ts: 2100), 1:A1 (ts: 2100), 2:A2 (ts: 2100), 3:A3 (ts: 2100),
            //        0:AA0 (ts: 2101), 1:AA1 (ts: 2101), 2:AA2 (ts: 2101), 3:AA3 (ts: 2101),
            //        0:AAA0 (ts: 2102), 1:AAA1 (ts: 2102), 2:AAA2 (ts: 2102), 3:AAA3 (ts: 2102) }
            // w2 = { 0:B0 (ts: 2000), 1:B1 (ts: 2001), 2:B2 (ts: 2002), 3:B3 (ts: 2003) }
            // --> w1 = { 0:A0 (ts: 2100), 1:A1 (ts: 2100), 2:A2 (ts: 2100), 3:A3 (ts: 2100),
            //            0:AA0 (ts: 2101), 1:AA1 (ts: 2101), 2:AA2 (ts: 2101), 3:AA3 (ts: 2101),
            //            0:AAA0 (ts: 2102), 1:AAA1 (ts: 2102), 2:AAA2 (ts: 2102), 3:AAA3 (ts: 2102),
            //            0:AAAA0 (ts: 2103), 1:AAAA1 (ts: 2103), 2:AAAA2 (ts: 2103), 3:AAAA3 (ts: 2103) }
            //     w2 = { 0:B0 (ts: 2000), 1:B1 (ts: 2001), 2:B2 (ts: 2002), 3:B3 (ts: 2003) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "AAAA" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("3:AAAA3+B3 (ts: 2103)");

            // push four items with increase timestamps (now out of window) to the primary stream; this should produce no items
            // w1 = { 0:A0 (ts: 2100), 1:A1 (ts: 2100), 2:A2 (ts: 2100), 3:A3 (ts: 2100),
            //        0:AA0 (ts: 2101), 1:AA1 (ts: 2101), 2:AA2 (ts: 2101), 3:AA3 (ts: 2101),
            //        0:AAA0 (ts: 2102), 1:AAA1 (ts: 2102), 2:AAA2 (ts: 2102), 3:AAA3 (ts: 2102),
            //        0:AAAA0 (ts: 2103), 1:AAAA1 (ts: 2103), 2:AAAA2 (ts: 2103), 3:AAAA3 (ts: 2103) }
            // w2 = { 0:B0 (ts: 2000), 1:B1 (ts: 2001), 2:B2 (ts: 2002), 3:B3 (ts: 2003) }
            // --> w1 = { 0:A0 (ts: 2100), 1:A1 (ts: 2100), 2:A2 (ts: 2100), 3:A3 (ts: 2100),
            //            0:AA0 (ts: 2101), 1:AA1 (ts: 2101), 2:AA2 (ts: 2101), 3:AA3 (ts: 2101),
            //            0:AAA0 (ts: 2102), 1:AAA1 (ts: 2102), 2:AAA2 (ts: 2102), 3:AAA3 (ts: 2102),
            //            0:AAAA0 (ts: 2103), 1:AAAA1 (ts: 2103), 2:AAAA2 (ts: 2103), 3:AAAA3 (ts: 2103),
            //            0:AAAAA0 (ts: 2104), 1:AAAAA1 (ts: 2104), 2:AAAAA2 (ts: 2104), 3:AAAAA3 (ts: 2104) }
            //     w2 = { 0:B0 (ts: 2000), 1:B1 (ts: 2001), 2:B2 (ts: 2002), 3:B3 (ts: 2003) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "AAAAA" + expectedKey, time));
            }
            processor.checkAndClearProcessResult();

            // push four items with smaller timestamps (before window) to the primary stream; this should produce no items
            // w1 = { 0:A0 (ts: 2100), 1:A1 (ts: 2100), 2:A2 (ts: 2100), 3:A3 (ts: 2100),
            //        0:AA0 (ts: 2101), 1:AA1 (ts: 2101), 2:AA2 (ts: 2101), 3:AA3 (ts: 2101),
            //        0:AAA0 (ts: 2102), 1:AAA1 (ts: 2102), 2:AAA2 (ts: 2102), 3:AAA3 (ts: 2102),
            //        0:AAAA0 (ts: 2103), 1:AAAA1 (ts: 2103), 2:AAAA2 (ts: 2103), 3:AAAA3 (ts: 2103),
            //        0:AAAAA0 (ts: 2104), 1:AAAAA1 (ts: 2104), 2:AAAAA2 (ts: 2104), 3:AAAAA3 (ts: 2104) }
            // w2 = { 0:B0 (ts: 2000), 1:B1 (ts: 2001), 2:B2 (ts: 2002), 3:B3 (ts: 2003) }
            // --> w1 = { 0:A0 (ts: 2100), 1:A1 (ts: 2100), 2:A2 (ts: 2100), 3:A3 (ts: 2100),
            //            0:AA0 (ts: 2101), 1:AA1 (ts: 2101), 2:AA2 (ts: 2101), 3:AA3 (ts: 2101),
            //            0:AAA0 (ts: 2102), 1:AAA1 (ts: 2102), 2:AAA2 (ts: 2102), 3:AAA3 (ts: 2102),
            //            0:AAAA0 (ts: 2103), 1:AAAA1 (ts: 2103), 2:AAAA2 (ts: 2103), 3:AAAA3 (ts: 2103),
            //            0:AAAAA0 (ts: 2104), 1:AAAAA1 (ts: 2104), 2:AAAAA2 (ts: 2104), 3:AAAAA3 (ts: 2104),
            //            0:AAAAAA0 (ts: 1899), 1:AAAAAA1 (ts: 1899), 2:AAAAAA2 (ts: 1899), 3:AAAAAA3 (ts: 1899) }
            //     w2 = { 0:B0 (ts: 2000), 1:B1 (ts: 2001), 2:B2 (ts: 2002), 3:B3 (ts: 2003) }
            time = 2000L - 100L - 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "AAAAAA" + expectedKey, time));
            }
            processor.checkAndClearProcessResult();

            // push four items with increased timestamps to the primary stream; this should produce one item
            // w1 = { 0:A0 (ts: 2100), 1:A1 (ts: 2100), 2:A2 (ts: 2100), 3:A3 (ts: 2100),
            //        0:AA0 (ts: 2101), 1:AA1 (ts: 2101), 2:AA2 (ts: 2101), 3:AA3 (ts: 2101),
            //        0:AAA0 (ts: 2102), 1:AAA1 (ts: 2102), 2:AAA2 (ts: 2102), 3:AAA3 (ts: 2102),
            //        0:AAAA0 (ts: 2103), 1:AAAA1 (ts: 2103), 2:AAAA2 (ts: 2103), 3:AAAA3 (ts: 2103),
            //        0:AAAAA0 (ts: 2104), 1:AAAAA1 (ts: 2104), 2:AAAAA2 (ts: 2104), 3:AAAAA3 (ts: 2104),
            //        0:AAAAAA0 (ts: 1899), 1:AAAAAA1 (ts: 1899), 2:AAAAAA2 (ts: 1899), 3:AAAAAA3 (ts: 1899) }
            // w2 = { 0:B0 (ts: 2000), 1:B1 (ts: 2001), 2:B2 (ts: 2002), 3:B3 (ts: 2003) }
            // --> w1 = { 0:A0 (ts: 2100), 1:A1 (ts: 2100), 2:A2 (ts: 2100), 3:A3 (ts: 2100),
            //            0:AA0 (ts: 2101), 1:AA1 (ts: 2101), 2:AA2 (ts: 2101), 3:AA3 (ts: 2101),
            //            0:AAA0 (ts: 2102), 1:AAA1 (ts: 2102), 2:AAA2 (ts: 2102), 3:AAA3 (ts: 2102),
            //            0:AAAA0 (ts: 2103), 1:AAAA1 (ts: 2103), 2:AAAA2 (ts: 2103), 3:AAAA3 (ts: 2103),
            //            0:AAAAA0 (ts: 2104), 1:AAAAA1 (ts: 2104), 2:AAAAA2 (ts: 2104), 3:AAAAA3 (ts: 2104),
            //            0:AAAAAA0 (ts: 1899), 1:AAAAAA1 (ts: 1899), 2:AAAAAA2 (ts: 1899), 3:AAAAAA3 (ts: 1899),
            //            0:AAAAAAA0 (ts: 1900), 1:AAAAAAA1 (ts: 1900), 2:AAAAAAA2 (ts: 1900), 3:AAAAAAA3 (ts: 1900) }
            //     w2 = { 0:B0 (ts: 2000), 1:B1 (ts: 2001), 2:B2 (ts: 2002), 3:B3 (ts: 2003) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "AAAAAAA" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:AAAAAAA0+B0 (ts: 2000)");

            // push four items with increased timestamps to the primary stream; this should produce two items
            // w1 = { 0:A0 (ts: 2100), 1:A1 (ts: 2100), 2:A2 (ts: 2100), 3:A3 (ts: 2100),
            //        0:AA0 (ts: 2101), 1:AA1 (ts: 2101), 2:AA2 (ts: 2101), 3:AA3 (ts: 2101),
            //        0:AAA0 (ts: 2102), 1:AAA1 (ts: 2102), 2:AAA2 (ts: 2102), 3:AAA3 (ts: 2102),
            //        0:AAAA0 (ts: 2103), 1:AAAA1 (ts: 2103), 2:AAAA2 (ts: 2103), 3:AAAA3 (ts: 2103),
            //        0:AAAAA0 (ts: 2104), 1:AAAAA1 (ts: 2104), 2:AAAAA2 (ts: 2104), 3:AAAAA3 (ts: 2104),
            //        0:AAAAAA0 (ts: 1899), 1:AAAAAA1 (ts: 1899), 2:AAAAAA2 (ts: 1899), 3:AAAAAA3 (ts: 1899),
            //        0:AAAAAAA0 (ts: 1900), 1:AAAAAAA1 (ts: 1900), 2:AAAAAAA2 (ts: 1900), 3:AAAAAAA3 (ts: 1900) }
            // w2 = { 0:B0 (ts: 2000), 1:B1 (ts: 2001), 2:B2 (ts: 2002), 3:B3 (ts: 2003) }
            // --> w1 = { 0:A0 (ts: 2100), 1:A1 (ts: 2100), 2:A2 (ts: 2100), 3:A3 (ts: 2100),
            //            0:AA0 (ts: 2101), 1:AA1 (ts: 2101), 2:AA2 (ts: 2101), 3:AA3 (ts: 2101),
            //            0:AAA0 (ts: 2102), 1:AAA1 (ts: 2102), 2:AAA2 (ts: 2102), 3:AAA3 (ts: 2102),
            //            0:AAAA0 (ts: 2103), 1:AAAA1 (ts: 2103), 2:AAAA2 (ts: 2103), 3:AAAA3 (ts: 2103),
            //            0:AAAAA0 (ts: 2104), 1:AAAAA1 (ts: 2104), 2:AAAAA2 (ts: 2104), 3:AAAAA3 (ts: 2104),
            //            0:AAAAAA0 (ts: 1899), 1:AAAAAA1 (ts: 1899), 2:AAAAAA2 (ts: 1899), 3:AAAAAA3 (ts: 1899),
            //            0:AAAAAAA0 (ts: 1900), 1:AAAAAAA1 (ts: 1900), 2:AAAAAAA2 (ts: 1900), 3:AAAAAAA3 (ts: 1900),
            //            0:AAAAAAAA0 (ts: 1901), 1:AAAAAAAA1 (ts: 1901), 2:AAAAAAAA2 (ts: 1901), 3:AAAAAAAA3 (ts: 1901) }
            //     w2 = { 0:B0 (ts: 2000), 1:B1 (ts: 2001), 2:B2 (ts: 2002), 3:B3 (ts: 2003) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "AAAAAAAA" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:AAAAAAAA0+B0 (ts: 2000)", "1:AAAAAAAA1+B1 (ts: 2001)");

            // push four items with increased timestamps to the primary stream; this should produce three items
            // w1 = { 0:A0 (ts: 2100), 1:A1 (ts: 2100), 2:A2 (ts: 2100), 3:A3 (ts: 2100),
            //        0:AA0 (ts: 2101), 1:AA1 (ts: 2101), 2:AA2 (ts: 2101), 3:AA3 (ts: 2101),
            //        0:AAA0 (ts: 2102), 1:AAA1 (ts: 2102), 2:AAA2 (ts: 2102), 3:AAA3 (ts: 2102),
            //        0:AAAA0 (ts: 2103), 1:AAAA1 (ts: 2103), 2:AAAA2 (ts: 2103), 3:AAAA3 (ts: 2103),
            //        0:AAAAA0 (ts: 2104), 1:AAAAA1 (ts: 2104), 2:AAAAA2 (ts: 2104), 3:AAAAA3 (ts: 2104),
            //        0:AAAAAA0 (ts: 1899), 1:AAAAAA1 (ts: 1899), 2:AAAAAA2 (ts: 1899), 3:AAAAAA3 (ts: 1899),
            //        0:AAAAAAA0 (ts: 1900), 1:AAAAAAA1 (ts: 1900), 2:AAAAAAA2 (ts: 1900), 3:AAAAAAA3 (ts: 1900),
            //        0:AAAAAAAA0 (ts: 1901), 1:AAAAAAAA1 (ts: 1901), 2:AAAAAAAA2 (ts: 1901), 3:AAAAAAAA3 (ts: 1901) }
            // w2 = { 0:B0 (ts: 2000), 1:B1 (ts: 2001), 2:B2 (ts: 2002), 3:B3 (ts: 2003) }
            // --> w1 = { 0:A0 (ts: 2100), 1:A1 (ts: 2100), 2:A2 (ts: 2100), 3:A3 (ts: 2100),
            //            0:AA0 (ts: 2101), 1:AA1 (ts: 2101), 2:AA2 (ts: 2101), 3:AA3 (ts: 2101),
            //            0:AAA0 (ts: 2102), 1:AAA1 (ts: 2102), 2:AAA2 (ts: 2102), 3:AAA3 (ts: 2102),
            //            0:AAAA0 (ts: 2103), 1:AAAA1 (ts: 2103), 2:AAAA2 (ts: 2103), 3:AAAA3 (ts: 2103),
            //            0:AAAAA0 (ts: 2104), 1:AAAAA1 (ts: 2104), 2:AAAAA2 (ts: 2104), 3:AAAAA3 (ts: 2104),
            //            0:AAAAAA0 (ts: 1899), 1:AAAAAA1 (ts: 1899), 2:AAAAAA2 (ts: 1899), 3:AAAAAA3 (ts: 1899),
            //            0:AAAAAAA0 (ts: 1900), 1:AAAAAAA1 (ts: 1900), 2:AAAAAAA2 (ts: 1900), 3:AAAAAAA3 (ts: 1900),
            //            0:AAAAAAAA0 (ts: 1901), 1:AAAAAAAA1 (ts: 1901), 2:AAAAAAAA2 (ts: 1901), 3:AAAAAAAA3 (ts: 1901),
            //            0:AAAAAAAAA0 (ts: 1902), 1:AAAAAAAAA1 (ts: 1902), 2:AAAAAAAAA2 (ts: 1902), 3:AAAAAAAAA3 (ts: 1902) }
            //     w2 = { 0:B0 (ts: 2000), 1:B1 (ts: 2001), 2:B2 (ts: 2002), 3:B3 (ts: 2003) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "AAAAAAAAA" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:AAAAAAAAA0+B0 (ts: 2000)", "1:AAAAAAAAA1+B1 (ts: 2001)", "2:AAAAAAAAA2+B2 (ts: 2002)");

            // push four items with increased timestamps to the primary stream; this should produce four items
            // w1 = { 0:A0 (ts: 2100), 1:A1 (ts: 2100), 2:A2 (ts: 2100), 3:A3 (ts: 2100),
            //        0:AA0 (ts: 2101), 1:AA1 (ts: 2101), 2:AA2 (ts: 2101), 3:AA3 (ts: 2101),
            //        0:AAA0 (ts: 2102), 1:AAA1 (ts: 2102), 2:AAA2 (ts: 2102), 3:AAA3 (ts: 2102),
            //        0:AAAA0 (ts: 2103), 1:AAAA1 (ts: 2103), 2:AAAA2 (ts: 2103), 3:AAAA3 (ts: 2103),
            //        0:AAAAA0 (ts: 2104), 1:AAAAA1 (ts: 2104), 2:AAAAA2 (ts: 2104), 3:AAAAA3 (ts: 2104),
            //        0:AAAAAA0 (ts: 1899), 1:AAAAAA1 (ts: 1899), 2:AAAAAA2 (ts: 1899), 3:AAAAAA3 (ts: 1899),
            //        0:AAAAAAA0 (ts: 1900), 1:AAAAAAA1 (ts: 1900), 2:AAAAAAA2 (ts: 1900), 3:AAAAAAA3 (ts: 1900),
            //        0:AAAAAAAA0 (ts: 1901), 1:AAAAAAAA1 (ts: 1901), 2:AAAAAAAA2 (ts: 1901), 3:AAAAAAAA3 (ts: 1901),
            //        0:AAAAAAAAA0 (ts: 1902), 1:AAAAAAAAA1 (ts: 1902), 2:AAAAAAAAA2 (ts: 1902), 3:AAAAAAAAA3 (ts: 1902) }
            // w2 = { 0:B0 (ts: 2000), 1:B1 (ts: 2001), 2:B2 (ts: 2002), 3:B3 (ts: 2003) }
            // --> w1 = { 0:A0 (ts: 2100), 1:A1 (ts: 2100), 2:A2 (ts: 2100), 3:A3 (ts: 2100),
            //            0:AA0 (ts: 2101), 1:AA1 (ts: 2101), 2:AA2 (ts: 2101), 3:AA3 (ts: 2101),
            //            0:AAA0 (ts: 2102), 1:AAA1 (ts: 2102), 2:AAA2 (ts: 2102), 3:AAA3 (ts: 2102),
            //            0:AAAA0 (ts: 2103), 1:AAAA1 (ts: 2103), 2:AAAA2 (ts: 2103), 3:AAAA3 (ts: 2103),
            //            0:AAAAA0 (ts: 2104), 1:AAAAA1 (ts: 2104), 2:AAAAA2 (ts: 2104), 3:AAAAA3 (ts: 2104),
            //            0:AAAAAA0 (ts: 1899), 1:AAAAAA1 (ts: 1899), 2:AAAAAA2 (ts: 1899), 3:AAAAAA3 (ts: 1899),
            //            0:AAAAAAA0 (ts: 1900), 1:AAAAAAA1 (ts: 1900), 2:AAAAAAA2 (ts: 1900), 3:AAAAAAA3 (ts: 1900),
            //            0:AAAAAAAA0 (ts: 1901), 1:AAAAAAAA1 (ts: 1901), 2:AAAAAAAA2 (ts: 1901), 3:AAAAAAAA3 (ts: 1901),
            //            0:AAAAAAAAA0 (ts: 1902), 1:AAAAAAAAA1 (ts: 1902), 2:AAAAAAAAA2 (ts: 1902), 3:AAAAAAAAA3 (ts: 1902),
            //            0:AAAAAAAAAA0 (ts: 1903), 1:AAAAAAAAAA1 (ts: 1903), 2:AAAAAAAAAA2 (ts: 1903), 3:AAAAAAAAAA3 (ts: 1903) }
            //     w2 = { 0:B0 (ts: 2000), 1:B1 (ts: 2001), 2:B2 (ts: 2002), 3:B3 (ts: 2003) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "AAAAAAAAAA" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:AAAAAAAAAA0+B0 (ts: 2000)", "1:AAAAAAAAAA1+B1 (ts: 2001)", "2:AAAAAAAAAA2+B2 (ts: 2002)", "3:AAAAAAAAAA3+B3 (ts: 2003)");
        }
    }

    @Test
    public void testAsymmetricWindowingAfter() {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockProcessorSupplier<Integer, String> supplier = new MockProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);

        joined = stream1.join(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.of(ofMillis(0)).after(ofMillis(100)),
            Joined.with(Serdes.Integer(),
                Serdes.String(),
                Serdes.String()));
        joined.process(supplier);

        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final MockProcessor<Integer, String> processor = supplier.theCapturedProcessor();
            long time = 1000L;

            // push four items with increasing timestamps to the primary stream; the other window is empty; this should produce no items
            // w1 = {}
            // w2 = {}
            // --> w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            //     w2 = {}
            for (int i = 0; i < expectedKeys.length; i++) {
                driver.pipeInput(recordFactory.create(topic1, expectedKeys[i], "X" + expectedKeys[i], time + i));
            }
            processor.checkAndClearProcessResult();

            // push four items smaller timestamps (out of window) to the secondary stream; this should produce no items
            // w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            // w2 = {}
            // --> w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 999), 1:Y1 (ts: 999), 2:Y2 (ts: 999), 3:Y3 (ts: 999) }
            time = 1000L - 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult();

            // push four items with increased timestamps to the secondary stream; this should produce one item
            // w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            // w2 = { 0:X0 (ts: 999), 1:X1 (ts: 999), 2:X2 (ts: 999), 3:X3 (ts: 999) }
            // --> w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 999), 1:Y1 (ts: 999), 2:Y2 (ts: 999), 3:Y3 (ts: 999),
            //            0:YY0 (ts: 1000), 1:YY1 (ts: 1000), 2:YY2 (ts: 1000), 3:YY3 (ts: 1000) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:X0+YY0 (ts: 1000)");

            // push four items with increased timestamps to the secondary stream; this should produce two items
            // w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            // w2 = { 0:X0 (ts: 999), 1:X1 (ts: 999), 2:X2 (ts: 999), 3:X3 (ts: 999),
            //        0:YY0 (ts: 1000), 1:YY1 (ts: 1000), 2:YY2 (ts: 1000), 3:YY3 (ts: 1000) }
            // --> w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 999), 1:Y1 (ts: 999), 2:Y2 (ts: 999), 3:Y3 (ts: 999),
            //            0:YY0 (ts: 1000), 1:YY1 (ts: 1000), 2:YY2 (ts: 1000), 3:YY3 (ts: 1000),
            //            0:YYY0 (ts: 1001), 1:YYY1 (ts: 1001), 2:YYY2 (ts: 1001), 3:YYY3 (ts: 1001) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:X0+YY0 (ts: 1001)", "1:X1+YY1 (ts: 1001)");

            // push four items with increased timestamps to the secondary stream; this should produce three items
            // w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            // w2 = { 0:X0 (ts: 999), 1:X1 (ts: 999), 2:X2 (ts: 999), 3:X3 (ts: 999),
            //        0:YY0 (ts: 1000), 1:YY1 (ts: 1000), 2:YY2 (ts: 1000), 3:YY3 (ts: 1000),
            //        0:YYY0 (ts: 1001), 1:YYY1 (ts: 1001), 2:YYY2 (ts: 1001), 3:YYY3 (ts: 1001) }
            // --> w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 999), 1:Y1 (ts: 999), 2:Y2 (ts: 999), 3:Y3 (ts: 999),
            //            0:YY0 (ts: 1000), 1:YY1 (ts: 1000), 2:YY2 (ts: 1000), 3:YY3 (ts: 1000),
            //            0:YYY0 (ts: 1001), 1:YYY1 (ts: 1001), 2:YYY2 (ts: 1001), 3:YYY3 (ts: 1001),
            //            0:YYYY0 (ts: 1002), 1:YYYY1 (ts: 1002), 2:YYYY2 (ts: 1002), 3:YYYY3 (ts: 1002) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:X0+YY0 (ts: 1002)", "1:X1+YY1 (ts: 1002)", "2:X2+YY2 (ts: 1002)");

            // push four items with increased timestamps to the secondary stream; this should produce four items
            // w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            // w2 = { 0:X0 (ts: 999), 1:X1 (ts: 999), 2:X2 (ts: 999), 3:X3 (ts: 999),
            //        0:YY0 (ts: 1000), 1:YY1 (ts: 1000), 2:YY2 (ts: 1000), 3:YY3 (ts: 1000),
            //        0:YYY0 (ts: 1001), 1:YYY1 (ts: 1001), 2:YYY2 (ts: 1001), 3:YYY3 (ts: 1001),
            //        0:YYYY0 (ts: 1002), 1:YYYY1 (ts: 1002), 2:YYYY2 (ts: 1002), 3:YYYY3 (ts: 1002) }
            // --> w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 999), 1:Y1 (ts: 999), 2:Y2 (ts: 999), 3:Y3 (ts: 999),
            //            0:YY0 (ts: 1000), 1:YY1 (ts: 1000), 2:YY2 (ts: 1000), 3:YY3 (ts: 1000),
            //            0:YYY0 (ts: 1001), 1:YYY1 (ts: 1001), 2:YYY2 (ts: 1001), 3:YYY3 (ts: 1001),
            //            0:YYYY0 (ts: 1002), 1:YYYY1 (ts: 1002), 2:YYYY2 (ts: 1002), 3:YYYY3 (ts: 1002),
            //            0:YYYYY0 (ts: 1003), 1:YYYYY1 (ts: 1003), 2:YYYYY2 (ts: 1003), 3:YYYYY3 (ts: 1003) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:X0+YY0 (ts: 1003)", "1:X1+YY1 (ts: 1003)", "2:X2+YY2 (ts: 1003)", "3:X3+YY3 (ts: 1003)");

            // push four items with larger timestamps to the secondary stream; this should produce four items
            // w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            // w2 = { 0:X0 (ts: 999), 1:X1 (ts: 999), 2:X2 (ts: 999), 3:X3 (ts: 999),
            //        0:YY0 (ts: 1000), 1:YY1 (ts: 1000), 2:YY2 (ts: 1000), 3:YY3 (ts: 1000),
            //        0:YYY0 (ts: 1001), 1:YYY1 (ts: 1001), 2:YYY2 (ts: 1001), 3:YYY3 (ts: 1001),
            //        0:YYYY0 (ts: 1002), 1:YYYY1 (ts: 1002), 2:YYYY2 (ts: 1002), 3:YYYY3 (ts: 1002),
            //        0:YYYYY0 (ts: 1003), 1:YYYYY1 (ts: 1003), 2:YYYYY2 (ts: 1003), 3:YYYYY3 (ts: 1003) }
            // --> w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 999), 1:Y1 (ts: 999), 2:Y2 (ts: 999), 3:Y3 (ts: 999),
            //            0:YY0 (ts: 1000), 1:YY1 (ts: 1000), 2:YY2 (ts: 1000), 3:YY3 (ts: 1000),
            //            0:YYY0 (ts: 1001), 1:YYY1 (ts: 1001), 2:YYY2 (ts: 1001), 3:YYY3 (ts: 1001),
            //            0:YYYY0 (ts: 1002), 1:YYYY1 (ts: 1002), 2:YYYY2 (ts: 1002), 3:YYYY3 (ts: 1002),
            //            0:YYYYY0 (ts: 1003), 1:YYYYY1 (ts: 1003), 2:YYYYY2 (ts: 1003), 3:YYYYY3 (ts: 1003),
            //            0:YYYYYY0 (ts: 1100), 1:YYYYYY1 (ts: 1100), 2:YYYYYY2 (ts: 1100), 3:YYYYYY3 (ts: 1100) }
            time = 1000 + 100L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:X0+YY0 (ts: 1100)", "1:X1+YY1 (ts: 1100)", "2:X2+YY2 (ts: 1100)", "3:X3+YY3 (ts: 1100)");

            // push four items with increased timestamps to the secondary stream; this should produce three items
            // w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            // w2 = { 0:X0 (ts: 999), 1:X1 (ts: 999), 2:X2 (ts: 999), 3:X3 (ts: 999),
            //        0:YY0 (ts: 1000), 1:YY1 (ts: 1000), 2:YY2 (ts: 1000), 3:YY3 (ts: 1000),
            //        0:YYY0 (ts: 1001), 1:YYY1 (ts: 1001), 2:YYY2 (ts: 1001), 3:YYY3 (ts: 1001),
            //        0:YYYY0 (ts: 1002), 1:YYYY1 (ts: 1002), 2:YYYY2 (ts: 1002), 3:YYYY3 (ts: 1002),
            //        0:YYYYY0 (ts: 1003), 1:YYYYY1 (ts: 1003), 2:YYYYY2 (ts: 1003), 3:YYYYY3 (ts: 1003),
            //        0:YYYYYY0 (ts: 1100), 1:YYYYYY1 (ts: 1100), 2:YYYYYY2 (ts: 1100), 3:YYYYYY3 (ts: 1100) }
            // --> w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 999), 1:Y1 (ts: 999), 2:Y2 (ts: 999), 3:Y3 (ts: 999),
            //            0:YY0 (ts: 1000), 1:YY1 (ts: 1000), 2:YY2 (ts: 1000), 3:YY3 (ts: 1000),
            //            0:YYY0 (ts: 1001), 1:YYY1 (ts: 1001), 2:YYY2 (ts: 1001), 3:YYY3 (ts: 1001),
            //            0:YYYY0 (ts: 1002), 1:YYYY1 (ts: 1002), 2:YYYY2 (ts: 1002), 3:YYYY3 (ts: 1002),
            //            0:YYYYY0 (ts: 1003), 1:YYYYY1 (ts: 1003), 2:YYYYY2 (ts: 1003), 3:YYYYY3 (ts: 1003),
            //            0:YYYYYY0 (ts: 1100), 1:YYYYYY1 (ts: 1100), 2:YYYYYY2 (ts: 1100), 3:YYYYYY3 (ts: 1100),
            //            0:YYYYYYY0 (ts: 1101), 1:YYYYYYY1 (ts: 1101), 2:YYYYYYY2 (ts: 1101), 3:YYYYYYY3 (ts: 1101) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("1:X1+YY1 (ts: 1101)", "2:X2+YY2 (ts: 1101)", "3:X3+YY3 (ts: 1101)");

            // push four items with increased timestamps to the secondary stream; this should produce two items
            // w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            // w2 = { 0:X0 (ts: 999), 1:X1 (ts: 999), 2:X2 (ts: 999), 3:X3 (ts: 999),
            //        0:YY0 (ts: 1000), 1:YY1 (ts: 1000), 2:YY2 (ts: 1000), 3:YY3 (ts: 1000),
            //        0:YYY0 (ts: 1001), 1:YYY1 (ts: 1001), 2:YYY2 (ts: 1001), 3:YYY3 (ts: 1001),
            //        0:YYYY0 (ts: 1002), 1:YYYY1 (ts: 1002), 2:YYYY2 (ts: 1002), 3:YYYY3 (ts: 1002),
            //        0:YYYYY0 (ts: 1003), 1:YYYYY1 (ts: 1003), 2:YYYYY2 (ts: 1003), 3:YYYYY3 (ts: 1003),
            //        0:YYYYYY0 (ts: 1100), 1:YYYYYY1 (ts: 1100), 2:YYYYYY2 (ts: 1100), 3:YYYYYY3 (ts: 1100),
            //        0:YYYYYYY0 (ts: 1101), 1:YYYYYYY1 (ts: 1101), 2:YYYYYYY2 (ts: 1101), 3:YYYYYYY3 (ts: 1101) }
            // --> w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 999), 1:Y1 (ts: 999), 2:Y2 (ts: 999), 3:Y3 (ts: 999),
            //            0:YY0 (ts: 1000), 1:YY1 (ts: 1000), 2:YY2 (ts: 1000), 3:YY3 (ts: 1000),
            //            0:YYY0 (ts: 1001), 1:YYY1 (ts: 1001), 2:YYY2 (ts: 1001), 3:YYY3 (ts: 1001),
            //            0:YYYY0 (ts: 1002), 1:YYYY1 (ts: 1002), 2:YYYY2 (ts: 1002), 3:YYYY3 (ts: 1002),
            //            0:YYYYY0 (ts: 1003), 1:YYYYY1 (ts: 1003), 2:YYYYY2 (ts: 1003), 3:YYYYY3 (ts: 1003),
            //            0:YYYYYY0 (ts: 1100), 1:YYYYYY1 (ts: 1100), 2:YYYYYY2 (ts: 1100), 3:YYYYYY3 (ts: 1100),
            //            0:YYYYYYY0 (ts: 1101), 1:YYYYYYY1 (ts: 1101), 2:YYYYYYY2 (ts: 1101), 3:YYYYYYY3 (ts: 1101),
            //            0:YYYYYYYY0 (ts: 1102), 1:YYYYYYYY1 (ts: 1102), 2:YYYYYYYY2 (ts: 1102), 3:YYYYYYYY3 (ts: 1102) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("2:X2+YY2 (ts: 1102)", "3:X3+YY3 (ts: 1102)");

            // push four items with increased timestamps to the secondary stream; this should produce one item
            // w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            // w2 = { 0:X0 (ts: 999), 1:X1 (ts: 999), 2:X2 (ts: 999), 3:X3 (ts: 999),
            //        0:YY0 (ts: 1000), 1:YY1 (ts: 1000), 2:YY2 (ts: 1000), 3:YY3 (ts: 1000),
            //        0:YYY0 (ts: 1001), 1:YYY1 (ts: 1001), 2:YYY2 (ts: 1001), 3:YYY3 (ts: 1001),
            //        0:YYYY0 (ts: 1002), 1:YYYY1 (ts: 1002), 2:YYYY2 (ts: 1002), 3:YYYY3 (ts: 1002),
            //        0:YYYYY0 (ts: 1003), 1:YYYYY1 (ts: 1003), 2:YYYYY2 (ts: 1003), 3:YYYYY3 (ts: 1003),
            //        0:YYYYYY0 (ts: 1100), 1:YYYYYY1 (ts: 1100), 2:YYYYYY2 (ts: 1100), 3:YYYYYY3 (ts: 1100),
            //        0:YYYYYYY0 (ts: 1101), 1:YYYYYYY1 (ts: 1101), 2:YYYYYYY2 (ts: 1101), 3:YYYYYYY3 (ts: 1101),
            //        0:YYYYYYYY0 (ts: 1102), 1:YYYYYYYY1 (ts: 1102), 2:YYYYYYYY2 (ts: 1102), 3:YYYYYYYY3 (ts: 1102) }
            // --> w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 999), 1:Y1 (ts: 999), 2:Y2 (ts: 999), 3:Y3 (ts: 999),
            //            0:YY0 (ts: 1000), 1:YY1 (ts: 1000), 2:YY2 (ts: 1000), 3:YY3 (ts: 1000),
            //            0:YYY0 (ts: 1001), 1:YYY1 (ts: 1001), 2:YYY2 (ts: 1001), 3:YYY3 (ts: 1001),
            //            0:YYYY0 (ts: 1002), 1:YYYY1 (ts: 1002), 2:YYYY2 (ts: 1002), 3:YYYY3 (ts: 1002),
            //            0:YYYYY0 (ts: 1003), 1:YYYYY1 (ts: 1003), 2:YYYYY2 (ts: 1003), 3:YYYYY3 (ts: 1003),
            //            0:YYYYYY0 (ts: 1100), 1:YYYYYY1 (ts: 1100), 2:YYYYYY2 (ts: 1100), 3:YYYYYY3 (ts: 1100),
            //            0:YYYYYYY0 (ts: 1101), 1:YYYYYYY1 (ts: 1101), 2:YYYYYYY2 (ts: 1101), 3:YYYYYYY3 (ts: 1101),
            //            0:YYYYYYYY0 (ts: 1102), 1:YYYYYYYY1 (ts: 1102), 2:YYYYYYYY2 (ts: 1102), 3:YYYYYYYY3 (ts: 1102),
            //            0:YYYYYYYYY0 (ts: 1103), 1:YYYYYYYYY1 (ts: 1103), 2:YYYYYYYYY2 (ts: 1103), 3:YYYYYYYYY3 (ts: 1103) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("3:X3+YY3 (ts: 1103)");

            // push four items with increased timestamps (no out of window) to the secondary stream; this should produce no items
            // w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            // w2 = { 0:X0 (ts: 999), 1:X1 (ts: 999), 2:X2 (ts: 999), 3:X3 (ts: 999),
            //        0:YY0 (ts: 1000), 1:YY1 (ts: 1000), 2:YY2 (ts: 1000), 3:YY3 (ts: 1000),
            //        0:YYY0 (ts: 1001), 1:YYY1 (ts: 1001), 2:YYY2 (ts: 1001), 3:YYY3 (ts: 1001),
            //        0:YYYY0 (ts: 1002), 1:YYYY1 (ts: 1002), 2:YYYY2 (ts: 1002), 3:YYYY3 (ts: 1002),
            //        0:YYYYY0 (ts: 1003), 1:YYYYY1 (ts: 1003), 2:YYYYY2 (ts: 1003), 3:YYYYY3 (ts: 1003),
            //        0:YYYYYY0 (ts: 1100), 1:YYYYYY1 (ts: 1100), 2:YYYYYY2 (ts: 1100), 3:YYYYYY3 (ts: 1100),
            //        0:YYYYYYY0 (ts: 1101), 1:YYYYYYY1 (ts: 1101), 2:YYYYYYY2 (ts: 1101), 3:YYYYYYY3 (ts: 1101),
            //        0:YYYYYYYY0 (ts: 1102), 1:YYYYYYYY1 (ts: 1102), 2:YYYYYYYY2 (ts: 1102), 3:YYYYYYYY3 (ts: 1102),
            //        0:YYYYYYYYY0 (ts: 1103), 1:YYYYYYYYY1 (ts: 1103), 2:YYYYYYYYY2 (ts: 1103), 3:YYYYYYYYY3 (ts: 1103) }
            // --> w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 999), 1:Y1 (ts: 999), 2:Y2 (ts: 999), 3:Y3 (ts: 999),
            //            0:YY0 (ts: 1000), 1:YY1 (ts: 1000), 2:YY2 (ts: 1000), 3:YY3 (ts: 1000),
            //            0:YYY0 (ts: 1001), 1:YYY1 (ts: 1001), 2:YYY2 (ts: 1001), 3:YYY3 (ts: 1001),
            //            0:YYYY0 (ts: 1002), 1:YYYY1 (ts: 1002), 2:YYYY2 (ts: 1002), 3:YYYY3 (ts: 1002),
            //            0:YYYYY0 (ts: 1003), 1:YYYYY1 (ts: 1003), 2:YYYYY2 (ts: 1003), 3:YYYYY3 (ts: 1003),
            //            0:YYYYYY0 (ts: 1100), 1:YYYYYY1 (ts: 1100), 2:YYYYYY2 (ts: 1100), 3:YYYYYY3 (ts: 1100),
            //            0:YYYYYYY0 (ts: 1101), 1:YYYYYYY1 (ts: 1101), 2:YYYYYYY2 (ts: 1101), 3:YYYYYYY3 (ts: 1101),
            //            0:YYYYYYYY0 (ts: 1102), 1:YYYYYYYY1 (ts: 1102), 2:YYYYYYYY2 (ts: 1102), 3:YYYYYYYY3 (ts: 1102),
            //            0:YYYYYYYYY0 (ts: 1103), 1:YYYYYYYYY1 (ts: 1103), 2:YYYYYYYYY2 (ts: 1103), 3:YYYYYYYYY3 (ts: 1103),
            //            0:YYYYYYYYYY0 (ts: 1104), 1:YYYYYYYYYY1 (ts: 1104), 2:YYYYYYYYYY2 (ts: 1104), 3:YYYYYYYYYY3 (ts: 1104) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult();
        }
    }

    @Test
    public void testAsymmetricWindowingBefore() {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockProcessorSupplier<Integer, String> supplier = new MockProcessorSupplier<>();

        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);

        joined = stream1.join(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.of(ofMillis(0)).before(ofMillis(100)),
            Joined.with(Serdes.Integer(), Serdes.String(), Serdes.String()));
        joined.process(supplier);

        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final MockProcessor<Integer, String> processor = supplier.theCapturedProcessor();
            long time = 1000L;

            // push four items with increasing timestamps to the primary stream; the other window is empty; this should produce no items
            // w1 = {}
            // w2 = {}
            // --> w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            //     w2 = {}
            for (int i = 0; i < expectedKeys.length; i++) {
                driver.pipeInput(recordFactory.create(topic1, expectedKeys[i], "X" + expectedKeys[i], time + i));
            }
            processor.checkAndClearProcessResult();

            // push four items with smaller timestamps (before the window) to the other stream; this should produce no items
            // w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            // w2 = {}
            // --> w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 899), 1:Y1 (ts: 899), 2:Y2 (ts: 899), 3:Y3 (ts: 899) }
            time = 1000L - 100L - 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "Y" + expectedKey, time));
            }
            processor.checkAndClearProcessResult();

            // push four items with increased timestamp to the other stream; this should produce one item
            // w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003),
            // w2 = { 0:Y0 (ts: 899), 1:Y1 (ts: 899), 2:Y2 (ts: 899), 3:Y3 (ts: 899),
            // --> w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 899), 1:Y1 (ts: 899), 2:Y2 (ts: 899), 3:Y3 (ts: 899),
            //            0:YY0 (ts: 900), 1:YY1 (ts: 900), 2:YY2 (ts: 900), 3:YY3 (ts: 900) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:X0+YY0 (ts: 1000)");

            // push four items with increased timestamp to the other stream; this should produce two items
            // w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003),
            // w2 = { 0:Y0 (ts: 899), 1:Y1 (ts: 899), 2:Y2 (ts: 899), 3:Y3 (ts: 899),
            //        0:YY0 (ts: 900), 1:YY1 (ts: 900), 2:YY2 (ts: 900), 3:YY3 (ts: 900) }
            // --> w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 899), 1:Y1 (ts: 899), 2:Y2 (ts: 899), 3:Y3 (ts: 899),
            //            0:YY0 (ts: 900), 1:YY1 (ts: 900), 2:YY2 (ts: 900), 3:YY3 (ts: 900),
            //            0:YYY0 (ts: 901), 1:YYY1 (ts: 901), 2:YYY2 (ts: 901), 3:YYY3 (ts: 901) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YYY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:X0+YYY0 (ts: 1000)", "1:X1+YYY1 (ts: 1001)");

            // push four items with increased timestamp to the other stream; this should produce three items
            // w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003),
            // w2 = { 0:Y0 (ts: 899), 1:Y1 (ts: 899), 2:Y2 (ts: 899), 3:Y3 (ts: 899),
            //        0:YY0 (ts: 900), 1:YY1 (ts: 900), 2:YY2 (ts: 900), 3:YY3 (ts: 900),
            //        0:YYY0 (ts: 901), 1:YYY1 (ts: 901), 2:YYY2 (ts: 901), 3:YYY3 (ts: 901) }
            // --> w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 899), 1:Y1 (ts: 899), 2:Y2 (ts: 899), 3:Y3 (ts: 899),
            //            0:YY0 (ts: 900), 1:YY1 (ts: 900), 2:YY2 (ts: 900), 3:YY3 (ts: 900),
            //            0:YYY0 (ts: 901), 1:YYY1 (ts: 901), 2:YYY2 (ts: 901), 3:YYY3 (ts: 901),
            //            0:YYYY0 (ts: 902), 1:YYYY1 (ts: 902), 2:YYYY2 (ts: 902), 3:YYYY3 (ts: 902) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YYYY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:X0+YYYY0 (ts: 1000)", "1:X1+YYYY1 (ts: 1001)", "2:X2+YYYY2 (ts: 1002)");

            // push four items with increased timestamp to the other stream; this should produce four items
            // w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003),
            // w2 = { 0:Y0 (ts: 899), 1:Y1 (ts: 899), 2:Y2 (ts: 899), 3:Y3 (ts: 899),
            //        0:YY0 (ts: 900), 1:YY1 (ts: 900), 2:YY2 (ts: 900), 3:YY3 (ts: 900),
            //        0:YYY0 (ts: 901), 1:YYY1 (ts: 901), 2:YYY2 (ts: 901), 3:YYY3 (ts: 901),
            //        0:YYYY0 (ts: 902), 1:YYYY1 (ts: 902), 2:YYYY2 (ts: 902), 3:YYYY3 (ts: 902) }
            // --> w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 899), 1:Y1 (ts: 899), 2:Y2 (ts: 899), 3:Y3 (ts: 899),
            //            0:YY0 (ts: 900), 1:YY1 (ts: 900), 2:YY2 (ts: 900), 3:YY3 (ts: 900),
            //            0:YYY0 (ts: 901), 1:YYY1 (ts: 901), 2:YYY2 (ts: 901), 3:YYY3 (ts: 901),
            //            0:YYYY0 (ts: 902), 1:YYYY1 (ts: 902), 2:YYYY2 (ts: 902), 3:YYYY3 (ts: 902),
            //            0:YYYYY0 (ts: 903), 1:YYYYY1 (ts: 903), 2:YYYYY2 (ts: 903), 3:YYYYY3 (ts: 903) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YYYYY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:X0+YYYYY0 (ts: 1000)", "1:X1+YYYYY1 (ts: 1001)", "2:X2+YYYYY2 (ts: 1002)", "3:X3+YYYYY3 (ts: 1003)");

            // push four items with larger timestamp to the other stream; this should produce four items
            // w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003),
            // w2 = { 0:Y0 (ts: 899), 1:Y1 (ts: 899), 2:Y2 (ts: 899), 3:Y3 (ts: 899),
            //        0:YY0 (ts: 900), 1:YY1 (ts: 900), 2:YY2 (ts: 900), 3:YY3 (ts: 900),
            //        0:YYY0 (ts: 901), 1:YYY1 (ts: 901), 2:YYY2 (ts: 901), 3:YYY3 (ts: 901),
            //        0:YYYY0 (ts: 902), 1:YYYY1 (ts: 902), 2:YYYY2 (ts: 902), 3:YYYY3 (ts: 902),
            //        0:YYYYY0 (ts: 903), 1:YYYYY1 (ts: 903), 2:YYYYY2 (ts: 903), 3:YYYYY3 (ts: 903) }
            // --> w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 899), 1:Y1 (ts: 899), 2:Y2 (ts: 899), 3:Y3 (ts: 899),
            //            0:YY0 (ts: 900), 1:YY1 (ts: 900), 2:YY2 (ts: 900), 3:YY3 (ts: 900),
            //            0:YYY0 (ts: 901), 1:YYY1 (ts: 901), 2:YYY2 (ts: 901), 3:YYY3 (ts: 901),
            //            0:YYYY0 (ts: 902), 1:YYYY1 (ts: 902), 2:YYYY2 (ts: 902), 3:YYYY3 (ts: 902),
            //            0:YYYYY0 (ts: 903), 1:YYYYY1 (ts: 903), 2:YYYYY2 (ts: 903), 3:YYYYY3 (ts: 903),
            //            0:YYYYYY0 (ts: 1000), 1:YYYYYY1 (ts: 1000), 2:YYYYYY2 (ts: 1000), 3:YYYYYY3 (ts: 1000) }
            time = 1000L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YYYYYY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:X0+YYYYYY0 (ts: 1000)", "1:X1+YYYYYY1 (ts: 1001)", "2:X2+YYYYYY2 (ts: 1002)", "3:X3+YYYYYY3 (ts: 1003)");

            // push four items with increase timestamp to the other stream; this should produce three items
            // w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003),
            // w2 = { 0:Y0 (ts: 899), 1:Y1 (ts: 899), 2:Y2 (ts: 899), 3:Y3 (ts: 899),
            //        0:YY0 (ts: 900), 1:YY1 (ts: 900), 2:YY2 (ts: 900), 3:YY3 (ts: 900),
            //        0:YYY0 (ts: 901), 1:YYY1 (ts: 901), 2:YYY2 (ts: 901), 3:YYY3 (ts: 901),
            //        0:YYYY0 (ts: 902), 1:YYYY1 (ts: 902), 2:YYYY2 (ts: 902), 3:YYYY3 (ts: 902),
            //        0:YYYYY0 (ts: 903), 1:YYYYY1 (ts: 903), 2:YYYYY2 (ts: 903), 3:YYYYY3 (ts: 903),
            //        0:YYYYYY0 (ts: 1000), 1:YYYYYY1 (ts: 1000), 2:YYYYYY2 (ts: 1000), 3:YYYYYY3 (ts: 1000) }
            // --> w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 899), 1:Y1 (ts: 899), 2:Y2 (ts: 899), 3:Y3 (ts: 899),
            //            0:YY0 (ts: 900), 1:YY1 (ts: 900), 2:YY2 (ts: 900), 3:YY3 (ts: 900),
            //            0:YYY0 (ts: 901), 1:YYY1 (ts: 901), 2:YYY2 (ts: 901), 3:YYY3 (ts: 901),
            //            0:YYYY0 (ts: 902), 1:YYYY1 (ts: 902), 2:YYYY2 (ts: 902), 3:YYYY3 (ts: 902),
            //            0:YYYYY0 (ts: 903), 1:YYYYY1 (ts: 903), 2:YYYYY2 (ts: 903), 3:YYYYY3 (ts: 903),
            //            0:YYYYYY0 (ts: 1000), 1:YYYYYY1 (ts: 1000), 2:YYYYYY2 (ts: 1000), 3:YYYYYY3 (ts: 1000),
            //            0:YYYYYYY0 (ts: 1001), 1:YYYYYYY1 (ts: 1001), 2:YYYYYYY2 (ts: 1001), 3:YYYYYYY3 (ts: 1001) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YYYYYYY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("1:X1+YYYYYYY1 (ts: 1001)", "2:X2+YYYYYYY2 (ts: 1002)", "3:X3+YYYYYYY3 (ts: 1003)");

            // push four items with increase timestamp to the other stream; this should produce two items
            // w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003),
            // w2 = { 0:Y0 (ts: 899), 1:Y1 (ts: 899), 2:Y2 (ts: 899), 3:Y3 (ts: 899),
            //        0:YY0 (ts: 900), 1:YY1 (ts: 900), 2:YY2 (ts: 900), 3:YY3 (ts: 900),
            //        0:YYY0 (ts: 901), 1:YYY1 (ts: 901), 2:YYY2 (ts: 901), 3:YYY3 (ts: 901),
            //        0:YYYY0 (ts: 902), 1:YYYY1 (ts: 902), 2:YYYY2 (ts: 902), 3:YYYY3 (ts: 902),
            //        0:YYYYY0 (ts: 903), 1:YYYYY1 (ts: 903), 2:YYYYY2 (ts: 903), 3:YYYYY3 (ts: 903),
            //        0:YYYYYY0 (ts: 1000), 1:YYYYYY1 (ts: 1000), 2:YYYYYY2 (ts: 1000), 3:YYYYYY3 (ts: 1000),
            //        0:YYYYYYY0 (ts: 1001), 1:YYYYYYY1 (ts: 1001), 2:YYYYYYY2 (ts: 1001), 3:YYYYYYY3 (ts: 1001) }
            // --> w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 899), 1:Y1 (ts: 899), 2:Y2 (ts: 899), 3:Y3 (ts: 899),
            //            0:YY0 (ts: 900), 1:YY1 (ts: 900), 2:YY2 (ts: 900), 3:YY3 (ts: 900),
            //            0:YYY0 (ts: 901), 1:YYY1 (ts: 901), 2:YYY2 (ts: 901), 3:YYY3 (ts: 901),
            //            0:YYYY0 (ts: 902), 1:YYYY1 (ts: 902), 2:YYYY2 (ts: 902), 3:YYYY3 (ts: 902),
            //            0:YYYYY0 (ts: 903), 1:YYYYY1 (ts: 903), 2:YYYYY2 (ts: 903), 3:YYYYY3 (ts: 903),
            //            0:YYYYYY0 (ts: 1000), 1:YYYYYY1 (ts: 1000), 2:YYYYYY2 (ts: 1000), 3:YYYYYY3 (ts: 1000),
            //            0:YYYYYYY0 (ts: 1001), 1:YYYYYYY1 (ts: 1001), 2:YYYYYYY2 (ts: 1001), 3:YYYYYYY3 (ts: 1001),
            //            0:YYYYYYYY0 (ts: 1002), 1:YYYYYYYY1 (ts: 1002), 2:YYYYYYYY2 (ts: 1002), 3:YYYYYYYY3 (ts: 1002) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YYYYYYYYY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("2:X2+YYYYYYYYY2 (ts: 1002)", "3:X3+YYYYYYYYY3 (ts: 1003)");

            // push four items with increase timestamp to the other stream; this should produce one item
            // w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003),
            // w2 = { 0:Y0 (ts: 899), 1:Y1 (ts: 899), 2:Y2 (ts: 899), 3:Y3 (ts: 899),
            //        0:YY0 (ts: 900), 1:YY1 (ts: 900), 2:YY2 (ts: 900), 3:YY3 (ts: 900),
            //        0:YYY0 (ts: 901), 1:YYY1 (ts: 901), 2:YYY2 (ts: 901), 3:YYY3 (ts: 901),
            //        0:YYYY0 (ts: 902), 1:YYYY1 (ts: 902), 2:YYYY2 (ts: 902), 3:YYYY3 (ts: 902),
            //        0:YYYYY0 (ts: 903), 1:YYYYY1 (ts: 903), 2:YYYYY2 (ts: 903), 3:YYYYY3 (ts: 903),
            //        0:YYYYYY0 (ts: 1000), 1:YYYYYY1 (ts: 1000), 2:YYYYYY2 (ts: 1000), 3:YYYYYY3 (ts: 1000),
            //        0:YYYYYYY0 (ts: 1001), 1:YYYYYYY1 (ts: 1001), 2:YYYYYYY2 (ts: 1001), 3:YYYYYYY3 (ts: 1001),
            //        0:YYYYYYYY0 (ts: 1002), 1:YYYYYYYY1 (ts: 1002), 2:YYYYYYYY2 (ts: 1002), 3:YYYYYYYY3 (ts: 1002) }
            // --> w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 899), 1:Y1 (ts: 899), 2:Y2 (ts: 899), 3:Y3 (ts: 899),
            //            0:YY0 (ts: 900), 1:YY1 (ts: 900), 2:YY2 (ts: 900), 3:YY3 (ts: 900),
            //            0:YYY0 (ts: 901), 1:YYY1 (ts: 901), 2:YYY2 (ts: 901), 3:YYY3 (ts: 901),
            //            0:YYYY0 (ts: 902), 1:YYYY1 (ts: 902), 2:YYYY2 (ts: 902), 3:YYYY3 (ts: 902),
            //            0:YYYYY0 (ts: 903), 1:YYYYY1 (ts: 903), 2:YYYYY2 (ts: 903), 3:YYYYY3 (ts: 903),
            //            0:YYYYYY0 (ts: 1000), 1:YYYYYY1 (ts: 1000), 2:YYYYYY2 (ts: 1000), 3:YYYYYY3 (ts: 1000),
            //            0:YYYYYYY0 (ts: 1001), 1:YYYYYYY1 (ts: 1001), 2:YYYYYYY2 (ts: 1001), 3:YYYYYYY3 (ts: 1001),
            //            0:YYYYYYYY0 (ts: 1002), 1:YYYYYYYY1 (ts: 1002), 2:YYYYYYYY2 (ts: 1002), 3:YYYYYYYY3 (ts: 1002),
            //            0:YYYYYYYYY0 (ts: 1003), 1:YYYYYYYYY1 (ts: 1003), 2:YYYYYYYYY2 (ts: 1003), 3:YYYYYYYYY3 (ts: 1003) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YYYYYYYYYY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("3:X3+YYYYYYYYYY3 (ts: 1003)");

            // push four items with increase timestamp (no out of window) to the other stream; this should produce no items
            // w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003),
            // w2 = { 0:Y0 (ts: 899), 1:Y1 (ts: 899), 2:Y2 (ts: 899), 3:Y3 (ts: 899),
            //        0:YY0 (ts: 900), 1:YY1 (ts: 900), 2:YY2 (ts: 900), 3:YY3 (ts: 900),
            //        0:YYY0 (ts: 901), 1:YYY1 (ts: 901), 2:YYY2 (ts: 901), 3:YYY3 (ts: 901),
            //        0:YYYY0 (ts: 902), 1:YYYY1 (ts: 902), 2:YYYY2 (ts: 902), 3:YYYY3 (ts: 902),
            //        0:YYYYY0 (ts: 903), 1:YYYYY1 (ts: 903), 2:YYYYY2 (ts: 903), 3:YYYYY3 (ts: 903),
            //        0:YYYYYY0 (ts: 1000), 1:YYYYYY1 (ts: 1000), 2:YYYYYY2 (ts: 1000), 3:YYYYYY3 (ts: 1000),
            //        0:YYYYYYY0 (ts: 1001), 1:YYYYYYY1 (ts: 1001), 2:YYYYYYY2 (ts: 1001), 3:YYYYYYY3 (ts: 1001),
            //        0:YYYYYYYY0 (ts: 1002), 1:YYYYYYYY1 (ts: 1002), 2:YYYYYYYY2 (ts: 1002), 3:YYYYYYYY3 (ts: 1002),
            //        0:YYYYYYYYY0 (ts: 1003), 1:YYYYYYYYY1 (ts: 1003), 2:YYYYYYYYY2 (ts: 1003), 3:YYYYYYYYY3 (ts: 1003) }
            // --> w1 = { 0:X0 (ts: 1000), 1:X1 (ts: 1001), 2:X2 (ts: 1002), 3:X3 (ts: 1003) }
            //     w2 = { 0:Y0 (ts: 899), 1:Y1 (ts: 899), 2:Y2 (ts: 899), 3:Y3 (ts: 899),
            //            0:YY0 (ts: 900), 1:YY1 (ts: 900), 2:YY2 (ts: 900), 3:YY3 (ts: 900),
            //            0:YYY0 (ts: 901), 1:YYY1 (ts: 901), 2:YYY2 (ts: 901), 3:YYY3 (ts: 901),
            //            0:YYYY0 (ts: 902), 1:YYYY1 (ts: 902), 2:YYYY2 (ts: 902), 3:YYYY3 (ts: 902),
            //            0:YYYYY0 (ts: 903), 1:YYYYY1 (ts: 903), 2:YYYYY2 (ts: 903), 3:YYYYY3 (ts: 903),
            //            0:YYYYYY0 (ts: 1000), 1:YYYYYY1 (ts: 1000), 2:YYYYYY2 (ts: 1000), 3:YYYYYY3 (ts: 1000),
            //            0:YYYYYYY0 (ts: 1001), 1:YYYYYYY1 (ts: 1001), 2:YYYYYYY2 (ts: 1001), 3:YYYYYYY3 (ts: 1001),
            //            0:YYYYYYYY0 (ts: 1002), 1:YYYYYYYY1 (ts: 1002), 2:YYYYYYYY2 (ts: 1002), 3:YYYYYYYY3 (ts: 1002),
            //            0:YYYYYYYYY0 (ts: 1003), 1:YYYYYYYYY1 (ts: 1003), 2:YYYYYYYYY2 (ts: 1003), 3:YYYYYYYYY3 (ts: 1003),
            //            0:YYYYYYYYYY0 (ts: 1004), 1:YYYYYYYYYY1 (ts: 1004), 2:YYYYYYYYYY2 (ts: 1004), 3:YYYYYYYYYY3 (ts: 1004) }
            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YYYYYYYYYYY" + expectedKey, time));
            }
            processor.checkAndClearProcessResult();
        }
    }
}
