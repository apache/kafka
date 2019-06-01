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
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
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
import static org.junit.Assert.assertEquals;

public class KStreamKStreamLeftJoinTest {
    private final static String[] EMPTY = new String[0];

    private final String topic1 = "topic1";
    private final String topic2 = "topic2";
    private final Consumed<Integer, String> consumed = Consumed.with(Serdes.Integer(), Serdes.String());
    private final ConsumerRecordFactory<Integer, String> recordFactory =
        new ConsumerRecordFactory<>(new IntegerSerializer(), new StringSerializer(), 0L);
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    @Test
    public void testLeftJoin() {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockProcessorSupplier<Integer, String> supplier = new MockProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);

        joined = stream1.leftJoin(
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
            // w1 {}
            // w2 {}
            // --> w1 = { 0:A0, 1:A1 }
            // --> w2 = {}
            for (int i = 0; i < 2; i++) {
                driver.pipeInput(recordFactory.create(topic1, expectedKeys[i], "A" + expectedKeys[i]));
            }
            processor.checkAndClearProcessResult("0:A0+null (ts: 0)", "1:A1+null (ts: 0)");

            // push two items to the other stream; this should produce two items
            // w1 = { 0:A0, 1:A1 }
            // w2 {}
            // --> w1 = { 0:A0, 1:A1 }
            // --> w2 = { 0:a0, 1:a1 }
            for (int i = 0; i < 2; i++) {
                driver.pipeInput(recordFactory.create(topic2, expectedKeys[i], "a" + expectedKeys[i]));
            }
            processor.checkAndClearProcessResult("0:A0+a0 (ts: 0)", "1:A1+a1 (ts: 0)");

            // push three items to the primary stream; this should produce four items
            // w1 = { 0:A0, 1:A1 }
            // w2 = { 0:a0, 1:a1 }
            // --> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2 }
            // --> w2 = { 0:a0, 1:a1 }
            for (int i = 0; i < 3; i++) {
                driver.pipeInput(recordFactory.create(topic1, expectedKeys[i], "B" + expectedKeys[i]));
            }
            processor.checkAndClearProcessResult("0:B0+a0 (ts: 0)", "1:B1+a1 (ts: 0)", "2:B2+null (ts: 0)");

            // push all items to the other stream; this should produce five items
            // w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2 }
            // w2 = { 0:a0, 1:a1 }
            // --> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2 }
            // --> w2 = { 0:a0, 1:a1, 0:b0, 1:b1, 2:b2, 3:b3 }
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "b" + expectedKey));
            }
            processor.checkAndClearProcessResult("0:A0+b0 (ts: 0)", "0:B0+b0 (ts: 0)", "1:A1+b1 (ts: 0)", "1:B1+b1 (ts: 0)", "2:B2+b2 (ts: 0)");

            // push all four items to the primary stream; this should produce six items
            // w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2 }
            // w2 = { 0:a0, 1:a1, 0:b0, 1:b1, 2:b2, 3:b3 }
            // --> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 0:C0, 1:C1, 2:C2, 3:C3 }
            // --> w2 = { 0:a0, 1:a1, 0:b0, 1:b1, 2:b2, 3:b3 }
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "C" + expectedKey));
            }
            processor.checkAndClearProcessResult("0:C0+a0 (ts: 0)", "0:C0+b0 (ts: 0)", "1:C1+a1 (ts: 0)", "1:C1+b1 (ts: 0)", "2:C2+b2 (ts: 0)", "3:C3+b3 (ts: 0)");
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

        joined = stream1.leftJoin(
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
            final long time = 0L;

            // push two items to the primary stream; the other window is empty; this should produce two left-join items
            // w1 = {}
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            // --> w2 = {}
            for (int i = 0; i < 2; i++) {
                driver.pipeInput(recordFactory.create(topic1, expectedKeys[i], "A" + expectedKeys[i], time));
            }
            processor.checkAndClearProcessResult("0:A0+null (ts: 0)", "1:A1+null (ts: 0)");

            // push four items to the other stream; this should produce two full-join items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            // --> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0) }
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "a" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:A0+a0 (ts: 0)", "1:A1+a1 (ts: 0)");

            testUpperWindowBound(expectedKeys, driver, processor);
            testLowerWindowBound(expectedKeys, driver, processor);
        }
    }

    private void testUpperWindowBound(final int[] expectedKeys,
                                      final TopologyTestDriver driver,
                                      final MockProcessor<Integer, String> processor) {
        long time;

        // push four items with larger and increasing timestamp (out of window) to the other stream; this should produce no items
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
        // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
        // --> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        time = 1000L;
        for (int i = 0; i < expectedKeys.length; i++) {
            driver.pipeInput(recordFactory.create(topic2, expectedKeys[i], "b" + expectedKeys[i], time + i));
        }
        processor.checkAndClearProcessResult(EMPTY);

        // push four items with larger timestamp to the primary stream; this should produce four full-join items
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
        // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100) }
        // --> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        time = 1000L + 100L;
        for (final int expectedKey : expectedKeys) {
            driver.pipeInput(recordFactory.create(topic1, expectedKey, "B" + expectedKey, time));
        }
        processor.checkAndClearProcessResult("0:B0+b0 (ts: 1100)", "1:B1+b1 (ts: 1100)", "2:B2+b2 (ts: 1100)", "3:B3+b3 (ts: 1100)");

        // push four items with increased timestamp to the primary stream; this should produce one left-join and three full-join items
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100) }
        // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101) }
        // --> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        time += 1L;
        for (final int expectedKey : expectedKeys) {
            driver.pipeInput(recordFactory.create(topic1, expectedKey, "C" + expectedKey, time));
        }
        processor.checkAndClearProcessResult("0:C0+null (ts: 1101)", "1:C1+b1 (ts: 1101)", "2:C2+b2 (ts: 1101)", "3:C3+b3 (ts: 1101)");

        // push four items with increased timestamp to the primary stream; this should produce two left-join and two full-join items
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101) }
        // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102) }
        // --> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        time += 1L;
        for (final int expectedKey : expectedKeys) {
            driver.pipeInput(recordFactory.create(topic1, expectedKey, "D" + expectedKey, time));
        }
        processor.checkAndClearProcessResult("0:D0+null (ts: 1102)", "1:D1+null (ts: 1102)", "2:D2+b2 (ts: 1102)", "3:D3+b3 (ts: 1102)");

        // push four items with increased timestamp to the primary stream; this should produce three left-join and one full-join items
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102) }
        // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103) }
        // --> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        time += 1L;
        for (final int expectedKey : expectedKeys) {
            driver.pipeInput(recordFactory.create(topic1, expectedKey, "E" + expectedKey, time));
        }
        processor.checkAndClearProcessResult("0:E0+null (ts: 1103)", "1:E1+null (ts: 1103)", "2:E2+null (ts: 1103)", "3:E3+b3 (ts: 1103)");

        // push four items with increased timestamp to the primary stream; this should produce four left-join and no full-join items
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //        0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103) }
        // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //            0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104) }
        // --> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        time += 1L;
        for (final int expectedKey : expectedKeys) {
            driver.pipeInput(recordFactory.create(topic1, expectedKey, "F" + expectedKey, time));
        }
        processor.checkAndClearProcessResult("0:F0+null (ts: 1104)", "1:F1+null (ts: 1104)", "2:F2+null (ts: 1104)", "3:F3+null (ts: 1104)");
    }

    private void testLowerWindowBound(final int[] expectedKeys,
                                      final TopologyTestDriver driver,
                                      final MockProcessor<Integer, String> processor) {
        long time;

        // push four items with smaller timestamp (before the window) to the primary stream; this should produce four left-join and no full-join items
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //        0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //        0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104) }
        // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //            0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //            0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899) }
        // --> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        time = 1000L - 100L - 1L;
        for (final int expectedKey : expectedKeys) {
            driver.pipeInput(recordFactory.create(topic1, expectedKey, "G" + expectedKey, time));
        }
        processor.checkAndClearProcessResult("0:G0+null (ts: 899)", "1:G1+null (ts: 899)", "2:G2+null (ts: 899)", "3:G3+null (ts: 899)");

        // push four items with increase timestamp to the primary stream; this should produce three left-join and one full-join items
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //        0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //        0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //        0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899) }
        // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //            0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //            0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
        //            0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900) }
        // --> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        time += 1L;
        for (final int expectedKey : expectedKeys) {
            driver.pipeInput(recordFactory.create(topic1, expectedKey, "H" + expectedKey, time));
        }
        processor.checkAndClearProcessResult("0:H0+b0 (ts: 1000)", "1:H1+null (ts: 900)", "2:H2+null (ts: 900)", "3:H3+null (ts: 900)");

        // push four items with increase timestamp to the primary stream; this should produce two left-join and two full-join items
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //        0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //        0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //        0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
        //        0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900) }
        // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //            0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //            0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
        //            0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900),
        //            0:I0 (ts: 901), 1:I1 (ts: 901), 2:I2 (ts: 901), 3:I3 (ts: 901) }
        // --> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        time += 1L;
        for (final int expectedKey : expectedKeys) {
            driver.pipeInput(recordFactory.create(topic1, expectedKey, "I" + expectedKey, time));
        }
        processor.checkAndClearProcessResult("0:I0+b0 (ts: 1000)", "1:I1+b1 (ts: 1001)", "2:I2+null (ts: 901)", "3:I3+null (ts: 901)");

        // push four items with increase timestamp to the primary stream; this should produce one left-join and three full-join items
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //        0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //        0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //        0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
        //        0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900),
        //        0:I0 (ts: 901), 1:I1 (ts: 901), 2:I2 (ts: 901), 3:I3 (ts: 901) }
        // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //            0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //            0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
        //            0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900),
        //            0:I0 (ts: 901), 1:I1 (ts: 901), 2:I2 (ts: 901), 3:I3 (ts: 901),
        //            0:J0 (ts: 902), 1:J1 (ts: 902), 2:J2 (ts: 902), 3:J3 (ts: 902) }
        // --> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        time += 1L;
        for (final int expectedKey : expectedKeys) {
            driver.pipeInput(recordFactory.create(topic1, expectedKey, "J" + expectedKey, time));
        }
        processor.checkAndClearProcessResult("0:J0+b0 (ts: 1000)", "1:J1+b1 (ts: 1001)", "2:J2+b2 (ts: 1002)", "3:J3+null (ts: 902)");

        // push four items with increase timestamp to the primary stream; this should produce one left-join and three full-join items
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //        0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //        0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //        0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
        //        0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900),
        //        0:I0 (ts: 901), 1:I1 (ts: 901), 2:I2 (ts: 901), 3:I3 (ts: 901),
        //        0:J0 (ts: 902), 1:J1 (ts: 902), 2:J2 (ts: 902), 3:J3 (ts: 902) }
        // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //            0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //            0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
        //            0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900),
        //            0:I0 (ts: 901), 1:I1 (ts: 901), 2:I2 (ts: 901), 3:I3 (ts: 901),
        //            0:J0 (ts: 902), 1:J1 (ts: 902), 2:J2 (ts: 902), 3:J3 (ts: 902),
        //            0:K0 (ts: 903), 1:K1 (ts: 903), 2:K2 (ts: 903), 3:K3 (ts: 903) }
        // --> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
        //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
        time += 1L;
        for (final int expectedKey : expectedKeys) {
            driver.pipeInput(recordFactory.create(topic1, expectedKey, "K" + expectedKey, time));
        }
        processor.checkAndClearProcessResult("0:K0+b0 (ts: 1000)", "1:K1+b1 (ts: 1001)", "2:K2+b2 (ts: 1002)", "3:K3+b3 (ts: 1003)");
    }
}
