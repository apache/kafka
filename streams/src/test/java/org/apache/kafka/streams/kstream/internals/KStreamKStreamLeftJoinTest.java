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
    final private String topic1 = "topic1";
    final private String topic2 = "topic2";

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

            // push two items to the primary stream. the other window is empty
            // w1 {}
            // w2 {}
            // --> w1 = { 0:X0, 1:X1 }
            // --> w2 = {}
            for (int i = 0; i < 2; i++) {
                driver.pipeInput(recordFactory.create(topic1, expectedKeys[i], "X" + expectedKeys[i]));
            }
            processor.checkAndClearProcessResult("0:X0+null (ts: 0)", "1:X1+null (ts: 0)");

            // push two items to the other stream. this should produce two items.
            // w1 = { 0:X0, 1:X1 }
            // w2 {}
            // --> w1 = { 0:X0, 1:X1 }
            // --> w2 = { 0:Y0, 1:Y1 }
            for (int i = 0; i < 2; i++) {
                driver.pipeInput(recordFactory.create(topic2, expectedKeys[i], "Y" + expectedKeys[i]));
            }
            processor.checkAndClearProcessResult("0:X0+Y0 (ts: 0)", "1:X1+Y1 (ts: 0)");

            // push three items to the primary stream. this should produce four items.
            // w1 = { 0:X0, 1:X1 }
            // w2 = { 0:Y0, 1:Y1 }
            // --> w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2 }
            // --> w2 = { 0:Y0, 1:Y1 }
            for (int i = 0; i < 3; i++) {
                driver.pipeInput(recordFactory.create(topic1, expectedKeys[i], "X" + expectedKeys[i]));
            }
            processor.checkAndClearProcessResult("0:X0+Y0 (ts: 0)", "1:X1+Y1 (ts: 0)", "2:X2+null (ts: 0)");

            // push all items to the other stream. this should produce 5 items
            // w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2 }
            // w2 = { 0:Y0, 1:Y1 }
            // --> w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2 }
            // --> w2 = { 0:Y0, 1:Y1, 0:YY0, 1:YY1, 2:YY2, 3:YY3}
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YY" + expectedKey));
            }
            processor.checkAndClearProcessResult("0:X0+YY0 (ts: 0)", "0:X0+YY0 (ts: 0)", "1:X1+YY1 (ts: 0)", "1:X1+YY1 (ts: 0)", "2:X2+YY2 (ts: 0)");

            // push all four items to the primary stream. this should produce six items.
            // w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2 }
            // w2 = { 0:Y0, 1:Y1, 0:YY0, 1:YY1, 2:YY2, 3:YY3}
            // --> w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 0:XX0, 1:XX1, 2:XX2, 3:XX3 }
            // --> w2 = { 0:Y0, 1:Y1, 0:YY0, 1:YY1, 2:YY2, 3:YY3}
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XX" + expectedKey));
            }
            processor.checkAndClearProcessResult("0:XX0+Y0 (ts: 0)", "0:XX0+YY0 (ts: 0)", "1:XX1+Y1 (ts: 0)", "1:XX1+YY1 (ts: 0)", "2:XX2+YY2 (ts: 0)", "3:XX3+YY3 (ts: 0)");
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
            long time = 0L;

            // push two items to the primary stream. the other window is empty. this should produce two items
            // w1 = {}
            // w2 = {}
            // --> w1 = { 0:X0, 1:X1 }
            // --> w2 = {}
            for (int i = 0; i < 2; i++) {
                driver.pipeInput(recordFactory.create(topic1, expectedKeys[i], "X" + expectedKeys[i], time));
            }
            processor.checkAndClearProcessResult("0:X0+null (ts: 0)", "1:X1+null (ts: 0)");

            // push two items to the other stream. this should produce no items.
            // w1 = { 0:X0, 1:X1 }
            // w2 = {}
            // --> w1 = { 0:X0, 1:X1 }
            // --> w2 = { 0:Y0, 1:Y1 }
            for (int i = 0; i < 2; i++) {
                driver.pipeInput(recordFactory.create(topic2, expectedKeys[i], "Y" + expectedKeys[i], time));
            }
            processor.checkAndClearProcessResult("0:X0+Y0 (ts: 0)", "1:X1+Y1 (ts: 0)");

            // clear logically
            time = 1000L;

            // push all items to the other stream. this should produce no items.
            // w1 = {}
            // w2 = {}
            // --> w1 = {}
            // --> w2 = { 0:Y0, 1:Y1, 2:Y2, 3:Y3 }
            for (int i = 0; i < expectedKeys.length; i++) {
                driver.pipeInput(recordFactory.create(topic2, expectedKeys[i], "Y" + expectedKeys[i], time + i));
            }
            processor.checkAndClearProcessResult();

            // gradually expire items in window 2.
            // w1 = {}
            // w2 = {}
            // --> w1 = {}
            // --> w2 = { 0:Y0, 1:Y1, 2:Y2, 3:Y3 }
            time = 1000L + 100L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XX" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:XX0+Y0 (ts: 1100)", "1:XX1+Y1 (ts: 1100)", "2:XX2+Y2 (ts: 1100)", "3:XX3+Y3 (ts: 1100)");

            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XX" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:XX0+null (ts: 1101)", "1:XX1+Y1 (ts: 1101)", "2:XX2+Y2 (ts: 1101)", "3:XX3+Y3 (ts: 1101)");

            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XX" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:XX0+null (ts: 1102)", "1:XX1+null (ts: 1102)", "2:XX2+Y2 (ts: 1102)", "3:XX3+Y3 (ts: 1102)");

            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XX" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:XX0+null (ts: 1103)", "1:XX1+null (ts: 1103)", "2:XX2+null (ts: 1103)", "3:XX3+Y3 (ts: 1103)");

            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XX" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:XX0+null (ts: 1104)", "1:XX1+null (ts: 1104)", "2:XX2+null (ts: 1104)", "3:XX3+null (ts: 1104)");

            // go back to the time before expiration

            time = 1000L - 100L - 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XX" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:XX0+null (ts: 899)", "1:XX1+null (ts: 899)", "2:XX2+null (ts: 899)", "3:XX3+null (ts: 899)");

            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XX" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:XX0+Y0 (ts: 900)", "1:XX1+null (ts: 900)", "2:XX2+null (ts: 900)", "3:XX3+null (ts: 900)");

            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XX" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:XX0+Y0 (ts: 901)", "1:XX1+Y1 (ts: 901)", "2:XX2+null (ts: 901)", "3:XX3+null (ts: 901)");

            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XX" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:XX0+Y0 (ts: 902)", "1:XX1+Y1 (ts: 902)", "2:XX2+Y2 (ts: 902)", "3:XX3+null (ts: 902)");

            time += 1L;
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XX" + expectedKey, time));
            }
            processor.checkAndClearProcessResult("0:XX0+Y0 (ts: 903)", "1:XX1+Y1 (ts: 903)", "2:XX2+Y2 (ts: 903)", "3:XX3+Y3 (ts: 903)");
        }
    }
}
