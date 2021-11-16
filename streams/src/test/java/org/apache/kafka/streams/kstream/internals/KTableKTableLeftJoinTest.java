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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyTestDriverWrapper;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.MockApiProcessor;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KTableKTableLeftJoinTest {
    private final String topic1 = "topic1";
    private final String topic2 = "topic2";
    private final String output = "output";
    private final Consumed<Integer, String> consumed = Consumed.with(Serdes.Integer(), Serdes.String());
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.String());

    @Test
    public void testJoin() {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[] {0, 1, 2, 3};

        final KTable<Integer, String> table1 = builder.table(topic1, consumed);
        final KTable<Integer, String> table2 = builder.table(topic2, consumed);
        final KTable<Integer, String> joined = table1.leftJoin(table2, MockValueJoiner.TOSTRING_JOINER);
        joined.toStream().to(output);

        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                    driver.createInputTopic(topic1, Serdes.Integer().serializer(), Serdes.String().serializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, String> inputTopic2 =
                    driver.createInputTopic(topic2, Serdes.Integer().serializer(), Serdes.String().serializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestOutputTopic<Integer, String> outputTopic =
                    driver.createOutputTopic(output, Serdes.Integer().deserializer(), Serdes.String().deserializer());

            // push two items to the primary stream. the other table is empty
            for (int i = 0; i < 2; i++) {
                inputTopic1.pipeInput(expectedKeys[i], "X" + expectedKeys[i], 5L + i);
            }
            // pass tuple with null key, it will be discarded in join process
            inputTopic1.pipeInput(null, "SomeVal", 42L);
            // left: X0:0 (ts: 5), X1:1 (ts: 6)
            // right:
            assertOutputKeyValueTimestamp(outputTopic, 0, "X0+null", 5L);
            assertOutputKeyValueTimestamp(outputTopic, 1, "X1+null", 6L);
            assertTrue(outputTopic.isEmpty());

            // push two items to the other stream. this should produce two items.
            for (int i = 0; i < 2; i++) {
                inputTopic2.pipeInput(expectedKeys[i], "Y" + expectedKeys[i], 10L * i);
            }
            // pass tuple with null key, it will be discarded in join process
            inputTopic2.pipeInput(null, "AnotherVal", 73L);
            // left: X0:0 (ts: 5), X1:1 (ts: 6)
            // right: Y0:0 (ts: 0), Y1:1 (ts: 10)
            assertOutputKeyValueTimestamp(outputTopic, 0, "X0+Y0", 5L);
            assertOutputKeyValueTimestamp(outputTopic, 1, "X1+Y1", 10L);
            assertTrue(outputTopic.isEmpty());

            // push all four items to the primary stream. this should produce four items.
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "XX" + expectedKey, 7L);
            }
            // left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
            // right: Y0:0 (ts: 0), Y1:1 (ts: 10)
            assertOutputKeyValueTimestamp(outputTopic, 0, "XX0+Y0", 7L);
            assertOutputKeyValueTimestamp(outputTopic, 1, "XX1+Y1", 10L);
            assertOutputKeyValueTimestamp(outputTopic, 2, "XX2+null", 7L);
            assertOutputKeyValueTimestamp(outputTopic, 3, "XX3+null", 7L);
            assertTrue(outputTopic.isEmpty());

            // push all items to the other stream. this should produce four items.
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "YY" + expectedKey, expectedKey * 5L);
            }
            // left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
            // right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
            assertOutputKeyValueTimestamp(outputTopic, 0, "XX0+YY0", 7L);
            assertOutputKeyValueTimestamp(outputTopic, 1, "XX1+YY1", 7L);
            assertOutputKeyValueTimestamp(outputTopic, 2, "XX2+YY2", 10L);
            assertOutputKeyValueTimestamp(outputTopic, 3, "XX3+YY3", 15L);
            assertTrue(outputTopic.isEmpty());

            // push all four items to the primary stream. this should produce four items.
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "XXX" + expectedKey, 6L);
            }
            // left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
            // right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
            assertOutputKeyValueTimestamp(outputTopic, 0, "XXX0+YY0", 6L);
            assertOutputKeyValueTimestamp(outputTopic, 1, "XXX1+YY1", 6L);
            assertOutputKeyValueTimestamp(outputTopic, 2, "XXX2+YY2", 10L);
            assertOutputKeyValueTimestamp(outputTopic, 3, "XXX3+YY3", 15L);
            assertTrue(outputTopic.isEmpty());

            // push two items with null to the other stream as deletes. this should produce two item.
            inputTopic2.pipeInput(expectedKeys[0], null, 5L);
            inputTopic2.pipeInput(expectedKeys[1], null, 7L);
            // left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            assertOutputKeyValueTimestamp(outputTopic, 0, "XXX0+null", 6L);
            assertOutputKeyValueTimestamp(outputTopic, 1, "XXX1+null", 7L);
            assertTrue(outputTopic.isEmpty());

            // push all four items to the primary stream. this should produce four items.
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "XXXX" + expectedKey, 13L);
            }
            // left: XXXX0:0 (ts: 13), XXXX1:1 (ts: 13), XXXX2:2 (ts: 13), XXXX3:3 (ts: 13)
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            assertOutputKeyValueTimestamp(outputTopic, 0, "XXXX0+null", 13L);
            assertOutputKeyValueTimestamp(outputTopic, 1, "XXXX1+null", 13L);
            assertOutputKeyValueTimestamp(outputTopic, 2, "XXXX2+YY2", 13L);
            assertOutputKeyValueTimestamp(outputTopic, 3, "XXXX3+YY3", 15L);
            assertTrue(outputTopic.isEmpty());

            // push three items to the primary stream with null. this should produce four items.
            inputTopic1.pipeInput(expectedKeys[0], null, 0L);
            inputTopic1.pipeInput(expectedKeys[1], null, 42L);
            inputTopic1.pipeInput(expectedKeys[2], null, 5L);
            inputTopic1.pipeInput(expectedKeys[3], null, 20L);
            // left:
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            assertOutputKeyValueTimestamp(outputTopic, 0, null, 0L);
            assertOutputKeyValueTimestamp(outputTopic, 1, null, 42L);
            assertOutputKeyValueTimestamp(outputTopic, 2, null, 10L);
            assertOutputKeyValueTimestamp(outputTopic, 3, null, 20L);
            assertTrue(outputTopic.isEmpty());
        }
    }

    @Test
    public void testNotSendingOldValue() {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[] {0, 1, 2, 3};

        final KTable<Integer, String> table1;
        final KTable<Integer, String> table2;
        final KTable<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();

        table1 = builder.table(topic1, consumed);
        table2 = builder.table(topic2, consumed);
        joined = table1.leftJoin(table2, MockValueJoiner.TOSTRING_JOINER);

        final Topology topology = builder.build().addProcessor("proc", supplier, ((KTableImpl<?, ?, ?>) joined).name);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                    driver.createInputTopic(topic1, Serdes.Integer().serializer(), Serdes.String().serializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, String> inputTopic2 =
                    driver.createInputTopic(topic2, Serdes.Integer().serializer(), Serdes.String().serializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> proc = supplier.theCapturedProcessor();

            assertTrue(((KTableImpl<?, ?, ?>) table1).sendingOldValueEnabled());
            assertFalse(((KTableImpl<?, ?, ?>) table2).sendingOldValueEnabled());
            assertFalse(((KTableImpl<?, ?, ?>) joined).sendingOldValueEnabled());

            // push two items to the primary stream. the other table is empty
            for (int i = 0; i < 2; i++) {
                inputTopic1.pipeInput(expectedKeys[i], "X" + expectedKeys[i], 5L + i);
            }
            // pass tuple with null key, it will be discarded in join process
            inputTopic1.pipeInput(null, "SomeVal", 42L);
            // left: X0:0 (ts: 5), X1:1 (ts: 6)
            // right:
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("X0+null", null), 5),
                new KeyValueTimestamp<>(1, new Change<>("X1+null", null), 6));

            // push two items to the other stream. this should produce two items.
            for (int i = 0; i < 2; i++) {
                inputTopic2.pipeInput(expectedKeys[i], "Y" + expectedKeys[i], 10L * i);
            }
            // pass tuple with null key, it will be discarded in join process
            inputTopic2.pipeInput(null, "AnotherVal", 73L);
            // left: X0:0 (ts: 5), X1:1 (ts: 6)
            // right: Y0:0 (ts: 0), Y1:1 (ts: 10)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("X0+Y0", null), 5),
                new KeyValueTimestamp<>(1, new Change<>("X1+Y1", null), 10));

            // push all four items to the primary stream. this should produce four items.
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "XX" + expectedKey, 7L);
            }
            // left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
            // right: Y0:0 (ts: 0), Y1:1 (ts: 10)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XX0+Y0", null), 7),
                new KeyValueTimestamp<>(1, new Change<>("XX1+Y1", null), 10),
                new KeyValueTimestamp<>(2, new Change<>("XX2+null", null), 7),
                new KeyValueTimestamp<>(3, new Change<>("XX3+null", null), 7));

            // push all items to the other stream. this should produce four items.
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "YY" + expectedKey, expectedKey * 5L);
            }
            // left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
            // right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XX0+YY0", null), 7),
                new KeyValueTimestamp<>(1, new Change<>("XX1+YY1", null), 7),
                new KeyValueTimestamp<>(2, new Change<>("XX2+YY2", null), 10),
                new KeyValueTimestamp<>(3, new Change<>("XX3+YY3", null), 15));

            // push all four items to the primary stream. this should produce four items.
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "XXX" + expectedKey, 6L);
            }
            // left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
            // right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XXX0+YY0", null), 6),
                new KeyValueTimestamp<>(1, new Change<>("XXX1+YY1", null), 6),
                new KeyValueTimestamp<>(2, new Change<>("XXX2+YY2", null), 10),
                new KeyValueTimestamp<>(3, new Change<>("XXX3+YY3", null), 15));

            // push two items with null to the other stream as deletes. this should produce two item.
            inputTopic2.pipeInput(expectedKeys[0], null, 5L);
            inputTopic2.pipeInput(expectedKeys[1], null, 7L);
            // left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XXX0+null", null), 6),
                new KeyValueTimestamp<>(1, new Change<>("XXX1+null", null), 7));

            // push all four items to the primary stream. this should produce four items.
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "XXXX" + expectedKey, 13L);
            }
            // left: XXXX0:0 (ts: 13), XXXX1:1 (ts: 13), XXXX2:2 (ts: 13), XXXX3:3 (ts: 13)
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XXXX0+null", null), 13),
                new KeyValueTimestamp<>(1, new Change<>("XXXX1+null", null), 13),
                new KeyValueTimestamp<>(2, new Change<>("XXXX2+YY2", null), 13),
                new KeyValueTimestamp<>(3, new Change<>("XXXX3+YY3", null), 15));

            // push four items to the primary stream with null. this should produce four items.
            inputTopic1.pipeInput(expectedKeys[0], null, 0L);
            inputTopic1.pipeInput(expectedKeys[1], null, 42L);
            inputTopic1.pipeInput(expectedKeys[2], null, 5L);
            inputTopic1.pipeInput(expectedKeys[3], null, 20L);
            // left:
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>(null, null), 0),
                new KeyValueTimestamp<>(1, new Change<>(null, null), 42),
                new KeyValueTimestamp<>(2, new Change<>(null, null), 10),
                new KeyValueTimestamp<>(3, new Change<>(null, null), 20));
        }
    }

    @Test
    public void testSendingOldValue() {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[] {0, 1, 2, 3};

        final KTable<Integer, String> table1;
        final KTable<Integer, String> table2;
        final KTable<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();

        table1 = builder.table(topic1, consumed);
        table2 = builder.table(topic2, consumed);
        joined = table1.leftJoin(table2, MockValueJoiner.TOSTRING_JOINER);

        ((KTableImpl<?, ?, ?>) joined).enableSendingOldValues(true);

        assertThat(((KTableImpl<?, ?, ?>) table1).sendingOldValueEnabled(), is(true));
        assertThat(((KTableImpl<?, ?, ?>) table2).sendingOldValueEnabled(), is(true));
        assertThat(((KTableImpl<?, ?, ?>) joined).sendingOldValueEnabled(), is(true));

        final Topology topology = builder.build().addProcessor("proc", supplier, ((KTableImpl<?, ?, ?>) joined).name);

        try (final TopologyTestDriver driver = new TopologyTestDriverWrapper(topology, props)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                    driver.createInputTopic(topic1, Serdes.Integer().serializer(), Serdes.String().serializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, String> inputTopic2 =
                    driver.createInputTopic(topic2, Serdes.Integer().serializer(), Serdes.String().serializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> proc = supplier.theCapturedProcessor();

            assertTrue(((KTableImpl<?, ?, ?>) table1).sendingOldValueEnabled());
            assertTrue(((KTableImpl<?, ?, ?>) table2).sendingOldValueEnabled());
            assertTrue(((KTableImpl<?, ?, ?>) joined).sendingOldValueEnabled());

            // push two items to the primary stream. the other table is empty
            for (int i = 0; i < 2; i++) {
                inputTopic1.pipeInput(expectedKeys[i], "X" + expectedKeys[i], 5L + i);
            }
            // pass tuple with null key, it will be discarded in join process
            inputTopic1.pipeInput(null, "SomeVal", 42L);
            // left: X0:0 (ts: 5), X1:1 (ts: 6)
            // right:
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("X0+null", null), 5),
                new KeyValueTimestamp<>(1, new Change<>("X1+null", null), 6));

            // push two items to the other stream. this should produce two items.
            for (int i = 0; i < 2; i++) {
                inputTopic2.pipeInput(expectedKeys[i], "Y" + expectedKeys[i], 10L * i);
            }
            // pass tuple with null key, it will be discarded in join process
            inputTopic2.pipeInput(null, "AnotherVal", 73L);
            // left: X0:0 (ts: 5), X1:1 (ts: 6)
            // right: Y0:0 (ts: 0), Y1:1 (ts: 10)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("X0+Y0", "X0+null"), 5),
                new KeyValueTimestamp<>(1, new Change<>("X1+Y1", "X1+null"), 10));

            // push all four items to the primary stream. this should produce four items.
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "XX" + expectedKey, 7L);
            }
            // left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
            // right: Y0:0 (ts: 0), Y1:1 (ts: 10)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XX0+Y0", "X0+Y0"), 7),
                new KeyValueTimestamp<>(1, new Change<>("XX1+Y1", "X1+Y1"), 10),
                new KeyValueTimestamp<>(2, new Change<>("XX2+null", null), 7),
                new KeyValueTimestamp<>(3, new Change<>("XX3+null", null), 7));

            // push all items to the other stream. this should produce four items.
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, "YY" + expectedKey, expectedKey * 5L);
            }
            // left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
            // right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XX0+YY0", "XX0+Y0"), 7),
                new KeyValueTimestamp<>(1, new Change<>("XX1+YY1", "XX1+Y1"), 7),
                new KeyValueTimestamp<>(2, new Change<>("XX2+YY2", "XX2+null"), 10),
                new KeyValueTimestamp<>(3, new Change<>("XX3+YY3", "XX3+null"), 15));
            // push all four items to the primary stream. this should produce four items.
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "XXX" + expectedKey, 6L);
            }
            // left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
            // right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XXX0+YY0", "XX0+YY0"), 6),
                new KeyValueTimestamp<>(1, new Change<>("XXX1+YY1", "XX1+YY1"), 6),
                new KeyValueTimestamp<>(2, new Change<>("XXX2+YY2", "XX2+YY2"), 10),
                new KeyValueTimestamp<>(3, new Change<>("XXX3+YY3", "XX3+YY3"), 15));

            // push two items with null to the other stream as deletes. this should produce two item.
            inputTopic2.pipeInput(expectedKeys[0], null, 5L);
            inputTopic2.pipeInput(expectedKeys[1], null, 7L);
            // left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XXX0+null", "XXX0+YY0"), 6),
                new KeyValueTimestamp<>(1, new Change<>("XXX1+null", "XXX1+YY1"), 7));

            // push all four items to the primary stream. this should produce four items.
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "XXXX" + expectedKey, 13L);
            }
            // left: XXXX0:0 (ts: 13), XXXX1:1 (ts: 13), XXXX2:2 (ts: 13), XXXX3:3 (ts: 13)
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XXXX0+null", "XXX0+null"), 13),
                new KeyValueTimestamp<>(1, new Change<>("XXXX1+null", "XXX1+null"), 13),
                new KeyValueTimestamp<>(2, new Change<>("XXXX2+YY2", "XXX2+YY2"), 13),
                new KeyValueTimestamp<>(3, new Change<>("XXXX3+YY3", "XXX3+YY3"), 15));
            // push four items to the primary stream with null. this should produce four items.
            inputTopic1.pipeInput(expectedKeys[0], null, 0L);
            inputTopic1.pipeInput(expectedKeys[1], null, 42L);
            inputTopic1.pipeInput(expectedKeys[2], null, 5L);
            inputTopic1.pipeInput(expectedKeys[3], null, 20L);
            // left:
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>(null, "XXXX0+null"), 0),
                new KeyValueTimestamp<>(1, new Change<>(null, "XXXX1+null"), 42),
                new KeyValueTimestamp<>(2, new Change<>(null, "XXXX2+YY2"), 10),
                new KeyValueTimestamp<>(3, new Change<>(null, "XXXX3+YY3"), 20));
        }
    }

    /**
     * This test was written to reproduce https://issues.apache.org/jira/browse/KAFKA-4492
     * It is based on a fairly complicated join used by the developer that reported the bug.
     * Before the fix this would trigger an IllegalStateException.
     */
    @Test
    public void shouldNotThrowIllegalStateExceptionWhenMultiCacheEvictions() {
        final String agg = "agg";
        final String tableOne = "tableOne";
        final String tableTwo = "tableTwo";
        final String tableThree = "tableThree";
        final String tableFour = "tableFour";
        final String tableFive = "tableFive";
        final String tableSix = "tableSix";
        final String[] inputs = {agg, tableOne, tableTwo, tableThree, tableFour, tableFive, tableSix};

        final StreamsBuilder builder = new StreamsBuilder();
        final Consumed<Long, String> consumed = Consumed.with(Serdes.Long(), Serdes.String());
        final KTable<Long, String> aggTable = builder
            .table(agg, consumed, Materialized.as(Stores.inMemoryKeyValueStore("agg-base-store")))
            .groupBy(KeyValue::new, Grouped.with(Serdes.Long(), Serdes.String()))
            .reduce(
                MockReducer.STRING_ADDER,
                MockReducer.STRING_ADDER,
                Materialized.as(Stores.inMemoryKeyValueStore("agg-store")));

        final KTable<Long, String> one = builder.table(
            tableOne,
            consumed,
            Materialized.as(Stores.inMemoryKeyValueStore("tableOne-base-store")));
        final KTable<Long, String> two = builder.table(
            tableTwo,
            consumed,
            Materialized.as(Stores.inMemoryKeyValueStore("tableTwo-base-store")));
        final KTable<Long, String> three = builder.table(
            tableThree,
            consumed,
            Materialized.as(Stores.inMemoryKeyValueStore("tableThree-base-store")));
        final KTable<Long, String> four = builder.table(
            tableFour,
            consumed,
            Materialized.as(Stores.inMemoryKeyValueStore("tableFour-base-store")));
        final KTable<Long, String> five = builder.table(
            tableFive,
            consumed,
            Materialized.as(Stores.inMemoryKeyValueStore("tableFive-base-store")));
        final KTable<Long, String> six = builder.table(
            tableSix,
            consumed,
            Materialized.as(Stores.inMemoryKeyValueStore("tableSix-base-store")));

        final ValueMapper<String, String> mapper = value -> value.toUpperCase(Locale.ROOT);

        final KTable<Long, String> seven = one.mapValues(mapper);

        final KTable<Long, String> eight = six.leftJoin(seven, MockValueJoiner.TOSTRING_JOINER);

        aggTable
            .leftJoin(one, MockValueJoiner.TOSTRING_JOINER)
            .leftJoin(two, MockValueJoiner.TOSTRING_JOINER)
            .leftJoin(three, MockValueJoiner.TOSTRING_JOINER)
            .leftJoin(four, MockValueJoiner.TOSTRING_JOINER)
            .leftJoin(five, MockValueJoiner.TOSTRING_JOINER)
            .leftJoin(eight, MockValueJoiner.TOSTRING_JOINER)
            .mapValues(mapper);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final String[] values = {
                "a", "AA", "BBB", "CCCC", "DD", "EEEEEEEE", "F", "GGGGGGGGGGGGGGG", "HHH", "IIIIIIIIII",
                "J", "KK", "LLLL", "MMMMMMMMMMMMMMMMMMMMMM", "NNNNN", "O", "P", "QQQQQ", "R", "SSSS",
                "T", "UU", "VVVVVVVVVVVVVVVVVVV"
            };

            TestInputTopic<Long, String> inputTopic;
            final Random random = new Random();
            for (int i = 0; i < 1000; i++) {
                for (final String input : inputs) {
                    final Long key = (long) random.nextInt(1000);
                    final String value = values[random.nextInt(values.length)];
                    inputTopic = driver.createInputTopic(input, Serdes.Long().serializer(), Serdes.String().serializer());
                    inputTopic.pipeInput(key, value);
                }
            }
        }
    }

    @Test
    public void shouldLogAndMeterSkippedRecordsDueToNullLeftKey() {
        final StreamsBuilder builder = new StreamsBuilder();

        @SuppressWarnings("unchecked")
        final Processor<String, Change<String>, String, Change<Object>> join = new KTableKTableLeftJoin<>(
            (KTableImpl<String, String, String>) builder.table("left", Consumed.with(Serdes.String(), Serdes.String())),
            (KTableImpl<String, String, String>) builder.table("right", Consumed.with(Serdes.String(), Serdes.String())),
            null
        ).get();

        final MockProcessorContext<String, Change<Object>> context = new MockProcessorContext<>(props);
        context.setRecordMetadata("left", -1, -2);
        join.init(context);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KTableKTableLeftJoin.class)) {
            join.process(new Record<>(null, new Change<>("new", "old"), 0));

            assertThat(
                appender.getMessages(),
                hasItem("Skipping record due to null key. topic=[left] partition=[-1] offset=[-2]")
            );
        }
    }

    private void assertOutputKeyValueTimestamp(final TestOutputTopic<Integer, String> outputTopic,
                                               final Integer expectedKey,
                                               final String expectedValue,
                                               final long expectedTimestamp) {
        assertThat(outputTopic.readRecord(), equalTo(new TestRecord<>(expectedKey, expectedValue, null, expectedTimestamp)));
    }
}
