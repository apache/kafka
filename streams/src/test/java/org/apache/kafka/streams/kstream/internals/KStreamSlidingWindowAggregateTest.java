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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender.Event;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.InMemoryWindowBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.InMemoryWindowStore;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockApiProcessor;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.StreamsTestUtils;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class KStreamSlidingWindowAggregateTest {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {
            {false},
            {true}
        });
    }

    @Parameterized.Parameter
    public boolean inOrderIterator;

    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
    private final String threadId = Thread.currentThread().getName();

    @Test
    public void testAggregateSmallInput() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic = "topic";

        final WindowBytesStoreSupplier storeSupplier =
            inOrderIterator
                ? new InOrderMemoryWindowStoreSupplier("InOrder", 50000L, 10L, false)
                : Stores.inMemoryWindowStore("Reverse", Duration.ofMillis(50000), Duration.ofMillis(10), false);
        final KTable<Windowed<String>, String> table = builder
            .stream(topic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(10), ofMillis(50)))
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                Materialized.as(storeSupplier)
            );
        final MockApiProcessorSupplier<Windowed<String>, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table.toStream().process(supplier);
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic, new StringSerializer(), new StringSerializer());
            inputTopic.pipeInput("A", "1", 10L);
            inputTopic.pipeInput("A", "2", 15L);
            inputTopic.pipeInput("A", "3", 20L);
            inputTopic.pipeInput("A", "4", 22L);
            inputTopic.pipeInput("A", "5", 30L);
        }

        final Map<Long, ValueAndTimestamp<String>> actual = new HashMap<>();

        for (final KeyValueTimestamp<Windowed<String>, String> entry : supplier.theCapturedProcessor().processed()) {
            final Windowed<String> window = entry.key();
            final Long start = window.window().start();
            final ValueAndTimestamp<String> valueAndTimestamp = ValueAndTimestamp.make(entry.value(), entry.timestamp());

            if (actual.putIfAbsent(start, valueAndTimestamp) != null) {
                actual.replace(start, valueAndTimestamp);
            }
        }

        final Map<Long, ValueAndTimestamp<String>> expected = new HashMap<>();
        expected.put(0L, ValueAndTimestamp.make("0+1", 10L));
        expected.put(5L, ValueAndTimestamp.make("0+1+2", 15L));
        expected.put(10L, ValueAndTimestamp.make("0+1+2+3", 20L));
        expected.put(11L, ValueAndTimestamp.make("0+2+3", 20L));
        expected.put(12L, ValueAndTimestamp.make("0+2+3+4", 22L));
        expected.put(16L, ValueAndTimestamp.make("0+3+4", 22L));
        expected.put(20L, ValueAndTimestamp.make("0+3+4+5", 30L));
        expected.put(21L, ValueAndTimestamp.make("0+4+5", 30L));
        expected.put(23L, ValueAndTimestamp.make("0+5", 30L));

        assertEquals(expected, actual);
    }

    @Test
    public void testReduceSmallInput() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic = "topic";
        final WindowBytesStoreSupplier storeSupplier =
            inOrderIterator
                ? new InOrderMemoryWindowStoreSupplier("InOrder", 50000L, 10L, false)
                : Stores.inMemoryWindowStore("Reverse", Duration.ofMillis(50000), Duration.ofMillis(10), false);

        final KTable<Windowed<String>, String> table = builder
            .stream(topic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(10), ofMillis(50)))
            .reduce(
                MockReducer.STRING_ADDER,
                Materialized.as(storeSupplier)
            );
        final MockApiProcessorSupplier<Windowed<String>, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table.toStream().process(supplier);
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic, new StringSerializer(), new StringSerializer());
            inputTopic.pipeInput("A", "1", 10L);
            inputTopic.pipeInput("A", "2", 14L);
            inputTopic.pipeInput("A", "3", 15L);
            inputTopic.pipeInput("A", "4", 22L);
            inputTopic.pipeInput("A", "5", 26L);
            inputTopic.pipeInput("A", "6", 30L);
        }

        final Map<Long, ValueAndTimestamp<String>> actual = new HashMap<>();

        for (final KeyValueTimestamp<Windowed<String>, String> entry : supplier.theCapturedProcessor().processed()) {
            final Windowed<String> window = entry.key();
            final Long start = window.window().start();
            final ValueAndTimestamp<String> valueAndTimestamp = ValueAndTimestamp.make(entry.value(), entry.timestamp());
            if (actual.putIfAbsent(start, valueAndTimestamp) != null) {
                actual.replace(start, valueAndTimestamp);
            }
        }

        final Map<Long, ValueAndTimestamp<String>> expected = new HashMap<>();
        expected.put(0L, ValueAndTimestamp.make("1", 10L));
        expected.put(4L, ValueAndTimestamp.make("1+2", 14L));
        expected.put(5L, ValueAndTimestamp.make("1+2+3", 15L));
        expected.put(11L, ValueAndTimestamp.make("2+3", 15L));
        expected.put(12L, ValueAndTimestamp.make("2+3+4", 22L));
        expected.put(15L, ValueAndTimestamp.make("3+4", 22L));
        expected.put(16L, ValueAndTimestamp.make("4+5", 26L));
        expected.put(20L, ValueAndTimestamp.make("4+5+6", 30L));
        expected.put(23L, ValueAndTimestamp.make("5+6", 30L));
        expected.put(27L, ValueAndTimestamp.make("6", 30L));

        assertEquals(expected, actual);
    }

    @Test
    public void testAggregateLargeInput() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final WindowBytesStoreSupplier storeSupplier =
            inOrderIterator
                ? new InOrderMemoryWindowStoreSupplier("InOrder", 50000L, 10L, false)
                : Stores.inMemoryWindowStore("Reverse", Duration.ofMillis(50000), Duration.ofMillis(10), false);
        final KTable<Windowed<String>, String> table2 = builder
            .stream(topic1, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(10), ofMillis(50)))
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                Materialized.as(storeSupplier)
            );

        final MockApiProcessorSupplier<Windowed<String>, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                    driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer());
            inputTopic1.pipeInput("A", "1", 10L);
            inputTopic1.pipeInput("A", "2", 20L);
            inputTopic1.pipeInput("A", "3", 22L);
            inputTopic1.pipeInput("A", "4", 15L);

            inputTopic1.pipeInput("B", "1", 12L);
            inputTopic1.pipeInput("B", "2", 13L);
            inputTopic1.pipeInput("B", "3", 18L);
            inputTopic1.pipeInput("B", "4", 19L);
            inputTopic1.pipeInput("B", "5", 25L);
            inputTopic1.pipeInput("B", "6", 14L);

            inputTopic1.pipeInput("C", "1", 11L);
            inputTopic1.pipeInput("C", "2", 15L);
            inputTopic1.pipeInput("C", "3", 16L);
            inputTopic1.pipeInput("C", "4", 21);
            inputTopic1.pipeInput("C", "5", 23L);
            
            inputTopic1.pipeInput("D", "4", 11L);
            inputTopic1.pipeInput("D", "2", 12L);
            inputTopic1.pipeInput("D", "3", 29L);
            inputTopic1.pipeInput("D", "5", 16L);
        }
        final Comparator<KeyValueTimestamp<Windowed<String>, String>> comparator =
            Comparator.comparing((KeyValueTimestamp<Windowed<String>, String> o) -> o.key().key())
                .thenComparing((KeyValueTimestamp<Windowed<String>, String> o) -> o.key().window().start());

        final ArrayList<KeyValueTimestamp<Windowed<String>, String>> actual = supplier.theCapturedProcessor().processed();
        actual.sort(comparator);
        assertEquals(
            asList(
                // FINAL WINDOW: A@10 left window created when A@10 processed
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0, 10)), "0+1", 10),
                // FINAL WINDOW: A@15 left window created when A@15 processed
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5, 15)), "0+1+4", 15),
                // A@20 left window created when A@20 processed
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(10, 20)), "0+1+2", 20),
                // FINAL WINDOW: A@20 left window updated when A@15 processed
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(10, 20)), "0+1+2+4", 20),
                // A@10 right window created when A@20 processed
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(11, 21)), "0+2", 20),
                // FINAL WINDOW: A@10 right window updated when A@15 processed
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(11, 21)), "0+2+4", 20),
                // A@22 left window created when A@22 processed
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(12, 22)), "0+2+3", 22),
                // FINAL WINDOW: A@22 left window updated when A@15 processed
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(12, 22)), "0+2+3+4", 22),
                // FINAL WINDOW: A@15 right window created when A@15 processed
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(16, 26)), "0+2+3", 22),
                // FINAL WINDOW: A@20 right window created when A@22 processed
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(21, 31)), "0+3", 22),
                // FINAL WINDOW: B@12 left window created when B@12 processed
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(2, 12)), "0+1", 12),
                // FINAL WINDOW: B@13 left window created when B@13 processed
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(3, 13)), "0+1+2", 13),
                // FINAL WINDOW: B@14 left window created when B@14 processed
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(4, 14)), "0+1+2+6", 14),
                // B@18 left window created when B@18 processed
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(8, 18)), "0+1+2+3", 18),
                // FINAL WINDOW: B@18 left window updated when B@14 processed
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(8, 18)), "0+1+2+3+6", 18),
                // B@19 left window created when B@19 processed
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(9, 19)), "0+1+2+3+4", 19),
                // FINAL WINDOW: B@19 left window updated when B@14 processed
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(9, 19)), "0+1+2+3+4+6", 19),
                // B@12 right window created when B@13 processed
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(13, 23)), "0+2", 13),
                // B@12 right window updated when B@18 processed
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(13, 23)), "0+2+3", 18),
                // B@12 right window updated when B@19 processed
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(13, 23)), "0+2+3+4", 19),
                // FINAL WINDOW: B@12 right window updated when B@14 processed
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(13, 23)), "0+2+3+4+6", 19),
                // B@13 right window created when B@18 processed
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(14, 24)), "0+3", 18),
                // B@13 right window updated when B@19 processed
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(14, 24)), "0+3+4", 19),
                // FINAL WINDOW: B@13 right window updated when B@14 processed
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(14, 24)), "0+3+4+6", 19),
                // FINAL WINDOW: B@25 left window created when B@25 processed
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(15, 25)), "0+3+4+5", 25),
                // B@18 right window created when B@19 processed
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(19, 29)), "0+4", 19),
                // FINAL WINDOW: B@18 right window updated when B@25 processed
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(19, 29)), "0+4+5", 25),
                // FINAL WINDOW: B@19 right window updated when B@25 processed
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(20, 30)), "0+5", 25),
                // FINAL WINDOW: C@11 left window created when C@11 processed
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(1, 11)), "0+1", 11),
                // FINAL WINDOW: C@15 left window created when C@15 processed
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(5, 15)), "0+1+2", 15),
                // FINAL WINDOW: C@16 left window created when C@16 processed
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(6, 16)), "0+1+2+3", 16),
                // FINAL WINDOW: C@21 left window created when C@21 processed
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(11, 21)), "0+1+2+3+4", 21),
                // C@11 right window created when C@15 processed
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(12, 22)), "0+2", 15),
                // C@11 right window updated when C@16 processed
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(12, 22)), "0+2+3", 16),
                // FINAL WINDOW: C@11 right window updated when C@21 processed
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(12, 22)), "0+2+3+4", 21),
                // FINAL WINDOW: C@23 left window created when C@23 processed
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(13, 23)), "0+2+3+4+5", 23),
                // C@15 right window created when C@16 processed
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(16, 26)), "0+3", 16),
                // C@15 right window updated when C@21 processed
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(16, 26)), "0+3+4", 21),
                // FINAL WINDOW: C@15 right window updated when C@23 processed
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(16, 26)), "0+3+4+5", 23),
                // C@16 right window created when C@21 processed
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(17, 27)), "0+4", 21),
                // FINAL WINDOW: C@16 right window updated when C@23 processed
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(17, 27)), "0+4+5", 23),
                // FINAL WINDOW: C@21 right window created when C@23 processed
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(22, 32)), "0+5", 23),
                // FINAL WINDOW: D@11 left window created when D@11 processed
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(1, 11)), "0+4", 11),
                // FINAL WINDOW: D@12 left window created when D@12 processed
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(2, 12)), "0+4+2", 12),
                // FINAL WINDOW: D@16 left window created when D@16 processed
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(6, 16)), "0+4+2+5", 16),
                // D@11 right window created when D@12 processed
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(12, 22)), "0+2", 12),
                // FINAL WINDOW: D@11 right window updated when D@16 processed
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(12, 22)), "0+2+5", 16),
                // FINAL WINDOW: D@12 right window created when D@16 processed
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(13, 23)), "0+5", 16),
                // FINAL WINDOW: D@29 left window created when D@29 processed
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(19, 29)), "0+3", 29)),
            actual
        );
    }

    @Test
    public void testJoin() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";
        final String topic2 = "topic2";
        final WindowBytesStoreSupplier storeSupplier1 =
            inOrderIterator
                ? new InOrderMemoryWindowStoreSupplier("InOrder1", 50000L, 10L, false)
                : Stores.inMemoryWindowStore("Reverse1", Duration.ofMillis(50000), Duration.ofMillis(10), false);
        final WindowBytesStoreSupplier storeSupplier2 =
            inOrderIterator
                ? new InOrderMemoryWindowStoreSupplier("InOrder2", 50000L, 10L, false)
                : Stores.inMemoryWindowStore("Reverse2", Duration.ofMillis(50000), Duration.ofMillis(10), false);

        final KTable<Windowed<String>, String> table1 = builder
            .stream(topic1, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(10), ofMillis(100)))
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                Materialized.as(storeSupplier1)
            );
        final KTable<Windowed<String>, String> table2 = builder
            .stream(topic2, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(10), ofMillis(100)))
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                Materialized.as(storeSupplier2)
            );

        final MockApiProcessorSupplier<Windowed<String>, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table1.toStream().process(supplier);

        table2.toStream().process(supplier);

        table1.join(table2, (p1, p2) -> p1 + "%" + p2).toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                    driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> inputTopic2 =
                    driver.createInputTopic(topic2, new StringSerializer(), new StringSerializer());
            inputTopic1.pipeInput("A", "1", 10L);
            inputTopic1.pipeInput("B", "2", 11L);
            inputTopic1.pipeInput("C", "3", 12L);

            final List<MockApiProcessor<Windowed<String>, String, Void, Void>> processors = supplier.capturedProcessors(3);

            processors.get(0).checkAndClearProcessResult(
                    // left windows created by the first set of records to table 1
                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0, 10)),  "0+1",  10),
                    new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(1, 11)),  "0+2",  11),
                    new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(2, 12)),  "0+3",  12)
            );
            processors.get(1).checkAndClearProcessResult();
            processors.get(2).checkAndClearProcessResult();

            inputTopic1.pipeInput("A", "1", 15L);
            inputTopic1.pipeInput("B", "2", 16L);
            inputTopic1.pipeInput("C", "3", 19L);

            processors.get(0).checkAndClearProcessResult(
                    // right windows from previous records are created, and left windows from new records to table 1
                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(11, 21)),  "0+1",  15),
                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5, 15)),  "0+1+1",  15),
                    new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(12, 22)),  "0+2",  16),
                    new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(6, 16)),  "0+2+2",  16),
                    new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(13, 23)),  "0+3",  19),
                    new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(9, 19)),  "0+3+3",  19)
            );
            processors.get(1).checkAndClearProcessResult();
            processors.get(2).checkAndClearProcessResult();

            inputTopic2.pipeInput("A", "a", 10L);
            inputTopic2.pipeInput("B", "b", 30L);
            inputTopic2.pipeInput("C", "c", 12L);
            inputTopic2.pipeInput("C", "c", 35L);


            processors.get(0).checkAndClearProcessResult();
            processors.get(1).checkAndClearProcessResult(
                    // left windows from first set of records sent to table 2
                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0, 10)),  "0+a",  10),
                    new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(20, 30)),  "0+b",  30),
                    new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(2, 12)),  "0+c",  12),
                    new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(25, 35)),  "0+c",  35)
            );
            processors.get(2).checkAndClearProcessResult(
                    // set of join windows from windows created by table 1 and table 2
                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0, 10)),  "0+1%0+a",  10),
                    new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(2, 12)),  "0+3%0+c",  12)
            );

            inputTopic2.pipeInput("A", "a", 15L);
            inputTopic2.pipeInput("B", "b", 16L);
            inputTopic2.pipeInput("C", "c", 17L);

            processors.get(0).checkAndClearProcessResult();
            processors.get(1).checkAndClearProcessResult(
                    // right windows from previous records are created (where applicable), and left windows from new records to table 2
                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(11, 21)),  "0+a",  15),
                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5, 15)),  "0+a+a",  15),
                    new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(6, 16)),  "0+b",  16),
                    new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(13, 23)),  "0+c",  17),
                    new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(7, 17)),  "0+c+c",  17)
            );
            processors.get(2).checkAndClearProcessResult(
                    // set of join windows from windows created by table 1 and table 2
                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(11, 21)),  "0+1%0+a",  15),
                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5, 15)),  "0+1+1%0+a+a",  15),
                    new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(6, 16)),  "0+2+2%0+b",  16),
                    new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(13, 23)),  "0+3%0+c",  19)
            );
        }
    }

    @Test
    public void testEarlyRecordsSmallInput() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic = "topic";

        final KTable<Windowed<String>, String> table2 = builder
            .stream(topic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(50), ofMillis(200)))
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                Materialized.<String, String, WindowStore<Bytes, byte[]>>as("topic-Canonized").withValueSerde(Serdes.String())
            );
        final MockApiProcessorSupplier<Windowed<String>, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic, new StringSerializer(), new StringSerializer());

            inputTopic.pipeInput("A", "1", 0L);
            inputTopic.pipeInput("A", "2", 5L);
            inputTopic.pipeInput("A", "3", 6L);
            inputTopic.pipeInput("A", "4", 3L);
            inputTopic.pipeInput("A", "5", 13L);
            inputTopic.pipeInput("A", "6", 10L);
        }

        final Map<Long, ValueAndTimestamp<String>> actual = new HashMap<>();
        for (final KeyValueTimestamp<Windowed<String>, String> entry : supplier.theCapturedProcessor().processed()) {
            final Windowed<String> window = entry.key();
            final Long start = window.window().start();
            final ValueAndTimestamp<String> valueAndTimestamp = ValueAndTimestamp.make(entry.value(), entry.timestamp());
            if (actual.putIfAbsent(start, valueAndTimestamp) != null) {
                actual.replace(start, valueAndTimestamp);
            }
        }

        final Map<Long, ValueAndTimestamp<String>> expected = new HashMap<>();
        expected.put(0L, ValueAndTimestamp.make("0+1+2+3+4+5+6", 13L));
        expected.put(1L, ValueAndTimestamp.make("0+2+3+4+5+6", 13L));
        expected.put(4L, ValueAndTimestamp.make("0+2+3+5+6", 13L));
        expected.put(6L, ValueAndTimestamp.make("0+3+5+6", 13L));
        expected.put(7L, ValueAndTimestamp.make("0+5+6", 13L));
        expected.put(11L, ValueAndTimestamp.make("0+5", 13L));

        assertEquals(expected, actual);
    }

    @Test
    public void testEarlyRecordsRepeatedInput() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic = "topic";

        final KTable<Windowed<String>, String> table2 = builder
            .stream(topic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(5), ofMillis(20)))
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                Materialized.<String, String, WindowStore<Bytes, byte[]>>as("topic-Canonized").withValueSerde(Serdes.String())
            );
        final MockApiProcessorSupplier<Windowed<String>, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic, new StringSerializer(), new StringSerializer());

            inputTopic.pipeInput("A", "1", 0L);
            inputTopic.pipeInput("A", "2", 2L);
            inputTopic.pipeInput("A", "3", 4L);
            inputTopic.pipeInput("A", "4", 0L);
            inputTopic.pipeInput("A", "5", 2L);
            inputTopic.pipeInput("A", "6", 2L);
            inputTopic.pipeInput("A", "7", 0L);
        }

        final Map<Long, ValueAndTimestamp<String>> actual = new HashMap<>();
        for (final KeyValueTimestamp<Windowed<String>, String> entry : supplier.theCapturedProcessor().processed()) {
            final Windowed<String> window = entry.key();
            final Long start = window.window().start();
            final ValueAndTimestamp<String> valueAndTimestamp = ValueAndTimestamp.make(entry.value(), entry.timestamp());
            if (actual.putIfAbsent(start, valueAndTimestamp) != null) {
                actual.replace(start, valueAndTimestamp);
            }
        }

        final Map<Long, ValueAndTimestamp<String>> expected = new HashMap<>();
        expected.put(0L, ValueAndTimestamp.make("0+1+2+3+4+5+6+7", 4L));
        expected.put(1L, ValueAndTimestamp.make("0+2+3+5+6", 4L));
        expected.put(3L, ValueAndTimestamp.make("0+3", 4L));
        assertEquals(expected, actual);
    }

    @Test
    public void testEarlyRecordsLargeInput() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic = "topic";
        final WindowBytesStoreSupplier storeSupplier =
            inOrderIterator
                ? new InOrderMemoryWindowStoreSupplier("InOrder", 50000L, 10L, false)
                : Stores.inMemoryWindowStore("Reverse", Duration.ofMillis(50000), Duration.ofMillis(10), false);

        final KTable<Windowed<String>, String> table2 = builder
            .stream(topic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(10), ofMillis(50)))
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                Materialized.as(storeSupplier)
            );

        final MockApiProcessorSupplier<Windowed<String>, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                driver.createInputTopic(topic, new StringSerializer(), new StringSerializer());

            inputTopic1.pipeInput("E", "1", 0L);
            inputTopic1.pipeInput("E", "3", 5L);
            inputTopic1.pipeInput("E", "4", 6L);
            inputTopic1.pipeInput("E", "2", 3L);
            inputTopic1.pipeInput("E", "6", 13L);
            inputTopic1.pipeInput("E", "5", 10L);
            inputTopic1.pipeInput("E", "7", 4L);
            inputTopic1.pipeInput("E", "8", 2L);
            inputTopic1.pipeInput("E", "9", 15L);
        }
        final Comparator<KeyValueTimestamp<Windowed<String>, String>> comparator =
            Comparator.comparing((KeyValueTimestamp<Windowed<String>, String> o) -> o.key().key())
                .thenComparing((KeyValueTimestamp<Windowed<String>, String> o) -> o.key().window().start());

        final ArrayList<KeyValueTimestamp<Windowed<String>, String>> actual = supplier.theCapturedProcessor().processed();
        actual.sort(comparator);
        assertEquals(
            asList(
                // E@0
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(0, 10)), "0+1", 0),
                // E@5
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(0, 10)), "0+1+3", 5),
                // E@6
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(0, 10)), "0+1+3+4", 6),
                // E@3
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(0, 10)), "0+1+3+4+2", 6),
                //E@10
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(0, 10)), "0+1+3+4+2+5", 10),
                //E@4
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(0, 10)), "0+1+3+4+2+5+7", 10),
                //E@2
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(0, 10)), "0+1+3+4+2+5+7+8", 10),
                // E@5
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(1, 11)), "0+3", 5),
                // E@6
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(1, 11)), "0+3+4", 6),
                // E@3
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(1, 11)), "0+3+4+2", 6),
                //E@10
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(1, 11)), "0+3+4+2+5", 10),
                //E@4
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(1, 11)), "0+3+4+2+5+7", 10),
                //E@2
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(1, 11)), "0+3+4+2+5+7+8", 10),
                //E@13
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(3, 13)), "0+3+4+2+6", 13),
                //E@10
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(3, 13)), "0+3+4+2+6+5", 13),
                //E@4
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(3, 13)), "0+3+4+2+6+5+7", 13),
                // E@3
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(4, 14)), "0+3+4", 6),
                //E@13
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(4, 14)), "0+3+4+6", 13),
                //E@10
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(4, 14)), "0+3+4+6+5", 13),
                //E@4
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(4, 14)), "0+3+4+6+5+7", 13),
                //E@4
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(5, 15)), "0+3+4+6+5", 13),
                //E@15
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(5, 15)), "0+3+4+6+5+9", 15),
                // E@6
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(6, 16)), "0+4", 6),
                //E@13
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(6, 16)), "0+4+6", 13),
                //E@10
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(6, 16)), "0+4+6+5", 13),
                //E@15
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(6, 16)), "0+4+6+5+9", 15),
                //E@13
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(7, 17)), "0+6", 13),
                //E@10
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(7, 17)), "0+6+5", 13),
                //E@15
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(7, 17)), "0+6+5+9", 15),
                //E@10
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(11, 21)), "0+6", 13),
                //E@15
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(11, 21)), "0+6+9", 15),
                //E@15
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(14, 24)), "0+9", 15)),
            actual
        );
    }

    @Test
    public void testEarlyNoGracePeriodSmallInput() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic = "topic";

        final KTable<Windowed<String>, String> table2 = builder
            .stream(topic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(ofMillis(50)))
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                Materialized.<String, String, WindowStore<Bytes, byte[]>>as("topic-Canonized").withValueSerde(Serdes.String())
            );
        final MockApiProcessorSupplier<Windowed<String>, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        // all events are considered as early events since record timestamp is less than time difference of the window
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic, new StringSerializer(), new StringSerializer());

            inputTopic.pipeInput("A", "1", 0L);
            inputTopic.pipeInput("A", "2", 5L);
            inputTopic.pipeInput("A", "3", 6L);
            inputTopic.pipeInput("A", "4", 3L);
            inputTopic.pipeInput("A", "5", 13L);
            inputTopic.pipeInput("A", "6", 10L);
        }

        final Map<Long, ValueAndTimestamp<String>> actual = new HashMap<>();
        for (final KeyValueTimestamp<Windowed<String>, String> entry : supplier.theCapturedProcessor().processed()) {
            final Windowed<String> window = entry.key();
            final Long start = window.window().start();
            final ValueAndTimestamp<String> valueAndTimestamp = ValueAndTimestamp.make(entry.value(), entry.timestamp());
            if (actual.putIfAbsent(start, valueAndTimestamp) != null) {
                actual.replace(start, valueAndTimestamp);
            }
        }

        final Map<Long, ValueAndTimestamp<String>> expected = new HashMap<>();
        expected.put(0L, ValueAndTimestamp.make("0+1+2+3+4+5+6", 13L));
        expected.put(1L, ValueAndTimestamp.make("0+2+3+4+5+6", 13L));
        expected.put(4L, ValueAndTimestamp.make("0+2+3+5+6", 13L));
        expected.put(6L, ValueAndTimestamp.make("0+3+5+6", 13L));
        expected.put(7L, ValueAndTimestamp.make("0+5+6", 13L));
        expected.put(11L, ValueAndTimestamp.make("0+5", 13L));

        assertEquals(expected, actual);
    }

    @Test
    public void testNoGracePeriodSmallInput() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic = "topic";

        final KTable<Windowed<String>, String> table2 = builder
                .stream(topic, Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(ofMillis(50)))
                .aggregate(
                        MockInitializer.STRING_INIT,
                        MockAggregator.TOSTRING_ADDER,
                        Materialized.<String, String, WindowStore<Bytes, byte[]>>as("topic-Canonized").withValueSerde(Serdes.String())
                );
        final MockApiProcessorSupplier<Windowed<String>, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic(topic, new StringSerializer(), new StringSerializer());

            inputTopic.pipeInput("A", "1", 100L);
            inputTopic.pipeInput("A", "2", 105L);
            inputTopic.pipeInput("A", "3", 106L);
            inputTopic.pipeInput("A", "4", 103L);
            inputTopic.pipeInput("A", "5", 113L);
            inputTopic.pipeInput("A", "6", 110L);
        }

        final Map<Long, ValueAndTimestamp<String>> actual = new HashMap<>();
        for (final KeyValueTimestamp<Windowed<String>, String> entry : supplier.theCapturedProcessor().processed()) {
            final Windowed<String> window = entry.key();
            final Long start = window.window().start();
            final ValueAndTimestamp<String> valueAndTimestamp = ValueAndTimestamp.make(entry.value(), entry.timestamp());
            if (actual.putIfAbsent(start, valueAndTimestamp) != null) {
                actual.replace(start, valueAndTimestamp);
            }
        }

        final Map<Long, ValueAndTimestamp<String>> expected = new HashMap<>();
        expected.put(50L, ValueAndTimestamp.make("0+1", 100L));
        expected.put(55L, ValueAndTimestamp.make("0+1+2", 105L));
        expected.put(56L, ValueAndTimestamp.make("0+1+2+3+4", 106L));
        expected.put(63L, ValueAndTimestamp.make("0+1+2+3+4+5+6", 113L));
        expected.put(101L, ValueAndTimestamp.make("0+2+3+4+5+6", 113L));
        expected.put(104L, ValueAndTimestamp.make("0+2+3+5+6", 113L));
        expected.put(106L, ValueAndTimestamp.make("0+3+5+6", 113L));
        expected.put(107L, ValueAndTimestamp.make("0+5+6", 113L));
        expected.put(111L, ValueAndTimestamp.make("0+5", 113L));

        assertEquals(expected, actual);
    }

    @Test
    public void testEarlyNoGracePeriodLargeInput() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic = "topic";
        final WindowBytesStoreSupplier storeSupplier =
                inOrderIterator
                        ? new InOrderMemoryWindowStoreSupplier("InOrder", 500L, 10L, false)
                        : Stores.inMemoryWindowStore("Reverse", Duration.ofMillis(500), Duration.ofMillis(10), false);

        final KTable<Windowed<String>, String> table2 = builder
                .stream(topic, Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(ofMillis(10)))
                .aggregate(
                        MockInitializer.STRING_INIT,
                        MockAggregator.TOSTRING_ADDER,
                        Materialized.as(storeSupplier)
                );

        final MockApiProcessorSupplier<Windowed<String>, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                    driver.createInputTopic(topic, new StringSerializer(), new StringSerializer());

            inputTopic1.pipeInput("E", "1", 0L);
            inputTopic1.pipeInput("E", "3", 5L);
            inputTopic1.pipeInput("E", "4", 6L);
            inputTopic1.pipeInput("E", "2", 3L);
            inputTopic1.pipeInput("E", "6", 13L);
            inputTopic1.pipeInput("E", "5", 10L);
            inputTopic1.pipeInput("E", "7", 4L);
            inputTopic1.pipeInput("E", "8", 2L);
            inputTopic1.pipeInput("E", "9", 15L);
        }
        final Comparator<KeyValueTimestamp<Windowed<String>, String>> comparator =
                Comparator.comparing((KeyValueTimestamp<Windowed<String>, String> o) -> o.key().key())
                        .thenComparing((KeyValueTimestamp<Windowed<String>, String> o) -> o.key().window().start());

        final ArrayList<KeyValueTimestamp<Windowed<String>, String>> actual = supplier.theCapturedProcessor().processed();
        actual.sort(comparator);
        assertEquals(
            asList(
                // E@0
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(0, 10)), "0+1", 0),
                // E@5
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(0, 10)), "0+1+3", 5),
                // E@6
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(0, 10)), "0+1+3+4", 6),
                // E@3
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(0, 10)), "0+1+3+4+2", 6),
                // E@5
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(1, 11)), "0+3", 5),
                // E@6
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(1, 11)), "0+3+4", 6),
                // E@3
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(1, 11)), "0+3+4+2", 6),
                //E@13
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(3, 13)), "0+3+4+2+6", 13),
                //E@10
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(3, 13)), "0+3+4+2+6+5", 13),
                //E@4
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(3, 13)), "0+3+4+2+6+5+7", 13),
                // E@3
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(4, 14)), "0+3+4", 6),
                //E@13
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(4, 14)), "0+3+4+6", 13),
                //E@10
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(4, 14)), "0+3+4+6+5", 13),
                //E@4
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(4, 14)), "0+3+4+6+5+7", 13),
                //E@4
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(5, 15)), "0+3+4+6+5", 13),
                //E@15
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(5, 15)), "0+3+4+6+5+9", 15),
                // E@6
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(6, 16)), "0+4", 6),
                //E@13
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(6, 16)), "0+4+6", 13),
                //E@10
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(6, 16)), "0+4+6+5", 13),
                //E@15
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(6, 16)), "0+4+6+5+9", 15),
                //E@13
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(7, 17)), "0+6", 13),
                //E@10
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(7, 17)), "0+6+5", 13),
                //E@15
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(7, 17)), "0+6+5+9", 15),
                //E@10
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(11, 21)), "0+6", 13),
                //E@15
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(11, 21)), "0+6+9", 15),
                //E@15
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(14, 24)), "0+9", 15)),
            actual
        );
    }

    @Test
    public void testNoGracePeriodLargeInput() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic = "topic";
        final WindowBytesStoreSupplier storeSupplier =
                inOrderIterator
                        ? new InOrderMemoryWindowStoreSupplier("InOrder", 500L, 10L, false)
                        : Stores.inMemoryWindowStore("Reverse", Duration.ofMillis(500), Duration.ofMillis(10), false);

        final KTable<Windowed<String>, String> table2 = builder
                .stream(topic, Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(ofMillis(10)))
                .aggregate(
                        MockInitializer.STRING_INIT,
                        MockAggregator.TOSTRING_ADDER,
                        Materialized.as(storeSupplier)
                );

        final MockApiProcessorSupplier<Windowed<String>, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                    driver.createInputTopic(topic, new StringSerializer(), new StringSerializer());

            inputTopic1.pipeInput("E", "1", 100L);
            inputTopic1.pipeInput("E", "3", 105L);
            inputTopic1.pipeInput("E", "4", 106L);
            inputTopic1.pipeInput("E", "2", 103L);
            inputTopic1.pipeInput("E", "6", 113L);
            inputTopic1.pipeInput("E", "5", 110L);
            inputTopic1.pipeInput("E", "7", 104L);
            inputTopic1.pipeInput("E", "8", 102L);
            inputTopic1.pipeInput("E", "9", 115L);
        }
        final Comparator<KeyValueTimestamp<Windowed<String>, String>> comparator =
                Comparator.comparing((KeyValueTimestamp<Windowed<String>, String> o) -> o.key().key())
                        .thenComparing((KeyValueTimestamp<Windowed<String>, String> o) -> o.key().window().start());

        final ArrayList<KeyValueTimestamp<Windowed<String>, String>> actual = supplier.theCapturedProcessor().processed();
        actual.sort(comparator);
        assertEquals(
            asList(
                // E@0
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(90, 100)), "0+1", 100),
                // E@5
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(95, 105)), "0+1+3", 105),
                // E@6
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(96, 106)), "0+1+3+4", 106),
                // E@3
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(96, 106)), "0+1+3+4+2", 106),
                // E@5
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(101, 111)), "0+3", 105),
                // E@6
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(101, 111)), "0+3+4", 106),
                // E@3
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(101, 111)), "0+3+4+2", 106),
                //E@13
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(103, 113)), "0+3+4+2+6", 113),
                //E@10
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(103, 113)), "0+3+4+2+6+5", 113),
                //E@4
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(103, 113)), "0+3+4+2+6+5+7", 113),
                // E@3
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(104, 114)), "0+3+4", 106),
                //E@13
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(104, 114)), "0+3+4+6", 113),
                //E@10
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(104, 114)), "0+3+4+6+5", 113),
                //E@4
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(104, 114)), "0+3+4+6+5+7", 113),
                //E@4
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(105, 115)), "0+3+4+6+5", 113),
                //E@15
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(105, 115)), "0+3+4+6+5+9", 115),
                // E@6
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(106, 116)), "0+4", 106),
                //E@13
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(106, 116)), "0+4+6", 113),
                //E@10
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(106, 116)), "0+4+6+5", 113),
                //E@15
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(106, 116)), "0+4+6+5+9", 115),
                //E@13
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(107, 117)), "0+6", 113),
                //E@10
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(107, 117)), "0+6+5", 113),
                //E@15
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(107, 117)), "0+6+5+9", 115),
                //E@10
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(111, 121)), "0+6", 113),
                //E@15
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(111, 121)), "0+6+9", 115),
                //E@15
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(114, 124)), "0+9", 115)),
            actual
        );
    }

    @Test
    public void shouldLogAndMeterWhenSkippingNullKey() {
        final String builtInMetricsVersion = StreamsConfig.METRICS_LATEST;
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic = "topic";
        builder
                .stream(topic, Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(10), ofMillis(100)))
                .aggregate(MockInitializer.STRING_INIT, MockAggregator.toStringInstance("+"), Materialized.<String, String, WindowStore<Bytes, byte[]>>as("topic1-Canonicalized").withValueSerde(Serdes.String()));

        props.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, builtInMetricsVersion);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KStreamSlidingWindowAggregate.class);
             final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic(topic, new StringSerializer(), new StringSerializer());
            inputTopic.pipeInput(null, "1");
            assertThat(
                appender.getEvents().stream()
                    .filter(e -> e.getLevel().equals("WARN"))
                    .map(Event::getMessage)
                    .collect(Collectors.toList()),
                hasItem("Skipping record due to null key or value. topic=[topic] partition=[0] offset=[0]")
            );
        }
    }

    @Test
    public void shouldLogAndMeterWhenSkippingExpiredWindowByGrace() {
        final String builtInMetricsVersion = StreamsConfig.METRICS_LATEST;
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic = "topic";
        final WindowBytesStoreSupplier storeSupplier =
            inOrderIterator
                ? new InOrderMemoryWindowStoreSupplier("InOrder", 50000L, 10L, false)
                : Stores.inMemoryWindowStore("Reverse", Duration.ofMillis(50000), Duration.ofMillis(10), false);

        final KStream<String, String> stream1 = builder.stream(topic, Consumed.with(Serdes.String(), Serdes.String()));
        stream1.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(10), ofMillis(90)))
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                Materialized.as(storeSupplier)
            )
            .toStream()
            .to("output");

        props.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, builtInMetricsVersion);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KStreamSlidingWindowAggregate.class);
             final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {

            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic(topic, new StringSerializer(), new StringSerializer());
            inputTopic.pipeInput("k", "100", 200L);
            inputTopic.pipeInput("k", "0", 100L);
            inputTopic.pipeInput("k", "1", 101L);
            inputTopic.pipeInput("k", "2", 102L);
            inputTopic.pipeInput("k", "3", 103L);
            inputTopic.pipeInput("k", "4", 104L);
            inputTopic.pipeInput("k", "5", 105L);
            inputTopic.pipeInput("k", "6", 15L);

            assertLatenessMetrics(driver, is(7.0), is(185.0), is(96.25));

            assertThat(appender.getMessages(), hasItems(
                    // left window for k@100
                    "Skipping record for expired window. topic=[topic] partition=[0] offset=[1] timestamp=[100] window=[90,100] expiration=[110] streamTime=[200]",
                    // left window for k@101
                    "Skipping record for expired window. topic=[topic] partition=[0] offset=[2] timestamp=[101] window=[91,101] expiration=[110] streamTime=[200]",
                    // left window for k@102
                    "Skipping record for expired window. topic=[topic] partition=[0] offset=[3] timestamp=[102] window=[92,102] expiration=[110] streamTime=[200]",
                    // left window for k@103
                    "Skipping record for expired window. topic=[topic] partition=[0] offset=[4] timestamp=[103] window=[93,103] expiration=[110] streamTime=[200]",
                    // left window for k@104
                    "Skipping record for expired window. topic=[topic] partition=[0] offset=[5] timestamp=[104] window=[94,104] expiration=[110] streamTime=[200]",
                    // left window for k@105
                    "Skipping record for expired window. topic=[topic] partition=[0] offset=[6] timestamp=[105] window=[95,105] expiration=[110] streamTime=[200]",
                    // left window for k@15
                    "Skipping record for expired window. topic=[topic] partition=[0] offset=[7] timestamp=[15] window=[5,15] expiration=[110] streamTime=[200]"
            ));
            final TestOutputTopic<Windowed<String>, String> outputTopic =
                    driver.createOutputTopic("output", new TimeWindowedDeserializer<>(new StringDeserializer(), 10L), new StringDeserializer());
            assertThat(outputTopic.readRecord(), equalTo(new TestRecord<>(new Windowed<>("k", new TimeWindow(190, 200)), "0+100", null, 200L)));
            assertTrue(outputTopic.isEmpty());
        }
    }

    @Test
    public void testAggregateRandomInput() {

        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";
        final WindowBytesStoreSupplier storeSupplier =
            inOrderIterator
                ? new InOrderMemoryWindowStoreSupplier("InOrder", 50000L, 10L, false)
                : Stores.inMemoryWindowStore("Reverse", Duration.ofMillis(50000), Duration.ofMillis(10), false);

        final KTable<Windowed<String>, String> table = builder
            .stream(topic1, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(10), ofMillis(10000)))
            // The aggregator needs to sort the strings so the window value is the same for the final windows even when
            // records are processed in a different order. Here, we sort alphabetically.
            .aggregate(
                () -> "",
                (key, value, aggregate) -> {
                    aggregate += value;
                    final char[] ch = aggregate.toCharArray();
                    Arrays.sort(ch);
                    aggregate = String.valueOf(ch);
                    return aggregate;
                },
                Materialized.as(storeSupplier)
            );
        final MockApiProcessorSupplier<Windowed<String>, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table.toStream().process(supplier);
        final long seed = new Random().nextLong();
        final Random shuffle = new Random(seed);

        try {

            final List<ValueAndTimestamp<String>> input = Arrays.asList(
                ValueAndTimestamp.make("A", 10L),
                ValueAndTimestamp.make("B", 15L),
                ValueAndTimestamp.make("C", 16L),
                ValueAndTimestamp.make("D", 18L),
                ValueAndTimestamp.make("E", 30L),
                ValueAndTimestamp.make("F", 40L),
                ValueAndTimestamp.make("G", 55L),
                ValueAndTimestamp.make("H", 56L),
                ValueAndTimestamp.make("I", 58L),
                ValueAndTimestamp.make("J", 58L),
                ValueAndTimestamp.make("K", 62L),
                ValueAndTimestamp.make("L", 63L),
                ValueAndTimestamp.make("M", 63L),
                ValueAndTimestamp.make("N", 63L),
                ValueAndTimestamp.make("O", 76L),
                ValueAndTimestamp.make("P", 77L),
                ValueAndTimestamp.make("Q", 80L),
                ValueAndTimestamp.make("R", 2L),
                ValueAndTimestamp.make("S", 3L),
                ValueAndTimestamp.make("T", 5L),
                ValueAndTimestamp.make("U", 8L)
                );

            Collections.shuffle(input, shuffle);
            try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
                final TestInputTopic<String, String> inputTopic1 =
                    driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer());
                for (final ValueAndTimestamp<String> i : input) {
                    inputTopic1.pipeInput("A", i.value(), i.timestamp());
                }
            }

            final Map<Long, ValueAndTimestamp<String>> results = new HashMap<>();

            for (final KeyValueTimestamp<Windowed<String>, String> entry : supplier.theCapturedProcessor().processed()) {
                final Windowed<String> window = entry.key();
                final Long start = window.window().start();
                final ValueAndTimestamp<String> valueAndTimestamp = ValueAndTimestamp.make(entry.value(), entry.timestamp());
                if (results.putIfAbsent(start, valueAndTimestamp) != null) {
                    results.replace(start, valueAndTimestamp);
                }
            }
            verifyRandomTestResults(results);
        } catch (final AssertionError t) {
            throw new AssertionError(
                "Assertion failed in randomized test. Reproduce with seed: " + seed + ".",
                t
            );
        } catch (final Throwable t) {
            final String msg = "Exception in randomized scenario. Reproduce with seed: " + seed + ".";
            throw new AssertionError(msg, t);
        }
    }

    private void verifyRandomTestResults(final Map<Long, ValueAndTimestamp<String>> actual) {
        final Map<Long, ValueAndTimestamp<String>> expected = new HashMap<>();
        expected.put(0L, ValueAndTimestamp.make("ARSTU", 10L));
        expected.put(3L, ValueAndTimestamp.make("ASTU", 10L));
        expected.put(4L, ValueAndTimestamp.make("ATU", 10L));
        expected.put(5L, ValueAndTimestamp.make("ABTU", 15L));
        expected.put(6L, ValueAndTimestamp.make("ABCU", 16L));
        expected.put(8L, ValueAndTimestamp.make("ABCDU", 18L));
        expected.put(9L, ValueAndTimestamp.make("ABCD", 18L));
        expected.put(11L, ValueAndTimestamp.make("BCD", 18L));
        expected.put(16L, ValueAndTimestamp.make("CD", 18L));
        expected.put(17L, ValueAndTimestamp.make("D", 18L));
        expected.put(20L, ValueAndTimestamp.make("E", 30L));
        expected.put(30L, ValueAndTimestamp.make("EF", 40L));
        expected.put(31L, ValueAndTimestamp.make("F", 40L));
        expected.put(45L, ValueAndTimestamp.make("G", 55L));
        expected.put(46L, ValueAndTimestamp.make("GH", 56L));
        expected.put(48L, ValueAndTimestamp.make("GHIJ", 58L));
        expected.put(52L, ValueAndTimestamp.make("GHIJK", 62L));
        expected.put(53L, ValueAndTimestamp.make("GHIJKLMN", 63L));
        expected.put(56L, ValueAndTimestamp.make("HIJKLMN", 63L));
        expected.put(57L, ValueAndTimestamp.make("IJKLMN", 63L));
        expected.put(59L, ValueAndTimestamp.make("KLMN", 63L));
        expected.put(63L, ValueAndTimestamp.make("LMN", 63L));
        expected.put(66L, ValueAndTimestamp.make("O", 76L));
        expected.put(67L, ValueAndTimestamp.make("OP", 77L));
        expected.put(70L, ValueAndTimestamp.make("OPQ", 80L));
        expected.put(77L, ValueAndTimestamp.make("PQ", 80L));
        expected.put(78L, ValueAndTimestamp.make("Q", 80L));

        assertEquals(expected, actual);
    }

    private void assertLatenessMetrics(final TopologyTestDriver driver,
                                       final Matcher<Object> dropTotal,
                                       final Matcher<Object> maxLateness,
                                       final Matcher<Object> avgLateness) {

        final MetricName dropTotalMetric;
        final MetricName dropRateMetric;
        final MetricName latenessMaxMetric;
        final MetricName latenessAvgMetric;
        dropTotalMetric = new MetricName(
                "dropped-records-total",
                "stream-task-metrics",
                "The total number of dropped records",
                mkMap(
                        mkEntry("thread-id", threadId),
                        mkEntry("task-id", "0_0")
                )
        );
        dropRateMetric = new MetricName(
                "dropped-records-rate",
                "stream-task-metrics",
                "The average number of dropped records per second",
                mkMap(
                        mkEntry("thread-id", threadId),
                        mkEntry("task-id", "0_0")
                )
        );
        latenessMaxMetric = new MetricName(
                "record-lateness-max",
                "stream-task-metrics",
                "The observed maximum lateness of records in milliseconds, measured by comparing the record "
                        + "timestamp with the current stream time",
                mkMap(
                        mkEntry("thread-id", threadId),
                        mkEntry("task-id", "0_0")
                )
        );
        latenessAvgMetric = new MetricName(
                "record-lateness-avg",
                "stream-task-metrics",
                "The observed average lateness of records in milliseconds, measured by comparing the record "
                        + "timestamp with the current stream time",
                mkMap(
                        mkEntry("thread-id", threadId),
                        mkEntry("task-id", "0_0")
                )
        );
        assertThat(driver.metrics().get(dropTotalMetric).metricValue(), dropTotal);
        assertThat(driver.metrics().get(dropRateMetric).metricValue(), not(0.0));
        assertThat(driver.metrics().get(latenessMaxMetric).metricValue(), maxLateness);
        assertThat(driver.metrics().get(latenessAvgMetric).metricValue(), avgLateness);
    }

    private static class InOrderMemoryWindowStore extends InMemoryWindowStore {
        InOrderMemoryWindowStore(final String name,
                        final long retentionPeriod,
                        final long windowSize,
                        final boolean retainDuplicates,
                        final String metricScope) {
            super(name, retentionPeriod, windowSize, retainDuplicates, metricScope);
        }

        @Override
        public WindowStoreIterator<byte[]> backwardFetch(final Bytes key, final long timeFrom, final long timeTo) {
            throw new UnsupportedOperationException("Backward fetch not supported here");
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom,
                                                                       final Bytes keyTo,
                                                                       final long timeFrom,
                                                                       final long timeTo) {
            throw new UnsupportedOperationException("Backward fetch not supported here");
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final long timeFrom, final long timeTo) {
            throw new UnsupportedOperationException("Backward fetch not supported here");
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
            throw new UnsupportedOperationException("Backward fetch not supported here");
        }
    }

    private static class InOrderMemoryWindowStoreSupplier extends InMemoryWindowBytesStoreSupplier {

        InOrderMemoryWindowStoreSupplier(final String name,
                                         final long retentionPeriod,
                                         final long windowSize,
                                         final boolean retainDuplicates) {
            super(name, retentionPeriod, windowSize, retainDuplicates);
        }

        @Override
        public WindowStore<Bytes, byte[]> get() {
            return new InOrderMemoryWindowStore(name(),
                retentionPeriod(),
                windowSize(),
                retainDuplicates(),
                metricsScope());
        }
    }
}
