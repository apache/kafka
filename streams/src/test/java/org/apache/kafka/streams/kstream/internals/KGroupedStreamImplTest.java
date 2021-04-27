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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.test.StreamsTestUtils.getMetricByName;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;


public class KGroupedStreamImplTest {

    private static final String TOPIC = "topic";
    private static final String INVALID_STORE_NAME = "~foo bar~";
    private final StreamsBuilder builder = new StreamsBuilder();
    private KGroupedStream<String, String> groupedStream;

    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    @Before
    public void before() {
        final KStream<String, String> stream = builder.stream(TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        groupedStream = stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
    }

    @Test
    public void shouldNotHaveNullAggregatorOnCogroup() {
        assertThrows(NullPointerException.class, () ->  groupedStream.cogroup(null));
    }

    @Test
    public void shouldNotHaveNullReducerOnReduce() {
        assertThrows(NullPointerException.class, () ->  groupedStream.reduce(null));
    }

    @Test
    public void shouldNotHaveInvalidStoreNameOnReduce() {
        assertThrows(TopologyException.class, () ->  groupedStream.reduce(MockReducer.STRING_ADDER, Materialized.as(INVALID_STORE_NAME)));
    }

    @Test
    public void shouldNotHaveNullReducerWithWindowedReduce() {
        assertThrows(NullPointerException.class, () ->  groupedStream
                .windowedBy(TimeWindows.of(ofMillis(10)))
                .reduce(null, Materialized.as("store")));
    }

    @Test
    public void shouldNotHaveNullWindowsWithWindowedReduce() {
        assertThrows(NullPointerException.class, () ->  groupedStream.windowedBy((Windows<?>) null));
    }

    @Test
    public void shouldNotHaveInvalidStoreNameWithWindowedReduce() {
        assertThrows(TopologyException.class, () ->  groupedStream
                .windowedBy(TimeWindows.of(ofMillis(10)))
                .reduce(MockReducer.STRING_ADDER, Materialized.as(INVALID_STORE_NAME)));
    }

    @Test
    public void shouldNotHaveNullInitializerOnAggregate() {
        assertThrows(NullPointerException.class, () ->  groupedStream.aggregate(null, MockAggregator.TOSTRING_ADDER, Materialized.as("store")));
    }

    @Test
    public void shouldNotHaveNullAdderOnAggregate() {
        assertThrows(NullPointerException.class, () ->  groupedStream.aggregate(MockInitializer.STRING_INIT, null, Materialized.as("store")));
    }

    @Test
    public void shouldNotHaveInvalidStoreNameOnAggregate() {
        assertThrows(TopologyException.class, () ->  groupedStream.aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                Materialized.as(INVALID_STORE_NAME)));
    }

    @Test
    public void shouldNotHaveNullInitializerOnWindowedAggregate() {
        assertThrows(NullPointerException.class, () ->  groupedStream
                .windowedBy(TimeWindows.of(ofMillis(10)))
                .aggregate(null, MockAggregator.TOSTRING_ADDER, Materialized.as("store")));
    }

    @Test
    public void shouldNotHaveNullAdderOnWindowedAggregate() {
        assertThrows(NullPointerException.class, () ->  groupedStream
                .windowedBy(TimeWindows.of(ofMillis(10)))
                .aggregate(MockInitializer.STRING_INIT, null, Materialized.as("store")));
    }

    @Test
    public void shouldNotHaveNullWindowsOnWindowedAggregate() {
        assertThrows(NullPointerException.class, () ->  groupedStream.windowedBy((Windows<?>) null));
    }

    @Test
    public void shouldNotHaveInvalidStoreNameOnWindowedAggregate() {
        assertThrows(TopologyException.class, () ->  groupedStream
                .windowedBy(TimeWindows.of(ofMillis(10)))
                .aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, Materialized.as(INVALID_STORE_NAME)));
    }

    @Test
    public void shouldNotHaveNullReducerWithSlidingWindowedReduce() {
        assertThrows(NullPointerException.class, () ->  groupedStream
                .windowedBy(SlidingWindows.withTimeDifferenceAndGrace(ofMillis(10), ofMillis(100)))
                .reduce(null, Materialized.as("store")));
    }

    @Test
    public void shouldNotHaveNullWindowsWithSlidingWindowedReduce() {
        assertThrows(NullPointerException.class, () ->  groupedStream.windowedBy((SlidingWindows) null));
    }

    @Test
    public void shouldNotHaveInvalidStoreNameWithSlidingWindowedReduce() {
        assertThrows(TopologyException.class, () ->  groupedStream
                .windowedBy(SlidingWindows.withTimeDifferenceAndGrace(ofMillis(10), ofMillis(100)))
                .reduce(MockReducer.STRING_ADDER, Materialized.as(INVALID_STORE_NAME)));
    }

    @Test
    public void shouldNotHaveNullInitializerOnSlidingWindowedAggregate() {
        assertThrows(NullPointerException.class, () ->  groupedStream
                .windowedBy(SlidingWindows.withTimeDifferenceAndGrace(ofMillis(10), ofMillis(100)))
                .aggregate(null, MockAggregator.TOSTRING_ADDER, Materialized.as("store")));
    }

    @Test
    public void shouldNotHaveNullAdderOnSlidingWindowedAggregate() {
        assertThrows(NullPointerException.class, () ->  groupedStream
                .windowedBy(SlidingWindows.withTimeDifferenceAndGrace(ofMillis(10), ofMillis(100)))
                .aggregate(MockInitializer.STRING_INIT, null, Materialized.as("store")));
    }

    @Test
    public void shouldNotHaveInvalidStoreNameOnSlidingWindowedAggregate() {
        assertThrows(TopologyException.class, () ->  groupedStream
                .windowedBy(SlidingWindows.withTimeDifferenceAndGrace(ofMillis(10), ofMillis(100)))
                .aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, Materialized.as(INVALID_STORE_NAME)));
    }

    @Test
    public void shouldCountSlidingWindows() {
        final MockProcessorSupplier<Windowed<String>, Long> supplier = new MockProcessorSupplier<>();
        groupedStream
                .windowedBy(SlidingWindows.withTimeDifferenceAndGrace(ofMillis(500L), ofMillis(2000L)))
                .count(Materialized.as("aggregate-by-key-windowed"))
                .toStream()
                .process(supplier);

        doCountSlidingWindows(supplier);
    }

    @Test
    public void shouldCountSlidingWindowsWithInternalStoreName() {
        final MockProcessorSupplier<Windowed<String>, Long> supplier = new MockProcessorSupplier<>();
        groupedStream
                .windowedBy(SlidingWindows.withTimeDifferenceAndGrace(ofMillis(500L), ofMillis(2000L)))
                .count()
                .toStream()
                .process(supplier);

        doCountSlidingWindows(supplier);
    }

    private void doCountSlidingWindows(final  MockProcessorSupplier<Windowed<String>, Long> supplier) {
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic(TOPIC, new StringSerializer(), new StringSerializer());
            inputTopic.pipeInput("1", "A", 500L);
            inputTopic.pipeInput("1", "A", 999L);
            inputTopic.pipeInput("1", "A", 600L);
            inputTopic.pipeInput("2", "B", 500L);
            inputTopic.pipeInput("2", "B", 600L);
            inputTopic.pipeInput("2", "B", 700L);
            inputTopic.pipeInput("3", "C", 501L);
            inputTopic.pipeInput("1", "A", 1000L);
            inputTopic.pipeInput("1", "A", 1000L);
            inputTopic.pipeInput("2", "B", 1000L);
            inputTopic.pipeInput("2", "B", 1000L);
            inputTopic.pipeInput("3", "C", 600L);
        }

        final Comparator<KeyValueTimestamp<Windowed<String>, Long>> comparator =
            Comparator.comparing((KeyValueTimestamp<Windowed<String>, Long> o) -> o.key().key())
                .thenComparing((KeyValueTimestamp<Windowed<String>, Long> o) -> o.key().window().start());

        final ArrayList<KeyValueTimestamp<Windowed<String>, Long>> actual = supplier.theCapturedProcessor().processed();
        actual.sort(comparator);

        assertThat(actual, equalTo(Arrays.asList(
            // processing A@500
            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(0L, 500L)), 1L, 500L),
            // processing A@600
            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(100L, 600L)), 2L, 600L),
            // processing A@999
            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(499L, 999L)), 2L, 999L),
            // processing A@600
            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(499L, 999L)), 3L, 999L),
            // processing first A@1000
            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(500L, 1000L)), 4L, 1000L),
            // processing second A@1000
            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(500L, 1000L)), 5L, 1000L),
            // processing A@999
            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(501L, 1001L)), 1L, 999L),
            // processing A@600
            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(501L, 1001L)), 2L, 999L),
            // processing first A@1000
            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(501L, 1001L)), 3L, 1000L),
            // processing second A@1000
            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(501L, 1001L)), 4L, 1000L),
            // processing A@600
            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(601L, 1101L)), 1L, 999L),
            // processing first A@1000
            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(601L, 1101L)), 2L, 1000L),
            // processing second A@1000
            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(601L, 1101L)), 3L, 1000L),
            // processing first A@1000
            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(1000L, 1500L)), 1L, 1000L),
            // processing second A@1000
            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(1000L, 1500L)), 2L, 1000L),

            // processing B@500
            new KeyValueTimestamp<>(new Windowed<>("2", new TimeWindow(0L, 500L)), 1L, 500L),
            // processing B@600
            new KeyValueTimestamp<>(new Windowed<>("2", new TimeWindow(100L, 600L)), 2L, 600L),
            // processing B@700
            new KeyValueTimestamp<>(new Windowed<>("2", new TimeWindow(200L, 700L)), 3L, 700L),
            // processing first B@1000
            new KeyValueTimestamp<>(new Windowed<>("2", new TimeWindow(500L, 1000L)), 4L, 1000L),
            // processing second B@1000
            new KeyValueTimestamp<>(new Windowed<>("2", new TimeWindow(500L, 1000L)), 5L, 1000L),
            // processing B@600
            new KeyValueTimestamp<>(new Windowed<>("2", new TimeWindow(501L, 1001L)), 1L, 600L),
            // processing B@700
            new KeyValueTimestamp<>(new Windowed<>("2", new TimeWindow(501L, 1001L)), 2L, 700L),
            // processing first B@1000
            new KeyValueTimestamp<>(new Windowed<>("2", new TimeWindow(501L, 1001L)), 3L, 1000L),
            // processing second B@1000
            new KeyValueTimestamp<>(new Windowed<>("2", new TimeWindow(501L, 1001L)), 4L, 1000L),
            // processing B@700
            new KeyValueTimestamp<>(new Windowed<>("2", new TimeWindow(601L, 1101L)), 1L, 700L),
            // processing first B@1000
            new KeyValueTimestamp<>(new Windowed<>("2", new TimeWindow(601L, 1101)), 2L, 1000L),
            // processing second B@1000
            new KeyValueTimestamp<>(new Windowed<>("2", new TimeWindow(601L, 1101)), 3L, 1000L),
            // processing first B@1000
            new KeyValueTimestamp<>(new Windowed<>("2", new TimeWindow(701L, 1201L)), 1L, 1000L),
            // processing second B@1000
            new KeyValueTimestamp<>(new Windowed<>("2", new TimeWindow(701L, 1201L)), 2L, 1000L),

            // processing C@501
            new KeyValueTimestamp<>(new Windowed<>("3", new TimeWindow(1L, 501L)), 1L, 501L),
            // processing C@600
            new KeyValueTimestamp<>(new Windowed<>("3", new TimeWindow(100L, 600L)), 2L, 600L),
            // processing C@600
            new KeyValueTimestamp<>(new Windowed<>("3", new TimeWindow(502L, 1002L)), 1L, 600L)
        )));
    }

    private void doAggregateSessionWindows(final MockProcessorSupplier<Windowed<String>, Integer> supplier) {
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic(TOPIC, new StringSerializer(), new StringSerializer());
            inputTopic.pipeInput("1", "1", 10);
            inputTopic.pipeInput("2", "2", 15);
            inputTopic.pipeInput("1", "1", 30);
            inputTopic.pipeInput("1", "1", 70);
            inputTopic.pipeInput("1", "1", 100);
            inputTopic.pipeInput("1", "1", 90);
        }
        final Map<Windowed<String>, ValueAndTimestamp<Integer>> result
            = supplier.theCapturedProcessor().lastValueAndTimestampPerKey();
        assertEquals(
            ValueAndTimestamp.make(2, 30L),
            result.get(new Windowed<>("1", new SessionWindow(10L, 30L))));
        assertEquals(
            ValueAndTimestamp.make(1, 15L),
            result.get(new Windowed<>("2", new SessionWindow(15L, 15L))));
        assertEquals(
            ValueAndTimestamp.make(3, 100L),
            result.get(new Windowed<>("1", new SessionWindow(70L, 100L))));
    }

    @Test
    public void shouldAggregateSessionWindows() {
        final MockProcessorSupplier<Windowed<String>, Integer> supplier = new MockProcessorSupplier<>();
        final KTable<Windowed<String>, Integer> table = groupedStream
            .windowedBy(SessionWindows.with(ofMillis(30)))
            .aggregate(
                () -> 0,
                (aggKey, value, aggregate) -> aggregate + 1,
                (aggKey, aggOne, aggTwo) -> aggOne + aggTwo,
                Materialized
                    .<String, Integer, SessionStore<Bytes, byte[]>>as("session-store").
                    withValueSerde(Serdes.Integer()));
        table.toStream().process(supplier);

        doAggregateSessionWindows(supplier);
        assertEquals(table.queryableStoreName(), "session-store");
    }

    @Test
    public void shouldAggregateSessionWindowsWithInternalStoreName() {
        final MockProcessorSupplier<Windowed<String>, Integer> supplier = new MockProcessorSupplier<>();
        final KTable<Windowed<String>, Integer> table = groupedStream
            .windowedBy(SessionWindows.with(ofMillis(30)))
            .aggregate(
                () -> 0,
                (aggKey, value, aggregate) -> aggregate + 1,
                (aggKey, aggOne, aggTwo) -> aggOne + aggTwo,
                Materialized.with(null, Serdes.Integer()));
        table.toStream().process(supplier);

        doAggregateSessionWindows(supplier);
    }

    private void doCountSessionWindows(final MockProcessorSupplier<Windowed<String>, Long> supplier) {

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic(TOPIC, new StringSerializer(), new StringSerializer());
            inputTopic.pipeInput("1", "1", 10);
            inputTopic.pipeInput("2", "2", 15);
            inputTopic.pipeInput("1", "1", 30);
            inputTopic.pipeInput("1", "1", 70);
            inputTopic.pipeInput("1", "1", 100);
            inputTopic.pipeInput("1", "1", 90);
        }
        final Map<Windowed<String>, ValueAndTimestamp<Long>> result =
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey();
        assertEquals(
            ValueAndTimestamp.make(2L, 30L),
            result.get(new Windowed<>("1", new SessionWindow(10L, 30L))));
        assertEquals(
            ValueAndTimestamp.make(1L, 15L),
            result.get(new Windowed<>("2", new SessionWindow(15L, 15L))));
        assertEquals(
            ValueAndTimestamp.make(3L, 100L),
            result.get(new Windowed<>("1", new SessionWindow(70L, 100L))));
    }

    @Test
    public void shouldCountSessionWindows() {
        final MockProcessorSupplier<Windowed<String>, Long> supplier = new MockProcessorSupplier<>();
        final KTable<Windowed<String>, Long> table = groupedStream
            .windowedBy(SessionWindows.with(ofMillis(30)))
            .count(Materialized.as("session-store"));
        table.toStream().process(supplier);
        doCountSessionWindows(supplier);
        assertEquals(table.queryableStoreName(), "session-store");
    }

    @Test
    public void shouldCountSessionWindowsWithInternalStoreName() {
        final MockProcessorSupplier<Windowed<String>, Long> supplier = new MockProcessorSupplier<>();
        final KTable<Windowed<String>, Long> table = groupedStream
            .windowedBy(SessionWindows.with(ofMillis(30)))
            .count();
        table.toStream().process(supplier);
        doCountSessionWindows(supplier);
        assertNull(table.queryableStoreName());
    }

    private void doReduceSessionWindows(final MockProcessorSupplier<Windowed<String>, String> supplier) {
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic(TOPIC, new StringSerializer(), new StringSerializer());
            inputTopic.pipeInput("1", "A", 10);
            inputTopic.pipeInput("2", "Z", 15);
            inputTopic.pipeInput("1", "B", 30);
            inputTopic.pipeInput("1", "A", 70);
            inputTopic.pipeInput("1", "B", 100);
            inputTopic.pipeInput("1", "C", 90);
        }
        final Map<Windowed<String>, ValueAndTimestamp<String>> result =
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey();
        assertEquals(
            ValueAndTimestamp.make("A:B", 30L),
            result.get(new Windowed<>("1", new SessionWindow(10L, 30L))));
        assertEquals(
            ValueAndTimestamp.make("Z", 15L),
            result.get(new Windowed<>("2", new SessionWindow(15L, 15L))));
        assertEquals(
            ValueAndTimestamp.make("A:B:C", 100L),
            result.get(new Windowed<>("1", new SessionWindow(70L, 100L))));
    }

    @Test
    public void shouldReduceSessionWindows() {
        final MockProcessorSupplier<Windowed<String>, String> supplier = new MockProcessorSupplier<>();
        final KTable<Windowed<String>, String> table = groupedStream
            .windowedBy(SessionWindows.with(ofMillis(30)))
            .reduce((value1, value2) -> value1 + ":" + value2, Materialized.as("session-store"));
        table.toStream().process(supplier);
        doReduceSessionWindows(supplier);
        assertEquals(table.queryableStoreName(), "session-store");
    }

    @Test
    public void shouldReduceSessionWindowsWithInternalStoreName() {
        final MockProcessorSupplier<Windowed<String>, String> supplier = new MockProcessorSupplier<>();
        final KTable<Windowed<String>, String> table = groupedStream
            .windowedBy(SessionWindows.with(ofMillis(30)))
            .reduce((value1, value2) -> value1 + ":" + value2);
        table.toStream().process(supplier);
        doReduceSessionWindows(supplier);
        assertNull(table.queryableStoreName());
    }

    @Test
    public void shouldNotAcceptNullReducerWhenReducingSessionWindows() {
        assertThrows(NullPointerException.class, () ->  groupedStream
                .windowedBy(SessionWindows.with(ofMillis(30)))
                .reduce(null, Materialized.as("store")));
    }

    @Test
    public void shouldNotAcceptNullSessionWindowsReducingSessionWindows() {
        assertThrows(NullPointerException.class, () ->  groupedStream.windowedBy((SessionWindows) null));
    }

    @Test
    public void shouldNotAcceptInvalidStoreNameWhenReducingSessionWindows() {
        assertThrows(TopologyException.class, () ->  groupedStream
                .windowedBy(SessionWindows.with(ofMillis(30)))
                .reduce(MockReducer.STRING_ADDER, Materialized.as(INVALID_STORE_NAME))
        );
    }

    @Test
    public void shouldNotAcceptNullStateStoreSupplierWhenReducingSessionWindows() {
        assertThrows(NullPointerException.class, () ->  groupedStream
                .windowedBy(SessionWindows.with(ofMillis(30)))
                .reduce(null, Materialized.<String, String, SessionStore<Bytes, byte[]>>as(null))
        );
    }

    @Test
    public void shouldNotAcceptNullInitializerWhenAggregatingSessionWindows() {
        assertThrows(NullPointerException.class, () ->  groupedStream
                .windowedBy(SessionWindows.with(ofMillis(30)))
                .aggregate(null, MockAggregator.TOSTRING_ADDER, (aggKey, aggOne, aggTwo) -> null, Materialized.as("storeName"))
        );
    }

    @Test
    public void shouldNotAcceptNullAggregatorWhenAggregatingSessionWindows() {
        assertThrows(NullPointerException.class, () -> groupedStream.
                windowedBy(SessionWindows.with(ofMillis(30)))
                .aggregate(MockInitializer.STRING_INIT, null, (aggKey, aggOne, aggTwo) -> null, Materialized.as("storeName"))
        );
    }

    @Test
    public void shouldNotAcceptNullSessionMergerWhenAggregatingSessionWindows() {
        assertThrows(NullPointerException.class, () ->  groupedStream
                .windowedBy(SessionWindows.with(ofMillis(30)))
                .aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, null, Materialized.as("storeName"))
        );
    }

    @Test
    public void shouldNotAcceptNullSessionWindowsWhenAggregatingSessionWindows() {
        assertThrows(NullPointerException.class, () ->  groupedStream.windowedBy((SessionWindows) null));
    }

    @Test
    public void shouldAcceptNullStoreNameWhenAggregatingSessionWindows() {
        groupedStream
            .windowedBy(SessionWindows.with(ofMillis(10)))
            .aggregate(
                    MockInitializer.STRING_INIT,
                    MockAggregator.TOSTRING_ADDER,
                    (aggKey, aggOne, aggTwo) -> null, Materialized.with(Serdes.String(), Serdes.String())
            );
    }

    @Test
    public void shouldNotAcceptInvalidStoreNameWhenAggregatingSessionWindows() {
        assertThrows(TopologyException.class, () ->  groupedStream
                .windowedBy(SessionWindows.with(ofMillis(10)))
                .aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, (aggKey, aggOne, aggTwo) -> null, Materialized.as(INVALID_STORE_NAME))
        );
    }

    @Test
    public void shouldThrowNullPointerOnReduceWhenMaterializedIsNull() {
        assertThrows(NullPointerException.class, () ->  groupedStream.reduce(MockReducer.STRING_ADDER, null));
    }

    @Test
    public void shouldThrowNullPointerOnAggregateWhenMaterializedIsNull() {
        assertThrows(NullPointerException.class, () ->  groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, null));
    }

    @Test
    public void shouldThrowNullPointerOnCountWhenMaterializedIsNull() {
        assertThrows(NullPointerException.class, () ->  groupedStream.count((Materialized<String, Long, KeyValueStore<Bytes, byte[]>>) null));
    }

    @Test
    public void shouldCountAndMaterializeResults() {
        groupedStream.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count").withKeySerde(Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);

            {
                final KeyValueStore<String, Long> count = driver.getKeyValueStore("count");

                assertThat(count.get("1"), equalTo(3L));
                assertThat(count.get("2"), equalTo(1L));
                assertThat(count.get("3"), equalTo(2L));
            }
            {
                final KeyValueStore<String, ValueAndTimestamp<Long>> count = driver.getTimestampedKeyValueStore("count");

                assertThat(count.get("1"), equalTo(ValueAndTimestamp.make(3L, 10L)));
                assertThat(count.get("2"), equalTo(ValueAndTimestamp.make(1L, 1L)));
                assertThat(count.get("3"), equalTo(ValueAndTimestamp.make(2L, 9L)));
            }
        }
    }

    @Test
    public void shouldLogAndMeasureSkipsInAggregateWithBuiltInMetricsVersion0100To24() {
        shouldLogAndMeasureSkipsInAggregate(StreamsConfig.METRICS_0100_TO_24);
    }

    @Test
    public void shouldLogAndMeasureSkipsInAggregateWithBuiltInMetricsVersionLatest() {
        shouldLogAndMeasureSkipsInAggregate(StreamsConfig.METRICS_LATEST);
    }

    private void shouldLogAndMeasureSkipsInAggregate(final String builtInMetricsVersion) {
        groupedStream.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count").withKeySerde(Serdes.String()));
        props.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, builtInMetricsVersion);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KStreamAggregate.class);
             final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {

            processData(driver);

            if (StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion)) {
                final Map<MetricName, ? extends Metric> metrics = driver.metrics();
                assertEquals(
                    1.0,
                    getMetricByName(metrics, "skipped-records-total", "stream-metrics").metricValue()
                );
                assertNotEquals(
                    0.0,
                    getMetricByName(metrics, "skipped-records-rate", "stream-metrics").metricValue()
                );
            }
            assertThat(
                appender.getMessages(),
                hasItem("Skipping record due to null key or value. key=[3] value=[null] topic=[topic] partition=[0] "
                    + "offset=[6]")
            );
        }
    }

    @Test
    public void shouldReduceAndMaterializeResults() {
        groupedStream.reduce(
            MockReducer.STRING_ADDER,
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("reduce")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);

            {
                final KeyValueStore<String, String> reduced = driver.getKeyValueStore("reduce");

                assertThat(reduced.get("1"), equalTo("A+C+D"));
                assertThat(reduced.get("2"), equalTo("B"));
                assertThat(reduced.get("3"), equalTo("E+F"));
            }
            {
                final KeyValueStore<String, ValueAndTimestamp<String>> reduced = driver.getTimestampedKeyValueStore("reduce");

                assertThat(reduced.get("1"), equalTo(ValueAndTimestamp.make("A+C+D", 10L)));
                assertThat(reduced.get("2"), equalTo(ValueAndTimestamp.make("B", 1L)));
                assertThat(reduced.get("3"), equalTo(ValueAndTimestamp.make("E+F", 9L)));
            }
        }
    }

    @Test
    public void shouldLogAndMeasureSkipsInReduceWithBuiltInMetricsVersion0100To24() {
        shouldLogAndMeasureSkipsInReduce(StreamsConfig.METRICS_0100_TO_24);
    }

    @Test
    public void shouldLogAndMeasureSkipsInReduceWithBuiltInMetricsVersionLatest() {
        shouldLogAndMeasureSkipsInReduce(StreamsConfig.METRICS_LATEST);
    }

    private void shouldLogAndMeasureSkipsInReduce(final String builtInMetricsVersion) {
        groupedStream.reduce(
            MockReducer.STRING_ADDER,
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("reduce")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
        );
        props.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, builtInMetricsVersion);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KStreamReduce.class);
             final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {

            processData(driver);

            if (StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion)) {
                final Map<MetricName, ? extends Metric> metrics = driver.metrics();
                assertEquals(
                    1.0,
                    getMetricByName(metrics, "skipped-records-total", "stream-metrics").metricValue()
                );
                assertNotEquals(
                    0.0,
                    getMetricByName(metrics, "skipped-records-rate", "stream-metrics").metricValue()
                );
            }
            assertThat(
                appender.getMessages(),
                hasItem("Skipping record due to null key or value. key=[3] value=[null] topic=[topic] partition=[0] "
                    + "offset=[6]")
            );
        }
    }

    @Test
    public void shouldAggregateAndMaterializeResults() {
        groupedStream.aggregate(
            MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("aggregate")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);

            {
                final KeyValueStore<String, String> aggregate = driver.getKeyValueStore("aggregate");

                assertThat(aggregate.get("1"), equalTo("0+A+C+D"));
                assertThat(aggregate.get("2"), equalTo("0+B"));
                assertThat(aggregate.get("3"), equalTo("0+E+F"));
            }
            {
                final KeyValueStore<String, ValueAndTimestamp<String>> aggregate = driver.getTimestampedKeyValueStore("aggregate");

                assertThat(aggregate.get("1"), equalTo(ValueAndTimestamp.make("0+A+C+D", 10L)));
                assertThat(aggregate.get("2"), equalTo(ValueAndTimestamp.make("0+B", 1L)));
                assertThat(aggregate.get("3"), equalTo(ValueAndTimestamp.make("0+E+F", 9L)));
            }
        }
    }

    @Test
    public void shouldAggregateWithDefaultSerdes() {
        final MockProcessorSupplier<String, String> supplier = new MockProcessorSupplier<>();
        groupedStream
            .aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER)
            .toStream()
            .process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);

            assertThat(
                supplier.theCapturedProcessor().lastValueAndTimestampPerKey().get("1"),
                equalTo(ValueAndTimestamp.make("0+A+C+D", 10L)));
            assertThat(
                supplier.theCapturedProcessor().lastValueAndTimestampPerKey().get("2"),
                equalTo(ValueAndTimestamp.make("0+B", 1L)));
            assertThat(
                supplier.theCapturedProcessor().lastValueAndTimestampPerKey().get("3"),
                equalTo(ValueAndTimestamp.make("0+E+F", 9L)));
        }
    }

    private void processData(final TopologyTestDriver driver) {
        final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(TOPIC, new StringSerializer(), new StringSerializer());
        inputTopic.pipeInput("1", "A", 5L);
        inputTopic.pipeInput("2", "B", 1L);
        inputTopic.pipeInput("1", "C", 3L);
        inputTopic.pipeInput("1", "D", 10L);
        inputTopic.pipeInput("3", "E", 8L);
        inputTopic.pipeInput("3", "F", 9L);
        inputTopic.pipeInput("3", (String) null);
    }

    private void doCountWindowed(final  MockProcessorSupplier<Windowed<String>, Long> supplier) {
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic(TOPIC, new StringSerializer(), new StringSerializer());
            inputTopic.pipeInput("1", "A", 0L);
            inputTopic.pipeInput("1", "A", 499L);
            inputTopic.pipeInput("1", "A", 100L);
            inputTopic.pipeInput("2", "B", 0L);
            inputTopic.pipeInput("2", "B", 100L);
            inputTopic.pipeInput("2", "B", 200L);
            inputTopic.pipeInput("3", "C", 1L);
            inputTopic.pipeInput("1", "A", 500L);
            inputTopic.pipeInput("1", "A", 500L);
            inputTopic.pipeInput("2", "B", 500L);
            inputTopic.pipeInput("2", "B", 500L);
            inputTopic.pipeInput("3", "B", 100L);
        }
        assertThat(supplier.theCapturedProcessor().processed(), equalTo(Arrays.asList(
            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(0L, 500L)), 1L, 0L),
            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(0L, 500L)), 2L, 499L),
            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(0L, 500L)), 3L, 499L),
            new KeyValueTimestamp<>(new Windowed<>("2", new TimeWindow(0L, 500L)), 1L, 0L),
            new KeyValueTimestamp<>(new Windowed<>("2", new TimeWindow(0L, 500L)), 2L, 100L),
            new KeyValueTimestamp<>(new Windowed<>("2", new TimeWindow(0L, 500L)), 3L, 200L),
            new KeyValueTimestamp<>(new Windowed<>("3", new TimeWindow(0L, 500L)), 1L, 1L),
            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(500L, 1000L)), 1L, 500L),
            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(500L, 1000L)), 2L, 500L),
            new KeyValueTimestamp<>(new Windowed<>("2", new TimeWindow(500L, 1000L)), 1L, 500L),
            new KeyValueTimestamp<>(new Windowed<>("2", new TimeWindow(500L, 1000L)), 2L, 500L),
            new KeyValueTimestamp<>(new Windowed<>("3", new TimeWindow(0L, 500L)), 2L, 100L)
        )));
    }

    @Test
    public void shouldCountWindowed() {
        final MockProcessorSupplier<Windowed<String>, Long> supplier = new MockProcessorSupplier<>();
        groupedStream
            .windowedBy(TimeWindows.of(ofMillis(500L)))
            .count(Materialized.as("aggregate-by-key-windowed"))
            .toStream()
            .process(supplier);

        doCountWindowed(supplier);
    }

    @Test
    public void shouldCountWindowedWithInternalStoreName() {
        final MockProcessorSupplier<Windowed<String>, Long> supplier = new MockProcessorSupplier<>();
        groupedStream
            .windowedBy(TimeWindows.of(ofMillis(500L)))
            .count()
            .toStream()
            .process(supplier);

        doCountWindowed(supplier);
    }
}
