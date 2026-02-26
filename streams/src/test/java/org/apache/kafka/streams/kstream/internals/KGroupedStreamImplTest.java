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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.KeyValueTimestampHeaders;
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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.StreamsTestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KGroupedStreamImplTest {

    private static final String TOPIC = "topic";
    private static final String INVALID_STORE_NAME = "~foo bar~";
    private final StreamsBuilder builder = new StreamsBuilder();
    private KGroupedStream<String, String> groupedStream;

    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    /**
     * Provides test parameters for different store formats.
     */
    private static Stream<Arguments> storeFormats() {
        return Stream.of(
            Arguments.of("default"),
            Arguments.of("headers")
        );
    }

    private Properties getProps(final String storeFormat) {
        final Properties properties = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
        properties.put(StreamsConfig.DSL_STORE_FORMAT_CONFIG, storeFormat);
        // Disable caching to avoid ValueTimestampHeaders casting issues
        properties.setProperty(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0");
        return properties;
    }

    private static Headers makeHeaders(final String key, final String value) {
        final RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(key, value.getBytes()));
        return headers;
    }

    @BeforeEach
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
                .windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(10)))
                .reduce(null, Materialized.as("store")));
    }

    @Test
    public void shouldNotHaveNullWindowsWithWindowedReduce() {
        assertThrows(NullPointerException.class, () ->  groupedStream.windowedBy((Windows<?>) null));
    }

    @Test
    public void shouldNotHaveInvalidStoreNameWithWindowedReduce() {
        assertThrows(TopologyException.class, () ->  groupedStream
                .windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(10)))
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
                .windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(10)))
                .aggregate(null, MockAggregator.TOSTRING_ADDER, Materialized.as("store")));
    }

    @Test
    public void shouldNotHaveNullAdderOnWindowedAggregate() {
        assertThrows(NullPointerException.class, () ->  groupedStream
                .windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(10)))
                .aggregate(MockInitializer.STRING_INIT, null, Materialized.as("store")));
    }

    @Test
    public void shouldNotHaveNullWindowsOnWindowedAggregate() {
        assertThrows(NullPointerException.class, () ->  groupedStream.windowedBy((Windows<?>) null));
    }

    @Test
    public void shouldNotHaveInvalidStoreNameOnWindowedAggregate() {
        assertThrows(TopologyException.class, () ->  groupedStream
                .windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(10)))
                .aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, Materialized.as(INVALID_STORE_NAME)));
    }

    @Test
    public void shouldNotHaveNullReducerWithSlidingWindowedReduce() {
        assertThrows(NullPointerException.class, () ->  groupedStream
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(10), ofMillis(100)))
                .reduce(null, Materialized.as("store")));
    }

    @Test
    public void shouldNotHaveNullWindowsWithSlidingWindowedReduce() {
        assertThrows(NullPointerException.class, () ->  groupedStream.windowedBy((SlidingWindows) null));
    }

    @Test
    public void shouldNotHaveInvalidStoreNameWithSlidingWindowedReduce() {
        assertThrows(TopologyException.class, () ->  groupedStream
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(10), ofMillis(100)))
                .reduce(MockReducer.STRING_ADDER, Materialized.as(INVALID_STORE_NAME)));
    }

    @Test
    public void shouldNotHaveNullInitializerOnSlidingWindowedAggregate() {
        assertThrows(NullPointerException.class, () ->  groupedStream
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(10), ofMillis(100)))
                .aggregate(null, MockAggregator.TOSTRING_ADDER, Materialized.as("store")));
    }

    @Test
    public void shouldNotHaveNullAdderOnSlidingWindowedAggregate() {
        assertThrows(NullPointerException.class, () ->  groupedStream
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(10), ofMillis(100)))
                .aggregate(MockInitializer.STRING_INIT, null, Materialized.as("store")));
    }

    @Test
    public void shouldNotHaveInvalidStoreNameOnSlidingWindowedAggregate() {
        assertThrows(TopologyException.class, () ->  groupedStream
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(10), ofMillis(100)))
                .aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, Materialized.as(INVALID_STORE_NAME)));
    }

    @Test
    public void shouldCountSlidingWindows() {
        final MockApiProcessorSupplier<Windowed<String>, Long, Void, Void> supplier = new MockApiProcessorSupplier<>();
        groupedStream
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(500L), ofMillis(2000L)))
                .count(Materialized.as("aggregate-by-key-windowed"))
                .toStream()
                .process(supplier);

        doCountSlidingWindows(supplier);
    }

    @Test
    public void shouldCountSlidingWindowsWithInternalStoreName() {
        final MockApiProcessorSupplier<Windowed<String>, Long, Void, Void> supplier = new MockApiProcessorSupplier<>();
        groupedStream
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(500L), ofMillis(2000L)))
                .count()
                .toStream()
                .process(supplier);

        doCountSlidingWindows(supplier);
    }

    private void doCountSlidingWindows(final MockApiProcessorSupplier<Windowed<String>, Long, Void, Void> supplier) {
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

    private void doAggregateSessionWindows(final MockApiProcessorSupplier<Windowed<String>, Integer, Void, Void> supplier) {
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
        final MockApiProcessorSupplier<Windowed<String>, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final KTable<Windowed<String>, Integer> table = groupedStream
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(ofMillis(30)))
            .aggregate(
                () -> 0,
                (aggKey, value, aggregate) -> aggregate + 1,
                (aggKey, aggOne, aggTwo) -> aggOne + aggTwo,
                Materialized
                    .<String, Integer, SessionStore<Bytes, byte[]>>as("session-store").
                    withValueSerde(Serdes.Integer()));
        table.toStream().process(supplier);

        doAggregateSessionWindows(supplier);
        assertEquals("session-store", table.queryableStoreName());
    }

    @Test
    public void shouldAggregateSessionWindowsWithInternalStoreName() {
        final MockApiProcessorSupplier<Windowed<String>, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final KTable<Windowed<String>, Integer> table = groupedStream
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(ofMillis(30)))
            .aggregate(
                () -> 0,
                (aggKey, value, aggregate) -> aggregate + 1,
                (aggKey, aggOne, aggTwo) -> aggOne + aggTwo,
                Materialized.with(null, Serdes.Integer()));
        table.toStream().process(supplier);

        doAggregateSessionWindows(supplier);
    }

    @Test
    public void sessionGapOfZeroShouldOnlyPutRecordsWithSameTsIntoSameSession() {
        final MockApiProcessorSupplier<Windowed<String>, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final KTable<Windowed<String>, Integer> table = groupedStream
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ZERO))
            .aggregate(
                () -> 0,
                (aggKey, value, aggregate) -> aggregate + 1,
                (aggKey, aggOne, aggTwo) -> aggOne + aggTwo,
                Materialized.with(null, Serdes.Integer()));
        table.toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(TOPIC, new StringSerializer(), new StringSerializer());
            inputTopic.pipeInput("1", "1", 10);
            inputTopic.pipeInput("1", "1", 11);
            inputTopic.pipeInput("1", "1", 11);
            inputTopic.pipeInput("1", "1", 12);
        }

        final Map<Windowed<String>, ValueAndTimestamp<Integer>> result
            = supplier.theCapturedProcessor().lastValueAndTimestampPerKey();
        assertEquals(
            ValueAndTimestamp.make(1, 10),
            result.get(new Windowed<>("1", new SessionWindow(10L, 10L))));
        assertEquals(
            ValueAndTimestamp.make(2, 11L),
            result.get(new Windowed<>("1", new SessionWindow(11L, 11L))));
        assertEquals(
            ValueAndTimestamp.make(1, 12L),
            result.get(new Windowed<>("1", new SessionWindow(12L, 12L))));
    }

    private void doCountSessionWindows(final MockApiProcessorSupplier<Windowed<String>, Long, Void, Void> supplier) {
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
        final MockApiProcessorSupplier<Windowed<String>, Long, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final KTable<Windowed<String>, Long> table = groupedStream
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(ofMillis(30)))
            .count(Materialized.as("session-store"));
        table.toStream().process(supplier);
        doCountSessionWindows(supplier);
        assertEquals("session-store", table.queryableStoreName());
    }

    @Test
    public void shouldCountSessionWindowsWithInternalStoreName() {
        final MockApiProcessorSupplier<Windowed<String>, Long, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final KTable<Windowed<String>, Long> table = groupedStream
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(ofMillis(30)))
            .count();
        table.toStream().process(supplier);
        doCountSessionWindows(supplier);
        assertNull(table.queryableStoreName());
    }

    private void doReduceSessionWindows(final MockApiProcessorSupplier<Windowed<String>, String, Void, Void> supplier) {
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
        final MockApiProcessorSupplier<Windowed<String>, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final KTable<Windowed<String>, String> table = groupedStream
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(ofMillis(30)))
            .reduce((value1, value2) -> value1 + ":" + value2, Materialized.as("session-store"));
        table.toStream().process(supplier);
        doReduceSessionWindows(supplier);
        assertEquals("session-store", table.queryableStoreName());
    }

    @Test
    public void shouldReduceSessionWindowsWithInternalStoreName() {
        final MockApiProcessorSupplier<Windowed<String>, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final KTable<Windowed<String>, String> table = groupedStream
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(ofMillis(30)))
            .reduce((value1, value2) -> value1 + ":" + value2);
        table.toStream().process(supplier);
        doReduceSessionWindows(supplier);
        assertNull(table.queryableStoreName());
    }

    @Test
    public void shouldNotAcceptNullReducerWhenReducingSessionWindows() {
        assertThrows(NullPointerException.class, () ->  groupedStream
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(ofMillis(30)))
                .reduce(null, Materialized.as("store")));
    }

    @Test
    public void shouldNotAcceptNullSessionWindowsReducingSessionWindows() {
        assertThrows(NullPointerException.class, () ->  groupedStream.windowedBy((SessionWindows) null));
    }

    @Test
    public void shouldNotAcceptInvalidStoreNameWhenReducingSessionWindows() {
        assertThrows(TopologyException.class, () ->  groupedStream
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(ofMillis(30)))
                .reduce(MockReducer.STRING_ADDER, Materialized.as(INVALID_STORE_NAME))
        );
    }

    @Test
    public void shouldNotAcceptNullStateStoreSupplierWhenReducingSessionWindows() {
        assertThrows(NullPointerException.class, () ->  groupedStream
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(ofMillis(30)))
                .reduce(null, Materialized.<String, String, SessionStore<Bytes, byte[]>>as((String) null))
        );
    }

    @Test
    public void shouldNotAcceptNullInitializerWhenAggregatingSessionWindows() {
        assertThrows(NullPointerException.class, () ->  groupedStream
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(ofMillis(30)))
                .aggregate(null, MockAggregator.TOSTRING_ADDER, (aggKey, aggOne, aggTwo) -> null, Materialized.as("storeName"))
        );
    }

    @Test
    public void shouldNotAcceptNullAggregatorWhenAggregatingSessionWindows() {
        assertThrows(NullPointerException.class, () -> groupedStream.
                windowedBy(SessionWindows.ofInactivityGapWithNoGrace(ofMillis(30)))
                .aggregate(MockInitializer.STRING_INIT, null, (aggKey, aggOne, aggTwo) -> null, Materialized.as("storeName"))
        );
    }

    @Test
    public void shouldNotAcceptNullSessionMergerWhenAggregatingSessionWindows() {
        assertThrows(NullPointerException.class, () ->  groupedStream
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(ofMillis(30)))
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
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(ofMillis(10)))
            .aggregate(
                    MockInitializer.STRING_INIT,
                    MockAggregator.TOSTRING_ADDER,
                    (aggKey, aggOne, aggTwo) -> null, Materialized.with(Serdes.String(), Serdes.String())
            );
    }

    @Test
    public void shouldNotAcceptInvalidStoreNameWhenAggregatingSessionWindows() {
        assertThrows(TopologyException.class, () ->  groupedStream
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(ofMillis(10)))
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

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldCountAndMaterializeResults(final String storeFormat) {
        groupedStream.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count").withKeySerde(Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), getProps(storeFormat))) {
            processData(driver, storeFormat);

            if (storeFormat.equals("default")) {
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
            } else if (storeFormat.equals("headers")) {
                {
                    final KeyValueStore<String, ValueTimestampHeaders<Long>> count = driver.getTimestampedKeyValueStoreWithHeaders("count");
                    final Headers headers1 = makeHeaders("key", "1");
                    final Headers headers2 = makeHeaders("key", "2");
                    final Headers headers3 = makeHeaders("key", "3");

                    assertThat(count.get("1"), equalTo(ValueTimestampHeaders.make(3L, 10L, headers1)));
                    assertThat(count.get("2"), equalTo(ValueTimestampHeaders.make(1L, 1L, headers2)));
                    assertThat(count.get("3"), equalTo(ValueTimestampHeaders.make(2L, 9L, headers3)));
                }

            }
        }
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldLogAndMeasureSkipsInAggregate(final String storeFormat) {
        groupedStream.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count").withKeySerde(Serdes.String()));

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KStreamAggregate.class);
             final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), getProps(storeFormat))) {

            processData(driver, storeFormat);

            assertThat(
                appender.getMessages(),
                hasItem("Skipping record due to null key or value. topic=[topic] partition=[0] "
                    + "offset=[6]")
            );
        }
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldReduceAndMaterializeResults(final String storeFormat) {
        groupedStream.reduce(
            MockReducer.STRING_ADDER,
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("reduce")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), getProps(storeFormat))) {
            processData(driver, storeFormat);

            if (storeFormat.equals("default")) {
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
            } else if (storeFormat.equals("headers")) {
                {
                    final KeyValueStore<String, ValueTimestampHeaders<Long>> count = driver.getTimestampedKeyValueStoreWithHeaders("reduce");
                    final Headers headers1 = makeHeaders("key", "1");
                    final Headers headers2 = makeHeaders("key", "2");
                    final Headers headers3 = makeHeaders("key", "3");

                    assertThat(count.get("1"), equalTo(ValueTimestampHeaders.make("A+C+D", 10L, headers1)));
                    assertThat(count.get("2"), equalTo(ValueTimestampHeaders.make("B", 1L, headers2)));
                    assertThat(count.get("3"), equalTo(ValueTimestampHeaders.make("E+F", 9L, headers3)));
                }

            }
        }
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldLogAndMeasureSkipsInReduce(final String storeFormat) {
        groupedStream.reduce(
            MockReducer.STRING_ADDER,
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("reduce")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
        );

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KStreamReduce.class);
             final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), getProps(storeFormat))) {

            processData(driver, storeFormat);

            assertThat(
                appender.getMessages(),
                hasItem("Skipping record due to null key or value. topic=[topic] partition=[0] "
                    + "offset=[6]")
            );
        }
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldAggregateAndMaterializeResults(final String storeFormat) {
        groupedStream.aggregate(
            MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("aggregate")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), getProps(storeFormat))) {
            processData(driver, storeFormat);

            if (storeFormat.equals("default")) {
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
            } else if (storeFormat.equals("headers")) {
                {
                    final KeyValueStore<String, ValueTimestampHeaders<Long>> count = driver.getTimestampedKeyValueStoreWithHeaders("aggregate");
                    final Headers headers1 = makeHeaders("key", "1");
                    final Headers headers2 = makeHeaders("key", "2");
                    final Headers headers3 = makeHeaders("key", "3");

                    assertThat(count.get("1"), equalTo(ValueTimestampHeaders.make("0+A+C+D", 10L, headers1)));
                    assertThat(count.get("2"), equalTo(ValueTimestampHeaders.make("0+B", 1L, headers2)));
                    assertThat(count.get("3"), equalTo(ValueTimestampHeaders.make("0+E+F", 9L, headers3)));
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldAggregateWithDefaultSerdes(final String storeFormat) {
        final MockApiProcessorSupplier<String, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        groupedStream
            .aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER)
            .toStream()
            .process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), getProps(storeFormat))) {
            processData(driver, storeFormat);

            assertThat(
                supplier.theCapturedProcessor().lastValueAndTimestampPerKey().get("1"),
                equalTo(ValueAndTimestamp.make("0+A+C+D", 10L)));
            assertThat(
                supplier.theCapturedProcessor().lastValueAndTimestampPerKey().get("2"),
                equalTo(ValueAndTimestamp.make("0+B", 1L)));
            assertThat(
                supplier.theCapturedProcessor().lastValueAndTimestampPerKey().get("3"),
                equalTo(ValueAndTimestamp.make("0+E+F", 9L)));

            if (storeFormat.equals("headers")) {
                final Headers headers1 = makeHeaders("key", "1");
                final Headers headers2 = makeHeaders("key", "2");
                final Headers headers3 = makeHeaders("key", "3");

                // Find the last record for each key and verify headers
                final ArrayList<KeyValueTimestampHeaders<String, String>> processedRecords = supplier.theCapturedProcessor().processedWithHeaders();

                KeyValueTimestampHeaders<String, String> lastKey1 = null;
                KeyValueTimestampHeaders<String, String> lastKey2 = null;
                KeyValueTimestampHeaders<String, String> lastKey3 = null;

                for (final KeyValueTimestampHeaders<String, String> record : processedRecords) {
                    if (record.key().equals("1")) {
                        lastKey1 = record;
                    } else if (record.key().equals("2")) {
                        lastKey2 = record;
                    } else if (record.key().equals("3")) {
                        lastKey3 = record;
                    }
                }

                assertThat("Expected record for key 1", lastKey1, org.hamcrest.Matchers.notNullValue());
                assertThat("Expected record for key 2", lastKey2, org.hamcrest.Matchers.notNullValue());
                assertThat("Expected record for key 3", lastKey3, org.hamcrest.Matchers.notNullValue());

                assertThat(lastKey1.headers(), equalTo(headers1));
                assertThat(lastKey2.headers(), equalTo(headers2));
                assertThat(lastKey3.headers(), equalTo(headers3));
            }
        }
    }

    private void processData(final TopologyTestDriver driver, final String storeFormat) {
        final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(TOPIC, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

        if (storeFormat.equals("headers")) {
            final Headers headers1 = makeHeaders("key", "1");
            final Headers headers2 = makeHeaders("key", "2");
            final Headers headers3 = makeHeaders("key", "3");

            inputTopic.pipeInput(new TestRecord<>("1", "A", headers1, 5L));
            inputTopic.pipeInput(new TestRecord<>("2", "B", headers2, 1L));
            inputTopic.pipeInput(new TestRecord<>("1", "C", headers1, 3L));
            inputTopic.pipeInput(new TestRecord<>("1", "D", headers1, 10L));
            inputTopic.pipeInput(new TestRecord<>("3", "E", headers3, 8L));
            inputTopic.pipeInput(new TestRecord<>("3", "F", headers3, 9L));
            inputTopic.pipeInput(new TestRecord<>("3", null, headers3));
        } else {
            inputTopic.pipeInput("1", "A", 5L);
            inputTopic.pipeInput("2", "B", 1L);
            inputTopic.pipeInput("1", "C", 3L);
            inputTopic.pipeInput("1", "D", 10L);
            inputTopic.pipeInput("3", "E", 8L);
            inputTopic.pipeInput("3", "F", 9L);
            inputTopic.pipeInput("3", (String) null);
        }
    }

    private void doCountWindowed(final MockApiProcessorSupplier<Windowed<String>, Long, Void, Void> supplier) {
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
        final MockApiProcessorSupplier<Windowed<String>, Long, Void, Void> supplier = new MockApiProcessorSupplier<>();
        groupedStream
            .windowedBy(TimeWindows.ofSizeAndGrace(ofMillis(500L), ofMillis(100L)))
            .count(Materialized.as("aggregate-by-key-windowed"))
            .toStream()
            .process(supplier);

        doCountWindowed(supplier);
    }

    @Test
    public void shouldCountWindowedWithInternalStoreName() {
        final MockApiProcessorSupplier<Windowed<String>, Long, Void, Void> supplier = new MockApiProcessorSupplier<>();
        groupedStream
            .windowedBy(TimeWindows.ofSizeAndGrace(ofMillis(500L), ofMillis(100L)))
            .count()
            .toStream()
            .process(supplier);

        doCountWindowed(supplier);
    }
}
