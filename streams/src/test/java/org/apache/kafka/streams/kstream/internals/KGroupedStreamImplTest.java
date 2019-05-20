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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

@SuppressWarnings("unchecked")
public class KGroupedStreamImplTest {

    private static final String TOPIC = "topic";
    private static final String INVALID_STORE_NAME = "~foo bar~";
    private final StreamsBuilder builder = new StreamsBuilder();
    private KGroupedStream<String, String> groupedStream;

    private final ConsumerRecordFactory<String, String> recordFactory =
        new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    @Before
    public void before() {
        final KStream<String, String> stream = builder.stream(TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        groupedStream = stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullReducerOnReduce() {
        groupedStream.reduce(null);
    }

    @Test(expected = TopologyException.class)
    public void shouldNotHaveInvalidStoreNameOnReduce() {
        groupedStream.reduce(MockReducer.STRING_ADDER, Materialized.as(INVALID_STORE_NAME));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullReducerWithWindowedReduce() {
        groupedStream
            .windowedBy(TimeWindows.of(ofMillis(10)))
            .reduce(null, Materialized.as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullWindowsWithWindowedReduce() {
        groupedStream.windowedBy((Windows) null);
    }

    @Test(expected = TopologyException.class)
    public void shouldNotHaveInvalidStoreNameWithWindowedReduce() {
        groupedStream
            .windowedBy(TimeWindows.of(ofMillis(10)))
            .reduce(MockReducer.STRING_ADDER, Materialized.as(INVALID_STORE_NAME));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullInitializerOnAggregate() {
        groupedStream.aggregate(null, MockAggregator.TOSTRING_ADDER, Materialized.as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullAdderOnAggregate() {
        groupedStream.aggregate(MockInitializer.STRING_INIT, null, Materialized.as("store"));
    }

    @Test(expected = TopologyException.class)
    public void shouldNotHaveInvalidStoreNameOnAggregate() {
        groupedStream.aggregate(
            MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            Materialized.as(INVALID_STORE_NAME));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullInitializerOnWindowedAggregate() {
        groupedStream
            .windowedBy(TimeWindows.of(ofMillis(10)))
            .aggregate(null, MockAggregator.TOSTRING_ADDER, Materialized.as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullAdderOnWindowedAggregate() {
        groupedStream
            .windowedBy(TimeWindows.of(ofMillis(10)))
            .aggregate(MockInitializer.STRING_INIT, null, Materialized.as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullWindowsOnWindowedAggregate() {
        groupedStream.windowedBy((Windows) null);
    }

    @Test(expected = TopologyException.class)
    public void shouldNotHaveInvalidStoreNameOnWindowedAggregate() {
        groupedStream
            .windowedBy(TimeWindows.of(ofMillis(10)))
            .aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, Materialized.as(INVALID_STORE_NAME));
    }

    private void doAggregateSessionWindows(final MockProcessorSupplier<Windowed<String>, Integer> supplier) {
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 10));
            driver.pipeInput(recordFactory.create(TOPIC, "2", "2", 15));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 30));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 70));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 100));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 90));
        }
        final Map<Windowed<String>, ValueAndTimestamp<Integer>> result
            = supplier.theCapturedProcessor().lastValueAndTimestampPerKey;
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
            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 10));
            driver.pipeInput(recordFactory.create(TOPIC, "2", "2", 15));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 30));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 70));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 100));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 90));
        }
        final Map<Windowed<String>, ValueAndTimestamp<Long>> result =
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey;
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
            driver.pipeInput(recordFactory.create(TOPIC, "1", "A", 10));
            driver.pipeInput(recordFactory.create(TOPIC, "2", "Z", 15));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "B", 30));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "A", 70));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "B", 100));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "C", 90));
        }
        final Map<Windowed<String>, ValueAndTimestamp<String>> result =
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey;
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

    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullReducerWhenReducingSessionWindows() {
        groupedStream
            .windowedBy(SessionWindows.with(ofMillis(30)))
            .reduce(null, Materialized.as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullSessionWindowsReducingSessionWindows() {
        groupedStream.windowedBy((SessionWindows) null);
    }

    @Test(expected = TopologyException.class)
    public void shouldNotAcceptInvalidStoreNameWhenReducingSessionWindows() {
        groupedStream
            .windowedBy(SessionWindows.with(ofMillis(30)))
            .reduce(MockReducer.STRING_ADDER, Materialized.as(INVALID_STORE_NAME));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullStateStoreSupplierWhenReducingSessionWindows() {
        groupedStream
            .windowedBy(SessionWindows.with(ofMillis(30)))
            .reduce(
                null,
                Materialized.<String, String, SessionStore<Bytes, byte[]>>as(null));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullInitializerWhenAggregatingSessionWindows() {
        groupedStream
            .windowedBy(SessionWindows.with(ofMillis(30)))
            .aggregate(
                null,
                MockAggregator.TOSTRING_ADDER,
                (aggKey, aggOne, aggTwo) -> null,
                Materialized.as("storeName"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullAggregatorWhenAggregatingSessionWindows() {
        groupedStream.
            windowedBy(SessionWindows.with(ofMillis(30)))
            .aggregate(
                MockInitializer.STRING_INIT,
                null,
                (aggKey, aggOne, aggTwo) -> null,
                Materialized.as("storeName"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullSessionMergerWhenAggregatingSessionWindows() {
        groupedStream
            .windowedBy(SessionWindows.with(ofMillis(30)))
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                null,
                Materialized.as("storeName"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAcceptNullSessionWindowsWhenAggregatingSessionWindows() {
        groupedStream.windowedBy((SessionWindows) null);
    }

    @Test
    public void shouldAcceptNullStoreNameWhenAggregatingSessionWindows() {
        groupedStream
            .windowedBy(SessionWindows.with(ofMillis(10)))
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                (aggKey, aggOne, aggTwo) -> null,
                Materialized.with(Serdes.String(), Serdes.String()));
    }

    @Test(expected = TopologyException.class)
    public void shouldNotAcceptInvalidStoreNameWhenAggregatingSessionWindows() {
        groupedStream
            .windowedBy(SessionWindows.with(ofMillis(10)))
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                (aggKey, aggOne, aggTwo) -> null,
                Materialized.as(INVALID_STORE_NAME));
    }

    @SuppressWarnings("unchecked")
    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnReduceWhenMaterializedIsNull() {
        groupedStream.reduce(MockReducer.STRING_ADDER, (Materialized) null);
    }

    @SuppressWarnings("unchecked")
    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnAggregateWhenMaterializedIsNull() {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, (Materialized) null);
    }

    @SuppressWarnings("unchecked")
    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnCountWhenMaterializedIsNull() {
        groupedStream.count((Materialized) null);
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
    public void shouldLogAndMeasureSkipsInAggregate() {
        groupedStream.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count").withKeySerde(Serdes.String()));
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
            LogCaptureAppender.unregister(appender);

            final Map<MetricName, ? extends Metric> metrics = driver.metrics();
            assertEquals(1.0, getMetricByName(metrics, "skipped-records-total", "stream-metrics").metricValue());
            assertNotEquals(0.0, getMetricByName(metrics, "skipped-records-rate", "stream-metrics").metricValue());
            assertThat(appender.getMessages(), hasItem("Skipping record due to null key or value. key=[3] value=[null] topic=[topic] partition=[0] offset=[6]"));
        }
    }


    @SuppressWarnings("unchecked")
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
    public void shouldLogAndMeasureSkipsInReduce() {
        groupedStream.reduce(
            MockReducer.STRING_ADDER,
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("reduce")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
        );

        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
            LogCaptureAppender.unregister(appender);

            final Map<MetricName, ? extends Metric> metrics = driver.metrics();
            assertEquals(1.0, getMetricByName(metrics, "skipped-records-total", "stream-metrics").metricValue());
            assertNotEquals(0.0, getMetricByName(metrics, "skipped-records-rate", "stream-metrics").metricValue());
            assertThat(appender.getMessages(), hasItem("Skipping record due to null key or value. key=[3] value=[null] topic=[topic] partition=[0] offset=[6]"));
        }
    }


    @SuppressWarnings("unchecked")
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

    @SuppressWarnings("unchecked")
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
                supplier.theCapturedProcessor().lastValueAndTimestampPerKey.get("1"),
                equalTo(ValueAndTimestamp.make("0+A+C+D", 10L)));
            assertThat(
                supplier.theCapturedProcessor().lastValueAndTimestampPerKey.get("2"),
                equalTo(ValueAndTimestamp.make("0+B", 1L)));
            assertThat(
                supplier.theCapturedProcessor().lastValueAndTimestampPerKey.get("3"),
                equalTo(ValueAndTimestamp.make("0+E+F", 9L)));
        }
    }

    private void processData(final TopologyTestDriver driver) {
        driver.pipeInput(recordFactory.create(TOPIC, "1", "A", 5L));
        driver.pipeInput(recordFactory.create(TOPIC, "2", "B", 1L));
        driver.pipeInput(recordFactory.create(TOPIC, "1", "C", 3L));
        driver.pipeInput(recordFactory.create(TOPIC, "1", "D", 10L));
        driver.pipeInput(recordFactory.create(TOPIC, "3", "E", 8L));
        driver.pipeInput(recordFactory.create(TOPIC, "3", "F", 9L));
        driver.pipeInput(recordFactory.create(TOPIC, "3", (String) null));
    }

    private void doCountWindowed(final  MockProcessorSupplier<Windowed<String>, Long> supplier) {
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(TOPIC, "1", "A", 0L));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "A", 499L));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "A", 100L));
            driver.pipeInput(recordFactory.create(TOPIC, "2", "B", 0L));
            driver.pipeInput(recordFactory.create(TOPIC, "2", "B", 100L));
            driver.pipeInput(recordFactory.create(TOPIC, "2", "B", 200L));
            driver.pipeInput(recordFactory.create(TOPIC, "3", "C", 1L));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "A", 500L));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "A", 500L));
            driver.pipeInput(recordFactory.create(TOPIC, "2", "B", 500L));
            driver.pipeInput(recordFactory.create(TOPIC, "2", "B", 500L));
            driver.pipeInput(recordFactory.create(TOPIC, "3", "B", 100L));
        }
        assertThat(supplier.theCapturedProcessor().processedWithTimestamps, equalTo(Arrays.asList(
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
        final List<KeyValue<Windowed<String>, KeyValue<Long, Long>>> results = new ArrayList<>();
        groupedStream
            .windowedBy(TimeWindows.of(ofMillis(500L)))
            .count()
            .toStream()
            .process(supplier);

        doCountWindowed(supplier);
    }
}
