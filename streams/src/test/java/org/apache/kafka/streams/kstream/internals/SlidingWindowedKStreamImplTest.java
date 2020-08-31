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
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static java.time.Duration.ofMillis;
import static java.time.Instant.ofEpochMilli;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

public class SlidingWindowedKStreamImplTest {

    private static final String TOPIC = "input";
    private final StreamsBuilder builder = new StreamsBuilder();
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
    private TimeWindowedKStream<String, String> windowedStream;

    @Before
    public void before() {
        final KStream<String, String> stream = builder.stream(TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        windowedStream = stream.
            groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(SlidingWindows.withTimeDifferenceAndGrace(ofMillis(100L), ofMillis(1000L)));
    }

    @Test
    public void shouldCountSlidingWindows() {
        final MockProcessorSupplier<Windowed<String>, Long> supplier = new MockProcessorSupplier<>();
        windowedStream
            .count()
            .toStream()
            .process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
        }
        assertThat(
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey()
                .get(new Windowed<>("1", new TimeWindow(0L, 100L))),
            equalTo(ValueAndTimestamp.make(1L, 100L)));
        assertThat(
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey()
                .get(new Windowed<>("1", new TimeWindow(101L, 201L))),
            equalTo(ValueAndTimestamp.make(1L, 150L)));
        assertThat(
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey()
                .get(new Windowed<>("1", new TimeWindow(50L, 150L))),
            equalTo(ValueAndTimestamp.make(2L, 150L)));
        assertThat(
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey()
                .get(new Windowed<>("1", new TimeWindow(400L, 500L))),
            equalTo(ValueAndTimestamp.make(1L, 500L)));
        assertThat(
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey()
                .get(new Windowed<>("2", new TimeWindow(100L, 200L))),
            equalTo(ValueAndTimestamp.make(2L, 200L)));
        assertThat(
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey()
                .get(new Windowed<>("2", new TimeWindow(50L, 150L))),
            equalTo(ValueAndTimestamp.make(1L, 150L)));
        assertThat(
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey()
                .get(new Windowed<>("2", new TimeWindow(151L, 251L))),
            equalTo(ValueAndTimestamp.make(1L, 200L)));
    }

    @Test
    public void shouldReduceSlidingWindows() {
        final MockProcessorSupplier<Windowed<String>, String> supplier = new MockProcessorSupplier<>();
        windowedStream
            .reduce(MockReducer.STRING_ADDER)
            .toStream()
            .process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
        }
        assertThat(
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey()
                .get(new Windowed<>("1", new TimeWindow(0L, 100L))),
            equalTo(ValueAndTimestamp.make("1", 100L)));
        assertThat(
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey()
                .get(new Windowed<>("1", new TimeWindow(101L, 201L))),
            equalTo(ValueAndTimestamp.make("2", 150L)));
        assertThat(
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey()
                .get(new Windowed<>("1", new TimeWindow(50L, 150L))),
            equalTo(ValueAndTimestamp.make("1+2", 150L)));
        assertThat(
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey()
                .get(new Windowed<>("1", new TimeWindow(400L, 500L))),
            equalTo(ValueAndTimestamp.make("3", 500L)));
        assertThat(
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey()
                .get(new Windowed<>("2", new TimeWindow(100L, 200L))),
            equalTo(ValueAndTimestamp.make("10+20", 200L)));
        assertThat(
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey()
                .get(new Windowed<>("2", new TimeWindow(50L, 150L))),
            equalTo(ValueAndTimestamp.make("20", 150L)));
        assertThat(
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey()
                .get(new Windowed<>("2", new TimeWindow(151L, 251L))),
            equalTo(ValueAndTimestamp.make("10", 200L)));
    }

    @Test
    public void shouldAggregateSlidingWindows() {
        final MockProcessorSupplier<Windowed<String>, String> supplier = new MockProcessorSupplier<>();
        windowedStream
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                Materialized.with(Serdes.String(), Serdes.String()))
            .toStream()
            .process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
        }
        assertThat(
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey()
                .get(new Windowed<>("1", new TimeWindow(0L, 100L))),
            equalTo(ValueAndTimestamp.make("0+1", 100L)));
        assertThat(
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey()
                .get(new Windowed<>("1", new TimeWindow(101L, 201L))),
            equalTo(ValueAndTimestamp.make("0+2", 150L)));
        assertThat(
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey()
                .get(new Windowed<>("1", new TimeWindow(50L, 150L))),
            equalTo(ValueAndTimestamp.make("0+1+2", 150L)));
        assertThat(
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey()
                .get(new Windowed<>("1", new TimeWindow(400L, 500L))),
            equalTo(ValueAndTimestamp.make("0+3", 500L)));
        assertThat(
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey()
                .get(new Windowed<>("2", new TimeWindow(100L, 200L))),
            equalTo(ValueAndTimestamp.make("0+10+20", 200L)));
        assertThat(
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey()
                .get(new Windowed<>("2", new TimeWindow(50L, 150L))),
            equalTo(ValueAndTimestamp.make("0+20", 150L)));
        assertThat(
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey()
                .get(new Windowed<>("2", new TimeWindow(151L, 251L))),
            equalTo(ValueAndTimestamp.make("0+10", 200L)));
    }

    @Test
    public void shouldMaterializeCount() {
        windowedStream.count(
            Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("count-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
            {
                final WindowStore<String, Long> windowStore = driver.getWindowStore("count-store");
                final List<KeyValue<Windowed<String>, Long>> data =
                    StreamsTestUtils.toList(windowStore.fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));

                assertThat(data, equalTo(Arrays.asList(
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(0, 100)), 1L),
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(50, 150)), 2L),
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(101, 201)), 1L),
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(400, 500)), 1L),
                    KeyValue.pair(new Windowed<>("2", new TimeWindow(50, 150)), 1L),
                    KeyValue.pair(new Windowed<>("2", new TimeWindow(100, 200)), 2L),
                    KeyValue.pair(new Windowed<>("2", new TimeWindow(151, 251)), 1L))));
            }
            {
                final WindowStore<String, ValueAndTimestamp<Long>> windowStore =
                    driver.getTimestampedWindowStore("count-store");
                final List<KeyValue<Windowed<String>, ValueAndTimestamp<Long>>> data =
                    StreamsTestUtils.toList(windowStore.fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));
                assertThat(data, equalTo(Arrays.asList(
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(0, 100)), ValueAndTimestamp.make(1L, 100L)),
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(50, 150)), ValueAndTimestamp.make(2L, 150L)),
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(101, 201)), ValueAndTimestamp.make(1L, 150L)),
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(400, 500)), ValueAndTimestamp.make(1L, 500L)),
                    KeyValue.pair(new Windowed<>("2", new TimeWindow(50, 150)), ValueAndTimestamp.make(1L, 150L)),
                    KeyValue.pair(new Windowed<>("2", new TimeWindow(100, 200)), ValueAndTimestamp.make(2L, 200L)),
                    KeyValue.pair(new Windowed<>("2", new TimeWindow(151, 251)), ValueAndTimestamp.make(1L, 200L)))));            }
        }
    }

    @Test
    public void shouldMaterializeReduced() {
        windowedStream.reduce(
            MockReducer.STRING_ADDER,
            Materialized.<String, String, WindowStore<Bytes, byte[]>>as("reduced")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
            {
                final WindowStore<String, String> windowStore = driver.getWindowStore("reduced");
                final List<KeyValue<Windowed<String>, String>> data =
                    StreamsTestUtils.toList(windowStore.fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));
                assertThat(data, equalTo(Arrays.asList(
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(0, 100)), "1"),
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(50, 150)), "1+2"),
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(101, 201)), "2"),
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(400, 500)), "3"),
                    KeyValue.pair(new Windowed<>("2", new TimeWindow(50, 150)), "20"),
                    KeyValue.pair(new Windowed<>("2", new TimeWindow(100, 200)), "10+20"),
                    KeyValue.pair(new Windowed<>("2", new TimeWindow(151, 251)), "10"))));
            }
            {
                final WindowStore<String, ValueAndTimestamp<Long>> windowStore =
                    driver.getTimestampedWindowStore("reduced");
                final List<KeyValue<Windowed<String>, ValueAndTimestamp<Long>>> data =
                    StreamsTestUtils.toList(windowStore.fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));
                assertThat(data, equalTo(Arrays.asList(
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(0, 100)), ValueAndTimestamp.make("1", 100L)),
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(50, 150)), ValueAndTimestamp.make("1+2", 150L)),
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(101, 201)), ValueAndTimestamp.make("2", 150L)),
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(400, 500)), ValueAndTimestamp.make("3", 500L)),
                    KeyValue.pair(new Windowed<>("2", new TimeWindow(50, 150)), ValueAndTimestamp.make("20", 150L)),
                    KeyValue.pair(new Windowed<>("2", new TimeWindow(100, 200)), ValueAndTimestamp.make("10+20", 200L)),
                    KeyValue.pair(new Windowed<>("2", new TimeWindow(151, 251)), ValueAndTimestamp.make("10", 200L)))));
            }
        }
    }

    @Test
    public void shouldMaterializeAggregated() {
        windowedStream.aggregate(
            MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            Materialized.<String, String, WindowStore<Bytes, byte[]>>as("aggregated")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
            {
                final WindowStore<String, String> windowStore = driver.getWindowStore("aggregated");
                final List<KeyValue<Windowed<String>, String>> data =
                    StreamsTestUtils.toList(windowStore.fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));
                assertThat(data, equalTo(Arrays.asList(
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(0, 100)), "0+1"),
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(50, 150)), "0+1+2"),
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(101, 201)), "0+2"),
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(400, 500)), "0+3"),
                    KeyValue.pair(new Windowed<>("2", new TimeWindow(50, 150)), "0+20"),
                    KeyValue.pair(new Windowed<>("2", new TimeWindow(100, 200)), "0+10+20"),
                    KeyValue.pair(new Windowed<>("2", new TimeWindow(151, 251)), "0+10"))));
            }
            {
                final WindowStore<String, ValueAndTimestamp<Long>> windowStore =
                    driver.getTimestampedWindowStore("aggregated");
                final List<KeyValue<Windowed<String>, ValueAndTimestamp<Long>>> data =
                    StreamsTestUtils.toList(windowStore.fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));
                assertThat(data, equalTo(Arrays.asList(
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(0, 100)), ValueAndTimestamp.make("0+1", 100L)),
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(50, 150)), ValueAndTimestamp.make("0+1+2", 150L)),
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(101, 201)), ValueAndTimestamp.make("0+2", 150L)),
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(400, 500)), ValueAndTimestamp.make("0+3", 500L)),
                    KeyValue.pair(new Windowed<>("2", new TimeWindow(50, 150)), ValueAndTimestamp.make("0+20", 150L)),
                    KeyValue.pair(new Windowed<>("2", new TimeWindow(100, 200)), ValueAndTimestamp.make("0+10+20", 200L)),
                    KeyValue.pair(new Windowed<>("2", new TimeWindow(151, 251)), ValueAndTimestamp.make("0+10", 200L)))));
            }
        }
    }

    @Test
    public void shouldThrowNullPointerOnAggregateIfInitializerIsNull() {
        assertThrows(NullPointerException.class, () -> windowedStream.aggregate(null, MockAggregator.TOSTRING_ADDER));
    }

    @Test
    public void shouldThrowNullPointerOnAggregateIfAggregatorIsNull() {
        assertThrows(NullPointerException.class, () -> windowedStream.aggregate(MockInitializer.STRING_INIT, null));
    }

    @Test
    public void shouldThrowNullPointerOnReduceIfReducerIsNull() {
        assertThrows(NullPointerException.class, () -> windowedStream.reduce(null));
    }

    @Test
    public void shouldThrowNullPointerOnMaterializedAggregateIfInitializerIsNull() {
        assertThrows(NullPointerException.class, () -> windowedStream.aggregate(null, MockAggregator.TOSTRING_ADDER, Materialized.as("store")));
    }

    @Test
    public void shouldThrowNullPointerOnMaterializedAggregateIfAggregatorIsNull() {
        assertThrows(NullPointerException.class, () -> windowedStream.aggregate(
            MockInitializer.STRING_INIT,
            null,
            Materialized.as("store")));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldThrowNullPointerOnMaterializedAggregateIfMaterializedIsNull() {
        assertThrows(NullPointerException.class, () -> windowedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, (Materialized) null));
    }

    @Test
    public void shouldThrowNullPointerOnMaterializedReduceIfReducerIsNull() {
        assertThrows(NullPointerException.class, () -> windowedStream.reduce(null, Materialized.as("store")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldThrowNullPointerOnMaterializedReduceIfMaterializedIsNull() {
        assertThrows(NullPointerException.class, () -> windowedStream.reduce(MockReducer.STRING_ADDER, (Materialized) null));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldThrowNullPointerOnMaterializedReduceIfNamedIsNull() {
        assertThrows(NullPointerException.class, () -> windowedStream.reduce(MockReducer.STRING_ADDER, (Named) null));
    }

    @Test
    public void shouldThrowNullPointerOnCountIfMaterializedIsNull() {
        assertThrows(NullPointerException.class, () -> windowedStream.count((Materialized<String, Long, WindowStore<Bytes, byte[]>>) null));
    }

    @Test
    public void shouldThrowIllegalArgumentWhenRetentionIsTooSmall() {
        assertThrows(IllegalArgumentException.class, () -> windowedStream
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                Materialized
                    .<String, String, WindowStore<Bytes, byte[]>>as("aggregated")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .withRetention(ofMillis(1L))
            )
        );
    }

    @Test
    public void shouldDropWindowsOutsideOfRetention() {
        final WindowBytesStoreSupplier storeSupplier = Stores.inMemoryWindowStore("aggregated", ofMillis(1200L), ofMillis(100L), false);
        windowedStream.aggregate(
            MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            Materialized.<String, String>as(storeSupplier)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
                .withCachingDisabled());

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(TOPIC, new StringSerializer(), new StringSerializer());

            inputTopic.pipeInput("1", "2", 100L);
            inputTopic.pipeInput("1", "3", 500L);
            inputTopic.pipeInput("1", "4", 799L);
            inputTopic.pipeInput("1", "4", 1000L);
            inputTopic.pipeInput("1", "5", 2000L);

            {
                final WindowStore<String, String> windowStore = driver.getWindowStore("aggregated");
                final List<KeyValue<Windowed<String>, String>> data =
                    StreamsTestUtils.toList(windowStore.fetch("1", "1", ofEpochMilli(0), ofEpochMilli(10000L)));
                assertThat(data, equalTo(Arrays.asList(
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(900, 1000)), "0+4"),
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(1900, 2000)), "0+5"))));
            }
            {
                final WindowStore<String, ValueAndTimestamp<Long>> windowStore =
                    driver.getTimestampedWindowStore("aggregated");
                final List<KeyValue<Windowed<String>, ValueAndTimestamp<Long>>> data =
                    StreamsTestUtils.toList(windowStore.fetch("1", "1", ofEpochMilli(0), ofEpochMilli(2000L)));
                assertThat(data, equalTo(Arrays.asList(
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(900, 1000)), ValueAndTimestamp.make("0+4", 1000L)),
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(1900, 2000)), ValueAndTimestamp.make("0+5", 2000L)))));
            }
        }
    }

    private void processData(final TopologyTestDriver driver) {
        final TestInputTopic<String, String> inputTopic =
            driver.createInputTopic(TOPIC, new StringSerializer(), new StringSerializer());
        inputTopic.pipeInput("1", "1", 100L);
        inputTopic.pipeInput("1", "2", 150L);
        inputTopic.pipeInput("1", "3", 500L);
        inputTopic.pipeInput("2", "10", 200L);
        inputTopic.pipeInput("2", "20", 150L);
    }
}