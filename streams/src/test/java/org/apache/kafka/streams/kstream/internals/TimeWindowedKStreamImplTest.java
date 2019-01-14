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
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.time.Duration.ofMillis;
import static java.time.Instant.ofEpochMilli;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class TimeWindowedKStreamImplTest {

    private static final String TOPIC = "input";
    private final StreamsBuilder builder = new StreamsBuilder();
    private final ConsumerRecordFactory<String, String> recordFactory =
        new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
    private TimeWindowedKStream<String, String> windowedStream;

    @Before
    public void before() {
        final KStream<String, String> stream = builder.stream(TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        windowedStream = stream.
            groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(TimeWindows.of(ofMillis(500L)));
    }

    @Test
    public void shouldCountWindowed() {
        final Map<Windowed<String>, Long> results = new HashMap<>();
        windowedStream
            .count()
            .toStream()
            .foreach(results::put);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props, 0L)) {
            processData(driver);
        }
        assertThat(results.get(new Windowed<>("1", new TimeWindow(0, 500))), equalTo(2L));
        assertThat(results.get(new Windowed<>("2", new TimeWindow(500, 1000))), equalTo(1L));
        assertThat(results.get(new Windowed<>("1", new TimeWindow(500, 1000))), equalTo(1L));
    }


    @Test
    public void shouldReduceWindowed() {
        final Map<Windowed<String>, String> results = new HashMap<>();
        windowedStream
            .reduce(MockReducer.STRING_ADDER)
            .toStream()
            .foreach(results::put);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props, 0L)) {
            processData(driver);
        }
        assertThat(results.get(new Windowed<>("1", new TimeWindow(0, 500))), equalTo("1+2"));
        assertThat(results.get(new Windowed<>("2", new TimeWindow(500, 1000))), equalTo("1"));
        assertThat(results.get(new Windowed<>("1", new TimeWindow(500, 1000))), equalTo("3"));
    }

    @Test
    public void shouldAggregateWindowed() {
        final Map<Windowed<String>, String> results = new HashMap<>();
        windowedStream
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                Materialized.with(Serdes.String(), Serdes.String()))
            .toStream()
            .foreach(results::put);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props, 0L)) {
            processData(driver);
        }
        assertThat(results.get(new Windowed<>("1", new TimeWindow(0, 500))), equalTo("0+1+2"));
        assertThat(results.get(new Windowed<>("2", new TimeWindow(500, 1000))), equalTo("0+1"));
        assertThat(results.get(new Windowed<>("1", new TimeWindow(500, 1000))), equalTo("0+3"));
    }

    @Test
    public void shouldMaterializeCount() {
        windowedStream.count(
            Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("count-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props, 0L)) {
            processData(driver);
            final WindowStore<String, Long> windowStore = driver.getWindowStore("count-store");
            final List<KeyValue<Windowed<String>, Long>> data =
                StreamsTestUtils.toList(windowStore.fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));

            assertThat(data, equalTo(Arrays.asList(
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(0, 500)), 2L),
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(500, 1000)), 1L),
                    KeyValue.pair(new Windowed<>("2", new TimeWindow(500, 1000)), 1L))));
        }
    }

    @Test
    public void shouldMaterializeReduced() {
        windowedStream.reduce(
            MockReducer.STRING_ADDER,
            Materialized.<String, String, WindowStore<Bytes, byte[]>>as("reduced")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props, 0L)) {
            processData(driver);
            final WindowStore<String, String> windowStore = driver.getWindowStore("reduced");
            final List<KeyValue<Windowed<String>, String>> data =
                StreamsTestUtils.toList(windowStore.fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));

            assertThat(data, equalTo(Arrays.asList(
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(0, 500)), "1+2"),
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(500, 1000)), "3"),
                    KeyValue.pair(new Windowed<>("2", new TimeWindow(500, 1000)), "1"))));
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

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props, 0L)) {
            processData(driver);
            final WindowStore<String, String> windowStore = driver.getWindowStore("aggregated");
            final List<KeyValue<Windowed<String>, String>> data =
                StreamsTestUtils.toList(windowStore.fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));

            assertThat(data, equalTo(Arrays.asList(
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(0, 500)), "0+1+2"),
                    KeyValue.pair(new Windowed<>("1", new TimeWindow(500, 1000)), "0+3"),
                    KeyValue.pair(new Windowed<>("2", new TimeWindow(500, 1000)), "0+1"))));
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnAggregateIfInitializerIsNull() {
        windowedStream.aggregate(null, MockAggregator.TOSTRING_ADDER);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnAggregateIfAggregatorIsNull() {
        windowedStream.aggregate(MockInitializer.STRING_INIT, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnReduceIfReducerIsNull() {
        windowedStream.reduce(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnMaterializedAggregateIfInitializerIsNull() {
        windowedStream.aggregate(
            null,
            MockAggregator.TOSTRING_ADDER,
            Materialized.as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnMaterializedAggregateIfAggregatorIsNull() {
        windowedStream.aggregate(
            MockInitializer.STRING_INIT,
            null,
            Materialized.as("store"));
    }

    @SuppressWarnings("unchecked")
    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnMaterializedAggregateIfMaterializedIsNull() {
        windowedStream.aggregate(
            MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            (Materialized) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnMaterializedReduceIfReducerIsNull() {
        windowedStream.reduce(
            null,
            Materialized.as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnMaterializedReduceIfMaterializedIsNull() {
        windowedStream.reduce(
            MockReducer.STRING_ADDER,
            null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnCountIfMaterializedIsNull() {
        windowedStream.count(null);
    }

    private void processData(final TopologyTestDriver driver) {
        driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 10L));
        driver.pipeInput(recordFactory.create(TOPIC, "1", "2", 15L));
        driver.pipeInput(recordFactory.create(TOPIC, "1", "3", 500L));
        driver.pipeInput(recordFactory.create(TOPIC, "2", "1", 500L));
    }

}
