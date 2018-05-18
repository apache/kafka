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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class TimeWindowedKStreamImplTest {

    private static final String TOPIC = "input";
    private final StreamsBuilder builder = new StreamsBuilder();

    @Rule
    public final KStreamTestDriver driver = new KStreamTestDriver();
    private TimeWindowedKStream<String, String> windowedStream;

    @Before
    public void before() {
        final KStream<String, String> stream = builder.stream(TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        windowedStream = stream.groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.of(500L));
    }

    @Test
    public void shouldCountWindowed() {
        final Map<Windowed<String>, Long> results = new HashMap<>();
        windowedStream.count()
                .toStream()
                .foreach(new ForeachAction<Windowed<String>, Long>() {
                    @Override
                    public void apply(final Windowed<String> key, final Long value) {
                        results.put(key, value);
                    }
                });

        processData();
        assertThat(results.get(new Windowed<>("1", new TimeWindow(0, 500))), equalTo(2L));
        assertThat(results.get(new Windowed<>("2", new TimeWindow(500, 1000))), equalTo(1L));
        assertThat(results.get(new Windowed<>("1", new TimeWindow(500, 1000))), equalTo(1L));
    }


    @Test
    public void shouldReduceWindowed() {
        final Map<Windowed<String>, String> results = new HashMap<>();
        windowedStream.reduce(MockReducer.STRING_ADDER)
                .toStream()
                .foreach(new ForeachAction<Windowed<String>, String>() {
                    @Override
                    public void apply(final Windowed<String> key, final String value) {
                        results.put(key, value);
                    }
                });

        processData();
        assertThat(results.get(new Windowed<>("1", new TimeWindow(0, 500))), equalTo("1+2"));
        assertThat(results.get(new Windowed<>("2", new TimeWindow(500, 1000))), equalTo("1"));
        assertThat(results.get(new Windowed<>("1", new TimeWindow(500, 1000))), equalTo("3"));
    }

    @Test
    public void shouldAggregateWindowed() {
        final Map<Windowed<String>, String> results = new HashMap<>();
        windowedStream.aggregate(MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                Materialized.<String, String, WindowStore<Bytes, byte[]>>with(Serdes.String(), Serdes.String()
        ))
                .toStream()
                .foreach(new ForeachAction<Windowed<String>, String>() {
                    @Override
                    public void apply(final Windowed<String> key, final String value) {
                        results.put(key, value);
                    }
                });
        processData();
        assertThat(results.get(new Windowed<>("1", new TimeWindow(0, 500))), equalTo("0+1+2"));
        assertThat(results.get(new Windowed<>("2", new TimeWindow(500, 1000))), equalTo("0+1"));
        assertThat(results.get(new Windowed<>("1", new TimeWindow(500, 1000))), equalTo("0+3"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldMaterializeCount() {
        windowedStream.count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("count-store")
                                     .withKeySerde(Serdes.String())
                                     .withValueSerde(Serdes.Long()));

        processData();
        final WindowStore<String, Long> windowStore = (WindowStore<String, Long>) driver.allStateStores().get("count-store");
        final List<KeyValue<Windowed<String>, Long>> data = StreamsTestUtils.toList(windowStore.fetch("1", "2", 0, 1000));
        assertThat(data, equalTo(Arrays.asList(
                KeyValue.pair(new Windowed<>("1", new TimeWindow(0, 500)), 2L),
                KeyValue.pair(new Windowed<>("1", new TimeWindow(500, 1000)), 1L),
                KeyValue.pair(new Windowed<>("2", new TimeWindow(500, 1000)), 1L))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldMaterializeReduced() {
        windowedStream.reduce(MockReducer.STRING_ADDER,
                              Materialized.<String, String, WindowStore<Bytes, byte[]>>as("reduced")
                                      .withKeySerde(Serdes.String())
                                      .withValueSerde(Serdes.String()));

        processData();
        final WindowStore<String, String> windowStore = (WindowStore<String, String>) driver.allStateStores().get("reduced");
        final List<KeyValue<Windowed<String>, String>> data = StreamsTestUtils.toList(windowStore.fetch("1", "2", 0, 1000));

        assertThat(data, equalTo(Arrays.asList(
                KeyValue.pair(new Windowed<>("1", new TimeWindow(0, 500)), "1+2"),
                KeyValue.pair(new Windowed<>("1", new TimeWindow(500, 1000)), "3"),
                KeyValue.pair(new Windowed<>("2", new TimeWindow(500, 1000)), "1"))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldMaterializeAggregated() {
        windowedStream.aggregate(MockInitializer.STRING_INIT,
                                 MockAggregator.TOSTRING_ADDER,
                                 Materialized.<String, String, WindowStore<Bytes, byte[]>>as("aggregated")
                                         .withKeySerde(Serdes.String())
                                         .withValueSerde(Serdes.String()));

        processData();
        final WindowStore<String, String> windowStore = (WindowStore<String, String>) driver.allStateStores().get("aggregated");
        final List<KeyValue<Windowed<String>, String>> data = StreamsTestUtils.toList(windowStore.fetch("1", "2", 0, 1000));
        assertThat(data, equalTo(Arrays.asList(
                KeyValue.pair(new Windowed<>("1", new TimeWindow(0, 500)), "0+1+2"),
                KeyValue.pair(new Windowed<>("1", new TimeWindow(500, 1000)), "0+3"),
                KeyValue.pair(new Windowed<>("2", new TimeWindow(500, 1000)), "0+1"))));
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
        windowedStream.aggregate(null,
                                 MockAggregator.TOSTRING_ADDER,
                                 Materialized.<String, String, WindowStore<Bytes, byte[]>>as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnMaterializedAggregateIfAggregatorIsNull() {
        windowedStream.aggregate(MockInitializer.STRING_INIT,
                                 null,
                                 Materialized.<String, String, WindowStore<Bytes, byte[]>>as("store"));
    }

    @SuppressWarnings("unchecked")
    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnMaterializedAggregateIfMaterializedIsNull() {
        windowedStream.aggregate(MockInitializer.STRING_INIT,
                                 MockAggregator.TOSTRING_ADDER,
                                 (Materialized) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnMaterializedReduceIfReducerIsNull() {
        windowedStream.reduce(null,
                              Materialized.<String, String, WindowStore<Bytes, byte[]>>as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnMaterializedReduceIfMaterializedIsNull() {
        windowedStream.reduce(MockReducer.STRING_ADDER,
                              null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnCountIfMaterializedIsNull() {
        windowedStream.count(null);
    }

    private void processData() {
        driver.setUp(builder, TestUtils.tempDirectory(), 0);
        driver.setTime(10);
        driver.process(TOPIC, "1", "1");
        driver.setTime(15);
        driver.process(TOPIC, "1", "2");
        driver.setTime(500);
        driver.process(TOPIC, "1", "3");
        driver.process(TOPIC, "2", "1");
        driver.flushState();
    }

}