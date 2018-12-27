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
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.SessionStore;
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
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class SessionWindowedKStreamImplTest {

    private static final String TOPIC = "input";
    private final StreamsBuilder builder = new StreamsBuilder();
    private final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    private final Merger<String, String> sessionMerger = new Merger<String, String>() {
        @Override
        public String apply(final String aggKey, final String aggOne, final String aggTwo) {
            return aggOne + "+" + aggTwo;
        }
    };
    private SessionWindowedKStream<String, String> stream;

    @Before
    public void before() {
        final KStream<String, String> stream = builder.stream(TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        this.stream = stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(SessionWindows.with(ofMillis(500)));
    }

    @Test
    public void shouldCountSessionWindowed() {
        final Map<Windowed<String>, Long> results = new HashMap<>();
        stream.count()
                .toStream()
                .foreach(new ForeachAction<Windowed<String>, Long>() {
                    @Override
                    public void apply(final Windowed<String> key, final Long value) {
                        results.put(key, value);
                    }
                });

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props, 0L)) {
            processData(driver);
        }
        assertThat(results.get(new Windowed<>("1", new SessionWindow(10, 15))), equalTo(2L));
        assertThat(results.get(new Windowed<>("2", new SessionWindow(600, 600))), equalTo(1L));
        assertThat(results.get(new Windowed<>("1", new SessionWindow(600, 600))), equalTo(1L));
    }

    @Test
    public void shouldReduceWindowed() {
        final Map<Windowed<String>, String> results = new HashMap<>();
        stream.reduce(MockReducer.STRING_ADDER)
                .toStream()
                .foreach(new ForeachAction<Windowed<String>, String>() {
                    @Override
                    public void apply(final Windowed<String> key, final String value) {
                        results.put(key, value);
                    }
                });

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props, 0L)) {
            processData(driver);
        }
        assertThat(results.get(new Windowed<>("1", new SessionWindow(10, 15))), equalTo("1+2"));
        assertThat(results.get(new Windowed<>("2", new SessionWindow(600, 600))), equalTo("1"));
        assertThat(results.get(new Windowed<>("1", new SessionWindow(600, 600))), equalTo("3"));
    }

    @Test
    public void shouldAggregateSessionWindowed() {
        final Map<Windowed<String>, String> results = new HashMap<>();
        stream.aggregate(MockInitializer.STRING_INIT,
                         MockAggregator.TOSTRING_ADDER,
                         sessionMerger,
                         Materialized.<String, String, SessionStore<Bytes, byte[]>>with(Serdes.String(), Serdes.String()))
                .toStream()
                .foreach(new ForeachAction<Windowed<String>, String>() {
                    @Override
                    public void apply(final Windowed<String> key, final String value) {
                        results.put(key, value);
                    }
                });
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props, 0L)) {
            processData(driver);
        }
        assertThat(results.get(new Windowed<>("1", new SessionWindow(10, 15))), equalTo("0+0+1+2"));
        assertThat(results.get(new Windowed<>("2", new SessionWindow(600, 600))), equalTo("0+1"));
        assertThat(results.get(new Windowed<>("1", new SessionWindow(600, 600))), equalTo("0+3"));
    }

    @Test
    public void shouldMaterializeCount() {
        stream.count(Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("count-store"));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props, 0L)) {
            processData(driver);
            final SessionStore<String, Long> store = driver.getSessionStore("count-store");
            final List<KeyValue<Windowed<String>, Long>> data = StreamsTestUtils.toList(store.fetch("1", "2"));
            assertThat(data, equalTo(Arrays.asList(
                    KeyValue.pair(new Windowed<>("1", new SessionWindow(10, 15)), 2L),
                    KeyValue.pair(new Windowed<>("1", new SessionWindow(600, 600)), 1L),
                    KeyValue.pair(new Windowed<>("2", new SessionWindow(600, 600)), 1L))));
        }
    }

    @Test
    public void shouldMaterializeReduced() {
        stream.reduce(MockReducer.STRING_ADDER, Materialized.<String, String, SessionStore<Bytes, byte[]>>as("reduced"));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props, 0L)) {
            processData(driver);
            final SessionStore<String, String> sessionStore = driver.getSessionStore("reduced");
            final List<KeyValue<Windowed<String>, String>> data = StreamsTestUtils.toList(sessionStore.fetch("1", "2"));

            assertThat(data, equalTo(Arrays.asList(
                    KeyValue.pair(new Windowed<>("1", new SessionWindow(10, 15)), "1+2"),
                    KeyValue.pair(new Windowed<>("1", new SessionWindow(600, 600)), "3"),
                    KeyValue.pair(new Windowed<>("2", new SessionWindow(600, 600)), "1"))));
        }
    }

    @Test
    public void shouldMaterializeAggregated() {
        stream.aggregate(MockInitializer.STRING_INIT,
                         MockAggregator.TOSTRING_ADDER,
                         sessionMerger,
                         Materialized.<String, String, SessionStore<Bytes, byte[]>>as("aggregated").withValueSerde(Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props, 0L)) {
            processData(driver);
            final SessionStore<String, String> sessionStore = driver.getSessionStore("aggregated");
            final List<KeyValue<Windowed<String>, String>> data = StreamsTestUtils.toList(sessionStore.fetch("1", "2"));
            assertThat(data, equalTo(Arrays.asList(
                    KeyValue.pair(new Windowed<>("1", new SessionWindow(10, 15)), "0+0+1+2"),
                    KeyValue.pair(new Windowed<>("1", new SessionWindow(600, 600)), "0+3"),
                    KeyValue.pair(new Windowed<>("2", new SessionWindow(600, 600)), "0+1"))));
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnAggregateIfInitializerIsNull() {
        stream.aggregate(null, MockAggregator.TOSTRING_ADDER, sessionMerger);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnAggregateIfAggregatorIsNull() {
        stream.aggregate(MockInitializer.STRING_INIT, null, sessionMerger);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnAggregateIfMergerIsNull() {
        stream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnReduceIfReducerIsNull() {
        stream.reduce(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnMaterializedAggregateIfInitializerIsNull() {
        stream.aggregate(null,
                         MockAggregator.TOSTRING_ADDER,
                         sessionMerger,
                         Materialized.<String, String, SessionStore<Bytes, byte[]>>as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnMaterializedAggregateIfAggregatorIsNull() {
        stream.aggregate(MockInitializer.STRING_INIT,
                         null,
                         sessionMerger,
                         Materialized.<String, String, SessionStore<Bytes, byte[]>>as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnMaterializedAggregateIfMergerIsNull() {
        stream.aggregate(MockInitializer.STRING_INIT,
                         MockAggregator.TOSTRING_ADDER,
                         null,
                         Materialized.<String, String, SessionStore<Bytes, byte[]>>as("store"));
    }

    @SuppressWarnings("unchecked")
    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnMaterializedAggregateIfMaterializedIsNull() {
        stream.aggregate(MockInitializer.STRING_INIT,
                         MockAggregator.TOSTRING_ADDER,
                         sessionMerger,
                         (Materialized) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnMaterializedReduceIfReducerIsNull() {
        stream.reduce(null,
                      Materialized.<String, String, SessionStore<Bytes, byte[]>>as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnMaterializedReduceIfMaterializedIsNull() {
        stream.reduce(MockReducer.STRING_ADDER,
                      null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnCountIfMaterializedIsNull() {
        stream.count(null);
    }

    private void processData(final TopologyTestDriver driver) {
        driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 10));
        driver.pipeInput(recordFactory.create(TOPIC, "1", "2", 15));
        driver.pipeInput(recordFactory.create(TOPIC, "1", "3", 600));
        driver.pipeInput(recordFactory.create(TOPIC, "2", "1", 600));
    }

}
