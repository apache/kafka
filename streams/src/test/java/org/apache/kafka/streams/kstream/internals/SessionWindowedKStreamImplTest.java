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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
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
import java.util.Map;
import java.util.Properties;

import static java.time.Duration.ofMillis;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class SessionWindowedKStreamImplTest {
    private static final String TOPIC = "input";
    private final StreamsBuilder builder = new StreamsBuilder();
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
    private final Merger<String, String> sessionMerger = (aggKey, aggOne, aggTwo) -> aggOne + "+" + aggTwo;
    private SessionWindowedKStream<String, String> stream;

    @Before
    public void before() {
        final KStream<String, String> stream = builder.stream(TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        this.stream = stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(SessionWindows.with(ofMillis(500)));
    }

    @Test
    public void shouldCountSessionWindowedWithCachingDisabled() {
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        shouldCountSessionWindowed();
    }

    @Test
    public void shouldCountSessionWindowedWithCachingEnabled() {
        shouldCountSessionWindowed();
    }

    private void shouldCountSessionWindowed() {
        final MockProcessorSupplier<Windowed<String>, Long> supplier = new MockProcessorSupplier<>();
        stream.count()
            .toStream()
            .process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
        }

        final Map<Windowed<String>, ValueAndTimestamp<Long>> result =
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey();

        assertThat(result.size(), equalTo(3));
        assertThat(
            result.get(new Windowed<>("1", new SessionWindow(10L, 15L))),
            equalTo(ValueAndTimestamp.make(2L, 15L)));
        assertThat(
            result.get(new Windowed<>("2", new SessionWindow(599L, 600L))),
            equalTo(ValueAndTimestamp.make(2L, 600L)));
        assertThat(
            result.get(new Windowed<>("1", new SessionWindow(600L, 600L))),
            equalTo(ValueAndTimestamp.make(1L, 600L)));
    }

    @Test
    public void shouldReduceWindowed() {
        final MockProcessorSupplier<Windowed<String>, String> supplier = new MockProcessorSupplier<>();
        stream.reduce(MockReducer.STRING_ADDER)
            .toStream()
            .process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
        }

        final Map<Windowed<String>, ValueAndTimestamp<String>> result =
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey();

        assertThat(result.size(), equalTo(3));
        assertThat(
            result.get(new Windowed<>("1", new SessionWindow(10, 15))),
            equalTo(ValueAndTimestamp.make("1+2", 15L)));
        assertThat(
            result.get(new Windowed<>("2", new SessionWindow(599L, 600))),
            equalTo(ValueAndTimestamp.make("1+2", 600L)));
        assertThat(
            result.get(new Windowed<>("1", new SessionWindow(600, 600))),
            equalTo(ValueAndTimestamp.make("3", 600L)));
    }

    @Test
    public void shouldAggregateSessionWindowed() {
        final MockProcessorSupplier<Windowed<String>, String> supplier = new MockProcessorSupplier<>();
        stream.aggregate(MockInitializer.STRING_INIT,
                         MockAggregator.TOSTRING_ADDER,
                         sessionMerger,
                         Materialized.with(Serdes.String(), Serdes.String()))
            .toStream()
            .process(supplier);
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
        }

        final Map<Windowed<String>, ValueAndTimestamp<String>> result =
            supplier.theCapturedProcessor().lastValueAndTimestampPerKey();

        assertThat(result.size(), equalTo(3));
        assertThat(
            result.get(new Windowed<>("1", new SessionWindow(10, 15))),
            equalTo(ValueAndTimestamp.make("0+0+1+2", 15L)));
        assertThat(
            result.get(new Windowed<>("2", new SessionWindow(599, 600))),
            equalTo(ValueAndTimestamp.make("0+0+1+2", 600L)));
        assertThat(
            result.get(new Windowed<>("1", new SessionWindow(600, 600))),
            equalTo(ValueAndTimestamp.make("0+3", 600L)));
    }

    @Test
    public void shouldMaterializeCount() {
        stream.count(Materialized.as("count-store"));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
            final SessionStore<String, Long> store = driver.getSessionStore("count-store");
            final List<KeyValue<Windowed<String>, Long>> data = StreamsTestUtils.toList(store.fetch("1", "2"));
            assertThat(
                data,
                equalTo(Arrays.asList(
                    KeyValue.pair(new Windowed<>("1", new SessionWindow(10, 15)), 2L),
                    KeyValue.pair(new Windowed<>("1", new SessionWindow(600, 600)), 1L),
                    KeyValue.pair(new Windowed<>("2", new SessionWindow(599, 600)), 2L))));
        }
    }

    @Test
    public void shouldMaterializeReduced() {
        stream.reduce(MockReducer.STRING_ADDER, Materialized.as("reduced"));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
            final SessionStore<String, String> sessionStore = driver.getSessionStore("reduced");
            final List<KeyValue<Windowed<String>, String>> data = StreamsTestUtils.toList(sessionStore.fetch("1", "2"));

            assertThat(
                data,
                equalTo(Arrays.asList(
                    KeyValue.pair(new Windowed<>("1", new SessionWindow(10, 15)), "1+2"),
                    KeyValue.pair(new Windowed<>("1", new SessionWindow(600, 600)), "3"),
                    KeyValue.pair(new Windowed<>("2", new SessionWindow(599, 600)), "1+2"))));
        }
    }

    @Test
    public void shouldMaterializeAggregated() {
        stream.aggregate(
            MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            sessionMerger,
            Materialized.<String, String, SessionStore<Bytes, byte[]>>as("aggregated").withValueSerde(Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
            final SessionStore<String, String> sessionStore = driver.getSessionStore("aggregated");
            final List<KeyValue<Windowed<String>, String>> data = StreamsTestUtils.toList(sessionStore.fetch("1", "2"));
            assertThat(
                data,
                equalTo(Arrays.asList(
                    KeyValue.pair(new Windowed<>("1", new SessionWindow(10, 15)), "0+0+1+2"),
                    KeyValue.pair(new Windowed<>("1", new SessionWindow(600, 600)), "0+3"),
                    KeyValue.pair(new Windowed<>("2", new SessionWindow(599, 600)), "0+0+1+2"))));
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
        stream.aggregate(
            null,
            MockAggregator.TOSTRING_ADDER,
            sessionMerger,
            Materialized.as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnMaterializedAggregateIfAggregatorIsNull() {
        stream.aggregate(
            MockInitializer.STRING_INIT,
            null,
            sessionMerger,
            Materialized.as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnMaterializedAggregateIfMergerIsNull() {
        stream.aggregate(
            MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            null,
            Materialized.as("store"));
    }

    @SuppressWarnings("unchecked")
    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnMaterializedAggregateIfMaterializedIsNull() {
        stream.aggregate(
            MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            sessionMerger,
            (Materialized) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnMaterializedReduceIfReducerIsNull() {
        stream.reduce(null, Materialized.as("store"));
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("unchecked")
    public void shouldThrowNullPointerOnMaterializedReduceIfMaterializedIsNull() {
        stream.reduce(MockReducer.STRING_ADDER, (Materialized) null);
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("unchecked")
    public void shouldThrowNullPointerOnMaterializedReduceIfNamedIsNull() {
        stream.reduce(MockReducer.STRING_ADDER, (Named) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnCountIfMaterializedIsNull() {
        stream.count((Materialized<String, Long, SessionStore<Bytes, byte[]>>) null);
    }

    private void processData(final TopologyTestDriver driver) {
        final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(TOPIC, new StringSerializer(), new StringSerializer());
        inputTopic.pipeInput("1", "1", 10);
        inputTopic.pipeInput("1", "2", 15);
        inputTopic.pipeInput("1", "3", 600);
        inputTopic.pipeInput("2", "1", 600);
        inputTopic.pipeInput("2", "2", 599);
    }
}
