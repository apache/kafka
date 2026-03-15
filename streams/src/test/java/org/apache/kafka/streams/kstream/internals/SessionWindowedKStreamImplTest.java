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
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.internals.ChangeLoggingSessionBytesStore;
import org.apache.kafka.streams.state.internals.ChangeLoggingSessionBytesStoreWithHeaders;
import org.apache.kafka.streams.state.internals.MeteredSessionStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.StreamsTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SessionWindowedKStreamImplTest {
    private static final String TOPIC = "input";
    private final StreamsBuilder builder = new StreamsBuilder();
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
    private final Merger<String, String> sessionMerger = (aggKey, aggOne, aggTwo) -> aggOne + "+" + aggTwo;

    private SessionWindowedKStream<String, String> stream;

    public EmitStrategy.StrategyType type;

    private boolean emitFinal;

    static Stream<Arguments> emitStrategyAndHeaders() {
        return Stream.of(
            Arguments.of(EmitStrategy.StrategyType.ON_WINDOW_UPDATE, false),
            Arguments.of(EmitStrategy.StrategyType.ON_WINDOW_UPDATE, true),
            Arguments.of(EmitStrategy.StrategyType.ON_WINDOW_CLOSE, false),
            Arguments.of(EmitStrategy.StrategyType.ON_WINDOW_CLOSE, true)
        );
    }

    public void setup(final EmitStrategy.StrategyType inputType, final boolean withHeaders) {
        type = inputType;
        final EmitStrategy emitStrategy = EmitStrategy.StrategyType.forType(type);
        emitFinal = type.equals(EmitStrategy.StrategyType.ON_WINDOW_CLOSE);
        if (withHeaders) {
            props.put(StreamsConfig.DSL_STORE_FORMAT_CONFIG, StreamsConfig.DSL_STORE_FORMAT_HEADERS);
        }

        final KStream<String, String> stream = builder.stream(TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        this.stream = stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(ofMillis(500)))
            .emitStrategy(emitStrategy);
    }

    @ParameterizedTest
    @MethodSource("emitStrategyAndHeaders")
    public void shouldCountSessionWindowedWithCachingDisabled(final EmitStrategy.StrategyType inputType, final boolean withHeaders) {
        setup(inputType, withHeaders);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        shouldCountSessionWindowed();
    }

    @ParameterizedTest
    @MethodSource("emitStrategyAndHeaders")
    public void shouldCountSessionWindowedWithCachingEnabled(final EmitStrategy.StrategyType inputType, final boolean withHeaders) {
        setup(inputType, withHeaders);
        shouldCountSessionWindowed();
    }

    private void shouldCountSessionWindowed() {
        final MockApiProcessorSupplier<Windowed<String>, Long, Void, Void> supplier = new MockApiProcessorSupplier<>();
        stream.count()
            .toStream()
            .process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
        }

        final ArrayList<KeyValueTimestamp<Windowed<String>, Long>> processed =
            supplier.theCapturedProcessor().processed();

        if (emitFinal) {
            assertEquals(
                Collections.singletonList(
                    new KeyValueTimestamp<>(new Windowed<>("1", new SessionWindow(10L, 15L)), 2L, 15L)
                ),
                processed
            );
        } else {
            assertEquals(
                asList(
                    new KeyValueTimestamp<>(new Windowed<>("1", new SessionWindow(10L, 10L)), 1L, 10L),
                    new KeyValueTimestamp<>(new Windowed<>("1", new SessionWindow(10L, 10L)), null, 10L),
                    new KeyValueTimestamp<>(new Windowed<>("1", new SessionWindow(10L, 15L)), 2L, 15L),
                    new KeyValueTimestamp<>(new Windowed<>("1", new SessionWindow(600L, 600L)), 1L, 600L),
                    new KeyValueTimestamp<>(new Windowed<>("2", new SessionWindow(600L, 600L)), 1L, 600L),
                    new KeyValueTimestamp<>(new Windowed<>("2", new SessionWindow(600L, 600L)), null, 600L),
                    new KeyValueTimestamp<>(new Windowed<>("2", new SessionWindow(599L, 600L)), 2L, 600L)
                ),
                processed
            );
        }
    }

    @ParameterizedTest
    @MethodSource("emitStrategyAndHeaders")
    public void shouldReduceWindowed(final EmitStrategy.StrategyType inputType, final boolean withHeaders) {
        setup(inputType, withHeaders);
        final MockApiProcessorSupplier<Windowed<String>, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        stream.reduce(MockReducer.STRING_ADDER)
            .toStream()
            .process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
        }

        final ArrayList<KeyValueTimestamp<Windowed<String>, String>> processed =
                supplier.theCapturedProcessor().processed();

        if (emitFinal) {
            assertEquals(
                Collections.singletonList(
                    new KeyValueTimestamp<>(new Windowed<>("1", new SessionWindow(10L, 15L)), "1+2", 15L)
                ),
                processed
            );
        } else {
            assertEquals(
                asList(
                    new KeyValueTimestamp<>(new Windowed<>("1", new SessionWindow(10L, 10L)), "1", 10L),
                    new KeyValueTimestamp<>(new Windowed<>("1", new SessionWindow(10L, 10L)), null, 10L),
                    new KeyValueTimestamp<>(new Windowed<>("1", new SessionWindow(10L, 15L)), "1+2", 15L),
                    new KeyValueTimestamp<>(new Windowed<>("1", new SessionWindow(600L, 600L)), "3", 600L),
                    new KeyValueTimestamp<>(new Windowed<>("2", new SessionWindow(600L, 600L)), "1", 600L),
                    new KeyValueTimestamp<>(new Windowed<>("2", new SessionWindow(600L, 600L)), null, 600L),
                    new KeyValueTimestamp<>(new Windowed<>("2", new SessionWindow(599L, 600L)), "1+2", 600L)
                ),
                processed
            );
        }
    }

    @ParameterizedTest
    @MethodSource("emitStrategyAndHeaders")
    public void shouldAggregateSessionWindowed(final EmitStrategy.StrategyType inputType, final boolean withHeaders) {
        setup(inputType, withHeaders);
        final MockApiProcessorSupplier<Windowed<String>, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        stream.aggregate(MockInitializer.STRING_INIT,
                         MockAggregator.TOSTRING_ADDER,
                         sessionMerger,
                         Materialized.with(Serdes.String(), Serdes.String()))
            .toStream()
            .process(supplier);
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
        }

        final ArrayList<KeyValueTimestamp<Windowed<String>, String>> processed =
                supplier.theCapturedProcessor().processed();

        if (emitFinal) {
            assertEquals(
                Collections.singletonList(
                    new KeyValueTimestamp<>(new Windowed<>("1", new SessionWindow(10L, 15L)), "0+0+1+2", 15L)
                ),
                processed
            );
        } else {
            assertEquals(
                asList(
                    new KeyValueTimestamp<>(new Windowed<>("1", new SessionWindow(10L, 10L)), "0+1", 10L),
                    new KeyValueTimestamp<>(new Windowed<>("1", new SessionWindow(10L, 10L)), null, 10L),
                    new KeyValueTimestamp<>(new Windowed<>("1", new SessionWindow(10L, 15L)), "0+0+1+2", 15L),
                    new KeyValueTimestamp<>(new Windowed<>("1", new SessionWindow(600L, 600L)), "0+3", 600L),
                    new KeyValueTimestamp<>(new Windowed<>("2", new SessionWindow(600L, 600L)), "0+1", 600L),
                    new KeyValueTimestamp<>(new Windowed<>("2", new SessionWindow(600L, 600L)), null, 600L),
                    new KeyValueTimestamp<>(new Windowed<>("2", new SessionWindow(599L, 600L)), "0+0+1+2", 600L)
                ),
                processed
            );
        }
    }

    @ParameterizedTest
    @MethodSource("emitStrategyAndHeaders")
    public void shouldMaterializeCount(final EmitStrategy.StrategyType inputType, final boolean withHeaders) {
        setup(inputType, withHeaders);
        stream.count(Materialized.as("count-store"));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
            final SessionStore<String, Long> store = driver.getSessionStore("count-store");
            final List<KeyValue<Windowed<String>, Long>> data = unwrapAggregations(store.fetch("1", "2"));
            if (!emitFinal) {
                assertThat(
                        data,
                        equalTo(Arrays.asList(
                                KeyValue.pair(new Windowed<>("1", new SessionWindow(10, 15)), 2L),
                                KeyValue.pair(new Windowed<>("1", new SessionWindow(600, 600)), 1L),
                                KeyValue.pair(new Windowed<>("2", new SessionWindow(599, 600)), 2L))));
            } else {
                assertThat(
                        data,
                        equalTo(Arrays.asList(
                                KeyValue.pair(new Windowed<>("1", new SessionWindow(600, 600)), 1L),
                                KeyValue.pair(new Windowed<>("2", new SessionWindow(599, 600)), 2L))));

            }
        }
    }

    @ParameterizedTest
    @MethodSource("emitStrategyAndHeaders")
    public void shouldMaterializeReduced(final EmitStrategy.StrategyType inputType, final boolean withHeaders) {
        setup(inputType, withHeaders);
        stream.reduce(MockReducer.STRING_ADDER, Materialized.as("reduced"));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
            final SessionStore<String, String> sessionStore = driver.getSessionStore("reduced");
            final List<KeyValue<Windowed<String>, String>> data = unwrapAggregations(sessionStore.fetch("1", "2"));

            if (!emitFinal) {
                assertThat(
                        data,
                        equalTo(Arrays.asList(
                                KeyValue.pair(new Windowed<>("1", new SessionWindow(10, 15)), "1+2"),
                                KeyValue.pair(new Windowed<>("1", new SessionWindow(600, 600)), "3"),
                                KeyValue.pair(new Windowed<>("2", new SessionWindow(599, 600)), "1+2"))));
            } else {
                assertThat(
                        data,
                        equalTo(Arrays.asList(
                                KeyValue.pair(new Windowed<>("1", new SessionWindow(600, 600)), "3"),
                                KeyValue.pair(new Windowed<>("2", new SessionWindow(599, 600)), "1+2"))));

            }
        }
    }

    @ParameterizedTest
    @MethodSource("emitStrategyAndHeaders")
    public void shouldMaterializeAggregated(final EmitStrategy.StrategyType inputType, final boolean withHeaders) {
        setup(inputType, withHeaders);
        stream.aggregate(
            MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            sessionMerger,
            Materialized.<String, String, SessionStore<Bytes, byte[]>>as("aggregated").withValueSerde(Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
            final SessionStore<String, String> sessionStore = driver.getSessionStore("aggregated");
            final List<KeyValue<Windowed<String>, String>> data = unwrapAggregations(sessionStore.fetch("1", "2"));
            if (!emitFinal) {
                assertThat(
                        data,
                        equalTo(Arrays.asList(
                                KeyValue.pair(new Windowed<>("1", new SessionWindow(10, 15)), "0+0+1+2"),
                                KeyValue.pair(new Windowed<>("1", new SessionWindow(600, 600)), "0+3"),
                                KeyValue.pair(new Windowed<>("2", new SessionWindow(599, 600)), "0+0+1+2"))));
            } else {
                assertThat(
                        data,
                        equalTo(Arrays.asList(
                                KeyValue.pair(new Windowed<>("1", new SessionWindow(600, 600)), "0+3"),
                                KeyValue.pair(new Windowed<>("2", new SessionWindow(599, 600)), "0+0+1+2"))));

            }
        }
    }

    @Test
    public void shouldThrowNullPointerOnAggregateIfInitializerIsNull() {
        setup(EmitStrategy.StrategyType.ON_WINDOW_UPDATE, false);
        assertThrows(NullPointerException.class, () -> stream.aggregate(null, MockAggregator.TOSTRING_ADDER, sessionMerger));
    }

    @Test
    public void shouldThrowNullPointerOnAggregateIfAggregatorIsNull() {
        setup(EmitStrategy.StrategyType.ON_WINDOW_UPDATE, false);
        assertThrows(NullPointerException.class, () -> stream.aggregate(MockInitializer.STRING_INIT, null, sessionMerger));
    }

    @Test
    public void shouldThrowNullPointerOnAggregateIfMergerIsNull() {
        setup(EmitStrategy.StrategyType.ON_WINDOW_UPDATE, false);
        assertThrows(NullPointerException.class, () -> stream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, null));
    }

    @Test
    public void shouldThrowNullPointerOnReduceIfReducerIsNull() {
        setup(EmitStrategy.StrategyType.ON_WINDOW_UPDATE, false);
        assertThrows(NullPointerException.class, () -> stream.reduce(null));
    }

    @Test
    public void shouldThrowNullPointerOnMaterializedAggregateIfInitializerIsNull() {
        setup(EmitStrategy.StrategyType.ON_WINDOW_UPDATE, false);
        assertThrows(NullPointerException.class, () -> stream.aggregate(
            null,
            MockAggregator.TOSTRING_ADDER,
            sessionMerger,
            Materialized.as("store")));
    }

    @Test
    public void shouldThrowNullPointerOnMaterializedAggregateIfAggregatorIsNull() {
        setup(EmitStrategy.StrategyType.ON_WINDOW_UPDATE, false);
        assertThrows(NullPointerException.class, () -> stream.aggregate(
            MockInitializer.STRING_INIT,
            null,
            sessionMerger,
            Materialized.as("store")));
    }

    @Test
    public void shouldThrowNullPointerOnMaterializedAggregateIfMergerIsNull() {
        setup(EmitStrategy.StrategyType.ON_WINDOW_UPDATE, false);
        assertThrows(NullPointerException.class, () -> stream.aggregate(
            MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            null,
            Materialized.as("store")));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldThrowNullPointerOnMaterializedAggregateIfMaterializedIsNull() {
        setup(EmitStrategy.StrategyType.ON_WINDOW_UPDATE, false);
        assertThrows(NullPointerException.class, () -> stream.aggregate(
            MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            sessionMerger,
            (Materialized) null));
    }

    @Test
    public void shouldThrowNullPointerOnMaterializedReduceIfReducerIsNull() {
        setup(EmitStrategy.StrategyType.ON_WINDOW_UPDATE, false);
        assertThrows(NullPointerException.class, () -> stream.reduce(null, Materialized.as("store")));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldThrowNullPointerOnMaterializedReduceIfMaterializedIsNull() {
        setup(EmitStrategy.StrategyType.ON_WINDOW_UPDATE, false);
        assertThrows(NullPointerException.class, () -> stream.reduce(MockReducer.STRING_ADDER, (Materialized) null));
    }

    @Test
    public void shouldThrowNullPointerOnMaterializedReduceIfNamedIsNull() {
        setup(EmitStrategy.StrategyType.ON_WINDOW_UPDATE, false);
        assertThrows(NullPointerException.class, () -> stream.reduce(MockReducer.STRING_ADDER, (Named) null));
    }

    @Test
    public void shouldThrowNullPointerOnCountIfMaterializedIsNull() {
        setup(EmitStrategy.StrategyType.ON_WINDOW_UPDATE, false);
        assertThrows(NullPointerException.class, () -> stream.count((Materialized<String, Long, SessionStore<Bytes, byte[]>>) null));
    }

    @ParameterizedTest
    @MethodSource("emitStrategyAndHeaders")
    public void shouldNotEnableCachingWithEmitFinal(final EmitStrategy.StrategyType inputType, final boolean withHeaders) {
        setup(inputType, withHeaders);
        if (!emitFinal)
            return;

        stream.aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                sessionMerger,
                Materialized.<String, String, SessionStore<Bytes, byte[]>>as("aggregated").withValueSerde(Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final StateStore store = driver.getAllStateStores().get("aggregated");
            final WrappedStateStore changeLogging = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
            assertThat(store, instanceOf(MeteredSessionStore.class));
            if (withHeaders) {
                assertThat(changeLogging, instanceOf(ChangeLoggingSessionBytesStoreWithHeaders.class));
            } else {
                assertThat(changeLogging, instanceOf(ChangeLoggingSessionBytesStore.class));
            }
        }
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

    private <V> List<KeyValue<Windowed<String>, V>> unwrapAggregations(
            final KeyValueIterator<Windowed<String>, V> iterator) {
        final List<KeyValue<Windowed<String>, V>> result = new ArrayList<>();
        while (iterator.hasNext()) {
            final KeyValue<Windowed<String>, V> next = iterator.next();
            result.add(KeyValue.pair(next.key, next.value));
        }
        iterator.close();
        return result;
    }
}
