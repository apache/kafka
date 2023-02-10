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
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.state.internals.ChangeLoggingSessionBytesStore;
import org.apache.kafka.streams.state.internals.MeteredSessionStore;
import org.apache.kafka.streams.state.internals.RocksDBTimeOrderedSessionStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RunWith(Parameterized.class)
public class SessionWindowedKStreamImplTest {
    private static final String TOPIC = "input";
    private final StreamsBuilder builder = new StreamsBuilder();
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
    private final Merger<String, String> sessionMerger = (aggKey, aggOne, aggTwo) -> aggOne + "+" + aggTwo;

    private SessionWindowedKStream<String, String> stream;

    @Parameterized.Parameter
    public EmitStrategy.StrategyType type;

    private boolean emitFinal;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return asList(new Object[][] {
            {EmitStrategy.StrategyType.ON_WINDOW_UPDATE},
            {EmitStrategy.StrategyType.ON_WINDOW_CLOSE}
        });
    }

    @Before
    public void before() {
        final EmitStrategy emitStrategy = EmitStrategy.StrategyType.forType(type);
        emitFinal = type.equals(EmitStrategy.StrategyType.ON_WINDOW_CLOSE);

        // Set interval to 0 so that it always tries to emit
        props.setProperty(StreamsConfig.InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_WINDOWED_AGGREGATION, "0");

        final KStream<String, String> stream = builder.stream(TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        this.stream = stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(ofMillis(500)))
            .emitStrategy(emitStrategy);
    }

    @Test
    public void shouldCountSessionWindowedWithCachingDisabled() {
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        shouldCountSessionWindowed();
    }

    @Test
    public void shouldCountSessionWindowedWithCachingEnabled() {
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

    @Test
    public void shouldReduceWindowed() {
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

    @Test
    public void shouldAggregateSessionWindowed() {
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

    @Test
    public void shouldMaterializeCount() {
        stream.count(Materialized.as("count-store"));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
            final SessionStore<String, Long> store = driver.getSessionStore("count-store");
            final List<KeyValue<Windowed<String>, Long>> data = StreamsTestUtils.toList(store.fetch("1", "2"));
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

    @Test
    public void shouldMaterializeReduced() {
        stream.reduce(MockReducer.STRING_ADDER, Materialized.as("reduced"));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
            final SessionStore<String, String> sessionStore = driver.getSessionStore("reduced");
            final List<KeyValue<Windowed<String>, String>> data = StreamsTestUtils.toList(sessionStore.fetch("1", "2"));

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
        assertThrows(NullPointerException.class, () -> stream.aggregate(null, MockAggregator.TOSTRING_ADDER, sessionMerger));
    }

    @Test
    public void shouldThrowNullPointerOnAggregateIfAggregatorIsNull() {
        assertThrows(NullPointerException.class, () -> stream.aggregate(MockInitializer.STRING_INIT, null, sessionMerger));
    }

    @Test
    public void shouldThrowNullPointerOnAggregateIfMergerIsNull() {
        assertThrows(NullPointerException.class, () -> stream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, null));
    }

    @Test
    public void shouldThrowNullPointerOnReduceIfReducerIsNull() {
        assertThrows(NullPointerException.class, () -> stream.reduce(null));
    }

    @Test
    public void shouldThrowNullPointerOnMaterializedAggregateIfInitializerIsNull() {
        assertThrows(NullPointerException.class, () -> stream.aggregate(
            null,
            MockAggregator.TOSTRING_ADDER,
            sessionMerger,
            Materialized.as("store")));
    }

    @Test
    public void shouldThrowNullPointerOnMaterializedAggregateIfAggregatorIsNull() {
        assertThrows(NullPointerException.class, () -> stream.aggregate(
            MockInitializer.STRING_INIT,
            null,
            sessionMerger,
            Materialized.as("store")));
    }

    @Test
    public void shouldThrowNullPointerOnMaterializedAggregateIfMergerIsNull() {
        assertThrows(NullPointerException.class, () -> stream.aggregate(
            MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            null,
            Materialized.as("store")));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldThrowNullPointerOnMaterializedAggregateIfMaterializedIsNull() {
        assertThrows(NullPointerException.class, () -> stream.aggregate(
            MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            sessionMerger,
            (Materialized) null));
    }

    @Test
    public void shouldThrowNullPointerOnMaterializedReduceIfReducerIsNull() {
        assertThrows(NullPointerException.class, () -> stream.reduce(null, Materialized.as("store")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldThrowNullPointerOnMaterializedReduceIfMaterializedIsNull() {
        assertThrows(NullPointerException.class, () -> stream.reduce(MockReducer.STRING_ADDER, (Materialized) null));
    }

    @Test
    public void shouldThrowNullPointerOnMaterializedReduceIfNamedIsNull() {
        assertThrows(NullPointerException.class, () -> stream.reduce(MockReducer.STRING_ADDER, (Named) null));
    }

    @Test
    public void shouldThrowNullPointerOnCountIfMaterializedIsNull() {
        assertThrows(NullPointerException.class, () -> stream.count((Materialized<String, Long, SessionStore<Bytes, byte[]>>) null));
    }

    @Test
    public void shouldNotEnableCachingWithEmitFinal() {
        if (!emitFinal)
            return;

        stream.aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                sessionMerger,
                Materialized.<String, String, SessionStore<Bytes, byte[]>>as("aggregated").withValueSerde(Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final SessionStore<String, String> store = driver.getSessionStore("aggregated");
            final WrappedStateStore changeLogging = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
            assertThat(store, instanceOf(MeteredSessionStore.class));
            assertThat(changeLogging, instanceOf(ChangeLoggingSessionBytesStore.class));
            assertThat(changeLogging.wrapped(), instanceOf(RocksDBTimeOrderedSessionStore.class));
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
}
