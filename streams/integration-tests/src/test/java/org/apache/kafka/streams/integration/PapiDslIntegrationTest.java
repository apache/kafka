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
package org.apache.kafka.streams.integration;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyTestDriverBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowStore;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration")
public class PapiDslIntegrationTest {
    final StreamsBuilder builder = new StreamsBuilder();

    private void verify(final KTable<String, String> table) {
        table.toStream()
            .process(() -> new ContextualProcessor<String, String, String, String>() {
                @Override
                public void process(final Record<String, String> record) {
                    final TimestampedKeyValueStore<String, String> store = context().getStateStore("table-store");

                    try (final KeyValueIterator<String, ValueAndTimestamp<String>> it = store.all()) {
                        while (it.hasNext()) {
                            final KeyValue<String, ValueAndTimestamp<String>> row = it.next();
                            context().forward(new Record<>(row.key, row.value.value(), row.value.timestamp()));
                        }
                    }
                }
            }, "table-store")
            .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        try (final TopologyTestDriver testDriver = new TopologyTestDriverBuilder(builder.build()).build()) {
            final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("output-topic", new StringDeserializer(), new StringDeserializer());

            inputTopic.pipeInput("key1", "value1");

            assertEquals(KeyValue.pair("key1", "value1"), outputTopic.readKeyValue());
        }
    }

    @Test
    public void processorShouldAccessSourceKTableStoreAsTimestampedStore() {
        verify(builder.table("input-topic", Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("table-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.String())));
    }

    @Test
    public void processorShouldAccessFilteredKTableStoreAsTimestampedStore() {
        verify(builder
            .table("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .filter((k, v) -> true, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("table-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.String()))
        );
    }

    @Test
    public void processorShouldAccessMappedKTableStoreAsTimestampedStore() {
        verify(builder
            .table("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .mapValues(v -> v, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("table-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.String()))
        );
    }

    @Test
    public void processorShouldAccessTransformedKTableStoreAsTimestampedStore() {
        verify(builder
            .table("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .transformValues(() -> new ValueTransformerWithKey<>() {
                @Override
                public void init(final ProcessorContext context) { }

                @Override
                public String transform(final String readOnlyKey, final String value) {
                    return value;
                }

                @Override
                public void close() { }
            }, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("table-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.String()))
        );
    }

    @Test
    public void processorShouldAccessReducedKTableStoreAsTimestampedStore() {
        verify(builder
            .table("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .groupBy((KeyValueMapper<String, String, KeyValue<String, String>>) KeyValue::pair, Grouped.with(Serdes.String(), Serdes.String()))
            .reduce(
                (value, aggregate) -> value,
                (value, aggregate) -> aggregate,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("table-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            )
        );
    }

    @Test
    public void processorShouldAccessAggregatedKTableStoreAsTimestampedStore() {
        verify(builder
            .table("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .groupBy((KeyValueMapper<String, String, KeyValue<String, String>>) KeyValue::pair, Grouped.with(Serdes.String(), Serdes.String()))
            .aggregate(
                () -> "",
                (key, value, aggregate) -> value,
                (key, value, aggregate) -> aggregate,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("table-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            )
        );
    }

    private void verifyJoin(final KTable<String, String> table) {
        table.toStream()
            .process(() -> new ContextualProcessor<String, String, String, String>() {
                @Override
                public void process(final Record<String, String> record) {
                    final TimestampedKeyValueStore<String, String> store = context().getStateStore("table-store");

                    try (final KeyValueIterator<String, ValueAndTimestamp<String>> it = store.all()) {
                        while (it.hasNext()) {
                            final KeyValue<String, ValueAndTimestamp<String>> row = it.next();
                            context().forward(new Record<>(row.key, row.value.value(), row.value.timestamp()));
                        }
                    }
                }
            }, "table-store")
            .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        try (final TopologyTestDriver testDriver = new TopologyTestDriverBuilder(builder.build()).build()) {
            final TestInputTopic<String, String> leftInputTopic = testDriver.createInputTopic("left-input-topic", new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> rightInputTopic = testDriver.createInputTopic("right-input-topic", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("output-topic", new StringDeserializer(), new StringDeserializer());

            leftInputTopic.pipeInput("key1", "left");
            rightInputTopic.pipeInput("key1", "right");

            assertEquals(KeyValue.pair("key1", "left-right"), outputTopic.readKeyValue());
        }
    }

    @Test
    public void processorShouldAccessJoinedKTableStoreAsTimestampedStore() {
        verifyJoin(builder
            .table("left-input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .join(
                builder.table("right-input-topic", Consumed.with(Serdes.String(), Serdes.String())),
                (left, right) -> left + "-" + right,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("table-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            )
        );
    }

    @Test
    public void processorShouldAccessFKJoinedKTableStoreAsTimestampedStore() {
        verifyJoin(builder
            .table("left-input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .join(
                builder.table("right-input-topic", Consumed.with(Serdes.String(), Serdes.String())),
                (key, value) -> key,
                (left, right) -> left + "-" + right,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("table-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            )
        );
    }

    @Test
    public void processorShouldAccessKStreamReducedKTableStoreAsTimestampedStore() {
        verify(builder
            .stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .reduce(
                (value, aggregate) -> value,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("table-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            )
        );
    }

    @Test
    public void processorShouldAccessKStreamAggregatedKTableStoreAsTimestampedStore() {
        verify(builder
            .stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .aggregate(
                () -> "",
                (key, value, aggregate) -> value,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("table-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            )
        );
    }

    private void verifyWindow(final KTable<Windowed<String>, String> table) {
        verifyWindow(table, false);
    }

    private void verifyWindow(final KTable<Windowed<String>, String> table, final boolean requiresFlush) {
        table.toStream((windowedKey, value) -> windowedKey.key())
            .process(() -> new ContextualProcessor<String, String, String, String>() {
                @Override
                public void process(final Record<String, String> record) {
                    final TimestampedWindowStore<String, String> store = context().getStateStore("table-store");

                    try (final KeyValueIterator<Windowed<String>, ValueAndTimestamp<String>> it = store.all()) {
                        while (it.hasNext()) {
                            final KeyValue<Windowed<String>, ValueAndTimestamp<String>> row = it.next();
                            context().forward(new Record<>(row.key.key(), row.value.value(), row.value.timestamp()));
                        }
                    }
                }
            }, "table-store")
            .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        try (final TopologyTestDriver testDriver = new TopologyTestDriverBuilder(builder.build()).build()) {
            final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("output-topic", new StringDeserializer(), new StringDeserializer());

            inputTopic.pipeInput("key1", "value1");

            if (requiresFlush) {
                inputTopic.advanceTime(Duration.ofHours(2));
                inputTopic.pipeInput("flush", "flush");
            }

            assertEquals(KeyValue.pair("key1", "value1"), outputTopic.readKeyValue());
        }
    }

    @Test
    public void processorShouldAccessKStreamWindowReducedKTableStoreAsTimestampedStore() {
        verifyWindow(builder
            .stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1L)))
            .reduce(
                (value, aggregate) -> value,
                Materialized.<String, String, WindowStore<Bytes, byte[]>>as("table-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            )
        );
    }

    @Test
    public void processorShouldAccessKStreamWindowReducedOnWindowCloseKTableStoreAsTimestampedStore() {
        verifyWindow(builder
            .stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1L)))
            .emitStrategy(EmitStrategy.onWindowClose())
            .reduce(
                (value, aggregate) -> value,
                Materialized.<String, String, WindowStore<Bytes, byte[]>>as("table-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            ),
            true
        );
    }

    @Test
    public void processorShouldAccessKStreamWindowAggregatedKTableStoreAsTimestampedStore() {
        verifyWindow(builder
            .stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1L)))
            .aggregate(
                () -> "",
                (key, value, aggregate) -> value,
                Materialized.<String, String, WindowStore<Bytes, byte[]>>as("table-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            )
        );
    }

    @Test
    public void processorShouldAccessKStreamWindowAggregatedOnWindowCloseKTableStoreAsTimestampedStore() {
        verifyWindow(builder
            .stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1L)))
            .emitStrategy(EmitStrategy.onWindowClose())
            .aggregate(
                () -> "",
                (key, value, aggregate) -> value,
                Materialized.<String, String, WindowStore<Bytes, byte[]>>as("table-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            ),
            true
        );
    }

    private void verifySession(final KTable<Windowed<String>, String> table) {
        verifySession(table, false);
    }

    private void verifySession(final KTable<Windowed<String>, String> table, final boolean requiresFlush) {
        table.toStream((windowedKey, value) -> windowedKey.key())
            .process(() -> new ContextualProcessor<String, String, String, String>() {
                @Override
                public void process(final Record<String, String> record) {
                    final SessionStore<String, String> store = context().getStateStore("table-store");

                    try (final KeyValueIterator<Windowed<String>, String> it = store.findSessions("key1", 0L, Long.MAX_VALUE)) {
                        while (it.hasNext()) {
                            final KeyValue<Windowed<String>, String> row = it.next();
                            context().forward(new Record<>(row.key.key(), row.value, record.timestamp()));
                        }
                    }
                }
            }, "table-store")
            .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        try (final TopologyTestDriver testDriver = new TopologyTestDriverBuilder(builder.build()).build()) {
            final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("output-topic", new StringDeserializer(), new StringDeserializer());

            inputTopic.pipeInput("key1", "value1");

            if (requiresFlush) {
                inputTopic.advanceTime(Duration.ofHours(2));
                inputTopic.pipeInput("flush", "flush");
            }

            assertEquals(KeyValue.pair("key1", "value1"), outputTopic.readKeyValue());
        }
    }

    @Test
    public void processorShouldAccessKStreamSessionReducedKTableStoreAsTimestampedStore() {
        verifySession(builder
            .stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofHours(1L)))
            .reduce(
                (value, aggregate) -> value,
                Materialized.<String, String, SessionStore<Bytes, byte[]>>as("table-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            )
        );
    }

    @Test
    public void processorShouldAccessKStreamSessionReducedOnWindowCloseKTableStoreAsTimestampedStore() {
        verifySession(builder
            .stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofHours(1L)))
            .emitStrategy(EmitStrategy.onWindowClose())
            .reduce(
                (value, aggregate) -> value,
                Materialized.<String, String, SessionStore<Bytes, byte[]>>as("table-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .withRetention(Duration.ofHours(10L))
            ),
            true
        );
    }

    @Test
    public void processorShouldAccessKStreamSessionAggregateKTableStoreAsTimestampedStore() {
        verifySession(builder
            .stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofHours(1L)))
            .aggregate(
                () -> "",
                (key, value, aggregate) -> value,
                (key, left, right) -> "",
                Materialized.<String, String, SessionStore<Bytes, byte[]>>as("table-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            )
        );
    }

    @Test
    public void processorShouldAccessKStreamSessionAggregateOnWindowCloseKTableStoreAsTimestampedStore() {
        verifySession(builder
            .stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofHours(1L)))
            .emitStrategy(EmitStrategy.onWindowClose())
            .aggregate(
                () -> "",
                (key, value, aggregate) -> value,
                (key, left, right) -> "",
                Materialized.<String, String, SessionStore<Bytes, byte[]>>as("table-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .withRetention(Duration.ofHours(10L))
            ),
            true
        );
    }

    @Test
    public void processorShouldAccessKStreamSlidingReducedKTableStoreAsTimestampedStore() {
        verifyWindow(builder
            .stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofHours(1L)))
            .reduce(
                (value, aggregate) -> value,
                Materialized.<String, String, WindowStore<Bytes, byte[]>>as("table-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            )
        );
    }

    @Test
    public void processorShouldAccessKStreamSlidingReducedOnWindowCloseKTableStoreAsTimestampedStore() {
        verifyWindow(builder
                .stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey()
                .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofHours(1L)))
                .emitStrategy(EmitStrategy.onWindowClose())
                .reduce(
                    (value, aggregate) -> value,
                    Materialized.<String, String, WindowStore<Bytes, byte[]>>as("table-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
                ),
            true
        );
    }

    @Test
    public void processorShouldAccessKStreamSlidingAggregatedKTableStoreAsTimestampedStore() {
        verifyWindow(builder
            .stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofHours(1L)))
            .aggregate(
                () -> "",
                (key, value, aggregate) -> value,
                Materialized.<String, String, WindowStore<Bytes, byte[]>>as("table-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            )
        );
    }

    @Test
    public void processorShouldAccessKStreamSlidingAggregatedOnWindowCloseKTableStoreAsTimestampedStore() {
        verifyWindow(builder
                .stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey()
                .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofHours(1L)))
                .emitStrategy(EmitStrategy.onWindowClose())
                .aggregate(
                    () -> "",
                    (key, value, aggregate) -> value,
                    Materialized.<String, String, WindowStore<Bytes, byte[]>>as("table-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
                ),
            true
        );
    }

    @Test
    public void processorShouldAccessKTableStoreAsHeadersStoreViaConfig() {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.table("input-topic", Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("table-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.String()))
            .toStream()
            .process(() -> new ContextualProcessor<String, String, String, String>() {
                @Override
                public void process(final Record<String, String> record) {
                    final TimestampedKeyValueStoreWithHeaders<String, String> store = context().getStateStore("table-store");

                    try (final KeyValueIterator<String, ValueTimestampHeaders<String>> it = store.all()) {
                        while (it.hasNext()) {
                            final KeyValue<String, ValueTimestampHeaders<String>> row = it.next();
                            context().forward(new Record<>(row.key, row.value.value(), row.value.timestamp()));
                        }
                    }
                }
            }, "table-store")
            .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        final Properties props = new Properties();
        props.put(StreamsConfig.DSL_STORE_FORMAT_CONFIG, StreamsConfig.DSL_STORE_FORMAT_HEADERS);

        try (final TopologyTestDriver testDriver = new TopologyTestDriverBuilder(builder.build()).withConfig(props).build()) {
            final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("output-topic", new StringDeserializer(), new StringDeserializer());

            inputTopic.pipeInput("key1", "value1");

            assertEquals(KeyValue.pair("key1", "value1"), outputTopic.readKeyValue());
        }
    }

    @Test
    public void processorShouldAccessKTableStoreAsHeadersStoreViaSupplier() {
        final StreamsBuilder builder = new StreamsBuilder();

        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> materialized = Materialized.as(Stores.persistentTimestampedKeyValueStoreWithHeaders("table-store"));
        builder.table("input-topic", materialized.withKeySerde(Serdes.String()).withValueSerde(Serdes.String()))
            .toStream()
            .process(() -> new ContextualProcessor<String, String, String, String>() {
                @Override
                public void process(final Record<String, String> record) {
                    final TimestampedKeyValueStoreWithHeaders<String, String> store = context().getStateStore("table-store");

                    try (final KeyValueIterator<String, ValueTimestampHeaders<String>> it = store.all()) {
                        while (it.hasNext()) {
                            final KeyValue<String, ValueTimestampHeaders<String>> row = it.next();
                            context().forward(new Record<>(row.key, row.value.value(), row.value.timestamp()));
                        }
                    }
                }
            }, "table-store")
            .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        try (final TopologyTestDriver testDriver = new TopologyTestDriverBuilder(builder.build()).build()) {
            final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("output-topic", new StringDeserializer(), new StringDeserializer());

            inputTopic.pipeInput("key1", "value1");

            assertEquals(KeyValue.pair("key1", "value1"), outputTopic.readKeyValue());
        }
    }

    @Test
    public void processorShouldAccessKStreamAggregatedKTableStoreAsHeadersStoreViaSupplier() {
        final StreamsBuilder builder = new StreamsBuilder();

        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> materialized =
            Materialized.as(Stores.persistentTimestampedKeyValueStoreWithHeaders("table-store"));

        builder
            .stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .aggregate(
                () -> "",
                (key, value, aggregate) -> value,
                materialized.withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            )
            .toStream()
            .process(() -> new ContextualProcessor<String, String, String, String>() {
                @Override
                public void process(final Record<String, String> record) {
                    final TimestampedKeyValueStoreWithHeaders<String, String> store = context().getStateStore("table-store");

                    try (final KeyValueIterator<String, ValueTimestampHeaders<String>> it = store.all()) {
                        while (it.hasNext()) {
                            final KeyValue<String, ValueTimestampHeaders<String>> row = it.next();
                            context().forward(new Record<>(row.key, row.value.value(), row.value.timestamp()));
                        }
                    }
                }
            }, "table-store")
            .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        try (final TopologyTestDriver testDriver = new TopologyTestDriverBuilder(builder.build()).build()) {
            final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("output-topic", new StringDeserializer(), new StringDeserializer());

            inputTopic.pipeInput("key1", "value1");

            assertEquals(KeyValue.pair("key1", "value1"), outputTopic.readKeyValue());
        }
    }

    @Test
    public void processorShouldAccessKStreamReducedKTableStoreAsHeadersStoreViaSupplier() {
        final StreamsBuilder builder = new StreamsBuilder();

        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> materialized =
            Materialized.as(Stores.persistentTimestampedKeyValueStoreWithHeaders("table-store"));

        builder
            .stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .reduce(
                (value, aggregate) -> value,
                materialized.withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            )
            .toStream()
            .process(() -> new ContextualProcessor<String, String, String, String>() {
                @Override
                public void process(final Record<String, String> record) {
                    final TimestampedKeyValueStoreWithHeaders<String, String> store = context().getStateStore("table-store");

                    try (final KeyValueIterator<String, ValueTimestampHeaders<String>> it = store.all()) {
                        while (it.hasNext()) {
                            final KeyValue<String, ValueTimestampHeaders<String>> row = it.next();
                            context().forward(new Record<>(row.key, row.value.value(), row.value.timestamp()));
                        }
                    }
                }
            }, "table-store")
            .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        try (final TopologyTestDriver testDriver = new TopologyTestDriverBuilder(builder.build()).build()) {
            final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("output-topic", new StringDeserializer(), new StringDeserializer());

            inputTopic.pipeInput("key1", "value1");

            assertEquals(KeyValue.pair("key1", "value1"), outputTopic.readKeyValue());
        }
    }

    @Test
    public void processorShouldAccessKStreamCountKTableStoreAsHeadersStoreViaSupplier() {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .count(Materialized.as(Stores.persistentTimestampedKeyValueStoreWithHeaders("table-store")))
            .toStream()
            .process(() -> new ContextualProcessor<String, Long, String, Long>() {
                @Override
                public void process(final Record<String, Long> record) {
                    final TimestampedKeyValueStoreWithHeaders<String, Long> store = context().getStateStore("table-store");

                    try (final KeyValueIterator<String, ValueTimestampHeaders<Long>> it = store.all()) {
                        while (it.hasNext()) {
                            final KeyValue<String, ValueTimestampHeaders<Long>> row = it.next();
                            context().forward(new Record<>(row.key, row.value.value(), row.value.timestamp()));
                        }
                    }
                }
            }, "table-store")
            .to("output-topic", Produced.with(Serdes.String(), Serdes.Long()));

        try (final TopologyTestDriver testDriver = new TopologyTestDriverBuilder(builder.build()).build()) {
            final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, Long> outputTopic = testDriver.createOutputTopic("output-topic", new StringDeserializer(), new LongDeserializer());

            inputTopic.pipeInput("key1", "value1");

            assertEquals(KeyValue.pair("key1", 1L), outputTopic.readKeyValue());
        }
    }

    @Test
    public void processorShouldBuildTopologyWithWindowStoreWithHeadersViaSupplier() {
        final StreamsBuilder builder = new StreamsBuilder();

        final Materialized<String, String, WindowStore<Bytes, byte[]>> materialized =
            Materialized.as(Stores.persistentTimestampedWindowStoreWithHeaders("table-store", Duration.ofHours(24L), Duration.ofHours(1L), false));

        builder
            .stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1L)))
            .aggregate(
                () -> "",
                (key, value, aggregate) -> value,
                materialized.withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            )
            .toStream()
            .process(() -> new ContextualProcessor<Windowed<String>, String, Windowed<String>, String>() {
                @Override
                public void process(final Record<Windowed<String>, String> record) {
                    final WindowStore<String, ValueTimestampHeaders<String>> store = context().getStateStore("table-store");

                    try (final KeyValueIterator<Windowed<String>, ValueTimestampHeaders<String>> it = store.all()) {
                        while (it.hasNext()) {
                            final KeyValue<Windowed<String>, ValueTimestampHeaders<String>> row = it.next();
                            context().forward(new Record<>(row.key, row.value.value(), row.value.timestamp()));
                        }
                    }
                }
            }, "table-store")
            .to("output-topic", Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class, Duration.ofHours(1L).toMillis()), Serdes.String()));

        // Verify topology can be built and run with window headers store supplier
        try (final TopologyTestDriver testDriver = new TopologyTestDriverBuilder(builder.build()).build()) {
            final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<Windowed<String>, String> outputTopic = testDriver.createOutputTopic("output-topic", new TimeWindowedDeserializer<>(new StringDeserializer(), Duration.ofHours(1L).toMillis()), new StringDeserializer());

            inputTopic.pipeInput("key1", "value1");

            assertEquals("value1", outputTopic.readKeyValue().value);
        }
    }

    @Test
    public void processorShouldAccessKStreamSessionAggregatedKTableStoreAsHeadersStoreViaSupplier() {
        final StreamsBuilder builder = new StreamsBuilder();

        final Materialized<String, String, SessionStore<Bytes, byte[]>> materialized =
            Materialized.as(Stores.persistentSessionStoreWithHeaders("table-store", Duration.ofHours(1L)));

        builder
            .stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofHours(1L)))
            .aggregate(
                () -> "",
                (key, value, aggregate) -> value,
                (key, left, right) -> left,
                materialized.withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            )
            .toStream((windowedKey, value) -> windowedKey.key())
            .process(() -> new ContextualProcessor<String, String, String, String>() {
                @Override
                public void process(final Record<String, String> record) {
                    final SessionStoreWithHeaders<String, String> store = context().getStateStore("table-store");

                    try (final KeyValueIterator<Windowed<String>, AggregationWithHeaders<String>> it = store.findSessions("key1", 0L, Long.MAX_VALUE)) {
                        while (it.hasNext()) {
                            final KeyValue<Windowed<String>, AggregationWithHeaders<String>> row = it.next();
                            context().forward(new Record<>(row.key.key(), row.value.aggregation(), record.timestamp()));
                        }
                    }
                }
            }, "table-store")
            .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        try (final TopologyTestDriver testDriver = new TopologyTestDriverBuilder(builder.build()).build()) {
            final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("output-topic", new StringDeserializer(), new StringDeserializer());

            inputTopic.pipeInput("key1", "value1");

            assertEquals(KeyValue.pair("key1", "value1"), outputTopic.readKeyValue());
        }
    }

    @Test
    public void processorShouldAccessKStreamSessionReducedKTableStoreAsHeadersStoreViaSupplier() {
        final StreamsBuilder builder = new StreamsBuilder();

        final Materialized<String, String, SessionStore<Bytes, byte[]>> materialized =
            Materialized.as(Stores.persistentSessionStoreWithHeaders("table-store", Duration.ofHours(1L)));

        builder
            .stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofHours(1L)))
            .reduce(
                (value, aggregate) -> value,
                materialized.withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            )
            .toStream((windowedKey, value) -> windowedKey.key())
            .process(() -> new ContextualProcessor<String, String, String, String>() {
                @Override
                public void process(final Record<String, String> record) {
                    final SessionStoreWithHeaders<String, String> store = context().getStateStore("table-store");

                    try (final KeyValueIterator<Windowed<String>, AggregationWithHeaders<String>> it = store.findSessions("key1", 0L, Long.MAX_VALUE)) {
                        while (it.hasNext()) {
                            final KeyValue<Windowed<String>, AggregationWithHeaders<String>> row = it.next();
                            context().forward(new Record<>(row.key.key(), row.value.aggregation(), record.timestamp()));
                        }
                    }
                }
            }, "table-store")
            .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        try (final TopologyTestDriver testDriver = new TopologyTestDriverBuilder(builder.build()).build()) {
            final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("output-topic", new StringDeserializer(), new StringDeserializer());

            inputTopic.pipeInput("key1", "value1");

            assertEquals(KeyValue.pair("key1", "value1"), outputTopic.readKeyValue());
        }
    }
}
