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
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.KeyValueTimestampHeaders;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyTestDriverWrapper;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.MockApiProcessor;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

@SuppressWarnings("unchecked")
public class KTableMapValuesTest {
    private final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());

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
        properties.setProperty(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0");
        properties.put(StreamsConfig.DSL_STORE_FORMAT_CONFIG, storeFormat);
        return properties;
    }

    private static Headers makeHeaders(final String key, final String value) {
        final RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(key, value.getBytes()));
        return headers;
    }

    private void doTestKTable(final StreamsBuilder builder,
                              final String topic1,
                              final String storeFormat,
                              final Properties props,
                              final MockApiProcessorSupplier<String, Integer, Void, Void> supplier) {
        final Headers headersA = makeHeaders("key", "A");
        final Headers headersB = makeHeaders("key", "B");
        final Headers headersC = makeHeaders("key", "C");
        final Headers headersD = makeHeaders("key", "D");

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                    driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            inputTopic1.pipeInput(new TestRecord<>("A", "1", headersA, 5L));
            inputTopic1.pipeInput(new TestRecord<>("B", "2", headersB, 25L));
            inputTopic1.pipeInput(new TestRecord<>("C", "3", headersC, 20L));
            inputTopic1.pipeInput(new TestRecord<>("D", "4", headersD, 10L));

            final List<MockApiProcessor<String, Integer, Void, Void>> processors = supplier.capturedProcessors(1);

            if (storeFormat.equals("default")) {
                processors.get(0).checkAndClearProcessResult(
                        new KeyValueTimestamp<>("A", 1, 5),
                        new KeyValueTimestamp<>("B", 2, 25),
                        new KeyValueTimestamp<>("C", 3, 20),
                        new KeyValueTimestamp<>("D", 4, 10));
            } else if (storeFormat.equals("headers")) {
                processors.get(0).checkAndClearProcessResultWithHeaders(
                        new KeyValueTimestampHeaders<>("A", 1, 5, headersA),
                        new KeyValueTimestampHeaders<>("B", 2, 25, headersB),
                        new KeyValueTimestampHeaders<>("C", 3, 20, headersC),
                        new KeyValueTimestampHeaders<>("D", 4, 10, headersD));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void testKTable(final String storeFormat) {
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, String> table1 = builder.table(topic1, consumed);
        final KTable<String, Integer> table2 = table1.mapValues(value -> value.charAt(0) - 48);

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        doTestKTable(builder, topic1, storeFormat, props, supplier);
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void testQueryableKTable(final String storeFormat) {
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, String> table1 = builder.table(topic1, consumed);
        final KTable<String, Integer> table2 = table1
            .mapValues(
                value -> value.charAt(0) - 48,
                Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("anyName")
                    .withValueSerde(Serdes.Integer()));

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        doTestKTable(builder, topic1, storeFormat, props, supplier);
    }

    private void doTestValueGetter(final StreamsBuilder builder,
                                   final String topic1,
                                   final String storeFormat,
                                   final Properties props,
                                   final KTableImpl<String, String, Integer> table2,
                                   final KTableImpl<String, String, Integer> table3) {

        final Topology topology = builder.build();

        final KTableValueGetterSupplier<String, Integer> getterSupplier2 = table2.valueGetterSupplier();
        final KTableValueGetterSupplier<String, Integer> getterSupplier3 = table3.valueGetterSupplier();

        final InternalTopologyBuilder topologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);
        topologyBuilder.connectProcessorAndStateStores(table2.name, getterSupplier2.storeNames());
        topologyBuilder.connectProcessorAndStateStores(table3.name, getterSupplier3.storeNames());

        try (final TopologyTestDriverWrapper driver = new TopologyTestDriverWrapper(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                    driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final KTableValueGetter<String, Integer> getter2 = getterSupplier2.get();
            final KTableValueGetter<String, Integer> getter3 = getterSupplier3.get();

            getter2.init(driver.setCurrentNodeForProcessorContext(table2.name));
            getter3.init(driver.setCurrentNodeForProcessorContext(table3.name));

            inputTopic1.pipeInput("A", "01", 50L);
            inputTopic1.pipeInput("B", "01", 10L);
            inputTopic1.pipeInput("C", "01", 30L);

            assertEquals(ValueAndTimestamp.make(1, 50L), getter2.get("A"));
            assertEquals(ValueAndTimestamp.make(1, 10L), getter2.get("B"));
            assertEquals(ValueAndTimestamp.make(1, 30L), getter2.get("C"));

            assertEquals(ValueAndTimestamp.make(-1, 50L), getter3.get("A"));
            assertEquals(ValueAndTimestamp.make(-1, 10L), getter3.get("B"));
            assertEquals(ValueAndTimestamp.make(-1, 30L), getter3.get("C"));

            inputTopic1.pipeInput("A", "02", 25L);
            inputTopic1.pipeInput("B", "02", 20L);

            assertEquals(ValueAndTimestamp.make(2, 25L), getter2.get("A"));
            assertEquals(ValueAndTimestamp.make(2, 20L), getter2.get("B"));
            assertEquals(ValueAndTimestamp.make(1, 30L), getter2.get("C"));

            assertEquals(ValueAndTimestamp.make(-2, 25L), getter3.get("A"));
            assertEquals(ValueAndTimestamp.make(-2, 20L), getter3.get("B"));
            assertEquals(ValueAndTimestamp.make(-1, 30L), getter3.get("C"));

            inputTopic1.pipeInput("A", "03", 35L);

            assertEquals(ValueAndTimestamp.make(3, 35L), getter2.get("A"));
            assertEquals(ValueAndTimestamp.make(2, 20L), getter2.get("B"));
            assertEquals(ValueAndTimestamp.make(1, 30L), getter2.get("C"));

            assertEquals(ValueAndTimestamp.make(-3, 35L), getter3.get("A"));
            assertEquals(ValueAndTimestamp.make(-2, 20L), getter3.get("B"));
            assertEquals(ValueAndTimestamp.make(-1, 30L), getter3.get("C"));

            inputTopic1.pipeInput("A", (String) null, 1L);

            assertNull(getter2.get("A"));
            assertEquals(ValueAndTimestamp.make(2, 20L), getter2.get("B"));
            assertEquals(ValueAndTimestamp.make(1, 30L), getter2.get("C"));

            assertNull(getter3.get("A"));
            assertEquals(ValueAndTimestamp.make(-2, 20L), getter3.get("B"));
            assertEquals(ValueAndTimestamp.make(-1, 30L), getter3.get("C"));
        }
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void testQueryableValueGetter(final String storeFormat) {
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";
        final String storeName2 = "store2";
        final String storeName3 = "store3";

        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        final KTableImpl<String, String, Integer> table2 =
            (KTableImpl<String, String, Integer>) table1.mapValues(
                s -> Integer.valueOf(s),
                Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as(storeName2)
                    .withValueSerde(Serdes.Integer()));
        final KTableImpl<String, String, Integer> table3 =
            (KTableImpl<String, String, Integer>) table1.mapValues(
                value -> Integer.valueOf(value) * (-1),
                Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as(storeName3)
                    .withValueSerde(Serdes.Integer()));
        final KTableImpl<String, String, Integer> table4 =
            (KTableImpl<String, String, Integer>) table1.mapValues(s -> Integer.valueOf(s));

        assertEquals(storeName2, table2.queryableStoreName());
        assertEquals(storeName3, table3.queryableStoreName());
        assertNull(table4.queryableStoreName());

        doTestValueGetter(builder, topic1, storeFormat, props, table2, table3);
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void testNotSendingOldValue(final String storeFormat) {
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        final KTableImpl<String, String, Integer> table2 =
            (KTableImpl<String, String, Integer>) table1.mapValues(s -> Integer.valueOf(s));

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final Topology topology = builder.build().addProcessor("proc", supplier, table2.name);

        final Headers headersA = makeHeaders("key", "A");
        final Headers headersB = makeHeaders("key", "B");
        final Headers headersC = makeHeaders("key", "C");

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            final TestInputTopic<String, String> inputTopic1 =
                    driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<String, Integer, Void, Void> proc = supplier.theCapturedProcessor();

            assertFalse(table1.sendingOldValueEnabled());
            assertFalse(table2.sendingOldValueEnabled());

            inputTopic1.pipeInput(new TestRecord<>("A", "01", headersA, 5L));
            inputTopic1.pipeInput(new TestRecord<>("B", "01", headersB, 10L));
            inputTopic1.pipeInput(new TestRecord<>("C", "01", headersC, 15L));

            if (storeFormat.equals("default")) {
                proc.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(1, null), 5),
                        new KeyValueTimestamp<>("B", new Change<>(1, null), 10),
                        new KeyValueTimestamp<>("C", new Change<>(1, null), 15));
            } else if (storeFormat.equals("headers")) {
                proc.checkAndClearProcessResultWithHeaders(new KeyValueTimestampHeaders<>("A", new Change<>(1, null), 5, headersA),
                        new KeyValueTimestampHeaders<>("B", new Change<>(1, null), 10, headersB),
                        new KeyValueTimestampHeaders<>("C", new Change<>(1, null), 15, headersC));
            }

            inputTopic1.pipeInput(new TestRecord<>("A", "02", headersA, 10L));
            inputTopic1.pipeInput(new TestRecord<>("B", "02", headersB, 8L));

            if (storeFormat.equals("default")) {
                proc.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(2, null), 10),
                        new KeyValueTimestamp<>("B", new Change<>(2, null), 8));
            } else if (storeFormat.equals("headers")) {
                proc.checkAndClearProcessResultWithHeaders(new KeyValueTimestampHeaders<>("A", new Change<>(2, null), 10, headersA),
                        new KeyValueTimestampHeaders<>("B", new Change<>(2, null), 8, headersB));
            }

            inputTopic1.pipeInput(new TestRecord<>("A", "03", headersA, 20L));

            if (storeFormat.equals("default")) {
                proc.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(3, null), 20));
            } else if (storeFormat.equals("headers")) {
                proc.checkAndClearProcessResultWithHeaders(new KeyValueTimestampHeaders<>("A", new Change<>(3, null), 20, headersA));
            }

            inputTopic1.pipeInput(new TestRecord<>("A", null, headersA, 30L));

            if (storeFormat.equals("default")) {
                proc.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(null, null), 30));
            } else if (storeFormat.equals("headers")) {
                proc.checkAndClearProcessResultWithHeaders(new KeyValueTimestampHeaders<>("A", new Change<>(null, null), 30, headersA));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldEnableSendingOldValuesOnParentIfMapValuesNotMaterialized(final String storeFormat) {
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        final KTableImpl<String, String, Integer> table2 =
            (KTableImpl<String, String, Integer>) table1.mapValues(s -> Integer.valueOf(s));

        table2.enableSendingOldValues(true);

        assertThat(table1.sendingOldValueEnabled(), is(true));
        assertThat(table2.sendingOldValueEnabled(), is(true));

        testSendingOldValues(builder, topic1, storeFormat, props, table2);
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldNotEnableSendingOldValuesOnParentIfMapValuesMaterialized(final String storeFormat) {
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        final KTableImpl<String, String, Integer> table2 =
            (KTableImpl<String, String, Integer>) table1.mapValues(
                s -> Integer.valueOf(s),
                Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("bob").withValueSerde(Serdes.Integer())
            );

        table2.enableSendingOldValues(true);

        assertThat(table1.sendingOldValueEnabled(), is(false));
        assertThat(table2.sendingOldValueEnabled(), is(true));

        testSendingOldValues(builder, topic1, storeFormat, props, table2);
    }

    private void testSendingOldValues(
        final StreamsBuilder builder,
        final String topic1,
        final String storeFormat,
        final Properties props,
        final KTableImpl<String, String, Integer> table2
    ) {
        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        builder.build().addProcessor("proc", supplier, table2.name);

        final Headers headersA = makeHeaders("key", "A");
        final Headers headersB = makeHeaders("key", "B");
        final Headers headersC = makeHeaders("key", "C");

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<String, Integer, Void, Void> proc = supplier.theCapturedProcessor();

            inputTopic1.pipeInput(new TestRecord<>("A", "01", headersA, 5L));
            inputTopic1.pipeInput(new TestRecord<>("B", "01", headersB, 10L));
            inputTopic1.pipeInput(new TestRecord<>("C", "01", headersC, 15L));

            if (storeFormat.equals("default")) {
                proc.checkAndClearProcessResult(
                    new KeyValueTimestamp<>("A", new Change<>(1, null), 5),
                    new KeyValueTimestamp<>("B", new Change<>(1, null), 10),
                    new KeyValueTimestamp<>("C", new Change<>(1, null), 15)
                );
            } else if (storeFormat.equals("headers")) {
                proc.checkAndClearProcessResultWithHeaders(
                    new KeyValueTimestampHeaders<>("A", new Change<>(1, null), 5, headersA),
                    new KeyValueTimestampHeaders<>("B", new Change<>(1, null), 10, headersB),
                    new KeyValueTimestampHeaders<>("C", new Change<>(1, null), 15, headersC)
                );
            }

            inputTopic1.pipeInput(new TestRecord<>("A", "02", headersA, 10L));
            inputTopic1.pipeInput(new TestRecord<>("B", "02", headersB, 8L));

            if (storeFormat.equals("default")) {
                proc.checkAndClearProcessResult(
                    new KeyValueTimestamp<>("A", new Change<>(2, 1), 10),
                    new KeyValueTimestamp<>("B", new Change<>(2, 1), 8)
                );
            } else if (storeFormat.equals("headers")) {
                proc.checkAndClearProcessResultWithHeaders(
                    new KeyValueTimestampHeaders<>("A", new Change<>(2, 1), 10, headersA),
                    new KeyValueTimestampHeaders<>("B", new Change<>(2, 1), 8, headersB)
                );
            }

            inputTopic1.pipeInput(new TestRecord<>("A", "03", headersA, 20L));

            if (storeFormat.equals("default")) {
                proc.checkAndClearProcessResult(
                    new KeyValueTimestamp<>("A", new Change<>(3, 2), 20)
                );
            } else if (storeFormat.equals("headers")) {
                proc.checkAndClearProcessResultWithHeaders(
                    new KeyValueTimestampHeaders<>("A", new Change<>(3, 2), 20, headersA)
                );
            }

            inputTopic1.pipeInput(new TestRecord<>("A", null, headersA, 30L));

            if (storeFormat.equals("default")) {
                proc.checkAndClearProcessResult(
                    new KeyValueTimestamp<>("A", new Change<>(null, 3), 30)
                );
            } else if (storeFormat.equals("headers")) {
                proc.checkAndClearProcessResultWithHeaders(
                    new KeyValueTimestampHeaders<>("A", new Change<>(null, 3), 30, headersA)
                );
            }
        }
    }

    @Test
    public void shouldPreserveHeadersThroughMultipleMapValues() {
        final Properties props = getProps("headers");
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, String> table1 = builder.table(topic1, consumed);
        final KTable<String, Integer> table2 = table1.mapValues(s -> Integer.valueOf(s));
        final KTable<String, Integer> table3 = table2.mapValues(v -> v * 10);
        final KTable<String, String> table4 = table3.mapValues(v -> "value=" + v);

        final MockApiProcessorSupplier<String, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table4.toStream().process(supplier);

        final Headers headersA = makeHeaders("key", "A");
        final Headers headersB = makeHeaders("key", "B");
        final Headers headersC = makeHeaders("key", "C");

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer());

            inputTopic.pipeInput(new TestRecord<>("A", "1", headersA, 10L));
            inputTopic.pipeInput(new TestRecord<>("B", "2", headersB, 15L));
            inputTopic.pipeInput(new TestRecord<>("C", "3", headersC, 20L));
        }

        final List<MockApiProcessor<String, String, Void, Void>> processors = supplier.capturedProcessors(1);

        // Headers should be preserved through the chain: "1" -> 1 -> 10 -> "value=10"
        processors.get(0).checkAndClearProcessResultWithHeaders(
            new KeyValueTimestampHeaders<>("A", "value=10", 10, headersA),
            new KeyValueTimestampHeaders<>("B", "value=20", 15, headersB),
            new KeyValueTimestampHeaders<>("C", "value=30", 20, headersC));
    }

    @Test
    public void shouldPreserveHeadersWithDifferentHeadersForSameKey() {
        final Properties props = getProps("headers");
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, String> table1 = builder.table(topic1, consumed);
        final KTable<String, Integer> table2 = table1.mapValues(s -> Integer.valueOf(s));

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        final Headers headers1 = makeHeaders("version", "v1");
        final Headers headers2 = makeHeaders("version", "v2");

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer());

            inputTopic.pipeInput(new TestRecord<>("A", "5", headers1, 10L));
            inputTopic.pipeInput(new TestRecord<>("A", "10", headers2, 20L));
        }

        final List<MockApiProcessor<String, Integer, Void, Void>> processors = supplier.capturedProcessors(1);

        // Headers should update when the same key is updated
        processors.get(0).checkAndClearProcessResultWithHeaders(
            new KeyValueTimestampHeaders<>("A", 5, 10, headers1),
            new KeyValueTimestampHeaders<>("A", 10, 20, headers2));
    }

    @Test
    public void shouldPreserveHeadersWithNullValue() {
        final Properties props = getProps("headers");
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, String> table1 = builder.table(topic1, consumed);
        final KTable<String, Integer> table2 = table1.mapValues(
            s -> s == null ? null : Integer.valueOf(s)
        );

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        final Headers headersA = makeHeaders("key", "A");
        final Headers headersB = makeHeaders("key", "B");

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer());

            inputTopic.pipeInput(new TestRecord<>("A", "5", headersA, 10L));
            inputTopic.pipeInput(new TestRecord<>("B", null, headersB, 15L));
        }

        final List<MockApiProcessor<String, Integer, Void, Void>> processors = supplier.capturedProcessors(1);

        // Headers should be preserved even with null values
        processors.get(0).checkAndClearProcessResultWithHeaders(
            new KeyValueTimestampHeaders<>("A", 5, 10, headersA),
            new KeyValueTimestampHeaders<>("B", null, 15, headersB));
    }

    @Test
    public void shouldPreserveHeadersWithMaterializedMapValues() {
        final Properties props = getProps("headers");
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, String> table1 = builder.table(topic1, consumed);
        final KTable<String, Integer> table2 = table1.mapValues(
            s -> Integer.valueOf(s),
            Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("mapped-store")
                .withValueSerde(Serdes.Integer())
        );

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        final Headers headersA = makeHeaders("key", "A");
        final Headers headersB = makeHeaders("key", "B");

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer());

            inputTopic.pipeInput(new TestRecord<>("A", "100", headersA, 10L));
            inputTopic.pipeInput(new TestRecord<>("B", "200", headersB, 15L));
        }

        final List<MockApiProcessor<String, Integer, Void, Void>> processors = supplier.capturedProcessors(1);

        // Headers should be preserved with materialized mapValues
        processors.get(0).checkAndClearProcessResultWithHeaders(
            new KeyValueTimestampHeaders<>("A", 100, 10, headersA),
            new KeyValueTimestampHeaders<>("B", 200, 15, headersB));
    }

    @Test
    public void shouldPreserveHeadersWithMultipleHeaders() {
        final Properties props = getProps("headers");
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, String> table1 = builder.table(topic1, consumed);
        final KTable<String, Integer> table2 = table1.mapValues(s -> Integer.valueOf(s));

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        final RecordHeaders multiHeaders = new RecordHeaders();
        multiHeaders.add(new RecordHeader("source", "kafka".getBytes()));
        multiHeaders.add(new RecordHeader("region", "us-west".getBytes()));
        multiHeaders.add(new RecordHeader("version", "1.0".getBytes()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer());

            inputTopic.pipeInput(new TestRecord<>("A", "42", multiHeaders, 10L));
        }

        final List<MockApiProcessor<String, Integer, Void, Void>> processors = supplier.capturedProcessors(1);

        processors.get(0).checkAndClearProcessResultWithHeaders(
            new KeyValueTimestampHeaders<>("A", 42, 10, multiHeaders));
    }
}
