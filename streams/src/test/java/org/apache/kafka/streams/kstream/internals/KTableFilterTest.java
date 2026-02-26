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
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
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
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.MockApiProcessor;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.StreamsTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@SuppressWarnings("unchecked")
public class KTableFilterTest {
    private final Consumed<String, Integer> consumed = Consumed.with(Serdes.String(), Serdes.Integer());

    private final Predicate<String, Integer> predicate = (key, value) -> (value % 2) == 0;

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
        final Properties properties = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.Integer());
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
                              final KTable<String, Integer> table2,
                              final KTable<String, Integer> table3,
                              final String topic,
                              final String storeFormat,
                              final Properties props) {
        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);
        table3.toStream().process(supplier);

        final Headers headersA = makeHeaders("key", "A");
        final Headers headersB = makeHeaders("key", "B");
        final Headers headersC = makeHeaders("key", "C");
        final Headers headersD = makeHeaders("key", "D");

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, Integer> inputTopic =
                    driver.createInputTopic(topic, new StringSerializer(), new IntegerSerializer());
            inputTopic.pipeInput(new TestRecord<>("A", 1, headersA, 10L));
            inputTopic.pipeInput(new TestRecord<>("B", 2, headersB, 5L));
            inputTopic.pipeInput(new TestRecord<>("C", 3, headersC, 8L));
            inputTopic.pipeInput(new TestRecord<>("D", 4, headersD, 14L));
            inputTopic.pipeInput(new TestRecord<>("A", null, headersA, 18L));
            inputTopic.pipeInput(new TestRecord<>("B", null, headersB, 15L));
        }

        final List<MockApiProcessor<String, Integer, Void, Void>> processors = supplier.capturedProcessors(2);

        if (storeFormat.equals("default")) {
            processors.get(0).checkAndClearProcessResult(new KeyValueTimestamp<>("A", null, 10),
                new KeyValueTimestamp<>("B", 2, 5),
                new KeyValueTimestamp<>("C", null, 8),
                new KeyValueTimestamp<>("D", 4, 14),
                new KeyValueTimestamp<>("A", null, 18),
                new KeyValueTimestamp<>("B", null, 15));
            processors.get(1).checkAndClearProcessResult(new KeyValueTimestamp<>("A", 1, 10),
                new KeyValueTimestamp<>("B", null, 5),
                new KeyValueTimestamp<>("C", 3, 8),
                new KeyValueTimestamp<>("D", null, 14),
                new KeyValueTimestamp<>("A", null, 18),
                new KeyValueTimestamp<>("B", null, 15));
        } else if (storeFormat.equals("headers")) {
            processors.get(0).checkAndClearProcessResultWithHeaders(new KeyValueTimestampHeaders<>("A", null, 10, headersA),
                new KeyValueTimestampHeaders<>("B", 2, 5, headersB),
                new KeyValueTimestampHeaders<>("C", null, 8, headersC),
                new KeyValueTimestampHeaders<>("D", 4, 14, headersD),
                new KeyValueTimestampHeaders<>("A", null, 18, headersA),
                new KeyValueTimestampHeaders<>("B", null, 15, headersB));
            processors.get(1).checkAndClearProcessResultWithHeaders(new KeyValueTimestampHeaders<>("A", 1, 10, headersA),
                new KeyValueTimestampHeaders<>("B", null, 5, headersB),
                new KeyValueTimestampHeaders<>("C", 3, 8, headersC),
                new KeyValueTimestampHeaders<>("D", null, 14, headersD),
                new KeyValueTimestampHeaders<>("A", null, 18, headersA),
                new KeyValueTimestampHeaders<>("B", null, 15, headersB));
        }
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldPassThroughWithoutMaterialization(final String storeFormat) {
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, Integer> table1 = builder.table(topic1, consumed);
        final KTable<String, Integer> table2 = table1.filter(predicate);
        final KTable<String, Integer> table3 = table1.filterNot(predicate);

        assertNull(table1.queryableStoreName());
        assertNull(table2.queryableStoreName());
        assertNull(table3.queryableStoreName());

        doTestKTable(builder, table2, table3, topic1, storeFormat, props);
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldPassThroughOnMaterialization(final String storeFormat) {
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, Integer> table1 = builder.table(topic1, consumed);
        final KTable<String, Integer> table2 = table1.filter(predicate, Materialized.as("store2"));
        final KTable<String, Integer> table3 = table1.filterNot(predicate);

        assertNull(table1.queryableStoreName());
        assertEquals("store2", table2.queryableStoreName());
        assertNull(table3.queryableStoreName());

        doTestKTable(builder, table2, table3, topic1, storeFormat, props);
    }

    private void doTestValueGetter(final StreamsBuilder builder,
                                   final KTableImpl<String, Integer, Integer> table2,
                                   final KTableImpl<String, Integer, Integer> table3,
                                   final String topic1,
                                   final String storeFormat,
                                   final Properties props) {

        final Topology topology = builder.build();

        final KTableValueGetterSupplier<String, Integer> getterSupplier2 = table2.valueGetterSupplier();
        final KTableValueGetterSupplier<String, Integer> getterSupplier3 = table3.valueGetterSupplier();

        final InternalTopologyBuilder topologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);
        topologyBuilder.connectProcessorAndStateStores(table2.name, getterSupplier2.storeNames());
        topologyBuilder.connectProcessorAndStateStores(table3.name, getterSupplier3.storeNames());

        final Headers headersA = makeHeaders("key", "A");
        final Headers headersB = makeHeaders("key", "B");
        final Headers headersC = makeHeaders("key", "C");

        try (final TopologyTestDriverWrapper driver = new TopologyTestDriverWrapper(topology, props)) {
            final TestInputTopic<String, Integer> inputTopic =
                    driver.createInputTopic(topic1, new StringSerializer(), new IntegerSerializer());

            final KTableValueGetter<String, Integer> getter2 = getterSupplier2.get();
            final KTableValueGetter<String, Integer> getter3 = getterSupplier3.get();

            getter2.init(driver.setCurrentNodeForProcessorContext(table2.name));
            getter3.init(driver.setCurrentNodeForProcessorContext(table3.name));

            inputTopic.pipeInput(new TestRecord<>("A", 1, headersA, 5L));
            inputTopic.pipeInput(new TestRecord<>("B", 1, headersB, 10L));
            inputTopic.pipeInput(new TestRecord<>("C", 1, headersC, 15L));

            assertNull(getter2.get("A"));
            assertNull(getter2.get("B"));
            assertNull(getter2.get("C"));

            assertEquals(ValueAndTimestamp.make(1, 5L), getter3.get("A"));
            assertEquals(ValueAndTimestamp.make(1, 10L), getter3.get("B"));
            assertEquals(ValueAndTimestamp.make(1, 15L), getter3.get("C"));

            inputTopic.pipeInput(new TestRecord<>("A", 2, headersA, 10L));
            inputTopic.pipeInput(new TestRecord<>("B", 2, headersB, 5L));

            assertEquals(ValueAndTimestamp.make(2, 10L), getter2.get("A"));
            assertEquals(ValueAndTimestamp.make(2, 5L), getter2.get("B"));
            assertNull(getter2.get("C"));

            assertNull(getter3.get("A"));
            assertNull(getter3.get("B"));
            assertEquals(ValueAndTimestamp.make(1, 15L), getter3.get("C"));

            inputTopic.pipeInput(new TestRecord<>("A", 3, headersA, 15L));

            assertNull(getter2.get("A"));
            assertEquals(ValueAndTimestamp.make(2, 5L), getter2.get("B"));
            assertNull(getter2.get("C"));

            assertEquals(ValueAndTimestamp.make(3, 15L), getter3.get("A"));
            assertNull(getter3.get("B"));
            assertEquals(ValueAndTimestamp.make(1, 15L), getter3.get("C"));

            inputTopic.pipeInput(new TestRecord<>("A", null, headersA, 10L));
            inputTopic.pipeInput(new TestRecord<>("B", null, headersB, 20L));

            assertNull(getter2.get("A"));
            assertNull(getter2.get("B"));
            assertNull(getter2.get("C"));

            assertNull(getter3.get("A"));
            assertNull(getter3.get("B"));
            assertEquals(ValueAndTimestamp.make(1, 15L), getter3.get("C"));
        }
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldGetValuesOnMaterialization(final String storeFormat) {
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTableImpl<String, Integer, Integer> table1 =
            (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed);
        final KTableImpl<String, Integer, Integer> table2 =
            (KTableImpl<String, Integer, Integer>) table1.filter(predicate, Materialized.as("store2"));
        final KTableImpl<String, Integer, Integer> table3 =
            (KTableImpl<String, Integer, Integer>) table1.filterNot(predicate, Materialized.as("store3"));
        final KTableImpl<String, Integer, Integer> table4 =
            (KTableImpl<String, Integer, Integer>) table1.filterNot(predicate);

        assertNull(table1.queryableStoreName());
        assertEquals("store2", table2.queryableStoreName());
        assertEquals("store3", table3.queryableStoreName());
        assertNull(table4.queryableStoreName());

        doTestValueGetter(builder, table2, table3, topic1, storeFormat, props);
    }

    private void doTestNotSendingOldValue(final StreamsBuilder builder,
                                          final KTableImpl<String, Integer, Integer> table1,
                                          final KTableImpl<String, Integer, Integer> table2,
                                          final String topic1,
                                          final String storeFormat,
                                          final Properties props) {
        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();

        builder.build().addProcessor("proc1", supplier, table1.name);
        builder.build().addProcessor("proc2", supplier, table2.name);

        final Headers headersA = makeHeaders("key", "A");
        final Headers headersB = makeHeaders("key", "B");
        final Headers headersC = makeHeaders("key", "C");

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, Integer> inputTopic =
                    driver.createInputTopic(topic1, new StringSerializer(), new IntegerSerializer());

            inputTopic.pipeInput(new TestRecord<>("A", 1, headersA, 5L));
            inputTopic.pipeInput(new TestRecord<>("B", 1, headersB, 10L));
            inputTopic.pipeInput(new TestRecord<>("C", 1, headersC, 15L));

            final List<MockApiProcessor<String, Integer, Void, Void>> processors = supplier.capturedProcessors(2);

            if (storeFormat.equals("default")) {
                processors.get(0).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(1, null), 5),
                    new KeyValueTimestamp<>("B", new Change<>(1, null), 10),
                    new KeyValueTimestamp<>("C", new Change<>(1, null), 15));
                processors.get(1).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(null, null), 5),
                    new KeyValueTimestamp<>("B", new Change<>(null, null), 10),
                    new KeyValueTimestamp<>("C", new Change<>(null, null), 15));
            } else if (storeFormat.equals("headers")) {
                processors.get(0).checkAndClearProcessResultWithHeaders(new KeyValueTimestampHeaders<>("A", new Change<>(1, null), 5, headersA),
                    new KeyValueTimestampHeaders<>("B", new Change<>(1, null), 10, headersB),
                    new KeyValueTimestampHeaders<>("C", new Change<>(1, null), 15, headersC));
                processors.get(1).checkAndClearProcessResultWithHeaders(new KeyValueTimestampHeaders<>("A", new Change<>(null, null), 5, headersA),
                    new KeyValueTimestampHeaders<>("B", new Change<>(null, null), 10, headersB),
                    new KeyValueTimestampHeaders<>("C", new Change<>(null, null), 15, headersC));
            }

            inputTopic.pipeInput(new TestRecord<>("A", 2, headersA, 15L));
            inputTopic.pipeInput(new TestRecord<>("B", 2, headersB, 8L));

            if (storeFormat.equals("default")) {
                processors.get(0).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(2, null), 15),
                    new KeyValueTimestamp<>("B", new Change<>(2, null), 8));
                processors.get(1).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(2, null), 15),
                    new KeyValueTimestamp<>("B", new Change<>(2, null), 8));
            } else if (storeFormat.equals("headers")) {
                processors.get(0).checkAndClearProcessResultWithHeaders(new KeyValueTimestampHeaders<>("A", new Change<>(2, null), 15, headersA),
                    new KeyValueTimestampHeaders<>("B", new Change<>(2, null), 8, headersB));
                processors.get(1).checkAndClearProcessResultWithHeaders(new KeyValueTimestampHeaders<>("A", new Change<>(2, null), 15, headersA),
                    new KeyValueTimestampHeaders<>("B", new Change<>(2, null), 8, headersB));
            }

            inputTopic.pipeInput(new TestRecord<>("A", 3, headersA, 20L));

            if (storeFormat.equals("default")) {
                processors.get(0).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(3, null), 20));
                processors.get(1).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(null, null), 20));
            } else if (storeFormat.equals("headers")) {
                processors.get(0).checkAndClearProcessResultWithHeaders(new KeyValueTimestampHeaders<>("A", new Change<>(3, null), 20, headersA));
                processors.get(1).checkAndClearProcessResultWithHeaders(new KeyValueTimestampHeaders<>("A", new Change<>(null, null), 20, headersA));
            }

            inputTopic.pipeInput(new TestRecord<>("A", null, headersA, 10L));
            inputTopic.pipeInput(new TestRecord<>("B", null, headersB, 20L));

            if (storeFormat.equals("default")) {
                processors.get(0).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(null, null), 10),
                    new KeyValueTimestamp<>("B", new Change<>(null, null), 20));
                processors.get(1).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(null, null), 10),
                    new KeyValueTimestamp<>("B", new Change<>(null, null), 20));
            } else if (storeFormat.equals("headers")) {
                processors.get(0).checkAndClearProcessResultWithHeaders(new KeyValueTimestampHeaders<>("A", new Change<>(null, null), 10, headersA),
                    new KeyValueTimestampHeaders<>("B", new Change<>(null, null), 20, headersB));
                processors.get(1).checkAndClearProcessResultWithHeaders(new KeyValueTimestampHeaders<>("A", new Change<>(null, null), 10, headersA),
                    new KeyValueTimestampHeaders<>("B", new Change<>(null, null), 20, headersB));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldNotSendOldValuesWithoutMaterialization(final String storeFormat) {
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTableImpl<String, Integer, Integer> table1 =
            (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed);
        final KTableImpl<String, Integer, Integer> table2 = (KTableImpl<String, Integer, Integer>) table1.filter(predicate);

        doTestNotSendingOldValue(builder, table1, table2, topic1, storeFormat, props);
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldNotSendOldValuesOnMaterialization(final String storeFormat) {
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTableImpl<String, Integer, Integer> table1 =
            (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed);
        final KTableImpl<String, Integer, Integer> table2 =
            (KTableImpl<String, Integer, Integer>) table1.filter(predicate, Materialized.as("store2"));

        doTestNotSendingOldValue(builder, table1, table2, topic1, storeFormat, props);
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldNotEnableSendingOldValuesIfNotAlreadyMaterializedAndNotForcedToMaterialize(final String storeFormat) {
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTableImpl<String, Integer, Integer> table1 =
            (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed);
        final KTableImpl<String, Integer, Integer> table2 = (KTableImpl<String, Integer, Integer>) table1.filter(predicate);

        table2.enableSendingOldValues(false);

        doTestNotSendingOldValue(builder, table1, table2, topic1, storeFormat, props);
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private void doTestSendingOldValue(final StreamsBuilder builder,
                                       final KTableImpl<String, Integer, Integer> table1,
                                       final KTableImpl<String, Integer, Integer> table2,
                                       final String topic1,
                                       final String storeFormat,
                                       final Properties props) {
        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final Topology topology = builder.build();

        topology.addProcessor("proc1", supplier, table1.name);
        topology.addProcessor("proc2", supplier, table2.name);

        final boolean parentSendOldVals = table1.sendingOldValueEnabled();

        final Headers headersA = makeHeaders("key", "A");
        final Headers headersB = makeHeaders("key", "B");
        final Headers headersC = makeHeaders("key", "C");

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            final TestInputTopic<String, Integer> inputTopic =
                    driver.createInputTopic(topic1, new StringSerializer(), new IntegerSerializer());

            inputTopic.pipeInput(new TestRecord<>("A", 1, headersA, 5L));
            inputTopic.pipeInput(new TestRecord<>("B", 1, headersB, 10L));
            inputTopic.pipeInput(new TestRecord<>("C", 1, headersC, 15L));

            final List<MockApiProcessor<String, Integer, Void, Void>> processors = supplier.capturedProcessors(2);
            final MockApiProcessor<String, Integer, Void, Void> table1Output = processors.get(0);
            final MockApiProcessor<String, Integer, Void, Void> table2Output = processors.get(1);

            if (storeFormat.equals("default")) {
                table1Output.checkAndClearProcessResult(
                    new KeyValueTimestamp<>("A", new Change<>(1, null), 5),
                    new KeyValueTimestamp<>("B", new Change<>(1, null), 10),
                    new KeyValueTimestamp<>("C", new Change<>(1, null), 15)
                );
            } else if (storeFormat.equals("headers")) {
                table1Output.checkAndClearProcessResultWithHeaders(
                    new KeyValueTimestampHeaders<>("A", new Change<>(1, null), 5, headersA),
                    new KeyValueTimestampHeaders<>("B", new Change<>(1, null), 10, headersB),
                    new KeyValueTimestampHeaders<>("C", new Change<>(1, null), 15, headersC)
                );
            }
            table2Output.checkEmptyAndClearProcessResult();

            inputTopic.pipeInput(new TestRecord<>("A", 2, headersA, 15L));
            inputTopic.pipeInput(new TestRecord<>("B", 2, headersB, 8L));

            if (storeFormat.equals("default")) {
                table1Output.checkAndClearProcessResult(
                    new KeyValueTimestamp<>("A", new Change<>(2, parentSendOldVals ? 1 : null), 15),
                    new KeyValueTimestamp<>("B", new Change<>(2, parentSendOldVals ? 1 : null), 8)
                );
                table2Output.checkAndClearProcessResult(
                    new KeyValueTimestamp<>("A", new Change<>(2, null), 15),
                    new KeyValueTimestamp<>("B", new Change<>(2, null), 8)
                );
            } else if (storeFormat.equals("headers")) {
                table1Output.checkAndClearProcessResultWithHeaders(
                    new KeyValueTimestampHeaders<>("A", new Change<>(2, parentSendOldVals ? 1 : null), 15, headersA),
                    new KeyValueTimestampHeaders<>("B", new Change<>(2, parentSendOldVals ? 1 : null), 8, headersB)
                );
                table2Output.checkAndClearProcessResultWithHeaders(
                    new KeyValueTimestampHeaders<>("A", new Change<>(2, null), 15, headersA),
                    new KeyValueTimestampHeaders<>("B", new Change<>(2, null), 8, headersB)
                );
            }

            inputTopic.pipeInput(new TestRecord<>("A", 3, headersA, 20L));

            if (storeFormat.equals("default")) {
                table1Output.checkAndClearProcessResult(
                    new KeyValueTimestamp<>("A", new Change<>(3, parentSendOldVals ? 2 : null), 20)
                );
                table2Output.checkAndClearProcessResult(
                    new KeyValueTimestamp<>("A", new Change<>(null, 2), 20)
                );
            } else if (storeFormat.equals("headers")) {
                table1Output.checkAndClearProcessResultWithHeaders(
                    new KeyValueTimestampHeaders<>("A", new Change<>(3, parentSendOldVals ? 2 : null), 20, headersA)
                );
                table2Output.checkAndClearProcessResultWithHeaders(
                    new KeyValueTimestampHeaders<>("A", new Change<>(null, 2), 20, headersA)
                );
            }

            inputTopic.pipeInput(new TestRecord<>("A", null, headersA, 10L));
            inputTopic.pipeInput(new TestRecord<>("B", null, headersB, 20L));

            if (storeFormat.equals("default")) {
                table1Output.checkAndClearProcessResult(
                    new KeyValueTimestamp<>("A", new Change<>(null, parentSendOldVals ? 3 : null), 10),
                    new KeyValueTimestamp<>("B", new Change<>(null, parentSendOldVals ? 2 : null), 20)
                );
                table2Output.checkAndClearProcessResult(
                    new KeyValueTimestamp<>("B", new Change<>(null, 2), 20)
                );
            } else if (storeFormat.equals("headers")) {
                table1Output.checkAndClearProcessResultWithHeaders(
                    new KeyValueTimestampHeaders<>("A", new Change<>(null, parentSendOldVals ? 3 : null), 10, headersA),
                    new KeyValueTimestampHeaders<>("B", new Change<>(null, parentSendOldVals ? 2 : null), 20, headersB)
                );
                table2Output.checkAndClearProcessResultWithHeaders(
                    new KeyValueTimestampHeaders<>("B", new Change<>(null, 2), 20, headersB)
                );
            }
        }
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldEnableSendOldValuesWhenNotMaterializedAlreadyButForcedToMaterialize(final String storeFormat) {
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTableImpl<String, Integer, Integer> table1 =
            (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed);
        final KTableImpl<String, Integer, Integer> table2 =
            (KTableImpl<String, Integer, Integer>) table1.filter(predicate);

        table2.enableSendingOldValues(true);

        assertThat(table1.sendingOldValueEnabled(), is(true));
        assertThat(table2.sendingOldValueEnabled(), is(true));

        doTestSendingOldValue(builder, table1, table2, topic1, storeFormat, props);
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldEnableSendOldValuesWhenMaterializedAlreadyAndForcedToMaterialize(final String storeFormat) {
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTableImpl<String, Integer, Integer> table1 =
            (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed);
        final KTableImpl<String, Integer, Integer> table2 =
            (KTableImpl<String, Integer, Integer>) table1.filter(predicate, Materialized.as("store2"));

        table2.enableSendingOldValues(true);

        assertThat(table1.sendingOldValueEnabled(), is(false));
        assertThat(table2.sendingOldValueEnabled(), is(true));

        doTestSendingOldValue(builder, table1, table2, topic1, storeFormat, props);
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldSendOldValuesWhenEnabledOnUpStreamMaterialization(final String storeFormat) {
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTableImpl<String, Integer, Integer> table1 =
            (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed, Materialized.as("store2"));
        final KTableImpl<String, Integer, Integer> table2 =
            (KTableImpl<String, Integer, Integer>) table1.filter(predicate);

        table2.enableSendingOldValues(false);

        assertThat(table1.sendingOldValueEnabled(), is(true));
        assertThat(table2.sendingOldValueEnabled(), is(true));

        doTestSendingOldValue(builder, table1, table2, topic1, storeFormat, props);
    }

    private void doTestSkipNullOnMaterialization(final StreamsBuilder builder,
                                                 final KTableImpl<String, String, String> table1,
                                                 final KTableImpl<String, String, String> table2,
                                                 final String topic1,
                                                 final boolean shouldSkip,
                                                 final String storeFormat,
                                                 final Properties props) {
        final MockApiProcessorSupplier<String, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final Topology topology = builder.build();

        topology.addProcessor("proc1", supplier, table1.name);
        topology.addProcessor("proc2", supplier, table2.name);

        final Headers headersA = makeHeaders("key", "A");
        final Headers headersB = makeHeaders("key", "B");
        final Headers headersC = makeHeaders("key", "C");

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            final TestInputTopic<String, String> stringinputTopic =
                    driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer());

            stringinputTopic.pipeInput(new TestRecord<>("A", "reject", headersA, 5L));
            stringinputTopic.pipeInput(new TestRecord<>("B", "reject", headersB, 10L));
            stringinputTopic.pipeInput(new TestRecord<>("C", "reject", headersC, 20L));
        }

        final List<MockApiProcessor<String, String, Void, Void>> processors = supplier.capturedProcessors(2);

        if (storeFormat.equals("default")) {
            processors.get(0).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>("reject", null), 5),
                new KeyValueTimestamp<>("B", new Change<>("reject", null), 10),
                new KeyValueTimestamp<>("C", new Change<>("reject", null), 20));
            if (shouldSkip) {
                processors.get(1).checkEmptyAndClearProcessResult();
            } else {
                processors.get(1).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(null, null), 5),
                    new KeyValueTimestamp<>("B", new Change<>(null, null), 10),
                    new KeyValueTimestamp<>("C", new Change<>(null, null), 20));
            }
        } else if (storeFormat.equals("headers")) {
            processors.get(0).checkAndClearProcessResultWithHeaders(new KeyValueTimestampHeaders<>("A", new Change<>("reject", null), 5, headersA),
                new KeyValueTimestampHeaders<>("B", new Change<>("reject", null), 10, headersB),
                new KeyValueTimestampHeaders<>("C", new Change<>("reject", null), 20, headersC));
            if (shouldSkip) {
                processors.get(1).checkEmptyAndClearProcessResult();
            } else {
                processors.get(1).checkAndClearProcessResultWithHeaders(new KeyValueTimestampHeaders<>("A", new Change<>(null, null), 5, headersA),
                    new KeyValueTimestampHeaders<>("B", new Change<>(null, null), 10, headersB),
                    new KeyValueTimestampHeaders<>("C", new Change<>(null, null), 20, headersC));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldSkipNullToRepartitionWithoutMaterialization(final String storeFormat) {
        // Do not explicitly set enableSendingOldValues. Let a further downstream stateful operator trigger it instead.
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";

        final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        final KTableImpl<String, String, String> table2 =
            (KTableImpl<String, String, String>) table1.filter((key, value) -> value.equalsIgnoreCase("accept"));
        table2.groupBy(MockMapper.noOpKeyValueMapper())
            .reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER);

        doTestSkipNullOnMaterialization(builder, table1, table2, topic1, true, storeFormat, props);
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldSkipNullToRepartitionOnMaterialization(final String storeFormat) {
        // Do not explicitly set enableSendingOldValues. Let a further downstream stateful operator trigger it instead.
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";

        final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        final KTableImpl<String, String, String> table2 =
            (KTableImpl<String, String, String>) table1.filter((key, value) -> value.equalsIgnoreCase("accept"), Materialized.as("store2"));
        table2.groupBy(MockMapper.noOpKeyValueMapper())
            .reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER, Materialized.as("mock-result"));

        doTestSkipNullOnMaterialization(builder, table1, table2, topic1, true, storeFormat, props);
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldNotSkipNullIfVersionedUpstream(final String storeFormat) {
        // stateful downstream operation enables sendOldValues, but duplicate nulls will still
        // be sent because the source table is versioned
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";
        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5)));
        final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());

        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, consumed, versionedMaterialize);
        final KTableImpl<String, String, String> table2 =
            (KTableImpl<String, String, String>) table1.filter((key, value) -> value.equalsIgnoreCase("accept"));
        table2.groupBy(MockMapper.noOpKeyValueMapper())
            .reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER);

        doTestSkipNullOnMaterialization(builder, table1, table2, topic1, false, storeFormat, props);
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldSkipNullIfVersionedDownstream(final String storeFormat) {
        // materializing the result of the filter as a versioned store does not prevent duplicate
        // tombstones from being sent, as it's whether the input table is versioned or not that
        // determines whether the optimization is enabled
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";
        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5)));
        final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());

        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, consumed, Materialized.as("store"));
        final KTableImpl<String, String, String> table2 =
            (KTableImpl<String, String, String>) table1.filter((key, value) -> value.equalsIgnoreCase("accept"), versionedMaterialize);
        table2.groupBy(MockMapper.noOpKeyValueMapper())
            .reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER);

        doTestSkipNullOnMaterialization(builder, table1, table2, topic1, true, storeFormat, props);
    }

    @Test
    public void testTypeVariance() {
        final Predicate<Number, Object> numberKeyPredicate = (key, value) -> false;

        new StreamsBuilder()
            .<Integer, String>table("empty")
            .filter(numberKeyPredicate)
            .filterNot(numberKeyPredicate)
            .toStream()
            .to("nirvana");
    }

    @Test
    public void shouldPreserveHeadersThroughFilterChain() {
        final Properties props = getProps("headers");
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, Integer> table1 = builder.table(topic1, consumed);
        final KTable<String, Integer> table2 = table1
            .filter((key, value) -> value != null && value > 0)
            .filter((key, value) -> value % 2 == 0)
            .filterNot((key, value) -> value > 100);

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        final Headers headersA = makeHeaders("key", "A");
        final Headers headersB = makeHeaders("key", "B");
        final Headers headersC = makeHeaders("key", "C");

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, Integer> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new IntegerSerializer());

            inputTopic.pipeInput(new TestRecord<>("A", 2, headersA, 10L));
            inputTopic.pipeInput(new TestRecord<>("B", 50, headersB, 15L));
            inputTopic.pipeInput(new TestRecord<>("C", 150, headersC, 20L));
        }

        final List<MockApiProcessor<String, Integer, Void, Void>> processors = supplier.capturedProcessors(1);

        // C is filtered out by filterNot, which produces a tombstone
        processors.get(0).checkAndClearProcessResultWithHeaders(
            new KeyValueTimestampHeaders<>("A", 2, 10, headersA),
            new KeyValueTimestampHeaders<>("B", 50, 15, headersB),
            new KeyValueTimestampHeaders<>("C", null, 20, headersC));
    }

    @Test
    public void shouldPreserveHeadersWhenFilterProducesTombstone() {
        final Properties props = getProps("headers");
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, Integer> table1 = builder.table(topic1, consumed);
        final KTable<String, Integer> table2 = table1.filter((key, value) -> value != null && value % 2 == 0);

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        final Headers headersA = makeHeaders("key", "A");
        final Headers headersB = makeHeaders("key", "B");

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, Integer> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new IntegerSerializer());

            inputTopic.pipeInput(new TestRecord<>("A", 2, headersA, 10L));
            inputTopic.pipeInput(new TestRecord<>("A", null, headersA, 15L));
            inputTopic.pipeInput(new TestRecord<>("B", 3, headersB, 20L));
        }

        final List<MockApiProcessor<String, Integer, Void, Void>> processors = supplier.capturedProcessors(1);

        processors.get(0).checkAndClearProcessResultWithHeaders(
            new KeyValueTimestampHeaders<>("A", 2, 10, headersA),
            new KeyValueTimestampHeaders<>("A", null, 15, headersA),
            new KeyValueTimestampHeaders<>("B", null, 20, headersB));
    }

    @Test
    public void shouldHandleDifferentHeadersForSameKey() {
        final Properties props = getProps("headers");
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, Integer> table1 = builder.table(topic1, consumed);
        final KTable<String, Integer> table2 = table1.filter(predicate, Materialized.as("filter-store"));

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        final Headers headersA1 = makeHeaders("version", "v1");
        final Headers headersA2 = makeHeaders("version", "v2");
        final Headers headersA3 = makeHeaders("version", "v3");

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, Integer> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new IntegerSerializer());

            inputTopic.pipeInput(new TestRecord<>("A", 2, headersA1, 10L));
            inputTopic.pipeInput(new TestRecord<>("A", 4, headersA2, 15L));
            inputTopic.pipeInput(new TestRecord<>("A", 6, headersA3, 20L));
        }

        final List<MockApiProcessor<String, Integer, Void, Void>> processors = supplier.capturedProcessors(1);

        processors.get(0).checkAndClearProcessResultWithHeaders(
            new KeyValueTimestampHeaders<>("A", 2, 10, headersA1),
            new KeyValueTimestampHeaders<>("A", 4, 15, headersA2),
            new KeyValueTimestampHeaders<>("A", 6, 20, headersA3));
    }

    @Test
    public void shouldHandleRecordsWithEmptyHeaders() {
        final Properties props = getProps("headers");
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, Integer> table1 = builder.table(topic1, consumed);
        final KTable<String, Integer> table2 = table1.filter(predicate);

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        final Headers emptyHeaders = new RecordHeaders();

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, Integer> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new IntegerSerializer());

            inputTopic.pipeInput(new TestRecord<>("A", 2, emptyHeaders, 10L));
            inputTopic.pipeInput(new TestRecord<>("B", 4, emptyHeaders, 15L));
        }

        final List<MockApiProcessor<String, Integer, Void, Void>> processors = supplier.capturedProcessors(1);

        processors.get(0).checkAndClearProcessResultWithHeaders(
            new KeyValueTimestampHeaders<>("A", 2, 10, emptyHeaders),
            new KeyValueTimestampHeaders<>("B", 4, 15, emptyHeaders));
    }

    @Test
    public void shouldHandleRecordsWithMultipleHeaders() {
        final Properties props = getProps("headers");
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, Integer> table1 = builder.table(topic1, consumed);
        final KTable<String, Integer> table2 = table1.filter(predicate);

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        final RecordHeaders multiHeaders = new RecordHeaders();
        multiHeaders.add(new RecordHeader("key1", "value1".getBytes()));
        multiHeaders.add(new RecordHeader("key2", "value2".getBytes()));
        multiHeaders.add(new RecordHeader("key3", "value3".getBytes()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, Integer> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new IntegerSerializer());

            inputTopic.pipeInput(new TestRecord<>("A", 2, multiHeaders, 10L));
            inputTopic.pipeInput(new TestRecord<>("B", 4, multiHeaders, 15L));
        }

        final List<MockApiProcessor<String, Integer, Void, Void>> processors = supplier.capturedProcessors(1);

        processors.get(0).checkAndClearProcessResultWithHeaders(
            new KeyValueTimestampHeaders<>("A", 2, 10, multiHeaders),
            new KeyValueTimestampHeaders<>("B", 4, 15, multiHeaders));
    }

    @Test
    public void shouldPreserveHeadersThroughFilterToStreamToTable() {
        final Properties props = getProps("headers");
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";
        final String topic2 = "topic2";

        final KTable<String, Integer> table1 = builder.table(topic1, consumed);
        final KTable<String, Integer> table2 = table1.filter(predicate);
        table2.toStream().to(topic2, Produced.with(Serdes.String(), Serdes.Integer()));

        final KTable<String, Integer> table3 = builder.table(topic2, consumed);

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table3.toStream().process(supplier);

        final Headers headersA = makeHeaders("key", "A");
        final Headers headersB = makeHeaders("key", "B");

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, Integer> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new IntegerSerializer());

            inputTopic.pipeInput(new TestRecord<>("A", 2, headersA, 10L));
            inputTopic.pipeInput(new TestRecord<>("B", 4, headersB, 15L));
        }

        final List<MockApiProcessor<String, Integer, Void, Void>> processors = supplier.capturedProcessors(1);

        processors.get(0).checkAndClearProcessResultWithHeaders(
            new KeyValueTimestampHeaders<>("A", 2, 10, headersA),
            new KeyValueTimestampHeaders<>("B", 4, 15, headersB));
    }

    @Test
    public void shouldPreserveHeadersWithSuppressAndFilter() {
        final Properties props = getProps("headers");
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, Integer> table1 = builder.table(topic1, consumed);
        final KTable<String, Integer> table2 = table1
            .filter(predicate)
            .suppress(Suppressed.untilTimeLimit(Duration.ofMillis(100), Suppressed.BufferConfig.unbounded()));

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        final Headers headersA = makeHeaders("key", "A");
        final Headers headersB = makeHeaders("key", "B");

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, Integer> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new IntegerSerializer());

            inputTopic.pipeInput(new TestRecord<>("A", 2, headersA, 10L));
            inputTopic.pipeInput(new TestRecord<>("B", 4, headersB, 15L));
            inputTopic.pipeInput(new TestRecord<>("A", 6, headersA, 20L));

            // Advance time to trigger suppression
            inputTopic.pipeInput(new TestRecord<>("C", 8, makeHeaders("key", "C"), 150L));
        }

        final List<MockApiProcessor<String, Integer, Void, Void>> processors = supplier.capturedProcessors(1);

        // Suppression should emit the last value for each key
        processors.get(0).checkAndClearProcessResultWithHeaders(
            new KeyValueTimestampHeaders<>("A", 6, 20, headersA),
            new KeyValueTimestampHeaders<>("B", 4, 15, headersB));
    }

    @Test
    public void shouldPreserveHeadersThroughRepartition() {
        final Properties props = getProps("headers");
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, Integer> table1 = builder.table(topic1, consumed);
        // GroupBy causes repartitioning
        final KTable<String, Long> table2 = table1
            .filter(predicate)
            .groupBy((key, value) -> KeyValue.pair(key.toUpperCase(Locale.ROOT), value))
            .count();

        final MockApiProcessorSupplier<String, Long, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        final Headers headersLowerA = makeHeaders("key", "lower-a");
        final Headers headersUpperA = makeHeaders("key", "upper-a");
        final Headers headersB = makeHeaders("key", "b");

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, Integer> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new IntegerSerializer());

            // Use both lowercase "a" and uppercase "A" - they both map to "A" via toUpperCase
            inputTopic.pipeInput(new TestRecord<>("a", 2, headersLowerA, 10L));
            inputTopic.pipeInput(new TestRecord<>("b", 4, headersB, 15L));
            inputTopic.pipeInput(new TestRecord<>("A", 6, headersUpperA, 20L));
        }

        final List<MockApiProcessor<String, Long, Void, Void>> processors = supplier.capturedProcessors(1);

        // After repartition and aggregation, headers should still be present
        // Both "a" and "A" map to "A" after toUpperCase, so count for A goes from 1 to 2
        processors.get(0).checkAndClearProcessResultWithHeaders(
            new KeyValueTimestampHeaders<>("A", 1L, 10, headersLowerA),
            new KeyValueTimestampHeaders<>("B", 1L, 15, headersB),
            new KeyValueTimestampHeaders<>("A", 2L, 20, headersUpperA));
    }

    @Test
    public void shouldRestoreHeadersFromChangelog() {
        final Properties props = getProps("headers");
        final String topic1 = "topic1";

        final Headers headersA = makeHeaders("key", "A");
        final Headers headersB = makeHeaders("key", "B");

        // First run: create data
        StreamsBuilder builder = new StreamsBuilder();
        builder.table(topic1, consumed).filter(predicate, Materialized.as("restore-store"));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, Integer> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new IntegerSerializer());

            inputTopic.pipeInput(new TestRecord<>("A", 2, headersA, 10L));
            inputTopic.pipeInput(new TestRecord<>("B", 4, headersB, 15L));
        }

        // Second run: restore from changelog and verify
        builder = new StreamsBuilder();
        final KTable<String, Integer> table2 = builder.table(topic1, consumed)
            .filter(predicate, Materialized.as("restore-store"));

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, Integer> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new IntegerSerializer());

            // Add new data to trigger processing
            inputTopic.pipeInput(new TestRecord<>("C", 6, makeHeaders("key", "C"), 20L));
        }

        final List<MockApiProcessor<String, Integer, Void, Void>> processors = supplier.capturedProcessors(1);

        processors.get(0).checkAndClearProcessResultWithHeaders(
            new KeyValueTimestampHeaders<>("C", 6, 20, makeHeaders("key", "C")));
    }

    @Test
    public void shouldQueryMaterializedStoreWithHeaders() {
        final Properties props = getProps("headers");
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, Integer> table2 = builder.table(topic1, consumed)
            .filter(predicate, Materialized.as("query-store"));

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        final Headers headersA = makeHeaders("key", "A");
        final Headers headersB = makeHeaders("key", "B");
        final Headers headersC = makeHeaders("key", "C");

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, Integer> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new IntegerSerializer());

            inputTopic.pipeInput(new TestRecord<>("A", 2, headersA, 10L));
            inputTopic.pipeInput(new TestRecord<>("B", 4, headersB, 15L));
            inputTopic.pipeInput(new TestRecord<>("C", 3, headersC, 20L));
        }

        final List<MockApiProcessor<String, Integer, Void, Void>> processors = supplier.capturedProcessors(1);

        // Verify that materialized store correctly filters and preserves headers
        // C is filtered out (odd number), producing a tombstone
        processors.get(0).checkAndClearProcessResultWithHeaders(
            new KeyValueTimestampHeaders<>("A", 2, 10, headersA),
            new KeyValueTimestampHeaders<>("B", 4, 15, headersB),
            new KeyValueTimestampHeaders<>("C", null, 20, headersC));
    }
}
