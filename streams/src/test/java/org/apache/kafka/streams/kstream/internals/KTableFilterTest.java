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

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValueTimestamp;
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
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.test.MockApiProcessor;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@SuppressWarnings("unchecked")
public class KTableFilterTest {
    private final Consumed<String, Integer> consumed = Consumed.with(Serdes.String(), Serdes.Integer());
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.Integer());

    @Before
    public void setUp() {
        // disable caching at the config level
        props.setProperty(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0");
    }

    private final Predicate<String, Integer> predicate = (key, value) -> (value % 2) == 0;

    private void doTestKTable(final StreamsBuilder builder,
                              final KTable<String, Integer> table2,
                              final KTable<String, Integer> table3,
                              final String topic) {
        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);
        table3.toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, Integer> inputTopic =
                    driver.createInputTopic(topic, new StringSerializer(), new IntegerSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            inputTopic.pipeInput("A", 1, 10L);
            inputTopic.pipeInput("B", 2, 5L);
            inputTopic.pipeInput("C", 3, 8L);
            inputTopic.pipeInput("D", 4, 14L);
            inputTopic.pipeInput("A", null, 18L);
            inputTopic.pipeInput("B", null, 15L);
        }

        final List<MockApiProcessor<String, Integer, Void, Void>> processors = supplier.capturedProcessors(2);

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
    }

    @Test
    public void shouldPassThroughWithoutMaterialization() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, Integer> table1 = builder.table(topic1, consumed);
        final KTable<String, Integer> table2 = table1.filter(predicate);
        final KTable<String, Integer> table3 = table1.filterNot(predicate);

        assertNull(table1.queryableStoreName());
        assertNull(table2.queryableStoreName());
        assertNull(table3.queryableStoreName());

        doTestKTable(builder, table2, table3, topic1);
    }

    @Test
    public void shouldPassThroughOnMaterialization() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, Integer> table1 = builder.table(topic1, consumed);
        final KTable<String, Integer> table2 = table1.filter(predicate, Materialized.as("store2"));
        final KTable<String, Integer> table3 = table1.filterNot(predicate);

        assertNull(table1.queryableStoreName());
        assertEquals("store2", table2.queryableStoreName());
        assertNull(table3.queryableStoreName());

        doTestKTable(builder, table2, table3, topic1);
    }

    private void doTestValueGetter(final StreamsBuilder builder,
                                   final KTableImpl<String, Integer, Integer> table2,
                                   final KTableImpl<String, Integer, Integer> table3,
                                   final String topic1) {

        final Topology topology = builder.build();

        final KTableValueGetterSupplier<String, Integer> getterSupplier2 = table2.valueGetterSupplier();
        final KTableValueGetterSupplier<String, Integer> getterSupplier3 = table3.valueGetterSupplier();

        final InternalTopologyBuilder topologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);
        topologyBuilder.connectProcessorAndStateStores(table2.name, getterSupplier2.storeNames());
        topologyBuilder.connectProcessorAndStateStores(table3.name, getterSupplier3.storeNames());

        try (final TopologyTestDriverWrapper driver = new TopologyTestDriverWrapper(topology, props)) {
            final TestInputTopic<String, Integer> inputTopic =
                    driver.createInputTopic(topic1, new StringSerializer(), new IntegerSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

            final KTableValueGetter<String, Integer> getter2 = getterSupplier2.get();
            final KTableValueGetter<String, Integer> getter3 = getterSupplier3.get();

            getter2.init(driver.setCurrentNodeForProcessorContext(table2.name));
            getter3.init(driver.setCurrentNodeForProcessorContext(table3.name));

            inputTopic.pipeInput("A", 1, 5L);
            inputTopic.pipeInput("B", 1, 10L);
            inputTopic.pipeInput("C", 1, 15L);

            assertNull(getter2.get("A"));
            assertNull(getter2.get("B"));
            assertNull(getter2.get("C"));

            assertEquals(ValueAndTimestamp.make(1, 5L), getter3.get("A"));
            assertEquals(ValueAndTimestamp.make(1, 10L), getter3.get("B"));
            assertEquals(ValueAndTimestamp.make(1, 15L), getter3.get("C"));

            inputTopic.pipeInput("A", 2, 10L);
            inputTopic.pipeInput("B", 2, 5L);

            assertEquals(ValueAndTimestamp.make(2, 10L), getter2.get("A"));
            assertEquals(ValueAndTimestamp.make(2, 5L), getter2.get("B"));
            assertNull(getter2.get("C"));

            assertNull(getter3.get("A"));
            assertNull(getter3.get("B"));
            assertEquals(ValueAndTimestamp.make(1, 15L), getter3.get("C"));

            inputTopic.pipeInput("A", 3, 15L);

            assertNull(getter2.get("A"));
            assertEquals(ValueAndTimestamp.make(2, 5L), getter2.get("B"));
            assertNull(getter2.get("C"));

            assertEquals(ValueAndTimestamp.make(3, 15L), getter3.get("A"));
            assertNull(getter3.get("B"));
            assertEquals(ValueAndTimestamp.make(1, 15L), getter3.get("C"));

            inputTopic.pipeInput("A", null, 10L);
            inputTopic.pipeInput("B", null, 20L);

            assertNull(getter2.get("A"));
            assertNull(getter2.get("B"));
            assertNull(getter2.get("C"));

            assertNull(getter3.get("A"));
            assertNull(getter3.get("B"));
            assertEquals(ValueAndTimestamp.make(1, 15L), getter3.get("C"));
        }
    }

    @Test
    public void shouldGetValuesOnMaterialization() {
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

        doTestValueGetter(builder, table2, table3, topic1);
    }

    private void doTestNotSendingOldValue(final StreamsBuilder builder,
                                          final KTableImpl<String, Integer, Integer> table1,
                                          final KTableImpl<String, Integer, Integer> table2,
                                          final String topic1) {
        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();

        builder.build().addProcessor("proc1", supplier, table1.name);
        builder.build().addProcessor("proc2", supplier, table2.name);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, Integer> inputTopic =
                    driver.createInputTopic(topic1, new StringSerializer(), new IntegerSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

            inputTopic.pipeInput("A", 1, 5L);
            inputTopic.pipeInput("B", 1, 10L);
            inputTopic.pipeInput("C", 1, 15L);

            final List<MockApiProcessor<String, Integer, Void, Void>> processors = supplier.capturedProcessors(2);

            processors.get(0).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(1, null), 5),
                new KeyValueTimestamp<>("B", new Change<>(1, null), 10),
                new KeyValueTimestamp<>("C", new Change<>(1, null), 15));
            processors.get(1).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(null, null), 5),
                new KeyValueTimestamp<>("B", new Change<>(null, null), 10),
                new KeyValueTimestamp<>("C", new Change<>(null, null), 15));

            inputTopic.pipeInput("A", 2, 15L);
            inputTopic.pipeInput("B", 2, 8L);

            processors.get(0).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(2, null), 15),
                new KeyValueTimestamp<>("B", new Change<>(2, null), 8));
            processors.get(1).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(2, null), 15),
                new KeyValueTimestamp<>("B", new Change<>(2, null), 8));

            inputTopic.pipeInput("A", 3, 20L);

            processors.get(0).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(3, null), 20));
            processors.get(1).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(null, null), 20));
            inputTopic.pipeInput("A", null, 10L);
            inputTopic.pipeInput("B", null, 20L);

            processors.get(0).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(null, null), 10),
                new KeyValueTimestamp<>("B", new Change<>(null, null), 20));
            processors.get(1).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(null, null), 10),
                new KeyValueTimestamp<>("B", new Change<>(null, null), 20));
        }
    }


    @Test
    public void shouldNotSendOldValuesWithoutMaterialization() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTableImpl<String, Integer, Integer> table1 =
            (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed);
        final KTableImpl<String, Integer, Integer> table2 = (KTableImpl<String, Integer, Integer>) table1.filter(predicate);

        doTestNotSendingOldValue(builder, table1, table2, topic1);
    }

    @Test
    public void shouldNotSendOldValuesOnMaterialization() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTableImpl<String, Integer, Integer> table1 =
            (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed);
        final KTableImpl<String, Integer, Integer> table2 =
            (KTableImpl<String, Integer, Integer>) table1.filter(predicate, Materialized.as("store2"));

        doTestNotSendingOldValue(builder, table1, table2, topic1);
    }

    @Test
    public void shouldNotEnableSendingOldValuesIfNotAlreadyMaterializedAndNotForcedToMaterialize() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTableImpl<String, Integer, Integer> table1 =
            (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed);
        final KTableImpl<String, Integer, Integer> table2 = (KTableImpl<String, Integer, Integer>) table1.filter(predicate);

        table2.enableSendingOldValues(false);

        doTestNotSendingOldValue(builder, table1, table2, topic1);
    }

    private void doTestSendingOldValue(final StreamsBuilder builder,
                                       final KTableImpl<String, Integer, Integer> table1,
                                       final KTableImpl<String, Integer, Integer> table2,
                                       final String topic1) {
        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final Topology topology = builder.build();

        topology.addProcessor("proc1", supplier, table1.name);
        topology.addProcessor("proc2", supplier, table2.name);

        final boolean parentSendOldVals = table1.sendingOldValueEnabled();

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            final TestInputTopic<String, Integer> inputTopic =
                    driver.createInputTopic(topic1, new StringSerializer(), new IntegerSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

            inputTopic.pipeInput("A", 1, 5L);
            inputTopic.pipeInput("B", 1, 10L);
            inputTopic.pipeInput("C", 1, 15L);

            final List<MockApiProcessor<String, Integer, Void, Void>> processors = supplier.capturedProcessors(2);
            final MockApiProcessor<String, Integer, Void, Void> table1Output = processors.get(0);
            final MockApiProcessor<String, Integer, Void, Void> table2Output = processors.get(1);

            table1Output.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>(1, null), 5),
                new KeyValueTimestamp<>("B", new Change<>(1, null), 10),
                new KeyValueTimestamp<>("C", new Change<>(1, null), 15)
            );
            table2Output.checkEmptyAndClearProcessResult();

            inputTopic.pipeInput("A", 2, 15L);
            inputTopic.pipeInput("B", 2, 8L);

            table1Output.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>(2, parentSendOldVals ? 1 : null), 15),
                new KeyValueTimestamp<>("B", new Change<>(2, parentSendOldVals ? 1 : null), 8)
            );
            table2Output.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>(2, null), 15),
                new KeyValueTimestamp<>("B", new Change<>(2, null), 8)
            );

            inputTopic.pipeInput("A", 3, 20L);

            table1Output.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>(3, parentSendOldVals ? 2 : null), 20)
            );
            table2Output.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>(null, 2), 20)
            );

            inputTopic.pipeInput("A", null, 10L);
            inputTopic.pipeInput("B", null, 20L);

            table1Output.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>(null, parentSendOldVals ? 3 : null), 10),
                new KeyValueTimestamp<>("B", new Change<>(null, parentSendOldVals ? 2 : null), 20)
            );
            table2Output.checkAndClearProcessResult(
                new KeyValueTimestamp<>("B", new Change<>(null, 2), 20)
            );
        }
    }

    @Test
    public void shouldEnableSendOldValuesWhenNotMaterializedAlreadyButForcedToMaterialize() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTableImpl<String, Integer, Integer> table1 =
            (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed);
        final KTableImpl<String, Integer, Integer> table2 =
            (KTableImpl<String, Integer, Integer>) table1.filter(predicate);

        table2.enableSendingOldValues(true);

        assertThat(table1.sendingOldValueEnabled(), is(true));
        assertThat(table2.sendingOldValueEnabled(), is(true));

        doTestSendingOldValue(builder, table1, table2, topic1);
    }

    @Test
    public void shouldEnableSendOldValuesWhenMaterializedAlreadyAndForcedToMaterialize() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTableImpl<String, Integer, Integer> table1 =
            (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed);
        final KTableImpl<String, Integer, Integer> table2 =
            (KTableImpl<String, Integer, Integer>) table1.filter(predicate, Materialized.as("store2"));

        table2.enableSendingOldValues(true);

        assertThat(table1.sendingOldValueEnabled(), is(false));
        assertThat(table2.sendingOldValueEnabled(), is(true));

        doTestSendingOldValue(builder, table1, table2, topic1);
    }

    @Test
    public void shouldSendOldValuesWhenEnabledOnUpStreamMaterialization() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTableImpl<String, Integer, Integer> table1 =
            (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed, Materialized.as("store2"));
        final KTableImpl<String, Integer, Integer> table2 =
            (KTableImpl<String, Integer, Integer>) table1.filter(predicate);

        table2.enableSendingOldValues(false);

        assertThat(table1.sendingOldValueEnabled(), is(true));
        assertThat(table2.sendingOldValueEnabled(), is(true));

        doTestSendingOldValue(builder, table1, table2, topic1);
    }

    private void doTestSkipNullOnMaterialization(final StreamsBuilder builder,
                                                 final KTableImpl<String, String, String> table1,
                                                 final KTableImpl<String, String, String> table2,
                                                 final String topic1,
                                                 final boolean shouldSkip) {
        final MockApiProcessorSupplier<String, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final Topology topology = builder.build();

        topology.addProcessor("proc1", supplier, table1.name);
        topology.addProcessor("proc2", supplier, table2.name);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            final TestInputTopic<String, String> stringinputTopic =
                    driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

            stringinputTopic.pipeInput("A", "reject", 5L);
            stringinputTopic.pipeInput("B", "reject", 10L);
            stringinputTopic.pipeInput("C", "reject", 20L);
        }

        final List<MockApiProcessor<String, String, Void, Void>> processors = supplier.capturedProcessors(2);
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
    }

    @Test
    public void shouldSkipNullToRepartitionWithoutMaterialization() {
        // Do not explicitly set enableSendingOldValues. Let a further downstream stateful operator trigger it instead.
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";

        final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        final KTableImpl<String, String, String> table2 =
            (KTableImpl<String, String, String>) table1.filter((key, value) -> value.equalsIgnoreCase("accept"));
        table2.groupBy(MockMapper.noOpKeyValueMapper())
            .reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER);

        doTestSkipNullOnMaterialization(builder, table1, table2, topic1, true);
    }

    @Test
    public void shouldSkipNullToRepartitionOnMaterialization() {
        // Do not explicitly set enableSendingOldValues. Let a further downstream stateful operator trigger it instead.
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";

        final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        final KTableImpl<String, String, String> table2 =
            (KTableImpl<String, String, String>) table1.filter((key, value) -> value.equalsIgnoreCase("accept"), Materialized.as("store2"));
        table2.groupBy(MockMapper.noOpKeyValueMapper())
            .reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER, Materialized.as("mock-result"));

        doTestSkipNullOnMaterialization(builder, table1, table2, topic1, true);
    }

    @Test
    public void shouldNotSkipNullIfVersionedUpstream() {
        // stateful downstream operation enables sendOldValues, but duplicate nulls will still
        // be sent because the source table is versioned
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

        doTestSkipNullOnMaterialization(builder, table1, table2, topic1, false);
    }

    @Test
    public void shouldSkipNullIfVersionedDownstream() {
        // materializing the result of the filter as a versioned store does not prevent duplicate
        // tombstones from being sent, as it's whether the input table is versioned or not that
        // determines whether the optimization is enabled
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

        doTestSkipNullOnMaterialization(builder, table1, table2, topic1, true);
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
}
