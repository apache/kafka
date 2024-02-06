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
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
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
import org.apache.kafka.test.MockApiProcessor;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

@SuppressWarnings("unchecked")
public class KTableMapValuesTest {
    private final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    private void doTestKTable(final StreamsBuilder builder,
                              final String topic1,
                              final MockApiProcessorSupplier<String, Integer, Void, Void> supplier) {
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                    driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            inputTopic1.pipeInput("A", "1", 5L);
            inputTopic1.pipeInput("B", "2", 25L);
            inputTopic1.pipeInput("C", "3", 20L);
            inputTopic1.pipeInput("D", "4", 10L);
            assertEquals(asList(new KeyValueTimestamp<>("A", 1, 5),
                    new KeyValueTimestamp<>("B", 2, 25),
                    new KeyValueTimestamp<>("C", 3, 20),
                    new KeyValueTimestamp<>("D", 4, 10)), supplier.theCapturedProcessor().processed());
        }
    }

    @Test
    public void testKTable() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, String> table1 = builder.table(topic1, consumed);
        final KTable<String, Integer> table2 = table1.mapValues(value -> value.charAt(0) - 48);

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table2.toStream().process(supplier);

        doTestKTable(builder, topic1, supplier);
    }

    @Test
    public void testQueryableKTable() {
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

        doTestKTable(builder, topic1, supplier);
    }

    private void doTestValueGetter(final StreamsBuilder builder,
                                   final String topic1,
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

    @Test
    public void testQueryableValueGetter() {
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

        doTestValueGetter(builder, topic1, table2, table3);
    }

    @Test
    public void testNotSendingOldValue() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        final KTableImpl<String, String, Integer> table2 =
            (KTableImpl<String, String, Integer>) table1.mapValues(s -> Integer.valueOf(s));

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final Topology topology = builder.build().addProcessor("proc", supplier, table2.name);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            final TestInputTopic<String, String> inputTopic1 =
                    driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<String, Integer, Void, Void> proc = supplier.theCapturedProcessor();

            assertFalse(table1.sendingOldValueEnabled());
            assertFalse(table2.sendingOldValueEnabled());

            inputTopic1.pipeInput("A", "01", 5L);
            inputTopic1.pipeInput("B", "01", 10L);
            inputTopic1.pipeInput("C", "01", 15L);
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(1, null), 5),
                    new KeyValueTimestamp<>("B", new Change<>(1, null), 10),
                    new KeyValueTimestamp<>("C", new Change<>(1, null), 15));

            inputTopic1.pipeInput("A", "02", 10L);
            inputTopic1.pipeInput("B", "02", 8L);
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(2, null), 10),
                    new KeyValueTimestamp<>("B", new Change<>(2, null), 8));

            inputTopic1.pipeInput("A", "03", 20L);
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(3, null), 20));

            inputTopic1.pipeInput("A", (String) null, 30L);
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(null, null), 30));
        }
    }

    @Test
    public void shouldEnableSendingOldValuesOnParentIfMapValuesNotMaterialized() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        final KTableImpl<String, String, Integer> table2 =
            (KTableImpl<String, String, Integer>) table1.mapValues(s -> Integer.valueOf(s));

        table2.enableSendingOldValues(true);

        assertThat(table1.sendingOldValueEnabled(), is(true));
        assertThat(table2.sendingOldValueEnabled(), is(true));

        testSendingOldValues(builder, topic1, table2);
    }

    @Test
    public void shouldNotEnableSendingOldValuesOnParentIfMapValuesMaterialized() {
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

        testSendingOldValues(builder, topic1, table2);
    }

    private void testSendingOldValues(
        final StreamsBuilder builder,
        final String topic1,
        final KTableImpl<String, String, Integer> table2
    ) {
        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        builder.build().addProcessor("proc", supplier, table2.name);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<String, Integer, Void, Void> proc = supplier.theCapturedProcessor();

            inputTopic1.pipeInput("A", "01", 5L);
            inputTopic1.pipeInput("B", "01", 10L);
            inputTopic1.pipeInput("C", "01", 15L);
            proc.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>(1, null), 5),
                new KeyValueTimestamp<>("B", new Change<>(1, null), 10),
                new KeyValueTimestamp<>("C", new Change<>(1, null), 15)
            );

            inputTopic1.pipeInput("A", "02", 10L);
            inputTopic1.pipeInput("B", "02", 8L);
            proc.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>(2, 1), 10),
                new KeyValueTimestamp<>("B", new Change<>(2, 1), 8)
            );

            inputTopic1.pipeInput("A", "03", 20L);
            proc.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>(3, 2), 20)
            );

            inputTopic1.pipeInput("A", (String) null, 30L);
            proc.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>(null, 3), 30)
            );
        }
    }
}
