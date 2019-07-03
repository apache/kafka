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
import org.apache.kafka.streams.StreamsBuilder;
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
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.Properties;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("unchecked")
public class KTableMapValuesTest {
    private final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
    private final ConsumerRecordFactory<String, String> recordFactory =
        new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer(), 0L);
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    private void doTestKTable(final StreamsBuilder builder,
                              final String topic1,
                              final MockProcessorSupplier<String, Integer> supplier) {
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(topic1, "A", "1", 5L));
            driver.pipeInput(recordFactory.create(topic1, "B", "2", 25L));
            driver.pipeInput(recordFactory.create(topic1, "C", "3", 20L));
            driver.pipeInput(recordFactory.create(topic1, "D", "4", 10L));
            assertEquals(asList("A:1 (ts: 5)", "B:2 (ts: 25)", "C:3 (ts: 20)", "D:4 (ts: 10)"), supplier.theCapturedProcessor().processed);
        }
    }

    @Test
    public void testKTable() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, String> table1 = builder.table(topic1, consumed);
        final KTable<String, Integer> table2 = table1.mapValues(value -> value.charAt(0) - 48);

        final MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();
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

        final MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();
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
            final KTableValueGetter<String, Integer> getter2 = getterSupplier2.get();
            final KTableValueGetter<String, Integer> getter3 = getterSupplier3.get();

            getter2.init(driver.setCurrentNodeForProcessorContext(table2.name));
            getter3.init(driver.setCurrentNodeForProcessorContext(table3.name));

            driver.pipeInput(recordFactory.create(topic1, "A", "01", 50L));
            driver.pipeInput(recordFactory.create(topic1, "B", "01", 10L));
            driver.pipeInput(recordFactory.create(topic1, "C", "01", 30L));

            assertEquals(ValueAndTimestamp.make(1, 50L), getter2.get("A"));
            assertEquals(ValueAndTimestamp.make(1, 10L), getter2.get("B"));
            assertEquals(ValueAndTimestamp.make(1, 30L), getter2.get("C"));

            assertEquals(ValueAndTimestamp.make(-1, 50L), getter3.get("A"));
            assertEquals(ValueAndTimestamp.make(-1, 10L), getter3.get("B"));
            assertEquals(ValueAndTimestamp.make(-1, 30L), getter3.get("C"));

            driver.pipeInput(recordFactory.create(topic1, "A", "02", 25L));
            driver.pipeInput(recordFactory.create(topic1, "B", "02", 20L));

            assertEquals(ValueAndTimestamp.make(2, 25L), getter2.get("A"));
            assertEquals(ValueAndTimestamp.make(2, 20L), getter2.get("B"));
            assertEquals(ValueAndTimestamp.make(1, 30L), getter2.get("C"));

            assertEquals(ValueAndTimestamp.make(-2, 25L), getter3.get("A"));
            assertEquals(ValueAndTimestamp.make(-2, 20L), getter3.get("B"));
            assertEquals(ValueAndTimestamp.make(-1, 30L), getter3.get("C"));

            driver.pipeInput(recordFactory.create(topic1, "A", "03", 35L));

            assertEquals(ValueAndTimestamp.make(3, 35L), getter2.get("A"));
            assertEquals(ValueAndTimestamp.make(2, 20L), getter2.get("B"));
            assertEquals(ValueAndTimestamp.make(1, 30L), getter2.get("C"));

            assertEquals(ValueAndTimestamp.make(-3, 35L), getter3.get("A"));
            assertEquals(ValueAndTimestamp.make(-2, 20L), getter3.get("B"));
            assertEquals(ValueAndTimestamp.make(-1, 30L), getter3.get("C"));

            driver.pipeInput(recordFactory.create(topic1, "A", (String) null, 1L));

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
                Integer::new,
                Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as(storeName2)
                    .withValueSerde(Serdes.Integer()));
        final KTableImpl<String, String, Integer> table3 =
            (KTableImpl<String, String, Integer>) table1.mapValues(
                value -> new Integer(value) * (-1),
                Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as(storeName3)
                    .withValueSerde(Serdes.Integer()));
        final KTableImpl<String, String, Integer> table4 =
            (KTableImpl<String, String, Integer>) table1.mapValues(Integer::new);

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
            (KTableImpl<String, String, Integer>) table1.mapValues(Integer::new);

        final MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();
        final Topology topology = builder.build().addProcessor("proc", supplier, table2.name);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            final MockProcessor<String, Integer> proc = supplier.theCapturedProcessor();

            assertFalse(table1.sendingOldValueEnabled());
            assertFalse(table2.sendingOldValueEnabled());

            driver.pipeInput(recordFactory.create(topic1, "A", "01", 5L));
            driver.pipeInput(recordFactory.create(topic1, "B", "01", 10L));
            driver.pipeInput(recordFactory.create(topic1, "C", "01", 15L));
            proc.checkAndClearProcessResult("A:(1<-null) (ts: 5)", "B:(1<-null) (ts: 10)", "C:(1<-null) (ts: 15)");

            driver.pipeInput(recordFactory.create(topic1, "A", "02", 10L));
            driver.pipeInput(recordFactory.create(topic1, "B", "02", 8L));
            proc.checkAndClearProcessResult("A:(2<-null) (ts: 10)", "B:(2<-null) (ts: 8)");

            driver.pipeInput(recordFactory.create(topic1, "A", "03", 20L));
            proc.checkAndClearProcessResult("A:(3<-null) (ts: 20)");

            driver.pipeInput(recordFactory.create(topic1, "A", (String) null, 30L));
            proc.checkAndClearProcessResult("A:(null<-null) (ts: 30)");
        }
    }

    @Test
    public void testSendingOldValue() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        final KTableImpl<String, String, Integer> table2 =
            (KTableImpl<String, String, Integer>) table1.mapValues(Integer::new);
        table2.enableSendingOldValues();

        final MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();
        builder.build().addProcessor("proc", supplier, table2.name);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final MockProcessor<String, Integer> proc = supplier.theCapturedProcessor();

            assertTrue(table1.sendingOldValueEnabled());
            assertTrue(table2.sendingOldValueEnabled());

            driver.pipeInput(recordFactory.create(topic1, "A", "01", 5L));
            driver.pipeInput(recordFactory.create(topic1, "B", "01", 10L));
            driver.pipeInput(recordFactory.create(topic1, "C", "01", 15L));
            proc.checkAndClearProcessResult("A:(1<-null) (ts: 5)", "B:(1<-null) (ts: 10)", "C:(1<-null) (ts: 15)");

            driver.pipeInput(recordFactory.create(topic1, "A", "02", 10L));
            driver.pipeInput(recordFactory.create(topic1, "B", "02", 8L));
            proc.checkAndClearProcessResult("A:(2<-1) (ts: 10)", "B:(2<-1) (ts: 8)");

            driver.pipeInput(recordFactory.create(topic1, "A", "03", 20L));
            proc.checkAndClearProcessResult("A:(3<-2) (ts: 20)");

            driver.pipeInput(recordFactory.create(topic1, "A", (String) null, 30L));
            proc.checkAndClearProcessResult("A:(null<-3) (ts: 30)");
        }
    }
}
