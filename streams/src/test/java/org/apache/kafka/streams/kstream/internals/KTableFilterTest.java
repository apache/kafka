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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyTestDriverWrapper;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@SuppressWarnings("unchecked")
public class KTableFilterTest {

    private final Consumed<String, Integer> consumed = Consumed.with(Serdes.String(), Serdes.Integer());
    private final ConsumerRecordFactory<String, Integer> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new IntegerSerializer());
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.Integer());

    private final Predicate<String, Integer> predicate = (key, value) -> (value % 2) == 0;

    private void doTestKTable(final StreamsBuilder builder,
                              final KTable<String, Integer> table2,
                              final KTable<String, Integer> table3,
                              final String topic) {
        final MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();
        table2.toStream().process(supplier);
        table3.toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(topic, "A", 1));
            driver.pipeInput(recordFactory.create(topic, "B", 2));
            driver.pipeInput(recordFactory.create(topic, "C", 3));
            driver.pipeInput(recordFactory.create(topic, "D", 4));
            driver.pipeInput(recordFactory.create(topic, "A", null));
            driver.pipeInput(recordFactory.create(topic, "B", null));
        }

        final List<MockProcessor<String, Integer>> processors = supplier.capturedProcessors(2);

        processors.get(0).checkAndClearProcessResult("A:null", "B:2", "C:null", "D:4", "A:null", "B:null");
        processors.get(1).checkAndClearProcessResult("A:1", "B:null", "C:3", "D:null", "A:null", "B:null");
    }

    @Test
    public void testKTable() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";

        final KTable<String, Integer> table1 = builder.table(topic1, consumed);

        final KTable<String, Integer> table2 = table1.filter(predicate);
        final KTable<String, Integer> table3 = table1.filterNot(predicate);

        doTestKTable(builder, table2, table3, topic1);
    }

    @Test
    public void testQueryableKTable() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";

        final KTable<String, Integer> table1 = builder.table(topic1, consumed);

        final KTable<String, Integer> table2 = table1.filter(predicate, Materialized.as("anyStoreNameFilter"));
        final KTable<String, Integer> table3 = table1.filterNot(predicate);

        assertEquals("anyStoreNameFilter", table2.queryableStoreName());
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

            final KTableValueGetter<String, Integer> getter2 = getterSupplier2.get();
            final KTableValueGetter<String, Integer> getter3 = getterSupplier3.get();

            getter2.init(driver.setCurrentNodeForProcessorContext(table2.name));
            getter3.init(driver.setCurrentNodeForProcessorContext(table3.name));

            driver.pipeInput(recordFactory.create(topic1, "A", 1));
            driver.pipeInput(recordFactory.create(topic1, "B", 1));
            driver.pipeInput(recordFactory.create(topic1, "C", 1));

            assertNull(getter2.get("A"));
            assertNull(getter2.get("B"));
            assertNull(getter2.get("C"));

            assertEquals(1, (int) getter3.get("A").value());
            assertEquals(1, (int) getter3.get("B").value());
            assertEquals(1, (int) getter3.get("C").value());

            driver.pipeInput(recordFactory.create(topic1, "A", 2));
            driver.pipeInput(recordFactory.create(topic1, "B", 2));

            assertEquals(2, (int) getter2.get("A").value());
            assertEquals(2, (int) getter2.get("B").value());
            assertNull(getter2.get("C"));

            assertNull(getter3.get("A"));
            assertNull(getter3.get("B"));
            assertEquals(1, (int) getter3.get("C").value());

            driver.pipeInput(recordFactory.create(topic1, "A", 3));

            assertNull(getter2.get("A"));
            assertEquals(2, (int) getter2.get("B").value());
            assertNull(getter2.get("C"));

            assertEquals(3, (int) getter3.get("A").value());
            assertNull(getter3.get("B"));
            assertEquals(1, (int) getter3.get("C").value());

            driver.pipeInput(recordFactory.create(topic1, "A", null));
            driver.pipeInput(recordFactory.create(topic1, "B", null));

            assertNull(getter2.get("A"));
            assertNull(getter2.get("B"));
            assertNull(getter2.get("C"));

            assertNull(getter3.get("A"));
            assertNull(getter3.get("B"));
            assertEquals(1, (int) getter3.get("C").value());
        }
    }

    @Test
    public void testQueryableValueGetter() {
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

        assertEquals("store2", table2.queryableStoreName());
        assertEquals("store3", table3.queryableStoreName());
        assertNull(table4.queryableStoreName());

        doTestValueGetter(builder, table2, table3, topic1);
    }

    private void doTestNotSendingOldValue(final StreamsBuilder builder,
                                          final KTableImpl<String, Integer, Integer> table1,
                                          final KTableImpl<String, Integer, Integer> table2,
                                          final String topic1) {
        final MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();

        builder.build().addProcessor("proc1", supplier, table1.name);
        builder.build().addProcessor("proc2", supplier, table2.name);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {

            driver.pipeInput(recordFactory.create(topic1, "A", 1));
            driver.pipeInput(recordFactory.create(topic1, "B", 1));
            driver.pipeInput(recordFactory.create(topic1, "C", 1));

            final List<MockProcessor<String, Integer>> processors = supplier.capturedProcessors(2);

            processors.get(0).checkAndClearProcessResult("A:(1<-null)", "B:(1<-null)", "C:(1<-null)");
            processors.get(1).checkAndClearProcessResult("A:(null<-null)", "B:(null<-null)", "C:(null<-null)");

            driver.pipeInput(recordFactory.create(topic1, "A", 2));
            driver.pipeInput(recordFactory.create(topic1, "B", 2));

            processors.get(0).checkAndClearProcessResult("A:(2<-null)", "B:(2<-null)");
            processors.get(1).checkAndClearProcessResult("A:(2<-null)", "B:(2<-null)");

            driver.pipeInput(recordFactory.create(topic1, "A", 3));

            processors.get(0).checkAndClearProcessResult("A:(3<-null)");
            processors.get(1).checkAndClearProcessResult("A:(null<-null)");

            driver.pipeInput(recordFactory.create(topic1, "A", null));
            driver.pipeInput(recordFactory.create(topic1, "B", null));

            processors.get(0).checkAndClearProcessResult("A:(null<-null)", "B:(null<-null)");
            processors.get(1).checkAndClearProcessResult("A:(null<-null)", "B:(null<-null)");
        }
    }


    @Test
    public void testNotSendingOldValue() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";

        final KTableImpl<String, Integer, Integer> table1 =
                (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed);
        final KTableImpl<String, Integer, Integer> table2 = (KTableImpl<String, Integer, Integer>) table1.filter(predicate);

        doTestNotSendingOldValue(builder, table1, table2, topic1);
    }

    @Test
    public void testQueryableNotSendingOldValue() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";

        final KTableImpl<String, Integer, Integer> table1 =
            (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed);
        final KTableImpl<String, Integer, Integer> table2 =
            (KTableImpl<String, Integer, Integer>) table1.filter(predicate, Materialized.as("anyStoreNameFilter"));

        doTestNotSendingOldValue(builder, table1, table2, topic1);
    }

    private void doTestSendingOldValue(final StreamsBuilder builder,
                                       final KTableImpl<String, Integer, Integer> table1,
                                       final KTableImpl<String, Integer, Integer> table2,
                                       final String topic1) {
        table2.enableSendingOldValues();

        final MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();
        final Topology topology = builder.build();

        topology.addProcessor("proc1", supplier, table1.name);
        topology.addProcessor("proc2", supplier, table2.name);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {

            driver.pipeInput(recordFactory.create(topic1, "A", 1));
            driver.pipeInput(recordFactory.create(topic1, "B", 1));
            driver.pipeInput(recordFactory.create(topic1, "C", 1));

            final List<MockProcessor<String, Integer>> processors = supplier.capturedProcessors(2);

            processors.get(0).checkAndClearProcessResult("A:(1<-null)", "B:(1<-null)", "C:(1<-null)");
            processors.get(1).checkEmptyAndClearProcessResult();

            driver.pipeInput(recordFactory.create(topic1, "A", 2));
            driver.pipeInput(recordFactory.create(topic1, "B", 2));

            processors.get(0).checkAndClearProcessResult("A:(2<-1)", "B:(2<-1)");
            processors.get(1).checkAndClearProcessResult("A:(2<-null)", "B:(2<-null)");

            driver.pipeInput(recordFactory.create(topic1, "A", 3));

            processors.get(0).checkAndClearProcessResult("A:(3<-2)");
            processors.get(1).checkAndClearProcessResult("A:(null<-2)");

            driver.pipeInput(recordFactory.create(topic1, "A", null));
            driver.pipeInput(recordFactory.create(topic1, "B", null));

            processors.get(0).checkAndClearProcessResult("A:(null<-3)", "B:(null<-2)");
            processors.get(1).checkAndClearProcessResult("B:(null<-2)");
        }
    }

    @Test
    public void testSendingOldValue() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";

        final KTableImpl<String, Integer, Integer> table1 =
                (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed);
        final KTableImpl<String, Integer, Integer> table2 =
            (KTableImpl<String, Integer, Integer>) table1.filter(predicate);

        doTestSendingOldValue(builder, table1, table2, topic1);
    }

    @Test
    public void testQueryableSendingOldValue() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";

        final KTableImpl<String, Integer, Integer> table1 =
            (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed);
        final KTableImpl<String, Integer, Integer> table2 =
            (KTableImpl<String, Integer, Integer>) table1.filter(predicate, Materialized.as("anyStoreNameFilter"));

        doTestSendingOldValue(builder, table1, table2, topic1);
    }

    private void doTestSkipNullOnMaterialization(final StreamsBuilder builder,
                                                 final KTableImpl<String, String, String> table1,
                                                 final KTableImpl<String, String, String> table2,
                                                 final String topic1) {
        final MockProcessorSupplier<String, String> supplier = new MockProcessorSupplier<>();
        final Topology topology = builder.build();

        topology.addProcessor("proc1", supplier, table1.name);
        topology.addProcessor("proc2", supplier, table2.name);

        final ConsumerRecordFactory<String, String> stringRecordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {

            driver.pipeInput(stringRecordFactory.create(topic1, "A", "reject"));
            driver.pipeInput(stringRecordFactory.create(topic1, "B", "reject"));
            driver.pipeInput(stringRecordFactory.create(topic1, "C", "reject"));
        }

        final List<MockProcessor<String, String>> processors = supplier.capturedProcessors(2);
        processors.get(0).checkAndClearProcessResult("A:(reject<-null)", "B:(reject<-null)", "C:(reject<-null)");
        processors.get(1).checkEmptyAndClearProcessResult();
    }

    @Test
    public void testSkipNullOnMaterialization() {
        // Do not explicitly set enableSendingOldValues. Let a further downstream stateful operator trigger it instead.
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";

        final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        final KTableImpl<String, String, String> table2 =
            (KTableImpl<String, String, String>) table1.filter((key, value) -> value.equalsIgnoreCase("accept"))
                .groupBy(MockMapper.noOpKeyValueMapper())
                .reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER);

        doTestSkipNullOnMaterialization(builder, table1, table2, topic1);
    }

    @Test
    public void testQueryableSkipNullOnMaterialization() {
        // Do not explicitly set enableSendingOldValues. Let a further downstream stateful operator trigger it instead.
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";

        final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        final KTableImpl<String, String, String> table2 =
            (KTableImpl<String, String, String>) table1.filter((key, value) -> value.equalsIgnoreCase("accept"), Materialized.as("anyStoreNameFilter"))
                .groupBy(MockMapper.noOpKeyValueMapper())
                .reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER, Materialized.as("mock-result"));

        doTestSkipNullOnMaterialization(builder, table1, table2, topic1);
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
