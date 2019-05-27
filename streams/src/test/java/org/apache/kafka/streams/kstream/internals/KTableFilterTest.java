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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyTestDriverWrapper;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@SuppressWarnings("unchecked")
public class KTableFilterTest {
    private final Consumed<String, Integer> consumed = Consumed.with(Serdes.String(), Serdes.Integer());
    private final ConsumerRecordFactory<String, Integer> recordFactory =
        new ConsumerRecordFactory<>(new StringSerializer(), new IntegerSerializer(), 0L);
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.Integer());

    @Before
    public void setUp() {
        // disable caching at the config level
        props.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
    }

    private final Predicate<String, Integer> predicate = (key, value) -> (value % 2) == 0;

    private void doTestKTable(final StreamsBuilder builder,
                              final KTable<String, Integer> table2,
                              final KTable<String, Integer> table3,
                              final String topic) {
        final MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();
        table2.toStream().process(supplier);
        table3.toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(topic, "A", 1, 10L));
            driver.pipeInput(recordFactory.create(topic, "B", 2, 5L));
            driver.pipeInput(recordFactory.create(topic, "C", 3, 8L));
            driver.pipeInput(recordFactory.create(topic, "D", 4, 14L));
            driver.pipeInput(recordFactory.create(topic, "A", null, 18L));
            driver.pipeInput(recordFactory.create(topic, "B", null, 15L));
        }

        final List<MockProcessor<String, Integer>> processors = supplier.capturedProcessors(2);

        processors.get(0).checkAndClearProcessResult("A:null (ts: 10)", "B:2 (ts: 5)", "C:null (ts: 8)", "D:4 (ts: 14)", "A:null (ts: 18)", "B:null (ts: 15)");
        processors.get(1).checkAndClearProcessResult("A:1 (ts: 10)", "B:null (ts: 5)", "C:3 (ts: 8)", "D:null (ts: 14)", "A:null (ts: 18)", "B:null (ts: 15)");
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
            final KTableValueGetter<String, Integer> getter2 = getterSupplier2.get();
            final KTableValueGetter<String, Integer> getter3 = getterSupplier3.get();

            getter2.init(driver.setCurrentNodeForProcessorContext(table2.name));
            getter3.init(driver.setCurrentNodeForProcessorContext(table3.name));

            driver.pipeInput(recordFactory.create(topic1, "A", 1, 5L));
            driver.pipeInput(recordFactory.create(topic1, "B", 1, 10L));
            driver.pipeInput(recordFactory.create(topic1, "C", 1, 15L));

            assertNull(getter2.get("A"));
            assertNull(getter2.get("B"));
            assertNull(getter2.get("C"));

            assertEquals(ValueAndTimestamp.make(1, 5L), getter3.get("A"));
            assertEquals(ValueAndTimestamp.make(1, 10L), getter3.get("B"));
            assertEquals(ValueAndTimestamp.make(1, 15L), getter3.get("C"));

            driver.pipeInput(recordFactory.create(topic1, "A", 2, 10L));
            driver.pipeInput(recordFactory.create(topic1, "B", 2, 5L));

            assertEquals(ValueAndTimestamp.make(2, 10L), getter2.get("A"));
            assertEquals(ValueAndTimestamp.make(2, 5L), getter2.get("B"));
            assertNull(getter2.get("C"));

            assertNull(getter3.get("A"));
            assertNull(getter3.get("B"));
            assertEquals(ValueAndTimestamp.make(1, 15L), getter3.get("C"));

            driver.pipeInput(recordFactory.create(topic1, "A", 3, 15L));

            assertNull(getter2.get("A"));
            assertEquals(ValueAndTimestamp.make(2, 5L), getter2.get("B"));
            assertNull(getter2.get("C"));

            assertEquals(ValueAndTimestamp.make(3, 15L), getter3.get("A"));
            assertNull(getter3.get("B"));
            assertEquals(ValueAndTimestamp.make(1, 15L), getter3.get("C"));

            driver.pipeInput(recordFactory.create(topic1, "A", null, 10L));
            driver.pipeInput(recordFactory.create(topic1, "B", null, 20L));

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
        final MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();

        builder.build().addProcessor("proc1", supplier, table1.name);
        builder.build().addProcessor("proc2", supplier, table2.name);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {

            driver.pipeInput(recordFactory.create(topic1, "A", 1, 5L));
            driver.pipeInput(recordFactory.create(topic1, "B", 1, 10L));
            driver.pipeInput(recordFactory.create(topic1, "C", 1, 15L));

            final List<MockProcessor<String, Integer>> processors = supplier.capturedProcessors(2);

            processors.get(0).checkAndClearProcessResult("A:(1<-null) (ts: 5)", "B:(1<-null) (ts: 10)", "C:(1<-null) (ts: 15)");
            processors.get(1).checkAndClearProcessResult("A:(null<-null) (ts: 5)", "B:(null<-null) (ts: 10)", "C:(null<-null) (ts: 15)");

            driver.pipeInput(recordFactory.create(topic1, "A", 2, 15L));
            driver.pipeInput(recordFactory.create(topic1, "B", 2, 8L));

            processors.get(0).checkAndClearProcessResult("A:(2<-null) (ts: 15)", "B:(2<-null) (ts: 8)");
            processors.get(1).checkAndClearProcessResult("A:(2<-null) (ts: 15)", "B:(2<-null) (ts: 8)");

            driver.pipeInput(recordFactory.create(topic1, "A", 3, 20L));

            processors.get(0).checkAndClearProcessResult("A:(3<-null) (ts: 20)");
            processors.get(1).checkAndClearProcessResult("A:(null<-null) (ts: 20)");

            driver.pipeInput(recordFactory.create(topic1, "A", null, 10L));
            driver.pipeInput(recordFactory.create(topic1, "B", null, 20L));

            processors.get(0).checkAndClearProcessResult("A:(null<-null) (ts: 10)", "B:(null<-null) (ts: 20)");
            processors.get(1).checkAndClearProcessResult("A:(null<-null) (ts: 10)", "B:(null<-null) (ts: 20)");
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
            driver.pipeInput(recordFactory.create(topic1, "A", 1, 5L));
            driver.pipeInput(recordFactory.create(topic1, "B", 1, 10L));
            driver.pipeInput(recordFactory.create(topic1, "C", 1, 15L));

            final List<MockProcessor<String, Integer>> processors = supplier.capturedProcessors(2);

            processors.get(0).checkAndClearProcessResult("A:(1<-null) (ts: 5)", "B:(1<-null) (ts: 10)", "C:(1<-null) (ts: 15)");
            processors.get(1).checkEmptyAndClearProcessResult();

            driver.pipeInput(recordFactory.create(topic1, "A", 2, 15L));
            driver.pipeInput(recordFactory.create(topic1, "B", 2, 8L));

            processors.get(0).checkAndClearProcessResult("A:(2<-1) (ts: 15)", "B:(2<-1) (ts: 8)");
            processors.get(1).checkAndClearProcessResult("A:(2<-null) (ts: 15)", "B:(2<-null) (ts: 8)");

            driver.pipeInput(recordFactory.create(topic1, "A", 3, 20L));

            processors.get(0).checkAndClearProcessResult("A:(3<-2) (ts: 20)");
            processors.get(1).checkAndClearProcessResult("A:(null<-2) (ts: 20)");

            driver.pipeInput(recordFactory.create(topic1, "A", null, 10L));
            driver.pipeInput(recordFactory.create(topic1, "B", null, 20L));

            processors.get(0).checkAndClearProcessResult("A:(null<-3) (ts: 10)", "B:(null<-2) (ts: 20)");
            processors.get(1).checkAndClearProcessResult("B:(null<-2) (ts: 20)");
        }
    }

    @Test
    public void shouldSendOldValuesWhenEnabledWithoutMaterialization() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTableImpl<String, Integer, Integer> table1 =
                (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed);
        final KTableImpl<String, Integer, Integer> table2 =
            (KTableImpl<String, Integer, Integer>) table1.filter(predicate);

        doTestSendingOldValue(builder, table1, table2, topic1);
    }

    @Test
    public void shouldSendOldValuesWhenEnabledOnMaterialization() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTableImpl<String, Integer, Integer> table1 =
            (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed);
        final KTableImpl<String, Integer, Integer> table2 =
            (KTableImpl<String, Integer, Integer>) table1.filter(predicate, Materialized.as("store2"));

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

        final ConsumerRecordFactory<String, String> stringRecordFactory =
            new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer(), 0L);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {

            driver.pipeInput(stringRecordFactory.create(topic1, "A", "reject", 5L));
            driver.pipeInput(stringRecordFactory.create(topic1, "B", "reject", 10L));
            driver.pipeInput(stringRecordFactory.create(topic1, "C", "reject", 20L));
        }

        final List<MockProcessor<String, String>> processors = supplier.capturedProcessors(2);
        processors.get(0).checkAndClearProcessResult("A:(reject<-null) (ts: 5)", "B:(reject<-null) (ts: 10)", "C:(reject<-null) (ts: 20)");
        processors.get(1).checkEmptyAndClearProcessResult();
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
            (KTableImpl<String, String, String>) table1.filter((key, value) -> value.equalsIgnoreCase("accept"))
                .groupBy(MockMapper.noOpKeyValueMapper())
                .reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER);

        doTestSkipNullOnMaterialization(builder, table1, table2, topic1);
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
            (KTableImpl<String, String, String>) table1.filter((key, value) -> value.equalsIgnoreCase("accept"), Materialized.as("store2"))
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
