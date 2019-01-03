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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyTestDriverWrapper;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.SinkNode;
import org.apache.kafka.streams.processor.internals.SourceNode;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;

import static java.util.Arrays.asList;
import static org.easymock.EasyMock.mock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@SuppressWarnings("unchecked")
public class KTableImplTest {

    private final Consumed<String, String> stringConsumed = Consumed.with(Serdes.String(), Serdes.String());
    private final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
    private final Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
    private final ConsumerRecordFactory<String, String> recordFactory =
        new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
    private final Serde<String> mySerde = new Serdes.StringSerde();

    private KTable<String, String> table;

    @Before
    public void setUp() {
        table = new StreamsBuilder().table("test");
    }

    @Test
    public void testKTable() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";
        final String topic2 = "topic2";

        final KTable<String, String> table1 = builder.table(topic1, consumed);

        final MockProcessorSupplier<String, Object> supplier = new MockProcessorSupplier<>();
        table1.toStream().process(supplier);

        final KTable<String, Integer> table2 = table1.mapValues(Integer::new);

        table2.toStream().process(supplier);

        final KTable<String, Integer> table3 = table2.filter((key, value) -> (value % 2) == 0);

        table3.toStream().process(supplier);

        table1.toStream().to(topic2, produced);
        final KTable<String, String> table4 = builder.table(topic2, consumed);

        table4.toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(topic1, "A", "01"));
            driver.pipeInput(recordFactory.create(topic1, "B", "02"));
            driver.pipeInput(recordFactory.create(topic1, "C", "03"));
            driver.pipeInput(recordFactory.create(topic1, "D", "04"));
        }

        final List<MockProcessor<String, Object>> processors = supplier.capturedProcessors(4);
        assertEquals(asList("A:01", "B:02", "C:03", "D:04"), processors.get(0).processed);
        assertEquals(asList("A:1", "B:2", "C:3", "D:4"), processors.get(1).processed);
        assertEquals(asList("A:null", "B:2", "C:null", "D:4"), processors.get(2).processed);
        assertEquals(asList("A:01", "B:02", "C:03", "D:04"), processors.get(3).processed);
    }

    @Test
    public void shouldPreserveSerdesForOperators() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, String> table1 = builder.table("topic-2", stringConsumed);
        final ConsumedInternal<String, String> consumedInternal = new ConsumedInternal<>(stringConsumed);

        final KeyValueMapper<String, String, String> selector = (key, value) -> key;
        final ValueMapper<String, String> mapper = value -> value;
        final ValueJoiner<String, String, String> joiner = (value1, value2) -> value1;
        final ValueTransformerWithKeySupplier<String, String, String> valueTransformerWithKeySupplier =
            () -> new ValueTransformerWithKey<String, String, String>() {
                @Override
                public void init(final ProcessorContext context) {}

                @Override
                public String transform(final String key, final String value) {
                    return value;
                }

                @Override
                public void close() {}
            };

        assertEquals(
            ((AbstractStream) table1.filter((key, value) -> false)).keySerde(),
            consumedInternal.keySerde());
        assertEquals(
            ((AbstractStream) table1.filter((key, value) -> false)).valueSerde(),
            consumedInternal.valueSerde());
        assertEquals(
            ((AbstractStream) table1.filter((key, value) -> false, Materialized.with(mySerde, mySerde))).keySerde(),
            mySerde);
        assertEquals(
            ((AbstractStream) table1.filter((key, value) -> false, Materialized.with(mySerde, mySerde))).valueSerde(),
            mySerde);

        assertEquals(
            ((AbstractStream) table1.filterNot((key, value) -> false)).keySerde(),
            consumedInternal.keySerde());
        assertEquals(
            ((AbstractStream) table1.filterNot((key, value) -> false)).valueSerde(),
            consumedInternal.valueSerde());
        assertEquals(
            ((AbstractStream) table1.filterNot((key, value) -> false, Materialized.with(mySerde, mySerde))).keySerde(),
            mySerde);
        assertEquals(
            ((AbstractStream) table1.filterNot((key, value) -> false, Materialized.with(mySerde, mySerde))).valueSerde(),
            mySerde);

        assertEquals(
            ((AbstractStream) table1.mapValues(mapper)).keySerde(),
            consumedInternal.keySerde());
        assertNull(((AbstractStream) table1.mapValues(mapper)).valueSerde());
        assertEquals(
            ((AbstractStream) table1.mapValues(mapper, Materialized.with(mySerde, mySerde))).keySerde(),
            mySerde);
        assertEquals(
            ((AbstractStream) table1.mapValues(mapper, Materialized.with(mySerde, mySerde))).valueSerde(),
            mySerde);

        assertEquals(
            ((AbstractStream) table1.toStream()).keySerde(),
            consumedInternal.keySerde());
        assertEquals(
            ((AbstractStream) table1.toStream()).valueSerde(),
            consumedInternal.valueSerde());
        assertNull(((AbstractStream) table1.toStream(selector)).keySerde());
        assertEquals(
            ((AbstractStream) table1.toStream(selector)).valueSerde(),
            consumedInternal.valueSerde());

        assertEquals(
            ((AbstractStream) table1.transformValues(valueTransformerWithKeySupplier)).keySerde(),
            consumedInternal.keySerde());
        assertNull(((AbstractStream) table1.transformValues(valueTransformerWithKeySupplier)).valueSerde());
        assertEquals(
            ((AbstractStream) table1.transformValues(valueTransformerWithKeySupplier, Materialized.with(mySerde, mySerde))).keySerde(),
            mySerde);
        assertEquals(((AbstractStream) table1.transformValues(valueTransformerWithKeySupplier, Materialized.with(mySerde, mySerde))).valueSerde(),
            mySerde);

        assertNull(((AbstractStream) table1.groupBy(KeyValue::new)).keySerde());
        assertNull(((AbstractStream) table1.groupBy(KeyValue::new)).valueSerde());
        assertEquals(
            ((AbstractStream) table1.groupBy(KeyValue::new, Grouped.with(mySerde, mySerde))).keySerde(),
            mySerde);
        assertEquals(
            ((AbstractStream) table1.groupBy(KeyValue::new, Grouped.with(mySerde, mySerde))).valueSerde(),
            mySerde);

        assertEquals(
            ((AbstractStream) table1.join(table1, joiner)).keySerde(),
            consumedInternal.keySerde());
        assertNull(((AbstractStream) table1.join(table1, joiner)).valueSerde());
        assertEquals(
            ((AbstractStream) table1.join(table1, joiner, Materialized.with(mySerde, mySerde))).keySerde(),
            mySerde);
        assertEquals(
            ((AbstractStream) table1.join(table1, joiner, Materialized.with(mySerde, mySerde))).valueSerde(),
            mySerde);

        assertEquals(
            ((AbstractStream) table1.leftJoin(table1, joiner)).keySerde(),
            consumedInternal.keySerde());
        assertNull(((AbstractStream) table1.leftJoin(table1, joiner)).valueSerde());
        assertEquals(
            ((AbstractStream) table1.leftJoin(table1, joiner, Materialized.with(mySerde, mySerde))).keySerde(),
            mySerde);
        assertEquals(
            ((AbstractStream) table1.leftJoin(table1, joiner, Materialized.with(mySerde, mySerde))).valueSerde(),
            mySerde);

        assertEquals(
            ((AbstractStream) table1.outerJoin(table1, joiner)).keySerde(),
            consumedInternal.keySerde());
        assertNull(((AbstractStream) table1.outerJoin(table1, joiner)).valueSerde());
        assertEquals(
            ((AbstractStream) table1.outerJoin(table1, joiner, Materialized.with(mySerde, mySerde))).keySerde(),
            mySerde);
        assertEquals(
            ((AbstractStream) table1.outerJoin(table1, joiner, Materialized.with(mySerde, mySerde))).valueSerde(),
            mySerde);
    }

    @Test
    public void testStateStoreLazyEval() {
        final String topic1 = "topic1";
        final String topic2 = "topic2";

        final StreamsBuilder builder = new StreamsBuilder();

        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        builder.table(topic2, consumed);

        final KTableImpl<String, String, Integer> table1Mapped =
            (KTableImpl<String, String, Integer>) table1.mapValues(Integer::new);
        table1Mapped.filter((key, value) -> (value % 2) == 0);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            assertEquals(0, driver.getAllStateStores().size());
        }
    }

    @Test
    public void testStateStore() {
        final String topic1 = "topic1";
        final String topic2 = "topic2";

        final StreamsBuilder builder = new StreamsBuilder();

        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        final KTableImpl<String, String, String> table2 =
            (KTableImpl<String, String, String>) builder.table(topic2, consumed);

        final KTableImpl<String, String, Integer> table1Mapped =
            (KTableImpl<String, String, Integer>) table1.mapValues(Integer::new);
        final KTableImpl<String, Integer, Integer> table1MappedFiltered =
            (KTableImpl<String, Integer, Integer>) table1Mapped.filter((key, value) -> (value % 2) == 0);
        table2.join(table1MappedFiltered, (v1, v2) -> v1 + v2);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            assertEquals(2, driver.getAllStateStores().size());
        }
    }

    private void assertTopologyContainsProcessor(final Topology topology, final String processorName) {
        for (final TopologyDescription.Subtopology subtopology: topology.describe().subtopologies()) {
            for (final TopologyDescription.Node node: subtopology.nodes()) {
                if (node.name().equals(processorName)) {
                    return;
                }
            }
        }
        throw new AssertionError("No processor named '" + processorName + "'"
            + "found in the provided Topology:\n" + topology.describe());
    }

    @Test
    public void shouldCreateSourceAndSinkNodesForRepartitioningTopic() throws Exception {
        final String topic1 = "topic1";
        final String storeName1 = "storeName1";

        final StreamsBuilder builder = new StreamsBuilder();

        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(
                topic1,
                consumed,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(storeName1)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
            );

        table1.groupBy(MockMapper.noOpKeyValueMapper())
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                MockAggregator.TOSTRING_REMOVER,
                Materialized.as("mock-result1"));

        table1.groupBy(MockMapper.noOpKeyValueMapper())
            .reduce(
                MockReducer.STRING_ADDER,
                MockReducer.STRING_REMOVER,
                Materialized.as("mock-result2"));

        final Topology topology = builder.build();
        try (final TopologyTestDriverWrapper driver = new TopologyTestDriverWrapper(topology, props)) {

            assertEquals(3, driver.getAllStateStores().size());

            assertTopologyContainsProcessor(topology, "KSTREAM-SINK-0000000003");
            assertTopologyContainsProcessor(topology, "KSTREAM-SOURCE-0000000004");
            assertTopologyContainsProcessor(topology, "KSTREAM-SINK-0000000007");
            assertTopologyContainsProcessor(topology, "KSTREAM-SOURCE-0000000008");

            final Field valSerializerField = ((SinkNode) driver.getProcessor("KSTREAM-SINK-0000000003"))
                .getClass()
                .getDeclaredField("valSerializer");
            final Field valDeserializerField = ((SourceNode) driver.getProcessor("KSTREAM-SOURCE-0000000004"))
                .getClass()
                .getDeclaredField("valDeserializer");
            valSerializerField.setAccessible(true);
            valDeserializerField.setAccessible(true);

            assertNotNull(((ChangedSerializer) valSerializerField.get(driver.getProcessor("KSTREAM-SINK-0000000003"))).inner());
            assertNotNull(((ChangedDeserializer) valDeserializerField.get(driver.getProcessor("KSTREAM-SOURCE-0000000004"))).inner());
            assertNotNull(((ChangedSerializer) valSerializerField.get(driver.getProcessor("KSTREAM-SINK-0000000007"))).inner());
            assertNotNull(((ChangedDeserializer) valDeserializerField.get(driver.getProcessor("KSTREAM-SOURCE-0000000008"))).inner());
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullSelectorOnToStream() {
        table.toStream(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullPredicateOnFilter() {
        table.filter(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullPredicateOnFilterNot() {
        table.filterNot(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnMapValues() {
        table.mapValues((ValueMapper) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnMapValueWithKey() {
        table.mapValues((ValueMapperWithKey) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullSelectorOnGroupBy() {
        table.groupBy(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullOtherTableOnJoin() {
        table.join(null, MockValueJoiner.TOSTRING_JOINER);
    }

    @Test
    public void shouldAllowNullStoreInJoin() {
        table.join(table, MockValueJoiner.TOSTRING_JOINER);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullJoinerJoin() {
        table.join(table, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullOtherTableOnOuterJoin() {
        table.outerJoin(null, MockValueJoiner.TOSTRING_JOINER);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullJoinerOnOuterJoin() {
        table.outerJoin(table, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullJoinerOnLeftJoin() {
        table.leftJoin(table, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullOtherTableOnLeftJoin() {
        table.leftJoin(null, MockValueJoiner.TOSTRING_JOINER);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnFilterWhenMaterializedIsNull() {
        table.filter((key, value) -> false, (Materialized) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnFilterNotWhenMaterializedIsNull() {
        table.filterNot((key, value) -> false, (Materialized) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnJoinWhenMaterializedIsNull() {
        table.join(table, MockValueJoiner.TOSTRING_JOINER, (Materialized) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnLeftJoinWhenMaterializedIsNull() {
        table.leftJoin(table, MockValueJoiner.TOSTRING_JOINER, (Materialized) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnOuterJoinWhenMaterializedIsNull() {
        table.outerJoin(table, MockValueJoiner.TOSTRING_JOINER, (Materialized) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnTransformValuesWithKeyWhenTransformerSupplierIsNull() {
        table.transformValues((ValueTransformerWithKeySupplier) null);
    }

    @SuppressWarnings("unchecked")
    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnTransformValuesWithKeyWhenMaterializedIsNull() {
        final ValueTransformerWithKeySupplier<String, String, ?> valueTransformerSupplier =
            mock(ValueTransformerWithKeySupplier.class);
        table.transformValues(valueTransformerSupplier, (Materialized) null);
    }

    @SuppressWarnings("unchecked")
    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnTransformValuesWithKeyWhenStoreNamesNull() {
        final ValueTransformerWithKeySupplier<String, String, ?> valueTransformerSupplier =
            mock(ValueTransformerWithKeySupplier.class);
        table.transformValues(valueTransformerSupplier, (String[]) null);
    }
}
