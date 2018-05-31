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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.internals.SinkNode;
import org.apache.kafka.streams.processor.internals.SourceNode;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.List;

import static org.easymock.EasyMock.mock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class KTableImplTest {

    private final Serde<String> stringSerde = Serdes.String();
    private final Consumed<String, String> consumed = Consumed.with(stringSerde, stringSerde);
    private final Produced<String, String> produced = Produced.with(stringSerde, stringSerde);

    @Rule
    public final KStreamTestDriver driver = new KStreamTestDriver();
    private File stateDir = null;
    private StreamsBuilder builder;
    private KTable<String, String> table;

    @Before
    public void setUp() {
        stateDir = TestUtils.tempDirectory("kafka-test");
        builder = new StreamsBuilder();
        table = builder.table("test");
    }

    @Test
    public void testKTable() {
        final StreamsBuilder builder = new StreamsBuilder();

        String topic1 = "topic1";
        String topic2 = "topic2";

        final KTable<String, String> table1 = builder.table(topic1, consumed);

        final MockProcessorSupplier<String, Object> supplier = new MockProcessorSupplier<>();
        table1.toStream().process(supplier);

        final KTable<String, Integer> table2 = table1.mapValues(new ValueMapper<String, Integer>() {
            @Override
            public Integer apply(String value) {
                return new Integer(value);
            }
        });

        table2.toStream().process(supplier);

        final KTable<String, Integer> table3 = table2.filter(new Predicate<String, Integer>() {
            @Override
            public boolean test(String key, Integer value) {
                return (value % 2) == 0;
            }
        });

        table3.toStream().process(supplier);

        table1.toStream().to(topic2, produced);
        final KTable<String, String> table4 = builder.table(topic2, consumed);

        table4.toStream().process(supplier);

        driver.setUp(builder, stateDir);

        driver.process(topic1, "A", "01");
        driver.flushState();
        driver.process(topic1, "B", "02");
        driver.flushState();
        driver.process(topic1, "C", "03");
        driver.flushState();
        driver.process(topic1, "D", "04");
        driver.flushState();
        driver.flushState();

        final List<MockProcessor<String, Object>> processors = supplier.capturedProcessors(4);
        assertEquals(Utils.mkList("A:01", "B:02", "C:03", "D:04"), processors.get(0).processed);
        assertEquals(Utils.mkList("A:1", "B:2", "C:3", "D:4"), processors.get(1).processed);
        assertEquals(Utils.mkList("A:null", "B:2", "C:null", "D:4"), processors.get(2).processed);
        assertEquals(Utils.mkList("A:01", "B:02", "C:03", "D:04"), processors.get(3).processed);
    }

    @Test
    public void testValueGetter() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";
        final String topic2 = "topic2";

        final KTableImpl<String, String, String> table1 =
                (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        final KTableImpl<String, String, Integer> table2 = (KTableImpl<String, String, Integer>) table1.mapValues(
                new ValueMapper<String, Integer>() {
                    @Override
                    public Integer apply(String value) {
                        return new Integer(value);
                    }
                });
        final KTableImpl<String, Integer, Integer> table3 = (KTableImpl<String, Integer, Integer>) table2.filter(
                new Predicate<String, Integer>() {
                    @Override
                    public boolean test(String key, Integer value) {
                        return (value % 2) == 0;
                    }
                });

        table1.toStream().to(topic2, produced);
        final KTableImpl<String, String, String> table4 = (KTableImpl<String, String, String>) builder.table(topic2, consumed);

        final KTableValueGetterSupplier<String, String> getterSupplier1 = table1.valueGetterSupplier();
        final KTableValueGetterSupplier<String, Integer> getterSupplier2 = table2.valueGetterSupplier();
        final KTableValueGetterSupplier<String, Integer> getterSupplier3 = table3.valueGetterSupplier();
        final KTableValueGetterSupplier<String, String> getterSupplier4 = table4.valueGetterSupplier();

        driver.setUp(builder, stateDir, null, null);

        // two state store should be created
        assertEquals(2, driver.allStateStores().size());

        final KTableValueGetter<String, String> getter1 = getterSupplier1.get();
        getter1.init(driver.context());
        final KTableValueGetter<String, Integer> getter2 = getterSupplier2.get();
        getter2.init(driver.context());
        final KTableValueGetter<String, Integer> getter3 = getterSupplier3.get();
        getter3.init(driver.context());
        final KTableValueGetter<String, String> getter4 = getterSupplier4.get();
        getter4.init(driver.context());

        driver.process(topic1, "A", "01");
        driver.process(topic1, "B", "01");
        driver.process(topic1, "C", "01");
        driver.flushState();

        assertEquals("01", getter1.get("A"));
        assertEquals("01", getter1.get("B"));
        assertEquals("01", getter1.get("C"));

        assertEquals(new Integer(1), getter2.get("A"));
        assertEquals(new Integer(1), getter2.get("B"));
        assertEquals(new Integer(1), getter2.get("C"));

        assertNull(getter3.get("A"));
        assertNull(getter3.get("B"));
        assertNull(getter3.get("C"));

        assertEquals("01", getter4.get("A"));
        assertEquals("01", getter4.get("B"));
        assertEquals("01", getter4.get("C"));

        driver.process(topic1, "A", "02");
        driver.process(topic1, "B", "02");
        driver.flushState();

        assertEquals("02", getter1.get("A"));
        assertEquals("02", getter1.get("B"));
        assertEquals("01", getter1.get("C"));

        assertEquals(new Integer(2), getter2.get("A"));
        assertEquals(new Integer(2), getter2.get("B"));
        assertEquals(new Integer(1), getter2.get("C"));

        assertEquals(new Integer(2), getter3.get("A"));
        assertEquals(new Integer(2), getter3.get("B"));
        assertNull(getter3.get("C"));

        assertEquals("02", getter4.get("A"));
        assertEquals("02", getter4.get("B"));
        assertEquals("01", getter4.get("C"));

        driver.process(topic1, "A", "03");
        driver.flushState();

        assertEquals("03", getter1.get("A"));
        assertEquals("02", getter1.get("B"));
        assertEquals("01", getter1.get("C"));

        assertEquals(new Integer(3), getter2.get("A"));
        assertEquals(new Integer(2), getter2.get("B"));
        assertEquals(new Integer(1), getter2.get("C"));

        assertNull(getter3.get("A"));
        assertEquals(new Integer(2), getter3.get("B"));
        assertNull(getter3.get("C"));

        assertEquals("03", getter4.get("A"));
        assertEquals("02", getter4.get("B"));
        assertEquals("01", getter4.get("C"));

        driver.process(topic1, "A", null);
        driver.flushState();

        assertNull(getter1.get("A"));
        assertEquals("02", getter1.get("B"));
        assertEquals("01", getter1.get("C"));


        assertNull(getter2.get("A"));
        assertEquals(new Integer(2), getter2.get("B"));
        assertEquals(new Integer(1), getter2.get("C"));

        assertNull(getter3.get("A"));
        assertEquals(new Integer(2), getter3.get("B"));
        assertNull(getter3.get("C"));

        assertNull(getter4.get("A"));
        assertEquals("02", getter4.get("B"));
        assertEquals("01", getter4.get("C"));
    }

    @Test
    public void testStateStoreLazyEval() {
        final String topic1 = "topic1";
        final String topic2 = "topic2";

        final StreamsBuilder builder = new StreamsBuilder();

        final KTableImpl<String, String, String> table1 =
                (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        builder.table(topic2, consumed);

        final KTableImpl<String, String, Integer> table1Mapped = (KTableImpl<String, String, Integer>) table1.mapValues(
                new ValueMapper<String, Integer>() {
                    @Override
                    public Integer apply(String value) {
                        return new Integer(value);
                    }
                });
        table1Mapped.filter(
                new Predicate<String, Integer>() {
                    @Override
                    public boolean test(String key, Integer value) {
                        return (value % 2) == 0;
                    }
                });

        driver.setUp(builder, stateDir, null, null);
        driver.setTime(0L);

        // two state stores should be created
        assertEquals(2, driver.allStateStores().size());
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

        final KTableImpl<String, String, Integer> table1Mapped = (KTableImpl<String, String, Integer>) table1.mapValues(
                new ValueMapper<String, Integer>() {
                    @Override
                    public Integer apply(String value) {
                        return new Integer(value);
                    }
                });
        final KTableImpl<String, Integer, Integer> table1MappedFiltered = (KTableImpl<String, Integer, Integer>) table1Mapped.filter(
                new Predicate<String, Integer>() {
                    @Override
                    public boolean test(String key, Integer value) {
                        return (value % 2) == 0;
                    }
                });
        table2.join(table1MappedFiltered,
                new ValueJoiner<String, Integer, String>() {
                    @Override
                    public String apply(String v1, Integer v2) {
                        return v1 + v2;
                    }
                });

        driver.setUp(builder, stateDir, null, null);
        driver.setTime(0L);

        // two state store should be created
        assertEquals(2, driver.allStateStores().size());
    }

    @Test
    public void testRepartition() throws NoSuchFieldException, IllegalAccessException {
        final String topic1 = "topic1";
        final String storeName1 = "storeName1";

        final StreamsBuilder builder = new StreamsBuilder();

        final KTableImpl<String, String, String> table1 =
                (KTableImpl<String, String, String>) builder.table(topic1,
                                                                   consumed,
                                                                   Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(storeName1)
                                                                           .withKeySerde(stringSerde)
                                                                           .withValueSerde(stringSerde)
                );

        table1.groupBy(MockMapper.<String, String>noOpKeyValueMapper())
            .aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, MockAggregator.TOSTRING_REMOVER, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("mock-result1"));


        table1.groupBy(MockMapper.<String, String>noOpKeyValueMapper())
            .reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("mock-result2"));

        driver.setUp(builder, stateDir, stringSerde, stringSerde);
        driver.setTime(0L);

        // three state store should be created, one for source, one for aggregate and one for reduce
        assertEquals(3, driver.allStateStores().size());

        // contains the corresponding repartition source / sink nodes
        assertTrue(driver.allProcessorNames().contains("KSTREAM-SINK-0000000003"));
        assertTrue(driver.allProcessorNames().contains("KSTREAM-SOURCE-0000000004"));
        assertTrue(driver.allProcessorNames().contains("KSTREAM-SINK-0000000007"));
        assertTrue(driver.allProcessorNames().contains("KSTREAM-SOURCE-0000000008"));

        Field valSerializerField  = ((SinkNode) driver.processor("KSTREAM-SINK-0000000003")).getClass().getDeclaredField("valSerializer");
        Field valDeserializerField  = ((SourceNode) driver.processor("KSTREAM-SOURCE-0000000004")).getClass().getDeclaredField("valDeserializer");
        valSerializerField.setAccessible(true);
        valDeserializerField.setAccessible(true);

        assertNotNull(((ChangedSerializer) valSerializerField.get(driver.processor("KSTREAM-SINK-0000000003"))).inner());
        assertNotNull(((ChangedDeserializer) valDeserializerField.get(driver.processor("KSTREAM-SOURCE-0000000004"))).inner());
        assertNotNull(((ChangedSerializer) valSerializerField.get(driver.processor("KSTREAM-SINK-0000000007"))).inner());
        assertNotNull(((ChangedDeserializer) valDeserializerField.get(driver.processor("KSTREAM-SOURCE-0000000008"))).inner());
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
        table.filter(new Predicate<String, String>() {
            @Override
            public boolean test(final String key, final String value) {
                return false;
            }
        }, (Materialized) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnFilterNotWhenMaterializedIsNull() {
        table.filterNot(new Predicate<String, String>() {
            @Override
            public boolean test(final String key, final String value) {
                return false;
            }
        }, (Materialized) null);
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
        table.leftJoin(table, MockValueJoiner.TOSTRING_JOINER, (Materialized) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnTransformValuesWithKeyWhenTransformerSupplierIsNull() {
        table.transformValues((ValueTransformerWithKeySupplier) null);
    }

    @SuppressWarnings("unchecked")
    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnTransformValuesWithKeyWhenMaterializedIsNull() {
        final ValueTransformerWithKeySupplier<String, String, ?> valueTransformerSupplier = mock(ValueTransformerWithKeySupplier.class);
        table.transformValues(valueTransformerSupplier, (Materialized) null);
    }

    @SuppressWarnings("unchecked")
    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnTransformValuesWithKeyWhenStoreNamesNull() {
        final ValueTransformerWithKeySupplier<String, String, ?> valueTransformerSupplier = mock(ValueTransformerWithKeySupplier.class);
        table.transformValues(valueTransformerSupplier, (String[]) null);
    }
}
