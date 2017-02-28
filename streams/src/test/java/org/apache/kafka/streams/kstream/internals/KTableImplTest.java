/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.internals.SinkNode;
import org.apache.kafka.streams.processor.internals.SourceNode;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockKeyValueMapper;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class KTableImplTest {

    final private Serde<String> stringSerde = Serdes.String();

    private KStreamTestDriver driver = null;
    private File stateDir = null;
    private KStreamBuilder builder;
    private KTable<String, String> table;

    @After
    public void tearDown() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Before
    public void setUp() throws IOException {
        stateDir = TestUtils.tempDirectory("kafka-test");
        builder = new KStreamBuilder();
        table = builder.table("test", "test");
    }

    @Test
    public void testKTable() {
        final KStreamBuilder builder = new KStreamBuilder();

        String topic1 = "topic1";
        String topic2 = "topic2";
        String storeName1 = "storeName1";
        String storeName2 = "storeName2";

        KTable<String, String> table1 = builder.table(stringSerde, stringSerde, topic1, storeName1);

        MockProcessorSupplier<String, String> proc1 = new MockProcessorSupplier<>();
        table1.toStream().process(proc1);

        KTable<String, Integer> table2 = table1.mapValues(new ValueMapper<String, Integer>() {
            @Override
            public Integer apply(String value) {
                return new Integer(value);
            }
        });

        MockProcessorSupplier<String, Integer> proc2 = new MockProcessorSupplier<>();
        table2.toStream().process(proc2);

        KTable<String, Integer> table3 = table2.filter(new Predicate<String, Integer>() {
            @Override
            public boolean test(String key, Integer value) {
                return (value % 2) == 0;
            }
        });

        MockProcessorSupplier<String, Integer> proc3 = new MockProcessorSupplier<>();
        table3.toStream().process(proc3);

        KTable<String, String> table4 = table1.through(stringSerde, stringSerde, topic2, storeName2);

        MockProcessorSupplier<String, String> proc4 = new MockProcessorSupplier<>();
        table4.toStream().process(proc4);

        driver = new KStreamTestDriver(builder, stateDir);

        driver.process(topic1, "A", "01");
        driver.flushState();
        driver.process(topic1, "B", "02");
        driver.flushState();
        driver.process(topic1, "C", "03");
        driver.flushState();
        driver.process(topic1, "D", "04");
        driver.flushState();
        driver.flushState();

        assertEquals(Utils.mkList("A:01", "B:02", "C:03", "D:04"), proc1.processed);
        assertEquals(Utils.mkList("A:1", "B:2", "C:3", "D:4"), proc2.processed);
        assertEquals(Utils.mkList("A:null", "B:2", "C:null", "D:4"), proc3.processed);
        assertEquals(Utils.mkList("A:01", "B:02", "C:03", "D:04"), proc4.processed);
    }

    @Test
    public void testValueGetter() throws IOException {
        final KStreamBuilder builder = new KStreamBuilder();

        String topic1 = "topic1";
        String topic2 = "topic2";
        String storeName1 = "storeName1";
        String storeName2 = "storeName2";

        KTableImpl<String, String, String> table1 =
                (KTableImpl<String, String, String>) builder.table(stringSerde, stringSerde, topic1, storeName1);
        KTableImpl<String, String, Integer> table2 = (KTableImpl<String, String, Integer>) table1.mapValues(
                new ValueMapper<String, Integer>() {
                    @Override
                    public Integer apply(String value) {
                        return new Integer(value);
                    }
                });
        KTableImpl<String, Integer, Integer> table3 = (KTableImpl<String, Integer, Integer>) table2.filter(
                new Predicate<String, Integer>() {
                    @Override
                    public boolean test(String key, Integer value) {
                        return (value % 2) == 0;
                    }
                });
        KTableImpl<String, String, String> table4 = (KTableImpl<String, String, String>)
                table1.through(stringSerde, stringSerde, topic2, storeName2);

        KTableValueGetterSupplier<String, String> getterSupplier1 = table1.valueGetterSupplier();
        KTableValueGetterSupplier<String, Integer> getterSupplier2 = table2.valueGetterSupplier();
        KTableValueGetterSupplier<String, Integer> getterSupplier3 = table3.valueGetterSupplier();
        KTableValueGetterSupplier<String, String> getterSupplier4 = table4.valueGetterSupplier();

        driver = new KStreamTestDriver(builder, stateDir, null, null);

        // two state store should be created
        assertEquals(2, driver.allStateStores().size());

        KTableValueGetter<String, String> getter1 = getterSupplier1.get();
        getter1.init(driver.context());
        KTableValueGetter<String, Integer> getter2 = getterSupplier2.get();
        getter2.init(driver.context());
        KTableValueGetter<String, Integer> getter3 = getterSupplier3.get();
        getter3.init(driver.context());
        KTableValueGetter<String, String> getter4 = getterSupplier4.get();
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
    public void testStateStoreLazyEval() throws IOException {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String storeName1 = "storeName1";
        String storeName2 = "storeName2";

        final KStreamBuilder builder = new KStreamBuilder();

        KTableImpl<String, String, String> table1 =
                (KTableImpl<String, String, String>) builder.table(stringSerde, stringSerde, topic1, storeName1);
        KTableImpl<String, String, String> table2 =
                (KTableImpl<String, String, String>) builder.table(stringSerde, stringSerde, topic2, storeName2);

        KTableImpl<String, String, Integer> table1Mapped = (KTableImpl<String, String, Integer>) table1.mapValues(
                new ValueMapper<String, Integer>() {
                    @Override
                    public Integer apply(String value) {
                        return new Integer(value);
                    }
                });
        KTableImpl<String, Integer, Integer> table1MappedFiltered = (KTableImpl<String, Integer, Integer>) table1Mapped.filter(
                new Predicate<String, Integer>() {
                    @Override
                    public boolean test(String key, Integer value) {
                        return (value % 2) == 0;
                    }
                });

        driver = new KStreamTestDriver(builder, stateDir, null, null);
        driver.setTime(0L);

        // two state stores should be created
        assertEquals(2, driver.allStateStores().size());
    }

    @Test
    public void testStateStore() throws IOException {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String storeName1 = "storeName1";
        String storeName2 = "storeName2";

        final KStreamBuilder builder = new KStreamBuilder();

        KTableImpl<String, String, String> table1 =
                (KTableImpl<String, String, String>) builder.table(stringSerde, stringSerde, topic1, storeName1);
        KTableImpl<String, String, String> table2 =
                (KTableImpl<String, String, String>) builder.table(stringSerde, stringSerde, topic2, storeName2);

        KTableImpl<String, String, Integer> table1Mapped = (KTableImpl<String, String, Integer>) table1.mapValues(
                new ValueMapper<String, Integer>() {
                    @Override
                    public Integer apply(String value) {
                        return new Integer(value);
                    }
                });
        KTableImpl<String, Integer, Integer> table1MappedFiltered = (KTableImpl<String, Integer, Integer>) table1Mapped.filter(
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

        driver = new KStreamTestDriver(builder, stateDir, null, null);
        driver.setTime(0L);

        // two state store should be created
        assertEquals(2, driver.allStateStores().size());
    }

    @Test
    public void testRepartition() throws Exception {
        String topic1 = "topic1";
        String storeName1 = "storeName1";

        final KStreamBuilder builder = new KStreamBuilder();

        KTableImpl<String, String, String> table1 =
                (KTableImpl<String, String, String>) builder.table(stringSerde, stringSerde, topic1, storeName1);

        KTableImpl<String, String, String> table1Aggregated = (KTableImpl<String, String, String>) table1
                .groupBy(MockKeyValueMapper.<String, String>NoOpKeyValueMapper())
                .aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, MockAggregator.TOSTRING_REMOVER, "mock-result1");


        KTableImpl<String, String, String> table1Reduced = (KTableImpl<String, String, String>) table1
                .groupBy(MockKeyValueMapper.<String, String>NoOpKeyValueMapper())
                .reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER, "mock-result2");

        driver = new KStreamTestDriver(builder, stateDir, stringSerde, stringSerde);
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
    public void shouldNotAllowNullSelectorOnToStream() throws Exception {
        table.toStream(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTopicOnTo() throws Exception {
        table.to(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullPredicateOnFilter() throws Exception {
        table.filter(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullPredicateOnFilterNot() throws Exception {
        table.filterNot(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnMapValues() throws Exception {
        table.mapValues(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullFilePathOnWriteAsText() throws Exception {
        table.writeAsText(null);
    }

    @Test(expected = TopologyBuilderException.class)
    public void shouldNotAllowEmptyFilePathOnWriteAsText() throws Exception {
        table.writeAsText("\t  \t");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullActionOnForEach() throws Exception {
        table.foreach(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTopicInThrough() throws Exception {
        table.through(null, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullStoreInThrough() throws Exception {
        table.through("topic", null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullSelectorOnGroupBy() throws Exception {
        table.groupBy(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullOtherTableOnJoin() throws Exception {
        table.join(null, MockValueJoiner.TOSTRING_JOINER);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullJoinerJoin() throws Exception {
        table.join(table, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullOtherTableOnOuterJoin() throws Exception {
        table.outerJoin(null, MockValueJoiner.TOSTRING_JOINER);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullJoinerOnOuterJoin() throws Exception {
        table.outerJoin(table, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullJoinerOnLeftJoin() throws Exception {
        table.leftJoin(table, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullOtherTableOnLeftJoin() throws Exception {
        table.leftJoin(null, MockValueJoiner.TOSTRING_JOINER);
    }

}
