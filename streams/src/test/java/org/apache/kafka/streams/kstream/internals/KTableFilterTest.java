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
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.MockKeyValueMapper;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class KTableFilterTest {

    final private Serde<Integer> intSerde = Serdes.Integer();
    final private Serde<String> stringSerde = Serdes.String();

    private KStreamTestDriver driver = null;
    private File stateDir = null;

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
    }

    @Test
    public void testKTable() {
        final KStreamBuilder builder = new KStreamBuilder();

        String topic1 = "topic1";

        KTable<String, Integer> table1 = builder.table(stringSerde, intSerde, topic1, "anyStoreName");

        KTable<String, Integer> table2 = table1.filter(new Predicate<String, Integer>() {
            @Override
            public boolean test(String key, Integer value) {
                return (value % 2) == 0;
            }
        });
        KTable<String, Integer> table3 = table1.filterNot(new Predicate<String, Integer>() {
            @Override
            public boolean test(String key, Integer value) {
                return (value % 2) == 0;
            }
        });

        MockProcessorSupplier<String, Integer> proc2 = new MockProcessorSupplier<>();
        MockProcessorSupplier<String, Integer> proc3 = new MockProcessorSupplier<>();
        table2.toStream().process(proc2);
        table3.toStream().process(proc3);

        driver = new KStreamTestDriver(builder, stateDir);

        driver.process(topic1, "A", 1);
        driver.process(topic1, "B", 2);
        driver.process(topic1, "C", 3);
        driver.process(topic1, "D", 4);
        driver.flushState();
        driver.process(topic1, "A", null);
        driver.process(topic1, "B", null);
        driver.flushState();

        proc2.checkAndClearProcessResult("A:null", "B:2", "C:null", "D:4", "A:null", "B:null");
        proc3.checkAndClearProcessResult("A:1", "B:null", "C:3", "D:null", "A:null", "B:null");
    }

    @Test
    public void testValueGetter() throws IOException {
        KStreamBuilder builder = new KStreamBuilder();

        String topic1 = "topic1";

        KTableImpl<String, Integer, Integer> table1 =
                (KTableImpl<String, Integer, Integer>) builder.table(stringSerde, intSerde, topic1, "anyStoreName");
        KTableImpl<String, Integer, Integer> table2 = (KTableImpl<String, Integer, Integer>) table1.filter(
                new Predicate<String, Integer>() {
                    @Override
                    public boolean test(String key, Integer value) {
                        return (value % 2) == 0;
                    }
                });
        KTableImpl<String, Integer, Integer> table3 = (KTableImpl<String, Integer, Integer>) table1.filterNot(
                new Predicate<String, Integer>() {
                    @Override
                    public boolean test(String key, Integer value) {
                        return (value % 2) == 0;
                    }
                });

        KTableValueGetterSupplier<String, Integer> getterSupplier2 = table2.valueGetterSupplier();
        KTableValueGetterSupplier<String, Integer> getterSupplier3 = table3.valueGetterSupplier();

        driver = new KStreamTestDriver(builder, stateDir, null, null);

        KTableValueGetter<String, Integer> getter2 = getterSupplier2.get();
        KTableValueGetter<String, Integer> getter3 = getterSupplier3.get();

        getter2.init(driver.context());
        getter3.init(driver.context());

        driver.process(topic1, "A", 1);
        driver.process(topic1, "B", 1);
        driver.process(topic1, "C", 1);

        assertNull(getter2.get("A"));
        assertNull(getter2.get("B"));
        assertNull(getter2.get("C"));

        assertEquals(1, (int) getter3.get("A"));
        assertEquals(1, (int) getter3.get("B"));
        assertEquals(1, (int) getter3.get("C"));

        driver.process(topic1, "A", 2);
        driver.process(topic1, "B", 2);

        assertEquals(2, (int) getter2.get("A"));
        assertEquals(2, (int) getter2.get("B"));
        assertNull(getter2.get("C"));

        assertNull(getter3.get("A"));
        assertNull(getter3.get("B"));
        assertEquals(1, (int) getter3.get("C"));

        driver.process(topic1, "A", 3);

        assertNull(getter2.get("A"));
        assertEquals(2, (int) getter2.get("B"));
        assertNull(getter2.get("C"));

        assertEquals(3, (int) getter3.get("A"));
        assertNull(getter3.get("B"));
        assertEquals(1, (int) getter3.get("C"));

        driver.process(topic1, "A", null);
        driver.process(topic1, "B", null);

        assertNull(getter2.get("A"));
        assertNull(getter2.get("B"));
        assertNull(getter2.get("C"));

        assertNull(getter3.get("A"));
        assertNull(getter3.get("B"));
        assertEquals(1, (int) getter3.get("C"));
    }

    @Test
    public void testNotSendingOldValue() throws IOException {
        KStreamBuilder builder = new KStreamBuilder();

        String topic1 = "topic1";

        KTableImpl<String, Integer, Integer> table1 =
                (KTableImpl<String, Integer, Integer>) builder.table(stringSerde, intSerde, topic1, "anyStoreName");
        KTableImpl<String, Integer, Integer> table2 = (KTableImpl<String, Integer, Integer>) table1.filter(
                new Predicate<String, Integer>() {
                    @Override
                    public boolean test(String key, Integer value) {
                        return (value % 2) == 0;
                    }
                });

        MockProcessorSupplier<String, Integer> proc1 = new MockProcessorSupplier<>();
        MockProcessorSupplier<String, Integer> proc2 = new MockProcessorSupplier<>();

        builder.addProcessor("proc1", proc1, table1.name);
        builder.addProcessor("proc2", proc2, table2.name);

        driver = new KStreamTestDriver(builder, stateDir, null, null);

        driver.process(topic1, "A", 1);
        driver.process(topic1, "B", 1);
        driver.process(topic1, "C", 1);
        driver.flushState();

        proc1.checkAndClearProcessResult("A:(1<-null)", "B:(1<-null)", "C:(1<-null)");
        proc2.checkAndClearProcessResult("A:(null<-null)", "B:(null<-null)", "C:(null<-null)");

        driver.process(topic1, "A", 2);
        driver.process(topic1, "B", 2);
        driver.flushState();
        proc1.checkAndClearProcessResult("A:(2<-null)", "B:(2<-null)");
        proc2.checkAndClearProcessResult("A:(2<-null)", "B:(2<-null)");

        driver.process(topic1, "A", 3);
        driver.flushState();
        proc1.checkAndClearProcessResult("A:(3<-null)");
        proc2.checkAndClearProcessResult("A:(null<-null)");

        driver.process(topic1, "A", null);
        driver.process(topic1, "B", null);
        driver.flushState();
        proc1.checkAndClearProcessResult("A:(null<-null)", "B:(null<-null)");
        proc2.checkAndClearProcessResult("A:(null<-null)", "B:(null<-null)");
    }

    @Test
    public void testSendingOldValue() throws IOException {
        KStreamBuilder builder = new KStreamBuilder();

        String topic1 = "topic1";

        KTableImpl<String, Integer, Integer> table1 =
                (KTableImpl<String, Integer, Integer>) builder.table(stringSerde, intSerde, topic1, "anyStoreName");
        KTableImpl<String, Integer, Integer> table2 = (KTableImpl<String, Integer, Integer>) table1.filter(
                new Predicate<String, Integer>() {
                    @Override
                    public boolean test(String key, Integer value) {
                        return (value % 2) == 0;
                    }
                });

        table2.enableSendingOldValues();

        MockProcessorSupplier<String, Integer> proc1 = new MockProcessorSupplier<>();
        MockProcessorSupplier<String, Integer> proc2 = new MockProcessorSupplier<>();

        builder.addProcessor("proc1", proc1, table1.name);
        builder.addProcessor("proc2", proc2, table2.name);

        driver = new KStreamTestDriver(builder, stateDir, null, null);

        driver.process(topic1, "A", 1);
        driver.process(topic1, "B", 1);
        driver.process(topic1, "C", 1);
        driver.flushState();

        proc1.checkAndClearProcessResult("A:(1<-null)", "B:(1<-null)", "C:(1<-null)");
        proc2.checkEmptyAndClearProcessResult();

        driver.process(topic1, "A", 2);
        driver.process(topic1, "B", 2);
        driver.flushState();
        proc1.checkAndClearProcessResult("A:(2<-1)", "B:(2<-1)");
        proc2.checkAndClearProcessResult("A:(2<-null)", "B:(2<-null)");

        driver.process(topic1, "A", 3);
        driver.flushState();
        proc1.checkAndClearProcessResult("A:(3<-2)");
        proc2.checkAndClearProcessResult("A:(null<-2)");

        driver.process(topic1, "A", null);
        driver.process(topic1, "B", null);
        driver.flushState();
        proc1.checkAndClearProcessResult("A:(null<-3)", "B:(null<-2)");
        proc2.checkAndClearProcessResult("B:(null<-2)");
    }

    @Test
    public void testSkipNullOnMaterialization() throws IOException {
        // Do not explicitly set enableSendingOldValues. Let a further downstream stateful operator trigger it instead.
        KStreamBuilder builder = new KStreamBuilder();

        String topic1 = "topic1";

        KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(stringSerde, stringSerde, topic1, "anyStoreName");
        KTableImpl<String, String, String> table2 = (KTableImpl<String, String, String>) table1.filter(
            new Predicate<String, String>() {
                @Override
                public boolean test(String key, String value) {
                    return value.equalsIgnoreCase("accept");
                }
            }).groupBy(MockKeyValueMapper.<String, String>NoOpKeyValueMapper())
            .reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER, "mock-result");

        MockProcessorSupplier<String, String> proc1 = new MockProcessorSupplier<>();
        MockProcessorSupplier<String, String> proc2 = new MockProcessorSupplier<>();

        builder.addProcessor("proc1", proc1, table1.name);
        builder.addProcessor("proc2", proc2, table2.name);

        driver = new KStreamTestDriver(builder, stateDir, stringSerde, stringSerde);

        driver.process(topic1, "A", "reject");
        driver.process(topic1, "B", "reject");
        driver.process(topic1, "C", "reject");
        driver.flushState();
        proc1.checkAndClearProcessResult("A:(reject<-null)", "B:(reject<-null)", "C:(reject<-null)");
        proc2.checkEmptyAndClearProcessResult();
    }
}
