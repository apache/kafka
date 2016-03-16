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
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class KTableFilterTest {

    final private Serde<Integer> intSerde = new Serdes.IntegerSerde();
    final private Serde<String> strSerde = new Serdes.StringSerde();

    @Test
    public void testKTable() {
        final KStreamBuilder builder = new KStreamBuilder();

        String topic1 = "topic1";

        KTable<String, Integer> table1 = builder.table(strSerde, intSerde, topic1);

        KTable<String, Integer> table2 = table1.filter(new Predicate<String, Integer>() {
            @Override
            public boolean test(String key, Integer value) {
                return (value % 2) == 0;
            }
        });
        KTable<String, Integer> table3 = table1.filterOut(new Predicate<String, Integer>() {
            @Override
            public boolean test(String key, Integer value) {
                return (value % 2) == 0;
            }
        });

        MockProcessorSupplier<String, Integer> proc2 = new MockProcessorSupplier<>();
        MockProcessorSupplier<String, Integer> proc3 = new MockProcessorSupplier<>();
        table2.toStream().process(proc2);
        table3.toStream().process(proc3);

        KStreamTestDriver driver = new KStreamTestDriver(builder);

        driver.process(topic1, "A", 1);
        driver.process(topic1, "B", 2);
        driver.process(topic1, "C", 3);
        driver.process(topic1, "D", 4);
        driver.process(topic1, "A", null);
        driver.process(topic1, "B", null);

        proc2.checkAndClearResult("A:null", "B:2", "C:null", "D:4", "A:null", "B:null");
        proc3.checkAndClearResult("A:1", "B:null", "C:3", "D:null", "A:null", "B:null");
    }

    @Test
    public void testValueGetter() throws IOException {
        File stateDir = Files.createTempDirectory("test").toFile();
        try {
            final KStreamBuilder builder = new KStreamBuilder();

            String topic1 = "topic1";

            KTableImpl<String, Integer, Integer> table1 =
                    (KTableImpl<String, Integer, Integer>) builder.table(strSerde, intSerde, topic1);
            KTableImpl<String, Integer, Integer> table2 = (KTableImpl<String, Integer, Integer>) table1.filter(
                    new Predicate<String, Integer>() {
                        @Override
                        public boolean test(String key, Integer value) {
                            return (value % 2) == 0;
                        }
                    });
            KTableImpl<String, Integer, Integer> table3 = (KTableImpl<String, Integer, Integer>) table1.filterOut(
                    new Predicate<String, Integer>() {
                        @Override
                        public boolean test(String key, Integer value) {
                            return (value % 2) == 0;
                        }
                    });

            KTableValueGetterSupplier<String, Integer> getterSupplier2 = table2.valueGetterSupplier();
            KTableValueGetterSupplier<String, Integer> getterSupplier3 = table3.valueGetterSupplier();

            KStreamTestDriver driver = new KStreamTestDriver(builder, stateDir, null, null, null, null);

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

        } finally {
            Utils.delete(stateDir);
        }
    }

    @Test
    public void testNotSendingOldValue() throws IOException {
        File stateDir = Files.createTempDirectory("test").toFile();
        try {
            final KStreamBuilder builder = new KStreamBuilder();

            String topic1 = "topic1";

            KTableImpl<String, Integer, Integer> table1 =
                    (KTableImpl<String, Integer, Integer>) builder.table(strSerde, intSerde, topic1);
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

            KStreamTestDriver driver = new KStreamTestDriver(builder, stateDir, null, null, null, null);

            driver.process(topic1, "A", 1);
            driver.process(topic1, "B", 1);
            driver.process(topic1, "C", 1);

            proc1.checkAndClearResult("A:(1<-null)", "B:(1<-null)", "C:(1<-null)");
            proc2.checkAndClearResult("A:(null<-null)", "B:(null<-null)", "C:(null<-null)");

            driver.process(topic1, "A", 2);
            driver.process(topic1, "B", 2);

            proc1.checkAndClearResult("A:(2<-null)", "B:(2<-null)");
            proc2.checkAndClearResult("A:(2<-null)", "B:(2<-null)");

            driver.process(topic1, "A", 3);

            proc1.checkAndClearResult("A:(3<-null)");
            proc2.checkAndClearResult("A:(null<-null)");

            driver.process(topic1, "A", null);
            driver.process(topic1, "B", null);

            proc1.checkAndClearResult("A:(null<-null)", "B:(null<-null)");
            proc2.checkAndClearResult("A:(null<-null)", "B:(null<-null)");

        } finally {
            Utils.delete(stateDir);
        }
    }

    @Test
    public void testSendingOldValue() throws IOException {
        File stateDir = Files.createTempDirectory("test").toFile();
        try {
            final KStreamBuilder builder = new KStreamBuilder();

            String topic1 = "topic1";

            KTableImpl<String, Integer, Integer> table1 =
                    (KTableImpl<String, Integer, Integer>) builder.table(strSerde, intSerde, topic1);
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

            KStreamTestDriver driver = new KStreamTestDriver(builder, stateDir, null, null, null, null);

            driver.process(topic1, "A", 1);
            driver.process(topic1, "B", 1);
            driver.process(topic1, "C", 1);

            proc1.checkAndClearResult("A:(1<-null)", "B:(1<-null)", "C:(1<-null)");
            proc2.checkAndClearResult("A:(null<-null)", "B:(null<-null)", "C:(null<-null)");

            driver.process(topic1, "A", 2);
            driver.process(topic1, "B", 2);

            proc1.checkAndClearResult("A:(2<-1)", "B:(2<-1)");
            proc2.checkAndClearResult("A:(2<-null)", "B:(2<-null)");

            driver.process(topic1, "A", 3);

            proc1.checkAndClearResult("A:(3<-2)");
            proc2.checkAndClearResult("A:(null<-2)");

            driver.process(topic1, "A", null);
            driver.process(topic1, "B", null);

            proc1.checkAndClearResult("A:(null<-3)", "B:(null<-2)");
            proc2.checkAndClearResult("A:(null<-null)", "B:(null<-2)");

        } finally {
            Utils.delete(stateDir);
        }
    }

}
