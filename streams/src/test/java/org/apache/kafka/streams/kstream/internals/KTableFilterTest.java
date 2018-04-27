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
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class KTableFilterTest {

    final private Serde<Integer> intSerde = Serdes.Integer();
    final private Serde<String> stringSerde = Serdes.String();
    private final Consumed<String, Integer> consumed = Consumed.with(stringSerde, intSerde);
    @Rule
    public final KStreamTestDriver driver = new KStreamTestDriver();
    private File stateDir = null;

    @Before
    public void setUp() {
        stateDir = TestUtils.tempDirectory("kafka-test");
    }

    private void doTestKTable(final StreamsBuilder builder,
                              final KTable<String, Integer> table2,
                              final KTable<String, Integer> table3,
                              final String topic) {
        MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();
        table2.toStream().process(supplier);
        table3.toStream().process(supplier);

        driver.setUp(builder, stateDir, Serdes.String(), Serdes.Integer());

        driver.process(topic, "A", 1);
        driver.process(topic, "B", 2);
        driver.process(topic, "C", 3);
        driver.process(topic, "D", 4);
        driver.flushState();
        driver.process(topic, "A", null);
        driver.process(topic, "B", null);
        driver.flushState();

        final List<MockProcessor<String, Integer>> processors = supplier.capturedProcessors(2);

        processors.get(0).checkAndClearProcessResult("A:null", "B:2", "C:null", "D:4", "A:null", "B:null");
        processors.get(1).checkAndClearProcessResult("A:1", "B:null", "C:3", "D:null", "A:null", "B:null");
    }

    @Test
    public void testKTable() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";

        KTable<String, Integer> table1 = builder.table(topic1, consumed);

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

        doTestKTable(builder, table2, table3, topic1);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testQueryableKTable() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";

        KTable<String, Integer> table1 = builder.table(topic1, consumed);

        KTable<String, Integer> table2 = table1.filter(new Predicate<String, Integer>() {
            @Override
            public boolean test(String key, Integer value) {
                return (value % 2) == 0;
            }
        }, "anyStoreNameFilter");
        KTable<String, Integer> table3 = table1.filterNot(new Predicate<String, Integer>() {
            @Override
            public boolean test(String key, Integer value) {
                return (value % 2) == 0;
            }
        });

        doTestKTable(builder, table2, table3, topic1);
    }

    @Test
    public void shouldAddQueryableStore() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";

        KTable<String, Integer> table1 = builder.table(topic1, consumed);

        KTable<String, Integer> table2 = table1.filter(new Predicate<String, Integer>() {
            @Override
            public boolean test(String key, Integer value) {
                return (value % 2) == 0;
            }
        }, Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("anyStoreNameFilter"));
        KTable<String, Integer> table3 = table1.filterNot(new Predicate<String, Integer>() {
            @Override
            public boolean test(String key, Integer value) {
                return (value % 2) == 0;
            }
        });

        doTestKTable(builder, table2, table3, topic1);
    }

    private void doTestValueGetter(final StreamsBuilder builder,
                                   final KTableImpl<String, Integer, Integer> table2,
                                   final KTableImpl<String, Integer, Integer> table3,
                                   final String topic1) {
        KTableValueGetterSupplier<String, Integer> getterSupplier2 = table2.valueGetterSupplier();
        KTableValueGetterSupplier<String, Integer> getterSupplier3 = table3.valueGetterSupplier();

        driver.setUp(builder, stateDir, Serdes.String(), Serdes.Integer());

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
        driver.flushState();

        assertEquals(2, (int) getter2.get("A"));
        assertEquals(2, (int) getter2.get("B"));
        assertNull(getter2.get("C"));

        assertNull(getter3.get("A"));
        assertNull(getter3.get("B"));
        assertEquals(1, (int) getter3.get("C"));

        driver.process(topic1, "A", 3);
        driver.flushState();

        assertNull(getter2.get("A"));
        assertEquals(2, (int) getter2.get("B"));
        assertNull(getter2.get("C"));

        assertEquals(3, (int) getter3.get("A"));
        assertNull(getter3.get("B"));
        assertEquals(1, (int) getter3.get("C"));

        driver.process(topic1, "A", null);
        driver.process(topic1, "B", null);
        driver.flushState();

        assertNull(getter2.get("A"));
        assertNull(getter2.get("B"));
        assertNull(getter2.get("C"));

        assertNull(getter3.get("A"));
        assertNull(getter3.get("B"));
        assertEquals(1, (int) getter3.get("C"));
    }

    @Test
    public void testValueGetter() {
        StreamsBuilder builder = new StreamsBuilder();

        String topic1 = "topic1";

        KTableImpl<String, Integer, Integer> table1 =
                (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed);
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

        doTestValueGetter(builder, table2, table3, topic1);
    }

    @Test
    public void testQueryableValueGetter() {
        StreamsBuilder builder = new StreamsBuilder();

        String topic1 = "topic1";

        KTableImpl<String, Integer, Integer> table1 =
            (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed);
        KTableImpl<String, Integer, Integer> table2 = (KTableImpl<String, Integer, Integer>) table1.filter(
            new Predicate<String, Integer>() {
                @Override
                public boolean test(String key, Integer value) {
                    return (value % 2) == 0;
                }
            }, "anyStoreNameFilter");
        KTableImpl<String, Integer, Integer> table3 = (KTableImpl<String, Integer, Integer>) table1.filterNot(
            new Predicate<String, Integer>() {
                @Override
                public boolean test(String key, Integer value) {
                    return (value % 2) == 0;
                }
            });

        doTestValueGetter(builder, table2, table3, topic1);
    }

    private void doTestNotSendingOldValue(final StreamsBuilder builder,
                                          final KTableImpl<String, Integer, Integer> table1,
                                          final KTableImpl<String, Integer, Integer> table2,
                                          final String topic1) {
        MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();

        builder.build().addProcessor("proc1", supplier, table1.name);
        builder.build().addProcessor("proc2", supplier, table2.name);

        driver.setUp(builder, stateDir, Serdes.String(), Serdes.Integer());

        driver.process(topic1, "A", 1);
        driver.process(topic1, "B", 1);
        driver.process(topic1, "C", 1);
        driver.flushState();

        final List<MockProcessor<String, Integer>> processors = supplier.capturedProcessors(2);

        processors.get(0).checkAndClearProcessResult("A:(1<-null)", "B:(1<-null)", "C:(1<-null)");
        processors.get(1).checkAndClearProcessResult("A:(null<-null)", "B:(null<-null)", "C:(null<-null)");

        driver.process(topic1, "A", 2);
        driver.process(topic1, "B", 2);
        driver.flushState();
        processors.get(0).checkAndClearProcessResult("A:(2<-null)", "B:(2<-null)");
        processors.get(1).checkAndClearProcessResult("A:(2<-null)", "B:(2<-null)");

        driver.process(topic1, "A", 3);
        driver.flushState();
        processors.get(0).checkAndClearProcessResult("A:(3<-null)");
        processors.get(1).checkAndClearProcessResult("A:(null<-null)");

        driver.process(topic1, "A", null);
        driver.process(topic1, "B", null);
        driver.flushState();
        processors.get(0).checkAndClearProcessResult("A:(null<-null)", "B:(null<-null)");
        processors.get(1).checkAndClearProcessResult("A:(null<-null)", "B:(null<-null)");
    }


    @Test
    public void testNotSendingOldValue() {
        StreamsBuilder builder = new StreamsBuilder();

        String topic1 = "topic1";

        KTableImpl<String, Integer, Integer> table1 =
                (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed);
        KTableImpl<String, Integer, Integer> table2 = (KTableImpl<String, Integer, Integer>) table1.filter(
                new Predicate<String, Integer>() {
                    @Override
                    public boolean test(String key, Integer value) {
                        return (value % 2) == 0;
                    }
                });

        doTestNotSendingOldValue(builder, table1, table2, topic1);
    }

    @Test
    public void testQueryableNotSendingOldValue() {
        StreamsBuilder builder = new StreamsBuilder();

        String topic1 = "topic1";

        KTableImpl<String, Integer, Integer> table1 =
            (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed);
        KTableImpl<String, Integer, Integer> table2 = (KTableImpl<String, Integer, Integer>) table1.filter(
            new Predicate<String, Integer>() {
                @Override
                public boolean test(String key, Integer value) {
                    return (value % 2) == 0;
                }
            }, "anyStoreNameFilter");

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

        driver.setUp(builder, stateDir, Serdes.String(), Serdes.Integer());

        driver.process(topic1, "A", 1);
        driver.process(topic1, "B", 1);
        driver.process(topic1, "C", 1);
        driver.flushState();

        final List<MockProcessor<String, Integer>> processors = supplier.capturedProcessors(2);

        processors.get(0).checkAndClearProcessResult("A:(1<-null)", "B:(1<-null)", "C:(1<-null)");
        processors.get(1).checkEmptyAndClearProcessResult();

        driver.process(topic1, "A", 2);
        driver.process(topic1, "B", 2);
        driver.flushState();
        processors.get(0).checkAndClearProcessResult("A:(2<-1)", "B:(2<-1)");
        processors.get(1).checkAndClearProcessResult("A:(2<-null)", "B:(2<-null)");

        driver.process(topic1, "A", 3);
        driver.flushState();
        processors.get(0).checkAndClearProcessResult("A:(3<-2)");
        processors.get(1).checkAndClearProcessResult("A:(null<-2)");

        driver.process(topic1, "A", null);
        driver.process(topic1, "B", null);
        driver.flushState();
        processors.get(0).checkAndClearProcessResult("A:(null<-3)", "B:(null<-2)");
        processors.get(1).checkAndClearProcessResult("B:(null<-2)");
    }

    @Test
    public void testSendingOldValue() {
        StreamsBuilder builder = new StreamsBuilder();

        String topic1 = "topic1";

        KTableImpl<String, Integer, Integer> table1 =
                (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed);
        KTableImpl<String, Integer, Integer> table2 = (KTableImpl<String, Integer, Integer>) table1.filter(
                new Predicate<String, Integer>() {
                    @Override
                    public boolean test(String key, Integer value) {
                        return (value % 2) == 0;
                    }
                });

        doTestSendingOldValue(builder, table1, table2, topic1);
    }

    @Test
    public void testQueryableSendingOldValue() {
        StreamsBuilder builder = new StreamsBuilder();

        String topic1 = "topic1";

        KTableImpl<String, Integer, Integer> table1 =
            (KTableImpl<String, Integer, Integer>) builder.table(topic1, consumed);
        KTableImpl<String, Integer, Integer> table2 = (KTableImpl<String, Integer, Integer>) table1.filter(
            new Predicate<String, Integer>() {
                @Override
                public boolean test(String key, Integer value) {
                    return (value % 2) == 0;
                }
            }, "anyStoreNameFilter");

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

        driver.setUp(builder, stateDir, stringSerde, stringSerde);

        driver.process(topic1, "A", "reject");
        driver.process(topic1, "B", "reject");
        driver.process(topic1, "C", "reject");
        driver.flushState();

        final List<MockProcessor<String, String>> processors = supplier.capturedProcessors(2);
        processors.get(0).checkAndClearProcessResult("A:(reject<-null)", "B:(reject<-null)", "C:(reject<-null)");
        processors.get(1).checkEmptyAndClearProcessResult();
    }

    @Test
    public void testSkipNullOnMaterialization() {
        // Do not explicitly set enableSendingOldValues. Let a further downstream stateful operator trigger it instead.
        StreamsBuilder builder = new StreamsBuilder();

        String topic1 = "topic1";

        final Consumed<String, String> consumed = Consumed.with(stringSerde, stringSerde);
        KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        KTableImpl<String, String, String> table2 = (KTableImpl<String, String, String>) table1.filter(
            new Predicate<String, String>() {
                @Override
                public boolean test(String key, String value) {
                    return value.equalsIgnoreCase("accept");
                }
            }).groupBy(MockMapper.<String, String>noOpKeyValueMapper())
            .reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER, "mock-result");

        doTestSkipNullOnMaterialization(builder, table1, table2, topic1);
    }

    @Test
    public void testQueryableSkipNullOnMaterialization() {
        // Do not explicitly set enableSendingOldValues. Let a further downstream stateful operator trigger it instead.
        StreamsBuilder builder = new StreamsBuilder();

        String topic1 = "topic1";

        final Consumed<String, String> consumed = Consumed.with(stringSerde, stringSerde);
        KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        KTableImpl<String, String, String> table2 = (KTableImpl<String, String, String>) table1.filter(
            new Predicate<String, String>() {
                @Override
                public boolean test(String key, String value) {
                    return value.equalsIgnoreCase("accept");
                }
            }, "anyStoreNameFilter").groupBy(MockMapper.<String, String>noOpKeyValueMapper())
            .reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER, "mock-result");

        doTestSkipNullOnMaterialization(builder, table1, table2, topic1);
    }

    @Test
    public void testTypeVariance() {
        Predicate<Number, Object> numberKeyPredicate = new Predicate<Number, Object>() {
            @Override
            public boolean test(Number key, Object value) {
                return false;
            }
        };

        new StreamsBuilder()
            .<Integer, String>table("empty")
            .filter(numberKeyPredicate)
            .filterNot(numberKeyPredicate)
            .to("nirvana");
    }
}
