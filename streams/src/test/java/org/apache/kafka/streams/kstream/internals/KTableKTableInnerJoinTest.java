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
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.apache.kafka.test.StreamsTestUtils.getMetricByName;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class KTableKTableInnerJoinTest {

    private final String topic1 = "topic1";
    private final String topic2 = "topic2";

    private final Serde<Integer> intSerde = Serdes.Integer();
    private final Serde<String> stringSerde = Serdes.String();
    private final Consumed<Integer, String> consumed = Consumed.with(intSerde, stringSerde);
    private final Materialized<Integer, String, KeyValueStore<Bytes, byte[]>> materialized = Materialized.with(intSerde, stringSerde);

    private File stateDir = null;

    @SuppressWarnings("deprecation")
    @Rule
    public final org.apache.kafka.test.KStreamTestDriver driver = new org.apache.kafka.test.KStreamTestDriver();

    @Before
    public void setUp() {
        stateDir = TestUtils.tempDirectory("kafka-test");
    }

    @Test
    public void testJoin() {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        final KTable<Integer, String> table1;
        final KTable<Integer, String> table2;
        final KTable<Integer, String> joined;
        final MockProcessorSupplier<Integer, String> supplier = new MockProcessorSupplier<>();
        table1 = builder.table(topic1, consumed);
        table2 = builder.table(topic2, consumed);
        joined = table1.join(table2, MockValueJoiner.TOSTRING_JOINER);
        joined.toStream().process(supplier);

        doTestJoin(builder, expectedKeys, supplier, joined);
    }

    @Test
    public void testQueryableJoin() {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        final KTable<Integer, String> table1;
        final KTable<Integer, String> table2;
        final KTable<Integer, String> table3;
        final MockProcessorSupplier<Integer, String> processor = new MockProcessorSupplier<>();
        table1 = builder.table(topic1, consumed);
        table2 = builder.table(topic2, consumed);
        table3 = table1.join(table2, MockValueJoiner.TOSTRING_JOINER, materialized);
        table3.toStream().process(processor);

        doTestJoin(builder, expectedKeys, processor, table3);
    }

    @Test
    public void testQueryableNotSendingOldValues() {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        final KTable<Integer, String> table1;
        final KTable<Integer, String> table2;
        final KTable<Integer, String> joined;
        final MockProcessorSupplier<Integer, String> supplier = new MockProcessorSupplier<>();

        table1 = builder.table(topic1, consumed);
        table2 = builder.table(topic2, consumed);
        joined = table1.join(table2, MockValueJoiner.TOSTRING_JOINER, materialized);
        builder.build().addProcessor("proc", supplier, ((KTableImpl<?, ?, ?>) joined).name);

        doTestNotSendingOldValues(builder, expectedKeys, table1, table2, supplier, joined);
    }

    @Test
    public void testNotSendingOldValues() {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        final KTable<Integer, String> table1;
        final KTable<Integer, String> table2;
        final KTable<Integer, String> joined;
        final MockProcessorSupplier<Integer, String> supplier = new MockProcessorSupplier<>();

        table1 = builder.table(topic1, consumed);
        table2 = builder.table(topic2, consumed);
        joined = table1.join(table2, MockValueJoiner.TOSTRING_JOINER);
        builder.build().addProcessor("proc", supplier, ((KTableImpl<?, ?, ?>) joined).name);

        doTestNotSendingOldValues(builder, expectedKeys, table1, table2, supplier, joined);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldLogAndMeterSkippedRecordsDueToNullLeftKey() {
        final StreamsBuilder builder = new StreamsBuilder();

        final Processor<String, Change<String>> join = new KTableKTableInnerJoin<>(
            (KTableImpl<String, String, String>) builder.table("left", Consumed.with(stringSerde, stringSerde)),
            (KTableImpl<String, String, String>) builder.table("right", Consumed.with(stringSerde, stringSerde)),
            null
        ).get();

        final MockProcessorContext context = new MockProcessorContext();
        context.setRecordMetadata("left", -1, -2, null, -3);
        join.init(context);
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
        join.process(null, new Change<>("new", "old"));
        LogCaptureAppender.unregister(appender);

        assertEquals(1.0, getMetricByName(context.metrics().metrics(), "skipped-records-total", "stream-metrics").metricValue());
        assertThat(appender.getMessages(), hasItem("Skipping record due to null key. change=[(new<-old)] topic=[left] partition=[-1] offset=[-2]"));
    }

    private void doTestNotSendingOldValues(final StreamsBuilder builder,
                                           final int[] expectedKeys,
                                           final KTable<Integer, String> table1,
                                           final KTable<Integer, String> table2,
                                           final MockProcessorSupplier<Integer, String> supplier,
                                           final KTable<Integer, String> joined) {

        assertFalse(((KTableImpl<?, ?, ?>) table1).sendingOldValueEnabled());
        assertFalse(((KTableImpl<?, ?, ?>) table2).sendingOldValueEnabled());
        assertFalse(((KTableImpl<?, ?, ?>) joined).sendingOldValueEnabled());

        driver.setUp(builder, stateDir, Serdes.Integer(), Serdes.String());
        driver.setTime(0L);

        final MockProcessor<Integer, String> proc = supplier.theCapturedProcessor();

        // push two items to the primary stream. the other table is empty

        for (int i = 0; i < 2; i++) {
            driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
        }
        driver.flushState();

        proc.checkAndClearProcessResult();

        // push two items to the other stream. this should produce two items.

        for (int i = 0; i < 2; i++) {
            driver.process(topic2, expectedKeys[i], "Y" + expectedKeys[i]);
        }
        driver.flushState();

        proc.checkAndClearProcessResult("0:(X0+Y0<-null)", "1:(X1+Y1<-null)");

        // push all four items to the primary stream. this should produce two items.

        for (final int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }
        driver.flushState();

        proc.checkAndClearProcessResult("0:(XX0+Y0<-null)", "1:(XX1+Y1<-null)");

        // push all items to the other stream. this should produce four items.
        for (final int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }
        driver.flushState();
        proc.checkAndClearProcessResult("0:(XX0+YY0<-null)", "1:(XX1+YY1<-null)", "2:(XX2+YY2<-null)", "3:(XX3+YY3<-null)");

        // push all four items to the primary stream. this should produce four items.

        for (final int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "X" + expectedKey);
        }
        driver.flushState();
        proc.checkAndClearProcessResult("0:(X0+YY0<-null)", "1:(X1+YY1<-null)", "2:(X2+YY2<-null)", "3:(X3+YY3<-null)");

        // push two items with null to the other stream as deletes. this should produce two item.

        for (int i = 0; i < 2; i++) {
            driver.process(topic2, expectedKeys[i], null);
        }
        driver.flushState();
        proc.checkAndClearProcessResult("0:(null<-null)", "1:(null<-null)");

        // push all four items to the primary stream. this should produce two items.

        for (final int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }
        driver.flushState();
        proc.checkAndClearProcessResult("2:(XX2+YY2<-null)", "3:(XX3+YY3<-null)");
    }

    @SuppressWarnings("unchecked")
    private void doTestJoin(final StreamsBuilder builder,
                            final int[] expectedKeys,
                            final MockProcessorSupplier<Integer, String> supplier,
                            final KTable<Integer, String> joined) {
        final Collection<Set<String>> copartitionGroups = TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        final KTableValueGetterSupplier<Integer, String> getterSupplier = ((KTableImpl<Integer, String, String>) joined).valueGetterSupplier();

        driver.setUp(builder, stateDir, Serdes.Integer(), Serdes.String());
        driver.setTime(0L);

        final MockProcessor<Integer, String> processor = supplier.theCapturedProcessor();

        final KTableValueGetter<Integer, String> getter = getterSupplier.get();
        getter.init(driver.context());

        // push two items to the primary stream. the other table is empty

        for (int i = 0; i < 2; i++) {
            driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
        }
        // pass tuple with null key, it will be discarded in join process
        driver.process(topic1, null, "SomeVal");
        driver.flushState();

        processor.checkAndClearProcessResult();

        // push two items to the other stream. this should produce two items.

        for (int i = 0; i < 2; i++) {
            driver.process(topic2, expectedKeys[i], "Y" + expectedKeys[i]);
        }
        // pass tuple with null key, it will be discarded in join process
        driver.process(topic2, null, "AnotherVal");
        driver.flushState();

        processor.checkAndClearProcessResult("0:X0+Y0", "1:X1+Y1");
        checkJoinedValues(getter, kv(0, "X0+Y0"), kv(1, "X1+Y1"));

        // push all four items to the primary stream. this should produce two items.

        for (final int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }
        driver.flushState();

        processor.checkAndClearProcessResult("0:XX0+Y0", "1:XX1+Y1");
        checkJoinedValues(getter, kv(0, "XX0+Y0"), kv(1, "XX1+Y1"));

        // push all items to the other stream. this should produce four items.
        for (final int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }
        driver.flushState();

        processor.checkAndClearProcessResult("0:XX0+YY0", "1:XX1+YY1", "2:XX2+YY2", "3:XX3+YY3");
        checkJoinedValues(getter, kv(0, "XX0+YY0"), kv(1, "XX1+YY1"), kv(2, "XX2+YY2"), kv(3, "XX3+YY3"));

        // push all four items to the primary stream. this should produce four items.

        for (final int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "X" + expectedKey);
        }
        driver.flushState();

        processor.checkAndClearProcessResult("0:X0+YY0", "1:X1+YY1", "2:X2+YY2", "3:X3+YY3");
        checkJoinedValues(getter, kv(0, "X0+YY0"), kv(1, "X1+YY1"), kv(2, "X2+YY2"), kv(3, "X3+YY3"));

        // push two items with null to the other stream as deletes. this should produce two item.

        for (int i = 0; i < 2; i++) {
            driver.process(topic2, expectedKeys[i], null);
        }
        driver.flushState();

        processor.checkAndClearProcessResult("0:null", "1:null");
        checkJoinedValues(getter, kv(0, null), kv(1, null));

        // push all four items to the primary stream. this should produce two items.

        for (final int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }
        driver.flushState();

        processor.checkAndClearProcessResult("2:XX2+YY2", "3:XX3+YY3");
        checkJoinedValues(getter, kv(2, "XX2+YY2"), kv(3, "XX3+YY3"));

        driver.process(topic1, null, "XX" + 1);
        checkJoinedValues(getter, kv(2, "XX2+YY2"), kv(3, "XX3+YY3"));

    }


    private KeyValue<Integer, String> kv(final Integer key, final String value) {
        return new KeyValue<>(key, value);
    }

    @SafeVarargs
    private final void checkJoinedValues(final KTableValueGetter<Integer, String> getter, final KeyValue<Integer, String>... expected) {
        for (final KeyValue<Integer, String> kv : expected) {
            final String value = getter.get(kv.key);
            if (kv.value == null) {
                assertNull(value);
            } else {
                assertEquals(kv.value, value);
            }
        }
    }

}
