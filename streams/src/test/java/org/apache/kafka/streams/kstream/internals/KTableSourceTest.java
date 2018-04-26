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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import static org.apache.kafka.test.StreamsTestUtils.getMetricByName;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class KTableSourceTest {

    final private Serde<String> stringSerde = Serdes.String();
    private final Consumed<String, String> stringConsumed = Consumed.with(stringSerde, stringSerde);
    final private Serde<Integer> intSerde = Serdes.Integer();
    @Rule
    public final KStreamTestDriver driver = new KStreamTestDriver();
    private File stateDir = null;

    @Before
    public void setUp() {
        stateDir = TestUtils.tempDirectory("kafka-test");
    }

    @Test
    public void testKTable() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";

        final KTable<String, Integer> table1 = builder.table(topic1, Consumed.with(stringSerde, intSerde));

        final MockProcessorSupplier<String, Integer> proc1 = new MockProcessorSupplier<>();
        table1.toStream().process(proc1);

        driver.setUp(builder, stateDir);
        driver.process(topic1, "A", 1);
        driver.process(topic1, "B", 2);
        driver.process(topic1, "C", 3);
        driver.process(topic1, "D", 4);
        driver.flushState();
        driver.process(topic1, "A", null);
        driver.process(topic1, "B", null);
        driver.flushState();

        assertEquals(Utils.mkList("A:1", "B:2", "C:3", "D:4", "A:null", "B:null"), proc1.processed);
    }

    @Test
    public void kTableShouldLogAndMeterOnSkippedRecords() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final String topic = "topic";
        streamsBuilder.table(topic, Consumed.with(stringSerde, intSerde));

        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
        driver.setUp(streamsBuilder, stateDir);
        driver.process(topic, null, "value");
        driver.flushState();
        LogCaptureAppender.unregister(appender);

        assertEquals(1.0, getMetricByName(driver.context().metrics().metrics(), "skipped-records-total", "stream-metrics").metricValue());
        assertThat(appender.getMessages(), hasItem("Skipping record due to null key. topic=[topic] partition=[-1] offset=[-1]"));
    }

    @Test
    public void testValueGetter() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";

        final KTableImpl<String, String, String> table1 = (KTableImpl<String, String, String>) builder.table(topic1, stringConsumed);

        final KTableValueGetterSupplier<String, String> getterSupplier1 = table1.valueGetterSupplier();

        driver.setUp(builder, stateDir);
        final KTableValueGetter<String, String> getter1 = getterSupplier1.get();
        getter1.init(driver.context());

        driver.process(topic1, "A", "01");
        driver.process(topic1, "B", "01");
        driver.process(topic1, "C", "01");

        assertEquals("01", getter1.get("A"));
        assertEquals("01", getter1.get("B"));
        assertEquals("01", getter1.get("C"));

        driver.process(topic1, "A", "02");
        driver.process(topic1, "B", "02");

        assertEquals("02", getter1.get("A"));
        assertEquals("02", getter1.get("B"));
        assertEquals("01", getter1.get("C"));

        driver.process(topic1, "A", "03");

        assertEquals("03", getter1.get("A"));
        assertEquals("02", getter1.get("B"));
        assertEquals("01", getter1.get("C"));

        driver.process(topic1, "A", null);
        driver.process(topic1, "B", null);

        assertNull(getter1.get("A"));
        assertNull(getter1.get("B"));
        assertEquals("01", getter1.get("C"));

    }

    @Test
    public void testNotSendingOldValue() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";

        final KTableImpl<String, String, String> table1 = (KTableImpl<String, String, String>) builder.table(topic1, stringConsumed);

        final MockProcessorSupplier<String, Integer> proc1 = new MockProcessorSupplier<>();

        builder.build().addProcessor("proc1", proc1, table1.name);

        driver.setUp(builder, stateDir);
        driver.process(topic1, "A", "01");
        driver.process(topic1, "B", "01");
        driver.process(topic1, "C", "01");
        driver.flushState();

        proc1.checkAndClearProcessResult("A:(01<-null)", "B:(01<-null)", "C:(01<-null)");

        driver.process(topic1, "A", "02");
        driver.process(topic1, "B", "02");
        driver.flushState();

        proc1.checkAndClearProcessResult("A:(02<-null)", "B:(02<-null)");

        driver.process(topic1, "A", "03");
        driver.flushState();

        proc1.checkAndClearProcessResult("A:(03<-null)");

        driver.process(topic1, "A", null);
        driver.process(topic1, "B", null);
        driver.flushState();

        proc1.checkAndClearProcessResult("A:(null<-null)", "B:(null<-null)");
    }

    @Test
    public void testSendingOldValue() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";

        final KTableImpl<String, String, String> table1 = (KTableImpl<String, String, String>) builder.table(topic1, stringConsumed);

        table1.enableSendingOldValues();

        assertTrue(table1.sendingOldValueEnabled());

        final MockProcessorSupplier<String, Integer> proc1 = new MockProcessorSupplier<>();

        builder.build().addProcessor("proc1", proc1, table1.name);

        driver.setUp(builder, stateDir);

        driver.process(topic1, "A", "01");
        driver.process(topic1, "B", "01");
        driver.process(topic1, "C", "01");
        driver.flushState();

        proc1.checkAndClearProcessResult("A:(01<-null)", "B:(01<-null)", "C:(01<-null)");

        driver.process(topic1, "A", "02");
        driver.process(topic1, "B", "02");
        driver.flushState();

        proc1.checkAndClearProcessResult("A:(02<-01)", "B:(02<-01)");

        driver.process(topic1, "A", "03");
        driver.flushState();

        proc1.checkAndClearProcessResult("A:(03<-02)");

        driver.process(topic1, "A", null);
        driver.process(topic1, "B", null);
        driver.flushState();

        proc1.checkAndClearProcessResult("A:(null<-03)", "B:(null<-02)");
    }
}
