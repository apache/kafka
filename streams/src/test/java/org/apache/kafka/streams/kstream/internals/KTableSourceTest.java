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
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.Properties;

import static java.util.Arrays.asList;
import static org.apache.kafka.test.StreamsTestUtils.getMetricByName;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class KTableSourceTest {
    private final Consumed<String, String> stringConsumed = Consumed.with(Serdes.String(), Serdes.String());
    private final ConsumerRecordFactory<String, String> recordFactory =
        new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer(), 0L);
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    @Test
    public void testKTable() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, Integer> table1 = builder.table(topic1, Consumed.with(Serdes.String(), Serdes.Integer()));

        final MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();
        table1.toStream().process(supplier);

        final ConsumerRecordFactory<String, Integer> integerFactory =
            new ConsumerRecordFactory<>(new StringSerializer(), new IntegerSerializer(), 0L);
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(integerFactory.create(topic1, "A", 1, 10L));
            driver.pipeInput(integerFactory.create(topic1, "B", 2, 11L));
            driver.pipeInput(integerFactory.create(topic1, "C", 3, 12L));
            driver.pipeInput(integerFactory.create(topic1, "D", 4, 13L));
            driver.pipeInput(integerFactory.create(topic1, "A", null, 14L));
            driver.pipeInput(integerFactory.create(topic1, "B", null, 15L));
        }

        assertEquals(
            asList("A:1 (ts: 10)", "B:2 (ts: 11)", "C:3 (ts: 12)", "D:4 (ts: 13)", "A:null (ts: 14)", "B:null (ts: 15)"),
            supplier.theCapturedProcessor().processed);
    }

    @Test
    public void kTableShouldLogAndMeterOnSkippedRecords() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic = "topic";
        builder.table(topic, stringConsumed);

        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(topic, null, "value"));
            LogCaptureAppender.unregister(appender);

            assertEquals(1.0, getMetricByName(driver.metrics(), "skipped-records-total", "stream-metrics").metricValue());
            assertThat(appender.getMessages(), hasItem("Skipping record due to null key. topic=[topic] partition=[0] offset=[0]"));
        }
    }

    @Test
    public void testValueGetter() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        @SuppressWarnings("unchecked")
        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, stringConsumed, Materialized.as("store"));

        final Topology topology = builder.build();
        final KTableValueGetterSupplier<String, String> getterSupplier1 = table1.valueGetterSupplier();

        final InternalTopologyBuilder topologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);
        topologyBuilder.connectProcessorAndStateStores(table1.name, getterSupplier1.storeNames());

        try (final TopologyTestDriverWrapper driver = new TopologyTestDriverWrapper(builder.build(), props)) {
            final KTableValueGetter<String, String> getter1 = getterSupplier1.get();
            getter1.init(driver.setCurrentNodeForProcessorContext(table1.name));

            driver.pipeInput(recordFactory.create(topic1, "A", "01", 10L));
            driver.pipeInput(recordFactory.create(topic1, "B", "01", 20L));
            driver.pipeInput(recordFactory.create(topic1, "C", "01", 15L));

            assertEquals(ValueAndTimestamp.make("01", 10L), getter1.get("A"));
            assertEquals(ValueAndTimestamp.make("01", 20L), getter1.get("B"));
            assertEquals(ValueAndTimestamp.make("01", 15L), getter1.get("C"));

            driver.pipeInput(recordFactory.create(topic1, "A", "02", 30L));
            driver.pipeInput(recordFactory.create(topic1, "B", "02", 5L));

            assertEquals(ValueAndTimestamp.make("02", 30L), getter1.get("A"));
            assertEquals(ValueAndTimestamp.make("02", 5L), getter1.get("B"));
            assertEquals(ValueAndTimestamp.make("01", 15L), getter1.get("C"));

            driver.pipeInput(recordFactory.create(topic1, "A", "03", 29L));

            assertEquals(ValueAndTimestamp.make("03", 29L), getter1.get("A"));
            assertEquals(ValueAndTimestamp.make("02", 5L), getter1.get("B"));
            assertEquals(ValueAndTimestamp.make("01", 15L), getter1.get("C"));

            driver.pipeInput(recordFactory.create(topic1, "A", (String) null, 50L));
            driver.pipeInput(recordFactory.create(topic1, "B", (String) null, 3L));

            assertNull(getter1.get("A"));
            assertNull(getter1.get("B"));
            assertEquals(ValueAndTimestamp.make("01", 15L), getter1.get("C"));
        }
    }

    @Test
    public void testNotSendingOldValue() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        @SuppressWarnings("unchecked")
        final KTableImpl<String, String, String> table1 = (KTableImpl<String, String, String>) builder.table(topic1, stringConsumed);

        final MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();
        final Topology topology = builder.build().addProcessor("proc1", supplier, table1.name);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            final MockProcessor<String, Integer> proc1 = supplier.theCapturedProcessor();

            driver.pipeInput(recordFactory.create(topic1, "A", "01", 10L));
            driver.pipeInput(recordFactory.create(topic1, "B", "01", 20L));
            driver.pipeInput(recordFactory.create(topic1, "C", "01", 15L));
            proc1.checkAndClearProcessResult("A:(01<-null) (ts: 10)", "B:(01<-null) (ts: 20)", "C:(01<-null) (ts: 15)");

            driver.pipeInput(recordFactory.create(topic1, "A", "02", 8L));
            driver.pipeInput(recordFactory.create(topic1, "B", "02", 22L));
            proc1.checkAndClearProcessResult("A:(02<-null) (ts: 8)", "B:(02<-null) (ts: 22)");

            driver.pipeInput(recordFactory.create(topic1, "A", "03", 12L));
            proc1.checkAndClearProcessResult("A:(03<-null) (ts: 12)");

            driver.pipeInput(recordFactory.create(topic1, "A", (String) null, 15L));
            driver.pipeInput(recordFactory.create(topic1, "B", (String) null, 20L));
            proc1.checkAndClearProcessResult("A:(null<-null) (ts: 15)", "B:(null<-null) (ts: 20)");
        }
    }

    @Test
    public void testSendingOldValue() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        @SuppressWarnings("unchecked")
        final KTableImpl<String, String, String> table1 = (KTableImpl<String, String, String>) builder.table(topic1, stringConsumed);
        table1.enableSendingOldValues();
        assertTrue(table1.sendingOldValueEnabled());

        final MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();
        final Topology topology = builder.build().addProcessor("proc1", supplier, table1.name);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            final MockProcessor<String, Integer> proc1 = supplier.theCapturedProcessor();

            driver.pipeInput(recordFactory.create(topic1, "A", "01", 10L));
            driver.pipeInput(recordFactory.create(topic1, "B", "01", 20L));
            driver.pipeInput(recordFactory.create(topic1, "C", "01", 15L));
            proc1.checkAndClearProcessResult("A:(01<-null) (ts: 10)", "B:(01<-null) (ts: 20)", "C:(01<-null) (ts: 15)");

            driver.pipeInput(recordFactory.create(topic1, "A", "02", 8L));
            driver.pipeInput(recordFactory.create(topic1, "B", "02", 22L));
            proc1.checkAndClearProcessResult("A:(02<-01) (ts: 8)", "B:(02<-01) (ts: 22)");

            driver.pipeInput(recordFactory.create(topic1, "A", "03", 12L));
            proc1.checkAndClearProcessResult("A:(03<-02) (ts: 12)");

            driver.pipeInput(recordFactory.create(topic1, "A", (String) null, 15L));
            driver.pipeInput(recordFactory.create(topic1, "B", (String) null, 20L));
            proc1.checkAndClearProcessResult("A:(null<-03) (ts: 15)", "B:(null<-02) (ts: 20)");
        }
    }
}
