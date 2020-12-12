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

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
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
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.MockApiProcessor;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static java.util.Arrays.asList;
import static org.apache.kafka.test.StreamsTestUtils.getMetricByName;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class KTableSourceTest {
    private final Consumed<String, String> stringConsumed = Consumed.with(Serdes.String(), Serdes.String());
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    @Test
    public void testKTable() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, Integer> table1 = builder.table(topic1, Consumed.with(Serdes.String(), Serdes.Integer()));

        final MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();
        table1.toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, Integer> inputTopic =
                    driver.createInputTopic(topic1, new StringSerializer(), new IntegerSerializer());
            inputTopic.pipeInput("A", 1, 10L);
            inputTopic.pipeInput("B", 2, 11L);
            inputTopic.pipeInput("C", 3, 12L);
            inputTopic.pipeInput("D", 4, 13L);
            inputTopic.pipeInput("A", null, 14L);
            inputTopic.pipeInput("B", null, 15L);
        }

        assertEquals(
            asList(new KeyValueTimestamp<>("A", 1, 10L),
                new KeyValueTimestamp<>("B", 2, 11L),
                new KeyValueTimestamp<>("C", 3, 12L),
                new KeyValueTimestamp<>("D", 4, 13L),
                new KeyValueTimestamp<>("A", null, 14L),
                new KeyValueTimestamp<>("B", null, 15L)),
            supplier.theCapturedProcessor().processed());
    }

    @Test
    public void testKTableSourceEmitOnChange() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        builder.table(topic1, Consumed.with(Serdes.String(), Serdes.Integer()), Materialized.as("store"))
               .toStream()
               .to("output");

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, Integer> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new IntegerSerializer());
            final TestOutputTopic<String, Integer> outputTopic =
                driver.createOutputTopic("output", new StringDeserializer(), new IntegerDeserializer());

            inputTopic.pipeInput("A", 1, 10L);
            inputTopic.pipeInput("B", 2, 11L);
            inputTopic.pipeInput("A", 1, 12L);
            inputTopic.pipeInput("B", 3, 13L);
            // this record should be kept since this is out of order, so the timestamp
            // should be updated in this scenario
            inputTopic.pipeInput("A", 1, 9L);

            assertEquals(
                1.0,
                getMetricByName(driver.metrics(), "idempotent-update-skip-total", "stream-processor-node-metrics").metricValue()
            );

            assertEquals(
                asList(new TestRecord<>("A", 1, Instant.ofEpochMilli(10L)),
                           new TestRecord<>("B", 2, Instant.ofEpochMilli(11L)),
                           new TestRecord<>("B", 3, Instant.ofEpochMilli(13L)),
                           new TestRecord<>("A", 1, Instant.ofEpochMilli(9L))),
                outputTopic.readRecordsToList()
            );
        }
    }

    @Test
    public void kTableShouldLogAndMeterOnSkippedRecordsWithBuiltInMetrics0100To24() {
        kTableShouldLogAndMeterOnSkippedRecords(StreamsConfig.METRICS_0100_TO_24);
    }

    @Test
    public void kTableShouldLogAndMeterOnSkippedRecordsWithBuiltInMetricsLatest() {
        kTableShouldLogAndMeterOnSkippedRecords(StreamsConfig.METRICS_LATEST);
    }

    private void kTableShouldLogAndMeterOnSkippedRecords(final String builtInMetricsVersion) {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic = "topic";
        builder.table(topic, stringConsumed);

        props.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, builtInMetricsVersion);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KTableSource.class);
             final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {

            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(
                    topic,
                    new StringSerializer(),
                    new StringSerializer(),
                    Instant.ofEpochMilli(0L),
                    Duration.ZERO
                );
            inputTopic.pipeInput(null, "value");

            if (StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion)) {
                assertEquals(
                    1.0,
                    getMetricByName(driver.metrics(), "skipped-records-total", "stream-metrics").metricValue()
                );
            }

            assertThat(
                appender.getMessages(),
                hasItem("Skipping record due to null key. topic=[topic] partition=[0] offset=[0]")
            );
        }
    }

    @Test
    public void kTableShouldLogAndMeterOnSkippedRecords() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic = "topic";
        builder.table(topic, stringConsumed);

        props.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, StreamsConfig.METRICS_0100_TO_24);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KTableSource.class);
             final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {

            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(
                    topic,
                    new StringSerializer(),
                    new StringSerializer(),
                    Instant.ofEpochMilli(0L),
                    Duration.ZERO
                );
            inputTopic.pipeInput(null, "value");

            assertThat(
                getMetricByName(driver.metrics(), "skipped-records-total", "stream-metrics").metricValue(),
                equalTo(1.0)
            );

            assertThat(
                appender.getMessages(),
                hasItem("Skipping record due to null key. topic=[topic] partition=[0] offset=[0]")
            );
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
            final TestInputTopic<String, String> inputTopic1 =
                driver.createInputTopic(
                    topic1,
                    new StringSerializer(),
                    new StringSerializer(),
                    Instant.ofEpochMilli(0L),
                    Duration.ZERO
                );
            final KTableValueGetter<String, String> getter1 = getterSupplier1.get();
            getter1.init(driver.setCurrentNodeForProcessorContext(table1.name));

            inputTopic1.pipeInput("A", "01", 10L);
            inputTopic1.pipeInput("B", "01", 20L);
            inputTopic1.pipeInput("C", "01", 15L);

            assertEquals(ValueAndTimestamp.make("01", 10L), getter1.get("A"));
            assertEquals(ValueAndTimestamp.make("01", 20L), getter1.get("B"));
            assertEquals(ValueAndTimestamp.make("01", 15L), getter1.get("C"));

            inputTopic1.pipeInput("A", "02", 30L);
            inputTopic1.pipeInput("B", "02", 5L);

            assertEquals(ValueAndTimestamp.make("02", 30L), getter1.get("A"));
            assertEquals(ValueAndTimestamp.make("02", 5L), getter1.get("B"));
            assertEquals(ValueAndTimestamp.make("01", 15L), getter1.get("C"));

            inputTopic1.pipeInput("A", "03", 29L);

            assertEquals(ValueAndTimestamp.make("03", 29L), getter1.get("A"));
            assertEquals(ValueAndTimestamp.make("02", 5L), getter1.get("B"));
            assertEquals(ValueAndTimestamp.make("01", 15L), getter1.get("C"));

            inputTopic1.pipeInput("A", null, 50L);
            inputTopic1.pipeInput("B", null, 3L);

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
        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, stringConsumed);

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final Topology topology = builder.build().addProcessor("proc1", supplier, table1.name);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            final TestInputTopic<String, String> inputTopic1 =
                driver.createInputTopic(
                    topic1,
                    new StringSerializer(),
                    new StringSerializer(),
                    Instant.ofEpochMilli(0L),
                    Duration.ZERO
                );
            final MockApiProcessor<String, Integer, Void, Void> proc1 = supplier.theCapturedProcessor();

            inputTopic1.pipeInput("A", "01", 10L);
            inputTopic1.pipeInput("B", "01", 20L);
            inputTopic1.pipeInput("C", "01", 15L);
            proc1.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>("01", null), 10),
                new KeyValueTimestamp<>("B", new Change<>("01", null), 20),
                new KeyValueTimestamp<>("C", new Change<>("01", null), 15)
            );

            inputTopic1.pipeInput("A", "02", 8L);
            inputTopic1.pipeInput("B", "02", 22L);
            proc1.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>("02", null), 8),
                new KeyValueTimestamp<>("B", new Change<>("02", null), 22)
            );

            inputTopic1.pipeInput("A", "03", 12L);
            proc1.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>("03", null), 12)
            );

            inputTopic1.pipeInput("A", null, 15L);
            inputTopic1.pipeInput("B", null, 20L);
            proc1.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>(null, null), 15),
                new KeyValueTimestamp<>("B", new Change<>(null, null), 20)
            );
        }
    }

    @Test
    public void testSendingOldValue() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        @SuppressWarnings("unchecked")
        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, stringConsumed);
        table1.enableSendingOldValues(true);
        assertTrue(table1.sendingOldValueEnabled());

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final Topology topology = builder.build().addProcessor("proc1", supplier, table1.name);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            final TestInputTopic<String, String> inputTopic1 =
                driver.createInputTopic(
                    topic1,
                    new StringSerializer(),
                    new StringSerializer(),
                    Instant.ofEpochMilli(0L),
                    Duration.ZERO
                );
            final MockApiProcessor<String, Integer, Void, Void> proc1 = supplier.theCapturedProcessor();

            inputTopic1.pipeInput("A", "01", 10L);
            inputTopic1.pipeInput("B", "01", 20L);
            inputTopic1.pipeInput("C", "01", 15L);
            proc1.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>("01", null), 10),
                new KeyValueTimestamp<>("B", new Change<>("01", null), 20),
                new KeyValueTimestamp<>("C", new Change<>("01", null), 15)
            );

            inputTopic1.pipeInput("A", "02", 8L);
            inputTopic1.pipeInput("B", "02", 22L);
            proc1.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>("02", "01"), 8),
                new KeyValueTimestamp<>("B", new Change<>("02", "01"), 22)
            );

            inputTopic1.pipeInput("A", "03", 12L);
            proc1.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>("03", "02"), 12)
            );

            inputTopic1.pipeInput("A", null, 15L);
            inputTopic1.pipeInput("B", null, 20L);
            proc1.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>(null, "03"), 15),
                new KeyValueTimestamp<>("B", new Change<>(null, "02"), 20)
            );
        }
    }
}
