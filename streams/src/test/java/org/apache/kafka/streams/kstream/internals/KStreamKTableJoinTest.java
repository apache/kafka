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
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import static org.apache.kafka.test.StreamsTestUtils.getMetricByName;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class KStreamKTableJoinTest {
    private final static KeyValueTimestamp[] EMPTY = new KeyValueTimestamp[0];

    private final String streamTopic = "streamTopic";
    private final String tableTopic = "tableTopic";
    private TestInputTopic<Integer, String> inputStreamTopic;
    private TestInputTopic<Integer, String> inputTableTopic;
    private final int[] expectedKeys = {0, 1, 2, 3};

    private MockProcessor<Integer, String> processor;
    private TopologyTestDriver driver;
    private StreamsBuilder builder;
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.String());
    private final MockProcessorSupplier<Integer, String> supplier = new MockProcessorSupplier<>();

    @Before
    public void setUp() {
        builder = new StreamsBuilder();

        final KStream<Integer, String> stream;
        final KTable<Integer, String> table;

        final Consumed<Integer, String> consumed = Consumed.with(Serdes.Integer(), Serdes.String());
        stream = builder.stream(streamTopic, consumed);
        table = builder.table(tableTopic, consumed);
        stream.join(table, MockValueJoiner.TOSTRING_JOINER).process(supplier);
        final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.String());
        driver = new TopologyTestDriver(builder.build(), props);
        inputStreamTopic = driver.createInputTopic(streamTopic, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
        inputTableTopic = driver.createInputTopic(tableTopic, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

        processor = supplier.theCapturedProcessor();
    }

    @After
    public void cleanup() {
        driver.close();
    }

    private void pushToStream(final int messageCount, final String valuePrefix) {
        for (int i = 0; i < messageCount; i++) {
            inputStreamTopic.pipeInput(expectedKeys[i], valuePrefix + expectedKeys[i], i);
        }
    }

    private void pushToTable(final int messageCount, final String valuePrefix) {
        final Random r = new Random(System.currentTimeMillis());
        for (int i = 0; i < messageCount; i++) {
            inputTableTopic.pipeInput(
                expectedKeys[i],
                valuePrefix + expectedKeys[i],
                r.nextInt(Integer.MAX_VALUE));
        }
    }

    private void pushNullValueToTable() {
        for (int i = 0; i < 2; i++) {
            inputTableTopic.pipeInput(expectedKeys[i], null);
        }
    }

    @Test
    public void shouldRequireCopartitionedStreams() {
        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(streamTopic, tableTopic)), copartitionGroups.iterator().next());
    }

    @Test
    public void shouldNotJoinWithEmptyTableOnStreamUpdates() {
        // push two items to the primary stream. the table is empty
        pushToStream(2, "X");
        processor.checkAndClearProcessResult(EMPTY);
    }

    @Test
    public void shouldNotJoinOnTableUpdates() {
        // push two items to the primary stream. the table is empty
        pushToStream(2, "X");
        processor.checkAndClearProcessResult(EMPTY);

        // push two items to the table. this should not produce any item.
        pushToTable(2, "Y");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce two items.
        pushToStream(4, "X");
        processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0+Y0", 0),
                new KeyValueTimestamp<>(1, "X1+Y1", 1));

        // push all items to the table. this should not produce any item
        pushToTable(4, "YY");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce four items.
        pushToStream(4, "X");
        processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0+YY0", 0),
                new KeyValueTimestamp<>(1, "X1+YY1", 1),
                new KeyValueTimestamp<>(2, "X2+YY2", 2),
                new KeyValueTimestamp<>(3, "X3+YY3", 3));

        // push all items to the table. this should not produce any item
        pushToTable(4, "YYY");
        processor.checkAndClearProcessResult(EMPTY);
    }

    @Test
    public void shouldJoinOnlyIfMatchFoundOnStreamUpdates() {
        // push two items to the table. this should not produce any item.
        pushToTable(2, "Y");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce two items.
        pushToStream(4, "X");
        processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0+Y0", 0),
                new KeyValueTimestamp<>(1, "X1+Y1", 1));
    }

    @Test
    public void shouldClearTableEntryOnNullValueUpdates() {
        // push all four items to the table. this should not produce any item.
        pushToTable(4, "Y");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce four items.
        pushToStream(4, "X");
        processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0+Y0", 0),
                new KeyValueTimestamp<>(1, "X1+Y1", 1),
                new KeyValueTimestamp<>(2, "X2+Y2", 2),
                new KeyValueTimestamp<>(3, "X3+Y3", 3));

        // push two items with null to the table as deletes. this should not produce any item.
        pushNullValueToTable();
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce two items.
        pushToStream(4, "XX");
        processor.checkAndClearProcessResult(new KeyValueTimestamp<>(2, "XX2+Y2", 2),
                new KeyValueTimestamp<>(3, "XX3+Y3", 3));
    }

    @Test
    public void shouldLogAndMeterWhenSkippingNullLeftKeyWithBuiltInMetricsVersion0100To24() {
        shouldLogAndMeterWhenSkippingNullLeftKey(StreamsConfig.METRICS_0100_TO_24);
    }

    @Test
    public void shouldLogAndMeterWhenSkippingNullLeftKeyWithBuiltInMetricsVersionLatest() {
        shouldLogAndMeterWhenSkippingNullLeftKey(StreamsConfig.METRICS_LATEST);
    }

    private void shouldLogAndMeterWhenSkippingNullLeftKey(final String builtInMetricsVersion) {
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
        props.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, builtInMetricsVersion);
        driver = new TopologyTestDriver(builder.build(), props);
        final TestInputTopic<Integer, String> inputTopic =
            driver.createInputTopic(streamTopic, new IntegerSerializer(), new StringSerializer());
        inputTopic.pipeInput(null, "A");
        LogCaptureAppender.unregister(appender);

        if (builtInMetricsVersion.equals(StreamsConfig.METRICS_0100_TO_24)) {
            assertEquals(
                1.0,
                getMetricByName(driver.metrics(), "skipped-records-total", "stream-metrics").metricValue()
            );
        }
        assertThat(
            appender.getMessages(),
            hasItem("Skipping record due to null key or value. key=[null] value=[A] topic=[streamTopic] partition=[0] "
                + "offset=[0]"));
    }

    @Test
    public void shouldLogAndMeterWhenSkippingNullLeftValueWithBuiltInMetricsVersionLatest() {
        shouldLogAndMeterWhenSkippingNullLeftValue(StreamsConfig.METRICS_LATEST);
    }

    @Test
    public void shouldLogAndMeterWhenSkippingNullLeftValueWithBuiltInMetricsVersion0100To24() {
        shouldLogAndMeterWhenSkippingNullLeftValue(StreamsConfig.METRICS_0100_TO_24);
    }

    private void shouldLogAndMeterWhenSkippingNullLeftValue(final String builtInMetricsVersion) {
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
        props.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, StreamsConfig.METRICS_0100_TO_24);
        driver = new TopologyTestDriver(builder.build(), props);
        final TestInputTopic<Integer, String> inputTopic =
            driver.createInputTopic(streamTopic, new IntegerSerializer(), new StringSerializer());
        inputTopic.pipeInput(1, null);
        LogCaptureAppender.unregister(appender);

        if (builtInMetricsVersion.equals(StreamsConfig.METRICS_0100_TO_24)) {
            assertEquals(
                1.0,
                getMetricByName(driver.metrics(), "skipped-records-total", "stream-metrics").metricValue()
            );
        }
        assertThat(
            appender.getMessages(),
            hasItem("Skipping record due to null key or value. key=[1] value=[null] topic=[streamTopic] partition=[0] "
                + "offset=[0]")
        );
    }
}
