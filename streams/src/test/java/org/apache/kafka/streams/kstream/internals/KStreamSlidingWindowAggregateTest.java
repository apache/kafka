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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KStreamSlidingWindowAggregateTest {
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
    private final String threadId = Thread.currentThread().getName();

    @Test
    public void testAggBasic() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<Windowed<String>, String> table2 = builder
                .stream(topic1, Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(SlidingWindows.withTimeDifferenceAndGrace(ofMillis(10), ofMillis(50)))
                .aggregate(
                        MockInitializer.STRING_INIT,
                        MockAggregator.TOSTRING_ADDER,
                        Materialized.<String, String, WindowStore<Bytes, byte[]>>as("topic1-Canonized").withValueSerde(Serdes.String())
                );
        final MockProcessorSupplier<Windowed<String>, String> supplier = new MockProcessorSupplier<>();
        table2.toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                    driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer());
            inputTopic1.pipeInput("A", "1", 10L);
            inputTopic1.pipeInput("A", "2", 20L);
            inputTopic1.pipeInput("A", "3", 22L);
            inputTopic1.pipeInput("A", "4", 15L);

            inputTopic1.pipeInput("B", "1", 12L);
            inputTopic1.pipeInput("B", "2", 13L);
            inputTopic1.pipeInput("B", "3", 18L);
            inputTopic1.pipeInput("B", "4", 19L);
            inputTopic1.pipeInput("B", "5", 25L);
            inputTopic1.pipeInput("B", "6", 14L);

            inputTopic1.pipeInput("C", "1", 11L);
            inputTopic1.pipeInput("C", "2", 15L);
            inputTopic1.pipeInput("C", "3", 16L);
            inputTopic1.pipeInput("C", "4", 21);
            inputTopic1.pipeInput("C", "5", 23L);

            inputTopic1.pipeInput("D", "4", 11L);
            inputTopic1.pipeInput("D", "2", 12L);
            inputTopic1.pipeInput("D", "3", 29L);
            inputTopic1.pipeInput("D", "5", 16L);
        }

        assertEquals(
                asList(
                        // FINAL WINDOW: A@10 left window created when A@10 processed
                        new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0, 10)), "0+1", 10),
                        // A@10 right window created when A@20 processed
                        new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(11, 21)), "0+2", 20),
                        // A@20 left window created when A@20 processed
                        new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(10, 20)), "0+1+2", 20),
                        // FINAL WINDOW: A@20 right window created when A@22 processed
                        new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(21, 31)), "0+3", 22),
                        // A@22 left window created when A@22 processed
                        new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(12, 22)), "0+2+3", 22),
                        // FINAL WINDOW: A@20 left window updated when A@15 processed
                        new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(10, 20)), "0+1+2+4", 20),
                        // FINAL WINDOW: A@10 right window updated when A@15 processed
                        new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(11, 21)), "0+2+4", 20),
                        // FINAL WINDOW: A@22 left window updated when A@15 processed
                        new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(12, 22)), "0+2+3+4", 22),
                        // FINAL WINDOW: A@15 left window created when A@15 processed
                        new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5, 15)), "0+1+4", 15),
                        // FINAL WINDOW: A@15 right window created when A@15 processed
                        new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(16, 26)), "0+2+3", 22),

                        // FINAL WINDOW: B@12 left window created when B@12 processed
                        new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(2, 12)), "0+1", 12),
                        // B@12 right window created when B@13 processed
                        new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(13, 23)), "0+2", 13),
                        // FINAL WINDOW: B@13 left window created when B@13 processed
                        new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(3, 13)), "0+1+2", 13),
                        // B@12 right window updated when B@18 processed
                        new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(13, 23)), "0+2+3", 18),
                        // B@13 right window created when B@18 processed
                        new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(14, 24)), "0+3", 18),
                        // B@18 left window created when B@18 processed
                        new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(8, 18)), "0+1+2+3", 18),
                        // B@12 right window updated when B@19 processed
                        new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(13, 23)), "0+2+3+4", 19),
                        // B@13 right window updated when B@19 processed
                        new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(14, 24)), "0+3+4", 19),
                        // B@18 right window created when B@19 processed
                        new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(19, 29)), "0+4", 19),
                        // B@19 left window created when B@19 processed
                        new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(9, 19)), "0+1+2+3+4", 19),
                        // FINAL WINDOW: B@18 right window updated when B@25 processed
                        new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(19, 29)), "0+4+5", 25),
                        // FINAL WINDOW: B@19 right window updated when B@25 processed
                        new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(20, 30)), "0+5", 25),
                        // FINAL WINDOW: B@25 left window created when B@25 processed
                        new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(15, 25)), "0+3+4+5", 25),
                        // FINAL WINDOW: B@18 left window updated when B@14 processed
                        new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(8, 18)), "0+1+2+3+6", 18),
                        // FINAL WINDOW: B@19 left window updated when B@14 processed
                        new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(9, 19)), "0+1+2+3+4+6", 19),
                        // FINAL WINDOW: B@12 right window updated when B@14 processed
                        new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(13, 23)), "0+2+3+4+6", 19),
                        // FINAL WINDOW: B@13 right window updated when B@14 processed
                        new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(14, 24)), "0+3+4+6", 19),
                        // FINAL WINDOW: B@14 left window created when B@14 processed
                        new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(4, 14)), "0+1+2+6", 14),

                        // FINAL WINDOW: C@11 left window created when C@11 processed
                        new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(1, 11)), "0+1", 11),
                        // C@11 right window created when C@15 processed
                        new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(12, 22)), "0+2", 15),
                        // FINAL WINDOW: C@15 left window created when C@15 processed
                        new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(5, 15)), "0+1+2", 15),
                        // C@11 right window updated when C@16 processed
                        new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(12, 22)), "0+2+3", 16),
                        // C@15 right window created when C@16 processed
                        new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(16, 26)), "0+3", 16),
                        // FINAL WINDOW: C@16 left window created when C@16 processed
                        new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(6, 16)), "0+1+2+3", 16),
                        // FINAL WINDOW: C@11 right window updated when C@21 processed
                        new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(12, 22)), "0+2+3+4", 21),
                        // C@15 right window updated when C@21 processed
                        new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(16, 26)), "0+3+4", 21),
                        // C@16 right window created when C@21 processed
                        new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(17, 27)), "0+4", 21),
                        // FINAL WINDOW: C@21 left window created when C@21 processed
                        new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(11, 21)), "0+1+2+3+4", 21),
                        // FINAL WINDOW: C@15 right window updated when C@23 processed
                        new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(16, 26)), "0+3+4+5", 23),
                        // FINAL WINDOW: C@16 right window updated when C@23 processed
                        new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(17, 27)), "0+4+5", 23),
                        // FINAL WINDOW: C@21 right window created when C@23 processed
                        new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(22, 32)), "0+5", 23),
                        // FINAL WINDOW: C@23 left window created when C@23 processed
                        new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(13, 23)), "0+2+3+4+5", 23),

                        // FINAL WINDOW: D@11 left window created when D@11 processed
                        new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(1, 11)), "0+4", 11),
                        // D@11 right window created when D@12 processed
                        new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(12, 22)), "0+2", 12),
                        // FINAL WINDOW: D@12 left window created when D@12 processed
                        new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(2, 12)), "0+4+2", 12),
                        // FINAL WINDOW: D@29 left window created when D@29 processed
                        new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(19, 29)), "0+3", 29),
                        // FINAL WINDOW: D@11 right window updated when D@16 processed
                        new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(12, 22)), "0+2+5", 16),
                        // FINAL WINDOW: D@12 right window created when D@16 processed
                        new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(13, 23)), "0+5", 16),
                        // FINAL WINDOW: D@16 left window created when D@16 processed
                        new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(6, 16)), "0+4+2+5", 16)
                        ),
                supplier.theCapturedProcessor().processed
        );
    }

    @Test
    public void testJoin() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";
        final String topic2 = "topic2";

        final KTable<Windowed<String>, String> table1 = builder
                .stream(topic1, Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(SlidingWindows.withTimeDifferenceAndGrace(ofMillis(10), ofMillis(100)))
                .aggregate(
                        MockInitializer.STRING_INIT,
                        MockAggregator.TOSTRING_ADDER,
                        Materialized.<String, String, WindowStore<Bytes, byte[]>>as("topic1-Canonized").withValueSerde(Serdes.String())
                );

        final MockProcessorSupplier<Windowed<String>, String> supplier = new MockProcessorSupplier<>();
        table1.toStream().process(supplier);

        final KTable<Windowed<String>, String> table2 = builder
                .stream(topic2, Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(SlidingWindows.withTimeDifferenceAndGrace(ofMillis(10), ofMillis(100)))
                .aggregate(
                        MockInitializer.STRING_INIT,
                        MockAggregator.TOSTRING_ADDER,
                        Materialized.<String, String, WindowStore<Bytes, byte[]>>as("topic2-Canonized").withValueSerde(Serdes.String())
                );
        table2.toStream().process(supplier);

        table1.join(table2, (p1, p2) -> p1 + "%" + p2).toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                    driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> inputTopic2 =
                    driver.createInputTopic(topic2, new StringSerializer(), new StringSerializer());
            inputTopic1.pipeInput("A", "1", 10L);
            inputTopic1.pipeInput("B", "2", 11L);
            inputTopic1.pipeInput("C", "3", 12L);

            final List<MockProcessor<Windowed<String>, String>> processors = supplier.capturedProcessors(3);

            processors.get(0).checkAndClearProcessResult(
                    // left windows created by the first set of records to table 1
                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0, 10)),  "0+1",  10),
                    new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(1, 11)),  "0+2",  11),
                    new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(2, 12)),  "0+3",  12)
            );
            processors.get(1).checkAndClearProcessResult();
            processors.get(2).checkAndClearProcessResult();

            inputTopic1.pipeInput("A", "1", 15L);
            inputTopic1.pipeInput("B", "2", 16L);
            inputTopic1.pipeInput("C", "3", 19L);

            processors.get(0).checkAndClearProcessResult(
                    // right windows from previous records are created, and left windows from new records to table 1
                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(11, 21)),  "0+1",  15),
                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5, 15)),  "0+1+1",  15),
                    new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(12, 22)),  "0+2",  16),
                    new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(6, 16)),  "0+2+2",  16),
                    new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(13, 23)),  "0+3",  19),
                    new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(9, 19)),  "0+3+3",  19)
            );
            processors.get(1).checkAndClearProcessResult();
            processors.get(2).checkAndClearProcessResult();

            inputTopic2.pipeInput("A", "a", 10L);
            inputTopic2.pipeInput("B", "b", 30L);
            inputTopic2.pipeInput("C", "c", 12L);
            inputTopic2.pipeInput("C", "c", 35L);


            processors.get(0).checkAndClearProcessResult();
            processors.get(1).checkAndClearProcessResult(
                    // left windows from first set of records sent to table 2
                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0, 10)),  "0+a",  10),
                    new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(20, 30)),  "0+b",  30),
                    new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(2, 12)),  "0+c",  12),
                    new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(25, 35)),  "0+c",  35)
            );
            processors.get(2).checkAndClearProcessResult(
                    // set of join windows from windows created by table 1 and table 2
                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0, 10)),  "0+1%0+a",  10),
                    new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(2, 12)),  "0+3%0+c",  12)
            );

            inputTopic2.pipeInput("A", "a", 15L);
            inputTopic2.pipeInput("B", "b", 16L);
            inputTopic2.pipeInput("C", "c", 17L);

            processors.get(0).checkAndClearProcessResult();
            processors.get(1).checkAndClearProcessResult(
                    // right windows from previous records are created (where applicable), and left windows from new records to table 2
                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(11, 21)),  "0+a",  15),
                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5, 15)),  "0+a+a",  15),
                    new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(6, 16)),  "0+b",  16),
                    new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(13, 23)),  "0+c",  17),
                    new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(7, 17)),  "0+c+c",  17)
            );
            processors.get(2).checkAndClearProcessResult(
                    // set of join windows from windows created by table 1 and table 2
                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(11, 21)),  "0+1%0+a",  15),
                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5, 15)),  "0+1+1%0+a+a",  15),
                    new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(6, 16)),  "0+2+2%0+b",  16),
                    new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(13, 23)),  "0+3%0+c",  19)
            );
        }
    }

    @Test
    public void shouldLogAndMeterWhenSkippingNullKey() {
        final String builtInMetricsVersion = StreamsConfig.METRICS_LATEST;
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic = "topic";
        builder
                .stream(topic, Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(SlidingWindows.withTimeDifferenceAndGrace(ofMillis(10), ofMillis(100)))
                .aggregate(MockInitializer.STRING_INIT, MockAggregator.toStringInstance("+"), Materialized.<String, String, WindowStore<Bytes, byte[]>>as("topic1-Canonicalized").withValueSerde(Serdes.String()));

        props.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, builtInMetricsVersion);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KStreamSlidingWindowAggregate.class);
             final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic(topic, new StringSerializer(), new StringSerializer());
            inputTopic.pipeInput(null, "1");
            assertThat(appender.getMessages(), hasItem("Skipping record due to null key or value. value=[1] topic=[topic] partition=[0] offset=[0]"));
        }
    }

    @Test
    public void shouldLogAndMeterWhenSkippingExpiredWindowByGrace() {
        final String builtInMetricsVersion = StreamsConfig.METRICS_LATEST;
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic = "topic";

        final KStream<String, String> stream1 = builder.stream(topic, Consumed.with(Serdes.String(), Serdes.String()));
        stream1.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(SlidingWindows.withTimeDifferenceAndGrace(ofMillis(10), ofMillis(90L)))
                .aggregate(
                        () -> "",
                        MockAggregator.toStringInstance("+"),
                        Materialized.<String, String, WindowStore<Bytes, byte[]>>as("topic1-Canonicalized").withValueSerde(Serdes.String()).withCachingDisabled().withLoggingDisabled()
                )
                .toStream()
                .map((key, value) -> new KeyValue<>(key.toString(), value))
                .to("output");

        props.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, builtInMetricsVersion);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KStreamSlidingWindowAggregate.class);
             final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {

            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic(topic, new StringSerializer(), new StringSerializer());
            inputTopic.pipeInput("k", "100", 200L);
            inputTopic.pipeInput("k", "0", 100L);
            inputTopic.pipeInput("k", "1", 101L);
            inputTopic.pipeInput("k", "2", 102L);
            inputTopic.pipeInput("k", "3", 103L);
            inputTopic.pipeInput("k", "4", 104L);
            inputTopic.pipeInput("k", "5", 105L);
            inputTopic.pipeInput("k", "6", 15L);

            assertLatenessMetrics(driver, is(7.0), is(185.0), is(96.25));

            assertThat(appender.getMessages(), hasItems(
                    // left window for k@100
                    "Skipping record for expired window. key=[k] topic=[topic] partition=[0] offset=[1] timestamp=[100] window=[90,100] expiration=[110] streamTime=[200]",
                    // left window for k@101
                    "Skipping record for expired window. key=[k] topic=[topic] partition=[0] offset=[2] timestamp=[101] window=[91,101] expiration=[110] streamTime=[200]",
                    // left window for k@102
                    "Skipping record for expired window. key=[k] topic=[topic] partition=[0] offset=[3] timestamp=[102] window=[92,102] expiration=[110] streamTime=[200]",
                    // left window for k@103
                    "Skipping record for expired window. key=[k] topic=[topic] partition=[0] offset=[4] timestamp=[103] window=[93,103] expiration=[110] streamTime=[200]",
                    // left window for k@104
                    "Skipping record for expired window. key=[k] topic=[topic] partition=[0] offset=[5] timestamp=[104] window=[94,104] expiration=[110] streamTime=[200]",
                    // left window for k@105
                    "Skipping record for expired window. key=[k] topic=[topic] partition=[0] offset=[6] timestamp=[105] window=[95,105] expiration=[110] streamTime=[200]",
                    // left window for k@15
                    "Skipping record for expired window. key=[k] topic=[topic] partition=[0] offset=[7] timestamp=[15] window=[5,15] expiration=[110] streamTime=[200]"
            ));
            final TestOutputTopic<String, String> outputTopic =
                    driver.createOutputTopic("output", new StringDeserializer(), new StringDeserializer());
            assertThat(outputTopic.readRecord(), equalTo(new TestRecord<>("[k@190/200]", "+100", null, 200L)));
            assertTrue(outputTopic.isEmpty());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testProcessorRandomInput() {

        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<Windowed<String>, String> table = builder
            .stream(topic1, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(SlidingWindows.withTimeDifferenceAndGrace(ofMillis(10), ofMillis(10000)))
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                Materialized.<String, String, WindowStore<Bytes, byte[]>>as("topic1-Canonized").withValueSerde(Serdes.String())
            );
        final MockProcessorSupplier<Windowed<String>, String> supplier = new MockProcessorSupplier<>();
        table.toStream().process(supplier);

        final List<ValueAndTimestamp<String>> input = Arrays.asList(
            ValueAndTimestamp.make("A", 10L),
            ValueAndTimestamp.make("A", 15L),
            ValueAndTimestamp.make("A", 16L),
            ValueAndTimestamp.make("A", 18L),
            ValueAndTimestamp.make("A", 30L),
            ValueAndTimestamp.make("A", 40L),
            ValueAndTimestamp.make("A", 55L),
            ValueAndTimestamp.make("A", 56L),
            ValueAndTimestamp.make("A", 58L),
            ValueAndTimestamp.make("A", 58L),
            ValueAndTimestamp.make("A", 62L),
            ValueAndTimestamp.make("A", 63L),
            ValueAndTimestamp.make("A", 63L),
            ValueAndTimestamp.make("A", 63L),
            ValueAndTimestamp.make("A", 76L),
            ValueAndTimestamp.make("A", 77L),
            ValueAndTimestamp.make("A", 80L)
        );

        final long seed = new Random().nextLong();
        final Random shuffle = new Random(seed);
        Collections.shuffle(input, shuffle);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer());
            for (int i = 0; i < input.size(); i++) {
                inputTopic1.pipeInput("A", input.get(i).value(), input.get(i).timestamp());
            }
        }

        final Map<Long, ValueAndTimestamp<String>> results = new HashMap<>();

        for (final KeyValueTimestamp<Object, Object> entry : supplier.theCapturedProcessor().processed) {
            final Windowed<String> window = (Windowed<String>) entry.key();
            final Long start = window.window().start();
            final ValueAndTimestamp valueAndTimestamp = ValueAndTimestamp.make((String) entry.value(), entry.timestamp());
            if (results.putIfAbsent(start, valueAndTimestamp) != null) {
                results.replace(start, valueAndTimestamp);
            }
        }
        randomEqualityCheck(results, seed);
    }

    private void randomEqualityCheck(final Map<Long, ValueAndTimestamp<String>> actual, final Long seed) {
        final Map<Long, ValueAndTimestamp<String>> expected = new HashMap<>();
        expected.put(0L, ValueAndTimestamp.make("0+A", 10L));
        expected.put(11L, ValueAndTimestamp.make("0+A+A+A", 18L));
        expected.put(5L, ValueAndTimestamp.make("0+A+A", 15L));
        expected.put(16L, ValueAndTimestamp.make("0+A+A", 18L));
        expected.put(6L, ValueAndTimestamp.make("0+A+A+A", 16L));
        expected.put(17L, ValueAndTimestamp.make("0+A", 18L));
        expected.put(8L, ValueAndTimestamp.make("0+A+A+A+A", 18L));
        expected.put(20L, ValueAndTimestamp.make("0+A", 30L));
        expected.put(31L, ValueAndTimestamp.make("0+A", 40L));
        expected.put(30L, ValueAndTimestamp.make("0+A+A", 40L));
        expected.put(45L, ValueAndTimestamp.make("0+A", 55L));
        expected.put(56L, ValueAndTimestamp.make("0+A+A+A+A+A+A+A", 63L));
        expected.put(46L, ValueAndTimestamp.make("0+A+A", 56L));
        expected.put(57L, ValueAndTimestamp.make("0+A+A+A+A+A+A", 63L));
        expected.put(48L, ValueAndTimestamp.make("0+A+A+A+A", 58L));
        expected.put(59L, ValueAndTimestamp.make("0+A+A+A+A", 63L));
        expected.put(52L, ValueAndTimestamp.make("0+A+A+A+A+A", 62L));
        expected.put(63L, ValueAndTimestamp.make("0+A+A+A", 63L));
        expected.put(53L, ValueAndTimestamp.make("0+A+A+A+A+A+A+A+A", 63L));
        expected.put(66L, ValueAndTimestamp.make("0+A", 76L));
        expected.put(77L, ValueAndTimestamp.make("0+A+A", 80L));
        expected.put(67L, ValueAndTimestamp.make("0+A+A", 77L));
        expected.put(78L, ValueAndTimestamp.make("0+A", 80L));
        expected.put(70L, ValueAndTimestamp.make("0+A+A+A", 80L));

        try {
            assertEquals(expected, actual);
        } catch (final AssertionError t) {
            throw new AssertionError(
                "Assertion failed in randomized test. Reproduce with seed: " + seed + ".",
                t
            );
        } catch (final Throwable t) {
            final StringBuilder sb =
                new StringBuilder()
                    .append("Exception in randomized scenario. Reproduce with seed: ")
                    .append(seed)
                    .append(".");
            throw new AssertionError(sb.toString(), t);
        }
    }

    private void assertLatenessMetrics(final TopologyTestDriver driver,
                                       final Matcher<Object> dropTotal,
                                       final Matcher<Object> maxLateness,
                                       final Matcher<Object> avgLateness) {

        final MetricName dropTotalMetric;
        final MetricName dropRateMetric;
        final MetricName latenessMaxMetric;
        final MetricName latenessAvgMetric;
        dropTotalMetric = new MetricName(
                "dropped-records-total",
                "stream-task-metrics",
                "The total number of dropped records",
                mkMap(
                        mkEntry("thread-id", threadId),
                        mkEntry("task-id", "0_0")
                )
        );
        dropRateMetric = new MetricName(
                "dropped-records-rate",
                "stream-task-metrics",
                "The average number of dropped records per second",
                mkMap(
                        mkEntry("thread-id", threadId),
                        mkEntry("task-id", "0_0")
                )
        );
        latenessMaxMetric = new MetricName(
                "record-lateness-max",
                "stream-task-metrics",
                "The observed maximum lateness of records in milliseconds, measured by comparing the record "
                        + "timestamp with the current stream time",
                mkMap(
                        mkEntry("thread-id", threadId),
                        mkEntry("task-id", "0_0")
                )
        );
        latenessAvgMetric = new MetricName(
                "record-lateness-avg",
                "stream-task-metrics",
                "The observed average lateness of records in milliseconds, measured by comparing the record "
                        + "timestamp with the current stream time",
                mkMap(
                        mkEntry("thread-id", threadId),
                        mkEntry("task-id", "0_0")
                )
        );
        assertThat(driver.metrics().get(dropTotalMetric).metricValue(), dropTotal);
        assertThat(driver.metrics().get(dropRateMetric).metricValue(), not(0.0));
        assertThat(driver.metrics().get(latenessMaxMetric).metricValue(), maxLateness);
        assertThat(driver.metrics().get(latenessAvgMetric).metricValue(), avgLateness);
    }
}