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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

import static org.apache.kafka.test.StreamsTestUtils.getMetricByName;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class KStreamWindowAggregateTest {

    private final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
    private final Properties props = StreamsTestUtils.topologyTestConfig(Serdes.String(), Serdes.String());

    @Test
    public void testAggBasic() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<Windowed<String>, String> table2 = builder
            .stream(topic1, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
            .windowedBy(TimeWindows.of(10).advanceBy(5))
            .aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, Materialized.<String, String, WindowStore<Bytes, byte[]>>as("topic1-Canonized").withValueSerde(Serdes.String()));

        final MockProcessorSupplier<Windowed<String>, String> supplier = new MockProcessorSupplier<>();
        table2.toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props, 0L)) {
            driver.pipeInput(recordFactory.create(topic1, "A", "1", 0L));
            driver.pipeInput(recordFactory.create(topic1, "B", "2", 1L));
            driver.pipeInput(recordFactory.create(topic1, "C", "3", 2L));
            driver.pipeInput(recordFactory.create(topic1, "D", "4", 3L));
            driver.pipeInput(recordFactory.create(topic1, "A", "1", 4L));

            driver.pipeInput(recordFactory.create(topic1, "A", "1", 5L));
            driver.pipeInput(recordFactory.create(topic1, "B", "2", 6L));
            driver.pipeInput(recordFactory.create(topic1, "D", "4", 7L));
            driver.pipeInput(recordFactory.create(topic1, "B", "2", 8L));
            driver.pipeInput(recordFactory.create(topic1, "C", "3", 9L));
            driver.pipeInput(recordFactory.create(topic1, "A", "1", 10L));
            driver.pipeInput(recordFactory.create(topic1, "B", "2", 11L));
            driver.pipeInput(recordFactory.create(topic1, "D", "4", 12L));
            driver.pipeInput(recordFactory.create(topic1, "B", "2", 13L));
            driver.pipeInput(recordFactory.create(topic1, "C", "3", 14L));
        }

        assertEquals(
            Utils.mkList(
                "[A@0/10]:0+1",
                "[B@0/10]:0+2",
                "[C@0/10]:0+3",
                "[D@0/10]:0+4",
                "[A@0/10]:0+1+1",

                "[A@0/10]:0+1+1+1", "[A@5/15]:0+1",
                "[B@0/10]:0+2+2", "[B@5/15]:0+2",
                "[D@0/10]:0+4+4", "[D@5/15]:0+4",
                "[B@0/10]:0+2+2+2", "[B@5/15]:0+2+2",
                "[C@0/10]:0+3+3", "[C@5/15]:0+3",

                "[A@5/15]:0+1+1", "[A@10/20]:0+1",
                "[B@5/15]:0+2+2+2", "[B@10/20]:0+2",
                "[D@5/15]:0+4+4", "[D@10/20]:0+4",
                "[B@5/15]:0+2+2+2+2", "[B@10/20]:0+2+2",
                "[C@5/15]:0+3+3", "[C@10/20]:0+3"
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
            .groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
            .windowedBy(TimeWindows.of(10).advanceBy(5))
            .aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, Materialized.<String, String, WindowStore<Bytes, byte[]>>as("topic1-Canonized").withValueSerde(Serdes.String()));

        final MockProcessorSupplier<Windowed<String>, String> supplier = new MockProcessorSupplier<>();
        table1.toStream().process(supplier);

        final KTable<Windowed<String>, String> table2 = builder
            .stream(topic2, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
            .windowedBy(TimeWindows.of(10).advanceBy(5))
            .aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, Materialized.<String, String, WindowStore<Bytes, byte[]>>as("topic2-Canonized").withValueSerde(Serdes.String()));

        table2.toStream().process(supplier);


        table1.join(table2, new ValueJoiner<String, String, String>() {
            @Override
            public String apply(final String p1, final String p2) {
                return p1 + "%" + p2;
            }
        }).toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props, 0L)) {
            driver.pipeInput(recordFactory.create(topic1, "A", "1", 0L));
            driver.pipeInput(recordFactory.create(topic1, "B", "2", 1L));
            driver.pipeInput(recordFactory.create(topic1, "C", "3", 2L));
            driver.pipeInput(recordFactory.create(topic1, "D", "4", 3L));
            driver.pipeInput(recordFactory.create(topic1, "A", "1", 4L));

            final List<MockProcessor<Windowed<String>, String>> processors = supplier.capturedProcessors(3);

            processors.get(0).checkAndClearProcessResult(
                    "[A@0/10]:0+1",
                    "[B@0/10]:0+2",
                    "[C@0/10]:0+3",
                    "[D@0/10]:0+4",
                    "[A@0/10]:0+1+1"
            );
            processors.get(1).checkAndClearProcessResult();
            processors.get(2).checkAndClearProcessResult();

            driver.pipeInput(recordFactory.create(topic1, "A", "1", 5L));
            driver.pipeInput(recordFactory.create(topic1, "B", "2", 6L));
            driver.pipeInput(recordFactory.create(topic1, "D", "4", 7L));
            driver.pipeInput(recordFactory.create(topic1, "B", "2", 8L));
            driver.pipeInput(recordFactory.create(topic1, "C", "3", 9L));

            processors.get(0).checkAndClearProcessResult(
                    "[A@0/10]:0+1+1+1", "[A@5/15]:0+1",
                    "[B@0/10]:0+2+2", "[B@5/15]:0+2",
                    "[D@0/10]:0+4+4", "[D@5/15]:0+4",
                    "[B@0/10]:0+2+2+2", "[B@5/15]:0+2+2",
                    "[C@0/10]:0+3+3", "[C@5/15]:0+3"
            );
            processors.get(1).checkAndClearProcessResult();
            processors.get(2).checkAndClearProcessResult();

            driver.pipeInput(recordFactory.create(topic2, "A", "a", 0L));
            driver.pipeInput(recordFactory.create(topic2, "B", "b", 1L));
            driver.pipeInput(recordFactory.create(topic2, "C", "c", 2L));
            driver.pipeInput(recordFactory.create(topic2, "D", "d", 3L));
            driver.pipeInput(recordFactory.create(topic2, "A", "a", 4L));

            processors.get(0).checkAndClearProcessResult();
            processors.get(1).checkAndClearProcessResult(
                    "[A@0/10]:0+a",
                    "[B@0/10]:0+b",
                    "[C@0/10]:0+c",
                    "[D@0/10]:0+d",
                    "[A@0/10]:0+a+a"
            );
            processors.get(2).checkAndClearProcessResult(
                    "[A@0/10]:0+1+1+1%0+a",
                    "[B@0/10]:0+2+2+2%0+b",
                    "[C@0/10]:0+3+3%0+c",
                    "[D@0/10]:0+4+4%0+d",
                    "[A@0/10]:0+1+1+1%0+a+a");

            driver.pipeInput(recordFactory.create(topic2, "A", "a", 5L));
            driver.pipeInput(recordFactory.create(topic2, "B", "b", 6L));
            driver.pipeInput(recordFactory.create(topic2, "D", "d", 7L));
            driver.pipeInput(recordFactory.create(topic2, "B", "b", 8L));
            driver.pipeInput(recordFactory.create(topic2, "C", "c", 9L));

            processors.get(0).checkAndClearProcessResult();
            processors.get(1).checkAndClearProcessResult(
                    "[A@0/10]:0+a+a+a", "[A@5/15]:0+a",
                    "[B@0/10]:0+b+b", "[B@5/15]:0+b",
                    "[D@0/10]:0+d+d", "[D@5/15]:0+d",
                    "[B@0/10]:0+b+b+b", "[B@5/15]:0+b+b",
                    "[C@0/10]:0+c+c", "[C@5/15]:0+c"
            );
            processors.get(2).checkAndClearProcessResult(
                    "[A@0/10]:0+1+1+1%0+a+a+a", "[A@5/15]:0+1%0+a",
                    "[B@0/10]:0+2+2+2%0+b+b", "[B@5/15]:0+2+2%0+b",
                    "[D@0/10]:0+4+4%0+d+d", "[D@5/15]:0+4%0+d",
                    "[B@0/10]:0+2+2+2%0+b+b+b", "[B@5/15]:0+2+2%0+b+b",
                    "[C@0/10]:0+3+3%0+c+c", "[C@5/15]:0+3%0+c"
            );
        }
    }

    @Test
    public void shouldLogAndMeterWhenSkippingNullKey() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic = "topic";

        final KStream<String, String> stream1 = builder.stream(topic, Consumed.with(Serdes.String(), Serdes.String()));
        stream1.groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
            .windowedBy(TimeWindows.of(10).advanceBy(5))
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.<String, String>toStringInstance("+"),
                Materialized.<String, String, WindowStore<Bytes, byte[]>>as("topic1-Canonicalized").withValueSerde(Serdes.String())
            );

        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props, 0L)) {
            driver.pipeInput(recordFactory.create(topic, null, "1"));
            LogCaptureAppender.unregister(appender);

            assertEquals(1.0, getMetricByName(driver.metrics(), "skipped-records-total", "stream-metrics").metricValue());
            assertThat(appender.getMessages(), hasItem("Skipping record due to null key. value=[1] topic=[topic] partition=[0] offset=[0]"));
        }
    }
}
