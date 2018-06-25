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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.Properties;

import static org.apache.kafka.test.StreamsTestUtils.getMetricByName;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class KStreamWindowReduceTest {

    private final Properties props = StreamsTestUtils.topologyTestConfig(Serdes.String(), Serdes.String());
    private final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());

    @Test
    public void shouldLogAndMeterOnNullKey() {

        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream("TOPIC", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
            .windowedBy(TimeWindows.of(500L))
            .reduce((value1, value2) -> value1 + "+" + value2);


        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
            driver.pipeInput(recordFactory.create("TOPIC", null, "asdf"));
            LogCaptureAppender.unregister(appender);

            assertEquals(1.0, getMetricByName(driver.metrics(), "skipped-records-total", "stream-metrics").metricValue());
            assertThat(appender.getMessages(), hasItem("Skipping record due to null key. value=[asdf] topic=[TOPIC] partition=[0] offset=[0]"));
        }
    }

    @Test
    public void shouldLogAndMeterOnExpiredEvent() {

        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream("TOPIC", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
            .windowedBy(TimeWindows.of(5L).until(100))
            .reduce((value1, value2) -> value1 + "+" + value2)
            .toStream()
            .map((key, value) -> new KeyValue<>(key.toString(), value))
            .to("output");


        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
            driver.pipeInput(recordFactory.create("TOPIC", "k", "100", 100L));
            driver.pipeInput(recordFactory.create("TOPIC", "k", "0", 0L));
            driver.pipeInput(recordFactory.create("TOPIC", "k", "1", 1L));
            driver.pipeInput(recordFactory.create("TOPIC", "k", "2", 2L));
            driver.pipeInput(recordFactory.create("TOPIC", "k", "3", 3L));
            driver.pipeInput(recordFactory.create("TOPIC", "k", "4", 4L));
            driver.pipeInput(recordFactory.create("TOPIC", "k", "5", 5L));
            LogCaptureAppender.unregister(appender);

            assertThat(getMetricByName(driver.metrics(), "skipped-records-total", "stream-metrics").metricValue(), equalTo(5.0));
            assertThat(appender.getMessages(), hasItems(
                "Skipping record for expired window. key=[k] topic=[TOPIC] partition=[0] offset=[1] timestamp=[0] window=[0] expiration=[0]",
                "Skipping record for expired window. key=[k] topic=[TOPIC] partition=[0] offset=[2] timestamp=[1] window=[0] expiration=[0]",
                "Skipping record for expired window. key=[k] topic=[TOPIC] partition=[0] offset=[3] timestamp=[2] window=[0] expiration=[0]",
                "Skipping record for expired window. key=[k] topic=[TOPIC] partition=[0] offset=[4] timestamp=[3] window=[0] expiration=[0]",
                "Skipping record for expired window. key=[k] topic=[TOPIC] partition=[0] offset=[5] timestamp=[4] window=[0] expiration=[0]"
            ));

            OutputVerifier.compareKeyValueTimestamp(getOutput(driver), "[k@100/105]", "100", 100);
            OutputVerifier.compareKeyValueTimestamp(getOutput(driver), "[k@5/10]", "5", 5);
            assertThat(driver.readOutput("output"), nullValue());
        }
    }

    private ProducerRecord<String, String> getOutput(final TopologyTestDriver driver) {
        return driver.readOutput("output", new StringDeserializer(), new StringDeserializer());
    }
}
