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
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import static org.apache.kafka.test.StreamsTestUtils.getMetricByName;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class KStreamWindowReduceTest {
    @Test
    public void shouldLogAndMeterOnNullKey() {
        final KStreamTestDriver driver = new KStreamTestDriver();

        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream("TOPIC", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
            .windowedBy(TimeWindows.of(500L))
            .reduce(new Reducer<String>() {
                @Override
                public String apply(final String value1, final String value2) {
                    return value1 + "+" + value2;
                }
            });

        driver.setUp(builder, TestUtils.tempDirectory(), 0);

        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
        driver.process("TOPIC", null, "asdf");
        driver.flushState();
        LogCaptureAppender.unregister(appender);

        assertEquals(1.0, getMetricByName(driver.context().metrics().metrics(), "skipped-records-total", "stream-metrics").metricValue());
        assertThat(appender.getMessages(), hasItem("Skipping record due to null key. value=[asdf] topic=[TOPIC] partition=[-1] offset=[-1]"));
    }
}
