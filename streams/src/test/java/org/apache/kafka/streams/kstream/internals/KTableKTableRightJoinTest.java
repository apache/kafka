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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.Properties;

import static org.apache.kafka.test.StreamsTestUtils.getMetricByName;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class KTableKTableRightJoinTest {

    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    @Test
    public void shouldLogAndMeterSkippedRecordsDueToNullLeftKeyWithBuiltInMetricsVersion0100To24() {
        shouldLogAndMeterSkippedRecordsDueToNullLeftKey(StreamsConfig.METRICS_0100_TO_24);
    }

    @Test
    public void shouldLogAndMeterSkippedRecordsDueToNullLeftKeyWithBuiltInMetricsVersionLatest() {
        shouldLogAndMeterSkippedRecordsDueToNullLeftKey(StreamsConfig.METRICS_LATEST);
    }

    private void shouldLogAndMeterSkippedRecordsDueToNullLeftKey(final String builtInMetricsVersion) {
        final StreamsBuilder builder = new StreamsBuilder();

        @SuppressWarnings("unchecked")
        final Processor<String, Change<String>> join = new KTableKTableRightJoin<>(
            (KTableImpl<String, String, String>) builder.table("left", Consumed.with(Serdes.String(), Serdes.String())),
            (KTableImpl<String, String, String>) builder.table("right", Consumed.with(Serdes.String(), Serdes.String())),
            null
        ).get();

        props.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, builtInMetricsVersion);
        final MockProcessorContext context = new MockProcessorContext(props);
        context.setRecordMetadata("left", -1, -2, null, -3);
        join.init(context);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KTableKTableRightJoin.class)) {
            join.process(null, new Change<>("new", "old"));

            assertThat(
                appender.getMessages(),
                hasItem("Skipping record due to null key. change=[(new<-old)] topic=[left] partition=[-1] offset=[-2]")
            );
        }

        if (StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion)) {
            assertEquals(
                1.0,
                getMetricByName(context.metrics().metrics(), "skipped-records-total", "stream-metrics").metricValue()
            );
        }
    }
}
