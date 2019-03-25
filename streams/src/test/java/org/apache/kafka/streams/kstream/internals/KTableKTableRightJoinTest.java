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
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.Properties;

import static org.apache.kafka.streams.processor.internals.StreamTask.TaskMetrics.SKIPPED_RECORDS;
import static org.apache.kafka.streams.processor.internals.StreamTask.TaskMetrics.STREAM_TASK_METRICS;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TOTAL_SUFFIX;
import static org.apache.kafka.test.StreamsTestUtils.getMetricByName;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class KTableKTableRightJoinTest {
    final private String topic1 = "topic1";
    final private String topic2 = "topic2";

    private final Consumed<Integer, String> consumed = Consumed.with(Serdes.Integer(), Serdes.String());
    private final ConsumerRecordFactory<Integer, String> recordFactory =
        new ConsumerRecordFactory<>(Serdes.Integer().serializer(), Serdes.String().serializer(), 0L);
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.String());

    @Test
    public void shouldLogAndMeterSkippedRecordsDueToNullLeftKey() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<Integer, String> table1;
        final KTable<Integer, String> table2;

        table1 = builder.table(topic1, consumed);
        table2 = builder.table(topic2, consumed);
        table1.outerJoin(table2, MockValueJoiner.TOSTRING_JOINER);

        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(topic1, null, "value"));

            assertEquals(1.0, getMetricByName(driver.metrics(), SKIPPED_RECORDS + TOTAL_SUFFIX, STREAM_TASK_METRICS).metricValue());
            assertThat(appender.getMessages(), hasItem("Skipping record due to null key. topic=[topic1] partition=[0] offset=[0]"));
        }
        LogCaptureAppender.unregister(appender);
    }
}
