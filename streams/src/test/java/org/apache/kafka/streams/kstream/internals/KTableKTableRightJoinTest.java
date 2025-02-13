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
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.LogCaptureAppender.Event;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.test.StreamsTestUtils;

import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;

public class KTableKTableRightJoinTest {

    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    @Test
    public void shouldLogAndMeterSkippedRecordsDueToNullLeftKeyWithBuiltInMetricsVersionLatest() {
        final StreamsBuilder builder = new StreamsBuilder();

        @SuppressWarnings("unchecked")
        final Processor<String, Change<String>, String, Change<Object>> join = new KTableKTableRightJoin<>(
            (KTableImpl<String, String, String>) builder.table("left", Consumed.with(Serdes.String(), Serdes.String())),
            (KTableImpl<String, String, String>) builder.table("right", Consumed.with(Serdes.String(), Serdes.String())),
            null
        ).get();

        props.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, StreamsConfig.METRICS_LATEST);
        final MockProcessorContext<String, Change<Object>> context = new MockProcessorContext<>(props);
        context.setRecordMetadata("left", -1, -2);
        join.init(context);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KTableKTableRightJoin.class)) {
            join.process(new Record<>(null, new Change<>("new", "old"), 0));

            assertThat(
                appender.getEvents().stream()
                    .filter(e -> e.getLevel().equals("WARN"))
                    .map(Event::getMessage)
                    .collect(Collectors.toList()),
                hasItem("Skipping record due to null key. topic=[left] partition=[-1] offset=[-2]")
            );
        }
    }
}
