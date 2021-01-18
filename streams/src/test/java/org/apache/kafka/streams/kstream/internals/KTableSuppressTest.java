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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofMinutes;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxRecords;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class KTableSuppressTest {
    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Test
    public void shouldTimeLimitSuppressionNotQueriableWithoutQueryEnabled() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Windowed<String>, Long> valueCounts = builder
                .stream("input", Consumed.with(STRING_SERDE, STRING_SERDE))
                .groupBy((String k, String v) -> k, Grouped.with(STRING_SERDE, STRING_SERDE))
                .windowedBy(TimeWindows.of(ofMillis(2L)).grace(ofMillis(2L)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("counts").withCachingDisabled().withKeySerde(STRING_SERDE));
        final KTable<Windowed<String>, Long> suppressed = valueCounts
                .suppress(untilTimeLimit(ofMinutes(1), maxRecords(1L).emitEarlyWhenFull()).withName("suppressed-counts"));
        builder.build();

        assertEquals("counts", valueCounts.queryableStoreName());
        assertNull(suppressed.queryableStoreName());
    }

    @Test
    public void shouldTimeLimitSuppressionQueriableWithMaterialized() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Windowed<String>, Long> valueCounts = builder
                .stream("input", Consumed.with(STRING_SERDE, STRING_SERDE))
                .groupBy((String k, String v) -> k, Grouped.with(STRING_SERDE, STRING_SERDE))
                .windowedBy(TimeWindows.of(ofMillis(2L)).grace(ofMillis(2L)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("counts").withCachingDisabled().withKeySerde(STRING_SERDE));
        final KTable<Windowed<String>, Long> suppressed = valueCounts
                .suppress(untilTimeLimit(ofMinutes(1), maxRecords(1L).emitEarlyWhenFull()).withName("suppressed-counts").enableQuery());
        builder.build();

        assertEquals("counts", valueCounts.queryableStoreName());
        assertEquals("suppressed-counts", suppressed.queryableStoreName());
    }

    @Test
    public void shouldWindowSuppressionNotQueriableWithoutQueryEnabled() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Windowed<String>, Long> valueCounts = builder
            .stream("input", Consumed.with(STRING_SERDE, STRING_SERDE))
            .groupBy((String k, String v) -> k, Grouped.with(STRING_SERDE, STRING_SERDE))
            .windowedBy(TimeWindows.of(ofMillis(2L)).grace(ofMillis(2L)))
            .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("counts").withCachingDisabled().withKeySerde(STRING_SERDE));
        final KTable<Windowed<String>, Long> suppressed = valueCounts
            .suppress(untilWindowCloses(unbounded()).withName("suppressed-counts"));
        builder.build();

        assertEquals("counts", valueCounts.queryableStoreName());
        assertNull(suppressed.queryableStoreName());
    }

    @Test
    public void shouldWindowSuppressionQueriableWithMaterialized() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Windowed<String>, Long> valueCounts = builder
            .stream("input", Consumed.with(STRING_SERDE, STRING_SERDE))
            .groupBy((String k, String v) -> k, Grouped.with(STRING_SERDE, STRING_SERDE))
            .windowedBy(TimeWindows.of(ofMillis(2L)).grace(ofMillis(2L)))
            .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("counts").withCachingDisabled().withKeySerde(STRING_SERDE));
        final KTable<Windowed<String>, Long> suppressed = valueCounts
            .suppress(untilWindowCloses(unbounded()).withName("suppressed-counts").enableQuery());
        builder.build();

        assertEquals("counts", valueCounts.queryableStoreName());
        assertEquals("suppressed-counts", suppressed.queryableStoreName());
    }
}
