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

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.EventCountRange;
import org.apache.kafka.streams.kstream.EventTimeRange;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.RangedKStream;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.StreamsTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RangedKStreamImplTest {

    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC = "output";
    private static final Duration BEFORE = Duration.ofMillis(100);
    private static final Duration AFTER = Duration.ofMillis(50);
    private static final Duration GRACE = Duration.ofMillis(20);

    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    // ---- EventTimeRange end-to-end ----

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void eventTimeRangeShouldProduceCorrectAggregation(final boolean cachingEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KGroupedStream<String, String> grouped = builder
            .stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

        final Materialized<String, String, WindowStore<Bytes, byte[]>> mat = buildMaterialized("store-" + cachingEnabled, cachingEnabled);

        final KStream<String, Long> counts = grouped
            .rangeOver(EventTimeRange.ofTimeBoundsWithNoGrace(BEFORE, AFTER), mat)
            .count();

        counts.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> input = driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, Long> output = driver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), new LongDeserializer());

            // t=10: range [10-100, 10+50] = [-90, 60] → only v1
            input.pipeInput("a", "v1", 10L);
            // t=80: range [80-100, 80+50] = [-20, 130] → v1(t=10) and v2(t=80)
            input.pipeInput("a", "v2", 80L);
            // t=200: range [200-100, 200+50] = [100, 250] → v3 only
            input.pipeInput("a", "v3", 200L);

            final List<KeyValue<String, Long>> results = output.readKeyValuesToList();
            assertEquals(3, results.size());
            assertEquals(1L, (long) results.get(0).value);
            assertEquals(2L, (long) results.get(1).value);
            assertEquals(1L, (long) results.get(2).value);
        }
    }

    @Test
    public void eventTimeRangeShouldRespectGracePeriod() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KGroupedStream<String, String> grouped = builder
            .stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

        final Materialized<String, String, WindowStore<Bytes, byte[]>> mat =
            Materialized.<String, String, WindowStore<Bytes, byte[]>>as("grace-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
                .withRetention(Duration.ofMillis(200));

        final KStream<String, Long> counts = grouped
            .rangeOver(EventTimeRange.ofTimeBoundsAndGrace(BEFORE, AFTER, GRACE), mat)
            .count();

        counts.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> input = driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, Long> output = driver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), new LongDeserializer());

            // t=100: stream time = 100. Accepted.
            input.pipeInput("a", "v1", 100L);
            // t=50: late. streamTime=100, gracePeriodMs=20. 100-20=80 > 50 → DROPPED.
            input.pipeInput("a", "late", 50L);
            // t=90: late. streamTime=100, gracePeriodMs=20. 100-20=80 ≤ 90 → ACCEPTED.
            input.pipeInput("a", "v2", 90L);

            final List<KeyValue<String, Long>> results = output.readKeyValuesToList();
            // Only v1 and v2 produce output (late was dropped)
            assertEquals(2, results.size());
        }
    }

    @Test
    public void nullKeyShouldBeDropped() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KGroupedStream<String, String> grouped = builder
            .stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

        final KStream<String, Long> counts = grouped
            .rangeOver(EventTimeRange.ofTimeBoundsWithNoGrace(BEFORE, AFTER))
            .count();
        counts.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> input = driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, Long> output = driver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), new LongDeserializer());

            input.pipeInput(null, "v1", 10L);
            input.pipeInput("a", "v2", 20L);

            final List<KeyValue<String, Long>> results = output.readKeyValuesToList();
            assertEquals(1, results.size());
            assertEquals("a", results.get(0).key);
        }
    }

    @Test
    public void nullValueShouldBeDropped() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KGroupedStream<String, String> grouped = builder
            .stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

        final KStream<String, Long> counts = grouped
            .rangeOver(EventTimeRange.ofTimeBoundsWithNoGrace(BEFORE, AFTER))
            .count();
        counts.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> input = driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, Long> output = driver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), new LongDeserializer());

            input.pipeInput("a", null, 10L);
            input.pipeInput("a", "v2", 20L);

            final List<KeyValue<String, Long>> results = output.readKeyValuesToList();
            assertEquals(1, results.size());
            assertEquals(1L, (long) results.get(0).value);
        }
    }

    @Test
    public void multipleAggregationsOnSameBuffer() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KGroupedStream<String, String> grouped = builder
            .stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

        final Materialized<String, String, WindowStore<Bytes, byte[]>> mat =
            Materialized.<String, String, WindowStore<Bytes, byte[]>>as("shared-buffer")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
                .withRetention(BEFORE.plusMillis(1));

        final RangedKStream<String, String> ranged = grouped.rangeOver(
            EventTimeRange.ofTimeBoundsWithNoGrace(BEFORE, AFTER), mat
        );

        final KStream<String, Long> counts = ranged.count();
        final KStream<String, Long> doubled = ranged.aggregate((anchor, records) -> {
            long n = 0L;
            final java.util.Iterator<?> it = records.iterator();
            while (it.hasNext()) {
                it.next();
                n++;
            }
            return n * 2;
        });

        counts.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
        final String outputTopic2 = "output2";
        doubled.to(outputTopic2, Produced.with(Serdes.String(), Serdes.Long()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> input = driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, Long> out1 = driver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), new LongDeserializer());
            final TestOutputTopic<String, Long> out2 = driver.createOutputTopic(outputTopic2, new StringDeserializer(), new LongDeserializer());

            input.pipeInput("a", "v1", 10L);

            final long count = out1.readKeyValue().value;
            final long agg = out2.readKeyValue().value;

            assertEquals(1L, count);
            assertEquals(2L, agg);
        }
    }

    @Test
    public void withMaxRecordsShouldCapRange() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KGroupedStream<String, String> grouped = builder
            .stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

        final Materialized<String, String, WindowStore<Bytes, byte[]>> mat =
            Materialized.<String, String, WindowStore<Bytes, byte[]>>as("maxrec-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
                .withRetention(Duration.ofMillis(200));

        final KStream<String, Long> counts = grouped
            .rangeOver(
                EventTimeRange.<String, String>ofTimeBoundsWithNoGrace(Duration.ofMillis(150), AFTER).withMaxRecords(2),
                mat
            )
            .count();
        counts.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> input = driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, Long> output = driver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), new LongDeserializer());

            input.pipeInput("a", "v1", 10L);
            input.pipeInput("a", "v2", 50L);
            input.pipeInput("a", "v3", 100L);
            input.pipeInput("a", "v4", 120L);

            final List<KeyValue<String, Long>> results = output.readKeyValuesToList();
            // At t=120: range [-30, 170], all 4 records in range but maxRecords=2
            final long lastCount = results.get(results.size() - 1).value;
            assertTrue(lastCount <= 2L, "Expected count <= 2 due to maxRecords, got: " + lastCount);
        }
    }

    // ---- EventCountRange end-to-end ----

    @Test
    public void eventCountRangeShouldIncludeCorrectCounts() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KGroupedStream<String, String> grouped = builder
            .stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

        final Materialized<String, String, WindowStore<Bytes, byte[]>> mat =
            Materialized.<String, String, WindowStore<Bytes, byte[]>>as("count-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
                .withRetention(Duration.ofHours(1).plusSeconds(5));

        final KStream<String, Long> counts = grouped
            .rangeOver(
                EventCountRange.ofCountBoundsWithNoGrace(1, 0, Duration.ofHours(1)),
                mat
            )
            .count();
        counts.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> input = driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, Long> output = driver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), new LongDeserializer());

            input.pipeInput("a", "v1", 100L);
            input.pipeInput("a", "v2", 200L);
            input.pipeInput("a", "v3", 300L);

            final List<KeyValue<String, Long>> results = output.readKeyValuesToList();
            assertEquals(3, results.size());
            // At t=100: no records before → just the anchor → count=1
            assertEquals(1L, (long) results.get(0).value);
            // At t=200: 1 before (v1) + anchor (v2) = 2
            assertEquals(2L, (long) results.get(1).value);
            // At t=300: 1 before (v2) + anchor (v3) = 2
            assertEquals(2L, (long) results.get(2).value);
        }
    }

    // ---- Retention validation ----

    @Test
    public void rangeOverShouldThrowWhenRetentionTooSmall() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KGroupedStream<String, String> grouped = builder
            .stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

        // before=100ms, grace=20ms → required retention=120ms. Providing 50ms → should throw.
        final Materialized<String, String, WindowStore<Bytes, byte[]>> mat =
            Materialized.<String, String, WindowStore<Bytes, byte[]>>as("tiny-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
                .withRetention(Duration.ofMillis(50));

        assertThrows(IllegalArgumentException.class, () ->
            grouped.rangeOver(
                EventTimeRange.ofTimeBoundsAndGrace(Duration.ofMillis(100), Duration.ofMillis(50), Duration.ofMillis(20)),
                mat
            )
        );
    }

    // ---- retainDuplicates validation ----

    @Test
    public void rangeOverShouldThrowForSupplierWithoutRetainDuplicates() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KGroupedStream<String, String> grouped = builder
            .stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

        final WindowBytesStoreSupplier badSupplier =
            Stores.persistentWindowStore("no-dups", Duration.ofSeconds(10), Duration.ofMillis(1), false);

        final Materialized<String, String, WindowStore<Bytes, byte[]>> mat =
            Materialized.<String, String>as(badSupplier)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String());

        assertThrows(IllegalArgumentException.class, () ->
            grouped.rangeOver(EventTimeRange.ofTimeBoundsWithNoGrace(Duration.ofSeconds(5), Duration.ofSeconds(1)), mat)
        );
    }

    private static Materialized<String, String, WindowStore<Bytes, byte[]>> buildMaterialized(
        final String storeName,
        final boolean cachingEnabled
    ) {
        final Materialized<String, String, WindowStore<Bytes, byte[]>> mat =
            Materialized.<String, String, WindowStore<Bytes, byte[]>>as(storeName)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
                .withRetention(Duration.ofMillis(200));
        if (!cachingEnabled) {
            mat.withCachingDisabled();
        }
        return mat;
    }
}
