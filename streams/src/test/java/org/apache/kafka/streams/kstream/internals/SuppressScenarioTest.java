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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static java.time.Duration.ZERO;
import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxBytes;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxRecords;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@SuppressWarnings("deprecation")
public class SuppressScenarioTest {
    private static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();
    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final LongDeserializer LONG_DESERIALIZER = new LongDeserializer();
    private final Properties config = Utils.mkProperties(Utils.mkMap(
        Utils.mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath())
    ));

    @Test
    public void shouldImmediatelyEmitEventsWithZeroEmitAfter() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String, Long> valueCounts = builder
            .table(
                "input",
                Consumed.with(STRING_SERDE, STRING_SERDE),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>with(STRING_SERDE, STRING_SERDE)
                    .withCachingDisabled()
                    .withLoggingDisabled()
            )
            .groupBy((k, v) -> new KeyValue<>(v, k), Grouped.with(STRING_SERDE, STRING_SERDE))
            .count();

        valueCounts
            .suppress(untilTimeLimit(ZERO, unbounded()))
            .toStream()
            .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));

        valueCounts
            .toStream()
            .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));

        final Topology topology = builder.build();

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic("input", STRING_SERIALIZER, STRING_SERIALIZER);
            inputTopic.pipeInput("k1", "v1", 0L);
            inputTopic.pipeInput("k1", "v2", 1L);
            inputTopic.pipeInput("k2", "v1", 2L);
            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("v1", 1L, 0L),
                    new KeyValueTimestamp<>("v1", 0L, 1L),
                    new KeyValueTimestamp<>("v2", 1L, 1L),
                    new KeyValueTimestamp<>("v1", 1L, 2L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("v1", 1L, 0L),
                    new KeyValueTimestamp<>("v1", 0L, 1L),
                    new KeyValueTimestamp<>("v2", 1L, 1L),
                    new KeyValueTimestamp<>("v1", 1L, 2L)
                )
            );
            inputTopic.pipeInput("x", "x", 3L);
            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(
                    new KeyValueTimestamp<>("x", 1L, 3L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(
                    new KeyValueTimestamp<>("x", 1L, 3L)
                )
            );
            inputTopic.pipeInput("x", "y", 4L);
            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("x", 0L, 4L),
                    new KeyValueTimestamp<>("y", 1L, 4L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("x", 0L, 4L),
                    new KeyValueTimestamp<>("y", 1L, 4L)
                )
            );
        }
    }

    @Test
    public void shouldSuppressIntermediateEventsWithTimeLimit() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, Long> valueCounts = builder
            .table(
                "input",
                Consumed.with(STRING_SERDE, STRING_SERDE),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>with(STRING_SERDE, STRING_SERDE)
                    .withCachingDisabled()
                    .withLoggingDisabled()
            )
            .groupBy((k, v) -> new KeyValue<>(v, k), Grouped.with(STRING_SERDE, STRING_SERDE))
            .count();
        valueCounts
            .suppress(untilTimeLimit(ofMillis(2L), unbounded()))
            .toStream()
            .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));
        valueCounts
            .toStream()
            .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));
        final Topology topology = builder.build();
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic("input", STRING_SERIALIZER, STRING_SERIALIZER);
            inputTopic.pipeInput("k1", "v1", 0L);
            inputTopic.pipeInput("k1", "v2", 1L);
            inputTopic.pipeInput("k2", "v1", 2L);
            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("v1", 1L, 0L),
                    new KeyValueTimestamp<>("v1", 0L, 1L),
                    new KeyValueTimestamp<>("v2", 1L, 1L),
                    new KeyValueTimestamp<>("v1", 1L, 2L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(new KeyValueTimestamp<>("v1", 1L, 2L))
            );
            // inserting a dummy "tick" record just to advance stream time
            inputTopic.pipeInput("tick", "tick", 3L);
            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(new KeyValueTimestamp<>("tick", 1L, 3L))
            );
            // the stream time is now 3, so it's time to emit this record
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(new KeyValueTimestamp<>("v2", 1L, 1L))
            );


            inputTopic.pipeInput("tick", "tock", 4L);
            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("tick", 0L, 4L),
                    new KeyValueTimestamp<>("tock", 1L, 4L)
                )
            );
            // tick is still buffered, since it was first inserted at time 3, and it is only time 4 right now.
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                emptyList()
            );
        }
    }

    @Test
    public void shouldSuppressIntermediateEventsWithRecordLimit() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, Long> valueCounts = builder
            .table(
                "input",
                Consumed.with(STRING_SERDE, STRING_SERDE),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>with(STRING_SERDE, STRING_SERDE)
                    .withCachingDisabled()
                    .withLoggingDisabled()
            )
            .groupBy((k, v) -> new KeyValue<>(v, k), Grouped.with(STRING_SERDE, STRING_SERDE))
            .count(Materialized.with(STRING_SERDE, Serdes.Long()));
        valueCounts
            .suppress(untilTimeLimit(ofMillis(Long.MAX_VALUE), maxRecords(1L).emitEarlyWhenFull()))
            .toStream()
            .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));
        valueCounts
            .toStream()
            .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));
        final Topology topology = builder.build();
        System.out.println(topology.describe());
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic("input", STRING_SERIALIZER, STRING_SERIALIZER);
            inputTopic.pipeInput("k1", "v1", 0L);
            inputTopic.pipeInput("k1", "v2", 1L);
            inputTopic.pipeInput("k2", "v1", 2L);
            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("v1", 1L, 0L),
                    new KeyValueTimestamp<>("v1", 0L, 1L),
                    new KeyValueTimestamp<>("v2", 1L, 1L),
                    new KeyValueTimestamp<>("v1", 1L, 2L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    // consecutive updates to v1 get suppressed into only the latter.
                    new KeyValueTimestamp<>("v1", 0L, 1L),
                    new KeyValueTimestamp<>("v2", 1L, 1L)
                    // the last update won't be evicted until another key comes along.
                )
            );
            inputTopic.pipeInput("x", "x", 3L);
            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(
                    new KeyValueTimestamp<>("x", 1L, 3L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(
                    // now we see that last update to v1, but we won't see the update to x until it gets evicted
                    new KeyValueTimestamp<>("v1", 1L, 2L)
                )
            );
        }
    }

    @Test
    public void shouldSuppressIntermediateEventsWithBytesLimit() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, Long> valueCounts = builder
            .table(
                "input",
                Consumed.with(STRING_SERDE, STRING_SERDE),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>with(STRING_SERDE, STRING_SERDE)
                    .withCachingDisabled()
                    .withLoggingDisabled()
            )
            .groupBy((k, v) -> new KeyValue<>(v, k), Grouped.with(STRING_SERDE, STRING_SERDE))
            .count();
        valueCounts
            // this is a bit brittle, but I happen to know that the entries are a little over 100 bytes in size.
            .suppress(untilTimeLimit(ofMillis(Long.MAX_VALUE), maxBytes(200L).emitEarlyWhenFull()))
            .toStream()
            .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));
        valueCounts
            .toStream()
            .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));
        final Topology topology = builder.build();
        System.out.println(topology.describe());
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic("input", STRING_SERIALIZER, STRING_SERIALIZER);
            inputTopic.pipeInput("k1", "v1", 0L);
            inputTopic.pipeInput("k1", "v2", 1L);
            inputTopic.pipeInput("k2", "v1", 2L);
            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("v1", 1L, 0L),
                    new KeyValueTimestamp<>("v1", 0L, 1L),
                    new KeyValueTimestamp<>("v2", 1L, 1L),
                    new KeyValueTimestamp<>("v1", 1L, 2L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    // consecutive updates to v1 get suppressed into only the latter.
                    new KeyValueTimestamp<>("v1", 0L, 1L),
                    new KeyValueTimestamp<>("v2", 1L, 1L)
                    // the last update won't be evicted until another key comes along.
                )
            );
            inputTopic.pipeInput("x", "x", 3L);
            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(
                    new KeyValueTimestamp<>("x", 1L, 3L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(
                    // now we see that last update to v1, but we won't see the update to x until it gets evicted
                    new KeyValueTimestamp<>("v1", 1L, 2L)
                )
            );
        }
    }

    @Test
    public void shouldSupportFinalResultsForTimeWindows() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Windowed<String>, Long> valueCounts = builder
            .stream("input", Consumed.with(STRING_SERDE, STRING_SERDE))
            .groupBy((String k, String v) -> k, Grouped.with(STRING_SERDE, STRING_SERDE))
            .windowedBy(TimeWindows.ofSizeAndGrace(ofMillis(2L), ofMillis(1L)))
            .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("counts").withCachingDisabled());
        valueCounts
            .suppress(untilWindowCloses(unbounded()))
            .toStream()
            .map((final Windowed<String> k, final Long v) -> new KeyValue<>(k.toString(), v))
            .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));
        valueCounts
            .toStream()
            .map((final Windowed<String> k, final Long v) -> new KeyValue<>(k.toString(), v))
            .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));
        final Topology topology = builder.build();
        System.out.println(topology.describe());
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic("input", STRING_SERIALIZER, STRING_SERIALIZER);
            inputTopic.pipeInput("k1", "v1", 0L);
            inputTopic.pipeInput("k1", "v1", 1L);
            inputTopic.pipeInput("k1", "v1", 2L);
            inputTopic.pipeInput("k1", "v1", 1L);
            inputTopic.pipeInput("k1", "v1", 0L);
            inputTopic.pipeInput("k1", "v1", 5L);
            // note this last record gets dropped because it is out of the grace period
            inputTopic.pipeInput("k1", "v1", 0L);
            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("[k1@0/2]", 1L, 0L),
                    new KeyValueTimestamp<>("[k1@0/2]", 2L, 1L),
                    new KeyValueTimestamp<>("[k1@2/4]", 1L, 2L),
                    new KeyValueTimestamp<>("[k1@0/2]", 3L, 1L),
                    new KeyValueTimestamp<>("[k1@0/2]", 4L, 1L),
                    new KeyValueTimestamp<>("[k1@4/6]", 1L, 5L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("[k1@0/2]", 4L, 1L),
                    new KeyValueTimestamp<>("[k1@2/4]", 1L, 2L)
                )
            );
        }
    }

    @Test
    public void shouldSupportFinalResultsForTimeWindowsWithLargeJump() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Windowed<String>, Long> valueCounts = builder
            .stream("input", Consumed.with(STRING_SERDE, STRING_SERDE))
            .groupBy((String k, String v) -> k, Grouped.with(STRING_SERDE, STRING_SERDE))
            .windowedBy(TimeWindows.ofSizeAndGrace(ofMillis(2L), ofMillis(2L)))
            .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("counts").withCachingDisabled().withKeySerde(STRING_SERDE));
        valueCounts
            .suppress(untilWindowCloses(unbounded()))
            .toStream()
            .map((final Windowed<String> k, final Long v) -> new KeyValue<>(k.toString(), v))
            .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));
        valueCounts
            .toStream()
            .map((final Windowed<String> k, final Long v) -> new KeyValue<>(k.toString(), v))
            .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));
        final Topology topology = builder.build();
        System.out.println(topology.describe());
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic("input", STRING_SERIALIZER, STRING_SERIALIZER);
            inputTopic.pipeInput("k1", "v1", 0L);
            inputTopic.pipeInput("k1", "v1", 1L);
            inputTopic.pipeInput("k1", "v1", 2L);
            inputTopic.pipeInput("k1", "v1", 0L);
            inputTopic.pipeInput("k1", "v1", 3L);
            inputTopic.pipeInput("k1", "v1", 0L);
            inputTopic.pipeInput("k1", "v1", 4L);
            // this update should get dropped, since the previous event advanced the stream time and closed the window.
            inputTopic.pipeInput("k1", "v1", 0L);
            inputTopic.pipeInput("k1", "v1", 30L);
            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("[k1@0/2]", 1L, 0L),
                    new KeyValueTimestamp<>("[k1@0/2]", 2L, 1L),
                    new KeyValueTimestamp<>("[k1@2/4]", 1L, 2L),
                    new KeyValueTimestamp<>("[k1@0/2]", 3L, 1L),
                    new KeyValueTimestamp<>("[k1@2/4]", 2L, 3L),
                    new KeyValueTimestamp<>("[k1@0/2]", 4L, 1L),
                    new KeyValueTimestamp<>("[k1@4/6]", 1L, 4L),
                    new KeyValueTimestamp<>("[k1@30/32]", 1L, 30L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("[k1@0/2]", 4L, 1L),
                    new KeyValueTimestamp<>("[k1@2/4]", 2L, 3L),
                    new KeyValueTimestamp<>("[k1@4/6]", 1L, 4L)
                )
            );
        }
    }

    @Test
    public void shouldSupportFinalResultsForSlidingWindows() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Windowed<String>, Long> valueCounts = builder
                .stream("input", Consumed.with(STRING_SERDE, STRING_SERDE))
                .groupBy((String k, String v) -> k, Grouped.with(STRING_SERDE, STRING_SERDE))
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(ofMillis(5L), ofMillis(15L)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("counts").withCachingDisabled().withKeySerde(STRING_SERDE));
        valueCounts
                .suppress(untilWindowCloses(unbounded()))
                .toStream()
                .map((final Windowed<String> k, final Long v) -> new KeyValue<>(k.toString(), v))
                .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));
        valueCounts
                .toStream()
                .map((final Windowed<String> k, final Long v) -> new KeyValue<>(k.toString(), v))
                .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));
        final Topology topology = builder.build();
        System.out.println(topology.describe());
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic("input", STRING_SERIALIZER, STRING_SERIALIZER);
            inputTopic.pipeInput("k1", "v1", 10L);
            inputTopic.pipeInput("k1", "v1", 11L);
            inputTopic.pipeInput("k1", "v1", 10L);
            inputTopic.pipeInput("k1", "v1", 13L);
            inputTopic.pipeInput("k1", "v1", 10L);
            inputTopic.pipeInput("k1", "v1", 24L);
            // this update should get dropped, since the previous event advanced the stream time and closed the window.
            inputTopic.pipeInput("k1", "v1", 5L);
            inputTopic.pipeInput("k1", "v1", 7L);
            // final record to advance stream time and flush windows
            inputTopic.pipeInput("k1", "v1", 90L);
            final Comparator<TestRecord<String, Long>> comparator =
                Comparator.comparing((TestRecord<String, Long> o) -> o.getKey())
                    .thenComparing((TestRecord<String, Long> o) -> o.timestamp());

            final List<TestRecord<String, Long>> actual = drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER);
            actual.sort(comparator);
            verify(
                actual,
                asList(
                    // right window for k1@10 created when k1@11 is processed
                    new KeyValueTimestamp<>("[k1@11/16]", 1L, 11L),
                    // right window for k1@10 updated when k1@13 is processed
                    new KeyValueTimestamp<>("[k1@11/16]", 2L, 13L),
                    // right window for k1@11 created when k1@13 is processed
                    new KeyValueTimestamp<>("[k1@12/17]", 1L, 13L),
                    // left window for k1@24 created when k1@24 is processed
                    new KeyValueTimestamp<>("[k1@19/24]", 1L, 24L),
                    // left window for k1@10 created when k1@10 is processed
                    new KeyValueTimestamp<>("[k1@5/10]", 1L, 10L),
                    // left window for k1@10 updated when k1@10 is processed
                    new KeyValueTimestamp<>("[k1@5/10]", 2L, 10L),
                    // left window for k1@10 updated when k1@10 is processed
                    new KeyValueTimestamp<>("[k1@5/10]", 3L, 10L),
                    // left window for k1@10 updated when k1@5 is processed
                    new KeyValueTimestamp<>("[k1@5/10]", 4L, 10L),
                    // left window for k1@10 updated when k1@7 is processed
                    new KeyValueTimestamp<>("[k1@5/10]", 5L, 10L),
                    // left window for k1@11 created when k1@11 is processed
                    new KeyValueTimestamp<>("[k1@6/11]", 2L, 11L),
                    // left window for k1@11 updated when k1@10 is processed
                    new KeyValueTimestamp<>("[k1@6/11]", 3L, 11L),
                    // left window for k1@11 updated when k1@10 is processed
                    new KeyValueTimestamp<>("[k1@6/11]", 4L, 11L),
                    // left window for k1@11 updated when k1@7 is processed
                    new KeyValueTimestamp<>("[k1@6/11]", 5L, 11L),
                    // left window for k1@13 created when k1@13 is processed
                    new KeyValueTimestamp<>("[k1@8/13]", 4L, 13L),
                    // left window for k1@13 updated when k1@10 is processed
                    new KeyValueTimestamp<>("[k1@8/13]", 5L, 13L),
                    // right window for k1@90 created when k1@90 is processed
                    new KeyValueTimestamp<>("[k1@85/90]", 1L, 90L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("[k1@5/10]", 5L, 10L),
                    new KeyValueTimestamp<>("[k1@6/11]", 5L, 11L),
                    new KeyValueTimestamp<>("[k1@8/13]", 5L, 13L),
                    new KeyValueTimestamp<>("[k1@11/16]", 2L, 13L),
                    new KeyValueTimestamp<>("[k1@12/17]", 1L, 13L),
                    new KeyValueTimestamp<>("[k1@19/24]", 1L, 24L)
                )
            );
        }
    }

    @Test
    public void shouldSupportFinalResultsForSessionWindows() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Windowed<String>, Long> valueCounts = builder
            .stream("input", Consumed.with(STRING_SERDE, STRING_SERDE))
            .groupBy((String k, String v) -> k, Grouped.with(STRING_SERDE, STRING_SERDE))
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(ofMillis(5L)))
            .count(Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("counts").withCachingDisabled());
        valueCounts
            .suppress(untilWindowCloses(unbounded()))
            .toStream()
            .map((final Windowed<String> k, final Long v) -> new KeyValue<>(k.toString(), v))
            .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));
        valueCounts
            .toStream()
            .map((final Windowed<String> k, final Long v) -> new KeyValue<>(k.toString(), v))
            .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));
        final Topology topology = builder.build();
        System.out.println(topology.describe());
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic("input", STRING_SERIALIZER, STRING_SERIALIZER);
            // first window
            inputTopic.pipeInput("k1", "v1", 0L);
            inputTopic.pipeInput("k1", "v1", 5L);
            // arbitrarily disordered records are admitted, because the *window* is not closed until stream-time > window-end + grace
            inputTopic.pipeInput("k1", "v1", 1L);
            // any record in the same partition advances stream time (note the key is different)
            inputTopic.pipeInput("k2", "v1", 11L);
            // late event for first window - this should get dropped from all streams, since the first window is now closed.
            inputTopic.pipeInput("k1", "v1", 5L);
            // just pushing stream time forward to flush the other events through.
            inputTopic.pipeInput("k1", "v1", 30L);
            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("[k1@0/0]", 1L, 0L),
                    new KeyValueTimestamp<>("[k1@0/0]", null, 0L),
                    new KeyValueTimestamp<>("[k1@0/5]", 2L, 5L),
                    new KeyValueTimestamp<>("[k1@0/5]", null, 5L),
                    new KeyValueTimestamp<>("[k1@0/5]", 3L, 5L),
                    new KeyValueTimestamp<>("[k2@11/11]", 1L, 11L),
                    new KeyValueTimestamp<>("[k1@30/30]", 1L, 30L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("[k1@0/5]", 3L, 5L),
                    new KeyValueTimestamp<>("[k2@11/11]", 1L, 11L)
                )
            );
        }
    }

    @Test
    public void shouldWorkBeforeGroupBy() {
        final StreamsBuilder builder = new StreamsBuilder();

        builder
            .table("topic", Consumed.with(Serdes.String(), Serdes.String()))
            .suppress(untilTimeLimit(ofMillis(10), unbounded()))
            .groupBy(KeyValue::pair, Grouped.with(Serdes.String(), Serdes.String()))
            .count()
            .toStream()
            .to("output", Produced.with(Serdes.String(), Serdes.Long()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic("topic", STRING_SERIALIZER, STRING_SERIALIZER);

            inputTopic.pipeInput("A", "a", 0L);
            inputTopic.pipeInput("tick", "tick", 10L);

            verify(
                drainProducerRecords(driver, "output", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(new KeyValueTimestamp<>("A", 1L, 0L))
            );
        }
    }

    @Test
    public void shouldWorkBeforeJoinRight() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String, String> left = builder
            .table("left", Consumed.with(Serdes.String(), Serdes.String()));

        final KTable<String, String> right = builder
            .table("right", Consumed.with(Serdes.String(), Serdes.String()))
            .suppress(untilTimeLimit(ofMillis(10), unbounded()));

        left
            .outerJoin(right, (l, r) -> String.format("(%s,%s)", l, r))
            .toStream()
            .to("output", Produced.with(Serdes.String(), Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config)) {
            final TestInputTopic<String, String> inputTopicRight =
                driver.createInputTopic("right", STRING_SERIALIZER, STRING_SERIALIZER);
            final TestInputTopic<String, String> inputTopicLeft =
                    driver.createInputTopic("left", STRING_SERIALIZER, STRING_SERIALIZER);

            inputTopicRight.pipeInput("B", "1", 0L);
            inputTopicRight.pipeInput("A", "1", 0L);
            // buffered, no output
            verify(
                drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
                emptyList()
            );


            inputTopicRight.pipeInput("tick", "tick", 10L);
            // flush buffer
            verify(
                drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("A", "(null,1)", 0L),
                    new KeyValueTimestamp<>("B", "(null,1)", 0L)
                )
            );


            inputTopicRight.pipeInput("A", "2", 11L);
            // buffered, no output
            verify(
                drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
                emptyList()
            );


            inputTopicLeft.pipeInput("A", "a", 12L);
            // should join with previously emitted right side
            verify(
                drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
                singletonList(new KeyValueTimestamp<>("A", "(a,1)", 12L))
            );


            inputTopicLeft.pipeInput("B", "b", 12L);
            // should view through to the parent KTable, since B is no longer buffered
            verify(
                drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
                singletonList(new KeyValueTimestamp<>("B", "(b,1)", 12L))
            );


            inputTopicLeft.pipeInput("A", "b", 13L);
            // should join with previously emitted right side
            verify(
                drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
                singletonList(new KeyValueTimestamp<>("A", "(b,1)", 13L))
            );


            inputTopicRight.pipeInput("tick", "tick1", 21L);
            verify(
                drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("tick", "(null,tick1)", 21), // just a testing artifact
                    new KeyValueTimestamp<>("A", "(b,2)", 13L)
                )
            );
        }

    }


    @Test
    public void shouldWorkBeforeJoinLeft() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String, String> left = builder
            .table("left", Consumed.with(Serdes.String(), Serdes.String()))
            .suppress(untilTimeLimit(ofMillis(10), unbounded()));

        final KTable<String, String> right = builder
            .table("right", Consumed.with(Serdes.String(), Serdes.String()));

        left
            .outerJoin(right, (l, r) -> String.format("(%s,%s)", l, r))
            .toStream()
            .to("output", Produced.with(Serdes.String(), Serdes.String()));

        final Topology topology = builder.build();
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {
            final TestInputTopic<String, String> inputTopicRight =
                driver.createInputTopic("right", STRING_SERIALIZER, STRING_SERIALIZER);
            final TestInputTopic<String, String> inputTopicLeft =
                    driver.createInputTopic("left", STRING_SERIALIZER, STRING_SERIALIZER);

            inputTopicLeft.pipeInput("B", "1", 0L);
            inputTopicLeft.pipeInput("A", "1", 0L);
            // buffered, no output
            verify(
                drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
                emptyList()
            );


            inputTopicLeft.pipeInput("tick", "tick", 10L);
            // flush buffer
            verify(
                drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("A", "(1,null)", 0L),
                    new KeyValueTimestamp<>("B", "(1,null)", 0L)
                )
            );


            inputTopicLeft.pipeInput("A", "2", 11L);
            // buffered, no output
            verify(
                drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
                emptyList()
            );


            inputTopicRight.pipeInput("A", "a", 12L);
            // should join with previously emitted left side
            verify(
                drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
                singletonList(new KeyValueTimestamp<>("A", "(1,a)", 12L))
            );


            inputTopicRight.pipeInput("B", "b", 12L);
            // should view through to the parent KTable, since B is no longer buffered
            verify(
                drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
                singletonList(new KeyValueTimestamp<>("B", "(1,b)", 12L))
            );


            inputTopicRight.pipeInput("A", "b", 13L);
            // should join with previously emitted left side
            verify(
                drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
                singletonList(new KeyValueTimestamp<>("A", "(1,b)", 13L))
            );


            inputTopicLeft.pipeInput("tick", "tick1", 21L);
            verify(
                drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("tick", "(tick1,null)", 21), // just a testing artifact
                    new KeyValueTimestamp<>("A", "(2,b)", 13L)
                )
            );
        }

    }

    @Test
    public void shouldWorkWithCogrouped() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KGroupedStream<String, String> stream1 = builder.stream("one", Consumed.with(Serdes.String(), Serdes.String())).groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
        final KGroupedStream<String, String> stream2 = builder.stream("two", Consumed.with(Serdes.String(), Serdes.String())).groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
        final KStream<Windowed<String>, Object> cogrouped = stream1.cogroup((key, value, aggregate) -> aggregate + value).cogroup(stream2, (key, value, aggregate) -> aggregate + value)
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(15)))
            .aggregate(() -> "", Named.as("test"), Materialized.as("store"))
            .suppress(Suppressed.untilWindowCloses(unbounded()))
            .toStream();
    }

    private static <K, V> void verify(final List<TestRecord<K, V>> results,
                                      final List<KeyValueTimestamp<K, V>> expectedResults) {
        if (results.size() != expectedResults.size()) {
            throw new AssertionError(printRecords(results) + " != " + expectedResults);
        }
        final Iterator<KeyValueTimestamp<K, V>> expectedIterator = expectedResults.iterator();
        for (final TestRecord<K, V> result : results) {
            final KeyValueTimestamp<K, V> expected = expectedIterator.next();
            try {
                assertThat(result, equalTo(new TestRecord<>(expected.key(), expected.value(), null, expected.timestamp())));
            } catch (final AssertionError e) {
                throw new AssertionError(printRecords(results) + " != " + expectedResults, e);
            }
        }
    }

    private static <K, V> List<TestRecord<K, V>> drainProducerRecords(final TopologyTestDriver driver,
                                                                      final String topic,
                                                                      final Deserializer<K> keyDeserializer,
                                                                      final Deserializer<V> valueDeserializer) {
        return driver.createOutputTopic(topic, keyDeserializer, valueDeserializer).readRecordsToList();
    }

    private static <K, V> String printRecords(final List<TestRecord<K, V>> result) {
        final StringBuilder resultStr = new StringBuilder();
        resultStr.append("[\n");
        for (final TestRecord<?, ?> record : result) {
            resultStr.append("  ").append(record).append("\n");
        }
        resultStr.append("]");
        return resultStr.toString();
    }
}
