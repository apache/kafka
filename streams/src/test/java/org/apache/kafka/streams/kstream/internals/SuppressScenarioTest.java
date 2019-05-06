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
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
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

public class SuppressScenarioTest {
    private static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();
    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final LongDeserializer LONG_DESERIALIZER = new LongDeserializer();
    private final Properties config = Utils.mkProperties(Utils.mkMap(
        Utils.mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, getClass().getSimpleName().toLowerCase(Locale.getDefault())),
        Utils.mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
        Utils.mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "bogus")
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


        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            driver.pipeInput(recordFactory.create("input", "k1", "v2", 1L));
            driver.pipeInput(recordFactory.create("input", "k2", "v1", 2L));
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
            driver.pipeInput(recordFactory.create("input", "x", "x", 3L));
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
            driver.pipeInput(recordFactory.create("input", "x", "x", 4L));
            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("x", 0L, 4L),
                    new KeyValueTimestamp<>("x", 1L, 4L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("x", 0L, 4L),
                    new KeyValueTimestamp<>("x", 1L, 4L)
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
        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            driver.pipeInput(recordFactory.create("input", "k1", "v2", 1L));
            driver.pipeInput(recordFactory.create("input", "k2", "v1", 2L));
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
            driver.pipeInput(recordFactory.create("input", "tick", "tick", 3L));
            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(new KeyValueTimestamp<>("tick", 1L, 3L))
            );
            // the stream time is now 3, so it's time to emit this record
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                singletonList(new KeyValueTimestamp<>("v2", 1L, 1L))
            );


            driver.pipeInput(recordFactory.create("input", "tick", "tick", 4L));
            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("tick", 0L, 4L),
                    new KeyValueTimestamp<>("tick", 1L, 4L)
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
        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            driver.pipeInput(recordFactory.create("input", "k1", "v2", 1L));
            driver.pipeInput(recordFactory.create("input", "k2", "v1", 2L));
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
            driver.pipeInput(recordFactory.create("input", "x", "x", 3L));
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
        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            driver.pipeInput(recordFactory.create("input", "k1", "v2", 1L));
            driver.pipeInput(recordFactory.create("input", "k2", "v1", 2L));
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
            driver.pipeInput(recordFactory.create("input", "x", "x", 3L));
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
            .windowedBy(TimeWindows.of(ofMillis(2L)).grace(ofMillis(1L)))
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
        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 1L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 2L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 1L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 5L));
            // note this last record gets dropped because it is out of the grace period
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("[k1@0/2]", 1L, 0L),
                    new KeyValueTimestamp<>("[k1@0/2]", 2L, 1L),
                    new KeyValueTimestamp<>("[k1@2/4]", 1L, 2L),
                    new KeyValueTimestamp<>("[k1@0/2]", 3L, 1L),
                    new KeyValueTimestamp<>("[k1@0/2]", 4L, 0L),
                    new KeyValueTimestamp<>("[k1@4/6]", 1L, 5L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("[k1@0/2]", 4L, 0L),
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
            .windowedBy(TimeWindows.of(ofMillis(2L)).grace(ofMillis(2L)))
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
        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 1L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 2L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 3L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 4L));
            // this update should get dropped, since the previous event advanced the stream time and closed the window.
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 30L));
            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("[k1@0/2]", 1L, 0L),
                    new KeyValueTimestamp<>("[k1@0/2]", 2L, 1L),
                    new KeyValueTimestamp<>("[k1@2/4]", 1L, 2L),
                    new KeyValueTimestamp<>("[k1@0/2]", 3L, 0L),
                    new KeyValueTimestamp<>("[k1@2/4]", 2L, 3L),
                    new KeyValueTimestamp<>("[k1@0/2]", 4L, 0L),
                    new KeyValueTimestamp<>("[k1@4/6]", 1L, 4L),
                    new KeyValueTimestamp<>("[k1@30/32]", 1L, 30L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("[k1@0/2]", 4L, 0L),
                    new KeyValueTimestamp<>("[k1@2/4]", 2L, 3L),
                    new KeyValueTimestamp<>("[k1@4/6]", 1L, 4L)
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
            .windowedBy(SessionWindows.with(ofMillis(5L)).grace(ofMillis(0L)))
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
        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {
            // first window
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 5L));
            // arbitrarily disordered records are admitted, because the *window* is not closed until stream-time > window-end + grace
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 1L));
            // any record in the same partition advances stream time (note the key is different)
            driver.pipeInput(recordFactory.create("input", "k2", "v1", 6L));
            // late event for first window - this should get dropped from all streams, since the first window is now closed.
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 5L));
            // just pushing stream time forward to flush the other events through.
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 30L));
            verify(
                drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("[k1@0/0]", 1L, 0L),
                    new KeyValueTimestamp<>("[k1@0/0]", null, 5L),
                    new KeyValueTimestamp<>("[k1@0/5]", 2L, 5L),
                    new KeyValueTimestamp<>("[k1@0/5]", null, 1L),
                    new KeyValueTimestamp<>("[k1@0/5]", 3L, 1L),
                    new KeyValueTimestamp<>("[k2@6/6]", 1L, 6L),
                    new KeyValueTimestamp<>("[k1@30/30]", 1L, 30L)
                )
            );
            verify(
                drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
                asList(
                    new KeyValueTimestamp<>("[k1@0/5]", 3L, 1L),
                    new KeyValueTimestamp<>("[k2@6/6]", 1L, 6L)
                )
            );
        }
    }

    private static <K, V> void verify(final List<ProducerRecord<K, V>> results, final List<KeyValueTimestamp<K, V>> expectedResults) {
        if (results.size() != expectedResults.size()) {
            throw new AssertionError(printRecords(results) + " != " + expectedResults);
        }
        final Iterator<KeyValueTimestamp<K, V>> expectedIterator = expectedResults.iterator();
        for (final ProducerRecord<K, V> result : results) {
            final KeyValueTimestamp<K, V> expected = expectedIterator.next();
            try {
                OutputVerifier.compareKeyValueTimestamp(result, expected.key(), expected.value(), expected.timestamp());
            } catch (final AssertionError e) {
                throw new AssertionError(printRecords(results) + " != " + expectedResults, e);
            }
        }
    }

    private static <K, V> List<ProducerRecord<K, V>> drainProducerRecords(final TopologyTestDriver driver, final String topic, final Deserializer<K> keyDeserializer, final Deserializer<V> valueDeserializer) {
        final List<ProducerRecord<K, V>> result = new LinkedList<>();
        for (ProducerRecord<K, V> next = driver.readOutput(topic, keyDeserializer, valueDeserializer);
             next != null;
             next = driver.readOutput(topic, keyDeserializer, valueDeserializer)) {
            result.add(next);
        }
        return new ArrayList<>(result);
    }

    private static <K, V> String printRecords(final List<ProducerRecord<K, V>> result) {
        final StringBuilder resultStr = new StringBuilder();
        resultStr.append("[\n");
        for (final ProducerRecord<?, ?> record : result) {
            resultStr.append("  ").append(record).append("\n");
        }
        resultStr.append("]");
        return resultStr.toString();
    }
}
