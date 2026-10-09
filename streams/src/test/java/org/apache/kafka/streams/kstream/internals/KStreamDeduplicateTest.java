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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Deduplicated;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.RocksDBTimeOrderedKeyValueBuffer;
import org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBuffer;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.test.MockInternalProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.apache.kafka.streams.state.Stores.keyValueStoreBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class KStreamDeduplicateTest {

    private static final Duration DEDUP_INTERVAL = Duration.ofMillis(100);
    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC = "output";

    private TopologyTestDriver deduplicateByKeyDriver() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
               .deduplicateByKey(DEDUP_INTERVAL)
               .to(OUTPUT_TOPIC);
        return new TopologyTestDriver(builder.build(),
            StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String()));
    }

    private TopologyTestDriver deduplicateByKeyValueDriver(final KeyValueMapper<String, String, String> mapper) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
               .deduplicateByKeyValue(mapper, DEDUP_INTERVAL, Deduplicated.<String, String, String>idSerde(Serdes.String()))
               .to(OUTPUT_TOPIC);
        return new TopologyTestDriver(builder.build(),
            StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String()));
    }

    private TestInputTopic<String, String> input(final TopologyTestDriver driver) {
        return driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer(),
            Instant.ofEpochMilli(0L), Duration.ZERO);
    }

    private TestOutputTopic<String, String> output(final TopologyTestDriver driver) {
        return driver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), new StringDeserializer());
    }

    @Test
    void shouldForwardFirstAndDropDuplicateWithinIntervalByKey() {
        try (final TopologyTestDriver driver = deduplicateByKeyDriver()) {
            final TestInputTopic<String, String> in = input(driver);
            in.pipeInput("k1", "v1", 0L);
            in.pipeInput("k1", "v2", 50L);
            final List<KeyValue<String, String>> results = output(driver).readKeyValuesToList();
            assertEquals(1, results.size());
            assertEquals(new KeyValue<>("k1", "v1"), results.get(0));
        }
    }

    @Test
    void shouldForwardFirstAndDropDuplicateWithinIntervalByKeyValue() {
        try (final TopologyTestDriver driver = deduplicateByKeyValueDriver((k, v) -> k)) {
            final TestInputTopic<String, String> in = input(driver);
            in.pipeInput("k1", "v1", 0L);
            in.pipeInput("k1", "v2", 50L);
            final List<KeyValue<String, String>> results = output(driver).readKeyValuesToList();
            assertEquals(1, results.size());
            assertEquals(new KeyValue<>("k1", "v1"), results.get(0));
        }
    }

    @Test
    void shouldDropDuplicateAtExactIntervalBoundaryByKey() {
        try (final TopologyTestDriver driver = deduplicateByKeyDriver()) {
            final TestInputTopic<String, String> in = input(driver);
            in.pipeInput("k1", "v1", 0L);
            in.pipeInput("k1", "v2", 100L);
            assertEquals(1, output(driver).readKeyValuesToList().size());
        }
    }

    @Test
    void shouldDropDuplicateAtExactIntervalBoundaryByKeyValue() {
        try (final TopologyTestDriver driver = deduplicateByKeyValueDriver((k, v) -> k)) {
            final TestInputTopic<String, String> in = input(driver);
            in.pipeInput("k1", "v1", 0L);
            in.pipeInput("k1", "v2", 100L);
            assertEquals(1, output(driver).readKeyValuesToList().size());
        }
    }

    @Test
    void shouldForwardRecordJustAfterIntervalExpiryByKey() {
        try (final TopologyTestDriver driver = deduplicateByKeyDriver()) {
            final TestInputTopic<String, String> in = input(driver);
            in.pipeInput("k1", "v1", 0L);
            in.pipeInput("k1", "v2", 101L); // (101 - 0) = 101 > 100 → FORWARD
            final List<KeyValue<String, String>> results = output(driver).readKeyValuesToList();
            assertEquals(2, results.size());
            assertEquals(new KeyValue<>("k1", "v1"), results.get(0));
            assertEquals(new KeyValue<>("k1", "v2"), results.get(1));
        }
    }

    @Test
    void shouldForwardRecordJustAfterIntervalExpiryByKeyValue() {
        try (final TopologyTestDriver driver = deduplicateByKeyValueDriver((k, v) -> k)) {
            final TestInputTopic<String, String> in = input(driver);
            in.pipeInput("k1", "v1", 0L);
            in.pipeInput("k1", "v2", 101L);
            final List<KeyValue<String, String>> results = output(driver).readKeyValuesToList();
            assertEquals(2, results.size());
            assertEquals(new KeyValue<>("k1", "v1"), results.get(0));
            assertEquals(new KeyValue<>("k1", "v2"), results.get(1));
        }
    }

    @Test
    void shouldAlwaysForwardNullKeyByKey() {
        try (final TopologyTestDriver driver = deduplicateByKeyDriver()) {
            final TestInputTopic<String, String> in = input(driver);
            in.pipeInput(null, "v1", 0L);
            in.pipeInput(null, "v2", 50L);
            assertEquals(2, output(driver).readKeyValuesToList().size());
        }
    }

    @Test
    void shouldAlwaysForwardNullComputedIdByKeyValue() {
        try (final TopologyTestDriver driver = deduplicateByKeyValueDriver((k, v) -> null)) {
            final TestInputTopic<String, String> in = input(driver);
            in.pipeInput("k1", "v1", 0L);
            in.pipeInput("k1", "v2", 50L);
            assertEquals(2, output(driver).readKeyValuesToList().size());
        }
    }

    @Test
    void shouldAlwaysForwardNullKeyByKeyValue() {
        try (final TopologyTestDriver driver = deduplicateByKeyValueDriver((k, v) -> v)) {
            final TestInputTopic<String, String> in = input(driver);
            in.pipeInput(null, "id1", 0L);
            in.pipeInput(null, "id1", 50L);
            assertEquals(2, output(driver).readKeyValuesToList().size());
        }
    }

    @Test
    void shouldDropOutOfOrderDuplicateWithinIntervalByKey() {
        try (final TopologyTestDriver driver = deduplicateByKeyDriver()) {
            final TestInputTopic<String, String> in = input(driver);
            in.pipeInput("k1", "v1", 50L); // observedStreamTime=50, stored at t=50
            in.pipeInput("k1", "v2", 30L); // (50 - 30) = 20 <= 100 → DROP
            final List<KeyValue<String, String>> results = output(driver).readKeyValuesToList();
            assertEquals(1, results.size());
            assertEquals(new KeyValue<>("k1", "v1"), results.get(0)); // first-arriving forwarded
        }
    }

    @Test
    void shouldDropOutOfOrderDuplicateWithinIntervalByKeyValue() {
        try (final TopologyTestDriver driver = deduplicateByKeyValueDriver((k, v) -> k)) {
            final TestInputTopic<String, String> in = input(driver);
            in.pipeInput("k1", "v1", 50L);
            in.pipeInput("k1", "v2", 30L);
            final List<KeyValue<String, String>> results = output(driver).readKeyValuesToList();
            assertEquals(1, results.size());
            assertEquals(new KeyValue<>("k1", "v1"), results.get(0));
        }
    }

    @Test
    void shouldForwardOutOfOrderRecordBeyondIntervalByKey() {
        try (final TopologyTestDriver driver = deduplicateByKeyDriver()) {
            final TestInputTopic<String, String> in = input(driver);
            in.pipeInput("k1", "v1", 200L); // stored at t=200
            in.pipeInput("k1", "v2", 50L);  // (200 - 50) = 150 > 100 → FORWARD
            final List<KeyValue<String, String>> results = output(driver).readKeyValuesToList();
            assertEquals(2, results.size());
            assertEquals(new KeyValue<>("k1", "v1"), results.get(0));
            assertEquals(new KeyValue<>("k1", "v2"), results.get(1));
        }
    }

    @Test
    void shouldForwardOutOfOrderRecordBeyondIntervalByKeyValue() {
        try (final TopologyTestDriver driver = deduplicateByKeyValueDriver((k, v) -> k)) {
            final TestInputTopic<String, String> in = input(driver);
            in.pipeInput("k1", "v1", 200L);
            in.pipeInput("k1", "v2", 50L);
            final List<KeyValue<String, String>> results = output(driver).readKeyValuesToList();
            assertEquals(2, results.size());
            assertEquals(new KeyValue<>("k1", "v1"), results.get(0));
            assertEquals(new KeyValue<>("k1", "v2"), results.get(1));
        }
    }

    @Test
    void shouldDropLateRecordWhenActiveWindowExistsByKey() {
        try (final TopologyTestDriver driver = deduplicateByKeyDriver()) {
            final TestInputTopic<String, String> in = input(driver);
            in.pipeInput("k1", "v1", 150L);
            in.pipeInput("other", "vX", 200L); // advance observedStreamTime
            in.pipeInput("k1", "v2", 60L);
            final List<KeyValue<String, String>> results = output(driver).readKeyValuesToList();
            assertEquals(2, results.size());
            assertEquals(new KeyValue<>("k1", "v1"), results.get(0)); // first-arriving forwarded
            assertEquals(new KeyValue<>("other", "vX"), results.get(1));
        }
    }

    @Test
    void shouldDropLateRecordWhenActiveWindowExistsByKeyValue() {
        try (final TopologyTestDriver driver = deduplicateByKeyValueDriver((k, v) -> k)) {
            final TestInputTopic<String, String> in = input(driver);
            in.pipeInput("k1", "v1", 150L);
            in.pipeInput("other", "vX", 200L);
            in.pipeInput("k1", "v2", 60L);
            final List<KeyValue<String, String>> results = output(driver).readKeyValuesToList();
            assertEquals(2, results.size());
            assertEquals(new KeyValue<>("k1", "v1"), results.get(0));
            assertEquals(new KeyValue<>("other", "vX"), results.get(1));
        }
    }

    @Test
    void shouldForwardLateRecordWhenWindowExpiredByKey() {
        try (final TopologyTestDriver driver = deduplicateByKeyDriver()) {
            final TestInputTopic<String, String> in = input(driver);
            in.pipeInput("other", "vX", 200L); // observedStreamTime=200
            in.pipeInput("k1", "v1", 50L);
            in.pipeInput("k1", "v2", 10L);
            final List<KeyValue<String, String>> results = output(driver).readKeyValuesToList();
            assertEquals(3, results.size());
            assertEquals(new KeyValue<>("other", "vX"), results.get(0));
            assertEquals(new KeyValue<>("k1", "v1"), results.get(1));
            assertEquals(new KeyValue<>("k1", "v2"), results.get(2));
        }
    }

    @Test
    void shouldForwardLateRecordWhenWindowExpiredByKeyValue() {
        try (final TopologyTestDriver driver = deduplicateByKeyValueDriver((k, v) -> k)) {
            final TestInputTopic<String, String> in = input(driver);
            in.pipeInput("other", "vX", 200L); // observedStreamTime=200
            in.pipeInput("k1", "v1", 50L);
            in.pipeInput("k1", "v2", 10L);
            final List<KeyValue<String, String>> results = output(driver).readKeyValuesToList();
            assertEquals(3, results.size());
            assertEquals(new KeyValue<>("other", "vX"), results.get(0));
            assertEquals(new KeyValue<>("k1", "v1"), results.get(1));
            assertEquals(new KeyValue<>("k1", "v2"), results.get(2));
        }
    }

    @Test
    void shouldDropDuplicateWhenSameKeyAndSameComputedIdByKeyValue() {
        // deduplication id = key + "|" + value
        try (final TopologyTestDriver driver = deduplicateByKeyValueDriver((k, v) -> k + "|" + v)) {
            final TestInputTopic<String, String> in = input(driver);
            in.pipeInput("k1", "id1", 0L);
            in.pipeInput("k1", "id1", 50L);
            assertEquals(1, output(driver).readKeyValuesToList().size());
        }
    }

    @Test
    void shouldForwardBothWhenSameIdButDifferentKeysByKeyValue() {
        try (final TopologyTestDriver driver = deduplicateByKeyValueDriver((k, v) -> v)) {
            final TestInputTopic<String, String> in = input(driver);
            in.pipeInput("k1", "id1", 0L);
            in.pipeInput("k2", "id1", 50L);
            assertEquals(2, output(driver).readKeyValuesToList().size());
        }
    }

    @Test
    void shouldDeduplicateDifferentKeysIndependentlyByKey() {
        try (final TopologyTestDriver driver = deduplicateByKeyDriver()) {
            final TestInputTopic<String, String> in = input(driver);
            in.pipeInput("k1", "v1", 0L);
            in.pipeInput("k2", "v2", 0L);
            in.pipeInput("k1", "dup1", 50L); // DROP — k1 window still open
            in.pipeInput("k2", "dup2", 50L); // DROP — k2 window still open
            final List<KeyValue<String, String>> results = output(driver).readKeyValuesToList();
            assertEquals(2, results.size());
            assertEquals(new KeyValue<>("k1", "v1"), results.get(0));
            assertEquals(new KeyValue<>("k2", "v2"), results.get(1));
        }
    }

    @Test
    void shouldDeduplicateDifferentKeysIndependentlyByKeyValue() {
        try (final TopologyTestDriver driver = deduplicateByKeyValueDriver((k, v) -> k)) {
            final TestInputTopic<String, String> in = input(driver);
            in.pipeInput("k1", "v1", 0L);
            in.pipeInput("k2", "v2", 0L);
            in.pipeInput("k1", "dup1", 50L);
            in.pipeInput("k2", "dup2", 50L);
            final List<KeyValue<String, String>> results = output(driver).readKeyValuesToList();
            assertEquals(2, results.size());
            assertEquals(new KeyValue<>("k1", "v1"), results.get(0));
            assertEquals(new KeyValue<>("k2", "v2"), results.get(1));
        }
    }

    @Test
    void shouldForwardOnSameOffsetRedelivery() throws Exception {
        final java.io.File stateDir = TestUtils.tempDirectory();
        KeyValueStore<Bytes, TimestampAndOffset> baseStore = null;
        TimeOrderedKeyValueBuffer<Bytes, String, String> timeIndexStore = null;
        try {
            final java.util.Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
            final MockInternalProcessorContext<String, String> context =
                new MockInternalProcessorContext<>(props,
                    new org.apache.kafka.streams.processor.TaskId(0, 0), stateDir);

            final String baseStoreName = "base";
            final String timeIndexStoreName = baseStoreName + "-time-index";

            baseStore = keyValueStoreBuilder(
                org.apache.kafka.streams.state.Stores.persistentKeyValueStore(baseStoreName),
                Serdes.Bytes(), new TimestampAndOffsetSerde())
                .withLoggingDisabled()
                .build();

            timeIndexStore =
                new RocksDBTimeOrderedKeyValueBuffer.Builder<>(
                    timeIndexStoreName, Serdes.Bytes(), Serdes.String(), DEDUP_INTERVAL, "topic")
                    .withLoggingDisabled()
                    .build();

            context.addStateStore(baseStore);
            baseStore.init(context, baseStore);
            context.addStateStore(timeIndexStore);
            timeIndexStore.init(context, timeIndexStore);

            final KStreamDeduplicateProcessor<String, String, String> processor =
                new KStreamDeduplicateProcessor<>((k, v) -> k, DEDUP_INTERVAL,
                    Serdes.String(), null, baseStoreName, timeIndexStoreName);
            processor.init(context);

            context.setRecordMetadata("input", 0, 42L);
            processor.process(new Record<>("k1", "v1", 0L));

            // Same offset received -> Forward
            context.setRecordMetadata("input", 0, 42L);
            processor.process(new Record<>("k1", "v1", 0L));

            assertEquals(2, context.forwarded().size());
        } finally {
            if (baseStore != null) baseStore.close();
            if (timeIndexStore != null) timeIndexStore.close();
            org.apache.kafka.common.utils.Utils.delete(stateDir);
        }
    }

    @Test
    void shouldDropDuplicatePunctuatedRecords() throws Exception {
        final java.io.File stateDir = TestUtils.tempDirectory();
        KeyValueStore<Bytes, TimestampAndOffset> baseStore = null;
        TimeOrderedKeyValueBuffer<Bytes, String, String> timeIndexStore = null;
        try {
            final java.util.Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
            final MockInternalProcessorContext<String, String> context =
                new MockInternalProcessorContext<>(props,
                    new org.apache.kafka.streams.processor.TaskId(0, 0), stateDir) {
                    @Override
                    public java.util.Optional<RecordMetadata> recordMetadata() {
                        return java.util.Optional.empty(); // punctuated record
                    }
                    @Override
                    public ProcessorRecordContext recordContext() {
                        return new ProcessorRecordContext(timestamp(), 0L, 0, "topic", headers());
                    }
                };

            final String baseStoreName = "base";
            final String timeIndexStoreName = baseStoreName + "-time-index";

            baseStore = keyValueStoreBuilder(
                Stores.persistentKeyValueStore(baseStoreName),
                Serdes.Bytes(), new TimestampAndOffsetSerde())
                .withLoggingDisabled()
                .build();

            timeIndexStore = new RocksDBTimeOrderedKeyValueBuffer.Builder<>(
                    timeIndexStoreName, Serdes.Bytes(), Serdes.String(), DEDUP_INTERVAL, "topic")
                    .withLoggingDisabled()
                    .build();

            context.addStateStore(baseStore);
            baseStore.init(context, baseStore);
            context.addStateStore(timeIndexStore);
            timeIndexStore.init(context, timeIndexStore);

            final KStreamDeduplicateProcessor<String, String, String> processor =
                new KStreamDeduplicateProcessor<>((k, v) -> k, DEDUP_INTERVAL,
                    Serdes.String(), null, baseStoreName, timeIndexStoreName);
            processor.init(context);

            // Empty offset for both
            processor.process(new Record<>("k1", "v1", 0L));
            processor.process(new Record<>("k1", "v2", 50L));

            assertEquals(1, context.forwarded().size(), "punctuated duplicate must be dropped");
        } finally {
            if (baseStore != null) baseStore.close();
            if (timeIndexStore != null) timeIndexStore.close();
            org.apache.kafka.common.utils.Utils.delete(stateDir);
        }
    }

    @Test
    void shouldEvictExpiredEntryFromBothStoresByKey() {
        final String storeName = "eviction-test-store";
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
               .deduplicateByKey(DEDUP_INTERVAL, Deduplicated.as(storeName))
               .to(OUTPUT_TOPIC);
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(),
                 StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String()))) {
            final TestInputTopic<String, String> in = input(driver);
            in.pipeInput("k1", "v1", 0L);
            in.pipeInput("dummy", "vX", 200L); // advances stream time → punctuator fires → k1 evicted

            @SuppressWarnings({"unchecked", "rawtypes"})
            final KeyValueStore<Bytes, TimestampAndOffset> baseStore =
                (KeyValueStore<Bytes, TimestampAndOffset>) (KeyValueStore) driver.getKeyValueStore(storeName);
            long baseStoreCount = 0;
            try (final KeyValueIterator<Bytes, TimestampAndOffset> it = baseStore.all()) {
                while (it.hasNext()) {
                    it.next();
                    baseStoreCount++;
                }
            }
            assertEquals(1L, baseStoreCount);

            @SuppressWarnings("unchecked")
            final TimeOrderedKeyValueBuffer<Bytes, String, String> timeIndexStore =
                (TimeOrderedKeyValueBuffer<Bytes, String, String>) driver.getStateStore(storeName + "-time-index");
            assertEquals(1, timeIndexStore.numRecords());
        }
    }

    @Test
    void shouldEvictExpiredEntryFromBothStoresByKeyValue() {
        final String storeName = "eviction-test-store";
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
               .deduplicateByKeyValue((k, v) -> k, DEDUP_INTERVAL, Deduplicated.<String, String, String>idSerde(Serdes.String()).withStoreName(storeName))
               .to(OUTPUT_TOPIC);
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(),
                 StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String()))) {
            final TestInputTopic<String, String> in = input(driver);
            in.pipeInput("k1", "v1", 0L);
            in.pipeInput("dummy", "vX", 200L); // advances stream time → punctuator fires → k1 evicted

            @SuppressWarnings({"unchecked", "rawtypes"})
            final KeyValueStore<Bytes, TimestampAndOffset> baseStore =
                (KeyValueStore<Bytes, TimestampAndOffset>) (KeyValueStore) driver.getKeyValueStore(storeName);
            long baseStoreCount = 0;
            try (final KeyValueIterator<Bytes, TimestampAndOffset> it = baseStore.all()) {
                while (it.hasNext()) {
                    it.next();
                    baseStoreCount++;
                }
            }
            assertEquals(1L, baseStoreCount);

            @SuppressWarnings("unchecked")
            final TimeOrderedKeyValueBuffer<Bytes, String, String> timeIndexStore =
                (TimeOrderedKeyValueBuffer<Bytes, String, String>) driver.getStateStore(storeName + "-time-index");
            assertEquals(1, timeIndexStore.numRecords());
        }
    }
}
