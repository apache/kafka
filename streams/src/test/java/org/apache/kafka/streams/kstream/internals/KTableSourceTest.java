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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.LogCaptureAppender.Event;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyTestDriverWrapper;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.MockApiProcessor;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.apache.kafka.test.StreamsTestUtils.getMetricByName;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for KTable source functionality with both default (timestamp-only) and headers-aware store formats.
 * <p>
 * The DSL_STORE_FORMAT_CONFIG allows KTable to use either:
 * - "default": Stores only value and timestamp (ValueAndTimestamp)
 * - "headers": Stores value, timestamp, and headers (ValueTimestampHeaders)
 * </p>
 */
public class KTableSourceTest {
    private final Consumed<String, String> stringConsumed = Consumed.with(Serdes.String(), Serdes.String());

    /**
     * Provides both store format configurations for parameterized tests.
     */
    private static Stream<Arguments> storeFormats() {
        return Stream.of(
            Arguments.of("default"),
            Arguments.of("headers")
        );
    }

    private Properties getProps(final String storeFormat) {
        final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
        props.put(StreamsConfig.DSL_STORE_FORMAT_CONFIG, storeFormat);
        return props;
    }

    private static Headers makeHeaders(final String key, final String value) {
        final RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(key, value.getBytes()));
        return headers;
    }

    /**
     * Tests basic KTable functionality with both store formats.
     * Verifies that records are processed correctly regardless of format.
     * Additionally, verifies that headers are stored when using "headers" format.
     */
    @ParameterizedTest
    @MethodSource("storeFormats")
    public void testKTable(final String storeFormat) {
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";
        final String storeName = "ktable-store";

        final KTable<String, Integer> table1 = builder.table(topic1, Consumed.with(Serdes.String(), Serdes.Integer()), Materialized.as(storeName));

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table1.toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, Integer> inputTopic =
                    driver.createInputTopic(topic1, new StringSerializer(), new IntegerSerializer());

            // Send records with headers to simulate realistic usage
            final Headers headersA = makeHeaders("source", "test-A");
            final Headers headersB = makeHeaders("source", "test-B");
            final Headers headersC = makeHeaders("source", "test-C");
            final Headers headersD = makeHeaders("source", "test-D");

            inputTopic.pipeInput(new TestRecord<>("A", 1, headersA, 10L));
            inputTopic.pipeInput(new TestRecord<>("B", 2, headersB, 11L));
            inputTopic.pipeInput(new TestRecord<>("C", 3, headersC, 12L));
            inputTopic.pipeInput(new TestRecord<>("D", 4, headersD, 13L));
            inputTopic.pipeInput(new TestRecord<>("A", null, headersA, 14L));
            inputTopic.pipeInput(new TestRecord<>("B", null, headersB, 15L));

            // Verify all records were processed correctly with both formats
            assertEquals(
                asList(new KeyValueTimestamp<>("A", 1, 10L),
                    new KeyValueTimestamp<>("B", 2, 11L),
                    new KeyValueTimestamp<>("C", 3, 12L),
                    new KeyValueTimestamp<>("D", 4, 13L),
                    new KeyValueTimestamp<>("A", null, 14L),
                    new KeyValueTimestamp<>("B", null, 15L)),
                supplier.theCapturedProcessor().processed());

            // Demonstrate how developers can read from the KTable with headers
            // When using "headers" format, the store returns ValueTimestampHeaders which includes
            // value, timestamp, AND headers - allowing full record context to be retrieved
            if (storeFormat.equals("headers")) {
                // Get the headers-aware store from the driver
                final KeyValueStore<String, ValueTimestampHeaders<Integer>> store =
                    driver.getTimestampedKeyValueStoreWithHeaders(storeName);
                assertNotNull(store, "Headers-aware store should be created with 'headers' format");

                // Example: Reading key "C" returns value, timestamp, and headers
                final ValueTimestampHeaders<Integer> recordC = store.get("C");
                assertNotNull(recordC);
                assertEquals(3, recordC.value());                                              // Value: 3
                assertEquals(12L, recordC.timestamp());                                        // Timestamp: 12L
                assertNotNull(recordC.headers());                                              // Headers are preserved
                assertEquals("test-C", new String(recordC.headers().lastHeader("source").value()));

                // Example: Reading key "D" also returns full context
                final ValueTimestampHeaders<Integer> recordD = store.get("D");
                assertNotNull(recordD);
                assertEquals(4, recordD.value());                                              // Value: 4
                assertEquals(13L, recordD.timestamp());                                        // Timestamp: 13L
                assertEquals("test-D", new String(recordD.headers().lastHeader("source").value()));

                // Keys with null values (tombstones) are deleted from the store
                assertNull(store.get("A"));  // Was set to null at timestamp 14L
                assertNull(store.get("B"));  // Was set to null at timestamp 15L
            }
        }
    }

    @Disabled // we have disabled KIP-557 until KAFKA-12508 can be properly addressed
    @Test
    public void testKTableSourceEmitOnChange() {
        final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        builder.table(topic1, Consumed.with(Serdes.String(), Serdes.Integer()), Materialized.as("store"))
               .toStream()
               .to("output");

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, Integer> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new IntegerSerializer());
            final TestOutputTopic<String, Integer> outputTopic =
                driver.createOutputTopic("output", new StringDeserializer(), new IntegerDeserializer());

            inputTopic.pipeInput("A", 1, 10L);
            inputTopic.pipeInput("B", 2, 11L);
            inputTopic.pipeInput("A", 1, 12L);
            inputTopic.pipeInput("B", 3, 13L);
            // this record should be kept since this is out of order, so the timestamp
            // should be updated in this scenario
            inputTopic.pipeInput("A", 1, 9L);

            assertEquals(
                1.0,
                getMetricByName(driver.metrics(), "idempotent-update-skip-total", "stream-processor-node-metrics").metricValue()
            );

            assertEquals(
                asList(new TestRecord<>("A", 1, Instant.ofEpochMilli(10L)),
                           new TestRecord<>("B", 2, Instant.ofEpochMilli(11L)),
                           new TestRecord<>("B", 3, Instant.ofEpochMilli(13L)),
                           new TestRecord<>("A", 1, Instant.ofEpochMilli(9L))),
                outputTopic.readRecordsToList()
            );
        }
    }

    /**
     * Tests that KTable logs and meters records with null keys (which are skipped).
     * This behavior should be the same regardless of store format.
     */
    @ParameterizedTest
    @MethodSource("storeFormats")
    public void kTableShouldLogAndMeterOnSkippedRecords(final String storeFormat) {
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic = "topic";
        builder.table(topic, stringConsumed);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KTableSource.class);
             final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {

            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(
                    topic,
                    new StringSerializer(),
                    new StringSerializer(),
                    Instant.ofEpochMilli(0L),
                    Duration.ZERO
                );

            // Send a record with null key (should be skipped)
            final Headers headers = makeHeaders("test", "header");
            inputTopic.pipeInput(new TestRecord<>(null, "value", headers));

            assertThat(
                appender.getEvents().stream()
                    .filter(e -> e.getLevel().equals("WARN"))
                    .map(Event::getMessage)
                    .collect(Collectors.toList()),
                hasItem("Skipping record due to null key. topic=[topic] partition=[0] offset=[0]")
            );
        }
    }

    /**
     * Tests that KTable logs warnings for out-of-order updates.
     * This behavior should be the same regardless of store format.
     */
    @ParameterizedTest
    @MethodSource("storeFormats")
    public void kTableShouldLogOnOutOfOrder(final String storeFormat) {
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic = "topic";
        builder.table(topic, stringConsumed, Materialized.as("store"));

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KTableSource.class);
            final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {

            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(
                    topic,
                    new StringSerializer(),
                    new StringSerializer(),
                    Instant.ofEpochMilli(0L),
                    Duration.ZERO
                );

            // Send records with headers - second record has earlier timestamp (out of order)
            final Headers headers = makeHeaders("test", "header");
            inputTopic.pipeInput(new TestRecord<>("key", "value", headers, 10L));
            inputTopic.pipeInput(new TestRecord<>("key", "value", headers, 5L));

            assertThat(
                appender.getEvents().stream()
                    .filter(e -> e.getLevel().equals("WARN"))
                    .map(Event::getMessage)
                    .collect(Collectors.toList()),
                hasItem("Detected out-of-order KTable update for store, old timestamp=[10] new timestamp=[5]. topic=[topic] partition=[0] offset=[1].")
            );
        }
    }

    /**
     * Tests KTable value getter functionality with both store formats.
     * Value getter returns ValueAndTimestamp regardless of whether headers are stored.
     * Additionally, verifies that headers are stored when using "headers" format.
     */
    @ParameterizedTest
    @MethodSource("storeFormats")
    public void testValueGetter(final String storeFormat) {
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        @SuppressWarnings("unchecked")
        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, stringConsumed, Materialized.as("store"));

        final Topology topology = builder.build();
        final KTableValueGetterSupplier<String, String> getterSupplier1 = table1.valueGetterSupplier();

        final InternalTopologyBuilder topologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);
        topologyBuilder.connectProcessorAndStateStores(table1.name, getterSupplier1.storeNames());

        try (final TopologyTestDriverWrapper driver = new TopologyTestDriverWrapper(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                driver.createInputTopic(
                    topic1,
                    new StringSerializer(),
                    new StringSerializer(),
                    Instant.ofEpochMilli(0L),
                    Duration.ZERO
                );
            final KTableValueGetter<String, String> getter1 = getterSupplier1.get();
            getter1.init(driver.setCurrentNodeForProcessorContext(table1.name));

            // Send records with unique headers for each key
            final Headers headersA = makeHeaders("key", "A");
            final Headers headersB = makeHeaders("key", "B");
            final Headers headersC = makeHeaders("key", "C");

            inputTopic1.pipeInput(new TestRecord<>("A", "01", headersA, 10L));
            inputTopic1.pipeInput(new TestRecord<>("B", "01", headersB, 20L));
            inputTopic1.pipeInput(new TestRecord<>("C", "01", headersC, 15L));

            // Verify value getter returns correct values and timestamps
            if (storeFormat.equals("headers")) {
                // With headers format, we can use getWithHeaders() to get value, timestamp, AND headers
                assertTrue(getter1.supportsHeaders(), "Getter should support headers with 'headers' format");

                final ValueTimestampHeaders<String> resultA = getter1.getWithHeaders("A");
                assertEquals("01", resultA.value());
                assertEquals(10L, resultA.timestamp());
                assertEquals("A", new String(resultA.headers().lastHeader("key").value()));

                final ValueTimestampHeaders<String> resultB = getter1.getWithHeaders("B");
                assertEquals("01", resultB.value());
                assertEquals(20L, resultB.timestamp());
                assertEquals("B", new String(resultB.headers().lastHeader("key").value()));

                final ValueTimestampHeaders<String> resultC = getter1.getWithHeaders("C");
                assertEquals("01", resultC.value());
                assertEquals(15L, resultC.timestamp());
                assertEquals("C", new String(resultC.headers().lastHeader("key").value()));
            } else {
                // With default format, getter only returns ValueAndTimestamp
                assertFalse(getter1.supportsHeaders(), "Getter should not support headers with 'default' format");
                assertEquals(ValueAndTimestamp.make("01", 10L), getter1.get("A"));
                assertEquals(ValueAndTimestamp.make("01", 20L), getter1.get("B"));
                assertEquals(ValueAndTimestamp.make("01", 15L), getter1.get("C"));
            }

            inputTopic1.pipeInput(new TestRecord<>("A", "02", headersA, 30L));
            inputTopic1.pipeInput(new TestRecord<>("B", "02", headersB, 5L));

            if (storeFormat.equals("headers")) {
                ValueTimestampHeaders<String> result = getter1.getWithHeaders("A");
                assertEquals("02", result.value());
                assertEquals(30L, result.timestamp());
                assertEquals("A", new String(result.headers().lastHeader("key").value()));

                result = getter1.getWithHeaders("B");
                assertEquals("02", result.value());
                assertEquals(5L, result.timestamp());
                assertEquals("B", new String(result.headers().lastHeader("key").value()));

                result = getter1.getWithHeaders("C");
                assertEquals("01", result.value());
                assertEquals(15L, result.timestamp());
                assertEquals("C", new String(result.headers().lastHeader("key").value()));
            } else {
                assertEquals(ValueAndTimestamp.make("02", 30L), getter1.get("A"));
                assertEquals(ValueAndTimestamp.make("02", 5L), getter1.get("B"));
                assertEquals(ValueAndTimestamp.make("01", 15L), getter1.get("C"));
            }

            inputTopic1.pipeInput(new TestRecord<>("A", "03", headersA, 29L));

            if (storeFormat.equals("headers")) {
                ValueTimestampHeaders<String> result = getter1.getWithHeaders("A");
                assertEquals("03", result.value());
                assertEquals(29L, result.timestamp());
                assertEquals("A", new String(result.headers().lastHeader("key").value()));

                result = getter1.getWithHeaders("B");
                assertEquals("02", result.value());
                assertEquals(5L, result.timestamp());
                assertEquals("B", new String(result.headers().lastHeader("key").value()));

                result = getter1.getWithHeaders("C");
                assertEquals("01", result.value());
                assertEquals(15L, result.timestamp());
                assertEquals("C", new String(result.headers().lastHeader("key").value()));
            } else {
                assertEquals(ValueAndTimestamp.make("03", 29L), getter1.get("A"));
                assertEquals(ValueAndTimestamp.make("02", 5L), getter1.get("B"));
                assertEquals(ValueAndTimestamp.make("01", 15L), getter1.get("C"));
            }

            inputTopic1.pipeInput(new TestRecord<>("A", null, headersA, 50L));
            inputTopic1.pipeInput(new TestRecord<>("B", null, headersB, 3L));

            if (storeFormat.equals("headers")) {
                assertNull(getter1.getWithHeaders("A"));
                assertNull(getter1.getWithHeaders("B"));
                final ValueTimestampHeaders<String> resultC2 = getter1.getWithHeaders("C");
                assertEquals("01", resultC2.value());
                assertEquals(15L, resultC2.timestamp());
                assertEquals("C", new String(resultC2.headers().lastHeader("key").value()));
            } else {
                assertNull(getter1.get("A"));
                assertNull(getter1.get("B"));
                assertEquals(ValueAndTimestamp.make("01", 15L), getter1.get("C"));
            }

            // Additionally verify headers are stored when using "headers" format
            if (storeFormat.equals("headers")) {
                final KeyValueStore<String, ValueTimestampHeaders<String>> store =
                    driver.getTimestampedKeyValueStoreWithHeaders("store");
                assertNotNull(store, "Headers-aware store should be created with 'headers' format");

                // Only C remains in the store (A and B were deleted)
                final ValueTimestampHeaders<String> resultC = store.get("C");
                assertNotNull(resultC);
                assertEquals("01", resultC.value());
                assertEquals(15L, resultC.timestamp());
                assertNotNull(resultC.headers());
                assertEquals(1, resultC.headers().toArray().length);
                assertEquals("C", new String(resultC.headers().lastHeader("key").value()));

                // A and B were deleted
                assertNull(store.get("A"));
                assertNull(store.get("B"));
            }
        }
    }

    /**
     * Tests that KTable doesn't send old values when not configured to do so.
     * This behavior should be the same regardless of store format.
     */
    @ParameterizedTest
    @MethodSource("storeFormats")
    public void testNotSendingOldValue(final String storeFormat) {
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        @SuppressWarnings("unchecked")
        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, stringConsumed);

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final Topology topology = builder.build().addProcessor("proc1", supplier, table1.name);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            final TestInputTopic<String, String> inputTopic1 =
                driver.createInputTopic(
                    topic1,
                    new StringSerializer(),
                    new StringSerializer(),
                    Instant.ofEpochMilli(0L),
                    Duration.ZERO
                );
            final MockApiProcessor<String, Integer, Void, Void> proc1 = supplier.theCapturedProcessor();

            final Headers headers = makeHeaders("test", "header");
            inputTopic1.pipeInput(new TestRecord<>("A", "01", headers, 10L));
            inputTopic1.pipeInput(new TestRecord<>("B", "01", headers, 20L));
            inputTopic1.pipeInput(new TestRecord<>("C", "01", headers, 15L));
            proc1.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>("01", null), 10),
                new KeyValueTimestamp<>("B", new Change<>("01", null), 20),
                new KeyValueTimestamp<>("C", new Change<>("01", null), 15)
            );

            inputTopic1.pipeInput(new TestRecord<>("A", "02", headers, 8L));
            inputTopic1.pipeInput(new TestRecord<>("B", "02", headers, 22L));
            proc1.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>("02", null), 8),
                new KeyValueTimestamp<>("B", new Change<>("02", null), 22)
            );

            inputTopic1.pipeInput(new TestRecord<>("A", "03", headers, 12L));
            proc1.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>("03", null), 12)
            );

            inputTopic1.pipeInput(new TestRecord<>("A", null, headers, 15L));
            inputTopic1.pipeInput(new TestRecord<>("B", null, headers, 20L));
            proc1.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>(null, null), 15),
                new KeyValueTimestamp<>("B", new Change<>(null, null), 20)
            );
        }
    }

    /**
     * Tests that KTable sends old values when configured to do so.
     * This behavior should be the same regardless of store format.
     */
    @ParameterizedTest
    @MethodSource("storeFormats")
    public void testSendingOldValue(final String storeFormat) {
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        @SuppressWarnings("unchecked")
        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, stringConsumed);
        table1.enableSendingOldValues(true);
        assertTrue(table1.sendingOldValueEnabled());

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final Topology topology = builder.build().addProcessor("proc1", supplier, table1.name);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            final TestInputTopic<String, String> inputTopic1 =
                driver.createInputTopic(
                    topic1,
                    new StringSerializer(),
                    new StringSerializer(),
                    Instant.ofEpochMilli(0L),
                    Duration.ZERO
                );
            final MockApiProcessor<String, Integer, Void, Void> proc1 = supplier.theCapturedProcessor();

            final Headers headers = makeHeaders("test", "header");
            inputTopic1.pipeInput(new TestRecord<>("A", "01", headers, 10L));
            inputTopic1.pipeInput(new TestRecord<>("B", "01", headers, 20L));
            inputTopic1.pipeInput(new TestRecord<>("C", "01", headers, 15L));
            proc1.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>("01", null), 10),
                new KeyValueTimestamp<>("B", new Change<>("01", null), 20),
                new KeyValueTimestamp<>("C", new Change<>("01", null), 15)
            );

            inputTopic1.pipeInput(new TestRecord<>("A", "02", headers, 8L));
            inputTopic1.pipeInput(new TestRecord<>("B", "02", headers, 22L));
            proc1.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>("02", "01"), 8),
                new KeyValueTimestamp<>("B", new Change<>("02", "01"), 22)
            );

            inputTopic1.pipeInput(new TestRecord<>("A", "03", headers, 12L));
            proc1.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>("03", "02"), 12)
            );

            inputTopic1.pipeInput(new TestRecord<>("A", null, headers, 15L));
            inputTopic1.pipeInput(new TestRecord<>("B", null, headers, 20L));
            proc1.checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", new Change<>(null, "03"), 15),
                new KeyValueTimestamp<>("B", new Change<>(null, "02"), 20)
            );
        }
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void testKTableAcceptsInputsWithHeaders(final String storeFormat) {
        final Properties props = getProps(storeFormat);
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, Integer> table1 = builder.table(topic1, Consumed.with(Serdes.String(), Serdes.Integer()));

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        table1.toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, Integer> inputTopic =
                    driver.createInputTopic(topic1, new StringSerializer(), new IntegerSerializer());

            final Headers headers1 = makeHeaders("key1", "value1");
            final Headers headers2 = makeHeaders("key2", "value2");

            // Send records with headers - both formats accept headers as input
            inputTopic.pipeInput(new TestRecord<>("A", 1, headers1, 10L));
            inputTopic.pipeInput(new TestRecord<>("B", 2, headers2, 11L));
        }

        // Verify records were processed successfully with both store formats
        assertEquals(2, supplier.theCapturedProcessor().processed().size());
    }

    /**
     * Verifies that when using "headers" format, headers are actually stored and can be retrieved.
     */
    @Test
    public void testHeadersAreStoredWithHeadersFormat() {
        final Properties props = getProps("headers");
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";
        final String storeName = "test-store";

        builder.table(topic1, stringConsumed, Materialized.as(storeName));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer());

            final Headers headers1 = makeHeaders("header-key-1", "header-value-1");
            final Headers headers2 = makeHeaders("header-key-2", "header-value-2");

            inputTopic.pipeInput(new TestRecord<>("A", "value1", headers1, 10L));
            inputTopic.pipeInput(new TestRecord<>("B", "value2", headers2, 20L));

            // Retrieve the headers-aware store
            final KeyValueStore<String, ValueTimestampHeaders<String>> store =
                driver.getTimestampedKeyValueStoreWithHeaders(storeName);

            assertNotNull(store, "Headers-aware store should be created with 'headers' format");

            // Verify first record: value, timestamp, and headers are all stored
            final ValueTimestampHeaders<String> resultA = store.get("A");
            assertNotNull(resultA);
            assertEquals("value1", resultA.value());
            assertEquals(10L, resultA.timestamp());
            assertEquals("header-value-1", new String(resultA.headers().lastHeader("header-key-1").value()));

            // Verify second record: value, timestamp, and headers are all stored
            final ValueTimestampHeaders<String> resultB = store.get("B");
            assertNotNull(resultB);
            assertEquals("value2", resultB.value());
            assertEquals(20L, resultB.timestamp());
            assertEquals("header-value-2", new String(resultB.headers().lastHeader("header-key-2").value()));
        }
    }

    /**
     * Verifies that when using "default" format, only value and timestamp are stored (not headers).
     */
    @Test
    public void testDefaultFormatUsesTimestampedStore() {
        final Properties props = getProps("default");
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";
        final String storeName = "test-store";

        builder.table(topic1, stringConsumed, Materialized.as(storeName));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer());

            final Headers headers1 = makeHeaders("header-key-1", "header-value-1");
            inputTopic.pipeInput(new TestRecord<>("A", "value1", headers1, 10L));

            // Default format uses standard timestamped store (not headers-aware)
            final KeyValueStore<String, ValueAndTimestamp<String>> store =
                driver.getTimestampedKeyValueStore(storeName);

            assertNotNull(store, "Timestamped store should be created with 'default' format");

            // Verify value and timestamp are stored
            final ValueAndTimestamp<String> resultA = store.get("A");
            assertNotNull(resultA);
            assertEquals("value1", resultA.value());
            assertEquals(10L, resultA.timestamp());
            // Note: Headers are accepted as input but not stored/retrievable with default format
        }
    }
}