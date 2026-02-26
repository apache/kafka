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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.KeyValueTimestampHeaders;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockApiProcessor;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.StoreFormatTestUtils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class KTableAggregateTest {
    private final Serde<String> stringSerde = Serdes.String();
    private final Consumed<String, String> consumed = Consumed.with(stringSerde, stringSerde);
    private final Grouped<String, String> stringSerialized = Grouped.with(stringSerde, stringSerde);

    /**
     * Provides both store format configurations for parameterized tests.
     */

    @ParameterizedTest
    @MethodSource("org.apache.kafka.test.StoreFormatTestUtils#storeFormats")
    public void testAggBasic(final String storeFormat) {
        final Properties props = StoreFormatTestUtils.getProps(storeFormat, Serdes.String(), Serdes.String());
        final MockApiProcessorSupplier<String, Object, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, String> table1 = builder.table(topic1, consumed);
        final KTable<String, String> table2 = table1
            .groupBy(
                MockMapper.noOpKeyValueMapper(),
                stringSerialized)
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                MockAggregator.TOSTRING_REMOVER,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("topic1-Canonized")
                    .withValueSerde(stringSerde));

        table2.toStream().process(supplier);

        try (
            final TopologyTestDriver driver = new TopologyTestDriver(
                builder.build(), props, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

            final Headers headersA = StoreFormatTestUtils.makeHeaders("key", "A");
            final Headers headersB = StoreFormatTestUtils.makeHeaders("key", "B");
            final Headers headersC = StoreFormatTestUtils.makeHeaders("key", "C");
            final Headers headersD = StoreFormatTestUtils.makeHeaders("key", "D");

            inputTopic.pipeInput(new TestRecord<>("A", "1", headersA, 10L));
            inputTopic.pipeInput(new TestRecord<>("B", "2", headersB, 15L));
            inputTopic.pipeInput(new TestRecord<>("A", "3", headersA, 20L));
            inputTopic.pipeInput(new TestRecord<>("B", "4", headersB, 18L));
            inputTopic.pipeInput(new TestRecord<>("C", "5", headersC, 5L));
            inputTopic.pipeInput(new TestRecord<>("D", "6", headersD, 25L));
            inputTopic.pipeInput(new TestRecord<>("B", "7", headersB, 15L));
            inputTopic.pipeInput(new TestRecord<>("C", "8", headersC, 10L));

            if (storeFormat.equals("default")) {
                assertEquals(
                    asList(
                        new KeyValueTimestamp<>("A", "0+1", 10L),
                        new KeyValueTimestamp<>("B", "0+2", 15L),
                        new KeyValueTimestamp<>("A", "0+1-1+3", 20L),
                        new KeyValueTimestamp<>("B", "0+2-2+4", 18L),
                        new KeyValueTimestamp<>("C", "0+5", 5L),
                        new KeyValueTimestamp<>("D", "0+6", 25L),
                        new KeyValueTimestamp<>("B", "0+2-2+4-4+7", 18L),
                        new KeyValueTimestamp<>("C", "0+5-5+8", 10L)),
                    supplier.theCapturedProcessor().processed());
            } else if (storeFormat.equals("headers")) {
                assertEquals(
                    asList(
                        new KeyValueTimestampHeaders<>("A", "0+1", 10L, headersA),
                        new KeyValueTimestampHeaders<>("B", "0+2", 15L, headersB),
                        new KeyValueTimestampHeaders<>("A", "0+1-1+3", 20L, headersA),
                        new KeyValueTimestampHeaders<>("B", "0+2-2+4", 18L, headersB),
                        new KeyValueTimestampHeaders<>("C", "0+5", 5L, headersC),
                        new KeyValueTimestampHeaders<>("D", "0+6", 25L, headersD),
                        new KeyValueTimestampHeaders<>("B", "0+2-2+4-4+7", 18L, headersB),
                        new KeyValueTimestampHeaders<>("C", "0+5-5+8", 10L, headersC)),
                    supplier.theCapturedProcessor().processedWithHeaders());
            }
        }
    }

    @ParameterizedTest
    @MethodSource("org.apache.kafka.test.StoreFormatTestUtils#storeFormats")
    public void testAggRepartition(final String storeFormat) {
        final Properties props = StoreFormatTestUtils.getProps(storeFormat, Serdes.String(), Serdes.String());
        final MockApiProcessorSupplier<String, Object, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, String> table1 = builder.table(topic1, consumed);
        final KTable<String, String> table2 = table1
            .groupBy(
                (key, value) -> {
                    switch (key) {
                        case "null":
                            return KeyValue.pair(null, value);
                        case "NULL":
                            return null;
                        default:
                            return KeyValue.pair(value, value);
                    }
                },
                stringSerialized)
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                MockAggregator.TOSTRING_REMOVER,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("topic1-Canonized")
                    .withValueSerde(stringSerde));

        table2.toStream().process(supplier);

        try (
            final TopologyTestDriver driver = new TopologyTestDriver(
                builder.build(), props, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

            final Headers headersA = StoreFormatTestUtils.makeHeaders("key", "A");
            final Headers headersB = StoreFormatTestUtils.makeHeaders("key", "B");
            final Headers headersNull = StoreFormatTestUtils.makeHeaders("key", "null");
            final Headers headersNULL = StoreFormatTestUtils.makeHeaders("key", "NULL");

            inputTopic.pipeInput(new TestRecord<>("A", "1", headersA, 10L));
            inputTopic.pipeInput(new TestRecord<>("A", null, headersA, 15L));
            inputTopic.pipeInput(new TestRecord<>("A", "1", headersA, 12L));
            inputTopic.pipeInput(new TestRecord<>("B", "2", headersB, 20L));
            inputTopic.pipeInput(new TestRecord<>("null", "3", headersNull, 25L));
            inputTopic.pipeInput(new TestRecord<>("B", "4", headersB, 23L));
            inputTopic.pipeInput(new TestRecord<>("NULL", "5", headersNULL, 24L));
            inputTopic.pipeInput(new TestRecord<>("B", "7", headersB, 22L));

            if (storeFormat.equals("default")) {
                assertEquals(
                    asList(
                        new KeyValueTimestamp<>("1", "0+1", 10),
                        new KeyValueTimestamp<>("1", "0+1-1", 15),
                        new KeyValueTimestamp<>("1", "0+1-1+1", 15),
                        new KeyValueTimestamp<>("2", "0+2", 20),
                        new KeyValueTimestamp<>("2", "0+2-2", 23),
                        new KeyValueTimestamp<>("4", "0+4", 23),
                        new KeyValueTimestamp<>("4", "0+4-4", 23),
                        new KeyValueTimestamp<>("7", "0+7", 22)),
                    supplier.theCapturedProcessor().processed());
            } else if (storeFormat.equals("headers")) {
                assertEquals(
                    asList(
                        new KeyValueTimestampHeaders<>("1", "0+1", 10, headersA),
                        new KeyValueTimestampHeaders<>("1", "0+1-1", 15, headersA),
                        new KeyValueTimestampHeaders<>("1", "0+1-1+1", 15, headersA),
                        new KeyValueTimestampHeaders<>("2", "0+2", 20, headersB),
                        new KeyValueTimestampHeaders<>("2", "0+2-2", 23, headersB),
                        new KeyValueTimestampHeaders<>("4", "0+4", 23, headersB),
                        new KeyValueTimestampHeaders<>("4", "0+4-4", 23, headersB),
                        new KeyValueTimestampHeaders<>("7", "0+7", 22, headersB)),
                    supplier.theCapturedProcessor().processedWithHeaders());
            }
        }
    }

    /**
     * The versioned store (table1) remains unchanged across both modes - it always uses VersionedKeyValueStore.
     * Only the aggregate store (table2) changes based on the DSL_STORE_FORMAT_CONFIG setting.
     */
    @ParameterizedTest
    @MethodSource("org.apache.kafka.test.StoreFormatTestUtils#storeFormats")
    public void testAggOfVersionedStore(final String storeFormat) {
        final Properties props = StoreFormatTestUtils.getProps(storeFormat, Serdes.String(), Serdes.String());
        final MockApiProcessorSupplier<String, Object, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5)));
        final KTable<String, String> table1 = builder.table(topic1, consumed, versionedMaterialize);
        final KTable<String, String> table2 = table1
            .groupBy(
                (key, value) -> {
                    switch (key) {
                        case "null":
                            return KeyValue.pair(null, value);
                        case "NULL":
                            return null;
                        default:
                            return KeyValue.pair(value, value);
                    }
                },
                stringSerialized)
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                MockAggregator.TOSTRING_REMOVER,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("topic1-Canonized")
                    .withValueSerde(stringSerde));

        table2.toStream().process(supplier);

        try (
            final TopologyTestDriver driver = new TopologyTestDriver(
                builder.build(), props, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

            final Headers headersA = StoreFormatTestUtils.makeHeaders("key", "A");
            final Headers headersB = StoreFormatTestUtils.makeHeaders("key", "B");
            final Headers headersNull = StoreFormatTestUtils.makeHeaders("key", "null");
            final Headers headersNULL = StoreFormatTestUtils.makeHeaders("key", "NULL");

            inputTopic.pipeInput(new TestRecord<>("A", "1", headersA, 10L));
            inputTopic.pipeInput(new TestRecord<>("A", null, headersA, 15L));
            inputTopic.pipeInput(new TestRecord<>("A", "1", headersA, 12L)); // out-of-order record will be ignored
            inputTopic.pipeInput(new TestRecord<>("B", "2", headersB, 20L));
            inputTopic.pipeInput(new TestRecord<>("null", "3", headersNull, 25L));
            inputTopic.pipeInput(new TestRecord<>("B", "4", headersB, 23L));
            inputTopic.pipeInput(new TestRecord<>("NULL", "5", headersNULL, 24L));
            inputTopic.pipeInput(new TestRecord<>("B", "7", headersB, 22L)); // out-of-order record will be ignored

            if (storeFormat.equals("default")) {
                assertEquals(
                    asList(
                        new KeyValueTimestamp<>("1", "0+1", 10),
                        new KeyValueTimestamp<>("1", "0+1-1", 15),
                        new KeyValueTimestamp<>("2", "0+2", 20),
                        new KeyValueTimestamp<>("2", "0+2-2", 23),
                        new KeyValueTimestamp<>("4", "0+4", 23)),
                    supplier.theCapturedProcessor().processed());
            } else if (storeFormat.equals("headers")) {
                assertEquals(
                    asList(
                        new KeyValueTimestampHeaders<>("1", "0+1", 10, headersA),
                        new KeyValueTimestampHeaders<>("1", "0+1-1", 15, headersA),
                        new KeyValueTimestampHeaders<>("2", "0+2", 20, headersB),
                        new KeyValueTimestampHeaders<>("2", "0+2-2", 23, headersB),
                        new KeyValueTimestampHeaders<>("4", "0+4", 23, headersB)),
                    supplier.theCapturedProcessor().processedWithHeaders());
            }
        }
    }

    private void testCountHelper(final String storeFormat,
                                  final Properties props,
                                  final boolean useMaterialized) {
        final MockApiProcessorSupplier<String, Object, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "count-test-input";

        if (useMaterialized) {
            builder
                .table(input, consumed)
                .groupBy(MockMapper.selectValueKeyValueMapper(), stringSerialized)
                .count(Materialized.as("count"))
                .toStream()
                .process(supplier);
        } else {
            builder
                .table(input, consumed)
                .groupBy(MockMapper.selectValueKeyValueMapper(), stringSerialized)
                .count()
                .toStream()
                .process(supplier);
        }
        try (
            final TopologyTestDriver driver = new TopologyTestDriver(
                builder.build(), props, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(input, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

            final Headers headersA = StoreFormatTestUtils.makeHeaders("key", "A");
            final Headers headersB = StoreFormatTestUtils.makeHeaders("key", "B");
            final Headers headersC = StoreFormatTestUtils.makeHeaders("key", "C");
            final Headers headersD = StoreFormatTestUtils.makeHeaders("key", "D");

            inputTopic.pipeInput(new TestRecord<>("A", "green", headersA, 10L));
            inputTopic.pipeInput(new TestRecord<>("B", "green", headersB, 9L));
            inputTopic.pipeInput(new TestRecord<>("A", "blue", headersA, 12L));
            inputTopic.pipeInput(new TestRecord<>("C", "yellow", headersC, 15L));
            inputTopic.pipeInput(new TestRecord<>("D", "green", headersD, 11L));

            if (storeFormat.equals("default")) {
                assertEquals(
                    asList(
                        new KeyValueTimestamp<>("green", 1L, 10),
                        new KeyValueTimestamp<>("green", 2L, 10),
                        new KeyValueTimestamp<>("green", 1L, 12),
                        new KeyValueTimestamp<>("blue", 1L, 12),
                        new KeyValueTimestamp<>("yellow", 1L, 15),
                        new KeyValueTimestamp<>("green", 2L, 12)),
                    supplier.theCapturedProcessor().processed());
            } else if (storeFormat.equals("headers")) {
                assertEquals(
                    asList(
                        new KeyValueTimestampHeaders<>("green", 1L, 10, headersA),
                        new KeyValueTimestampHeaders<>("green", 2L, 10, headersB),
                        new KeyValueTimestampHeaders<>("green", 1L, 12, headersA),
                        new KeyValueTimestampHeaders<>("blue", 1L, 12, headersA),
                        new KeyValueTimestampHeaders<>("yellow", 1L, 15, headersC),
                        new KeyValueTimestampHeaders<>("green", 2L, 12, headersD)),
                    supplier.theCapturedProcessor().processedWithHeaders());
            }
        }
    }


    @ParameterizedTest
    @MethodSource("org.apache.kafka.test.StoreFormatTestUtils#storeFormats")
    public void testCount(final String storeFormat) {
        final Properties props = StoreFormatTestUtils.getProps(storeFormat, Serdes.String(), Serdes.String());
        testCountHelper(storeFormat, props, true);
    }

    @ParameterizedTest
    @MethodSource("org.apache.kafka.test.StoreFormatTestUtils#storeFormats")
    public void testCountWithInternalStore(final String storeFormat) {
        final Properties props = StoreFormatTestUtils.getProps(storeFormat, Serdes.String(), Serdes.String());
        testCountHelper(storeFormat, props, false);
    }

    /** Source table: VersionedKeyValueStore (explicitly materialized)
    * Aggregate store: Created by .count() - will respect DSL_STORE_FORMAT_CONFIG
    */
    @ParameterizedTest
    @MethodSource("org.apache.kafka.test.StoreFormatTestUtils#storeFormats")
    public void testCountOfVersionedStore(final String storeFormat) {
        final Properties props = StoreFormatTestUtils.getProps(storeFormat, Serdes.String(), Serdes.String());
        final MockApiProcessorSupplier<String, Object, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "count-test-input";

        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> versionedMaterialize =
            Materialized.as(Stores.persistentVersionedKeyValueStore("versioned", Duration.ofMinutes(5)));
        builder
            .table(input, consumed, versionedMaterialize)
            .groupBy(MockMapper.selectValueKeyValueMapper(), stringSerialized)
            .count()
            .toStream()
            .process(supplier);

        try (
            final TopologyTestDriver driver = new TopologyTestDriver(
                builder.build(), props, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(input, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

            final Headers headersA = StoreFormatTestUtils.makeHeaders("key", "A");
            final Headers headersB = StoreFormatTestUtils.makeHeaders("key", "B");
            final Headers headersC = StoreFormatTestUtils.makeHeaders("key", "C");
            final Headers headersD = StoreFormatTestUtils.makeHeaders("key", "D");

            inputTopic.pipeInput(new TestRecord<>("A", "green", headersA, 10L));
            inputTopic.pipeInput(new TestRecord<>("B", "green", headersB, 9L));
            inputTopic.pipeInput(new TestRecord<>("A", "blue", headersA, 12L));
            inputTopic.pipeInput(new TestRecord<>("A", "blue", headersA, 11L)); // out-of-order record will be ignored
            inputTopic.pipeInput(new TestRecord<>("C", "yellow", headersC, 15L));
            inputTopic.pipeInput(new TestRecord<>("D", "green", headersD, 11L));

            if (storeFormat.equals("default")) {
                assertEquals(
                    asList(
                        new KeyValueTimestamp<>("green", 1L, 10),
                        new KeyValueTimestamp<>("green", 2L, 10),
                        new KeyValueTimestamp<>("green", 1L, 12),
                        new KeyValueTimestamp<>("blue", 1L, 12),
                        new KeyValueTimestamp<>("yellow", 1L, 15),
                        new KeyValueTimestamp<>("green", 2L, 12)),
                    supplier.theCapturedProcessor().processed());
            } else if (storeFormat.equals("headers")) {
                assertEquals(
                    asList(
                        new KeyValueTimestampHeaders<>("green", 1L, 10, headersA),
                        new KeyValueTimestampHeaders<>("green", 2L, 10, headersB),
                        new KeyValueTimestampHeaders<>("green", 1L, 12, headersA),
                        new KeyValueTimestampHeaders<>("blue", 1L, 12, headersA),
                        new KeyValueTimestampHeaders<>("yellow", 1L, 15, headersC),
                        new KeyValueTimestampHeaders<>("green", 2L, 12, headersD)),
                    supplier.theCapturedProcessor().processedWithHeaders());
            }
        }
    }

    @ParameterizedTest
    @MethodSource("org.apache.kafka.test.StoreFormatTestUtils#storeFormats")
    public void testRemoveOldBeforeAddNew(final String storeFormat) {
        final Properties props = StoreFormatTestUtils.getProps(storeFormat, Serdes.String(), Serdes.String());
        final MockApiProcessorSupplier<String, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "count-test-input";

        builder
            .table(input, consumed)
            .groupBy(
                (key, value) -> KeyValue.pair(
                    String.valueOf(key.charAt(0)),
                    String.valueOf(key.charAt(1))),
                stringSerialized)
            .aggregate(
                () -> "",
                (aggKey, value, aggregate) -> aggregate + value,
                (key, value, aggregate) -> aggregate.replaceAll(value, ""),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("someStore")
                    .withValueSerde(Serdes.String()))
            .toStream()
            .process(supplier);

        try (
            final TopologyTestDriver driver = new TopologyTestDriver(
                builder.build(), props, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(input, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

            final MockApiProcessor<String, String, Void, Void> proc = supplier.theCapturedProcessor();

            final Headers headers11 = StoreFormatTestUtils.makeHeaders("key", "11");
            final Headers headers12 = StoreFormatTestUtils.makeHeaders("key", "12");

            inputTopic.pipeInput(new TestRecord<>("11", "A", headers11, 10L));
            inputTopic.pipeInput(new TestRecord<>("12", "B", headers12, 8L));
            inputTopic.pipeInput(new TestRecord<>("11", null, headers11, 12L));
            inputTopic.pipeInput(new TestRecord<>("12", "C", headers12, 6L));

            if (storeFormat.equals("default")) {
                assertEquals(
                    asList(
                        new KeyValueTimestamp<>("1", "1", 10),
                        new KeyValueTimestamp<>("1", "12", 10),
                        new KeyValueTimestamp<>("1", "2", 12),
                        new KeyValueTimestamp<>("1", "2", 12L)
                    ),
                    proc.processed()
                );
            } else if (storeFormat.equals("headers")) {
                assertEquals(
                    asList(
                        new KeyValueTimestampHeaders<>("1", "1", 10, headers11),
                        new KeyValueTimestampHeaders<>("1", "12", 10, headers12),
                        new KeyValueTimestampHeaders<>("1", "2", 12, headers11),
                        new KeyValueTimestampHeaders<>("1", "2", 12L, headers12)
                    ),
                    proc.processedWithHeaders()
                );
            }
        }
    }

    private void testUpgradeFromConfig(final Properties config,
                                       final List<KeyValueTimestamp<String, Long>> expectedDefault,
                                       final List<KeyValueTimestampHeaders<String, Long>> expectedHeaders,
                                       final String storeFormat) {
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "input-topic";
        final String output = "output-topic";
        final Serde<String> stringSerde = Serdes.String();

        builder
                .table(input, Consumed.with(stringSerde, stringSerde))
                // key is not changed
                .groupBy(KeyValue::pair, Grouped.with(stringSerde, stringSerde))
                .count()
                .toStream()
                .to(output);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic(input, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestOutputTopic<String, Long> outputTopic =
                    driver.createOutputTopic(output, new StringDeserializer(), new LongDeserializer());

            final Headers headers1 = StoreFormatTestUtils.makeHeaders("key", "1");

            inputTopic.pipeInput(new TestRecord<>("1", "", headers1, 8L));
            inputTopic.pipeInput(new TestRecord<>("1", "", headers1, 9L));

            final List<KeyValueTimestamp<String, Long>> actualDefault = new ArrayList<>();
            final List<KeyValueTimestampHeaders<String, Long>> actualHeaders = new ArrayList<>();

            outputTopic.readRecordsToList().forEach(tr -> {
                actualDefault.add(new KeyValueTimestamp<>(tr.key(), tr.value(), tr.timestamp()));
                actualHeaders.add(new KeyValueTimestampHeaders<>(tr.key(), tr.value(), tr.timestamp(), tr.headers()));
            });

            if (storeFormat.equals("default")) {
                assertEquals(expectedDefault, actualDefault);
            } else if (storeFormat.equals("headers")) {
                assertEquals(expectedHeaders, actualHeaders);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("org.apache.kafka.test.StoreFormatTestUtils#storeFormats")
    public void testShouldSendTransientStateWhenUpgrading(final String storeFormat) {
        final Properties upgradingConfig = StoreFormatTestUtils.getProps(storeFormat, Serdes.String(), Serdes.String());
        upgradingConfig.put(StreamsConfig.UPGRADE_FROM_CONFIG, StreamsConfig.UPGRADE_FROM_33);

        final Headers headers1 = StoreFormatTestUtils.makeHeaders("key", "1");

        testUpgradeFromConfig(
            upgradingConfig,
            asList(
                new KeyValueTimestamp<>("1", 1L, 8),
                new KeyValueTimestamp<>("1", 0L, 9), // transient inconsistent state
                new KeyValueTimestamp<>("1", 1L, 9)
            ),
            asList(
                new KeyValueTimestampHeaders<>("1", 1L, 8, headers1),
                new KeyValueTimestampHeaders<>("1", 0L, 9, headers1), // transient inconsistent state
                new KeyValueTimestampHeaders<>("1", 1L, 9, headers1)
            ),
            storeFormat
        );
    }

    @ParameterizedTest
    @MethodSource("org.apache.kafka.test.StoreFormatTestUtils#storeFormats")
    public void testShouldNotSendTransientStateIfNotUpgrading(final String storeFormat) {
        final Properties props = StoreFormatTestUtils.getProps(storeFormat, Serdes.String(), Serdes.String());

        final Headers headers1 = StoreFormatTestUtils.makeHeaders("key", "1");

        testUpgradeFromConfig(
            props,
            asList(
                new KeyValueTimestamp<>("1", 1L, 8),
                new KeyValueTimestamp<>("1", 1L, 9)
            ),
            asList(
                new KeyValueTimestampHeaders<>("1", 1L, 8, headers1),
                new KeyValueTimestampHeaders<>("1", 1L, 9, headers1)
            ),
            storeFormat
        );
    }

    private static class NoEqualsImpl {
        private final String x;

        public NoEqualsImpl(final String x) {
            this.x = x;
        }

        public String getX() {
            return x;
        }
    }

    private static class NoEqualsImplSerde implements Serde<NoEqualsImpl> {
        @Override
        public Serializer<NoEqualsImpl> serializer() {
            return (topic, data) -> data == null ? null : data.x.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public Deserializer<NoEqualsImpl> deserializer() {
            return (topic, data) -> data == null ? null : new NoEqualsImpl(new String(data, StandardCharsets.UTF_8));
        }
    }

    // `NoEqualsImpl` doesn't implement `equals` but we can still compare two `NoEqualsImpl` instances by comparing their underlying `x` field
    private List<TestRecord<String, Long>> toComparableList(final List<TestRecord<NoEqualsImpl, Long>> list) {
        final List<TestRecord<String, Long>> comparableList = new ArrayList<>();
        list.forEach(tr -> comparableList.add(new TestRecord<>(tr.key().getX(), tr.value(), Instant.ofEpochMilli(tr.timestamp()))));
        return comparableList;
    }

    private void testKeyWithNoEquals(
            final KeyValueMapper<NoEqualsImpl, NoEqualsImpl, KeyValue<NoEqualsImpl, NoEqualsImpl>> keyValueMapper,
            final List<TestRecord<NoEqualsImpl, Long>> expected,
            final String storeFormat) {
        final Properties props = StoreFormatTestUtils.getProps(storeFormat, Serdes.String(), Serdes.String());
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "input-topic";
        final String output = "output-topic";
        final Serde<NoEqualsImpl> noEqualsImplSerde = new NoEqualsImplSerde();

        builder
                .table(input, Consumed.with(noEqualsImplSerde, noEqualsImplSerde))
                .groupBy(keyValueMapper, Grouped.with(noEqualsImplSerde, noEqualsImplSerde))
                .count()
                .toStream()
                .to(output);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<NoEqualsImpl, NoEqualsImpl> inputTopic =
                    driver.createInputTopic(input, noEqualsImplSerde.serializer(), noEqualsImplSerde.serializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestOutputTopic<NoEqualsImpl, Long> outputTopic =
                    driver.createOutputTopic(output, noEqualsImplSerde.deserializer(), new LongDeserializer());

            final NoEqualsImpl a = new NoEqualsImpl("1");
            final NoEqualsImpl b = new NoEqualsImpl("1");
            assertNotEquals(a, b);
            assertNotSame(a, b);

            final Headers headersA = StoreFormatTestUtils.makeHeaders("key", "A");
            final Headers headersB = StoreFormatTestUtils.makeHeaders("key", "B");

            inputTopic.pipeInput(new TestRecord<>(a, a, headersA, 8L));
            inputTopic.pipeInput(new TestRecord<>(b, b, headersB, 9L));

            final List<TestRecord<String, Long>> actualComparable = toComparableList(outputTopic.readRecordsToList());
            final List<TestRecord<String, Long>> expectedComparable = toComparableList(expected);
            assertEquals(expectedComparable, actualComparable);
        }
    }

    @ParameterizedTest
    @MethodSource("org.apache.kafka.test.StoreFormatTestUtils#storeFormats")
    public void testNoEqualsAndNotSameObject(final String storeFormat) {
        final Headers headersA = StoreFormatTestUtils.makeHeaders("key", "A");
        final Headers headersB = StoreFormatTestUtils.makeHeaders("key", "B");

        testKeyWithNoEquals(
                // key changes, different object reference (deserializer returns a new object reference)
                (k, v) -> new KeyValue<>(v, v),
                asList(
                        new TestRecord<>(new NoEqualsImpl("1"), 1L, headersA, Instant.ofEpochMilli(8)),
                        new TestRecord<>(new NoEqualsImpl("1"), 0L, headersB, Instant.ofEpochMilli(9)), // transient inconsistent state
                        new TestRecord<>(new NoEqualsImpl("1"), 1L, headersB, Instant.ofEpochMilli(9))
                ),
                storeFormat
        );
    }

    @ParameterizedTest
    @MethodSource("org.apache.kafka.test.StoreFormatTestUtils#storeFormats")
    public void testNoEqualsAndSameObject(final String storeFormat) {
        final Headers headersA = StoreFormatTestUtils.makeHeaders("key", "A");
        final Headers headersB = StoreFormatTestUtils.makeHeaders("key", "B");

        testKeyWithNoEquals(
                // key does not change, same object reference
                KeyValue::new,
                asList(
                        new TestRecord<>(new NoEqualsImpl("1"), 1L, headersA, Instant.ofEpochMilli(8)),
                        new TestRecord<>(new NoEqualsImpl("1"), 1L, headersB, Instant.ofEpochMilli(9))
                ),
                storeFormat
        );
    }
}
