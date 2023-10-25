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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockApiProcessor;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Properties;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.junit.Assert.assertEquals;

public class KTableAggregateTest {
    private final Serde<String> stringSerde = Serdes.String();
    private final Consumed<String, String> consumed = Consumed.with(stringSerde, stringSerde);
    private final Grouped<String, String> stringSerialized = Grouped.with(stringSerde, stringSerde);
    private final MockApiProcessorSupplier<String, Object, Void, Void> supplier = new MockApiProcessorSupplier<>();
    private final static Properties CONFIG = mkProperties(mkMap(
        mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory("kafka-test").getAbsolutePath())));

    @Test
    public void testAggBasic() {
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
                builder.build(), CONFIG, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

            inputTopic.pipeInput("A", "1", 10L);
            inputTopic.pipeInput("B", "2", 15L);
            inputTopic.pipeInput("A", "3", 20L);
            inputTopic.pipeInput("B", "4", 18L);
            inputTopic.pipeInput("C", "5", 5L);
            inputTopic.pipeInput("D", "6", 25L);
            inputTopic.pipeInput("B", "7", 15L);
            inputTopic.pipeInput("C", "8", 10L);

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
        }
    }

    @Test
    public void testAggRepartition() {
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
                builder.build(), CONFIG, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

            inputTopic.pipeInput("A", "1", 10L);
            inputTopic.pipeInput("A", (String) null, 15L);
            inputTopic.pipeInput("A", "1", 12L);
            inputTopic.pipeInput("B", "2", 20L);
            inputTopic.pipeInput("null", "3", 25L);
            inputTopic.pipeInput("B", "4", 23L);
            inputTopic.pipeInput("NULL", "5", 24L);
            inputTopic.pipeInput("B", "7", 22L);

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
        }
    }

    @Test
    public void testAggOfVersionedStore() {
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
                builder.build(), CONFIG, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

            inputTopic.pipeInput("A", "1", 10L);
            inputTopic.pipeInput("A", (String) null, 15L);
            inputTopic.pipeInput("A", "1", 12L); // out-of-order record will be ignored
            inputTopic.pipeInput("B", "2", 20L);
            inputTopic.pipeInput("null", "3", 25L);
            inputTopic.pipeInput("B", "4", 23L);
            inputTopic.pipeInput("NULL", "5", 24L);
            inputTopic.pipeInput("B", "7", 22L); // out-of-order record will be ignored

            assertEquals(
                asList(
                    new KeyValueTimestamp<>("1", "0+1", 10),
                    new KeyValueTimestamp<>("1", "0+1-1", 15),
                    new KeyValueTimestamp<>("2", "0+2", 20),
                    new KeyValueTimestamp<>("2", "0+2-2", 23),
                    new KeyValueTimestamp<>("4", "0+4", 23)),
                supplier.theCapturedProcessor().processed());
        }
    }

    private static void testCountHelper(final StreamsBuilder builder,
                                        final String input,
                                        final MockApiProcessorSupplier<String, Object, Void, Void> supplier) {
        try (
            final TopologyTestDriver driver = new TopologyTestDriver(
                builder.build(), CONFIG, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(input, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

            inputTopic.pipeInput("A", "green", 10L);
            inputTopic.pipeInput("B", "green", 9L);
            inputTopic.pipeInput("A", "blue", 12L);
            inputTopic.pipeInput("C", "yellow", 15L);
            inputTopic.pipeInput("D", "green", 11L);

            assertEquals(
                asList(
                    new KeyValueTimestamp<>("green", 1L, 10),
                    new KeyValueTimestamp<>("green", 2L, 10),
                    new KeyValueTimestamp<>("green", 1L, 12),
                    new KeyValueTimestamp<>("blue", 1L, 12),
                    new KeyValueTimestamp<>("yellow", 1L, 15),
                    new KeyValueTimestamp<>("green", 2L, 12)),
                supplier.theCapturedProcessor().processed());
        }
    }


    @Test
    public void testCount() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "count-test-input";

        builder
            .table(input, consumed)
            .groupBy(MockMapper.selectValueKeyValueMapper(), stringSerialized)
            .count(Materialized.as("count"))
            .toStream()
            .process(supplier);

        testCountHelper(builder, input, supplier);
    }

    @Test
    public void testCountWithInternalStore() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "count-test-input";

        builder
            .table(input, consumed)
            .groupBy(MockMapper.selectValueKeyValueMapper(), stringSerialized)
            .count()
            .toStream()
            .process(supplier);

        testCountHelper(builder, input, supplier);
    }

    @Test
    public void testCountOfVersionedStore() {
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
                builder.build(), CONFIG, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(input, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

            inputTopic.pipeInput("A", "green", 10L);
            inputTopic.pipeInput("B", "green", 9L);
            inputTopic.pipeInput("A", "blue", 12L);
            inputTopic.pipeInput("A", "blue", 11L); // out-of-order record will be ignored
            inputTopic.pipeInput("C", "yellow", 15L);
            inputTopic.pipeInput("D", "green", 11L);

            assertEquals(
                asList(
                    new KeyValueTimestamp<>("green", 1L, 10),
                    new KeyValueTimestamp<>("green", 2L, 10),
                    new KeyValueTimestamp<>("green", 1L, 12),
                    new KeyValueTimestamp<>("blue", 1L, 12),
                    new KeyValueTimestamp<>("yellow", 1L, 15),
                    new KeyValueTimestamp<>("green", 2L, 12)),
                supplier.theCapturedProcessor().processed());
        }
    }

    @Test
    public void testRemoveOldBeforeAddNew() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "count-test-input";
        final MockApiProcessorSupplier<String, String, Void, Void> supplier = new MockApiProcessorSupplier<>();

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
                builder.build(), CONFIG, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(input, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

            final MockApiProcessor<String, String, Void, Void> proc = supplier.theCapturedProcessor();

            inputTopic.pipeInput("11", "A", 10L);
            inputTopic.pipeInput("12", "B", 8L);
            inputTopic.pipeInput("11", (String) null, 12L);
            inputTopic.pipeInput("12", "C", 6L);

            assertEquals(
                asList(
                    new KeyValueTimestamp<>("1", "1", 10),
                    new KeyValueTimestamp<>("1", "12", 10),
                    new KeyValueTimestamp<>("1", "2", 12),
                    new KeyValueTimestamp<>("1", "2", 12L)
                ),
                proc.processed()
            );
        }
    }

    private void testUpgradeFromConfig(final Properties config, final List<KeyValueTimestamp<String, Long>> expected) {
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

            inputTopic.pipeInput("1", "", 8L);
            inputTopic.pipeInput("1", "", 9L);

            final List<KeyValueTimestamp<String, Long>> actual = new ArrayList<>();
            outputTopic.readRecordsToList().forEach(tr -> actual.add(new KeyValueTimestamp<>(tr.key(), tr.value(), tr.timestamp())));

            assertEquals(expected, actual);
        }
    }

    @Test
    public void testShouldSendTransientStateWhenUpgrading() {
        final Properties upgradingConfig = new Properties();
        upgradingConfig.putAll(CONFIG);
        upgradingConfig.put(StreamsConfig.UPGRADE_FROM_CONFIG, StreamsConfig.UPGRADE_FROM_33);
        testUpgradeFromConfig(upgradingConfig, asList(
                new KeyValueTimestamp<>("1", 1L, 8),
                new KeyValueTimestamp<>("1", 0L, 9), // transient inconsistent state
                new KeyValueTimestamp<>("1", 1L, 9)
        ));
    }

    @Test
    public void testShouldNotSendTransientStateIfNotUpgrading() {
        testUpgradeFromConfig(CONFIG, asList(
                new KeyValueTimestamp<>("1", 1L, 8),
                new KeyValueTimestamp<>("1", 1L, 9)
        ));
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
            final List<TestRecord<NoEqualsImpl, Long>> expected) {
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

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), CONFIG, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<NoEqualsImpl, NoEqualsImpl> inputTopic =
                    driver.createInputTopic(input, noEqualsImplSerde.serializer(), noEqualsImplSerde.serializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestOutputTopic<NoEqualsImpl, Long> outputTopic =
                    driver.createOutputTopic(output, noEqualsImplSerde.deserializer(), new LongDeserializer());

            final NoEqualsImpl a = new NoEqualsImpl("1");
            final NoEqualsImpl b = new NoEqualsImpl("1");
            Assert.assertNotEquals(a, b);
            Assert.assertNotSame(a, b);

            inputTopic.pipeInput(a, a, 8);
            inputTopic.pipeInput(b, b, 9);

            final List<TestRecord<String, Long>> actualComparable = toComparableList(outputTopic.readRecordsToList());
            final List<TestRecord<String, Long>> expectedComparable = toComparableList(expected);
            assertEquals(expectedComparable, actualComparable);
        }
    }

    @Test
    public void testNoEqualsAndNotSameObject() {
        testKeyWithNoEquals(
                // key changes, different object reference (deserializer returns a new object reference)
                (k, v) -> new KeyValue<>(v, v),
                asList(
                        new TestRecord<>(new NoEqualsImpl("1"), 1L, Instant.ofEpochMilli(8)),
                        new TestRecord<>(new NoEqualsImpl("1"), 0L, Instant.ofEpochMilli(9)), // transient inconsistent state
                        new TestRecord<>(new NoEqualsImpl("1"), 1L, Instant.ofEpochMilli(9))
                )
        );
    }

    @Test
    public void testNoEqualsAndSameObject() {
        testKeyWithNoEquals(
                // key does not change, same object reference
                KeyValue::new,
                asList(
                        new TestRecord<>(new NoEqualsImpl("1"), 1L, Instant.ofEpochMilli(8)),
                        new TestRecord<>(new NoEqualsImpl("1"), 1L, Instant.ofEpochMilli(9))
                )
        );
    }
}
