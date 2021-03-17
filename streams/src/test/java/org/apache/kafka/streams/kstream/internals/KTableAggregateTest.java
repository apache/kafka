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
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;
import java.util.Properties;

import java.time.Duration;
import java.time.Instant;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.junit.Assert.assertEquals;

public class KTableAggregateTest {
    private final Serde<String> stringSerde = Serdes.String();
    private final Consumed<String, String> consumed = Consumed.with(stringSerde, stringSerde);
    private final Grouped<String, String> stringSerialized = Grouped.with(stringSerde, stringSerde);
    private final MockProcessorSupplier<String, Object> supplier = new MockProcessorSupplier<>();
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
                    new KeyValueTimestamp<>("A", "0+1-1", 20L),
                    new KeyValueTimestamp<>("A", "0+1-1+3", 20L),
                    new KeyValueTimestamp<>("B", "0+2-2", 18L),
                    new KeyValueTimestamp<>("B", "0+2-2+4", 18L),
                    new KeyValueTimestamp<>("C", "0+5", 5L),
                    new KeyValueTimestamp<>("D", "0+6", 25L),
                    new KeyValueTimestamp<>("B", "0+2-2+4-4", 18L),
                    new KeyValueTimestamp<>("B", "0+2-2+4-4+7", 18L),
                    new KeyValueTimestamp<>("C", "0+5-5", 10L),
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

    private static void testCountHelper(final StreamsBuilder builder,
                                        final String input,
                                        final MockProcessorSupplier<String, Object> supplier) {
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
    public void testRemoveOldBeforeAddNew() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "count-test-input";
        final MockProcessorSupplier<String, String> supplier = new MockProcessorSupplier<>();

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

            final MockProcessor<String, String> proc = supplier.theCapturedProcessor();

            inputTopic.pipeInput("11", "A", 10L);
            inputTopic.pipeInput("12", "B", 8L);
            inputTopic.pipeInput("11", (String) null, 12L);
            inputTopic.pipeInput("12", "C", 6L);

            assertEquals(
                asList(
                    new KeyValueTimestamp<>("1", "1", 10),
                    new KeyValueTimestamp<>("1", "12", 10),
                    new KeyValueTimestamp<>("1", "2", 12),
                    new KeyValueTimestamp<>("1", "", 12),
                    new KeyValueTimestamp<>("1", "2", 12L)
                ),
                proc.processed()
            );
        }
    }
}
