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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

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
                builder.build(),
                mkProperties(mkMap(
                    mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy"),
                    mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "test"),
                    mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory("kafka-test").getAbsolutePath())
                )),
                0L)) {
            final ConsumerRecordFactory<String, String> recordFactory =
                new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer(), 0L, 0L);

            driver.pipeInput(recordFactory.create(topic1, "A", "1", 10L));
            driver.pipeInput(recordFactory.create(topic1, "B", "2", 15L));
            driver.pipeInput(recordFactory.create(topic1, "A", "3", 20L));
            driver.pipeInput(recordFactory.create(topic1, "B", "4", 18L));
            driver.pipeInput(recordFactory.create(topic1, "C", "5", 5L));
            driver.pipeInput(recordFactory.create(topic1, "D", "6", 25L));
            driver.pipeInput(recordFactory.create(topic1, "B", "7", 15L));
            driver.pipeInput(recordFactory.create(topic1, "C", "8", 10L));

            assertEquals(
                asList(
                    "A:0+1 (ts: 10)",
                    "B:0+2 (ts: 15)",
                    "A:0+1-1 (ts: 20)",
                    "A:0+1-1+3 (ts: 20)",
                    "B:0+2-2 (ts: 18)",
                    "B:0+2-2+4 (ts: 18)",
                    "C:0+5 (ts: 5)",
                    "D:0+6 (ts: 25)",
                    "B:0+2-2+4-4 (ts: 18)",
                    "B:0+2-2+4-4+7 (ts: 18)",
                    "C:0+5-5 (ts: 10)",
                    "C:0+5-5+8 (ts: 10)"),
                supplier.theCapturedProcessor().processed);
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
                builder.build(),
                mkProperties(mkMap(
                    mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy"),
                    mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "test"),
                    mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory("kafka-test").getAbsolutePath())
                )),
                0L)) {
            final ConsumerRecordFactory<String, String> recordFactory =
                new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer(), 0L, 0L);

            driver.pipeInput(recordFactory.create(topic1, "A", "1", 10L));
            driver.pipeInput(recordFactory.create(topic1, "A", (String) null, 15L));
            driver.pipeInput(recordFactory.create(topic1, "A", "1", 12L));
            driver.pipeInput(recordFactory.create(topic1, "B", "2", 20L));
            driver.pipeInput(recordFactory.create(topic1, "null", "3", 25L));
            driver.pipeInput(recordFactory.create(topic1, "B", "4", 23L));
            driver.pipeInput(recordFactory.create(topic1, "NULL", "5", 24L));
            driver.pipeInput(recordFactory.create(topic1, "B", "7", 22L));

            assertEquals(
                asList(
                    "1:0+1 (ts: 10)",
                    "1:0+1-1 (ts: 15)",
                    "1:0+1-1+1 (ts: 15)",
                    "2:0+2 (ts: 20)",
                    //noop
                    "2:0+2-2 (ts: 23)", "4:0+4 (ts: 23)",
                    //noop
                    "4:0+4-4 (ts: 23)", "7:0+7 (ts: 22)"),
                supplier.theCapturedProcessor().processed);
        }
    }

    private static void testCountHelper(final StreamsBuilder builder,
                                        final String input,
                                        final MockProcessorSupplier<String, Object> supplier) {
        try (
            final TopologyTestDriver driver = new TopologyTestDriver(
                builder.build(),
                mkProperties(mkMap(
                    mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy"),
                    mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "test"),
                    mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory("kafka-test").getAbsolutePath())
                )),
                0L)) {
            final ConsumerRecordFactory<String, String> recordFactory =
                new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer(), 0L, 0L);

            driver.pipeInput(recordFactory.create(input, "A", "green", 10L));
            driver.pipeInput(recordFactory.create(input, "B", "green", 9L));
            driver.pipeInput(recordFactory.create(input, "A", "blue", 12L));
            driver.pipeInput(recordFactory.create(input, "C", "yellow", 15L));
            driver.pipeInput(recordFactory.create(input, "D", "green", 11L));

            assertEquals(
                asList(
                    "green:1 (ts: 10)",
                    "green:2 (ts: 10)",
                    "green:1 (ts: 12)", "blue:1 (ts: 12)",
                    "yellow:1 (ts: 15)",
                    "green:2 (ts: 12)"),
                supplier.theCapturedProcessor().processed);
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
                builder.build(),
                mkProperties(mkMap(
                    mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy"),
                    mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "test"),
                    mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory("kafka-test").getAbsolutePath())
                )),
                0L)) {
            final ConsumerRecordFactory<String, String> recordFactory =
                new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer(), 0L, 0L);

            final MockProcessor<String, String> proc = supplier.theCapturedProcessor();

            driver.pipeInput(recordFactory.create(input, "11", "A", 10L));
            driver.pipeInput(recordFactory.create(input, "12", "B", 8L));
            driver.pipeInput(recordFactory.create(input, "11", (String) null, 12L));
            driver.pipeInput(recordFactory.create(input, "12", "C", 6L));

            assertEquals(
                asList(
                    "1:1 (ts: 10)",
                    "1:12 (ts: 10)",
                    "1:2 (ts: 12)",
                    "1: (ts: 12)",
                    "1:2 (ts: 12)"
                ),
                proc.processed
            );
        }
    }
}
