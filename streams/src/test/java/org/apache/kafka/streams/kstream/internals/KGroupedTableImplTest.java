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

import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class KGroupedTableImplTest {

    private final StreamsBuilder builder = new StreamsBuilder();
    private static final String INVALID_STORE_NAME = "~foo bar~";
    private KGroupedTable<String, String> groupedTable;
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.Integer());
    private final String topic = "input";

    @Before
    public void before() {
        groupedTable = builder
            .table("blah", Consumed.with(Serdes.String(), Serdes.String()))
            .groupBy(MockMapper.selectValueKeyValueMapper());
    }

    @Test
    public void shouldNotAllowInvalidStoreNameOnAggregate() {
        assertThrows(TopologyException.class, () -> groupedTable.aggregate(
            MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            MockAggregator.TOSTRING_REMOVER,
            Materialized.as(INVALID_STORE_NAME)));
    }

    @Test
    public void shouldNotAllowNullInitializerOnAggregate() {
        assertThrows(NullPointerException.class, () -> groupedTable.aggregate(
            null,
            MockAggregator.TOSTRING_ADDER,
            MockAggregator.TOSTRING_REMOVER,
            Materialized.as("store")));
    }

    @Test
    public void shouldNotAllowNullAdderOnAggregate() {
        assertThrows(NullPointerException.class, () -> groupedTable.aggregate(
            MockInitializer.STRING_INIT,
            null,
            MockAggregator.TOSTRING_REMOVER,
            Materialized.as("store")));
    }

    @Test
    public void shouldNotAllowNullSubtractorOnAggregate() {
        assertThrows(NullPointerException.class, () -> groupedTable.aggregate(
            MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            null,
            Materialized.as("store")));
    }

    @Test
    public void shouldNotAllowNullAdderOnReduce() {
        assertThrows(NullPointerException.class, () -> groupedTable.reduce(
            null,
            MockReducer.STRING_REMOVER,
            Materialized.as("store")));
    }

    @Test
    public void shouldNotAllowNullSubtractorOnReduce() {
        assertThrows(NullPointerException.class, () -> groupedTable.reduce(
            MockReducer.STRING_ADDER,
            null,
            Materialized.as("store")));
    }

    @Test
    public void shouldNotAllowInvalidStoreNameOnReduce() {
        assertThrows(TopologyException.class, () -> groupedTable.reduce(
            MockReducer.STRING_ADDER,
            MockReducer.STRING_REMOVER,
            Materialized.as(INVALID_STORE_NAME)));
    }

    private MockApiProcessorSupplier<String, Integer, Void, Void> getReducedResults(final KTable<String, Integer> inputKTable) {
        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
        inputKTable
            .toStream()
            .process(supplier);
        return supplier;
    }

    private void assertReduced(final Map<String, ValueAndTimestamp<Integer>> reducedResults,
                               final String topic,
                               final TopologyTestDriver driver) {
        final TestInputTopic<String, Double> inputTopic =
            driver.createInputTopic(topic, new StringSerializer(), new DoubleSerializer());
        inputTopic.pipeInput("A", 1.1, 10);
        inputTopic.pipeInput("B", 2.2, 11);

        assertEquals(ValueAndTimestamp.make(1, 10L), reducedResults.get("A"));
        assertEquals(ValueAndTimestamp.make(2, 11L), reducedResults.get("B"));

        inputTopic.pipeInput("A", 2.6, 30);
        inputTopic.pipeInput("B", 1.3, 30);
        inputTopic.pipeInput("A", 5.7, 50);
        inputTopic.pipeInput("B", 6.2, 20);

        assertEquals(ValueAndTimestamp.make(5, 50L), reducedResults.get("A"));
        assertEquals(ValueAndTimestamp.make(6, 30L), reducedResults.get("B"));
    }

    @Test
    public void shouldReduce() {
        final KeyValueMapper<String, Number, KeyValue<String, Integer>> intProjection =
            (key, value) -> KeyValue.pair(key, value.intValue());

        final KTable<String, Integer> reduced = builder
            .table(
                topic,
                Consumed.with(Serdes.String(), Serdes.Double()),
                Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as("store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Double()))
            .groupBy(intProjection)
            .reduce(
                MockReducer.INTEGER_ADDER,
                MockReducer.INTEGER_SUBTRACTOR,
                Materialized.as("reduced"));

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = getReducedResults(reduced);
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            assertReduced(supplier.theCapturedProcessor().lastValueAndTimestampPerKey(), topic, driver);
            assertEquals(reduced.queryableStoreName(), "reduced");
        }
    }

    @Test
    public void shouldReduceWithInternalStoreName() {
        final KeyValueMapper<String, Number, KeyValue<String, Integer>> intProjection =
            (key, value) -> KeyValue.pair(key, value.intValue());

        final KTable<String, Integer> reduced = builder
            .table(
                topic,
                Consumed.with(Serdes.String(), Serdes.Double()),
                Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as("store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Double()))
            .groupBy(intProjection)
            .reduce(MockReducer.INTEGER_ADDER, MockReducer.INTEGER_SUBTRACTOR);

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = getReducedResults(reduced);
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            assertReduced(supplier.theCapturedProcessor().lastValueAndTimestampPerKey(), topic, driver);
            assertNull(reduced.queryableStoreName());
        }
    }

    @Test
    public void shouldReduceAndMaterializeResults() {
        final KeyValueMapper<String, Number, KeyValue<String, Integer>> intProjection =
            (key, value) -> KeyValue.pair(key, value.intValue());

        final KTable<String, Integer> reduced = builder
            .table(
                topic,
                Consumed.with(Serdes.String(), Serdes.Double()))
            .groupBy(intProjection)
            .reduce(
                MockReducer.INTEGER_ADDER,
                MockReducer.INTEGER_SUBTRACTOR,
                Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("reduce")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Integer()));

        final MockApiProcessorSupplier<String, Integer, Void, Void> supplier = getReducedResults(reduced);
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            assertReduced(supplier.theCapturedProcessor().lastValueAndTimestampPerKey(), topic, driver);
            {
                final KeyValueStore<String, Integer> reduce = driver.getKeyValueStore("reduce");
                assertThat(reduce.get("A"), equalTo(5));
                assertThat(reduce.get("B"), equalTo(6));
            }
            {
                final KeyValueStore<String, ValueAndTimestamp<Integer>> reduce = driver.getTimestampedKeyValueStore("reduce");
                assertThat(reduce.get("A"), equalTo(ValueAndTimestamp.make(5, 50L)));
                assertThat(reduce.get("B"), equalTo(ValueAndTimestamp.make(6, 30L)));
            }
        }
    }

    @Test
    public void shouldCountAndMaterializeResults() {
        builder
            .table(
                topic,
                Consumed.with(Serdes.String(), Serdes.String()))
            .groupBy(
                MockMapper.selectValueKeyValueMapper(),
                Grouped.with(Serdes.String(), Serdes.String()))
            .count(
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Long()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(topic, driver);
            {
                final KeyValueStore<String, Long> counts = driver.getKeyValueStore("count");
                assertThat(counts.get("1"), equalTo(3L));
                assertThat(counts.get("2"), equalTo(2L));
            }
            {
                final KeyValueStore<String, ValueAndTimestamp<Long>> counts = driver.getTimestampedKeyValueStore("count");
                assertThat(counts.get("1"), equalTo(ValueAndTimestamp.make(3L, 50L)));
                assertThat(counts.get("2"), equalTo(ValueAndTimestamp.make(2L, 60L)));
            }
        }
    }

    @Test
    public void shouldAggregateAndMaterializeResults() {
        builder
            .table(
                topic,
                Consumed.with(Serdes.String(), Serdes.String()))
            .groupBy(
                MockMapper.selectValueKeyValueMapper(),
                Grouped.with(Serdes.String(), Serdes.String()))
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                MockAggregator.TOSTRING_REMOVER,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("aggregate")
                    .withValueSerde(Serdes.String())
                    .withKeySerde(Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(topic, driver);
            {
                {
                    final KeyValueStore<String, String> aggregate = driver.getKeyValueStore("aggregate");
                    assertThat(aggregate.get("1"), equalTo("0+1+1+1"));
                    assertThat(aggregate.get("2"), equalTo("0+2+2"));
                }
                {
                    final KeyValueStore<String, ValueAndTimestamp<String>> aggregate = driver.getTimestampedKeyValueStore("aggregate");
                    assertThat(aggregate.get("1"), equalTo(ValueAndTimestamp.make("0+1+1+1", 50L)));
                    assertThat(aggregate.get("2"), equalTo(ValueAndTimestamp.make("0+2+2", 60L)));
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldThrowNullPointOnCountWhenMaterializedIsNull() {
        assertThrows(NullPointerException.class, () -> groupedTable.count((Materialized) null));
    }

    @Test
    public void shouldThrowNullPointerOnReduceWhenMaterializedIsNull() {
        assertThrows(NullPointerException.class, () -> groupedTable.reduce(
            MockReducer.STRING_ADDER,
            MockReducer.STRING_REMOVER,
            null));
    }

    @Test
    public void shouldThrowNullPointerOnReduceWhenAdderIsNull() {
        assertThrows(NullPointerException.class, () -> groupedTable.reduce(
            null,
            MockReducer.STRING_REMOVER,
            Materialized.as("store")));
    }

    @Test
    public void shouldThrowNullPointerOnReduceWhenSubtractorIsNull() {
        assertThrows(NullPointerException.class, () -> groupedTable.reduce(
            MockReducer.STRING_ADDER,
            null,
            Materialized.as("store")));
    }

    @Test
    public void shouldThrowNullPointerOnAggregateWhenInitializerIsNull() {
        assertThrows(NullPointerException.class, () -> groupedTable.aggregate(
            null,
            MockAggregator.TOSTRING_ADDER,
            MockAggregator.TOSTRING_REMOVER,
            Materialized.as("store")));
    }

    @Test
    public void shouldThrowNullPointerOnAggregateWhenAdderIsNull() {
        assertThrows(NullPointerException.class, () -> groupedTable.aggregate(
            MockInitializer.STRING_INIT,
            null,
            MockAggregator.TOSTRING_REMOVER,
            Materialized.as("store")));
    }

    @Test
    public void shouldThrowNullPointerOnAggregateWhenSubtractorIsNull() {
        assertThrows(NullPointerException.class, () -> groupedTable.aggregate(
            MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            null,
            Materialized.as("store")));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldThrowNullPointerOnAggregateWhenMaterializedIsNull() {
        assertThrows(NullPointerException.class, () -> groupedTable.aggregate(
            MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            MockAggregator.TOSTRING_REMOVER,
            (Materialized) null));
    }

    private void processData(final String topic,
                             final TopologyTestDriver driver) {
        final TestInputTopic<String, String> inputTopic =
            driver.createInputTopic(topic, new StringSerializer(), new StringSerializer());
        inputTopic.pipeInput("A", "1", 10L);
        inputTopic.pipeInput("B", "1", 50L);
        inputTopic.pipeInput("C", "1", 30L);
        inputTopic.pipeInput("D", "2", 40L);
        inputTopic.pipeInput("E", "2", 60L);
    }
}
