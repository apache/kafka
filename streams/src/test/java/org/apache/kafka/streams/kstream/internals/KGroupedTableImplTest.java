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

import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class KGroupedTableImplTest {

    private final StreamsBuilder builder = new StreamsBuilder();
    private static final String INVALID_STORE_NAME = "~foo bar~";
    private KGroupedTable<String, String> groupedTable;
    private final Properties props = StreamsTestUtils.topologyTestConfig(Serdes.String(), Serdes.Integer());
    private final String topic = "input";

    @Before
    public void before() {
        groupedTable = builder.table("blah", Consumed.with(Serdes.String(), Serdes.String()))
                .groupBy(MockMapper.<String, String>selectValueKeyValueMapper());
    }

    @Test(expected = InvalidTopicException.class)
    public void shouldNotAllowInvalidStoreNameOnAggregate() {
        groupedTable.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, MockAggregator.TOSTRING_REMOVER, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(INVALID_STORE_NAME));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullInitializerOnAggregate() {
        groupedTable.aggregate(null, MockAggregator.TOSTRING_ADDER, MockAggregator.TOSTRING_REMOVER, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullAdderOnAggregate() {
        groupedTable.aggregate(MockInitializer.STRING_INIT, null, MockAggregator.TOSTRING_REMOVER, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullSubtractorOnAggregate() {
        groupedTable.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, null, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullAdderOnReduce() {
        groupedTable.reduce(null, MockReducer.STRING_REMOVER, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullSubtractorOnReduce() {
        groupedTable.reduce(MockReducer.STRING_ADDER, null, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("store"));
    }

    @Test(expected = InvalidTopicException.class)
    public void shouldNotAllowInvalidStoreNameOnReduce() {
        groupedTable.reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(INVALID_STORE_NAME));
    }

    private Map<String, Integer> getReducedResults(final KTable<String, Integer> inputKTable) {
        final Map<String, Integer> reducedResults = new HashMap<>();
        inputKTable.toStream().foreach(new ForeachAction<String, Integer>() {
            @Override
            public void apply(final String key, final Integer value) {
                reducedResults.put(key, value);
            }
        });
        return reducedResults;
    }
    private void assertReduced(final Map<String, Integer> reducedResults, final String topic, final TopologyTestDriver driver) {
        final ConsumerRecordFactory<String, Double> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new DoubleSerializer());
        driver.pipeInput(recordFactory.create(topic, "A", 1.1, 10));
        driver.pipeInput(recordFactory.create(topic, "B", 2.2, 10));

        assertEquals(Integer.valueOf(1), reducedResults.get("A"));
        assertEquals(Integer.valueOf(2), reducedResults.get("B"));

        driver.pipeInput(recordFactory.create(topic, "A", 2.6, 10));
        driver.pipeInput(recordFactory.create(topic, "B", 1.3, 10));
        driver.pipeInput(recordFactory.create(topic, "A", 5.7, 10));
        driver.pipeInput(recordFactory.create(topic, "B", 6.2, 10));

        assertEquals(Integer.valueOf(5), reducedResults.get("A"));
        assertEquals(Integer.valueOf(6), reducedResults.get("B"));
    }

    @Test
    public void shouldReduce() {
        final KeyValueMapper<String, Number, KeyValue<String, Integer>> intProjection =
            new KeyValueMapper<String, Number, KeyValue<String, Integer>>() {
                @Override
                public KeyValue<String, Integer> apply(String key, Number value) {
                    return KeyValue.pair(key, value.intValue());
                }
            };

        final KTable<String, Integer> reduced = builder.table(topic,
                                                              Consumed.with(Serdes.String(), Serdes.Double()),
                                                              Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as("store")
                                                                      .withKeySerde(Serdes.String())
                                                                      .withValueSerde(Serdes.Double()))
            .groupBy(intProjection)
            .reduce(MockReducer.INTEGER_ADDER, MockReducer.INTEGER_SUBTRACTOR, Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("reduced"));

        final Map<String, Integer> results = getReducedResults(reduced);
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            assertReduced(results, topic, driver);
            assertEquals(reduced.queryableStoreName(), "reduced");
        }
    }

    @Test
    public void shouldReduceWithInternalStoreName() {
        final KeyValueMapper<String, Number, KeyValue<String, Integer>> intProjection =
            new KeyValueMapper<String, Number, KeyValue<String, Integer>>() {
                @Override
                public KeyValue<String, Integer> apply(String key, Number value) {
                    return KeyValue.pair(key, value.intValue());
                }
            };

        final KTable<String, Integer> reduced = builder.table(topic,
                                                              Consumed.with(Serdes.String(), Serdes.Double()),
                                                              Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as("store")
                                                                      .withKeySerde(Serdes.String())
                                                                      .withValueSerde(Serdes.Double()))
            .groupBy(intProjection)
            .reduce(MockReducer.INTEGER_ADDER, MockReducer.INTEGER_SUBTRACTOR);

        final Map<String, Integer> results = getReducedResults(reduced);
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            assertReduced(results, topic, driver);
            assertNull(reduced.queryableStoreName());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldReduceAndMaterializeResults() {
        final KeyValueMapper<String, Number, KeyValue<String, Integer>> intProjection =
            new KeyValueMapper<String, Number, KeyValue<String, Integer>>() {
                @Override
                public KeyValue<String, Integer> apply(String key, Number value) {
                    return KeyValue.pair(key, value.intValue());
                }
            };

        final KTable<String, Integer> reduced = builder.table(topic, Consumed.with(Serdes.String(), Serdes.Double()))
                .groupBy(intProjection)
                .reduce(MockReducer.INTEGER_ADDER,
                        MockReducer.INTEGER_SUBTRACTOR,
                        Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("reduce")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Integer()));

        final Map<String, Integer> results = getReducedResults(reduced);
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            assertReduced(results, topic, driver);
            final KeyValueStore<String, Integer> reduce = (KeyValueStore<String, Integer>) driver.getStateStore("reduce");
            assertThat(reduce.get("A"), equalTo(5));
            assertThat(reduce.get("B"), equalTo(6));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldCountAndMaterializeResults() {
        final KTable<String, String> table = builder.table(topic, Consumed.with(Serdes.String(), Serdes.String()));
        table.groupBy(MockMapper.<String, String>selectValueKeyValueMapper(),
                      Serialized.with(Serdes.String(),
                                      Serdes.String()))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count")
                               .withKeySerde(Serdes.String())
                               .withValueSerde(Serdes.Long()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(topic, driver);
            final KeyValueStore<String, Long> counts = driver.getKeyValueStore("count");
            assertThat(counts.get("1"), equalTo(3L));
            assertThat(counts.get("2"), equalTo(2L));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldAggregateAndMaterializeResults() {
        final KTable<String, String> table = builder.table(topic, Consumed.with(Serdes.String(), Serdes.String()));
        table.groupBy(MockMapper.<String, String>selectValueKeyValueMapper(),
                      Serialized.with(Serdes.String(),
                                      Serdes.String()))
                .aggregate(MockInitializer.STRING_INIT,
                           MockAggregator.TOSTRING_ADDER,
                           MockAggregator.TOSTRING_REMOVER,
                           Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("aggregate")
                                   .withValueSerde(Serdes.String())
                                   .withKeySerde(Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(topic, driver);
            final KeyValueStore<String, String> aggregate = (KeyValueStore<String, String>) driver.getStateStore("aggregate");
            assertThat(aggregate.get("1"), equalTo("0+1+1+1"));
            assertThat(aggregate.get("2"), equalTo("0+2+2"));
        }
    }

    @SuppressWarnings("unchecked")
    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointOnCountWhenMaterializedIsNull() {
        groupedTable.count((Materialized) null);
    }

    @SuppressWarnings("unchecked")
    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnReduceWhenMaterializedIsNull() {
        groupedTable.reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER, (Materialized) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnReduceWhenAdderIsNull() {
        groupedTable.reduce(null, MockReducer.STRING_REMOVER, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnReduceWhenSubtractorIsNull() {
        groupedTable.reduce(MockReducer.STRING_ADDER, null, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnAggregateWhenInitializerIsNull() {
        groupedTable.aggregate(null,
                               MockAggregator.TOSTRING_ADDER,
                               MockAggregator.TOSTRING_REMOVER,
                               Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnAggregateWhenAdderIsNull() {
        groupedTable.aggregate(MockInitializer.STRING_INIT,
                               null,
                               MockAggregator.TOSTRING_REMOVER,
                               Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("store"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnAggregateWhenSubtractorIsNull() {
        groupedTable.aggregate(MockInitializer.STRING_INIT,
                               MockAggregator.TOSTRING_ADDER,
                               null,
                               Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("store"));
    }

    @SuppressWarnings("unchecked")
    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnAggregateWhenMaterializedIsNull() {
        groupedTable.aggregate(MockInitializer.STRING_INIT,
                               MockAggregator.TOSTRING_ADDER,
                               MockAggregator.TOSTRING_REMOVER,
                               (Materialized) null);
    }

    private void processData(final String topic, final TopologyTestDriver driver) {
        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
        driver.pipeInput(recordFactory.create(topic, "A", "1"));
        driver.pipeInput(recordFactory.create(topic, "B", "1"));
        driver.pipeInput(recordFactory.create(topic, "C", "1"));
        driver.pipeInput(recordFactory.create(topic, "D", "2"));
        driver.pipeInput(recordFactory.create(topic, "E", "2"));
    }
}
