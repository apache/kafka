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
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


@SuppressWarnings("deprecation")
public class KGroupedTableImplTest {

    private final StreamsBuilder builder = new StreamsBuilder();
    private static final String INVALID_STORE_NAME = "~foo bar~";
    private KGroupedTable<String, String> groupedTable;
    private TopologyTestDriver driver;
    private final Properties props = new Properties();
    private final String topic = "input";

    @Before
    public void before() {
        groupedTable = builder.table("blah", Consumed.with(Serdes.String(), Serdes.String()))
                .groupBy(MockMapper.<String, String>selectValueKeyValueMapper());

        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kgrouped-table-impl-test");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
    }

    @After
    public void cleanup() {
        props.clear();
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Test
    public void shouldAllowNullStoreNameOnCount()  {
        groupedTable.count((String) null);
    }

    @Test
    public void shouldAllowNullStoreNameOnAggregate() {
        groupedTable.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, MockAggregator.TOSTRING_REMOVER, (String) null);
    }

    @Test(expected = InvalidTopicException.class)
    public void shouldNotAllowInvalidStoreNameOnAggregate() {
        groupedTable.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, MockAggregator.TOSTRING_REMOVER, INVALID_STORE_NAME);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullInitializerOnAggregate() {
        groupedTable.aggregate(null, MockAggregator.TOSTRING_ADDER, MockAggregator.TOSTRING_REMOVER, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullAdderOnAggregate() {
        groupedTable.aggregate(MockInitializer.STRING_INIT, null, MockAggregator.TOSTRING_REMOVER, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullSubtractorOnAggregate() {
        groupedTable.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, null, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullAdderOnReduce() {
        groupedTable.reduce(null, MockReducer.STRING_REMOVER, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullSubtractorOnReduce() {
        groupedTable.reduce(MockReducer.STRING_ADDER, null, "store");
    }

    @Test
    public void shouldAllowNullStoreNameOnReduce() {
        groupedTable.reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER, (String) null);
    }

    @Test(expected = InvalidTopicException.class)
    public void shouldNotAllowInvalidStoreNameOnReduce() {
        groupedTable.reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER, INVALID_STORE_NAME);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullStoreSupplierOnReduce() {
        groupedTable.reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER, (StateStoreSupplier<KeyValueStore>) null);
    }

    private void doShouldReduce(final KTable<String, Integer> reduced, final String topic) {
        final Map<String, Integer> results = new HashMap<>();
        final ConsumerRecordFactory<String, Double> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new DoubleSerializer());
        reduced.foreach(new ForeachAction<String, Integer>() {
            @Override
            public void apply(final String key, final Integer value) {
                results.put(key, value);
            }
        });

        driver = new TopologyTestDriver(builder.build(), props);
        driver.pipeInput(recordFactory.create(topic, "A", 1.1, 10));
        driver.pipeInput(recordFactory.create(topic, "B", 2.2, 10));

        assertEquals(Integer.valueOf(1), results.get("A"));
        assertEquals(Integer.valueOf(2), results.get("B"));

        driver.pipeInput(recordFactory.create(topic, "A", 2.6, 10));
        driver.pipeInput(recordFactory.create(topic, "B", 1.3, 10));
        driver.pipeInput(recordFactory.create(topic, "A", 5.7, 10));
        driver.pipeInput(recordFactory.create(topic, "B", 6.2, 10));

        assertEquals(Integer.valueOf(5), results.get("A"));
        assertEquals(Integer.valueOf(6), results.get("B"));
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
            .reduce(MockReducer.INTEGER_ADDER, MockReducer.INTEGER_SUBTRACTOR, "reduced");

        doShouldReduce(reduced, topic);
        assertEquals(reduced.queryableStoreName(), "reduced");
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

        doShouldReduce(reduced, topic);
        assertNull(reduced.queryableStoreName());
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

        doShouldReduce(reduced, topic);
        final KeyValueStore<String, Integer> reduce = (KeyValueStore<String, Integer>) driver.getStateStore("reduce");
        assertThat(reduce.get("A"), equalTo(5));
        assertThat(reduce.get("B"), equalTo(6));
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

        processData(topic);
        final KeyValueStore<String, Long> counts = driver.getKeyValueStore("count");
        assertThat(counts.get("1"), equalTo(3L));
        assertThat(counts.get("2"), equalTo(2L));
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

        processData(topic);
        final KeyValueStore<String, String> aggregate = (KeyValueStore<String, String>) driver.getStateStore("aggregate");
        assertThat(aggregate.get("1"), equalTo("0+1+1+1"));
        assertThat(aggregate.get("2"), equalTo("0+2+2"));
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

    private void processData(final String topic) {
        driver = new TopologyTestDriver(builder.build(), props);
        final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
        driver.pipeInput(recordFactory.create(topic, "A", "1"));
        driver.pipeInput(recordFactory.create(topic, "B", "1"));
        driver.pipeInput(recordFactory.create(topic, "C", "1"));
        driver.pipeInput(recordFactory.create(topic, "D", "2"));
        driver.pipeInput(recordFactory.create(topic, "E", "2"));
    }
}
