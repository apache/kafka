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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("deprecation")
public class KTableAggregateTest {
    private final Serde<String> stringSerde = Serdes.String();
    private final Consumed<String, String> consumed = Consumed.with(stringSerde, stringSerde);
    private final Grouped<String, String> stringSerialzied = Grouped.with(stringSerde, stringSerde);
    private final MockProcessorSupplier<String, Object> supplier = new MockProcessorSupplier<>();

    private File stateDir = null;

    @Rule
    public EmbeddedKafkaCluster cluster = null;
    @Rule
    public final org.apache.kafka.test.KStreamTestDriver driver = new org.apache.kafka.test.KStreamTestDriver();

    @Before
    public void setUp() {
        stateDir = TestUtils.tempDirectory("kafka-test");
    }

    @Test
    public void testAggBasic() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, String> table1 = builder.table(topic1, consumed);
        final KTable<String, String> table2 = table1
            .groupBy(
                MockMapper.noOpKeyValueMapper(),
                stringSerialzied)
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                MockAggregator.TOSTRING_REMOVER,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("topic1-Canonized")
                    .withValueSerde(stringSerde));

        table2.toStream().process(supplier);

        driver.setUp(builder, stateDir, Serdes.String(), Serdes.String());

        driver.process(topic1, "A", "1");
        driver.flushState();
        driver.process(topic1, "B", "2");
        driver.flushState();
        driver.process(topic1, "A", "3");
        driver.flushState();
        driver.process(topic1, "B", "4");
        driver.flushState();
        driver.process(topic1, "C", "5");
        driver.flushState();
        driver.process(topic1, "D", "6");
        driver.flushState();
        driver.process(topic1, "B", "7");
        driver.flushState();
        driver.process(topic1, "C", "8");
        driver.flushState();

        assertEquals(
            asList(
                "A:0+1 (ts: 0)",
                "B:0+2 (ts: 0)",
                "A:0+1-1+3 (ts: 0)",
                "B:0+2-2+4 (ts: 0)",
                "C:0+5 (ts: 0)",
                "D:0+6 (ts: 0)",
                "B:0+2-2+4-4+7 (ts: 0)",
                "C:0+5-5+8 (ts: 0)"),
            supplier.theCapturedProcessor().processed);
    }


    @Test
    public void testAggCoalesced() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, String> table1 = builder.table(topic1, consumed);
        final KTable<String, String> table2 = table1
            .groupBy(
                MockMapper.noOpKeyValueMapper(),
                stringSerialzied)
            .aggregate(MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                MockAggregator.TOSTRING_REMOVER,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("topic1-Canonized")
                    .withValueSerde(stringSerde));

        table2.toStream().process(supplier);

        driver.setUp(builder, stateDir);

        driver.process(topic1, "A", "1");
        driver.process(topic1, "A", "3");
        driver.process(topic1, "A", "4");
        driver.flushState();

        assertEquals(Collections.singletonList("A:0+4 (ts: 0)"), supplier.theCapturedProcessor().processed);
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
                stringSerialzied)
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                MockAggregator.TOSTRING_REMOVER,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("topic1-Canonized")
                    .withValueSerde(stringSerde));

        table2.toStream().process(supplier);

        driver.setUp(builder, stateDir);

        driver.process(topic1, "A", "1");
        driver.flushState();
        driver.process(topic1, "A", null);
        driver.flushState();
        driver.process(topic1, "A", "1");
        driver.flushState();
        driver.process(topic1, "B", "2");
        driver.flushState();
        driver.process(topic1, "null", "3");
        driver.flushState();
        driver.process(topic1, "B", "4");
        driver.flushState();
        driver.process(topic1, "NULL", "5");
        driver.flushState();
        driver.process(topic1, "B", "7");
        driver.flushState();

        assertEquals(
            asList(
                "1:0+1 (ts: 0)",
                "1:0+1-1 (ts: 0)",
                "1:0+1-1+1 (ts: 0)",
                "2:0+2 (ts: 0)",
                  //noop
                "2:0+2-2 (ts: 0)", "4:0+4 (ts: 0)",
                  //noop
                "4:0+4-4 (ts: 0)", "7:0+7 (ts: 0)"),
            supplier.theCapturedProcessor().processed);
    }

    private void testCountHelper(final StreamsBuilder builder,
                                 final String input,
                                 final MockProcessorSupplier<String, Object> supplier) {
        driver.setUp(builder, stateDir);

        driver.process(input, "A", "green");
        driver.flushState();
        driver.process(input, "B", "green");
        driver.flushState();
        driver.process(input, "A", "blue");
        driver.flushState();
        driver.process(input, "C", "yellow");
        driver.flushState();
        driver.process(input, "D", "green");
        driver.flushState();
        driver.flushState();

        assertEquals(
            asList(
                "green:1 (ts: 0)",
                "green:2 (ts: 0)",
                "green:1 (ts: 0)", "blue:1 (ts: 0)",
                "yellow:1 (ts: 0)",
                "green:2 (ts: 0)"),
            supplier.theCapturedProcessor().processed);
    }

    @Test
    public void testCount() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "count-test-input";

        builder
            .table(input, consumed)
            .groupBy(MockMapper.selectValueKeyValueMapper(), stringSerialzied)
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
            .groupBy(MockMapper.selectValueKeyValueMapper(), stringSerialzied)
            .count()
            .toStream()
            .process(supplier);

        testCountHelper(builder, input, supplier);
    }

    @Test
    public void testCountCoalesced() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "count-test-input";
        final MockProcessorSupplier<String, Long> supplier = new MockProcessorSupplier<>();

        builder
            .table(input, consumed)
            .groupBy(MockMapper.selectValueKeyValueMapper(), stringSerialzied)
            .count(Materialized.as("count"))
            .toStream()
            .process(supplier);

        driver.setUp(builder, stateDir);

        final MockProcessor<String, Long> proc = supplier.theCapturedProcessor();

        driver.process(input, "A", "green");
        driver.process(input, "B", "green");
        driver.process(input, "A", "blue");
        driver.process(input, "C", "yellow");
        driver.process(input, "D", "green");
        driver.flushState();

        assertEquals(
            asList(
                "blue:1 (ts: 0)",
                "yellow:1 (ts: 0)",
                "green:2 (ts: 0)"),
            proc.processed);
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
                stringSerialzied)
            .aggregate(
                () -> "",
                (aggKey, value, aggregate) -> aggregate + value,
                (key, value, aggregate) -> aggregate.replaceAll(value, ""),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("someStore")
                    .withValueSerde(Serdes.String()))
            .toStream()
            .process(supplier);

        driver.setUp(builder, stateDir);

        final MockProcessor<String, String> proc = supplier.theCapturedProcessor();

        driver.process(input, "11", "A");
        driver.flushState();
        driver.process(input, "12", "B");
        driver.flushState();
        driver.process(input, "11", null);
        driver.flushState();
        driver.process(input, "12", "C");
        driver.flushState();

        assertEquals(
            asList(
                "1:1 (ts: 0)",
                "1:12 (ts: 0)",
                "1:2 (ts: 0)",
                "1:2 (ts: 0)"),
            proc.processed);
    }

    @Test
    public void shouldForwardToCorrectProcessorNodeWhenMultiCacheEvictions() {
        final String tableOne = "tableOne";
        final String tableTwo = "tableTwo";
        final StreamsBuilder builder = new StreamsBuilder();
        final String reduceTopic = "TestDriver-reducer-store-repartition";
        final Map<String, Long> reduceResults = new HashMap<>();

        final KTable<String, String> one = builder.table(tableOne, consumed);
        final KTable<Long, String> two = builder.table(tableTwo, Consumed.with(Serdes.Long(), Serdes.String()));

        final KTable<String, Long> reduce = two
            .groupBy(
                (key, value) -> new KeyValue<>(value, key),
                Grouped.with(Serdes.String(), Serdes.Long()))
            .reduce(
                (value1, value2) -> value1 + value2,
                (value1, value2) -> value1 - value2,
                Materialized.as("reducer-store"));

        reduce.toStream().foreach(reduceResults::put);

        one.leftJoin(reduce, (value1, value2) -> value1 + ":" + value2)
            .mapValues(value -> value);

        driver.setUp(builder, stateDir, 111);
        driver.process(reduceTopic, "1", new Change<>(1L, null));
        driver.process("tableOne", "2", "2");
        // this should trigger eviction on the reducer-store topic
        driver.process(reduceTopic, "2", new Change<>(2L, null));
        // this wont as it is the same value
        driver.process(reduceTopic, "2", new Change<>(2L, null));
        assertEquals(Long.valueOf(2L), reduceResults.get("2"));

        // this will trigger eviction on the tableOne topic
        // that in turn will cause an eviction on reducer-topic. It will flush
        // key 2 as it is the only dirty entry in the cache
        driver.process("tableOne", "1", "5");
        assertEquals(Long.valueOf(4L), reduceResults.get("2"));
    }
}
