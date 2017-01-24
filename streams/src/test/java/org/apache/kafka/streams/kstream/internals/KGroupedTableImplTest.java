/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockKeyValueMapper;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class KGroupedTableImplTest {

    private final KStreamBuilder builder = new KStreamBuilder();
    private KGroupedTable<String, String> groupedTable;
    private KStreamTestDriver driver = null;

    @Before
    public void before() {
        groupedTable = builder.table(Serdes.String(), Serdes.String(), "blah", "blah")
                .groupBy(MockKeyValueMapper.<String, String>SelectValueKeyValueMapper());
    }

    @After
    public void cleanup() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullStoreNameOnAggregate() throws Exception {
        groupedTable.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, MockAggregator.TOSTRING_REMOVER, (String) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullInitializerOnAggregate() throws Exception {
        groupedTable.aggregate(null, MockAggregator.TOSTRING_ADDER, MockAggregator.TOSTRING_REMOVER, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullAdderOnAggregate() throws Exception {
        groupedTable.aggregate(MockInitializer.STRING_INIT, null, MockAggregator.TOSTRING_REMOVER, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullSubtractorOnAggregate() throws Exception {
        groupedTable.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, null, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullAdderOnReduce() throws Exception {
        groupedTable.reduce(null, MockReducer.STRING_REMOVER, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullSubtractorOnReduce() throws Exception {
        groupedTable.reduce(MockReducer.STRING_ADDER, null, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullStoreNameOnReduce() throws Exception {
        groupedTable.reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER, (String) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullStoreSupplierOnReduce() throws Exception {
        groupedTable.reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER, (String) null);
    }

    @Test
    public void shouldReduce() throws Exception {
        final String topic = "input";
        final KeyValueMapper<String, Number, KeyValue<String, Integer>> intProjection =
            new KeyValueMapper<String, Number, KeyValue<String, Integer>>() {
                @Override
                public KeyValue<String, Integer> apply(String key, Number value) {
                    return KeyValue.pair(key, value.intValue());
                }
            };

        final KTable<String, Integer> reduced = builder.table(Serdes.String(), Serdes.Double(), topic, "store")
            .groupBy(intProjection)
            .reduce(MockReducer.INTEGER_ADDER, MockReducer.INTEGER_SUBTRACTOR, "reduced");

        final Map<String, Integer> results = new HashMap<>();
        reduced.foreach(new ForeachAction<String, Integer>() {
            @Override
            public void apply(final String key, final Integer value) {
                results.put(key, value);
            }
        });

        driver = new KStreamTestDriver(builder, TestUtils.tempDirectory(), Serdes.String(), Serdes.Integer());
        driver.setTime(10L);
        driver.process(topic, "A", 1.1);
        driver.process(topic, "B", 2.2);
        driver.flushState();

        assertEquals(Integer.valueOf(1), results.get("A"));
        assertEquals(Integer.valueOf(2), results.get("B"));

        driver.process(topic, "A", 2.6);
        driver.process(topic, "B", 1.3);
        driver.process(topic, "A", 5.7);
        driver.process(topic, "B", 6.2);
        driver.flushState();

        assertEquals(Integer.valueOf(5), results.get("A"));
        assertEquals(Integer.valueOf(6), results.get("B"));
    }
}
