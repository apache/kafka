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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockKeyValueMapper;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class KTableAggregateTest {

    final private Serde<String> stringSerde = Serdes.String();

    private KStreamTestDriver driver = null;
    private File stateDir = null;

    @After
    public void tearDown() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Before
    public void setUp() throws IOException {
        stateDir = TestUtils.tempDirectory("kafka-test");
    }

    @Test
    public void testAggBasic() throws Exception {
        final KStreamBuilder builder = new KStreamBuilder();
        final String topic1 = "topic1";
        final MockProcessorSupplier<String, String> proc = new MockProcessorSupplier<>();

        KTable<String, String> table1 = builder.table(stringSerde, stringSerde, topic1, "anyStoreName");
        KTable<String, String> table2 = table1.groupBy(MockKeyValueMapper.<String, String>NoOpKeyValueMapper(),
                stringSerde,
                stringSerde
        ).aggregate(MockInitializer.STRING_INIT,
                MockAggregator.STRING_ADDER,
                MockAggregator.STRING_REMOVER,
                stringSerde,
                "topic1-Canonized");

        table2.toStream().process(proc);

        driver = new KStreamTestDriver(builder, stateDir);

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

        assertEquals(Utils.mkList(
                "A:0+1",
                "B:0+2",
                "A:0+1-1+3",
                "B:0+2-2+4",
                "C:0+5",
                "D:0+6",
                "B:0+2-2+4-4+7",
                "C:0+5-5+8"), proc.processed);
    }


    @Test
    public void testAggCoalesced() throws Exception {
        final KStreamBuilder builder = new KStreamBuilder();
        final String topic1 = "topic1";
        final MockProcessorSupplier<String, String> proc = new MockProcessorSupplier<>();

        KTable<String, String> table1 = builder.table(stringSerde, stringSerde, topic1, "anyStoreName");
        KTable<String, String> table2 = table1.groupBy(MockKeyValueMapper.<String, String>NoOpKeyValueMapper(),
            stringSerde,
            stringSerde
        ).aggregate(MockInitializer.STRING_INIT,
            MockAggregator.STRING_ADDER,
            MockAggregator.STRING_REMOVER,
            stringSerde,
            "topic1-Canonized");

        table2.toStream().process(proc);

        driver = new KStreamTestDriver(builder, stateDir);

        driver.process(topic1, "A", "1");
        driver.process(topic1, "A", "3");
        driver.process(topic1, "A", "4");
        driver.flushState();
        assertEquals(Utils.mkList(
            "A:0+4"), proc.processed);
    }


    @Test
    public void testAggRepartition() throws Exception {
        final KStreamBuilder builder = new KStreamBuilder();
        final String topic1 = "topic1";
        final MockProcessorSupplier<String, String> proc = new MockProcessorSupplier<>();

        KTable<String, String> table1 = builder.table(stringSerde, stringSerde, topic1, "anyStoreName");
        KTable<String, String> table2 = table1.groupBy(new KeyValueMapper<String, String, KeyValue<String, String>>() {
            @Override
                public KeyValue<String, String> apply(String key, String value) {
                    if (key.equals("null")) {
                        return KeyValue.pair(null, value);
                    } else if (key.equals("NULL")) {
                        return null;
                    } else {
                        return KeyValue.pair(value, value);
                    }
                }
            },
                stringSerde,
                stringSerde
        )
                .aggregate(MockInitializer.STRING_INIT,
                MockAggregator.STRING_ADDER,
                MockAggregator.STRING_REMOVER,
                stringSerde,
                "topic1-Canonized");

        table2.toStream().process(proc);

        driver = new KStreamTestDriver(builder, stateDir);

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

        assertEquals(Utils.mkList(
                "1:0+1",
                "1:0+1-1",
                "1:0+1-1+1",
                "2:0+2", 
                  //noop
                "2:0+2-2", "4:0+4",
                  //noop
                "4:0+4-4", "7:0+7"
                ), proc.processed);
    }

    @Test
    public void testCount() throws IOException {
        final KStreamBuilder builder = new KStreamBuilder();
        final String input = "count-test-input";
        final MockProcessorSupplier<String, Long> proc = new MockProcessorSupplier<>();

        builder.table(Serdes.String(), Serdes.String(), input, "anyStoreName")
                .groupBy(MockKeyValueMapper.<String, String>SelectValueKeyValueMapper(), stringSerde, stringSerde)
                .count("count")
                .toStream()
                .process(proc);

        final KStreamTestDriver driver = new KStreamTestDriver(builder, stateDir);

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


        assertEquals(Utils.mkList(
                 "green:1",
                 "green:2",
                 "green:1", "blue:1",
                 "yellow:1",
                 "green:2"
                 ), proc.processed);
    }

    @Test
    public void testCountCoalesced() throws IOException {
        final KStreamBuilder builder = new KStreamBuilder();
        final String input = "count-test-input";
        final MockProcessorSupplier<String, Long> proc = new MockProcessorSupplier<>();

        builder.table(Serdes.String(), Serdes.String(), input, "anyStoreName")
            .groupBy(MockKeyValueMapper.<String, String>SelectValueKeyValueMapper(), stringSerde, stringSerde)
            .count("count")
            .toStream()
            .process(proc);

        final KStreamTestDriver driver = new KStreamTestDriver(builder, stateDir);

        driver.process(input, "A", "green");
        driver.process(input, "B", "green");
        driver.process(input, "A", "blue");
        driver.process(input, "C", "yellow");
        driver.process(input, "D", "green");
        driver.flushState();


        assertEquals(Utils.mkList(
            "blue:1",
            "yellow:1",
            "green:2"
            ), proc.processed);
    }
    
    @Test
    public void testRemoveOldBeforeAddNew() throws IOException {
        final KStreamBuilder builder = new KStreamBuilder();
        final String input = "count-test-input";
        final MockProcessorSupplier<String, String> proc = new MockProcessorSupplier<>();

        builder.table(Serdes.String(), Serdes.String(), input, "anyStoreName")
                .groupBy(new KeyValueMapper<String, String, KeyValue<String, String>>() {

                    @Override
                    public KeyValue<String, String> apply(String key, String value) {
                        return KeyValue.pair(String.valueOf(key.charAt(0)), String.valueOf(key.charAt(1)));
                    }
                }, stringSerde, stringSerde)
                .aggregate(new Initializer<String>() {

                    @Override
                    public String apply() {
                        return "";
                    }
                }, new Aggregator<String, String, String>() {
                    
                    @Override
                    public String apply(String aggKey, String value, String aggregate) {
                        return aggregate + value;
                    } 
                }, new Aggregator<String, String, String>() {

                    @Override
                    public String apply(String key, String value, String aggregate) {
                        return aggregate.replaceAll(value, "");
                    }
                }, Serdes.String(), "someStore")
                .toStream()
                .process(proc);

        final KStreamTestDriver driver = new KStreamTestDriver(builder, stateDir);

        driver.process(input, "11", "A");
        driver.flushState();
        driver.process(input, "12", "B");
        driver.flushState();
        driver.process(input, "11", null);
        driver.flushState();
        driver.process(input, "12", "C");
        driver.flushState();

        assertEquals(Utils.mkList(
                 "1:1",
                 "1:12",
                 "1:2",
                 "1:2"
                 ), proc.processed);
    }
}
