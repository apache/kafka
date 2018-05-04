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

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class KStreamSelectKeyTest {

    private String topicName = "topic_key_select";

    final private Serde<Integer> integerSerde = Serdes.Integer();
    final private Serde<String> stringSerde = Serdes.String();
    private final ConsumerRecordFactory<String, Integer> recordFactory = new ConsumerRecordFactory<>(topicName, new StringSerializer(), new IntegerSerializer());
    private TopologyTestDriver driver;
    private final Properties props = new Properties();

    @Before
    public void setup() {
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-select-key-test");
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
    public void testSelectKey() {
        StreamsBuilder builder = new StreamsBuilder();

        final Map<Number, String> keyMap = new HashMap<>();
        keyMap.put(1, "ONE");
        keyMap.put(2, "TWO");
        keyMap.put(3, "THREE");


        KeyValueMapper<Object, Number, String> selector = new KeyValueMapper<Object, Number, String>() {
            @Override
            public String apply(Object key, Number value) {
                return keyMap.get(value);
            }
        };

        final String[] expected = new String[]{"ONE:1", "TWO:2", "THREE:3"};
        final int[] expectedValues = new int[]{1, 2, 3};

        KStream<String, Integer>  stream = builder.stream(topicName, Consumed.with(stringSerde, integerSerde));

        MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();

        stream.selectKey(selector).process(supplier);

        driver = new TopologyTestDriver(builder.build(), props);

        for (int expectedValue : expectedValues) {
            driver.pipeInput(recordFactory.create(expectedValue));
        }

        assertEquals(3, supplier.theCapturedProcessor().processed.size());

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], supplier.theCapturedProcessor().processed.get(i));
        }

    }

    @Test
    public void testTypeVariance() {
        ForeachAction<Number, Object> consume = new ForeachAction<Number, Object>() {
            @Override
            public void apply(Number key, Object value) {}
        };

        new StreamsBuilder()
            .<Integer, String>stream("empty")
            .foreach(consume);
    }
}
