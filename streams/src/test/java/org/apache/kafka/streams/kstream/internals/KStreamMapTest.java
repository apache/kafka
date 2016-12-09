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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KStreamMapTest {

    private String topicName = "topic";

    final private Serde<Integer> intSerde = Serdes.Integer();
    final private Serde<String> stringSerde = Serdes.String();

    private KStreamTestDriver driver = null;

    @After
    public void cleanup() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Test
    public void testMap() {
        KStreamBuilder builder = new KStreamBuilder();

        KeyValueMapper<Integer, String, KeyValue<String, Integer>> mapper =
            new KeyValueMapper<Integer, String, KeyValue<String, Integer>>() {
                @Override
                public KeyValue<String, Integer> apply(Integer key, String value) {
                    return KeyValue.pair(value, key);
                }
            };

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        KStream<Integer, String> stream = builder.stream(intSerde, stringSerde, topicName);
        MockProcessorSupplier<String, Integer> processor;

        processor = new MockProcessorSupplier<>();
        stream.map(mapper).process(processor);

        driver = new KStreamTestDriver(builder);
        for (int expectedKey : expectedKeys) {
            driver.process(topicName, expectedKey, "V" + expectedKey);
        }

        assertEquals(4, processor.processed.size());

        String[] expected = new String[]{"V0:0", "V1:1", "V2:2", "V3:3"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processor.processed.get(i));
        }
    }

    @Test
    public void testTypeVariance() throws Exception {
        KeyValueMapper<Number, Object, KeyValue<Number, String>> stringify = new KeyValueMapper<Number, Object, KeyValue<Number, String>>() {
            @Override
            public KeyValue<Number, String> apply(Number key, Object value) {
                return KeyValue.pair(key, key + ":" + value);
            }
        };

        new KStreamBuilder()
            .<Integer, String>stream("numbers")
            .map(stringify)
            .to("strings");
    }
}
