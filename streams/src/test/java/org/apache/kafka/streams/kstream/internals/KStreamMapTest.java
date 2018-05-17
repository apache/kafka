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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class KStreamMapTest {

    private String topicName = "topic";
    private final ConsumerRecordFactory<Integer, String> recordFactory = new ConsumerRecordFactory<>(new IntegerSerializer(), new StringSerializer());
    private final Properties props = StreamsTestUtils.topologyTestConfig(Serdes.Integer(), Serdes.String());

    @Test
    public void testMap() {
        StreamsBuilder builder = new StreamsBuilder();

        KeyValueMapper<Integer, String, KeyValue<String, Integer>> mapper =
            new KeyValueMapper<Integer, String, KeyValue<String, Integer>>() {
                @Override
                public KeyValue<String, Integer> apply(Integer key, String value) {
                    return KeyValue.pair(value, key);
                }
            };

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();
        KStream<Integer, String> stream = builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.String()));
        stream.map(mapper).process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            for (int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topicName, expectedKey, "V" + expectedKey));
            }
        }

        assertEquals(4, supplier.theCapturedProcessor().processed.size());

        String[] expected = new String[]{"V0:0", "V1:1", "V2:2", "V3:3"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], supplier.theCapturedProcessor().processed.get(i));
        }
    }

    @Test
    public void testTypeVariance() {
        KeyValueMapper<Number, Object, KeyValue<Number, String>> stringify = new KeyValueMapper<Number, Object, KeyValue<Number, String>>() {
            @Override
            public KeyValue<Number, String> apply(Number key, Object value) {
                return KeyValue.pair(key, key + ":" + value);
            }
        };

        new StreamsBuilder()
            .<Integer, String>stream("numbers")
            .map(stringify)
            .to("strings");
    }
}
