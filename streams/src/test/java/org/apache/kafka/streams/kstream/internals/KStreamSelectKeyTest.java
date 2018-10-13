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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class KStreamSelectKeyTest {

    private String topicName = "topic_key_select";

    private final ConsumerRecordFactory<String, Integer> recordFactory = new ConsumerRecordFactory<>(topicName, new StringSerializer(), new IntegerSerializer());
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.Integer());

    @Test
    public void testSelectKey() {
        final StreamsBuilder builder = new StreamsBuilder();

        final Map<Number, String> keyMap = new HashMap<>();
        keyMap.put(1, "ONE");
        keyMap.put(2, "TWO");
        keyMap.put(3, "THREE");


        final KeyValueMapper<Object, Number, String> selector = new KeyValueMapper<Object, Number, String>() {
            @Override
            public String apply(final Object key, final Number value) {
                return keyMap.get(value);
            }
        };

        final String[] expected = new String[]{"ONE:1", "TWO:2", "THREE:3"};
        final int[] expectedValues = new int[]{1, 2, 3};

        final KStream<String, Integer>  stream = builder.stream(topicName, Consumed.with(Serdes.String(), Serdes.Integer()));

        final MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();

        stream.selectKey(selector).process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            for (final int expectedValue : expectedValues) {
                driver.pipeInput(recordFactory.create(expectedValue));
            }
        }

        assertEquals(3, supplier.theCapturedProcessor().processed.size());

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], supplier.theCapturedProcessor().processed.get(i));
        }

    }

    @Test
    public void testTypeVariance() {
        final ForeachAction<Number, Object> consume = new ForeachAction<Number, Object>() {
            @Override
            public void apply(final Number key, final Object value) {}
        };

        new StreamsBuilder()
            .<Integer, String>stream("empty")
            .foreach(consume);
    }
}
