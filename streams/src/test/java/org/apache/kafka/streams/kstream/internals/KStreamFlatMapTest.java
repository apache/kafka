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
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class KStreamFlatMapTest {
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.String());

    @Test
    public void testFlatMap() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topicName = "topic";

        final KeyValueMapper<Number, Object, Iterable<KeyValue<String, String>>> mapper =
            (key, value) -> {
                final ArrayList<KeyValue<String, String>> result = new ArrayList<>();
                for (int i = 0; i < key.intValue(); i++) {
                    result.add(KeyValue.pair(Integer.toString(key.intValue() * 10 + i), value.toString()));
                }
                return result;
            };

        final int[] expectedKeys = {0, 1, 2, 3};

        final KStream<Integer, String> stream;
        final MockProcessorSupplier<String, String> supplier;

        supplier = new MockProcessorSupplier<>();
        stream = builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.String()));
        stream.flatMap(mapper).process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<Integer, String> inputTopic =
                    driver.createInputTopic(topicName, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0), Duration.ZERO);
            for (final int expectedKey : expectedKeys) {
                inputTopic.pipeInput(expectedKey, "V" + expectedKey);
            }
        }

        assertEquals(6, supplier.theCapturedProcessor().processed().size());

        final KeyValueTimestamp[] expected = {new KeyValueTimestamp<>("10", "V1", 0),
            new KeyValueTimestamp<>("20", "V2", 0),
            new KeyValueTimestamp<>("21", "V2", 0),
            new KeyValueTimestamp<>("30", "V3", 0),
            new KeyValueTimestamp<>("31", "V3", 0),
            new KeyValueTimestamp<>("32", "V3", 0)};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], supplier.theCapturedProcessor().processed().get(i));
        }
    }
}
