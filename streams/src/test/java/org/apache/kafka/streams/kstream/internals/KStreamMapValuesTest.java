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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;

public class KStreamMapValuesTest {
    private final String topicName = "topic";
    private final MockApiProcessorSupplier<Integer, Integer, Void, Void> supplier = new MockApiProcessorSupplier<>();
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.String());

    @Test
    public void testFlatMapValues() {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = {1, 10, 100, 1000};

        final KStream<Integer, String> stream = builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.String()));
        stream.mapValues(CharSequence::length).process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            for (final int expectedKey : expectedKeys) {
                final TestInputTopic<Integer, String> inputTopic =
                        driver.createInputTopic(topicName, new IntegerSerializer(), new StringSerializer());
                inputTopic.pipeInput(expectedKey, Integer.toString(expectedKey), expectedKey / 2L);
            }
        }
        final KeyValueTimestamp[] expected = {new KeyValueTimestamp<>(1, 1, 0),
            new KeyValueTimestamp<>(10, 2, 5),
            new KeyValueTimestamp<>(100, 3, 50),
            new KeyValueTimestamp<>(1000, 4, 500)};

        assertArrayEquals(expected, supplier.theCapturedProcessor().processed().toArray());
    }

    @Test
    public void testMapValuesWithKeys() {
        final StreamsBuilder builder = new StreamsBuilder();

        final ValueMapperWithKey<Integer, CharSequence, Integer> mapper = (readOnlyKey, value) -> value.length() + readOnlyKey;

        final int[] expectedKeys = {1, 10, 100, 1000};

        final KStream<Integer, String> stream = builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.String()));
        stream.mapValues(mapper).process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<Integer, String> inputTopic =
                    driver.createInputTopic(topicName, new IntegerSerializer(), new StringSerializer());
            for (final int expectedKey : expectedKeys) {
                inputTopic.pipeInput(expectedKey, Integer.toString(expectedKey), expectedKey / 2L);
            }
        }
        final KeyValueTimestamp[] expected = {new KeyValueTimestamp<>(1, 2, 0),
            new KeyValueTimestamp<>(10, 12, 5),
            new KeyValueTimestamp<>(100, 103, 50),
            new KeyValueTimestamp<>(1000, 1004, 500)};

        assertArrayEquals(expected, supplier.theCapturedProcessor().processed().toArray());
    }

}
