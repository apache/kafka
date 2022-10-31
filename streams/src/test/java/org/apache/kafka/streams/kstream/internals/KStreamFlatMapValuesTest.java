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
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;

public class KStreamFlatMapValuesTest {
    private final String topicName = "topic";
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.String());

    @Test
    public void testFlatMapValues() {
        final StreamsBuilder builder = new StreamsBuilder();

        final ValueMapper<Number, Iterable<String>> mapper =
            value -> {
                final ArrayList<String> result = new ArrayList<>();
                result.add("v" + value);
                result.add("V" + value);
                return result;
            };

        final int[] expectedKeys = {0, 1, 2, 3};

        final KStream<Integer, Integer> stream = builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.Integer()));
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        stream.flatMapValues(mapper).process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<Integer, Integer> inputTopic =
                    driver.createInputTopic(topicName, new IntegerSerializer(), new IntegerSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            for (final int expectedKey : expectedKeys) {
                // passing the timestamp to inputTopic.create to disambiguate the call
                inputTopic.pipeInput(expectedKey, expectedKey, 0L);
            }
        }

        final KeyValueTimestamp[] expected = {new KeyValueTimestamp<>(0, "v0", 0), new KeyValueTimestamp<>(0, "V0", 0),
            new KeyValueTimestamp<>(1, "v1", 0), new KeyValueTimestamp<>(1, "V1", 0),
            new KeyValueTimestamp<>(2, "v2", 0), new KeyValueTimestamp<>(2, "V2", 0),
            new KeyValueTimestamp<>(3, "v3", 0), new KeyValueTimestamp<>(3, "V3", 0)};

        assertArrayEquals(expected, supplier.theCapturedProcessor().processed().toArray());
    }


    @Test
    public void testFlatMapValuesWithKeys() {
        final StreamsBuilder builder = new StreamsBuilder();

        final ValueMapperWithKey<Integer, Number, Iterable<String>> mapper =
            (readOnlyKey, value) -> {
                final ArrayList<String> result = new ArrayList<>();
                result.add("v" + value);
                result.add("k" + readOnlyKey);
                return result;
            };

        final int[] expectedKeys = {0, 1, 2, 3};

        final KStream<Integer, Integer> stream = builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.Integer()));
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();

        stream.flatMapValues(mapper).process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<Integer, Integer> inputTopic =
                    driver.createInputTopic(topicName, new IntegerSerializer(), new IntegerSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            for (final int expectedKey : expectedKeys) {
                // passing the timestamp to inputTopic.create to disambiguate the call
                inputTopic.pipeInput(expectedKey, expectedKey, 0L);
            }
        }

        final KeyValueTimestamp[] expected = {new KeyValueTimestamp<>(0, "v0", 0),
            new KeyValueTimestamp<>(0, "k0", 0),
            new KeyValueTimestamp<>(1, "v1", 0),
            new KeyValueTimestamp<>(1, "k1", 0),
            new KeyValueTimestamp<>(2, "v2", 0),
            new KeyValueTimestamp<>(2, "k2", 0),
            new KeyValueTimestamp<>(3, "v3", 0),
            new KeyValueTimestamp<>(3, "k3", 0)};

        assertArrayEquals(expected, supplier.theCapturedProcessor().processed().toArray());
    }
}
