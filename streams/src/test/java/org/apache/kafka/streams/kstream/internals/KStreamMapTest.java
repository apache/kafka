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
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class KStreamMapTest {
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.String());

    @Test
    public void testMap() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topicName = "topic";
        final int[] expectedKeys = new int[] {0, 1, 2, 3};

        final MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();
        final KStream<Integer, String> stream = builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.String()));
        stream.map((key, value) -> KeyValue.pair(value, key)).process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            for (final int expectedKey : expectedKeys) {
                final TestInputTopic<Integer, String> inputTopic =
                        driver.createInputTopic(topicName, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
                inputTopic.pipeInput(expectedKey, "V" + expectedKey, 10L - expectedKey);
            }
        }

        final KeyValueTimestamp[] expected = new KeyValueTimestamp[] {new KeyValueTimestamp<>("V0", 0, 10),
            new KeyValueTimestamp<>("V1", 1, 9),
            new KeyValueTimestamp<>("V2", 2, 8),
            new KeyValueTimestamp<>("V3", 3, 7)};
        assertEquals(4, supplier.theCapturedProcessor().processed().size());
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], supplier.theCapturedProcessor().processed().get(i));
        }
    }

    @Test
    public void testTypeVariance() {
        new StreamsBuilder()
            .<Integer, String>stream("numbers")
            .map((key, value) -> KeyValue.pair(key, key + ":" + value))
            .to("strings");
    }
}
