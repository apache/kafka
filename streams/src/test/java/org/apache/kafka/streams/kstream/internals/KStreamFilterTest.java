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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class KStreamFilterTest {

    private final String topicName = "topic";
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.String());

    private final Predicate<Integer, String> isMultipleOfThree = (key, value) -> (key % 3) == 0;

    @Test
    public void testFilter() {
        final StreamsBuilder builder = new StreamsBuilder();
        final int[] expectedKeys = new int[]{1, 2, 3, 4, 5, 6, 7};

        final KStream<Integer, String> stream;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();

        stream = builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.String()));
        stream.filter(isMultipleOfThree).process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<Integer, String> inputTopic = driver.createInputTopic(topicName, new IntegerSerializer(), new StringSerializer());
            for (final int expectedKey : expectedKeys) {
                inputTopic.pipeInput(expectedKey, "V" + expectedKey);
            }
        }

        assertEquals(2, supplier.theCapturedProcessor().processed().size());
    }

    @Test
    public void testFilterNot() {
        final StreamsBuilder builder = new StreamsBuilder();
        final int[] expectedKeys = new int[]{1, 2, 3, 4, 5, 6, 7};

        final KStream<Integer, String> stream;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();

        stream = builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.String()));
        stream.filterNot(isMultipleOfThree).process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            for (final int expectedKey : expectedKeys) {
                final TestInputTopic<Integer, String> inputTopic = driver.createInputTopic(topicName, new IntegerSerializer(), new StringSerializer());
                inputTopic.pipeInput(expectedKey, "V" + expectedKey);
            }
        }

        assertEquals(5, supplier.theCapturedProcessor().processed().size());
    }

    @Test
    public void testTypeVariance() {
        final Predicate<Number, Object> numberKeyPredicate = (key, value) -> false;

        new StreamsBuilder()
            .<Integer, String>stream("empty")
            .filter(numberKeyPredicate)
            .filterNot(numberKeyPredicate)
            .to("nirvana");
    }
}
