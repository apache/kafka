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
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class KStreamBranchTest {

    private final String topicName = "topic";
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    @SuppressWarnings("unchecked")
    @Test
    public void testKStreamBranch() {
        final StreamsBuilder builder = new StreamsBuilder();

        final Predicate<Integer, String> isEven = (key, value) -> (key % 2) == 0;
        final Predicate<Integer, String> isMultipleOfThree = (key, value) -> (key % 3) == 0;
        final Predicate<Integer, String> isOdd = (key, value) -> (key % 2) != 0;

        final int[] expectedKeys = new int[]{1, 2, 3, 4, 5, 6};

        final KStream<Integer, String> stream;
        final KStream<Integer, String>[] branches;

        stream = builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.String()));
        branches = stream.branch(isEven, isMultipleOfThree, isOdd);

        assertEquals(3, branches.length);

        final MockProcessorSupplier<Integer, String> supplier = new MockProcessorSupplier<>();
        for (int i = 0; i < branches.length; i++) {
            branches[i].process(supplier);
        }

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<Integer, String> inputTopic = driver.createInputTopic(topicName, new IntegerSerializer(), new StringSerializer());
            for (final int expectedKey : expectedKeys) {
                inputTopic.pipeInput(expectedKey, "V" + expectedKey);
            }
        }

        final List<MockProcessor<Integer, String>> processors = supplier.capturedProcessors(3);
        assertEquals(3, processors.get(0).processed().size());
        assertEquals(1, processors.get(1).processed().size());
        assertEquals(2, processors.get(2).processed().size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testTypeVariance() {
        final Predicate<Number, Object> positive = (key, value) -> key.doubleValue() > 0;

        final Predicate<Number, Object> negative = (key, value) -> key.doubleValue() < 0;

        new StreamsBuilder()
            .<Integer, String>stream("empty")
            .branch(positive, negative);
    }
}
