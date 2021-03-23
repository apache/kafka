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

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

public class KStreamSplitTest {

    private final String topicName = "topic";
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
    private final StreamsBuilder builder = new StreamsBuilder();
    private final Predicate<Integer, String> isEven = (key, value) -> (key % 2) == 0;
    private final Predicate<Integer, String> isMultipleOfThree = (key, value) -> (key % 3) == 0;
    private final Predicate<Integer, String> isMultipleOfFive = (key, value) -> (key % 5) == 0;
    private final Predicate<Integer, String> isMultipleOfSeven = (key, value) -> (key % 7) == 0;
    private final KStream<Integer, String> source = builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.String()));

    @Test
    public void testKStreamSplit() {
        final Map<String, KStream<Integer, String>> branches =
                source.split()
                        .branch(isEven, Branched.withConsumer(ks -> ks.to("x2")))
                        .branch(isMultipleOfThree, Branched.withConsumer(ks -> ks.to("x3")))
                        .branch(isMultipleOfFive, Branched.withConsumer(ks -> ks.to("x5"))).noDefaultBranch();

        assertEquals(0, branches.size());

        builder.build();

        withDriver(driver -> {
            final TestOutputTopic<Integer, String> x2 = driver.createOutputTopic("x2", new IntegerDeserializer(), new StringDeserializer());
            final TestOutputTopic<Integer, String> x3 = driver.createOutputTopic("x3", new IntegerDeserializer(), new StringDeserializer());
            final TestOutputTopic<Integer, String> x5 = driver.createOutputTopic("x5", new IntegerDeserializer(), new StringDeserializer());
            assertEquals(Arrays.asList("V2", "V4", "V6"), x2.readValuesToList());
            assertEquals(Arrays.asList("V3"), x3.readValuesToList());
            assertEquals(Arrays.asList("V5"), x5.readValuesToList());
        });
    }

    private void withDriver(final Consumer<TopologyTestDriver> test) {
        final int[] expectedKeys = new int[]{1, 2, 3, 4, 5, 6, 7};
        final Topology topology = builder.build();
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            final TestInputTopic<Integer, String> inputTopic = driver.createInputTopic(topicName, new IntegerSerializer(), new StringSerializer());
            for (final int expectedKey : expectedKeys) {
                inputTopic.pipeInput(expectedKey, "V" + expectedKey);
            }
            test.accept(driver);
        }
    }

    @Test
    public void testTypeVariance() {
        final Predicate<Number, Object> positive = (key, value) -> key.doubleValue() > 0;
        final Predicate<Number, Object> negative = (key, value) -> key.doubleValue() < 0;
        new StreamsBuilder()
                .<Integer, String>stream("empty")
                .split()
                .branch(positive)
                .branch(negative);
    }

    @Test
    public void testResultingMap() {
        final Map<String, KStream<Integer, String>> branches =
                source.split(Named.as("foo-"))
                        // "foo-bar"
                        .branch(isEven, Branched.as("bar"))
                        // no entry: a Consumer is provided
                        .branch(isMultipleOfThree, Branched.withConsumer(ks -> {
                        }))
                        // no entry: chain function returns null
                        .branch(isMultipleOfFive, Branched.withFunction(ks -> null))
                        // "foo-4": name defaults to the branch position
                        .branch(isMultipleOfSeven)
                        // "foo-0": "0" is the default name for the default branch
                        .defaultBranch();
        assertEquals(3, branches.size());
        branches.get("foo-bar").to("foo-bar");
        branches.get("foo-4").to("foo-4");
        branches.get("foo-0").to("foo-0");
        builder.build();

        withDriver(driver -> {
            final TestOutputTopic<Integer, String> even = driver.createOutputTopic("foo-bar", new IntegerDeserializer(), new StringDeserializer());
            final TestOutputTopic<Integer, String> x7 = driver.createOutputTopic("foo-4", new IntegerDeserializer(), new StringDeserializer());
            final TestOutputTopic<Integer, String> defaultBranch = driver.createOutputTopic("foo-0", new IntegerDeserializer(), new StringDeserializer());
            assertEquals(Arrays.asList("V2", "V4", "V6"), even.readValuesToList());
            assertEquals(Arrays.asList("V7"), x7.readValuesToList());
            assertEquals(Arrays.asList("V1"), defaultBranch.readValuesToList());
        });
    }

    @Test
    public void testBranchingWithNoTerminalOperation() {
        source.split()
                .branch(isEven, Branched.withConsumer(ks -> ks.to("output")))
                .branch(isMultipleOfFive, Branched.withConsumer(ks -> ks.to("output")));
        builder.build();
        withDriver(driver -> {
            final TestOutputTopic<Integer, String> outputTopic =
                    driver.createOutputTopic("output", new IntegerDeserializer(), new StringDeserializer());
            assertEquals(Arrays.asList("V2", "V4", "V5", "V6"), outputTopic.readValuesToList());
        });
    }
}
