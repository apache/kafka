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
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

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
        final int[] expectedKeys = new int[]{1, 2, 3, 4, 5, 6};

        final Map<String, KStream<Integer, String>> branches =
                source.split()
                        .branch(isEven, Branched.withConsumer(ks -> ks.to("x2")))
                        .branch(isMultipleOfThree, Branched.withConsumer(ks -> ks.to("x3")))
                        .branch(isMultipleOfFive, Branched.withConsumer(ks -> ks.to("x5"))).noDefaultBranch();

        assertEquals(0, branches.size());

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<Integer, String> inputTopic = driver.createInputTopic(topicName, new IntegerSerializer(), new StringSerializer());
            for (final int expectedKey : expectedKeys) {
                inputTopic.pipeInput(expectedKey, "V" + expectedKey);
            }
            final TestOutputTopic<Integer, String> x2 = driver.createOutputTopic("x2", new IntegerDeserializer(), new StringDeserializer());
            final TestOutputTopic<Integer, String> x3 = driver.createOutputTopic("x3", new IntegerDeserializer(), new StringDeserializer());
            final TestOutputTopic<Integer, String> x5 = driver.createOutputTopic("x5", new IntegerDeserializer(), new StringDeserializer());
            assertEquals(Arrays.asList("V2", "V4", "V6"), x2.readValuesToList());
            assertEquals(Arrays.asList("V3"), x3.readValuesToList());
            assertEquals(Arrays.asList("V5"), x5.readValuesToList());
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
/*
    public void testResultingMap() {
        final KStream<Integer, String> source = builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.String()));
        final Map<String, KStream<Integer, String>> result =
                source.split(Named.as("foo-"))
                        .branch(isEven, Branched.as("bar"))            // "foo-bar"
                        .branch(isMultipleOfThree, Branched.<Integer, String>withConsumer( ks -> myConsumer(ks))) // no entry: a Consumer is provided
                        .branch(isMultipleOfFive, Branched.withFunction(ks -> null))       // no entry: chain function returns null
                        .branch(isMultipleOfSeven)                                // "foo-4": name defaults to the branch position
                        .defaultBranch();                                   // "foo-0": "0" is the default name for the default branch
    }

    private void myConsumer(KStream<Integer, String> consumer){

    }*/
}
