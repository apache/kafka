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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.DistinctParameters;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class KStreamDistinctTest {

    private final String input = "input-topic";
    private final String output = "output-topic";
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    @Test
    public void testStreamDistinct() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String[] inputKeys = new String[]{"A", "B", "B", "A", "C", "A"};
        builder.stream(input, Consumed.with(Serdes.String(), Serdes.String()))
                .distinct(DistinctParameters.with(
                        TimeWindows.of(Duration.ofMinutes(5)), (k, v) -> k, Serdes.String()))
                .to(output);
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic(input, new StringSerializer(), new StringSerializer());
            final Instant now = Instant.now();
            for (final String expectedKey : inputKeys) {
                inputTopic.pipeInput(expectedKey, expectedKey, now);
            }
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(output, new StringDeserializer(), new StringDeserializer());
            final List<String> strings = outputTopic.readValuesToList();
            assertEquals(Arrays.asList("A", "B", "C"), strings);
        }
    }
}
