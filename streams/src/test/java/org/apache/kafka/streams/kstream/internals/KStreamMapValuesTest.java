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
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;

public class KStreamMapValuesTest {

    private String topicName = "topic";
    private final MockProcessorSupplier<Integer, Integer> supplier = new MockProcessorSupplier<>();
    private final ConsumerRecordFactory<Integer, String> recordFactory = new ConsumerRecordFactory<>(new IntegerSerializer(), new StringSerializer());
    private final Properties props = StreamsTestUtils.topologyTestConfig(Serdes.Integer(), Serdes.String());

    @Test
    public void testFlatMapValues() {
        StreamsBuilder builder = new StreamsBuilder();

        ValueMapper<CharSequence, Integer> mapper =
            new ValueMapper<CharSequence, Integer>() {
                @Override
                public Integer apply(CharSequence value) {
                    return value.length();
                }
            };

        final int[] expectedKeys = {1, 10, 100, 1000};

        KStream<Integer, String> stream = builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.String()));
        stream.mapValues(mapper).process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            for (int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topicName, expectedKey, Integer.toString(expectedKey)));
            }
        }
        String[] expected = {"1:1", "10:2", "100:3", "1000:4"};

        assertArrayEquals(expected, supplier.theCapturedProcessor().processed.toArray());
    }

    @Test
    public void testMapValuesWithKeys() {
        StreamsBuilder builder = new StreamsBuilder();

        ValueMapperWithKey<Integer, CharSequence, Integer> mapper =
                new ValueMapperWithKey<Integer, CharSequence, Integer>() {
            @Override
            public Integer apply(final Integer readOnlyKey, final CharSequence value) {
                return value.length() + readOnlyKey;
            }
        };

        final int[] expectedKeys = {1, 10, 100, 1000};

        KStream<Integer, String> stream = builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.String()));
        stream.mapValues(mapper).process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            for (int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topicName, expectedKey, Integer.toString(expectedKey)));
            }
        }
        String[] expected = {"1:2", "10:12", "100:103", "1000:1004"};

        assertArrayEquals(expected, supplier.theCapturedProcessor().processed.toArray());
    }

}
