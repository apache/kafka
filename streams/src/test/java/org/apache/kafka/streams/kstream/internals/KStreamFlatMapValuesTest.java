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
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;

public class KStreamFlatMapValuesTest {

    private String topicName = "topic";
    private final ConsumerRecordFactory<Integer, Integer> recordFactory = new ConsumerRecordFactory<>(new IntegerSerializer(), new IntegerSerializer());
    private final Properties props = StreamsTestUtils.topologyTestConfig(Serdes.Integer(), Serdes.String());

    @Test
    public void testFlatMapValues() {
        StreamsBuilder builder = new StreamsBuilder();

        ValueMapper<Number, Iterable<String>> mapper =
            new ValueMapper<Number, Iterable<String>>() {
                @Override
                public Iterable<String> apply(Number value) {
                    ArrayList<String> result = new ArrayList<String>();
                    result.add("v" + value);
                    result.add("V" + value);
                    return result;
                }
            };

        final int[] expectedKeys = {0, 1, 2, 3};

        final KStream<Integer, Integer> stream = builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.Integer()));
        final MockProcessorSupplier<Integer, String> supplier = new MockProcessorSupplier<>();
        stream.flatMapValues(mapper).process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            for (final int expectedKey : expectedKeys) {
                // passing the timestamp to recordFactory.create to disambiguate the call
                driver.pipeInput(recordFactory.create(topicName, expectedKey, expectedKey, 0L));
            }
        }

        String[] expected = {"0:v0", "0:V0", "1:v1", "1:V1", "2:v2", "2:V2", "3:v3", "3:V3"};

        assertArrayEquals(expected, supplier.theCapturedProcessor().processed.toArray());
    }


    @Test
    public void testFlatMapValuesWithKeys() {
        StreamsBuilder builder = new StreamsBuilder();

        ValueMapperWithKey<Integer, Number, Iterable<String>> mapper =
                new ValueMapperWithKey<Integer, Number, Iterable<String>>() {
            @Override
            public Iterable<String> apply(final Integer readOnlyKey, final Number value) {
                ArrayList<String> result = new ArrayList<>();
                result.add("v" + value);
                result.add("k" + readOnlyKey);
                return result;
            }
        };

        final int[] expectedKeys = {0, 1, 2, 3};

        final KStream<Integer, Integer> stream = builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.Integer()));
        final MockProcessorSupplier<Integer, String> supplier = new MockProcessorSupplier<>();

        stream.flatMapValues(mapper).process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            for (final int expectedKey : expectedKeys) {
                // passing the timestamp to recordFactory.create to disambiguate the call
                driver.pipeInput(recordFactory.create(topicName, expectedKey, expectedKey, 0L));
            }
        }

        String[] expected = {"0:v0", "0:k0", "1:v1", "1:k1", "2:v2", "2:k2", "3:v3", "3:k3"};

        assertArrayEquals(expected, supplier.theCapturedProcessor().processed.toArray());
    }
}
