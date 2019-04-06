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

import java.util.ArrayList;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;

public class KStreamFlatMapValuesTest {
    private final String topicName = "topic";
    private final ConsumerRecordFactory<Integer, Integer> recordFactory =
        new ConsumerRecordFactory<>(new IntegerSerializer(), new IntegerSerializer(), 0L);
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
        final MockProcessorSupplier<Integer, String> supplier = new MockProcessorSupplier<>();
        stream.flatMapValues(mapper).process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            for (final int expectedKey : expectedKeys) {
                // passing the timestamp to recordFactory.create to disambiguate the call
                driver.pipeInput(recordFactory.create(topicName, expectedKey, expectedKey, 0L));
            }
        }

        final String[] expected = {"0:v0 (ts: 0)", "0:V0 (ts: 0)", "1:v1 (ts: 0)", "1:V1 (ts: 0)", "2:v2 (ts: 0)", "2:V2 (ts: 0)", "3:v3 (ts: 0)", "3:V3 (ts: 0)"};

        assertArrayEquals(expected, supplier.theCapturedProcessor().processed.toArray());
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
        final MockProcessorSupplier<Integer, String> supplier = new MockProcessorSupplier<>();

        stream.flatMapValues(mapper).process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            for (final int expectedKey : expectedKeys) {
                // passing the timestamp to recordFactory.create to disambiguate the call
                driver.pipeInput(recordFactory.create(topicName, expectedKey, expectedKey, 0L));
            }
        }

        final String[] expected = {"0:v0 (ts: 0)", "0:k0 (ts: 0)", "1:v1 (ts: 0)", "1:k1 (ts: 0)", "2:v2 (ts: 0)", "2:k2 (ts: 0)", "3:v3 (ts: 0)", "3:k3 (ts: 0)"};

        assertArrayEquals(expected, supplier.theCapturedProcessor().processed.toArray());
    }
}
