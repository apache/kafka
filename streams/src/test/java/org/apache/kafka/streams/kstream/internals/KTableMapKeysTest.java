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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.KeyValueTimestampHeaders;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.StoreFormatTestUtils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KTableMapKeysTest {

    @ParameterizedTest
    @MethodSource("org.apache.kafka.test.StoreFormatTestUtils#storeFormats")
    public void testMapKeysConvertingToStream(final String storeFormat) {
        final Properties props = StoreFormatTestUtils.getProps(storeFormat, Serdes.Integer(), Serdes.String());
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic_map_keys";

        final KTable<Integer, String> table1 = builder.table(topic1, Consumed.with(Serdes.Integer(), Serdes.String()));

        final Map<Integer, String> keyMap = new HashMap<>();
        keyMap.put(1, "ONE");
        keyMap.put(2, "TWO");
        keyMap.put(3, "THREE");

        final KStream<String, String> convertedStream = table1.toStream((key, value) -> keyMap.get(key));

        final int[] originalKeys = new int[] {1, 2, 3};
        final String[] values = new String[] {"V_ONE", "V_TWO", "V_THREE"};
        final Headers[] headers = new Headers[] {
            StoreFormatTestUtils.makeHeaders("key", "ONE"),
            StoreFormatTestUtils.makeHeaders("key", "TWO"),
            StoreFormatTestUtils.makeHeaders("key", "THREE")
        };

        final MockApiProcessorSupplier<String, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        convertedStream.process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<Integer, String> inputTopic =
                    driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer());
            for (int i = 0; i < originalKeys.length; i++) {
                inputTopic.pipeInput(new TestRecord<>(originalKeys[i], values[i], headers[i], 5L + i * 5L));
            }
        }

        final var processors = supplier.capturedProcessors(1);

        if (storeFormat.equals("default")) {
            processors.get(0).checkAndClearProcessResult(
                new KeyValueTimestamp<>("ONE", "V_ONE", 5),
                new KeyValueTimestamp<>("TWO", "V_TWO", 10),
                new KeyValueTimestamp<>("THREE", "V_THREE", 15)
            );
        } else if (storeFormat.equals("headers")) {
            processors.get(0).checkAndClearProcessResultWithHeaders(
                new KeyValueTimestampHeaders<>("ONE", "V_ONE", 5, headers[0]),
                new KeyValueTimestampHeaders<>("TWO", "V_TWO", 10, headers[1]),
                new KeyValueTimestampHeaders<>("THREE", "V_THREE", 15, headers[2])
            );
        }
    }
}