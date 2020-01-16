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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class KStreamForeachTest {

    private final String topicName = "topic";
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.String());

    @Test
    public void testForeach() {
        // Given
        final List<KeyValue<Integer, String>> inputRecords = Arrays.asList(
            new KeyValue<>(0, "zero"),
            new KeyValue<>(1, "one"),
            new KeyValue<>(2, "two"),
            new KeyValue<>(3, "three")
        );

        final List<KeyValue<Integer, String>> expectedRecords = Arrays.asList(
            new KeyValue<>(0, "ZERO"),
            new KeyValue<>(2, "ONE"),
            new KeyValue<>(4, "TWO"),
            new KeyValue<>(6, "THREE")
        );

        final List<KeyValue<Integer, String>> actualRecords = new ArrayList<>();
        final ForeachAction<Integer, String> action =
            (key, value) -> actualRecords.add(new KeyValue<>(key * 2, value.toUpperCase(Locale.ROOT)));

        // When
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Integer, String> stream = builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.String()));
        stream.foreach(action);

        // Then
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<Integer, String> inputTopic = driver.createInputTopic(topicName, new IntegerSerializer(), new StringSerializer());
            for (final KeyValue<Integer, String> record : inputRecords) {
                inputTopic.pipeInput(record.key, record.value);
            }
        }

        assertEquals(expectedRecords.size(), actualRecords.size());
        for (int i = 0; i < expectedRecords.size(); i++) {
            final KeyValue<Integer, String> expectedRecord = expectedRecords.get(i);
            final KeyValue<Integer, String> actualRecord = actualRecords.get(i);
            assertEquals(expectedRecord, actualRecord);
        }
    }

    @Test
    public void testTypeVariance() {
        final ForeachAction<Number, Object> consume = (key, value) -> { };

        new StreamsBuilder()
            .<Integer, String>stream("emptyTopic")
            .foreach(consume);
    }
}
