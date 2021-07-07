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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class KStreamTransformTest {
    private static final String TOPIC_NAME = "topic";
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.Integer());

    @SuppressWarnings("deprecation") // Old PAPI. Needs to be migrated.
    @Test
    public void testTransform() {
        final StreamsBuilder builder = new StreamsBuilder();

        final TransformerSupplier<Number, Number, KeyValue<Integer, Integer>> transformerSupplier =
            () -> new Transformer<Number, Number, KeyValue<Integer, Integer>>() {
                private int total = 0;

                @Override
                public void init(final ProcessorContext context) {
                    context.schedule(
                        Duration.ofMillis(1),
                        PunctuationType.WALL_CLOCK_TIME,
                        timestamp -> context.forward(-1, (int) timestamp, To.all().withTimestamp(timestamp))
                    );
                }

                @Override
                public KeyValue<Integer, Integer> transform(final Number key, final Number value) {
                    total += value.intValue();
                    return KeyValue.pair(key.intValue() * 2, total);
                }

                @Override
                public void close() { }
            };

        final int[] expectedKeys = {1, 10, 100, 1000};

        final MockProcessorSupplier<Integer, Integer> processor = new MockProcessorSupplier<>();
        final KStream<Integer, Integer> stream = builder.stream(TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer()));
        stream.transform(transformerSupplier).process(processor);

        try (final TopologyTestDriver driver = new TopologyTestDriver(
            builder.build(),
            Instant.ofEpochMilli(0L))) {
            final TestInputTopic<Integer, Integer> inputTopic =
                driver.createInputTopic(TOPIC_NAME, new IntegerSerializer(), new IntegerSerializer());

            for (final int expectedKey : expectedKeys) {
                inputTopic.pipeInput(expectedKey, expectedKey * 10, expectedKey / 2L);
            }

            driver.advanceWallClockTime(Duration.ofMillis(2));
            driver.advanceWallClockTime(Duration.ofMillis(1));

            final KeyValueTimestamp[] expected = {
                new KeyValueTimestamp<>(2, 10, 0),
                new KeyValueTimestamp<>(20, 110, 5),
                new KeyValueTimestamp<>(200, 1110, 50),
                new KeyValueTimestamp<>(2000, 11110, 500),
                new KeyValueTimestamp<>(-1, 2, 2),
                new KeyValueTimestamp<>(-1, 3, 3)
            };

            assertEquals(expected.length, processor.theCapturedProcessor().processed().size());
            for (int i = 0; i < expected.length; i++) {
                assertEquals(expected[i], processor.theCapturedProcessor().processed().get(i));
            }
        }
    }

    @SuppressWarnings("deprecation") // Old PAPI. Needs to be migrated.
    @Test
    public void testTransformWithNewDriverAndPunctuator() {
        final StreamsBuilder builder = new StreamsBuilder();

        final TransformerSupplier<Number, Number, KeyValue<Integer, Integer>> transformerSupplier =
            () -> new Transformer<Number, Number, KeyValue<Integer, Integer>>() {
                private int total = 0;

                @Override
                public void init(final ProcessorContext context) {
                    context.schedule(
                        Duration.ofMillis(1),
                        PunctuationType.WALL_CLOCK_TIME,
                        timestamp -> context.forward(-1, (int) timestamp, To.all().withTimestamp(timestamp)));
                }

                @Override
                public KeyValue<Integer, Integer> transform(final Number key, final Number value) {
                    total += value.intValue();
                    return KeyValue.pair(key.intValue() * 2, total);
                }

                @Override
                public void close() { }
            };

        final int[] expectedKeys = {1, 10, 100, 1000};

        final MockProcessorSupplier<Integer, Integer> processor = new MockProcessorSupplier<>();
        final KStream<Integer, Integer> stream = builder.stream(TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer()));
        stream.transform(transformerSupplier).process(processor);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<Integer, Integer> inputTopic =
                    driver.createInputTopic(TOPIC_NAME, new IntegerSerializer(), new IntegerSerializer());
            for (final int expectedKey : expectedKeys) {
                inputTopic.pipeInput(expectedKey, expectedKey * 10, 0L);
            }

            // This tick yields the "-1:2" result
            driver.advanceWallClockTime(Duration.ofMillis(2));
            // This tick further advances the clock to 3, which leads to the "-1:3" result
            driver.advanceWallClockTime(Duration.ofMillis(1));
        }

        assertEquals(6, processor.theCapturedProcessor().processed().size());

        final KeyValueTimestamp[] expected = {new KeyValueTimestamp<>(2, 10, 0),
            new KeyValueTimestamp<>(20, 110, 0),
            new KeyValueTimestamp<>(200, 1110, 0),
            new KeyValueTimestamp<>(2000, 11110, 0),
            new KeyValueTimestamp<>(-1, 2, 2),
            new KeyValueTimestamp<>(-1, 3, 3)};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processor.theCapturedProcessor().processed().get(i));
        }
    }

}
