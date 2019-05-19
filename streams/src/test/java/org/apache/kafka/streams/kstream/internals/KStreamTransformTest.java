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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.time.Duration;
import java.util.Properties;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.junit.Assert.assertEquals;

public class KStreamTransformTest {
    private static final String TOPIC_NAME = "topic";
    private final ConsumerRecordFactory<Integer, Integer> recordFactory =
        new ConsumerRecordFactory<>(new IntegerSerializer(), new IntegerSerializer(), 0L);
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.Integer());

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
                        timestamp -> context.forward(-1, (int) timestamp)
                    );
                }

                @Override
                public KeyValue<Integer, Integer> transform(final Number key, final Number value) {
                    total += value.intValue();
                    return KeyValue.pair(key.intValue() * 2, total);
                }

                @Override
                public void close() {}
            };

        final int[] expectedKeys = {1, 10, 100, 1000};

        final MockProcessorSupplier<Integer, Integer> processor = new MockProcessorSupplier<>();
        final KStream<Integer, Integer> stream = builder.stream(TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer()));
        stream.transform(transformerSupplier).process(processor);

        try (final TopologyTestDriver driver = new TopologyTestDriver(
            builder.build(),
            mkProperties(mkMap(
                mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy"),
                mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "test")
            )),
            0L)) {
            final ConsumerRecordFactory<Integer, Integer> recordFactory =
                new ConsumerRecordFactory<>(TOPIC_NAME, new IntegerSerializer(), new IntegerSerializer());

            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(expectedKey, expectedKey * 10, expectedKey / 2L));
            }

            driver.advanceWallClockTime(2);
            driver.advanceWallClockTime(1);

            final String[] expected = {
                "2:10 (ts: 0)",
                "20:110 (ts: 5)",
                "200:1110 (ts: 50)",
                "2000:11110 (ts: 500)",
                "-1:2 (ts: 2)",
                "-1:3 (ts: 3)"
            };

            assertEquals(expected.length, processor.theCapturedProcessor().processed.size());
            for (int i = 0; i < expected.length; i++) {
                assertEquals(expected[i], processor.theCapturedProcessor().processed.get(i));
            }
        }
    }

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
                        timestamp -> context.forward(-1, (int) timestamp));
                }

                @Override
                public KeyValue<Integer, Integer> transform(final Number key, final Number value) {
                    total += value.intValue();
                    return KeyValue.pair(key.intValue() * 2, total);
                }

                @Override
                public void close() {}
            };

        final int[] expectedKeys = {1, 10, 100, 1000};

        final MockProcessorSupplier<Integer, Integer> processor = new MockProcessorSupplier<>();
        final KStream<Integer, Integer> stream = builder.stream(TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer()));
        stream.transform(transformerSupplier).process(processor);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props, 0L)) {
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(TOPIC_NAME, expectedKey, expectedKey * 10, 0L));
            }

            // This tick yields the "-1:2" result
            driver.advanceWallClockTime(2);
            // This tick further advances the clock to 3, which leads to the "-1:3" result
            driver.advanceWallClockTime(1);
        }

        assertEquals(6, processor.theCapturedProcessor().processed.size());

        final String[] expected = {"2:10 (ts: 0)", "20:110 (ts: 0)", "200:1110 (ts: 0)", "2000:11110 (ts: 0)", "-1:2 (ts: 2)", "-1:3 (ts: 3)"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processor.theCapturedProcessor().processed.get(i));
        }
    }

}
