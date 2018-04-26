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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class KStreamTransformTest {

    private String topicName = "topic";

    final private Serde<Integer> intSerde = Serdes.Integer();
    private final ConsumerRecordFactory<Integer, Integer> recordFactory = new ConsumerRecordFactory<>(new IntegerSerializer(), new IntegerSerializer());
    private TopologyTestDriver driver;
    private final Properties props = new Properties();

    @Rule
    public final KStreamTestDriver kstreamDriver = new KStreamTestDriver();

    @Before
    public void setup() {
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-transform-test");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
    }

    @After
    public void cleanup() {
        props.clear();
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Test
    public void testTransform() {
        StreamsBuilder builder = new StreamsBuilder();

        TransformerSupplier<Number, Number, KeyValue<Integer, Integer>> transformerSupplier =
            new TransformerSupplier<Number, Number, KeyValue<Integer, Integer>>() {
                public Transformer<Number, Number, KeyValue<Integer, Integer>> get() {
                    return new Transformer<Number, Number, KeyValue<Integer, Integer>>() {

                        private int total = 0;

                        @Override
                        public void init(ProcessorContext context) {
                        }

                        @Override
                        public KeyValue<Integer, Integer> transform(Number key, Number value) {
                            total += value.intValue();
                            return KeyValue.pair(key.intValue() * 2, total);
                        }

                        @Override
                        public KeyValue<Integer, Integer> punctuate(long timestamp) {
                            return KeyValue.pair(-1, (int) timestamp);
                        }

                        @Override
                        public void close() {
                        }
                    };
                }
            };

        final int[] expectedKeys = {1, 10, 100, 1000};

        MockProcessorSupplier<Integer, Integer> processor = new MockProcessorSupplier<>();
        KStream<Integer, Integer> stream = builder.stream(topicName, Consumed.with(intSerde, intSerde));
        stream.transform(transformerSupplier).process(processor);

        kstreamDriver.setUp(builder);
        for (int expectedKey : expectedKeys) {
            kstreamDriver.process(topicName, expectedKey, expectedKey * 10);
        }

        kstreamDriver.punctuate(2);
        kstreamDriver.punctuate(3);

        assertEquals(6, processor.processed.size());

        String[] expected = {"2:10", "20:110", "200:1110", "2000:11110", "-1:2", "-1:3"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processor.processed.get(i));
        }
    }

    @Test
    public void testTransformWithNewDriverAndPunctuator() {
        StreamsBuilder builder = new StreamsBuilder();

        TransformerSupplier<Number, Number, KeyValue<Integer, Integer>> transformerSupplier =
            new TransformerSupplier<Number, Number, KeyValue<Integer, Integer>>() {
                public Transformer<Number, Number, KeyValue<Integer, Integer>> get() {
                    return new Transformer<Number, Number, KeyValue<Integer, Integer>>() {

                        private int total = 0;

                        @Override
                        public void init(final ProcessorContext context) {
                            context.schedule(1, PunctuationType.WALL_CLOCK_TIME, new Punctuator() {
                                @Override
                                public void punctuate(long timestamp) {
                                    context.forward(-1, (int) timestamp);
                                }
                            });
                        }

                        @Override
                        public KeyValue<Integer, Integer> transform(Number key, Number value) {
                            total += value.intValue();
                            return KeyValue.pair(key.intValue() * 2, total);
                        }

                        @Override
                        public KeyValue<Integer, Integer> punctuate(long timestamp) {
                            return null;
                        }

                        @Override
                        public void close() {
                        }
                    };
                }
            };

        final int[] expectedKeys = {1, 10, 100, 1000};

        MockProcessorSupplier<Integer, Integer> processor = new MockProcessorSupplier<>();
        KStream<Integer, Integer> stream = builder.stream(topicName, Consumed.with(intSerde, intSerde));
        stream.transform(transformerSupplier).process(processor);

        driver = new TopologyTestDriver(builder.build(), props, 0L);
        for (int expectedKey : expectedKeys) {
            driver.pipeInput(recordFactory.create(topicName, expectedKey, expectedKey * 10, 0L));
        }

        // This tick will yield yields the "-1:2" result
        driver.advanceWallClockTime(2);
        // This tick further advances the clock to 3, which leads to the "-1:3" result
        driver.advanceWallClockTime(1);

        assertEquals(6, processor.processed.size());

        String[] expected = {"2:10", "20:110", "200:1110", "2000:11110", "-1:2", "-1:3"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processor.processed.get(i));
        }
    }

}
