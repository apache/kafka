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
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

public class KStreamTransformValuesTest {

    private String topicName = "topic";
    private final MockProcessorSupplier<Integer, Integer> supplier = new MockProcessorSupplier<>();
    private final ConsumerRecordFactory<Integer, Integer> recordFactory = new ConsumerRecordFactory<>(new IntegerSerializer(), new IntegerSerializer());
    private final Properties props = StreamsTestUtils.topologyTestConfig(Serdes.Integer(), Serdes.Integer());

    @Test
    public void testTransform() {
        StreamsBuilder builder = new StreamsBuilder();

        ValueTransformerSupplier<Number, Integer> valueTransformerSupplier =
            new ValueTransformerSupplier<Number, Integer>() {
                public ValueTransformer<Number, Integer> get() {
                    return new ValueTransformer<Number, Integer>() {

                        private int total = 0;

                        @Override
                        public void init(ProcessorContext context) {
                        }

                        @Override
                        public Integer transform(Number value) {
                            total += value.intValue();
                            return total;
                        }

                        @Override
                        public void close() {
                        }
                    };
                }
            };

        final int[] expectedKeys = {1, 10, 100, 1000};

        KStream<Integer, Integer> stream;
        stream = builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.Integer()));
        stream.transformValues(valueTransformerSupplier).process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props, 0L)) {
            for (int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topicName, expectedKey, expectedKey * 10, 0L));
            }
        }
        String[] expected = {"1:10", "10:110", "100:1110", "1000:11110"};

        assertArrayEquals(expected, supplier.theCapturedProcessor().processed.toArray());
    }

    @Test
    public void testTransformWithKey() {
        StreamsBuilder builder = new StreamsBuilder();

        ValueTransformerWithKeySupplier<Integer, Number, Integer> valueTransformerSupplier =
                new ValueTransformerWithKeySupplier<Integer, Number, Integer>() {
            public ValueTransformerWithKey<Integer, Number, Integer> get() {
                return new ValueTransformerWithKey<Integer, Number, Integer>() {
                    private int total = 0;
                    @Override
                    public void init(final ProcessorContext context) {

                    }
                    @Override
                    public Integer transform(final Integer readOnlyKey, final Number value) {
                        total += value.intValue() + readOnlyKey;
                        return total;
                    }

                    @Override
                    public void close() {

                    }
                };
            }
        };

        final int[] expectedKeys = {1, 10, 100, 1000};

        KStream<Integer, Integer> stream;
        stream = builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.Integer()));
        stream.transformValues(valueTransformerSupplier).process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props, 0L)) {
            for (int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topicName, expectedKey, expectedKey * 10, 0L));
            }
        }
        String[] expected = {"1:11", "10:121", "100:1221", "1000:12221"};

        assertArrayEquals(expected, supplier.theCapturedProcessor().processed.toArray());
    }


    @Test
    public void shouldNotAllowValueTransformerToCallInternalProcessorContextMethods() {
        final BadValueTransformer badValueTransformer = new BadValueTransformer();
        final KStreamTransformValues<Integer, Integer, Integer> transformValue = new KStreamTransformValues<>(new ValueTransformerWithKeySupplier<Integer, Integer, Integer>() {
            @Override
            public ValueTransformerWithKey<Integer, Integer, Integer> get() {
                return new ValueTransformerWithKey<Integer, Integer, Integer>() {
                    @Override
                    public void init(final ProcessorContext context) {
                        badValueTransformer.init(context);
                    }

                    @Override
                    public Integer transform(final Integer readOnlyKey, final Integer value) {
                        return badValueTransformer.transform(readOnlyKey, value);
                    }

                    @Override
                    public void close() {
                        badValueTransformer.close();
                    }
                };
            }
        });

        final Processor transformValueProcessor = transformValue.get();
        transformValueProcessor.init(null);

        try {
            transformValueProcessor.process(null, 0);
            fail("should not allow call to context.forward() within ValueTransformer");
        } catch (final StreamsException e) {
            // expected
        }

        try {
            transformValueProcessor.process(null, 1);
            fail("should not allow call to context.forward() within ValueTransformer");
        } catch (final StreamsException e) {
            // expected
        }

        try {
            transformValueProcessor.process(null, 2);
            fail("should not allow call to context.forward() within ValueTransformer");
        } catch (final StreamsException e) {
            // expected
        }

        try {
            transformValueProcessor.process(null, 3);
            fail("should not allow call to context.forward() within ValueTransformer");
        } catch (final StreamsException e) {
            // expected
        }
    }

    private static final class BadValueTransformer implements ValueTransformerWithKey<Integer, Integer, Integer> {
        private ProcessorContext context;

        @Override
        public void init(final ProcessorContext context) {
            this.context = context;
        }

        @Override
        public Integer transform(final Integer key, final Integer value) {
            if (value == 0) {
                context.forward(null, null);
            }
            if (value == 1) {
                context.forward(null, null, (String) null);
            }
            if (value == 2) {
                context.forward(null, null, 0);
            }
            if (value == 3) {
                context.forward(null, null, To.all());
            }
            throw new RuntimeException("Should never happen in this test");
        }

        @Override
        public void close() { }
    }
}
