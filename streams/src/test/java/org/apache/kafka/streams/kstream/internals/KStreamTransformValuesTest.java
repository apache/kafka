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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

public class KStreamTransformValuesTest {

    private String topicName = "topic";

    final private Serde<Integer> intSerde = Serdes.Integer();
    @Rule
    public final KStreamTestDriver driver = new KStreamTestDriver();

    @Test
    public void testTransform() {
        StreamsBuilder builder = new StreamsBuilder();

        ValueTransformerSupplier<Number, Integer> valueTransformerSupplier =
            ()-> { return new ValueTransformer<Number, Integer>() {

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
                        public Integer punctuate(long timestamp) {
                            return null;
                        }

                        @Override
                        public void close() {
                        }
                    };};

        final int[] expectedKeys = {1, 10, 100, 1000};

        KStream<Integer, Integer> stream;
        MockProcessorSupplier<Integer, Integer> processor = new MockProcessorSupplier<>();
        stream = builder.stream(topicName, Consumed.with(intSerde, intSerde));
        stream.transformValues(valueTransformerSupplier).process(processor);

        driver.setUp(builder);
        for (int expectedKey : expectedKeys) {
            driver.process(topicName, expectedKey, expectedKey * 10);
        }
        String[] expected = {"1:10", "10:110", "100:1110", "1000:11110"};

        assertArrayEquals(expected, processor.processed.toArray());
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
        MockProcessorSupplier<Integer, Integer> processor = new MockProcessorSupplier<>();
        stream = builder.stream(topicName, Consumed.with(intSerde, intSerde));
        stream.transformValues(valueTransformerSupplier).process(processor);

        driver.setUp(builder);
        for (int expectedKey : expectedKeys) {
            driver.process(topicName, expectedKey, expectedKey * 10);
        }
        String[] expected = {"1:11", "10:121", "100:1221", "1000:12221"};

        assertArrayEquals(expected, processor.processed.toArray());
    }


    @Test
    public void shouldNotAllowValueTransformerToCallInternalProcessorContextMethods() {
        final BadValueTransformer badValueTransformer = new BadValueTransformer();
        final KStreamTransformValues<Integer, Integer, Integer> transformValue = new KStreamTransformValues<>(new InternalValueTransformerWithKeySupplier<Integer, Integer, Integer>() {
            @Override
            public InternalValueTransformerWithKey<Integer, Integer, Integer> get() {
                return new InternalValueTransformerWithKey<Integer, Integer, Integer>() {
                    @Override
                    public Integer punctuate(long timestamp) {
                        throw new StreamsException("ValueTransformerWithKey#punctuate should not be called.");
                    }

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

        try {
            transformValueProcessor.punctuate(0);
            fail("should not allow ValueTransformer#puntuate() to return not-null value");
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
