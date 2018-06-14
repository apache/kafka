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
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.ForwardingDisabledProcessorContext;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.SingletonNoOpValueTransformer;
import org.apache.kafka.test.StreamsTestUtils;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;

@RunWith(EasyMockRunner.class)
public class KStreamTransformValuesTest {

    private String topicName = "topic";
    private final MockProcessorSupplier<Integer, Integer> supplier = new MockProcessorSupplier<>();
    private final ConsumerRecordFactory<Integer, Integer> recordFactory = new ConsumerRecordFactory<>(new IntegerSerializer(), new IntegerSerializer());
    private final Properties props = StreamsTestUtils.topologyTestConfig(Serdes.Integer(), Serdes.Integer());
    @Mock(MockType.NICE)
    private ProcessorContext context;

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
    public void shouldInitializeTransformerWithForwardDisabledProcessorContext() {
        final SingletonNoOpValueTransformer<String, String> transformer = new SingletonNoOpValueTransformer<>();
        final KStreamTransformValues<String, String, String> transformValues = new KStreamTransformValues<>(transformer);
        final Processor<String, String> processor = transformValues.get();

        processor.init(context);

        assertThat(transformer.context, isA((Class) ForwardingDisabledProcessorContext.class));
    }
}
