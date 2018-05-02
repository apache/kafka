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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
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
import org.apache.kafka.test.NoOpInternalValueTransformer;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;

@RunWith(EasyMockRunner.class)
public class KStreamTransformValuesTest {

    private String topicName = "topic";

    final private Serde<Integer> intSerde = Serdes.Integer();
    private final ConsumerRecordFactory<Integer, Integer> recordFactory = new ConsumerRecordFactory<>(new IntegerSerializer(), new IntegerSerializer());
    private TopologyTestDriver driver;
    private final Properties props = new Properties();
    @Mock(MockType.NICE)
    private ProcessorContext context;

    @Before
    public void setup() {
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-transform-values-test");
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
                        public Integer punctuate(long timestamp) {
                            return null;
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

        driver = new TopologyTestDriver(builder.build(), props);

        for (int expectedKey : expectedKeys) {
            driver.pipeInput(recordFactory.create(topicName, expectedKey, expectedKey * 10, 0L));
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

        driver = new TopologyTestDriver(builder.build(), props);

        for (int expectedKey : expectedKeys) {
            driver.pipeInput(recordFactory.create(topicName, expectedKey, expectedKey * 10, 0L));
        }
        String[] expected = {"1:11", "10:121", "100:1221", "1000:12221"};

        assertArrayEquals(expected, processor.processed.toArray());
    }

    @Test
    public void shouldInitializeTransformerWithForwardDisabledProcessorContext() {
        final NoOpInternalValueTransformer<String, String> transformer = new NoOpInternalValueTransformer<>();
        final KStreamTransformValues<String, String, String> transformValues = new KStreamTransformValues<>(transformer);
        final Processor<String, String> processor = transformValues.get();

        processor.init(context);

        assertThat(transformer.context, isA((Class) ForwardingDisabledProcessorContext.class));
    }
}
