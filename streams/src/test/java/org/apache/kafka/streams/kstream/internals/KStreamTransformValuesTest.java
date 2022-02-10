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
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.internals.ForwardingDisabledProcessorContext;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.NoOpValueTransformerWithKeySupplier;
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
    private final String topicName = "topic";
    private final MockProcessorSupplier<Integer, Integer> supplier = new MockProcessorSupplier<>();
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.Integer());
    @Mock(MockType.NICE)
    private InternalProcessorContext context;

    @SuppressWarnings("deprecation") // Old PAPI. Needs to be migrated.
    @Test
    public void testTransform() {
        final StreamsBuilder builder = new StreamsBuilder();

        final ValueTransformerSupplier<Number, Integer> valueTransformerSupplier =
            () -> new ValueTransformer<Number, Integer>() {
                private int total = 0;

                @Override
                public void init(final org.apache.kafka.streams.processor.ProcessorContext context) { }

                @Override
                public Integer transform(final Number value) {
                    total += value.intValue();
                    return total;
                }

                @Override
                public void close() { }
            };

        final int[] expectedKeys = {1, 10, 100, 1000};

        final KStream<Integer, Integer> stream;
        stream = builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.Integer()));
        stream.transformValues(valueTransformerSupplier).process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            for (final int expectedKey : expectedKeys) {
                final TestInputTopic<Integer, Integer> inputTopic =
                        driver.createInputTopic(topicName, new IntegerSerializer(), new IntegerSerializer());
                inputTopic.pipeInput(expectedKey, expectedKey * 10, expectedKey / 2L);
            }
        }
        final KeyValueTimestamp[] expected = {new KeyValueTimestamp<>(1, 10, 0),
            new KeyValueTimestamp<>(10, 110, 5),
            new KeyValueTimestamp<>(100, 1110, 50),
            new KeyValueTimestamp<>(1000, 11110, 500)};

        assertArrayEquals(expected, supplier.theCapturedProcessor().processed().toArray());
    }

    @SuppressWarnings("deprecation") // Old PAPI. Needs to be migrated.
    @Test
    public void testTransformWithKey() {
        final StreamsBuilder builder = new StreamsBuilder();

        final ValueTransformerWithKeySupplier<Integer, Number, Integer> valueTransformerSupplier =
            () -> new ValueTransformerWithKey<Integer, Number, Integer>() {
                private int total = 0;

                @Override
                public void init(final org.apache.kafka.streams.processor.ProcessorContext context) { }

                @Override
                public Integer transform(final Integer readOnlyKey, final Number value) {
                    total += value.intValue() + readOnlyKey;
                    return total;
                }

                @Override
                public void close() { }
            };

        final int[] expectedKeys = {1, 10, 100, 1000};

        final KStream<Integer, Integer> stream;
        stream = builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.Integer()));
        stream.transformValues(valueTransformerSupplier).process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<Integer, Integer> inputTopic =
                    driver.createInputTopic(topicName, new IntegerSerializer(), new IntegerSerializer());
            for (final int expectedKey : expectedKeys) {
                inputTopic.pipeInput(expectedKey, expectedKey * 10, expectedKey / 2L);
            }
        }
        final KeyValueTimestamp[] expected = {new KeyValueTimestamp<>(1, 11, 0),
            new KeyValueTimestamp<>(10, 121, 5),
            new KeyValueTimestamp<>(100, 1221, 50),
            new KeyValueTimestamp<>(1000, 12221, 500)};

        assertArrayEquals(expected, supplier.theCapturedProcessor().processed().toArray());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldInitializeTransformerWithForwardDisabledProcessorContext() {
        final NoOpValueTransformerWithKeySupplier<String, String> transformer = new NoOpValueTransformerWithKeySupplier<>();
        final KStreamTransformValues<String, String, String> transformValues = new KStreamTransformValues<>(transformer);
        final Processor<String, String, String, String> processor = transformValues.get();

        processor.init(context);

        assertThat(transformer.context, isA((Class) ForwardingDisabledProcessorContext.class));
    }
}
