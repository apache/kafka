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
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.NoopValueTransformer;
import org.apache.kafka.test.NoopValueTransformerWithKey;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorGraphNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Random;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class AbstractStreamTest {

    @Test
    public void testToInternalValueTransformerSupplierSuppliesNewTransformers() {
        final ValueTransformerSupplier<?, ?> valueTransformerSupplier = mock(ValueTransformerSupplier.class);
        when(valueTransformerSupplier.get())
            .thenReturn(new NoopValueTransformer<>())
            .thenReturn(new NoopValueTransformer<>());
        final ValueTransformerWithKeySupplier<?, ?, ?> valueTransformerWithKeySupplier =
            AbstractStream.toValueTransformerWithKeySupplier(valueTransformerSupplier);
        valueTransformerWithKeySupplier.get();
        valueTransformerWithKeySupplier.get();
        valueTransformerWithKeySupplier.get();
    }

    @Test
    public void testToInternalValueTransformerWithKeySupplierSuppliesNewTransformers() {
        final ValueTransformerWithKeySupplier<?, ?, ?> valueTransformerWithKeySupplier =
            mock(ValueTransformerWithKeySupplier.class);
        when(valueTransformerWithKeySupplier.get()).thenReturn(new NoopValueTransformerWithKey<>());
        valueTransformerWithKeySupplier.get();
        valueTransformerWithKeySupplier.get();
        valueTransformerWithKeySupplier.get();
    }

    @Test
    public void testShouldBeExtensible() {
        final StreamsBuilder builder = new StreamsBuilder();
        final int[] expectedKeys = new int[]{1, 2, 3, 4, 5, 6, 7};
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final String topicName = "topic";

        final ExtendedKStream<Integer, String> stream = new ExtendedKStream<>(builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.String())));

        stream.randomFilter().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build())) {

            final TestInputTopic<Integer, String> inputTopic = driver.createInputTopic(topicName, new IntegerSerializer(), new StringSerializer());
            for (final int expectedKey : expectedKeys) {
                inputTopic.pipeInput(expectedKey, "V" + expectedKey);
            }

            assertTrue(supplier.theCapturedProcessor().processed().size() <= expectedKeys.length);
        }
    }

    private static class ExtendedKStream<K, V> extends AbstractStream<K, V> {

        ExtendedKStream(final KStream<K, V> stream) {
            super((KStreamImpl<K, V>) stream);
        }

        KStream<K, V> randomFilter() {
            final String name = builder.newProcessorName("RANDOM-FILTER-");
            final ProcessorGraphNode<K, V> processorNode = new ProcessorGraphNode<>(
                name,
                new ProcessorParameters<>(new ExtendedKStreamDummy<>(), name));
            builder.addGraphNode(this.graphNode, processorNode);
            return new KStreamImpl<>(name, null, null, subTopologySourceNodes, false, processorNode, builder);
        }
    }

    private static class ExtendedKStreamDummy<K, V> implements ProcessorSupplier<K, V, K, V> {

        private final Random rand;

        ExtendedKStreamDummy() {
            rand = new Random();
        }

        @Override
        public Processor<K, V, K, V> get() {
            return new ExtendedKStreamDummyProcessor();
        }

        private class ExtendedKStreamDummyProcessor extends ContextualProcessor<K, V, K, V> {
            @Override
            public void process(final Record<K, V> record) {
                // flip a coin and filter
                if (rand.nextBoolean()) {
                    context().forward(record);
                }
            }
        }
    }
}
