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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.Rule;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertTrue;

public class AbstractStreamTest {

    private final String topicName = "topic";
    @Rule
    public final KStreamTestDriver driver = new KStreamTestDriver();

    @Test
    public void testShouldBeExtensible() {
        final StreamsBuilder builder = new StreamsBuilder();
        final int[] expectedKeys = new int[]{1, 2, 3, 4, 5, 6, 7};
        final MockProcessorSupplier<Integer, String> processor = new MockProcessorSupplier<>();
        final String topicName = "topic";

        ExtendedKStream<Integer, String> stream = new ExtendedKStream<>(builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.String())));

        stream.randomFilter().process(processor);

        driver.setUp(builder);
        for (int expectedKey : expectedKeys) {
            driver.process(topicName, expectedKey, "V" + expectedKey);
        }

        assertTrue(processor.processed.size() <= expectedKeys.length);
    }

    private class ExtendedKStream<K, V> extends AbstractStream<K> {

        ExtendedKStream(final KStream<K, V> stream) {
            super((KStreamImpl<K, V>) stream);
        }

        KStream<K, V> randomFilter() {
            String name = builder.newProcessorName("RANDOM-FILTER-");
            builder.internalTopologyBuilder.addProcessor(name, new ExtendedKStreamDummy(), this.name);
            return new KStreamImpl<>(builder, name, sourceNodes, false);
        }
    }

    private class ExtendedKStreamDummy<K, V> implements ProcessorSupplier<K, V> {

        private Random rand;

        ExtendedKStreamDummy() {
            rand = new Random();
        }

        @Override
        public Processor<K, V> get() {
            return new ExtendedKStreamDummyProcessor();
        }

        private class ExtendedKStreamDummyProcessor extends AbstractProcessor<K, V> {
            @Override
            public void process(K key, V value) {
                // flip a coin and filter
                if (rand.nextBoolean())
                    context().forward(key, value);
            }
        }
    }
}
