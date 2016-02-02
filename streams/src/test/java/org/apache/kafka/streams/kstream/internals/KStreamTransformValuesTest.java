/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KStreamTransformValuesTest {

    private String topicName = "topic";

    @Test
    public void testTransform() {
        KStreamBuilder builder = new KStreamBuilder();

        builder.register(Integer.class, new IntegerDeserializer());

        ValueTransformerSupplier<Integer, Integer> valueTransformerSupplier =
            new ValueTransformerSupplier<Integer, Integer>() {
                public ValueTransformer<Integer, Integer> get() {
                    return new ValueTransformer<Integer, Integer>() {

                        private int total = 0;

                        @Override
                        public void init(ProcessorContext context) {
                        }

                        @Override
                        public Integer transform(Integer value) {
                            total += value;
                            return total;
                        }

                        @Override
                        public void punctuate(long timestamp) {
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
        stream = builder.stream(Integer.class, Integer.class, topicName);
        stream.transformValues(valueTransformerSupplier).process(processor);

        KStreamTestDriver driver = new KStreamTestDriver(builder);
        for (int i = 0; i < expectedKeys.length; i++) {
            driver.process(topicName, expectedKeys[i], expectedKeys[i] * 10);
        }

        assertEquals(4, processor.processed.size());

        String[] expected = {"1:10", "10:110", "100:1110", "1000:11110"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processor.processed.get(i));
        }
    }

}
