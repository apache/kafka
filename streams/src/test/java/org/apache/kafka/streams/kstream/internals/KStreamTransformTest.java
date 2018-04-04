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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KStreamTransformTest {

    private String topicName = "topic";

    final private Serde<Integer> intSerde = Serdes.Integer();
    @Rule
    public final KStreamTestDriver driver = new KStreamTestDriver();

    @Test
    public void testTransform() {
        StreamsBuilder builder = new StreamsBuilder();

        TransformerSupplier<Number, Number, KeyValue<Integer, Integer>> transformerSupplier =
            ()-> { return new Transformer<Number, Number, KeyValue<Integer, Integer>>() {

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
                    };};

        final int[] expectedKeys = {1, 10, 100, 1000};

        MockProcessorSupplier<Integer, Integer> processor = new MockProcessorSupplier<>();
        KStream<Integer, Integer> stream = builder.stream(topicName, Consumed.with(intSerde, intSerde));
        stream.transform(transformerSupplier).process(processor);

        driver.setUp(builder);
        for (int expectedKey : expectedKeys) {
            driver.process(topicName, expectedKey, expectedKey * 10);
        }

        driver.punctuate(2);
        driver.punctuate(3);

        assertEquals(6, processor.processed.size());

        String[] expected = {"2:10", "20:110", "200:1110", "2000:11110", "-1:2", "-1:3"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processor.processed.get(i));
        }
    }

}
