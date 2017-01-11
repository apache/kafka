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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KStreamFilterTest {

    private String topicName = "topic";

    private KStreamTestDriver driver = null;

    @After
    public void cleanup() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    private Predicate<Integer, String> isMultipleOfThree = new Predicate<Integer, String>() {
        @Override
        public boolean test(Integer key, String value) {
            return (key % 3) == 0;
        }
    };

    @Test
    public void testFilter() {
        KStreamBuilder builder = new KStreamBuilder();
        final int[] expectedKeys = new int[]{1, 2, 3, 4, 5, 6, 7};

        KStream<Integer, String> stream;
        MockProcessorSupplier<Integer, String> processor;

        processor = new MockProcessorSupplier<>();
        stream = builder.stream(Serdes.Integer(), Serdes.String(), topicName);
        stream.filter(isMultipleOfThree).process(processor);

        driver = new KStreamTestDriver(builder);
        for (int expectedKey : expectedKeys) {
            driver.process(topicName, expectedKey, "V" + expectedKey);
        }

        assertEquals(2, processor.processed.size());
    }

    @Test
    public void testFilterNot() {
        KStreamBuilder builder = new KStreamBuilder();
        final int[] expectedKeys = new int[]{1, 2, 3, 4, 5, 6, 7};

        KStream<Integer, String> stream;
        MockProcessorSupplier<Integer, String> processor;

        processor = new MockProcessorSupplier<>();
        stream = builder.stream(Serdes.Integer(), Serdes.String(), topicName);
        stream.filterNot(isMultipleOfThree).process(processor);

        driver = new KStreamTestDriver(builder);
        for (int expectedKey : expectedKeys) {
            driver.process(topicName, expectedKey, "V" + expectedKey);
        }

        assertEquals(5, processor.processed.size());
    }

    @Test
    public void testTypeVariance() throws Exception {
        Predicate<Number, Object> numberKeyPredicate = new Predicate<Number, Object>() {
            @Override
            public boolean test(Number key, Object value) {
                return false;
            }
        };

        new KStreamBuilder()
            .<Integer, String>stream("empty")
            .filter(numberKeyPredicate)
            .filterNot(numberKeyPredicate)
            .to("nirvana");
    }
}
