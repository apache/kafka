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

import java.lang.reflect.Array;

import static org.junit.Assert.assertEquals;

public class KStreamBranchTest {

    private String topicName = "topic";

    private KStreamTestDriver driver = null;

    @After
    public void cleanup() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testKStreamBranch() {
        KStreamBuilder builder = new KStreamBuilder();
        builder.setApplicationId("X");

        Predicate<Integer, String> isEven = new Predicate<Integer, String>() {
            @Override
            public boolean test(Integer key, String value) {
                return (key % 2) == 0;
            }
        };
        Predicate<Integer, String> isMultipleOfThree = new Predicate<Integer, String>() {
            @Override
            public boolean test(Integer key, String value) {
                return (key % 3) == 0;
            }
        };
        Predicate<Integer, String> isOdd = new Predicate<Integer, String>() {
            @Override
            public boolean test(Integer key, String value) {
                return (key % 2) != 0;
            }
        };

        final int[] expectedKeys = new int[]{1, 2, 3, 4, 5, 6};

        KStream<Integer, String> stream;
        KStream<Integer, String>[] branches;
        MockProcessorSupplier<Integer, String>[] processors;

        stream = builder.stream(Serdes.Integer(), Serdes.String(), topicName);
        branches = stream.branch(isEven, isMultipleOfThree, isOdd);

        assertEquals(3, branches.length);

        processors = (MockProcessorSupplier<Integer, String>[]) Array.newInstance(MockProcessorSupplier.class, branches.length);
        for (int i = 0; i < branches.length; i++) {
            processors[i] = new MockProcessorSupplier<>();
            branches[i].process(processors[i]);
        }

        driver = new KStreamTestDriver(builder);
        for (int expectedKey : expectedKeys) {
            driver.process(topicName, expectedKey, "V" + expectedKey);
        }

        assertEquals(3, processors[0].processed.size());
        assertEquals(1, processors[1].processed.size());
        assertEquals(2, processors[2].processed.size());
    }

    @Test
    public void testTypeVariance() throws Exception {
        Predicate<Number, Object> positive = new Predicate<Number, Object>() {
            @Override
            public boolean test(Number key, Object value) {
                return key.doubleValue() > 0;
            }
        };

        Predicate<Number, Object> negative = new Predicate<Number, Object>() {
            @Override
            public boolean test(Number key, Object value) {
                return key.doubleValue() < 0;
            }
        };

        @SuppressWarnings("unchecked")
        final KStream<Integer, String>[] branches = new KStreamBuilder()
            .<Integer, String>stream("empty")
            .branch(positive, negative);
    }
}
