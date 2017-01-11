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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KStreamMapValuesTest {

    private String topicName = "topic";

    final private Serde<Integer> intSerde = Serdes.Integer();
    final private Serde<String> stringSerde = Serdes.String();

    private KStreamTestDriver driver;

    @After
    public void cleanup() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Test
    public void testFlatMapValues() {
        KStreamBuilder builder = new KStreamBuilder();

        ValueMapper<CharSequence, Integer> mapper =
            new ValueMapper<CharSequence, Integer>() {
                @Override
                public Integer apply(CharSequence value) {
                    return value.length();
                }
            };

        final int[] expectedKeys = {1, 10, 100, 1000};

        KStream<Integer, String> stream;
        MockProcessorSupplier<Integer, Integer> processor = new MockProcessorSupplier<>();
        stream = builder.stream(intSerde, stringSerde, topicName);
        stream.mapValues(mapper).process(processor);

        driver = new KStreamTestDriver(builder);
        for (int expectedKey : expectedKeys) {
            driver.process(topicName, expectedKey, Integer.toString(expectedKey));
        }

        assertEquals(4, processor.processed.size());

        String[] expected = {"1:1", "10:2", "100:3", "1000:4"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processor.processed.get(i));
        }
    }

}
