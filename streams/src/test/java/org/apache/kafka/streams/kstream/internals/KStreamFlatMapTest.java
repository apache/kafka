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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

public class KStreamFlatMapTest {

    private String topicName = "topic";

    private KStreamTestDriver driver = null;

    @After
    public void cleanup() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Test
    public void testFlatMap() {
        KStreamBuilder builder = new KStreamBuilder();

        KeyValueMapper<Integer, String, Iterable<KeyValue<String, String>>> mapper =
            new KeyValueMapper<Integer, String, Iterable<KeyValue<String, String>>>() {
                @Override
                public Iterable<KeyValue<String, String>> apply(Integer key, String value) {
                    ArrayList<KeyValue<String, String>> result = new ArrayList<>();
                    for (int i = 0; i < key; i++) {
                        result.add(KeyValue.pair(Integer.toString(key * 10 + i), value));
                    }
                    return result;
                }
            };

        final int[] expectedKeys = {0, 1, 2, 3};

        KStream<Integer, String> stream;
        MockProcessorSupplier<String, String> processor;

        processor = new MockProcessorSupplier<>();
        stream = builder.stream(Serdes.Integer(), Serdes.String(), topicName);
        stream.flatMap(mapper).process(processor);

        driver = new KStreamTestDriver(builder);
        for (int i = 0; i < expectedKeys.length; i++) {
            driver.process(topicName, expectedKeys[i], "V" + expectedKeys[i]);
        }

        assertEquals(6, processor.processed.size());

        String[] expected = {"10:V1", "20:V2", "21:V2", "30:V3", "31:V3", "32:V3"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processor.processed.get(i));
        }
    }

}