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

package org.apache.kafka.streaming.kstream.internals;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streaming.kstream.KStream;
import org.apache.kafka.streaming.kstream.KStreamBuilder;
import org.apache.kafka.streaming.kstream.ValueMapper;
import org.apache.kafka.streaming.kstream.internals.KStreamSource;
import org.apache.kafka.test.MockKStreamBuilder;
import org.apache.kafka.test.MockProcessor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KStreamMapValuesTest {

    private String topicName = "topic";

    private KStreamBuilder topology = new MockKStreamBuilder();
    private IntegerDeserializer keyDeserializer = new IntegerDeserializer();
    private StringDeserializer valDeserializer = new StringDeserializer();

    @Test
    public void testFlatMapValues() {

        ValueMapper<String, Integer> mapper =
            new ValueMapper<String, Integer>() {
                @Override
                public Integer apply(String value) {
                    return value.length();
                }
            };

        final int[] expectedKeys = new int[]{1, 10, 100, 1000};

        KStream<Integer, String> stream;
        MockProcessor<Integer, Integer> processor = new MockProcessor<>();
        stream = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
        stream.mapValues(mapper).process(processor);

        for (int i = 0; i < expectedKeys.length; i++) {
            ((KStreamSource<Integer, String>) stream).source().process(expectedKeys[i], Integer.toString(expectedKeys[i]));
        }

        assertEquals(4, processor.processed.size());

        String[] expected = new String[]{"1:1", "10:2", "100:3", "1000:4"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processor.processed.get(i));
        }
    }

}
