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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KStreamWindowed;
import org.apache.kafka.streams.kstream.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorDef;
import org.apache.kafka.test.UnlimitedWindowDef;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KStreamJoinTest {

    private String topic1 = "topic1";
    private String topic2 = "topic2";

    private IntegerDeserializer keyDeserializer = new IntegerDeserializer();
    private StringDeserializer valDeserializer = new StringDeserializer();

    private ValueJoiner<String, String, String> joiner = new ValueJoiner<String, String, String>() {
        @Override
        public String apply(String value1, String value2) {
            return value1 + "+" + value2;
        }
    };

    private ValueMapper<String, String> valueMapper = new ValueMapper<String, String>() {
        @Override
        public String apply(String value) {
            return "#" + value;
        }
    };

    private ValueMapper<String, Iterable<String>> valueMapper2 = new ValueMapper<String, Iterable<String>>() {
        @Override
        public Iterable<String> apply(String value) {
            return (Iterable<String>) Utils.mkSet(value);
        }
    };

    private KeyValueMapper<Integer, String, KeyValue<Integer, String>> keyValueMapper =
        new KeyValueMapper<Integer, String, KeyValue<Integer, String>>() {
            @Override
            public KeyValue<Integer, String> apply(Integer key, String value) {
                return KeyValue.pair(key, value);
            }
        };

    KeyValueMapper<Integer, String, KeyValue<Integer, Iterable<String>>> keyValueMapper2 =
        new KeyValueMapper<Integer, String, KeyValue<Integer, Iterable<String>>>() {
            @Override
            public KeyValue<Integer, Iterable<String>> apply(Integer key, String value) {
                return KeyValue.pair(key, (Iterable<String>) Utils.mkSet(value));
            }
        };


    @Test
    public void testJoin() {
        KStreamBuilder builder = new KStreamBuilder();

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        KStream<Integer, String> stream1;
        KStream<Integer, String> stream2;
        KStreamWindowed<Integer, String> windowed1;
        KStreamWindowed<Integer, String> windowed2;
        MockProcessorDef<Integer, String> processor;
        String[] expected;

        processor = new MockProcessorDef<>();
        stream1 = builder.from(keyDeserializer, valDeserializer, topic1);
        stream2 = builder.from(keyDeserializer, valDeserializer, topic2);
        windowed1 = stream1.with(new UnlimitedWindowDef<Integer, String>("window1"));
        windowed2 = stream2.with(new UnlimitedWindowDef<Integer, String>("window2"));

        windowed1.join(windowed2, joiner).process(processor);

        KStreamTestDriver driver = new KStreamTestDriver(builder);
        driver.setTime(0L);

        // push two items to the main stream. the other stream's window is empty

        for (int i = 0; i < 2; i++) {
            driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
        }

        assertEquals(0, processor.processed.size());

        // push two items to the other stream. the main stream's window has two items

        for (int i = 0; i < 2; i++) {
            driver.process(topic2, expectedKeys[i], "Y" + expectedKeys[i]);
        }

        assertEquals(2, processor.processed.size());

        expected = new String[]{"0:X0+Y0", "1:X1+Y1"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processor.processed.get(i));
        }

        processor.processed.clear();

        // push all items to the main stream. this should produce two items.

        for (int i = 0; i < expectedKeys.length; i++) {
            driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
        }

        assertEquals(2, processor.processed.size());

        expected = new String[]{"0:X0+Y0", "1:X1+Y1"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processor.processed.get(i));
        }

        processor.processed.clear();

        // there will be previous two items + all items in the main stream's window, thus two are duplicates.

        // push all items to the other stream. this should produce 6 items
        for (int i = 0; i < expectedKeys.length; i++) {
            driver.process(topic2, expectedKeys[i], "Y" + expectedKeys[i]);
        }

        assertEquals(6, processor.processed.size());

        expected = new String[]{"0:X0+Y0", "0:X0+Y0", "1:X1+Y1", "1:X1+Y1", "2:X2+Y2", "3:X3+Y3"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processor.processed.get(i));
        }
    }

    // TODO: test for joinability
}
