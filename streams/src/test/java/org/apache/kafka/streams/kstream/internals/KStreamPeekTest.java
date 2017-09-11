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
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.test.KStreamTestDriver;

import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class KStreamPeekTest {

    private final String topicName = "topic";
    private final Serde<Integer> intSerd = Serdes.Integer();
    private final Serde<String> stringSerd = Serdes.String();
    @Rule
    public final KStreamTestDriver driver = new KStreamTestDriver();

    @Test
    public void shouldObserveStreamElements() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Integer, String> stream = builder.stream(topicName, Consumed.with(intSerd, stringSerd));
        final List<KeyValue<Integer, String>> peekObserved = new ArrayList<>(), streamObserved = new ArrayList<>();
        stream.peek(collect(peekObserved)).foreach(collect(streamObserved));

        driver.setUp(builder);
        final List<KeyValue<Integer, String>> expected = new ArrayList<>();
        for (int key = 0; key < 32; key++) {
            final String value = "V" + key;
            driver.process(topicName, key, value);
            expected.add(new KeyValue<>(key, value));
        }

        assertEquals(expected, peekObserved);
        assertEquals(expected, streamObserved);
    }

    @Test
    public void shouldNotAllowNullAction() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Integer, String> stream = builder.stream(topicName, Consumed.with(intSerd, stringSerd));
        try {
            stream.peek(null);
            fail("expected null action to throw NPE");
        } catch (NullPointerException expected) {
            // do nothing
        }
    }

    private static <K, V> ForeachAction<K, V> collect(final List<KeyValue<K, V>> into) {
        return new ForeachAction<K, V>() {
            @Override
            public void apply(final K key, final V value) {
                into.add(new KeyValue<>(key, value));
            }
        };
    }
}
