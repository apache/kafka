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
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.test.KStreamTestDriver;
import org.junit.Test;
import java.util.List;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class KStreamForeachTest {

    private String topicName = "topic";

    final private Serde<Integer> intSerde = Serdes.Integer();
    final private Serde<String> stringSerde = Serdes.String();

    @Test
    public void testForeach() {
        KStreamBuilder builder = new KStreamBuilder();
        final List<String> valueList = new ArrayList<>();
        ForeachAction<Integer, String> action =
            new ForeachAction<Integer, String>() {
                @Override
                public void apply(Integer key, String value) {
                    valueList.add(value);
                }
            };
        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        KStream<Integer, String> stream = builder.stream(intSerde, stringSerde, topicName);
        stream.foreach(action);

        KStreamTestDriver driver = new KStreamTestDriver(builder);
        for (int i = 0; i < expectedKeys.length; i++) {
            driver.process(topicName, expectedKeys[i], "V" + expectedKeys[i]);
        }

        String[] expected = new String[]{"V0", "V1", "V2", "V3"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], valueList.get(i));
        }
    }
}
