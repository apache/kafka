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
import org.apache.kafka.streaming.kstream.WindowDef;
import org.apache.kafka.streaming.kstream.internals.KStreamSource;
import org.apache.kafka.test.MockKStreamBuilder;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.UnlimitedWindow;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;

public class KStreamWindowedTest {

    private String topicName = "topic";

    private KStreamBuilder topology = new MockKStreamBuilder();
    private IntegerDeserializer keyDeserializer = new IntegerDeserializer();
    private StringDeserializer valDeserializer = new StringDeserializer();

    @Test
    public void testWindowedStream() {

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        KStream<Integer, String> stream;
        WindowDef<Integer, String> window;

        window = new UnlimitedWindow<>();
        stream = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
        stream.with(window);

        MockProcessorContext context = new MockProcessorContext(null, null);
        topology.init(context);
        context.setTime(0L);

        // two items in the window

        for (int i = 0; i < 2; i++) {
            ((KStreamSource<Integer, String>) stream).source().process(expectedKeys[i], "V" + expectedKeys[i]);
        }

        assertEquals(1, countItem(window.find(0, 0L)));
        assertEquals(1, countItem(window.find(1, 0L)));
        assertEquals(0, countItem(window.find(2, 0L)));
        assertEquals(0, countItem(window.find(3, 0L)));

        // previous two items + all items, thus two are duplicates, in the window

        for (int i = 0; i < expectedKeys.length; i++) {
            ((KStreamSource<Integer, String>) stream).source().process(expectedKeys[i], "Y" + expectedKeys[i]);
        }

        assertEquals(2, countItem(window.find(0, 0L)));
        assertEquals(2, countItem(window.find(1, 0L)));
        assertEquals(1, countItem(window.find(2, 0L)));
        assertEquals(1, countItem(window.find(3, 0L)));
    }


    private <T> int countItem(Iterator<T> iter) {
        int i = 0;
        while (iter.hasNext()) {
            i++;
            iter.next();
        }
        return i;
    }
}
