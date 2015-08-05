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

package org.apache.kafka.stream;

import org.apache.kafka.stream.internals.PartitioningInfo;
import org.apache.kafka.stream.topology.KStreamTopology;
import org.apache.kafka.stream.topology.Window;
import org.apache.kafka.stream.topology.internals.KStreamMetadata;
import org.apache.kafka.stream.topology.internals.KStreamSource;
import org.apache.kafka.test.MockKStreamContext;
import org.apache.kafka.test.MockKStreamTopology;
import org.apache.kafka.test.UnlimitedWindow;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

public class KStreamWindowedTest {

    private String topicName = "topic";

    private KStreamMetadata streamMetadata = new KStreamMetadata(Collections.singletonMap(topicName, new PartitioningInfo(1)));

    @Test
    public void testWindowedStream() {

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

<<<<<<< HEAD
    KStreamSource<Integer, String> stream;
    Window<Integer, String> window;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
    KStreamTopology initializer = new MockKStreamTopology();
=======
    KStreamInitializer initializer = new KStreamInitializerImpl(null, null, null, null) {
    };
>>>>>>> new api model
=======
    KStreamInitializer initializer = new KStreamInitializerImpl();
>>>>>>> wip
=======
    KStreamTopology initializer = new MockKStreamTopology();
>>>>>>> wip
=======
        KStreamSource<Integer, String> stream;
        Window<Integer, String> window;
        KStreamTopology initializer = new MockKStreamTopology();
>>>>>>> compile and test passed

        window = new UnlimitedWindow<>();
        stream = new KStreamSource<>(null, initializer);
        stream.with(window);

        boolean exceptionRaised = false;

        KStreamContext context = new MockKStreamContext(null, null);
        stream.bind(context, streamMetadata);

        // two items in the window

        for (int i = 0; i < 2; i++) {
            stream.receive(expectedKeys[i], "V" + expectedKeys[i], 0L);
        }

        assertEquals(1, countItem(window.find(0, 0L)));
        assertEquals(1, countItem(window.find(1, 0L)));
        assertEquals(0, countItem(window.find(2, 0L)));
        assertEquals(0, countItem(window.find(3, 0L)));

        // previous two items + all items, thus two are duplicates, in the window

        for (int i = 0; i < expectedKeys.length; i++) {
            stream.receive(expectedKeys[i], "Y" + expectedKeys[i], 0L);
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
