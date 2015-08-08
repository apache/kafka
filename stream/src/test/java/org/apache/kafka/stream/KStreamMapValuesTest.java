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

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
import io.confluent.streaming.KStreamContext;
<<<<<<< HEAD
<<<<<<< HEAD
import io.confluent.streaming.KStreamTopology;
=======
import io.confluent.streaming.KStreamInitializer;
>>>>>>> new api model
=======
import io.confluent.streaming.KStreamTopology;
>>>>>>> wip
import io.confluent.streaming.ValueMapper;
import io.confluent.streaming.testutil.MockKStreamContext;
import io.confluent.streaming.testutil.MockKStreamTopology;
import io.confluent.streaming.testutil.TestProcessor;
import org.apache.kafka.clients.processor.KStreamContext;
=======
import org.apache.kafka.stream.internal.PartitioningInfo;
=======
import org.apache.kafka.stream.internals.PartitioningInfo;
>>>>>>> compile and test passed
=======
import org.apache.kafka.clients.processor.internals.PartitioningInfo;
<<<<<<< HEAD
>>>>>>> wip
import org.apache.kafka.stream.topology.KStreamTopology;
import org.apache.kafka.stream.topology.ValueMapper;
import org.apache.kafka.stream.topology.internals.KStreamMetadata;
import org.apache.kafka.stream.topology.internals.KStreamSource;
=======
import org.apache.kafka.stream.internals.KStreamMetadata;
import org.apache.kafka.stream.internals.KStreamSource;
>>>>>>> wip
import org.apache.kafka.test.MockKStreamTopology;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockKStreamContext;
>>>>>>> removing io.confluent imports: wip
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class KStreamMapValuesTest {

    private String topicName = "topic";

    private KStreamMetadata streamMetadata = new KStreamMetadata(Collections.singletonMap(topicName, new PartitioningInfo(1)));

    @Test
    public void testFlatMapValues() {

        ValueMapper<Integer, String> mapper =
            new ValueMapper<Integer, String>() {
                @Override
                public Integer apply(String value) {
                    return value.length();
                }
            };

        final int[] expectedKeys = new int[]{1, 10, 100, 1000};

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
    KStreamTopology initializer = new MockKStreamTopology();
=======
    KStreamInitializer initializer = new KStreamInitializerImpl(null, null, null, null);
>>>>>>> new api model
=======
    KStreamInitializer initializer = new KStreamInitializerImpl();
>>>>>>> wip
=======
    KStreamTopology initializer = new MockKStreamTopology();
>>>>>>> wip
    KStreamSource<Integer, String> stream;
    MockProcessor<Integer, Integer> processor;
=======
        KStreamTopology initializer = new MockKStreamTopology();
        KStreamSource<Integer, String> stream;
        MockProcessor<Integer, Integer> processor;
>>>>>>> compile and test passed

        processor = new MockProcessor<>();
        stream = new KStreamSource<>(null, initializer);
        stream.mapValues(mapper).process(processor);

        KStreamContext context = new MockKStreamContext(null, null);
        stream.bind(context, streamMetadata);
        for (int i = 0; i < expectedKeys.length; i++) {
            stream.receive(expectedKeys[i], Integer.toString(expectedKeys[i]), 0L);
        }

        assertEquals(4, processor.processed.size());

        String[] expected = new String[]{"1:1", "10:2", "100:3", "1000:4"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processor.processed.get(i));
        }
    }

}
