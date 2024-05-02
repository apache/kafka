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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.TimeWindowedSerializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class WindowedStreamPartitionerTest {

    private String topicName = "topic";

    private IntegerSerializer intSerializer = new IntegerSerializer();
    private StringSerializer stringSerializer = new StringSerializer();

    private List<PartitionInfo> infos = Arrays.asList(
            new PartitionInfo(topicName, 0, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo(topicName, 1, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo(topicName, 2, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo(topicName, 3, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo(topicName, 4, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo(topicName, 5, Node.noNode(), new Node[0], new Node[0])
    );

    private Cluster cluster = new Cluster("cluster", Collections.singletonList(Node.noNode()), infos,
            Collections.emptySet(), Collections.emptySet());

    @Test
    public void testCopartitioning() {
        final Random rand = new Random();
        @SuppressWarnings("deprecation")
        final org.apache.kafka.clients.producer.internals.DefaultPartitioner defaultPartitioner = new org.apache.kafka.clients.producer.internals.DefaultPartitioner();
        final WindowedSerializer<Integer> timeWindowedSerializer = new TimeWindowedSerializer<>(intSerializer);
        final WindowedStreamPartitioner<Integer, String> streamPartitioner = new WindowedStreamPartitioner<>(timeWindowedSerializer);

        for (int k = 0; k < 10; k++) {
            final Integer key = rand.nextInt();
            final byte[] keyBytes = intSerializer.serialize(topicName, key);

            final String value = key.toString();
            final byte[] valueBytes = stringSerializer.serialize(topicName, value);

            final Integer expected = defaultPartitioner.partition("topic", key, keyBytes, value, valueBytes, cluster);

            for (int w = 1; w < 10; w++) {
                final TimeWindow window = new TimeWindow(10 * w, 20 * w);

                final Windowed<Integer> windowedKey = new Windowed<>(key, window);
                @SuppressWarnings("deprecation")
                final Integer actual = streamPartitioner.partition(topicName, windowedKey, value, infos.size());

                assertEquals(expected, actual);
            }
        }

        defaultPartitioner.close();
    }
}
