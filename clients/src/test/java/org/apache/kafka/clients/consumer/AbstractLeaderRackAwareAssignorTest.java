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
package org.apache.kafka.clients.consumer;


import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

public abstract class AbstractLeaderRackAwareAssignorTest {

    private String topic = "test-leader-rack--aware-topic";
    public abstract AbstractPartitionAssignor getAssignor();
    @Test
    public void testFullBalancedLeaderRackAssign() {
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, Arrays.asList(
                new PartitionInfo(topic, 0,
                        new Node(0, "node-0", 8888, "rack-a"),
                        new Node[]{
                            new Node(1, "node-1", 8888, "rack-b"),
                            new Node(2, "node-2", 8888, "rack-c")
                        }, null),
                new PartitionInfo(topic, 1,
                        new Node(0, "node-0", 8888, "rack-a"),
                        new Node[]{
                            new Node(1, "node-1", 8888, "rack-b"),
                            new Node(2, "node-2", 8888, "rack-c")
                        }, null),
                new PartitionInfo(topic, 2,
                        new Node(1, "node-1", 8888, "rack-b"),
                        new Node[]{
                            new Node(0, "node-0", 8888, "rack-a"),
                            new Node(2, "node-2", 8888, "rack-c")
                        }, null),
                new PartitionInfo(topic, 3,
                        new Node(1, "node-1", 8888, "rack-b"),
                        new Node[]{
                            new Node(0, "node-0", 8888, "rack-a"),
                            new Node(2, "node-2", 8888, "rack-c")
                        }, null),
                new PartitionInfo(topic, 4,
                        new Node(2, "node-2", 8888, "rack-c"),
                        new Node[]{
                            new Node(0, "node-0", 8888, "rack-a"),
                            new Node(1, "node-1", 8888, "rack-b")
                        }, null),
                new PartitionInfo(topic, 5,
                        new Node(1, "node-1", 8888, "rack-b"),
                        new Node[]{
                            new Node(0, "node-0", 8888, "rack-a"),
                            new Node(2, "node-2", 8888, "rack-c")
                        }, null)
        ));

        // Simulate two consumers subscribing to the topic
        Map<String, Subscription> subscriptions = new HashMap<>();
        subscriptions.put("consumer-a",
                new Subscription(Collections.singletonList(topic), null, Collections.emptyList(), 1, Optional.of("rack-a")));
        subscriptions.put("consumer-b",
                new Subscription(Collections.singletonList(topic), null, Collections.emptyList(), 1, Optional.of("rack-b")));
        subscriptions.put("consumer-c",
                new Subscription(Collections.singletonList(topic), null, Collections.emptyList(), 1, Optional.of("rack-c")));

        // Call the assignPartitions method
        Map<String, List<TopicPartition>> assignment = getAssignor().assignPartitions(partitionsPerTopic,
                subscriptions);

        assertEquals(assignment.get("consumer-a"), Arrays.asList(new TopicPartition(topic, 0), new TopicPartition(topic, 1)));
        assertEquals(assignment.get("consumer-b"), Arrays.asList(new TopicPartition(topic, 2), new TopicPartition(topic, 3)));
        assertEquals(assignment.get("consumer-c"), Arrays.asList(new TopicPartition(topic, 4), new TopicPartition(topic, 5)));

    }
    @Test
    public void testUnbalancedLeaderRackAssign() {
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, Arrays.asList(
                new PartitionInfo(topic, 0,
                        new Node(0, "node-0", 8888, "rack-a"),
                        new Node[]{
                            new Node(1, "node-1", 8888, "rack-b"),
                            new Node(2, "node-2", 8888, "rack-c")
                        }, null),
                new PartitionInfo(topic, 1,
                        new Node(0, "node-0", 8888, "rack-a"),
                        new Node[]{
                            new Node(1, "node-1", 8888, "rack-b"),
                            new Node(2, "node-2", 8888, "rack-c")
                        }, null),
                new PartitionInfo(topic, 2,
                        new Node(1, "node-1", 8888, "rack-b"),
                        new Node[]{
                            new Node(0, "node-0", 8888, "rack-a"),
                            new Node(2, "node-2", 8888, "rack-c")
                        }, null),
                new PartitionInfo(topic, 3,
                        new Node(1, "node-1", 8888, "rack-b"),
                        new Node[]{
                            new Node(0, "node-0", 8888, "rack-a"),
                            new Node(2, "node-2", 8888, "rack-c")
                        }, null),
                new PartitionInfo(topic, 4,
                        new Node(2, "node-2", 8888, "rack-c"),
                        new Node[]{
                            new Node(0, "node-0", 8888, "rack-a"),
                            new Node(1, "node-1", 8888, "rack-b")
                        }, null),
                new PartitionInfo(topic, 5,
                        new Node(1, "node-1", 8888, "rack-c"),
                        new Node[]{
                            new Node(0, "node-0", 8888, "rack-a"),
                            new Node(2, "node-2", 8888, "rack-b")
                        }, null)
        ));

        // Simulate two consumers subscribing to the topic
        Map<String, Subscription> subscriptions = new HashMap<>();
        subscriptions.put("consumer-a",
                new Subscription(Collections.singletonList(topic), null, Collections.emptyList(), 1, Optional.of("rack-a")));
        subscriptions.put("consumer-b",
                new Subscription(Collections.singletonList(topic), null, Collections.emptyList(), 1, Optional.of("rack-b")));

        // Call the assignPartitions method
        Map<String, List<TopicPartition>> assignment = getAssignor().assignPartitions(partitionsPerTopic,
                subscriptions);

        assertEquals(assignment.get("consumer-a"), Arrays.asList(new TopicPartition(topic, 0), new TopicPartition(topic, 1), new TopicPartition(topic, 4)));
        assertEquals(assignment.get("consumer-b"), Arrays.asList(new TopicPartition(topic, 2), new TopicPartition(topic, 3), new TopicPartition(topic, 5)));

    }

}
