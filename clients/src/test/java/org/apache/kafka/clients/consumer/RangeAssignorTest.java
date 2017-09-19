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

import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Subscription;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RangeAssignorTest {

    private RangeAssignor assignor = new RangeAssignor();


    @Test
    public void testOneConsumerNoTopic() {
        String consumerId = "consumer";

        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumerId, new Subscription(Collections.<String>emptyList())));

        assertEquals(Collections.singleton(consumerId), assignment.keySet());
        assertTrue(assignment.get(consumerId).isEmpty());
    }

    @Test
    public void testOneConsumerNonexistentTopic() {
        String topic = "topic";
        String consumerId = "consumer";

        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumerId, new Subscription(topics(topic))));
        assertEquals(Collections.singleton(consumerId), assignment.keySet());
        assertTrue(assignment.get(consumerId).isEmpty());
    }

    @Test
    public void testOneConsumerOneTopic() {
        String topic = "topic";
        String consumerId = "consumer";

        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        List<PartitionInfo> partitionInfos = new ArrayList<>(3);
        for (int i = 0; i < 3; i++) {
            partitionInfos.add(new PartitionInfo(topic, i, null, null, null));
        }
        partitionsPerTopic.put(topic, partitionInfos);

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumerId, new Subscription(topics(topic))));

        assertEquals(Collections.singleton(consumerId), assignment.keySet());
        assertAssignment(partitions(tp(topic, 0), tp(topic, 1), tp(topic, 2)), assignment.get(consumerId));
    }

    @Test
    public void testOnlyAssignsPartitionsFromSubscribedTopics() {
        String topic = "topic";
        String otherTopic = "other";
        String consumerId = "consumer";

        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        List<PartitionInfo> partitionInfos1 = new ArrayList<>(3);
        List<PartitionInfo> partitionInfos2 = new ArrayList<>(3);
        for (int i = 0; i < 3; i++) {
            partitionInfos1.add(new PartitionInfo(topic, i, null, null, null));
            partitionInfos2.add(new PartitionInfo(topic, i, null, null, null));
        }
        partitionsPerTopic.put(topic, partitionInfos1);
        partitionsPerTopic.put(otherTopic, partitionInfos2);

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumerId, new Subscription(topics(topic))));
        assertEquals(Collections.singleton(consumerId), assignment.keySet());
        assertAssignment(partitions(tp(topic, 0), tp(topic, 1), tp(topic, 2)), assignment.get(consumerId));
    }

    @Test
    public void testOneConsumerMultipleTopics() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumerId = "consumer";

        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        List<PartitionInfo> partitionInfos1 = new ArrayList<>(1);
        List<PartitionInfo> partitionInfos2 = new ArrayList<>(2);
        partitionInfos1.add(new PartitionInfo(topic1, 0, null, null, null));

        for (int i = 0; i < 2; ++i) {
            partitionInfos2.add(new PartitionInfo(topic2, i, null, null, null));
        }

        partitionsPerTopic.put(topic1, partitionInfos1);
        partitionsPerTopic.put(topic2, partitionInfos2);

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumerId, new Subscription(topics(topic1, topic2))));

        assertEquals(Collections.singleton(consumerId), assignment.keySet());
        assertAssignment(partitions(tp(topic1, 0), tp(topic2, 0), tp(topic2, 1)), assignment.get(consumerId));
    }

    @Test
    public void testTwoConsumersOneTopicOnePartition() {
        String topic = "topic";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";

        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        List<PartitionInfo> partitionInfos = new ArrayList<>(1);
        partitionInfos.add(new PartitionInfo(topic, 0, null, null, null));
        partitionsPerTopic.put(topic, partitionInfos);

        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(consumer1, new Subscription(topics(topic)));
        consumers.put(consumer2, new Subscription(topics(topic)));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertAssignment(partitions(tp(topic, 0)), assignment.get(consumer1));
        assertAssignment(Collections.<TopicPartition>emptyList(), assignment.get(consumer2));
    }


    @Test
    public void testTwoConsumersOneTopicTwoPartitions() {
        String topic = "topic";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";

        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        List<PartitionInfo> partitionInfos = new ArrayList<>(2);
        for (int i = 0; i < 2; ++i) {
            partitionInfos.add(new PartitionInfo(topic, i, null, null, null));
        }

        partitionsPerTopic.put(topic, partitionInfos);

        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(consumer1, new Subscription(topics(topic)));
        consumers.put(consumer2, new Subscription(topics(topic)));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertAssignment(partitions(tp(topic, 0)), assignment.get(consumer1));
        assertAssignment(partitions(tp(topic, 1)), assignment.get(consumer2));
    }

    @Test
    public void testMultipleConsumersMixedTopics() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";
        String consumer3 = "consumer3";

        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        List<PartitionInfo> partitionInfos1 = new ArrayList<>(3);
        List<PartitionInfo> partitionInfos2 = new ArrayList<>(3);
        for (int i = 0; i < 3; i++) {
            partitionInfos1.add(new PartitionInfo(topic1, i, null, null, null));
            if (i != 2) {
                partitionInfos2.add(new PartitionInfo(topic2, i, null, null, null));
            }
        }

        partitionsPerTopic.put(topic1, partitionInfos1);
        partitionsPerTopic.put(topic2, partitionInfos2);

        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(consumer1, new Subscription(topics(topic1)));
        consumers.put(consumer2, new Subscription(topics(topic1, topic2)));
        consumers.put(consumer3, new Subscription(topics(topic1)));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertAssignment(partitions(tp(topic1, 0)), assignment.get(consumer1));
        assertAssignment(partitions(tp(topic1, 1), tp(topic2, 0), tp(topic2, 1)), assignment.get(consumer2));
        assertAssignment(partitions(tp(topic1, 2)), assignment.get(consumer3));
    }

    @Test
    public void testTwoConsumersTwoTopicsSixPartitions() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";

        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        List<PartitionInfo> partitionInfos1 = new ArrayList<>(3);
        List<PartitionInfo> partitionInfos2 = new ArrayList<>(3);
        for (int i = 0; i < 3; i++) {
            partitionInfos1.add(new PartitionInfo(topic1, i, null, null, null));
            partitionInfos2.add(new PartitionInfo(topic2, i, null, null, null));
        }

        partitionsPerTopic.put(topic1, partitionInfos1);
        partitionsPerTopic.put(topic2, partitionInfos2);

        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(consumer1, new Subscription(topics(topic1, topic2)));
        consumers.put(consumer2, new Subscription(topics(topic1, topic2)));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertAssignment(partitions(tp(topic1, 0), tp(topic1, 1), tp(topic2, 0), tp(topic2, 1)), assignment.get(consumer1));
        assertAssignment(partitions(tp(topic1, 2), tp(topic2, 2)), assignment.get(consumer2));
    }

    @Test
    public void testAssignForNonZeroBasedPartitions() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";

        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        List<PartitionInfo> partitionInfos1 = new ArrayList<>(3);
        List<PartitionInfo> partitionInfos2 = new ArrayList<>(3);
        for (int i = 2; i >= 0; i--) { // partitions can be sorted in an ascending order
            partitionInfos1.add(new PartitionInfo(topic1, 2 * i + 1, null, null, null));
            partitionInfos2.add(new PartitionInfo(topic2, 2 * i + 1, null, null, null));
        }
        partitionsPerTopic.put(topic1, partitionInfos1);
        partitionsPerTopic.put(topic2, partitionInfos2);

        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(consumer1, new Subscription(topics(topic1, topic2)));
        consumers.put(consumer2, new Subscription(topics(topic1, topic2)));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertAssignment(partitions(tp(topic1, 1), tp(topic1, 3), tp(topic2, 1), tp(topic2, 3)), assignment.get(consumer1));
        assertAssignment(partitions(tp(topic1, 5), tp(topic2, 5)), assignment.get(consumer2));
    }

    private void assertAssignment(List<TopicPartition> expected, List<TopicPartition> actual) {
        // order doesn't matter for assignment, so convert to a set
        assertEquals(new HashSet<>(expected), new HashSet<>(actual));
    }

    private static List<String> topics(String... topics) {
        return Arrays.asList(topics);
    }

    private static List<TopicPartition> partitions(TopicPartition... partitions) {
        return Arrays.asList(partitions);
    }

    private static TopicPartition tp(String topic, int partition) {
        return new TopicPartition(topic, partition);
    }
}
