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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

public class StickyAssignorTest {

    private StickyAssignor assignor = new StickyAssignor();

    @Test
    public void testOneConsumerNoTopic() {
        String consumerId = "consumer";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        Map<String, List<String>> subscriptions = Collections.singletonMap(consumerId, Collections.<String>emptyList());

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        assertEquals(Collections.singleton(consumerId), assignment.keySet());
        assertTrue(assignment.get(consumerId).isEmpty());

        assertTrue(isFullyBalanced(assignment));
        verifyBalance(assignment, subscriptions, partitionsPerTopic);
    }

    @Test
    public void testOneConsumerNonexistentTopic() {
        String topic = "topic";
        String consumerId = "consumer";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 0);
        Map<String, List<String>> subscriptions = Collections.singletonMap(consumerId, topics(topic));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);

        assertEquals(Collections.singleton(consumerId), assignment.keySet());
        assertTrue(assignment.get(consumerId).isEmpty());

        assertTrue(isFullyBalanced(assignment));
        verifyBalance(assignment, subscriptions, partitionsPerTopic);
    }

    @Test
    public void testOneConsumerOneTopic() {
        String topic = "topic";
        String consumerId = "consumer";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 3);
        Map<String, List<String>> subscriptions = Collections.singletonMap(consumerId, topics(topic));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        assertEquals(Arrays.asList(
                new TopicPartition(topic, 0),
                new TopicPartition(topic, 1),
                new TopicPartition(topic, 2)), assignment.get(consumerId));

        assertTrue(isFullyBalanced(assignment));
        verifyBalance(assignment, subscriptions, partitionsPerTopic);
    }

    @Test
    public void testOnlyAssignsPartitionsFromSubscribedTopics() {
        String topic = "topic";
        String otherTopic = "other";
        String consumerId = "consumer";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 3);
        partitionsPerTopic.put(otherTopic, 3);
        Map<String, List<String>> subscriptions = Collections.singletonMap(consumerId, topics(topic));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        assertEquals(Arrays.asList(
                new TopicPartition(topic, 0),
                new TopicPartition(topic, 1),
                new TopicPartition(topic, 2)), assignment.get(consumerId));

        assertTrue(isFullyBalanced(assignment));
        verifyBalance(assignment, subscriptions, partitionsPerTopic);
    }

    @Test
    public void testOneConsumerMultipleTopics() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumerId = "consumer";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 1);
        partitionsPerTopic.put(topic2, 2);
        Map<String, List<String>> subscriptions = Collections.singletonMap(consumerId, topics(topic1, topic2));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        assertEquals(Arrays.asList(
                new TopicPartition(topic1, 0),
                new TopicPartition(topic2, 0),
                new TopicPartition(topic2, 1)), assignment.get(consumerId));

        assertTrue(isFullyBalanced(assignment));
        verifyBalance(assignment, subscriptions, partitionsPerTopic);
    }

    @Test
    public void testTwoConsumersOneTopicOnePartition() {
        String topic = "topic";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 1);

        Map<String, List<String>> subscriptions = new HashMap<>();
        subscriptions.put(consumer1, topics(topic));
        subscriptions.put(consumer2, topics(topic));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        assertEquals(Arrays.asList(new TopicPartition(topic, 0)), assignment.get(consumer1));
        assertEquals(Collections.<TopicPartition>emptyList(), assignment.get(consumer2));

        assertTrue(isFullyBalanced(assignment));
        verifyBalance(assignment, subscriptions, partitionsPerTopic);
    }

    @Test
    public void testTwoConsumersOneTopicTwoPartitions() {
        String topic = "topic";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 2);

        Map<String, List<String>> subscriptions = new HashMap<>();
        subscriptions.put(consumer1, topics(topic));
        subscriptions.put(consumer2, topics(topic));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        assertEquals(Arrays.asList(new TopicPartition(topic, 0)), assignment.get(consumer1));
        assertEquals(Arrays.asList(new TopicPartition(topic, 1)), assignment.get(consumer2));

        assertTrue(isFullyBalanced(assignment));
        verifyBalance(assignment, subscriptions, partitionsPerTopic);
    }

    @Test
    public void testMultipleConsumersMixedTopicSubscriptions() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";
        String consumer3 = "consumer3";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 3);
        partitionsPerTopic.put(topic2, 2);

        Map<String, List<String>> subscriptions = new HashMap<>();
        subscriptions.put(consumer1, topics(topic1));
        subscriptions.put(consumer2, topics(topic1, topic2));
        subscriptions.put(consumer3, topics(topic1));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        assertEquals(Arrays.asList(
                new TopicPartition(topic1, 0),
                new TopicPartition(topic1, 2)), assignment.get(consumer1));
        assertEquals(Arrays.asList(
                new TopicPartition(topic2, 0),
                new TopicPartition(topic2, 1)), assignment.get(consumer2));
        assertEquals(Arrays.asList(
                new TopicPartition(topic1, 1)), assignment.get(consumer3));

        assertTrue(isFullyBalanced(assignment));
        verifyBalance(assignment, subscriptions, partitionsPerTopic);
    }

    @Test
    public void testTwoConsumersTwoTopicsSixPartitions() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 3);
        partitionsPerTopic.put(topic2, 3);

        Map<String, List<String>> subscriptions = new HashMap<>();
        subscriptions.put(consumer1, topics(topic1, topic2));
        subscriptions.put(consumer2, topics(topic1, topic2));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        assertEquals(Arrays.asList(
                new TopicPartition(topic1, 0),
                new TopicPartition(topic1, 2),
                new TopicPartition(topic2, 1)), assignment.get(consumer1));
        assertEquals(Arrays.asList(
                new TopicPartition(topic1, 1),
                new TopicPartition(topic2, 0),
                new TopicPartition(topic2, 2)), assignment.get(consumer2));

        assertTrue(isFullyBalanced(assignment));
        verifyBalance(assignment, subscriptions, partitionsPerTopic);
    }

    @Test
    public void testAddRemoveConsumerOneTopic() {
        String topic = "topic";
        String consumer1 = "consumer";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 3);
        Map<String, List<String>> subscriptions = new HashMap<>();
        subscriptions.put(consumer1, topics(topic));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        assertEquals(Arrays.asList(
                new TopicPartition(topic, 0),
                new TopicPartition(topic, 1),
                new TopicPartition(topic, 2)), assignment.get(consumer1));
        assertTrue(isFullyBalanced(assignment));
        verifyBalance(assignment, subscriptions, partitionsPerTopic);

        String consumer2 = "consumer2";
        subscriptions.put(consumer2, topics(topic));
        Map<String, List<TopicPartition>> assignment2 = assignor.assign(partitionsPerTopic, subscriptions);
        assertEquals(Arrays.asList(new TopicPartition(topic, 1), new TopicPartition(topic, 2)), assignment2.get(consumer1));
        assertEquals(Arrays.asList(new TopicPartition(topic, 0)), assignment2.get(consumer2));
        assertTrue(isFullyBalanced(assignment2));
        verifyBalance(assignment, subscriptions, partitionsPerTopic);

        subscriptions.remove(consumer1);
        Map<String, List<TopicPartition>> assignment3 = assignor.assign(partitionsPerTopic, subscriptions);
        assertTrue(assignment3.get(consumer2).contains(new TopicPartition(topic, 0)));
        assertTrue(assignment3.get(consumer2).contains(new TopicPartition(topic, 1)));
        assertTrue(assignment3.get(consumer2).contains(new TopicPartition(topic, 2)));
        assertTrue(isFullyBalanced(assignment3));
        verifyBalance(assignment, subscriptions, partitionsPerTopic);
    }

    /**
     * This unit test performs sticky assignment for a scenario that round robin assignor handles poorly.
     * Topics (partitions per topic): topic1 (2), topic2 (1), topic3 (2), topic4 (1), topic5 (2)
     * Subscriptions:
     *  - consumer1: topic1, topic2, topic3, topic4, topic5
     *  - consumer2: topic1, topic3, topic5
     *  - consumer3: topic1, topic3, topic5
     *  - consumer4: topic1, topic2, topic3, topic4, topic5
     * Round Robin Assignment Result:
     *  - consumer1: topic1-0, topic3-0, topic5-0
     *  - consumer2: topic1-1, topic3-1, topic5-1
     *  - consumer3:
     *  - consumer4: topic2-0, topic4-0
     * Sticky Assignment Result:
     *  - consumer1: topic2-0, topic3-0
     *  - consumer2: topic1-0, topic3-1
     *  - consumer3: topic1-1, topic5-0
     *  - consumer4: topic4-0, topic5-1
     */
    @Test
    public void testPoorRoundRobinAssignmentScenario() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        for (int i = 1; i <= 5; i++)
            partitionsPerTopic.put(String.format("topic%d", i), (i % 2) + 1);

        Map<String, List<String>> subscriptions = new HashMap<>();
        subscriptions.put("consumer1", Arrays.asList("topic1", "topic2", "topic3", "topic4", "topic5"));
        subscriptions.put("consumer2", Arrays.asList("topic1", "topic3", "topic5"));
        subscriptions.put("consumer3", Arrays.asList("topic1", "topic3", "topic5"));
        subscriptions.put("consumer4", Arrays.asList("topic1", "topic2", "topic3", "topic4", "topic5"));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyBalance(assignment, subscriptions, partitionsPerTopic);
    }

    @Test
    public void testAddRemoveTopicTwoConsumers() {
        String topic = "topic";
        String consumer1 = "consumer";
        String consumer2 = "consumer2";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 3);
        Map<String, List<String>> subscriptions = new HashMap<>();
        subscriptions.put(consumer1, topics(topic));
        subscriptions.put(consumer2, topics(topic));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        assertEquals(Arrays.asList(new TopicPartition(topic, 0), new TopicPartition(topic, 2)), assignment.get(consumer1));
        assertEquals(Arrays.asList(new TopicPartition(topic, 1)), assignment.get(consumer2));
        assertTrue(isFullyBalanced(assignment));
        verifyBalance(assignment, subscriptions, partitionsPerTopic);

        String topic2 = "topic2";
        partitionsPerTopic.put(topic2, 3);
        subscriptions.put(consumer1, topics(topic, topic2));
        subscriptions.put(consumer2, topics(topic, topic2));
        Map<String, List<TopicPartition>> assignment2 = assignor.assign(partitionsPerTopic, subscriptions);
        assertEquals(Arrays.asList(new TopicPartition(topic, 0), new TopicPartition(topic, 2), new TopicPartition(topic2, 1)), assignment2.get(consumer1));
        assertEquals(Arrays.asList(new TopicPartition(topic, 1), new TopicPartition(topic2, 2), new TopicPartition(topic2, 0)), assignment2.get(consumer2));
        assertTrue(isFullyBalanced(assignment2));
        verifyBalance(assignment, subscriptions, partitionsPerTopic);

        partitionsPerTopic.remove(topic);
        subscriptions.put(consumer1, topics(topic2));
        subscriptions.put(consumer2, topics(topic2));
        Map<String, List<TopicPartition>> assignment3 = assignor.assign(partitionsPerTopic, subscriptions);
        assertEquals(Arrays.asList(new TopicPartition(topic2, 1)), assignment3.get(consumer1));
        assertEquals(Arrays.asList(new TopicPartition(topic2, 2), new TopicPartition(topic2, 0)), assignment3.get(consumer2));
        assertTrue(isFullyBalanced(assignment3));
        verifyBalance(assignment, subscriptions, partitionsPerTopic);
    }

    @Test
    public void testReassignmentAfterOneConsumerLeaves() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        for (int i = 1; i < 20; i++)
            partitionsPerTopic.put(String.format("topic%02d", i), i);

        Map<String, List<String>> subscriptions = new HashMap<>();
        for (int i = 1; i < 20; i++) {
            List<String> topics = new ArrayList<String>();
            for (int j = 1; j <= i; j++)
                topics.add(String.format("topic%02d", j));
            subscriptions.put(String.format("consumer%02d", i), topics);
        }

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyBalance(assignment, subscriptions, partitionsPerTopic);

        subscriptions.remove("consumer10");
        assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyBalance(assignment, subscriptions, partitionsPerTopic);
    }

    @Test
    public void testReassignmentAfterOneConsumerAdded() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put("topic", 20);

        Map<String, List<String>> subscriptions = new HashMap<>();
        for (int i = 1; i < 10; i++)
            subscriptions.put(String.format("consumer%02d", i), Collections.singletonList("topic"));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyBalance(assignment, subscriptions, partitionsPerTopic);

        subscriptions.put("consumer10", Collections.singletonList("topic"));
        assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyBalance(assignment, subscriptions, partitionsPerTopic);
    }

    @Test
    public void testSameSubscriptions() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        for (int i = 1; i < 15; i++)
            partitionsPerTopic.put(String.format("topic%02d", i), i);

        Map<String, List<String>> subscriptions = new HashMap<>();
        for (int i = 1; i < 9; i++) {
            List<String> topics = new ArrayList<String>();
            for (int j = 1; j <= partitionsPerTopic.size(); j++)
                topics.add(String.format("topic%02d", j));
            subscriptions.put(String.format("consumer%02d", i), topics);
        }

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyBalance(assignment, subscriptions, partitionsPerTopic);

        subscriptions.remove("consumer05");
        assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyBalance(assignment, subscriptions, partitionsPerTopic);
    }

    @Test
    public void testLargeAssignmentWithMultipleConsumersLeaving() {
        Random rand = new Random();
        int topicCount = 400;
        int consumerCount = 1000;

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        for (int i = 0; i < topicCount; i++)
            partitionsPerTopic.put(String.format("t%d", i), rand.nextInt(10) + 1);

        Map<String, List<String>> subscriptions = new HashMap<>();
        for (int i = 0; i < consumerCount; i++) {
            List<String> topics = new ArrayList<String>();
            for (int j = 0; j < rand.nextInt(20); j++)
                topics.add(String.format("t%d", rand.nextInt(topicCount)));
            subscriptions.put(String.format("c%d", i), topics);
        }

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyBalance(assignment, subscriptions, partitionsPerTopic);

        for (int i = 0; i < 100; ++i) {
            String c = String.format("c%d", rand.nextInt(consumerCount));
            subscriptions.remove(c);
        }

        assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyBalance(assignment, subscriptions, partitionsPerTopic);
    }

    @Test
    public void testNewSubscription() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        for (int i = 1; i < 5; i++)
            partitionsPerTopic.put(String.format("topic%02d", i), 1);

        Map<String, List<String>> subscriptions = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            List<String> topics = new ArrayList<String>();
            for (int j = i; j <= 3 * i - 2; j++)
                topics.add(String.format("topic%02d", j));
            subscriptions.put(String.format("consumer%02d", i), topics);
        }

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyBalance(assignment, subscriptions, partitionsPerTopic);

        subscriptions.get("consumer00").add("topic01");

        assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyBalance(assignment, subscriptions, partitionsPerTopic);
    }

    @Test
    public void testMoveExistingAssignments() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        for (int i = 1; i <= 6; i++)
            partitionsPerTopic.put(String.format("topic%02d", i), 1);

        Map<String, List<String>> subscriptions = new HashMap<>();
        subscriptions.put("consumer01", topics("topic01", "topic02"));
        subscriptions.put("consumer02", topics("topic01", "topic02", "topic03", "topic04"));
        subscriptions.put("consumer03", topics("topic02", "topic03", "topic04", "topic05", "topic06"));

        assignor.currentAssignment.put("consumer01", new ArrayList<>(Arrays.asList(new TopicPartition("topic01", 0))));
        assignor.currentAssignment.put("consumer02", new ArrayList<>(Arrays.asList(new TopicPartition("topic02", 0), new TopicPartition("topic03", 0))));
        assignor.currentAssignment.put("consumer03", new ArrayList<>(Arrays.asList(new TopicPartition("topic04", 0), new TopicPartition("topic05", 0), new TopicPartition("topic06", 0))));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyBalance(assignment, subscriptions, partitionsPerTopic);
    }

    @Test
    public void testStickiness() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put("topic01", 3);
        Map<String, List<String>> subscriptions = new HashMap<>();
        subscriptions.put("consumer01", topics("topic01"));
        subscriptions.put("consumer02", topics("topic01"));
        subscriptions.put("consumer03", topics("topic01"));
        subscriptions.put("consumer04", topics("topic01"));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyBalance(assignment, subscriptions, partitionsPerTopic);
        Map<String, TopicPartition> partitionsAssigned = new HashMap<>();

        Set<Entry<String, List<TopicPartition>>> assignments = assignment.entrySet();
        for (Map.Entry<String, List<TopicPartition>> entry: assignments) {
            String consumer = entry.getKey();
            List<TopicPartition> topicPartitions = entry.getValue();
            int size = topicPartitions.size();
            assertTrue("Consumer " + consumer + " is assigned more topic partitions than expected.", size <= 1);
            if (size == 1)
                partitionsAssigned.put(consumer, topicPartitions.get(0));
        }

        // removing the potential group leader
        subscriptions.remove("consumer01");

        assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyBalance(assignment, subscriptions, partitionsPerTopic);

        assignments = assignment.entrySet();
        for (Map.Entry<String, List<TopicPartition>> entry: assignments) {
            String consumer = entry.getKey();
            List<TopicPartition> topicPartitions = entry.getValue();
            assertEquals("Consumer " + consumer + " is assigned more topic partitions than expected.", 1, topicPartitions.size());
            assertTrue("Stickiness was not honored for consumer " + consumer,
                    (!partitionsAssigned.containsKey(consumer)) || (assignment.get(consumer).contains(partitionsAssigned.get(consumer))));
        }
    }

    public static List<String> topics(String... topics) {
        return Arrays.asList(topics);
    }

    public static TopicPartition tp(String topic, int partition) {
        return new TopicPartition(topic, partition);
    }

    private boolean isFullyBalanced(Map<String, List<TopicPartition>> assignment) {
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (List<TopicPartition> topicPartitions: assignment.values()) {
            int size = topicPartitions.size();
            if (size < min)
                min = size;
            if (size > max)
                max = size;
        }
        return max - min <= 1;
    }

    /**
     * Verify that either:
     * - the given assignment is fully balanced (the numbers of topic partitions assigned to consumers differ by at most one), or
     * - there is no topic partition that can be moved from one consumer to another that has 2+ fewer topic partitions
     *
     * @param assignment: given assignment for balance check
     * @param subscriptions: topic subscriptions of each consumer
     * @param partitionsPerTopic: number of partitions per topic
     */
    private void verifyBalance(Map<String, List<TopicPartition>> assignment, Map<String, List<String>> subscriptions, Map<String, Integer> partitionsPerTopic) {
        if (isFullyBalanced(assignment))
            return;

        // an ascending sorted set of consumer based on how many topic partitions are assigned to them in the given assignment
        TreeSet<String> consumers = new TreeSet<String>(new StickyAssignor.SubscriptionComparator(assignment));
        consumers.addAll(assignment.keySet());

        // all possible assignments
        final HashMap<String, Set<TopicPartition>> allSubscriptions = new HashMap<>();
        for (Map.Entry<String, List<String>> entry: subscriptions.entrySet()) {
            String consumer = entry.getKey();
            Set<TopicPartition> topicPartitions = new HashSet<>();
            allSubscriptions.put(consumer, topicPartitions);
            for (String topic: entry.getValue()) {
                for (int i = 0; i < partitionsPerTopic.get(topic); i++) {
                    TopicPartition topicPartition = new TopicPartition(topic, i);
                    topicPartitions.add(topicPartition);
                }
            }
        }

        // create a mapping from partitions to the consumer assigned to them
        final HashMap<TopicPartition, String> allPartitions = new HashMap<>();
        for (Map.Entry<String, List<TopicPartition>> entry: assignment.entrySet()) {
            for (TopicPartition topicPartition: entry.getValue()) {
                assertTrue(topicPartition + " is assigned to more than one consumer.", !allPartitions.containsKey(topicPartition));
                allPartitions.put(topicPartition, entry.getKey());
            }
        }

        // starting from the consumer with fewest assignments make sure there is no topic partition that could be
        // assigned to this consumer and it is not (because it is assigned to a consumer with more topic partitions)
        for (String consumer: consumers) {
            for (TopicPartition topicPartition: allSubscriptions.get(consumer)) {
                if (!assignment.get(consumer).contains(topicPartition)) {
                    String otherConsumer = allPartitions.get(topicPartition);
                    assertNotNull(otherConsumer);
                    assertTrue(topicPartition + " can be assigned to another consumer for a more balanced assignment", assignment.get(consumer).size() >= assignment.get(otherConsumer).size() - 1);
                }
            }
        }
    }
}
