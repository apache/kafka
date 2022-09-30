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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.CircularIterator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * <p>The topic round robin assignor lays out all the available topics and all the available consumers. It
 * then proceeds to do a round robin assignment from topic to consumer. If the subscriptions of all consumer
 * instances are identical (on all topics), then the topics will be uniformly distributed.
 * This guarantees that a topic is only consumed by one consumer.
 * Therefore, if each topic is assigned to exactly one partition, a consumer can be stopped without impacting the other topics.
 *
 * <p>For example, suppose there are two consumers <code>C0</code> and <code>C1</code>, two topics <code>t0</code> and <code>t1</code>,
 * and each topic has 3 partitions, resulting in partitions <code>t0p0</code>, <code>t0p1</code>, <code>t0p2</code>,
 * <code>t1p0</code>, <code>t1p1</code>, and <code>t1p2</code>.
 *
 * <p>The assignment will be:
 * <ul>
 * <li><code>C0: [t0p0, t0p1 t0p1]</code>
 * <li><code>C1: [t1p0, t1p1, t1p2]</code>
 * </ul>
 *
 * <p>If the subscriptions of all consumers are identical (on all topics)
 * and if the number of consumer instances is greater than to the number of topics,
 * then some consumer will not have any partitions to consume.
 *
 * <p>For example, suppose there are three consumers <code>C0</code>, <code>C1</code> and <code>C2</code>, two topics <code>t0</code> and <code>t1</code>,
 * and each topic has 3 partitions, resulting in partitions <code>t0p0</code>, <code>t0p1</code>, <code>t0p2</code>,
 * <code>t1p0</code>, <code>t1p1</code>, and <code>t1p2</code>.
 *
 * <p>The assignment will be:
 * <ul>
 * <li><code>C0: [t0p0, t0p1 t0p1]</code>
 * <li><code>C1: [t1p0, t1p1, t1p2]</code>
 * <li><code>C2: []</code>
 * </ul>
 *
 * <p>If the subscriptions of all consumers are identical (on all topics)
 * and if the number of consumer instances is lower than to the number of topics,
 * then some consumer will have multiple topics to consume.
 *
 * <p>For example, suppose there are three consumers <code>C0</code> and <code>C1</code>, three topics <code>t0</code>, <code>t1</code> and <code>t2</code>,
 * and each topic has 2 partitions, resulting in partitions <code>t0p0</code>, <code>t0p1</code>, <code>t1p0</code>,
 * <code>t1p1</code>, <code>t2p0</code>, and <code>t2p1</code>.
 *
 * <p>The assignment will be:
 * <ul>
 * <li><code>C0: [t0p0, t0p1, t2p0, t2p1]</code>
 * <li><code>C1: [t1p0, t1p1]</code>
 * </ul>
 */
public class TopicRoundRobinAssignor extends RoundRobinAssignor {

    public static final String TOPIC_ROUND_ROBIN_ASSIGNOR_NAME = "topicroundrobin";

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic, Map<String, Subscription> subscriptions) {
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        Set<String> subscribedTopics = new HashSet<>();
        subscriptions.forEach((key, subscription) -> {
            subscribedTopics.addAll(subscription.topics());
            assignment.put(key, new ArrayList<>());
        });
        CircularIterator<Map.Entry<String, Subscription>> assigner = getRoundRobinAssigner(subscriptions);
        partitionsPerTopic
                .entrySet()
                .stream()
                .filter(topicPartition -> subscribedTopics.contains(topicPartition.getKey()))
                .forEach(topicPartition -> assignPartitions(topicPartition, assigner, assignment));
        return assignment;
    }

    private CircularIterator<Map.Entry<String, Subscription>> getRoundRobinAssigner(Map<String, Subscription> subscriptions) {
        List<Map.Entry<String, Subscription>> sortedSubscriptions = subscriptions.entrySet()
                .stream()
                .sorted(subscriptionComparator())
                .collect(Collectors.toList());
        return new CircularIterator<>(sortedSubscriptions);
    }

    private Comparator<Map.Entry<String, Subscription>> subscriptionComparator() {
        return (subscription1, subscription2) -> {
            int numPartitionSubscription1 = subscription1.getValue().ownedPartitions().size();
            int numPartitionSubscription2 = subscription2.getValue().ownedPartitions().size();
            if (numPartitionSubscription1 != numPartitionSubscription2) {
                return Integer.compare(numPartitionSubscription1, numPartitionSubscription2);
            }
            int numTopicsSubscription1 = subscription1.getValue().topics().size();
            int numTopicsSubscription2 = subscription2.getValue().topics().size();
            if (numTopicsSubscription1 != numTopicsSubscription2) {
                return Integer.compare(numTopicsSubscription1, numTopicsSubscription2);
            }
            return subscription1.getKey().compareTo(subscription2.getKey());
        };
    }

    private void assignPartitions(Map.Entry<String, Integer> topicPartition, CircularIterator<Map.Entry<String, Subscription>> assigner, Map<String, List<TopicPartition>> assignment) {
        String topic = topicPartition.getKey();
        int numPartition = topicPartition.getValue();
        while (!assigner.peek().getValue().topics().contains(topic)) {
            assigner.next();
        }
        List<TopicPartition> topicPartitions = IntStream.range(0, numPartition)
                .mapToObj(i -> new TopicPartition(topic, i))
                .collect(Collectors.toList());
        assignment.get(assigner.next().getKey()).addAll(topicPartitions);
    }

    @Override
    public String name() {
        return TOPIC_ROUND_ROBIN_ASSIGNOR_NAME;
    }
}
