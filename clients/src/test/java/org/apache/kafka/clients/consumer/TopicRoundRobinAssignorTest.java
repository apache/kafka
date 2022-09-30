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
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicRoundRobinAssignorTest {

    private static final String CONSUMER_ID_1 = "consumer1";
    private static final String CONSUMER_ID_2 = "consumer2";
    private static final String CONSUMER_ID_3 = "consumer3";

    private static final String TOPIC_1 = "topic1";
    private static final String TOPIC_2 = "topic2";

    private final TopicRoundRobinAssignor assignor = new TopicRoundRobinAssignor();

    @Test
    void testOneConsumerNoTopic() {
        // Given
        Map<String, Integer> partitionsPerTopic = Collections.emptyMap();
        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(CONSUMER_ID_1, new Subscription(Collections.emptyList()));
        // When
        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        // Then
        assertEquals(consumers(CONSUMER_ID_1), assignment.keySet());
        assertTrue(assignment.get(CONSUMER_ID_1).isEmpty());
    }

    @Test
    void testOneConsumerNonexistentTopic() {
        // Given
        Map<String, Integer> partitionsPerTopic = Collections.emptyMap();
        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(CONSUMER_ID_1, new Subscription(topics(TOPIC_1)));
        // When
        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        // Then
        assertEquals(consumers(CONSUMER_ID_1), assignment.keySet());
        assertTrue(assignment.get(CONSUMER_ID_1).isEmpty());
    }

    @Test
    void testOneConsumerOneTopic() {
        // Given
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(TOPIC_1, 3);
        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(CONSUMER_ID_1, new Subscription(topics(TOPIC_1)));
        // When
        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        // Then
        assertEquals(consumers(CONSUMER_ID_1), assignment.keySet());
        assertEquals(
                partitions(tp(TOPIC_1, 0), tp(TOPIC_1, 1), tp(TOPIC_1, 2)),
                assignment.get(CONSUMER_ID_1)
        );
    }

    @Test
    void testOnlyAssignsPartitionsFromSubscribedTopics() {
        // Given
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(TOPIC_1, 3);
        partitionsPerTopic.put(TOPIC_2, 3);
        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(CONSUMER_ID_1, new Subscription(topics(TOPIC_1)));
        // When
        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        // Then
        assertEquals(consumers(CONSUMER_ID_1), assignment.keySet());
        assertEquals(
                partitions(tp(TOPIC_1, 0), tp(TOPIC_1, 1), tp(TOPIC_1, 2)),
                assignment.get(CONSUMER_ID_1)
        );
    }

    @Test
    void testOneConsumerMultipleTopics() {
        // Given
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(TOPIC_1, 3);
        partitionsPerTopic.put(TOPIC_2, 2);
        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(CONSUMER_ID_1, new Subscription(topics(TOPIC_1, TOPIC_2)));
        // When
        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        // Then
        assertEquals(consumers(CONSUMER_ID_1), assignment.keySet());
        assertEquals(
                partitions(
                        tp(TOPIC_1, 0), tp(TOPIC_1, 1), tp(TOPIC_1, 2),
                        tp(TOPIC_2, 0), tp(TOPIC_2, 1)
                ),
                assignment.get(CONSUMER_ID_1)
        );
    }

    @Test
    void testTwoConsumersOneTopicOnePartition() {
        // Given
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(TOPIC_1, 1);
        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(CONSUMER_ID_1, new Subscription(topics(TOPIC_1)));
        consumers.put(CONSUMER_ID_2, new Subscription(topics(TOPIC_1)));
        // When
        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        // Then
        assertEquals(consumers(CONSUMER_ID_1, CONSUMER_ID_2), assignment.keySet());
        assertEquals(partitions(tp(TOPIC_1, 0)), assignment.get(CONSUMER_ID_1));
        assertEquals(Collections.emptyList(), assignment.get(CONSUMER_ID_2));
    }

    @Test
    void testTwoConsumersTwoTopicMultiplePartitions() {
        // Given
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(TOPIC_1, 3);
        partitionsPerTopic.put(TOPIC_2, 2);
        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(CONSUMER_ID_1, new Subscription(topics(TOPIC_1)));
        consumers.put(CONSUMER_ID_2, new Subscription(topics(TOPIC_2)));
        // When
        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        // Then
        assertEquals(consumers(CONSUMER_ID_1, CONSUMER_ID_2), assignment.keySet());
        assertEquals(partitions(tp(TOPIC_1, 0), tp(TOPIC_1, 1), tp(TOPIC_1, 2)), assignment.get(CONSUMER_ID_1));
        assertEquals(partitions(tp(TOPIC_2, 0), tp(TOPIC_2, 1)), assignment.get(CONSUMER_ID_2));
    }

    @Test
    void testTwoConsumersOneTopicTwoPartitions() {
        // Given
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(TOPIC_1, 2);
        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(CONSUMER_ID_1, new Subscription(topics(TOPIC_1)));
        consumers.put(CONSUMER_ID_2, new Subscription(topics(TOPIC_1)));
        // When
        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        // Then
        assertEquals(consumers(CONSUMER_ID_1, CONSUMER_ID_2), assignment.keySet());
        assertEquals(partitions(tp(TOPIC_1, 0), tp(TOPIC_1, 1)), assignment.get(CONSUMER_ID_1));
        assertEquals(Collections.emptyList(), assignment.get(CONSUMER_ID_2));
    }

    @Test
    void testThreeConsumersMixedTopicsMultiplePartitions() {
        // Given
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(TOPIC_1, 3);
        partitionsPerTopic.put(TOPIC_2, 2);
        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(CONSUMER_ID_1, new Subscription(topics(TOPIC_1)));
        consumers.put(CONSUMER_ID_2, new Subscription(topics(TOPIC_1, TOPIC_2)));
        consumers.put(CONSUMER_ID_3, new Subscription(topics(TOPIC_1)));
        // When
        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        // Then
        assertEquals(consumers(CONSUMER_ID_1, CONSUMER_ID_2, CONSUMER_ID_3), assignment.keySet());
        assertEquals(partitions(tp(TOPIC_1, 0), tp(TOPIC_1, 1), tp(TOPIC_1, 2)), assignment.get(CONSUMER_ID_1));
        assertEquals(partitions(tp(TOPIC_2, 0), tp(TOPIC_2, 1)), assignment.get(CONSUMER_ID_2));
        assertEquals(Collections.emptyList(), assignment.get(CONSUMER_ID_3));
    }

    @Test
    void testRebalanceToEmptyConsumer() {
        // Given
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(TOPIC_1, 3);
        partitionsPerTopic.put(TOPIC_2, 2);
        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(CONSUMER_ID_1, new Subscription(topics(TOPIC_1)));
        consumers.put(CONSUMER_ID_2, new Subscription(topics(TOPIC_1, TOPIC_2)));
        consumers.put(CONSUMER_ID_3, new Subscription(topics(TOPIC_1)));
        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertEquals(consumers(CONSUMER_ID_1, CONSUMER_ID_2, CONSUMER_ID_3), assignment.keySet());
        assertEquals(partitions(tp(TOPIC_1, 0), tp(TOPIC_1, 1), tp(TOPIC_1, 2)), assignment.get(CONSUMER_ID_1));
        assertEquals(partitions(tp(TOPIC_2, 0), tp(TOPIC_2, 1)), assignment.get(CONSUMER_ID_2));
        assertEquals(Collections.emptyList(), assignment.get(CONSUMER_ID_3));
        // When
        consumers.remove(CONSUMER_ID_1);
        assignment = assignor.assign(partitionsPerTopic, consumers);
        // Then
        assertEquals(consumers(CONSUMER_ID_2, CONSUMER_ID_3), assignment.keySet());
        assertEquals(partitions(tp(TOPIC_2, 0), tp(TOPIC_2, 1)), assignment.get(CONSUMER_ID_2));
        assertEquals(partitions(tp(TOPIC_1, 0), tp(TOPIC_1, 1), tp(TOPIC_1, 2)), assignment.get(CONSUMER_ID_3));
    }

    private static Set<String> consumers(String... consumers) {
        return new HashSet<>(Arrays.asList(consumers));
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
