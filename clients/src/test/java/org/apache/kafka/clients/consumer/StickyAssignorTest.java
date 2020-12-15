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

import static org.apache.kafka.clients.consumer.StickyAssignor.serializeTopicPartitionAssignment;
import static org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor.DEFAULT_GENERATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor;
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor.MemberData;
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignorTest;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;
import org.junit.Test;

public class StickyAssignorTest extends AbstractStickyAssignorTest {

    @Override
    public AbstractStickyAssignor createAssignor() {
        return new StickyAssignor();
    }

    @Override
    public Subscription buildSubscription(List<String> topics, List<TopicPartition> partitions) {
        return new Subscription(topics,
            serializeTopicPartitionAssignment(new MemberData(partitions, Optional.of(DEFAULT_GENERATION))));
    }

    @Test
    public void testAssignmentWithMultipleGenerations1() {
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";
        String consumer3 = "consumer3";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 6);
        subscriptions.put(consumer1, new Subscription(topics(topic)));
        subscriptions.put(consumer2, new Subscription(topics(topic)));
        subscriptions.put(consumer3, new Subscription(topics(topic)));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        List<TopicPartition> r1partitions1 = assignment.get(consumer1);
        List<TopicPartition> r1partitions2 = assignment.get(consumer2);
        List<TopicPartition> r1partitions3 = assignment.get(consumer3);
        assertTrue(r1partitions1.size() == 2 && r1partitions2.size() == 2 && r1partitions3.size() == 2);
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));

        subscriptions.put(consumer1, buildSubscription(topics(topic), r1partitions1));
        subscriptions.put(consumer2, buildSubscription(topics(topic), r1partitions2));
        subscriptions.remove(consumer3);

        assignment = assignor.assign(partitionsPerTopic, subscriptions);
        List<TopicPartition> r2partitions1 = assignment.get(consumer1);
        List<TopicPartition> r2partitions2 = assignment.get(consumer2);
        assertTrue(r2partitions1.size() == 3 && r2partitions2.size() == 3);
        assertTrue(r2partitions1.containsAll(r1partitions1));
        assertTrue(r2partitions2.containsAll(r1partitions2));
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
        assertFalse(Collections.disjoint(r2partitions2, r1partitions3));

        subscriptions.remove(consumer1);
        subscriptions.put(consumer2, buildSubscriptionWithGeneration(topics(topic), r2partitions2, 2));
        subscriptions.put(consumer3, buildSubscriptionWithGeneration(topics(topic), r1partitions3, 1));

        assignment = assignor.assign(partitionsPerTopic, subscriptions);
        List<TopicPartition> r3partitions2 = assignment.get(consumer2);
        List<TopicPartition> r3partitions3 = assignment.get(consumer3);
        assertTrue(r3partitions2.size() == 3 && r3partitions3.size() == 3);
        assertTrue(Collections.disjoint(r3partitions2, r3partitions3));
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @Test
    public void testAssignmentWithMultipleGenerations2() {
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";
        String consumer3 = "consumer3";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 6);
        subscriptions.put(consumer1, new Subscription(topics(topic)));
        subscriptions.put(consumer2, new Subscription(topics(topic)));
        subscriptions.put(consumer3, new Subscription(topics(topic)));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        List<TopicPartition> r1partitions1 = assignment.get(consumer1);
        List<TopicPartition> r1partitions2 = assignment.get(consumer2);
        List<TopicPartition> r1partitions3 = assignment.get(consumer3);
        assertTrue(r1partitions1.size() == 2 && r1partitions2.size() == 2 && r1partitions3.size() == 2);
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));

        subscriptions.remove(consumer1);
        subscriptions.put(consumer2, buildSubscriptionWithGeneration(topics(topic), r1partitions2, 1));
        subscriptions.remove(consumer3);

        assignment = assignor.assign(partitionsPerTopic, subscriptions);
        List<TopicPartition> r2partitions2 = assignment.get(consumer2);
        assertEquals(6, r2partitions2.size());
        assertTrue(r2partitions2.containsAll(r1partitions2));
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));

        subscriptions.put(consumer1, buildSubscriptionWithGeneration(topics(topic), r1partitions1, 1));
        subscriptions.put(consumer2, buildSubscriptionWithGeneration(topics(topic), r2partitions2, 2));
        subscriptions.put(consumer3, buildSubscriptionWithGeneration(topics(topic), r1partitions3, 1));

        assignment = assignor.assign(partitionsPerTopic, subscriptions);
        List<TopicPartition> r3partitions1 = assignment.get(consumer1);
        List<TopicPartition> r3partitions2 = assignment.get(consumer2);
        List<TopicPartition> r3partitions3 = assignment.get(consumer3);
        assertTrue(r3partitions1.size() == 2 && r3partitions2.size() == 2 && r3partitions3.size() == 2);
        assertEquals(r1partitions1, r3partitions1);
        assertEquals(r1partitions2, r3partitions2);
        assertEquals(r1partitions3, r3partitions3);
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @Test
    public void testAssignmentWithConflictingPreviousGenerations() {
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";
        String consumer3 = "consumer3";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 6);
        subscriptions.put(consumer1, new Subscription(topics(topic)));
        subscriptions.put(consumer2, new Subscription(topics(topic)));
        subscriptions.put(consumer3, new Subscription(topics(topic)));

        TopicPartition tp0 = new TopicPartition(topic, 0);
        TopicPartition tp1 = new TopicPartition(topic, 1);
        TopicPartition tp2 = new TopicPartition(topic, 2);
        TopicPartition tp3 = new TopicPartition(topic, 3);
        TopicPartition tp4 = new TopicPartition(topic, 4);
        TopicPartition tp5 = new TopicPartition(topic, 5);

        List<TopicPartition> c1partitions0 = partitions(tp0, tp1, tp4);
        List<TopicPartition> c2partitions0 = partitions(tp0, tp1, tp2);
        List<TopicPartition> c3partitions0 = partitions(tp3, tp4, tp5);
        subscriptions.put(consumer1, buildSubscriptionWithGeneration(topics(topic), c1partitions0, 1));
        subscriptions.put(consumer2, buildSubscriptionWithGeneration(topics(topic), c2partitions0, 2));
        subscriptions.put(consumer3, buildSubscriptionWithGeneration(topics(topic), c3partitions0, 2));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        List<TopicPartition> c1partitions = assignment.get(consumer1);
        List<TopicPartition> c2partitions = assignment.get(consumer2);
        List<TopicPartition> c3partitions = assignment.get(consumer3);

        assertTrue(c1partitions.size() == 2 && c2partitions.size() == 2 && c3partitions.size() == 2);
        assertTrue(c2partitions0.containsAll(c2partitions));
        assertTrue(c3partitions0.containsAll(c3partitions));
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @Test
    public void testSchemaBackwardCompatibility() {
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";
        String consumer3 = "consumer3";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 3);
        subscriptions.put(consumer1, new Subscription(topics(topic)));
        subscriptions.put(consumer2, new Subscription(topics(topic)));
        subscriptions.put(consumer3, new Subscription(topics(topic)));

        TopicPartition tp0 = new TopicPartition(topic, 0);
        TopicPartition tp1 = new TopicPartition(topic, 1);
        TopicPartition tp2 = new TopicPartition(topic, 2);

        List<TopicPartition> c1partitions0 = partitions(tp0, tp2);
        List<TopicPartition> c2partitions0 = partitions(tp1);
        subscriptions.put(consumer1, buildSubscriptionWithGeneration(topics(topic), c1partitions0, 1));
        subscriptions.put(consumer2, buildSubscriptionWithOldSchema(topics(topic), c2partitions0));
        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        List<TopicPartition> c1partitions = assignment.get(consumer1);
        List<TopicPartition> c2partitions = assignment.get(consumer2);
        List<TopicPartition> c3partitions = assignment.get(consumer3);

        assertTrue(c1partitions.size() == 1 && c2partitions.size() == 1 && c3partitions.size() == 1);
        assertTrue(c1partitions0.containsAll(c1partitions));
        assertTrue(c2partitions0.containsAll(c2partitions));
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    private Subscription buildSubscriptionWithGeneration(List<String> topics, List<TopicPartition> partitions, int generation) {
        return new Subscription(topics,
            serializeTopicPartitionAssignment(new MemberData(partitions, Optional.of(generation))));
    }

    private static Subscription buildSubscriptionWithOldSchema(List<String> topics, List<TopicPartition> partitions) {
        Struct struct = new Struct(StickyAssignor.STICKY_ASSIGNOR_USER_DATA_V0);
        List<Struct> topicAssignments = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> topicEntry : CollectionUtils.groupPartitionsByTopic(partitions).entrySet()) {
            Struct topicAssignment = new Struct(StickyAssignor.TOPIC_ASSIGNMENT);
            topicAssignment.set(StickyAssignor.TOPIC_KEY_NAME, topicEntry.getKey());
            topicAssignment.set(StickyAssignor.PARTITIONS_KEY_NAME, topicEntry.getValue().toArray());
            topicAssignments.add(topicAssignment);
        }
        struct.set(StickyAssignor.TOPIC_PARTITIONS_KEY_NAME, topicAssignments.toArray());
        ByteBuffer buffer = ByteBuffer.allocate(StickyAssignor.STICKY_ASSIGNOR_USER_DATA_V0.sizeOf(struct));
        StickyAssignor.STICKY_ASSIGNOR_USER_DATA_V0.write(buffer, struct);
        buffer.flip();

        return new Subscription(topics, buffer);
    }
}