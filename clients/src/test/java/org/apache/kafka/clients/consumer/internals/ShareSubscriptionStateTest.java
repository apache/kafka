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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShareSubscriptionStateTest {

    private ShareSubscriptionState state;

    private final String topic = "topic";
    private final String topic1 = "topic1";
    private final TopicPartition tp0 = new TopicPartition(topic, 0);
    private final TopicPartition tp1 = new TopicPartition(topic, 1);
    private final TopicPartition t1p0 = new TopicPartition(topic1, 0);

    @BeforeEach
    public void setUp() {
        state = new ShareSubscriptionState(new LogContext());
    }

    @Test
    public void testInitialStateIsEmpty() {
        assertTrue(state.subscription().isEmpty());
        assertTrue(state.assignedPartitions().isEmpty());
        assertTrue(state.assignedTopicIds().isEmpty());
        assertTrue(state.fetchablePartitions().isEmpty());
        assertFalse(state.hasAutoAssignedPartitions());
        assertTrue(state.hasNoSubscriptionOrUserAssignment());
    }

    @Test
    public void testSubscribeToShareGroup() {
        assertTrue(state.subscribeToShareGroup(Set.of(topic)));
        assertEquals(Set.of(topic), state.subscription());
        assertTrue(state.hasAutoAssignedPartitions());
        assertFalse(state.hasNoSubscriptionOrUserAssignment());

        // Subscribing to the same set again reports no change...
        assertFalse(state.subscribeToShareGroup(Set.of(topic)));

        // ...but a different set does.
        assertTrue(state.subscribeToShareGroup(Set.of(topic, topic1)));
        assertEquals(Set.of(topic, topic1), state.subscription());
    }

    @Test
    public void testSubscribeToEmptySetStillMarksSubscribed() {
        // Even subscribing to an empty set marks the consumer as subscribed to a share group.
        assertFalse(state.subscribeToShareGroup(Set.of()));
        assertTrue(state.hasAutoAssignedPartitions());
        assertFalse(state.hasNoSubscriptionOrUserAssignment());
    }

    @Test
    public void testMetadataTopics() {
        state.subscribeToShareGroup(Set.of(topic, topic1));
        assertEquals(Set.of(topic, topic1), state.metadataTopics());
        assertTrue(state.needsMetadata(topic));
        assertTrue(state.needsMetadata(topic1));
        assertFalse(state.needsMetadata("other"));
    }

    @Test
    public void testAssignFromSubscribedPreservesOrder() {
        state.subscribeToShareGroup(Set.of(topic));
        state.assignFromSubscribed(List.of(tp0, tp1, t1p0));

        assertEquals(List.of(tp0, tp1, t1p0), state.fetchablePartitions());
        assertEquals(Set.of(tp0, tp1, t1p0), state.assignedPartitions());
    }

    @Test
    public void testAssignFromSubscribedReplacesPreviousAssignment() {
        state.subscribeToShareGroup(Set.of(topic));
        state.assignFromSubscribed(List.of(tp0, tp1));
        state.assignFromSubscribed(List.of(t1p0));

        assertEquals(List.of(t1p0), state.fetchablePartitions());
        assertEquals(Set.of(t1p0), state.assignedPartitions());
    }

    @Test
    public void testMovePartitionToEndRoundRobin() {
        state.subscribeToShareGroup(Set.of(topic, topic1));
        state.assignFromSubscribed(List.of(tp0, tp1, t1p0));

        // Move the head to the end - mimics the round-robin used when fetching.
        state.movePartitionToEnd(tp0);
        assertEquals(List.of(tp1, t1p0, tp0), state.fetchablePartitions());

        state.movePartitionToEnd(tp1);
        assertEquals(List.of(t1p0, tp0, tp1), state.fetchablePartitions());
    }

    @Test
    public void testAssignedTopicIds() {
        Set<Uuid> topicIds = Set.of(Uuid.randomUuid(), Uuid.randomUuid());
        state.setAssignedTopicIds(topicIds);
        assertEquals(topicIds, state.assignedTopicIds());
    }

    @Test
    public void testUnsubscribeClearsAllState() {
        state.subscribeToShareGroup(Set.of(topic));
        state.assignFromSubscribed(List.of(tp0, tp1));
        state.setAssignedTopicIds(Set.of(Uuid.randomUuid()));

        state.unsubscribe();

        assertTrue(state.subscription().isEmpty());
        assertTrue(state.assignedPartitions().isEmpty());
        assertTrue(state.assignedTopicIds().isEmpty());
        assertTrue(state.fetchablePartitions().isEmpty());
        assertFalse(state.hasAutoAssignedPartitions());
        assertTrue(state.hasNoSubscriptionOrUserAssignment());
    }

    @Test
    public void testEnablePartitionsAwaitingCallbackIsNoOp() {
        state.subscribeToShareGroup(Set.of(topic));
        state.assignFromSubscribed(List.of(tp0, tp1));

        // No-op for share consumers: the assignment and its fetchability are unchanged.
        state.enablePartitionsAwaitingCallback(List.of(tp0, tp1));
        assertEquals(List.of(tp0, tp1), state.fetchablePartitions());
    }

    @Test
    public void testMarkPendingRevocationIsNoOp() {
        state.subscribeToShareGroup(Set.of(topic));
        state.assignFromSubscribed(List.of(tp0, tp1));

        // No-op for share consumers: revoked partitions remain assigned and fetchable until the
        // next assignment is applied.
        state.markPendingRevocation(Set.of(tp0));
        assertEquals(List.of(tp0, tp1), state.fetchablePartitions());
        assertEquals(Set.of(tp0, tp1), state.assignedPartitions());
    }
}
