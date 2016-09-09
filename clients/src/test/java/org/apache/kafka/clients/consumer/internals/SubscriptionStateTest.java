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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SubscriptionStateTest {

    private final SubscriptionState state = new SubscriptionState(OffsetResetStrategy.EARLIEST);
    private final String topic = "test";
    private final String topic1 = "test1";
    private final TopicPartition tp0 = new TopicPartition(topic, 0);
    private final TopicPartition tp1 = new TopicPartition(topic, 1);
    private final TopicPartition t1p0 = new TopicPartition(topic1, 0);
    private final MockRebalanceListener rebalanceListener = new MockRebalanceListener();

    @Test
    public void partitionAssignment() {
        state.assignFromUser(singleton(tp0));
        assertEquals(singleton(tp0), state.assignedPartitions());
        assertFalse(state.hasAllFetchPositions());
        assertTrue(state.refreshCommitsNeeded());
        state.committed(tp0, new OffsetAndMetadata(1));
        state.seek(tp0, 1);
        assertTrue(state.isFetchable(tp0));
        assertAllPositions(tp0, 1L);
        state.assignFromUser(Collections.<TopicPartition>emptySet());
        assertTrue(state.assignedPartitions().isEmpty());
        assertFalse(state.isAssigned(tp0));
        assertFalse(state.isFetchable(tp0));
    }

    @Test
    public void partitionAssignmentChangeOnTopicSubscription() {
        state.assignFromUser(new HashSet<>(Arrays.asList(tp0, tp1)));
        // assigned partitions should immediately change
        assertEquals(2, state.assignedPartitions().size());
        assertTrue(state.assignedPartitions().contains(tp0));
        assertTrue(state.assignedPartitions().contains(tp1));

        state.unsubscribe();
        // assigned partitions should immediately change
        assertTrue(state.assignedPartitions().isEmpty());

        state.subscribe(singleton(topic1), rebalanceListener);
        // assigned partitions should remain unchanged
        assertTrue(state.assignedPartitions().isEmpty());

        state.assignFromSubscribed(asList(t1p0));
        // assigned partitions should immediately change
        assertEquals(singleton(t1p0), state.assignedPartitions());

        state.subscribe(singleton(topic), rebalanceListener);
        // assigned partitions should remain unchanged
        assertEquals(singleton(t1p0), state.assignedPartitions());

        state.unsubscribe();
        // assigned partitions should immediately change
        assertTrue(state.assignedPartitions().isEmpty());
    }

    @Test
    public void partitionAssignmentChangeOnPatternSubscription() {
        state.subscribe(Pattern.compile(".*"), rebalanceListener);
        // assigned partitions should remain unchanged
        assertTrue(state.assignedPartitions().isEmpty());

        state.subscribeFromPattern(new HashSet<>(Arrays.asList(topic, topic1)));
        // assigned partitions should remain unchanged
        assertTrue(state.assignedPartitions().isEmpty());

        state.assignFromSubscribed(asList(tp1));
        // assigned partitions should immediately change
        assertEquals(singleton(tp1), state.assignedPartitions());

        state.subscribe(Pattern.compile(".*t"), rebalanceListener);
        // assigned partitions should remain unchanged
        assertEquals(singleton(tp1), state.assignedPartitions());

        state.subscribeFromPattern(singleton(topic));
        // assigned partitions should remain unchanged
        assertEquals(singleton(tp1), state.assignedPartitions());

        state.unsubscribe();
        // assigned partitions should immediately change
        assertTrue(state.assignedPartitions().isEmpty());
    }

    @Test
    public void partitionReset() {
        state.assignFromUser(singleton(tp0));
        state.seek(tp0, 5);
        assertEquals(5L, (long) state.position(tp0));
        state.needOffsetReset(tp0);
        assertFalse(state.isFetchable(tp0));
        assertTrue(state.isOffsetResetNeeded(tp0));
        assertEquals(null, state.position(tp0));

        // seek should clear the reset and make the partition fetchable
        state.seek(tp0, 0);
        assertTrue(state.isFetchable(tp0));
        assertFalse(state.isOffsetResetNeeded(tp0));
    }

    @Test
    public void topicSubscription() {
        state.subscribe(singleton(topic), rebalanceListener);
        assertEquals(1, state.subscription().size());
        assertTrue(state.assignedPartitions().isEmpty());
        assertTrue(state.partitionsAutoAssigned());
        state.assignFromSubscribed(asList(tp0));
        state.seek(tp0, 1);
        state.committed(tp0, new OffsetAndMetadata(1));
        assertAllPositions(tp0, 1L);
        state.assignFromSubscribed(asList(tp1));
        assertTrue(state.isAssigned(tp1));
        assertFalse(state.isAssigned(tp0));
        assertFalse(state.isFetchable(tp1));
        assertEquals(singleton(tp1), state.assignedPartitions());
    }

    @Test
    public void partitionPause() {
        state.assignFromUser(singleton(tp0));
        state.seek(tp0, 100);
        assertTrue(state.isFetchable(tp0));
        state.pause(tp0);
        assertFalse(state.isFetchable(tp0));
        state.resume(tp0);
        assertTrue(state.isFetchable(tp0));
    }

    @Test
    public void commitOffsetMetadata() {
        state.assignFromUser(singleton(tp0));
        state.committed(tp0, new OffsetAndMetadata(5, "hi"));

        assertEquals(5, state.committed(tp0).offset());
        assertEquals("hi", state.committed(tp0).metadata());
    }

    @Test(expected = IllegalStateException.class)
    public void invalidPositionUpdate() {
        state.subscribe(singleton(topic), rebalanceListener);
        state.assignFromSubscribed(asList(tp0));
        state.position(tp0, 0);
    }

    @Test(expected = IllegalStateException.class)
    public void cantChangePositionForNonAssignedPartition() {
        state.position(tp0, 1);
    }

    public void assertAllPositions(TopicPartition tp, Long offset) {
        assertEquals(offset.longValue(), state.committed(tp).offset());
        assertEquals(offset, state.position(tp));
    }

    @Test(expected = IllegalStateException.class)
    public void cantSubscribeTopicAndPattern() {
        state.subscribe(singleton(topic), rebalanceListener);
        state.subscribe(Pattern.compile(".*"), rebalanceListener);
    }

    @Test(expected = IllegalStateException.class)
    public void cantSubscribePartitionAndPattern() {
        state.assignFromUser(singleton(tp0));
        state.subscribe(Pattern.compile(".*"), rebalanceListener);
    }

    @Test(expected = IllegalStateException.class)
    public void cantSubscribePatternAndTopic() {
        state.subscribe(Pattern.compile(".*"), rebalanceListener);
        state.subscribe(singleton(topic), rebalanceListener);
    }

    @Test(expected = IllegalStateException.class)
    public void cantSubscribePatternAndPartition() {
        state.subscribe(Pattern.compile(".*"), rebalanceListener);
        state.assignFromUser(singleton(tp0));
    }

    @Test
    public void patternSubscription() {
        state.subscribe(Pattern.compile(".*"), rebalanceListener);
        state.subscribeFromPattern(new HashSet<>(Arrays.asList(topic, topic1)));

        assertEquals(
                "Expected subscribed topics count is incorrect", 2, state.subscription().size());
    }

    @Test
    public void unsubscribeUserAssignment() {
        state.assignFromUser(new HashSet<>(Arrays.asList(tp0, tp1)));
        state.unsubscribe();
        state.subscribe(singleton(topic), rebalanceListener);
        assertEquals(singleton(topic), state.subscription());
    }

    @Test
    public void unsubscribeUserSubscribe() {
        state.subscribe(singleton(topic), rebalanceListener);
        state.unsubscribe();
        state.assignFromUser(singleton(tp0));
        assertEquals(singleton(tp0), state.assignedPartitions());
    }

    @Test
    public void unsubscription() {
        state.subscribe(Pattern.compile(".*"), rebalanceListener);
        state.subscribeFromPattern(new HashSet<>(Arrays.asList(topic, topic1)));
        state.assignFromSubscribed(asList(tp1));
        assertEquals(singleton(tp1), state.assignedPartitions());

        state.unsubscribe();
        assertEquals(0, state.subscription().size());
        assertTrue(state.assignedPartitions().isEmpty());

        state.assignFromUser(singleton(tp0));
        assertEquals(singleton(tp0), state.assignedPartitions());

        state.unsubscribe();
        assertEquals(0, state.subscription().size());
        assertTrue(state.assignedPartitions().isEmpty());
    }

    private static class MockRebalanceListener implements ConsumerRebalanceListener {
        public Collection<TopicPartition> revoked;
        public Collection<TopicPartition> assigned;
        public int revokedCount = 0;
        public int assignedCount = 0;


        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            this.assigned = partitions;
            assignedCount++;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            this.revoked = partitions;
            revokedCount++;
        }

    }

}
