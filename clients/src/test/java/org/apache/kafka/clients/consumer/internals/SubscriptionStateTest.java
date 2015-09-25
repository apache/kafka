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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static java.util.Arrays.asList;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

public class SubscriptionStateTest {

    private final SubscriptionState state = new SubscriptionState(OffsetResetStrategy.EARLIEST);
    private final TopicPartition tp0 = new TopicPartition("test", 0);
    private final TopicPartition tp1 = new TopicPartition("test", 1);
    private final MockRebalanceListener rebalanceListener = new MockRebalanceListener();

    @Test
    public void partitionAssignment() {
        state.assign(Arrays.asList(tp0));
        assertEquals(Collections.singleton(tp0), state.assignedPartitions());
        state.committed(tp0, new OffsetAndMetadata(1));
        state.seek(tp0, 1);
        assertTrue(state.isFetchable(tp0));
        assertAllPositions(tp0, 1L);
        state.assign(Arrays.<TopicPartition>asList());
        assertTrue(state.assignedPartitions().isEmpty());
        assertFalse(state.isAssigned(tp0));
        assertFalse(state.isFetchable(tp0));
    }

    @Test
    public void partitionReset() {
        state.assign(Arrays.asList(tp0));
        state.seek(tp0, 5);
        assertEquals(5L, (long) state.fetched(tp0));
        assertEquals(5L, (long) state.consumed(tp0));
        state.needOffsetReset(tp0);
        assertFalse(state.isFetchable(tp0));
        assertTrue(state.isOffsetResetNeeded(tp0));
        assertEquals(null, state.fetched(tp0));
        assertEquals(null, state.consumed(tp0));

        // seek should clear the reset and make the partition fetchable
        state.seek(tp0, 0);
        assertTrue(state.isFetchable(tp0));
        assertFalse(state.isOffsetResetNeeded(tp0));
    }

    @Test
    public void topicSubscription() {
        state.subscribe(Arrays.asList("test"), rebalanceListener);
        assertEquals(1, state.subscription().size());
        assertTrue(state.assignedPartitions().isEmpty());
        assertTrue(state.partitionsAutoAssigned());
        state.changePartitionAssignment(asList(tp0));
        state.seek(tp0, 1);
        state.committed(tp0, new OffsetAndMetadata(1));
        assertAllPositions(tp0, 1L);
        state.changePartitionAssignment(asList(tp1));
        assertTrue(state.isAssigned(tp1));
        assertFalse(state.isAssigned(tp0));
        assertFalse(state.isFetchable(tp1));
        assertEquals(Collections.singleton(tp1), state.assignedPartitions());
    }

    @Test
    public void partitionPause() {
        state.assign(Arrays.asList(tp0));
        state.seek(tp0, 100);
        assertTrue(state.isFetchable(tp0));
        state.pause(tp0);
        assertFalse(state.isFetchable(tp0));
        state.resume(tp0);
        assertTrue(state.isFetchable(tp0));
    }

    @Test
    public void commitOffsetMetadata() {
        state.assign(Arrays.asList(tp0));
        state.committed(tp0, new OffsetAndMetadata(5, "hi"));

        assertEquals(5, state.committed(tp0).offset());
        assertEquals("hi", state.committed(tp0).metadata());
    }

    @Test
    public void topicUnsubscription() {
        final String topic = "test";
        state.subscribe(Arrays.asList(topic), rebalanceListener);
        assertEquals(1, state.subscription().size());
        assertTrue(state.assignedPartitions().isEmpty());
        assertTrue(state.partitionsAutoAssigned());
        state.changePartitionAssignment(asList(tp0));
        state.committed(tp0, new OffsetAndMetadata(1));
        state.seek(tp0, 1);
        assertAllPositions(tp0, 1L);
        state.changePartitionAssignment(asList(tp1));
        assertFalse(state.isAssigned(tp0));
        assertEquals(Collections.singleton(tp1), state.assignedPartitions());

        state.subscribe(Arrays.<String>asList(), rebalanceListener);
        assertEquals(0, state.subscription().size());
        assertTrue(state.assignedPartitions().isEmpty());
    }

    @Test(expected = IllegalStateException.class)
    public void invalidConsumedPositionUpdate() {
        state.subscribe(Arrays.asList("test"), rebalanceListener);
        state.changePartitionAssignment(asList(tp0));
        state.consumed(tp0, 0);
    }

    @Test(expected = IllegalStateException.class)
    public void invalidFetchPositionUpdate() {
        state.subscribe(Arrays.asList("test"), rebalanceListener);
        state.changePartitionAssignment(asList(tp0));
        state.fetched(tp0, 0);
    }

    @Test(expected = IllegalStateException.class)
    public void cantChangeFetchPositionForNonAssignedPartition() {
        state.fetched(tp0, 1);
    }
    
    @Test(expected = IllegalStateException.class)
    public void cantChangeConsumedPositionForNonAssignedPartition() {
        state.consumed(tp0, 1);
    }
    
    public void assertAllPositions(TopicPartition tp, Long offset) {
        assertEquals(offset.longValue(), state.committed(tp).offset());
        assertEquals(offset, state.fetched(tp));
        assertEquals(offset, state.consumed(tp));
    }

    @Test(expected = IllegalStateException.class)
    public void cantSubscribeTopicAndPattern() {
        state.subscribe(Arrays.asList("test"), rebalanceListener);
        state.subscribe(Pattern.compile(".*"), rebalanceListener);
    }

    @Test(expected = IllegalStateException.class)
    public void cantSubscribePartitionAndPattern() {
        state.assign(Arrays.asList(new TopicPartition("test", 0)));
        state.subscribe(Pattern.compile(".*"), rebalanceListener);
    }

    @Test(expected = IllegalStateException.class)
    public void cantSubscribePatternAndTopic() {
        state.subscribe(Pattern.compile(".*"), rebalanceListener);
        state.subscribe(Arrays.asList("test"), rebalanceListener);
    }

    @Test(expected = IllegalStateException.class)
    public void cantSubscribePatternAndPartition() {
        state.subscribe(Pattern.compile(".*"), rebalanceListener);
        state.assign(Arrays.asList(new TopicPartition("test", 0)));
    }

    @Test
    public void patternSubscription() {
        state.subscribe(Pattern.compile(".*"), rebalanceListener);
        state.changeSubscription(Arrays.asList("test", "test1"));

        assertEquals(
                "Expected subscribed topics count is incorrect", 2, state.subscription().size());
    }

    @Test
    public void patternUnsubscription() {
        state.subscribe(Pattern.compile(".*"), rebalanceListener);
        state.changeSubscription(Arrays.asList("test", "test1"));

        state.unsubscribe();

        assertEquals(
                "Expected subscribed topics count is incorrect", 0, state.subscription().size());
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
