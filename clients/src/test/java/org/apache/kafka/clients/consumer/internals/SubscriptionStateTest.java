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

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.EpochEndOffset;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SubscriptionStateTest {

    private SubscriptionState state = new SubscriptionState(new LogContext(), OffsetResetStrategy.EARLIEST);
    private final String topic = "test";
    private final String topic1 = "test1";
    private final TopicPartition tp0 = new TopicPartition(topic, 0);
    private final TopicPartition tp1 = new TopicPartition(topic, 1);
    private final TopicPartition t1p0 = new TopicPartition(topic1, 0);
    private final MockRebalanceListener rebalanceListener = new MockRebalanceListener();
    private final Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(Node.noNode(), Optional.empty());

    @Test
    public void partitionAssignment() {
        state.assignFromUser(singleton(tp0));
        assertEquals(singleton(tp0), state.assignedPartitions());
        assertEquals(1, state.numAssignedPartitions());
        assertFalse(state.hasAllFetchPositions());
        state.seek(tp0, 1);
        assertTrue(state.isFetchable(tp0));
        assertEquals(1L, state.position(tp0).offset);
        state.assignFromUser(Collections.<TopicPartition>emptySet());
        assertTrue(state.assignedPartitions().isEmpty());
        assertEquals(0, state.numAssignedPartitions());
        assertFalse(state.isAssigned(tp0));
        assertFalse(state.isFetchable(tp0));
    }

    @Test
    public void partitionAssignmentChangeOnTopicSubscription() {
        state.assignFromUser(new HashSet<>(Arrays.asList(tp0, tp1)));
        // assigned partitions should immediately change
        assertEquals(2, state.assignedPartitions().size());
        assertEquals(2, state.numAssignedPartitions());
        assertTrue(state.assignedPartitions().contains(tp0));
        assertTrue(state.assignedPartitions().contains(tp1));

        state.unsubscribe();
        // assigned partitions should immediately change
        assertTrue(state.assignedPartitions().isEmpty());
        assertEquals(0, state.numAssignedPartitions());

        state.subscribe(singleton(topic1), rebalanceListener);
        // assigned partitions should remain unchanged
        assertTrue(state.assignedPartitions().isEmpty());
        assertEquals(0, state.numAssignedPartitions());

        assertTrue(state.assignFromSubscribed(singleton(t1p0)));
        // assigned partitions should immediately change
        assertEquals(singleton(t1p0), state.assignedPartitions());
        assertEquals(1, state.numAssignedPartitions());

        state.subscribe(singleton(topic), rebalanceListener);
        // assigned partitions should remain unchanged
        assertEquals(singleton(t1p0), state.assignedPartitions());
        assertEquals(1, state.numAssignedPartitions());

        state.unsubscribe();
        // assigned partitions should immediately change
        assertTrue(state.assignedPartitions().isEmpty());
        assertEquals(0, state.numAssignedPartitions());
    }

    @Test
    public void partitionAssignmentChangeOnPatternSubscription() {
        state.subscribe(Pattern.compile(".*"), rebalanceListener);
        // assigned partitions should remain unchanged
        assertTrue(state.assignedPartitions().isEmpty());
        assertEquals(0, state.numAssignedPartitions());

        state.subscribeFromPattern(new HashSet<>(Collections.singletonList(topic)));
        // assigned partitions should remain unchanged
        assertTrue(state.assignedPartitions().isEmpty());
        assertEquals(0, state.numAssignedPartitions());

        assertTrue(state.assignFromSubscribed(singleton(tp1)));
        // assigned partitions should immediately change
        assertEquals(singleton(tp1), state.assignedPartitions());
        assertEquals(1, state.numAssignedPartitions());
        assertEquals(singleton(topic), state.subscription());

        assertTrue(state.assignFromSubscribed(Collections.singletonList(t1p0)));
        // assigned partitions should immediately change
        assertEquals(singleton(t1p0), state.assignedPartitions());
        assertEquals(1, state.numAssignedPartitions());
        assertEquals(singleton(topic), state.subscription());

        state.subscribe(Pattern.compile(".*t"), rebalanceListener);
        // assigned partitions should remain unchanged
        assertEquals(singleton(t1p0), state.assignedPartitions());
        assertEquals(1, state.numAssignedPartitions());

        state.subscribeFromPattern(singleton(topic));
        // assigned partitions should remain unchanged
        assertEquals(singleton(t1p0), state.assignedPartitions());
        assertEquals(1, state.numAssignedPartitions());

        assertTrue(state.assignFromSubscribed(Collections.singletonList(tp0)));
        // assigned partitions should immediately change
        assertEquals(singleton(tp0), state.assignedPartitions());
        assertEquals(1, state.numAssignedPartitions());
        assertEquals(singleton(topic), state.subscription());

        state.unsubscribe();
        // assigned partitions should immediately change
        assertTrue(state.assignedPartitions().isEmpty());
        assertEquals(0, state.numAssignedPartitions());
    }

    @Test
    public void verifyAssignmentId() {
        assertEquals(0, state.assignmentId());
        Set<TopicPartition> userAssignment = Utils.mkSet(tp0, tp1);
        state.assignFromUser(userAssignment);
        assertEquals(1, state.assignmentId());
        assertEquals(userAssignment, state.assignedPartitions());

        state.unsubscribe();
        assertEquals(2, state.assignmentId());
        assertEquals(Collections.emptySet(), state.assignedPartitions());

        Set<TopicPartition> autoAssignment = Utils.mkSet(t1p0);
        state.subscribe(singleton(topic1), rebalanceListener);
        assertTrue(state.assignFromSubscribed(autoAssignment));
        assertEquals(3, state.assignmentId());
        assertEquals(autoAssignment, state.assignedPartitions());
    }

    @Test
    public void partitionReset() {
        state.assignFromUser(singleton(tp0));
        state.seek(tp0, 5);
        assertEquals(5L, state.position(tp0).offset);
        state.requestOffsetReset(tp0);
        assertFalse(state.isFetchable(tp0));
        assertTrue(state.isOffsetResetNeeded(tp0));
        assertNotNull(state.position(tp0));

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
        assertEquals(0, state.numAssignedPartitions());
        assertTrue(state.partitionsAutoAssigned());
        assertTrue(state.assignFromSubscribed(singleton(tp0)));
        state.seek(tp0, 1);
        assertEquals(1L, state.position(tp0).offset);
        assertTrue(state.assignFromSubscribed(singleton(tp1)));
        assertTrue(state.isAssigned(tp1));
        assertFalse(state.isAssigned(tp0));
        assertFalse(state.isFetchable(tp1));
        assertEquals(singleton(tp1), state.assignedPartitions());
        assertEquals(1, state.numAssignedPartitions());
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

    @Test(expected = IllegalStateException.class)
    public void invalidPositionUpdate() {
        state.subscribe(singleton(topic), rebalanceListener);
        assertTrue(state.assignFromSubscribed(singleton(tp0)));
        state.position(tp0, new SubscriptionState.FetchPosition(0, Optional.empty(), leaderAndEpoch));
    }

    @Test
    public void cantAssignPartitionForUnsubscribedTopics() {
        state.subscribe(singleton(topic), rebalanceListener);
        assertFalse(state.assignFromSubscribed(Collections.singletonList(t1p0)));
    }

    @Test
    public void cantAssignPartitionForUnmatchedPattern() {
        state.subscribe(Pattern.compile(".*t"), rebalanceListener);
        state.subscribeFromPattern(new HashSet<>(Collections.singletonList(topic)));
        assertFalse(state.assignFromSubscribed(Collections.singletonList(t1p0)));
    }

    @Test(expected = IllegalStateException.class)
    public void cantChangePositionForNonAssignedPartition() {
        state.position(tp0, new SubscriptionState.FetchPosition(1, Optional.empty(), leaderAndEpoch));
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
        assertEquals("Expected subscribed topics count is incorrect", 2, state.subscription().size());
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
        assertEquals(1, state.numAssignedPartitions());
    }

    @Test
    public void unsubscription() {
        state.subscribe(Pattern.compile(".*"), rebalanceListener);
        state.subscribeFromPattern(new HashSet<>(Arrays.asList(topic, topic1)));
        assertTrue(state.assignFromSubscribed(singleton(tp1)));
        assertEquals(singleton(tp1), state.assignedPartitions());
        assertEquals(1, state.numAssignedPartitions());

        state.unsubscribe();
        assertEquals(0, state.subscription().size());
        assertTrue(state.assignedPartitions().isEmpty());
        assertEquals(0, state.numAssignedPartitions());

        state.assignFromUser(singleton(tp0));
        assertEquals(singleton(tp0), state.assignedPartitions());
        assertEquals(1, state.numAssignedPartitions());

        state.unsubscribe();
        assertEquals(0, state.subscription().size());
        assertTrue(state.assignedPartitions().isEmpty());
        assertEquals(0, state.numAssignedPartitions());
    }

    @Test
    public void testPreferredReadReplicaLease() {
        state.assignFromUser(Collections.singleton(tp0));

        // Default state
        assertFalse(state.preferredReadReplica(tp0, 0L).isPresent());

        // Set the preferred replica with lease
        state.updatePreferredReadReplica(tp0, 42, () -> 10L);
        TestUtils.assertOptional(state.preferredReadReplica(tp0, 9L),  value -> assertEquals(value.intValue(), 42));
        TestUtils.assertOptional(state.preferredReadReplica(tp0, 10L),  value -> assertEquals(value.intValue(), 42));
        assertFalse(state.preferredReadReplica(tp0, 11L).isPresent());

        // Unset the preferred replica
        state.clearPreferredReadReplica(tp0);
        assertFalse(state.preferredReadReplica(tp0, 9L).isPresent());
        assertFalse(state.preferredReadReplica(tp0, 11L).isPresent());

        // Set to new preferred replica with lease
        state.updatePreferredReadReplica(tp0, 43, () -> 20L);
        TestUtils.assertOptional(state.preferredReadReplica(tp0, 11L),  value -> assertEquals(value.intValue(), 43));
        TestUtils.assertOptional(state.preferredReadReplica(tp0, 20L),  value -> assertEquals(value.intValue(), 43));
        assertFalse(state.preferredReadReplica(tp0, 21L).isPresent());

        // Set to new preferred replica without clearing first
        state.updatePreferredReadReplica(tp0, 44, () -> 30L);
        TestUtils.assertOptional(state.preferredReadReplica(tp0, 30L),  value -> assertEquals(value.intValue(), 44));
        assertFalse(state.preferredReadReplica(tp0, 31L).isPresent());
    }

    @Test
    public void testSeekUnvalidatedWithNoOffsetEpoch() {
        Node broker1 = new Node(1, "localhost", 9092);
        state.assignFromUser(Collections.singleton(tp0));

        // Seek with no offset epoch requires no validation no matter what the current leader is
        state.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(0L, Optional.empty(),
                new Metadata.LeaderAndEpoch(broker1, Optional.of(5))));
        assertTrue(state.hasValidPosition(tp0));
        assertFalse(state.awaitingValidation(tp0));

        assertFalse(state.maybeValidatePositionForCurrentLeader(tp0, new Metadata.LeaderAndEpoch(broker1, Optional.empty())));
        assertTrue(state.hasValidPosition(tp0));
        assertFalse(state.awaitingValidation(tp0));

        assertFalse(state.maybeValidatePositionForCurrentLeader(tp0, new Metadata.LeaderAndEpoch(broker1, Optional.of(10))));
        assertTrue(state.hasValidPosition(tp0));
        assertFalse(state.awaitingValidation(tp0));
    }

    @Test
    public void testSeekUnvalidatedWithNoEpochClearsAwaitingValidation() {
        Node broker1 = new Node(1, "localhost", 9092);
        state.assignFromUser(Collections.singleton(tp0));

        // Seek with no offset epoch requires no validation no matter what the current leader is
        state.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(0L, Optional.of(2),
                new Metadata.LeaderAndEpoch(broker1, Optional.of(5))));
        assertFalse(state.hasValidPosition(tp0));
        assertTrue(state.awaitingValidation(tp0));

        state.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(0L, Optional.empty(),
                new Metadata.LeaderAndEpoch(broker1, Optional.of(5))));
        assertTrue(state.hasValidPosition(tp0));
        assertFalse(state.awaitingValidation(tp0));
    }

    @Test
    public void testSeekUnvalidatedWithOffsetEpoch() {
        Node broker1 = new Node(1, "localhost", 9092);
        state.assignFromUser(Collections.singleton(tp0));

        state.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(0L, Optional.of(2),
                new Metadata.LeaderAndEpoch(broker1, Optional.of(5))));
        assertFalse(state.hasValidPosition(tp0));
        assertTrue(state.awaitingValidation(tp0));

        // Update using the current leader and epoch
        assertTrue(state.maybeValidatePositionForCurrentLeader(tp0, new Metadata.LeaderAndEpoch(broker1, Optional.of(5))));
        assertFalse(state.hasValidPosition(tp0));
        assertTrue(state.awaitingValidation(tp0));

        // Update with a newer leader and epoch
        assertTrue(state.maybeValidatePositionForCurrentLeader(tp0, new Metadata.LeaderAndEpoch(broker1, Optional.of(15))));
        assertFalse(state.hasValidPosition(tp0));
        assertTrue(state.awaitingValidation(tp0));

        // If the updated leader has no epoch information, then skip validation and begin fetching
        assertFalse(state.maybeValidatePositionForCurrentLeader(tp0, new Metadata.LeaderAndEpoch(broker1, Optional.empty())));
        assertTrue(state.hasValidPosition(tp0));
        assertFalse(state.awaitingValidation(tp0));
    }

    @Test
    public void testSeekValidatedShouldClearAwaitingValidation() {
        Node broker1 = new Node(1, "localhost", 9092);
        state.assignFromUser(Collections.singleton(tp0));

        state.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(10L, Optional.of(5),
                new Metadata.LeaderAndEpoch(broker1, Optional.of(10))));
        assertFalse(state.hasValidPosition(tp0));
        assertTrue(state.awaitingValidation(tp0));
        assertEquals(10L, state.position(tp0).offset);

        state.seekValidated(tp0, new SubscriptionState.FetchPosition(8L, Optional.of(4),
                new Metadata.LeaderAndEpoch(broker1, Optional.of(10))));
        assertTrue(state.hasValidPosition(tp0));
        assertFalse(state.awaitingValidation(tp0));
        assertEquals(8L, state.position(tp0).offset);
    }

    @Test
    public void testCompleteValidationShouldClearAwaitingValidation() {
        Node broker1 = new Node(1, "localhost", 9092);
        state.assignFromUser(Collections.singleton(tp0));

        state.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(10L, Optional.of(5),
                new Metadata.LeaderAndEpoch(broker1, Optional.of(10))));
        assertFalse(state.hasValidPosition(tp0));
        assertTrue(state.awaitingValidation(tp0));
        assertEquals(10L, state.position(tp0).offset);

        state.completeValidation(tp0);
        assertTrue(state.hasValidPosition(tp0));
        assertFalse(state.awaitingValidation(tp0));
        assertEquals(10L, state.position(tp0).offset);
    }

    @Test
    public void testOffsetResetWhileAwaitingValidation() {
        Node broker1 = new Node(1, "localhost", 9092);
        state.assignFromUser(Collections.singleton(tp0));

        state.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(10L, Optional.of(5),
                new Metadata.LeaderAndEpoch(broker1, Optional.of(10))));
        assertTrue(state.awaitingValidation(tp0));

        state.requestOffsetReset(tp0, OffsetResetStrategy.EARLIEST);
        assertFalse(state.awaitingValidation(tp0));
        assertTrue(state.isOffsetResetNeeded(tp0));
    }

    @Test
    public void testMaybeCompleteValidation() {
        Node broker1 = new Node(1, "localhost", 9092);
        state.assignFromUser(Collections.singleton(tp0));

        int currentEpoch = 10;
        long initialOffset = 10L;
        int initialOffsetEpoch = 5;

        SubscriptionState.FetchPosition initialPosition = new SubscriptionState.FetchPosition(initialOffset,
                Optional.of(initialOffsetEpoch), new Metadata.LeaderAndEpoch(broker1, Optional.of(currentEpoch)));
        state.seekUnvalidated(tp0, initialPosition);
        assertTrue(state.awaitingValidation(tp0));

        Optional<OffsetAndMetadata> divergentOffsetMetadataOpt = state.maybeCompleteValidation(tp0, initialPosition,
                new EpochEndOffset(initialOffsetEpoch, initialOffset + 5));
        assertEquals(Optional.empty(), divergentOffsetMetadataOpt);
        assertFalse(state.awaitingValidation(tp0));
        assertEquals(initialPosition, state.position(tp0));
    }

    @Test
    public void testMaybeCompleteValidationAfterPositionChange() {
        Node broker1 = new Node(1, "localhost", 9092);
        state.assignFromUser(Collections.singleton(tp0));

        int currentEpoch = 10;
        long initialOffset = 10L;
        int initialOffsetEpoch = 5;
        long updateOffset = 20L;
        int updateOffsetEpoch = 8;

        SubscriptionState.FetchPosition initialPosition = new SubscriptionState.FetchPosition(initialOffset,
                Optional.of(initialOffsetEpoch), new Metadata.LeaderAndEpoch(broker1, Optional.of(currentEpoch)));
        state.seekUnvalidated(tp0, initialPosition);
        assertTrue(state.awaitingValidation(tp0));

        SubscriptionState.FetchPosition updatePosition = new SubscriptionState.FetchPosition(updateOffset,
                Optional.of(updateOffsetEpoch), new Metadata.LeaderAndEpoch(broker1, Optional.of(currentEpoch)));
        state.seekUnvalidated(tp0, updatePosition);

        Optional<OffsetAndMetadata> divergentOffsetMetadataOpt = state.maybeCompleteValidation(tp0, initialPosition,
                new EpochEndOffset(initialOffsetEpoch, initialOffset + 5));
        assertEquals(Optional.empty(), divergentOffsetMetadataOpt);
        assertTrue(state.awaitingValidation(tp0));
        assertEquals(updatePosition, state.position(tp0));
    }

    @Test
    public void testMaybeCompleteValidationAfterOffsetReset() {
        Node broker1 = new Node(1, "localhost", 9092);
        state.assignFromUser(Collections.singleton(tp0));

        int currentEpoch = 10;
        long initialOffset = 10L;
        int initialOffsetEpoch = 5;

        SubscriptionState.FetchPosition initialPosition = new SubscriptionState.FetchPosition(initialOffset,
                Optional.of(initialOffsetEpoch), new Metadata.LeaderAndEpoch(broker1, Optional.of(currentEpoch)));
        state.seekUnvalidated(tp0, initialPosition);
        assertTrue(state.awaitingValidation(tp0));

        state.requestOffsetReset(tp0);

        Optional<OffsetAndMetadata> divergentOffsetMetadataOpt = state.maybeCompleteValidation(tp0, initialPosition,
                new EpochEndOffset(initialOffsetEpoch, initialOffset + 5));
        assertEquals(Optional.empty(), divergentOffsetMetadataOpt);
        assertFalse(state.awaitingValidation(tp0));
        assertTrue(state.isOffsetResetNeeded(tp0));
    }

    @Test
    public void testTruncationDetectionWithResetPolicy() {
        Node broker1 = new Node(1, "localhost", 9092);
        state.assignFromUser(Collections.singleton(tp0));

        int currentEpoch = 10;
        long initialOffset = 10L;
        int initialOffsetEpoch = 5;
        long divergentOffset = 5L;
        int divergentOffsetEpoch = 7;

        SubscriptionState.FetchPosition initialPosition = new SubscriptionState.FetchPosition(initialOffset,
                Optional.of(initialOffsetEpoch), new Metadata.LeaderAndEpoch(broker1, Optional.of(currentEpoch)));
        state.seekUnvalidated(tp0, initialPosition);
        assertTrue(state.awaitingValidation(tp0));

        Optional<OffsetAndMetadata> divergentOffsetMetadata = state.maybeCompleteValidation(tp0, initialPosition,
                new EpochEndOffset(divergentOffsetEpoch, divergentOffset));
        assertEquals(Optional.empty(), divergentOffsetMetadata);
        assertFalse(state.awaitingValidation(tp0));

        SubscriptionState.FetchPosition updatedPosition = new SubscriptionState.FetchPosition(divergentOffset,
                Optional.of(divergentOffsetEpoch), new Metadata.LeaderAndEpoch(broker1, Optional.of(currentEpoch)));
        assertEquals(updatedPosition, state.position(tp0));
    }

    @Test
    public void testTruncationDetectionWithoutResetPolicy() {
        Node broker1 = new Node(1, "localhost", 9092);
        state = new SubscriptionState(new LogContext(), OffsetResetStrategy.NONE);
        state.assignFromUser(Collections.singleton(tp0));

        int currentEpoch = 10;
        long initialOffset = 10L;
        int initialOffsetEpoch = 5;
        long divergentOffset = 5L;
        int divergentOffsetEpoch = 7;

        SubscriptionState.FetchPosition initialPosition = new SubscriptionState.FetchPosition(initialOffset,
                Optional.of(initialOffsetEpoch), new Metadata.LeaderAndEpoch(broker1, Optional.of(currentEpoch)));
        state.seekUnvalidated(tp0, initialPosition);
        assertTrue(state.awaitingValidation(tp0));

        Optional<OffsetAndMetadata> divergentOffsetMetadata = state.maybeCompleteValidation(tp0, initialPosition,
                new EpochEndOffset(divergentOffsetEpoch, divergentOffset));
        assertEquals(Optional.of(new OffsetAndMetadata(divergentOffset, Optional.of(divergentOffsetEpoch), "")),
                divergentOffsetMetadata);
        assertTrue(state.awaitingValidation(tp0));
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
