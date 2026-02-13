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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.clients.consumer.internals.SubscriptionState.LogTruncation;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH;
import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SubscriptionStateTest {

    private SubscriptionState state = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.EARLIEST);
    private final String topic = "test";
    private final String topic1 = "test1";
    private final TopicPartition tp0 = new TopicPartition(topic, 0);
    private final TopicPartition tp1 = new TopicPartition(topic, 1);
    private final TopicPartition t1p0 = new TopicPartition(topic1, 0);
    private final MockRebalanceListener rebalanceListener = new MockRebalanceListener();
    private final Metadata.LeaderAndEpoch leaderAndEpoch = Metadata.LeaderAndEpoch.noLeaderOrEpoch();

    @Test
    public void partitionAssignment() {
        state.assignFromUser(Set.of(tp0));
        assertEquals(Set.of(tp0), state.assignedPartitions());
        assertEquals(1, state.numAssignedPartitions());
        assertFalse(state.hasAllFetchPositions());
        state.seek(tp0, 1);
        assertTrue(state.isFetchable(tp0));
        assertEquals(1L, state.position(tp0).offset);
        state.assignFromUser(Set.of());
        assertTrue(state.assignedPartitions().isEmpty());
        assertEquals(0, state.numAssignedPartitions());
        assertFalse(state.isAssigned(tp0));
        assertFalse(state.isFetchable(tp0));
    }

    @Test
    public void partitionAssignmentChangeOnTopicSubscription() {
        state.assignFromUser(Set.of(tp0, tp1));
        // assigned partitions should immediately change
        assertEquals(2, state.assignedPartitions().size());
        assertEquals(2, state.numAssignedPartitions());
        assertTrue(state.assignedPartitions().contains(tp0));
        assertTrue(state.assignedPartitions().contains(tp1));

        state.unsubscribe();
        // assigned partitions should immediately change
        assertTrue(state.assignedPartitions().isEmpty());
        assertEquals(0, state.numAssignedPartitions());

        state.subscribe(Set.of(topic1), Optional.of(rebalanceListener));
        // assigned partitions should remain unchanged
        assertTrue(state.assignedPartitions().isEmpty());
        assertEquals(0, state.numAssignedPartitions());

        assertTrue(state.checkAssignmentMatchedSubscription(Set.of(t1p0)));
        state.assignFromSubscribed(Set.of(t1p0));
        // assigned partitions should immediately change
        assertEquals(Set.of(t1p0), state.assignedPartitions());
        assertEquals(1, state.numAssignedPartitions());

        state.subscribe(Set.of(topic), Optional.of(rebalanceListener));
        // assigned partitions should remain unchanged
        assertEquals(Set.of(t1p0), state.assignedPartitions());
        assertEquals(1, state.numAssignedPartitions());

        state.unsubscribe();
        // assigned partitions should immediately change
        assertTrue(state.assignedPartitions().isEmpty());
        assertEquals(0, state.numAssignedPartitions());
    }

    @Test
    public void testIsFetchableOnManualAssignment() {
        state.assignFromUser(Set.of(tp0, tp1));
        assertAssignedPartitionIsFetchable();
    }

    @Test
    public void testIsFetchableOnAutoAssignment() {
        state.subscribe(Set.of(topic), Optional.of(rebalanceListener));
        state.assignFromSubscribed(Set.of(tp0, tp1));
        assertAssignedPartitionIsFetchable();
    }

    private void assertAssignedPartitionIsFetchable() {
        assertEquals(2, state.assignedPartitions().size());
        assertTrue(state.assignedPartitions().contains(tp0));
        assertTrue(state.assignedPartitions().contains(tp1));

        assertFalse(state.isFetchable(tp0), "Should not be fetchable without a valid position");
        assertFalse(state.isFetchable(tp1), "Should not be fetchable without a valid position");

        state.seek(tp0, 1);
        state.seek(tp1, 1);

        assertTrue(state.isFetchable(tp0));
        assertTrue(state.isFetchable(tp1));
    }

    @Test
    public void testIsFetchableConsidersExplicitTopicSubscription() {
        state.subscribe(Set.of(topic1), Optional.of(rebalanceListener));
        state.assignFromSubscribed(Set.of(t1p0));
        state.seek(t1p0, 1);

        assertEquals(Set.of(t1p0), state.assignedPartitions());
        assertTrue(state.isFetchable(t1p0));

        // Change subscription. Assigned partitions should remain unchanged but not fetchable.
        state.subscribe(Set.of(topic), Optional.of(rebalanceListener));
        assertEquals(Set.of(t1p0), state.assignedPartitions());
        assertFalse(state.isFetchable(t1p0), "Assigned partitions not in the subscription should not be fetchable");

        // Unsubscribe. Assigned partitions should be cleared and not fetchable.
        state.unsubscribe();
        assertTrue(state.assignedPartitions().isEmpty());
        assertFalse(state.isFetchable(t1p0));
    }

    @Test
    public void testGroupSubscribe() {
        state.subscribe(Set.of(topic1), Optional.of(rebalanceListener));
        assertEquals(Set.of(topic1), state.metadataTopics());

        assertFalse(state.groupSubscribe(Set.of(topic1)));
        assertEquals(Set.of(topic1), state.metadataTopics());

        assertTrue(state.groupSubscribe(Set.of(topic, topic1)));
        assertEquals(Set.of(topic, topic1), state.metadataTopics());

        // `groupSubscribe` does not accumulate
        assertFalse(state.groupSubscribe(Set.of(topic1)));
        assertEquals(Set.of(topic1), state.metadataTopics());

        state.subscribe(Set.of("anotherTopic"), Optional.of(rebalanceListener));
        assertEquals(Set.of(topic1, "anotherTopic"), state.metadataTopics());

        assertFalse(state.groupSubscribe(Set.of("anotherTopic")));
        assertEquals(Set.of("anotherTopic"), state.metadataTopics());
    }

    @Test
    public void partitionAssignmentChangeOnPatternSubscription() {
        state.subscribe(Pattern.compile(".*"), Optional.of(rebalanceListener));
        // assigned partitions should remain unchanged
        assertTrue(state.assignedPartitions().isEmpty());
        assertEquals(0, state.numAssignedPartitions());

        state.subscribeFromPattern(Set.of(topic));
        // assigned partitions should remain unchanged
        assertTrue(state.assignedPartitions().isEmpty());
        assertEquals(0, state.numAssignedPartitions());

        assertTrue(state.checkAssignmentMatchedSubscription(Set.of(tp1)));
        state.assignFromSubscribed(Set.of(tp1));

        // assigned partitions should immediately change
        assertEquals(Set.of(tp1), state.assignedPartitions());
        assertEquals(1, state.numAssignedPartitions());
        assertEquals(Set.of(topic), state.subscription());

        assertTrue(state.checkAssignmentMatchedSubscription(Set.of(t1p0)));
        state.assignFromSubscribed(Set.of(t1p0));

        // assigned partitions should immediately change
        assertEquals(Set.of(t1p0), state.assignedPartitions());
        assertEquals(1, state.numAssignedPartitions());
        assertEquals(Set.of(topic), state.subscription());

        state.subscribe(Pattern.compile(".*t"), Optional.of(rebalanceListener));
        // assigned partitions should remain unchanged
        assertEquals(Set.of(t1p0), state.assignedPartitions());
        assertEquals(1, state.numAssignedPartitions());

        state.subscribeFromPattern(Set.of(topic));
        // assigned partitions should remain unchanged
        assertEquals(Set.of(t1p0), state.assignedPartitions());
        assertEquals(1, state.numAssignedPartitions());

        assertTrue(state.checkAssignmentMatchedSubscription(Set.of(tp0)));
        state.assignFromSubscribed(Set.of(tp0));

        // assigned partitions should immediately change
        assertEquals(Set.of(tp0), state.assignedPartitions());
        assertEquals(1, state.numAssignedPartitions());
        assertEquals(Set.of(topic), state.subscription());

        state.unsubscribe();
        // assigned partitions should immediately change
        assertTrue(state.assignedPartitions().isEmpty());
        assertEquals(0, state.numAssignedPartitions());
    }

    @Test
    public void verifyAssignmentId() {
        assertEquals(0, state.assignmentId());
        Set<TopicPartition> userAssignment = Set.of(tp0, tp1);
        state.assignFromUser(userAssignment);
        assertEquals(1, state.assignmentId());
        assertEquals(userAssignment, state.assignedPartitions());

        state.unsubscribe();
        assertEquals(2, state.assignmentId());
        assertEquals(Set.of(), state.assignedPartitions());

        Set<TopicPartition> autoAssignment = Set.of(t1p0);
        state.subscribe(Set.of(topic1), Optional.of(rebalanceListener));
        assertTrue(state.checkAssignmentMatchedSubscription(autoAssignment));
        state.assignFromSubscribed(autoAssignment);
        assertEquals(3, state.assignmentId());
        assertEquals(autoAssignment, state.assignedPartitions());
    }

    @Test
    public void partitionReset() {
        state.assignFromUser(Set.of(tp0));
        state.seek(tp0, 5);
        assertEquals(5L, state.position(tp0).offset);
        state.requestOffsetReset(tp0);
        assertFalse(state.isFetchable(tp0));
        assertTrue(state.isOffsetResetNeeded(tp0));
        assertNull(state.position(tp0));

        // seek should clear the reset and make the partition fetchable
        state.seek(tp0, 0);
        assertTrue(state.isFetchable(tp0));
        assertFalse(state.isOffsetResetNeeded(tp0));
    }

    @Test
    public void topicSubscription() {
        state.subscribe(Set.of(topic), Optional.of(rebalanceListener));
        assertEquals(1, state.subscription().size());
        assertTrue(state.assignedPartitions().isEmpty());
        assertEquals(0, state.numAssignedPartitions());
        assertTrue(state.hasAutoAssignedPartitions());
        assertTrue(state.checkAssignmentMatchedSubscription(Set.of(tp0)));
        state.assignFromSubscribed(Set.of(tp0));

        state.seek(tp0, 1);
        assertEquals(1L, state.position(tp0).offset);
        assertTrue(state.checkAssignmentMatchedSubscription(Set.of(tp1)));
        state.assignFromSubscribed(Set.of(tp1));

        assertTrue(state.isAssigned(tp1));
        assertFalse(state.isAssigned(tp0));
        assertFalse(state.isFetchable(tp1));
        assertEquals(Set.of(tp1), state.assignedPartitions());
        assertEquals(1, state.numAssignedPartitions());
    }

    @Test
    public void partitionPause() {
        state.assignFromUser(Set.of(tp0));
        state.seek(tp0, 100);
        assertTrue(state.isFetchable(tp0));
        state.pause(tp0);
        assertFalse(state.isFetchable(tp0));
        state.resume(tp0);
        assertTrue(state.isFetchable(tp0));
    }

    @Test
    public void testMarkingPendingRevocation() {
        state.assignFromUser(Set.of(tp0));
        state.seek(tp0, 100);
        assertTrue(state.isFetchable(tp0));
        assertFalse(state.isPaused(tp0));
        state.markPendingRevocation(Set.of(tp0));
        assertFalse(state.isFetchable(tp0));
        assertFalse(state.isPaused(tp0));
    }

    @Test
    public void testMarkingPendingRevocationPreventsInitializingPosition() {
        state.assignFromUser(Set.of(tp0));
        assertTrue(state.initializingPartitions().contains(tp0));
        state.markPendingRevocation(Set.of(tp0));
        assertFalse(state.initializingPartitions().contains(tp0));
    }

    @Test
    public void testAssignedPartitionsAwaitingCallbackKeepPositionDefinedInCallback() {
        // New partition assigned. Should not be fetchable or initializing positions.
        state.subscribe(Set.of(topic), Optional.of(rebalanceListener));
        state.assignFromSubscribedAwaitingCallback(Set.of(tp0), Set.of(tp0));
        assertAssignmentAppliedAwaitingCallback(tp0);
        assertEquals(Set.of(tp0.topic()), state.subscription());

        // Simulate callback setting position to start fetching from
        state.seek(tp0, 100);

        // Callback completed. Partition should be fetchable, and should not require
        // initializing positions (position already defined in the callback)
        state.enablePartitionsAwaitingCallback(Set.of(tp0));
        assertEquals(0, state.initializingPartitions().size());
        assertTrue(state.isFetchable(tp0));
        assertTrue(state.hasAllFetchPositions());
        assertEquals(100L, state.position(tp0).offset);
    }

    @Test
    public void testAssignedPartitionsAwaitingCallbackInitializePositionsWhenCallbackCompletes() {
        // New partition assigned. Should not be fetchable or initializing positions.
        state.subscribe(Set.of(topic), Optional.of(rebalanceListener));
        state.assignFromSubscribedAwaitingCallback(Set.of(tp0), Set.of(tp0));
        assertAssignmentAppliedAwaitingCallback(tp0);
        assertEquals(Set.of(tp0.topic()), state.subscription());

        // Callback completed (without updating positions). Partition should require initializing
        // positions, and start fetching once a valid position is set.
        state.enablePartitionsAwaitingCallback(Set.of(tp0));
        assertEquals(1, state.initializingPartitions().size());
        state.seek(tp0, 100);
        assertTrue(state.isFetchable(tp0));
        assertTrue(state.hasAllFetchPositions());
        assertEquals(100L, state.position(tp0).offset);
    }

    @Test
    public void testAssignedPartitionsAwaitingCallbackDoesNotAffectPreviouslyOwnedPartitions() {
        // First partition assigned and callback completes.
        state.subscribe(Set.of(topic), Optional.of(rebalanceListener));
        state.assignFromSubscribedAwaitingCallback(Set.of(tp0), Set.of(tp0));
        assertAssignmentAppliedAwaitingCallback(tp0);
        assertEquals(Set.of(tp0.topic()), state.subscription());
        state.enablePartitionsAwaitingCallback(Set.of(tp0));
        state.seek(tp0, 100);
        assertTrue(state.isFetchable(tp0));

        // New partition added to the assignment. Owned partitions should continue to be
        // fetchable, while the newly added should not be fetchable until callback completes.
        state.assignFromSubscribedAwaitingCallback(Set.of(tp0, tp1), Set.of(tp1));
        assertTrue(state.isFetchable(tp0));
        assertFalse(state.isFetchable(tp1));
        assertEquals(1, state.initializingPartitions().size());

        // Callback completed. Added partition be initializing positions and become fetchable when it gets one.
        state.enablePartitionsAwaitingCallback(Set.of(tp1));
        assertEquals(1, state.initializingPartitions().size());
        assertEquals(tp1, state.initializingPartitions().iterator().next());
        state.seek(tp1, 200);
        assertTrue(state.isFetchable(tp1));
    }

    private void assertAssignmentAppliedAwaitingCallback(TopicPartition topicPartition) {
        assertEquals(Set.of(topicPartition), state.assignedPartitions());
        assertEquals(1, state.numAssignedPartitions());

        assertFalse(state.isFetchable(topicPartition));
        assertEquals(1, state.initializingPartitions().size());
        assertFalse(state.isPaused(topicPartition));
    }

    @Test
    public void invalidPositionUpdate() {
        state.subscribe(Set.of(topic), Optional.of(rebalanceListener));
        assertTrue(state.checkAssignmentMatchedSubscription(Set.of(tp0)));
        state.assignFromSubscribed(Set.of(tp0));

        assertThrows(IllegalStateException.class, () -> state.position(tp0,
            new SubscriptionState.FetchPosition(0, Optional.empty(), leaderAndEpoch)));
    }

    @Test
    public void cantAssignPartitionForUnsubscribedTopics() {
        state.subscribe(Set.of(topic), Optional.of(rebalanceListener));
        assertFalse(state.checkAssignmentMatchedSubscription(List.of(t1p0)));
    }

    @Test
    public void cantAssignPartitionForUnmatchedPattern() {
        state.subscribe(Pattern.compile(".*t"), Optional.of(rebalanceListener));
        state.subscribeFromPattern(Set.of(topic));
        assertFalse(state.checkAssignmentMatchedSubscription(List.of(t1p0)));
    }

    @Test
    public void cantChangePositionForNonAssignedPartition() {
        assertThrows(IllegalStateException.class, () -> state.position(tp0,
            new SubscriptionState.FetchPosition(1, Optional.empty(), leaderAndEpoch)));
    }

    @Test
    public void cantSubscribeTopicAndPattern() {
        state.subscribe(Set.of(topic), Optional.of(rebalanceListener));
        assertThrows(IllegalStateException.class, () -> state.subscribe(Pattern.compile(".*"), Optional.of(rebalanceListener)));
    }

    @Test
    public void cantSubscribePartitionAndPattern() {
        state.assignFromUser(Set.of(tp0));
        assertThrows(IllegalStateException.class, () -> state.subscribe(Pattern.compile(".*"), Optional.of(rebalanceListener)));
    }

    @Test
    public void cantSubscribePatternAndTopic() {
        state.subscribe(Pattern.compile(".*"), Optional.of(rebalanceListener));
        assertThrows(IllegalStateException.class, () -> state.subscribe(Set.of(topic), Optional.of(rebalanceListener)));
    }

    @Test
    public void cantSubscribePatternAndPartition() {
        state.subscribe(Pattern.compile(".*"), Optional.of(rebalanceListener));
        assertThrows(IllegalStateException.class, () -> state.assignFromUser(Set.of(tp0)));
    }

    @Test
    public void patternSubscription() {
        state.subscribe(Pattern.compile(".*"), Optional.of(rebalanceListener));
        state.subscribeFromPattern(Set.of(topic, topic1));
        assertEquals(2, state.subscription().size(), "Expected subscribed topics count is incorrect");
    }

    @Test
    public void testSubscribeToRe2JPattern() {
        String pattern = "t.*";
        state.subscribe(new SubscriptionPattern(pattern), Optional.of(rebalanceListener));
        assertTrue(state.toString().contains("type=AUTO_PATTERN_RE2J"));
        assertTrue(state.toString().contains("subscribedPattern=" + pattern));
        assertTrue(state.assignedTopicIds().isEmpty());
    }

    @Test
    public void testIsAssignedFromRe2j() {
        assertFalse(state.isAssignedFromRe2j(null));
        Uuid assignedUuid = Uuid.randomUuid();
        assertFalse(state.isAssignedFromRe2j(assignedUuid));

        state.subscribe(new SubscriptionPattern("foo.*"), Optional.empty());
        assertTrue(state.hasRe2JPatternSubscription());
        assertFalse(state.isAssignedFromRe2j(assignedUuid));

        state.setAssignedTopicIds(Set.of(assignedUuid));
        assertTrue(state.isAssignedFromRe2j(assignedUuid));

        state.unsubscribe();
        assertFalse(state.isAssignedFromRe2j(assignedUuid));
        assertFalse(state.hasRe2JPatternSubscription());

    }

    @Test
    public void testAssignedPartitionsWithTopicIdsForRe2Pattern() {
        state.subscribe(new SubscriptionPattern("t.*"), Optional.of(rebalanceListener));
        assertTrue(state.assignedTopicIds().isEmpty());

        TopicIdPartitionSet reconciledAssignmentFromRegex = new TopicIdPartitionSet();
        reconciledAssignmentFromRegex.addAll(Uuid.randomUuid(), topic, Set.of(0));
        state.assignFromSubscribedAwaitingCallback(Set.of(tp0), Set.of(tp0));
        assertAssignmentAppliedAwaitingCallback(tp0);

        // Simulate callback setting position to start fetching from
        state.seek(tp0, 100);

        // Callback completed. Partition should be fetchable, from the position previously defined
        state.enablePartitionsAwaitingCallback(Set.of(tp0));
        assertEquals(0, state.initializingPartitions().size());
        assertTrue(state.isFetchable(tp0));
        assertTrue(state.hasAllFetchPositions());
        assertEquals(100L, state.position(tp0).offset);
    }

    @Test
    public void testAssignedTopicIdsPreservedWhenReconciliationCompletes() {
        state.subscribe(new SubscriptionPattern("t.*"), Optional.of(rebalanceListener));
        assertTrue(state.assignedTopicIds().isEmpty());

        // First assignment received from coordinator
        Uuid firstAssignedUuid = Uuid.randomUuid();
        state.setAssignedTopicIds(Set.of(firstAssignedUuid));

        // Second assignment received from coordinator (while the 1st still be reconciling)
        Uuid secondAssignedUuid = Uuid.randomUuid();
        state.setAssignedTopicIds(Set.of(firstAssignedUuid, secondAssignedUuid));

        // First reconciliation completes and updates the subscription state
        state.assignFromSubscribedAwaitingCallback(Set.of(tp0), Set.of(tp0));

        // First assignment should have been applied
        assertAssignmentAppliedAwaitingCallback(tp0);

        // Assigned topic IDs should still have both topics (one reconciled, one not reconciled yet)
        assertEquals(
                Set.of(firstAssignedUuid, secondAssignedUuid),
                state.assignedTopicIds(),
                "Updating the subscription state when a reconciliation completes " +
                        "should not overwrite assigned topics that have not been reconciled yet"
        );
    }

    @Test
    public void testMixedPatternSubscriptionNotAllowed() {
        state.subscribe(Pattern.compile(".*"), Optional.of(rebalanceListener));
        assertThrows(IllegalStateException.class, () -> state.subscribe(new SubscriptionPattern("t.*"),
            Optional.of(rebalanceListener)));

        state.unsubscribe();

        state.subscribe(new SubscriptionPattern("t.*"), Optional.of(rebalanceListener));
        assertThrows(IllegalStateException.class, () -> state.subscribe(Pattern.compile(".*"), Optional.of(rebalanceListener)));
    }

    @Test
    public void testSubscriptionPattern() {
        SubscriptionPattern pattern = new SubscriptionPattern("t.*");
        state.subscribe(pattern, Optional.of(rebalanceListener));
        assertTrue(state.hasRe2JPatternSubscription());
        assertEquals(pattern, state.subscriptionPattern());
        assertTrue(state.hasAutoAssignedPartitions());

        state.unsubscribe();
        assertFalse(state.hasRe2JPatternSubscription());
        assertNull(state.subscriptionPattern());
    }


    @Test
    public void unsubscribeUserAssignment() {
        state.assignFromUser(Set.of(tp0, tp1));
        state.unsubscribe();
        state.subscribe(Set.of(topic), Optional.of(rebalanceListener));
        assertEquals(Set.of(topic), state.subscription());
    }

    @Test
    public void unsubscribeUserSubscribe() {
        state.subscribe(Set.of(topic), Optional.of(rebalanceListener));
        state.unsubscribe();
        state.assignFromUser(Set.of(tp0));
        assertEquals(Set.of(tp0), state.assignedPartitions());
        assertEquals(1, state.numAssignedPartitions());
    }

    @Test
    public void unsubscription() {
        state.subscribe(Pattern.compile(".*"), Optional.of(rebalanceListener));
        state.subscribeFromPattern(Set.of(topic, topic1));
        assertTrue(state.checkAssignmentMatchedSubscription(Set.of(tp1)));
        state.assignFromSubscribed(Set.of(tp1));

        assertEquals(Set.of(tp1), state.assignedPartitions());
        assertEquals(1, state.numAssignedPartitions());

        state.unsubscribe();
        assertEquals(0, state.subscription().size());
        assertTrue(state.assignedPartitions().isEmpty());
        assertEquals(0, state.numAssignedPartitions());

        state.assignFromUser(Set.of(tp0));
        assertEquals(Set.of(tp0), state.assignedPartitions());
        assertEquals(1, state.numAssignedPartitions());

        state.unsubscribe();
        assertEquals(0, state.subscription().size());
        assertTrue(state.assignedPartitions().isEmpty());
        assertEquals(0, state.numAssignedPartitions());
    }

    @Test
    public void testPreferredReadReplicaLease() {
        state.assignFromUser(Set.of(tp0));

        // Default state
        assertFalse(state.preferredReadReplica(tp0, 0L).isPresent());

        // Set the preferred replica with lease
        state.updatePreferredReadReplica(tp0, 42, () -> 10L);
        TestUtils.assertOptional(state.preferredReadReplica(tp0, 9L),  value -> assertEquals(42, value.intValue()));
        TestUtils.assertOptional(state.preferredReadReplica(tp0, 10L),  value -> assertEquals(42, value.intValue()));
        assertFalse(state.preferredReadReplica(tp0, 11L).isPresent());

        // Unset the preferred replica
        state.clearPreferredReadReplica(tp0);
        assertFalse(state.preferredReadReplica(tp0, 9L).isPresent());
        assertFalse(state.preferredReadReplica(tp0, 11L).isPresent());

        // Set to new preferred replica with lease
        state.updatePreferredReadReplica(tp0, 43, () -> 20L);
        TestUtils.assertOptional(state.preferredReadReplica(tp0, 11L),  value -> assertEquals(43, value.intValue()));
        TestUtils.assertOptional(state.preferredReadReplica(tp0, 20L),  value -> assertEquals(43, value.intValue()));
        assertFalse(state.preferredReadReplica(tp0, 21L).isPresent());

        // Set to new preferred replica without clearing first
        state.updatePreferredReadReplica(tp0, 44, () -> 30L);
        TestUtils.assertOptional(state.preferredReadReplica(tp0, 30L),  value -> assertEquals(44, value.intValue()));
        assertFalse(state.preferredReadReplica(tp0, 31L).isPresent());
    }

    @Test
    public void testSeekUnvalidatedWithNoOffsetEpoch() {
        Node broker1 = new Node(1, "localhost", 9092);
        state.assignFromUser(Set.of(tp0));

        // Seek with no offset epoch requires no validation no matter what the current leader is
        state.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(0L, Optional.empty(),
                new Metadata.LeaderAndEpoch(Optional.of(broker1), Optional.of(5))));
        assertTrue(state.hasValidPosition(tp0));
        assertFalse(state.awaitingValidation(tp0));
        ApiVersions apiVersions = new ApiVersions();
        apiVersions.update(broker1.idString(), NodeApiVersions.create());

        assertFalse(state.maybeValidatePositionForCurrentLeader(apiVersions, tp0, new Metadata.LeaderAndEpoch(
                Optional.of(broker1), Optional.empty())));
        assertTrue(state.hasValidPosition(tp0));
        assertFalse(state.awaitingValidation(tp0));

        assertFalse(state.maybeValidatePositionForCurrentLeader(apiVersions, tp0, new Metadata.LeaderAndEpoch(
                Optional.of(broker1), Optional.of(10))));
        assertTrue(state.hasValidPosition(tp0));
        assertFalse(state.awaitingValidation(tp0));
    }

    @Test
    public void testSeekUnvalidatedWithNoEpochClearsAwaitingValidation() {
        Node broker1 = new Node(1, "localhost", 9092);
        state.assignFromUser(Set.of(tp0));

        // Seek with no offset epoch requires no validation no matter what the current leader is
        state.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(0L, Optional.of(2),
                new Metadata.LeaderAndEpoch(Optional.of(broker1), Optional.of(5))));
        assertFalse(state.hasValidPosition(tp0));
        assertTrue(state.awaitingValidation(tp0));

        state.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(0L, Optional.empty(),
                new Metadata.LeaderAndEpoch(Optional.of(broker1), Optional.of(5))));
        assertTrue(state.hasValidPosition(tp0));
        assertFalse(state.awaitingValidation(tp0));
    }

    @Test
    public void testSeekUnvalidatedWithOffsetEpoch() {
        Node broker1 = new Node(1, "localhost", 9092);
        ApiVersions apiVersions = new ApiVersions();
        apiVersions.update(broker1.idString(), NodeApiVersions.create());

        state.assignFromUser(Set.of(tp0));

        state.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(0L, Optional.of(2),
                new Metadata.LeaderAndEpoch(Optional.of(broker1), Optional.of(5))));
        assertFalse(state.hasValidPosition(tp0));
        assertTrue(state.awaitingValidation(tp0));

        // Update using the current leader and epoch
        assertTrue(state.maybeValidatePositionForCurrentLeader(apiVersions, tp0, new Metadata.LeaderAndEpoch(
                Optional.of(broker1), Optional.of(5))));
        assertFalse(state.hasValidPosition(tp0));
        assertTrue(state.awaitingValidation(tp0));

        // Update with a newer leader and epoch
        assertTrue(state.maybeValidatePositionForCurrentLeader(apiVersions, tp0, new Metadata.LeaderAndEpoch(
                Optional.of(broker1), Optional.of(15))));
        assertFalse(state.hasValidPosition(tp0));
        assertTrue(state.awaitingValidation(tp0));

        // If the updated leader has no epoch information, then skip validation and begin fetching
        assertFalse(state.maybeValidatePositionForCurrentLeader(apiVersions, tp0, new Metadata.LeaderAndEpoch(
                Optional.of(broker1), Optional.empty())));
        assertTrue(state.hasValidPosition(tp0));
        assertFalse(state.awaitingValidation(tp0));
    }

    @Test
    public void testSeekValidatedShouldClearAwaitingValidation() {
        Node broker1 = new Node(1, "localhost", 9092);
        state.assignFromUser(Set.of(tp0));

        state.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(10L, Optional.of(5),
                new Metadata.LeaderAndEpoch(Optional.of(broker1), Optional.of(10))));
        assertFalse(state.hasValidPosition(tp0));
        assertTrue(state.awaitingValidation(tp0));
        assertEquals(10L, state.position(tp0).offset);

        state.seekValidated(tp0, new SubscriptionState.FetchPosition(8L, Optional.of(4),
                new Metadata.LeaderAndEpoch(Optional.of(broker1), Optional.of(10))));
        assertTrue(state.hasValidPosition(tp0));
        assertFalse(state.awaitingValidation(tp0));
        assertEquals(8L, state.position(tp0).offset);
    }

    @Test
    public void testCompleteValidationShouldClearAwaitingValidation() {
        Node broker1 = new Node(1, "localhost", 9092);
        state.assignFromUser(Set.of(tp0));

        state.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(10L, Optional.of(5),
                new Metadata.LeaderAndEpoch(Optional.of(broker1), Optional.of(10))));
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
        state.assignFromUser(Set.of(tp0));

        state.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(10L, Optional.of(5),
                new Metadata.LeaderAndEpoch(Optional.of(broker1), Optional.of(10))));
        assertTrue(state.awaitingValidation(tp0));

        state.requestOffsetReset(tp0, AutoOffsetResetStrategy.EARLIEST);
        assertFalse(state.awaitingValidation(tp0));
        assertTrue(state.isOffsetResetNeeded(tp0));
    }

    @Test
    public void testMaybeCompleteValidation() {
        Node broker1 = new Node(1, "localhost", 9092);
        state.assignFromUser(Set.of(tp0));

        int currentEpoch = 10;
        long initialOffset = 10L;
        int initialOffsetEpoch = 5;

        SubscriptionState.FetchPosition initialPosition = new SubscriptionState.FetchPosition(initialOffset,
                Optional.of(initialOffsetEpoch), new Metadata.LeaderAndEpoch(Optional.of(broker1), Optional.of(currentEpoch)));
        state.seekUnvalidated(tp0, initialPosition);
        assertTrue(state.awaitingValidation(tp0));

        Optional<LogTruncation> truncationOpt = state.maybeCompleteValidation(tp0, initialPosition,
                new EpochEndOffset()
                    .setLeaderEpoch(initialOffsetEpoch)
                    .setEndOffset(initialOffset + 5));
        assertEquals(Optional.empty(), truncationOpt);
        assertFalse(state.awaitingValidation(tp0));
        assertEquals(initialPosition, state.position(tp0));
    }

    @Test
    public void testMaybeValidatePositionForCurrentLeader() {
        NodeApiVersions oldApis = NodeApiVersions.create(ApiKeys.OFFSET_FOR_LEADER_EPOCH.id, (short) 0, (short) 2);
        ApiVersions apiVersions = new ApiVersions();
        apiVersions.update("1", oldApis);

        Node broker1 = new Node(1, "localhost", 9092);
        state.assignFromUser(Set.of(tp0));

        state.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(10L, Optional.of(5),
                new Metadata.LeaderAndEpoch(Optional.of(broker1), Optional.of(10))));

        // if API is too old to be usable, we just skip validation
        assertFalse(state.maybeValidatePositionForCurrentLeader(apiVersions, tp0, new Metadata.LeaderAndEpoch(
                Optional.of(broker1), Optional.of(10))));
        assertTrue(state.hasValidPosition(tp0));

        // New API
        apiVersions.update("1", NodeApiVersions.create());
        state.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(10L, Optional.of(5),
                new Metadata.LeaderAndEpoch(Optional.of(broker1), Optional.of(10))));

        // API is too old to be usable, we just skip validation
        assertTrue(state.maybeValidatePositionForCurrentLeader(apiVersions, tp0, new Metadata.LeaderAndEpoch(
                Optional.of(broker1), Optional.of(10))));
        assertFalse(state.hasValidPosition(tp0));

        // tp1 is not part of the subscription, so validation should be skipped.
        assertFalse(state.maybeValidatePositionForCurrentLeader(apiVersions, tp1, new Metadata.LeaderAndEpoch(
            Optional.of(broker1), Optional.of(10))));
        assertFalse(state.assignedPartitions().contains(tp1));
    }

    @Test
    public void testMaybeCompleteValidationAfterPositionChange() {
        Node broker1 = new Node(1, "localhost", 9092);
        state.assignFromUser(Set.of(tp0));

        int currentEpoch = 10;
        long initialOffset = 10L;
        int initialOffsetEpoch = 5;
        long updateOffset = 20L;
        int updateOffsetEpoch = 8;

        SubscriptionState.FetchPosition initialPosition = new SubscriptionState.FetchPosition(initialOffset,
                Optional.of(initialOffsetEpoch), new Metadata.LeaderAndEpoch(Optional.of(broker1), Optional.of(currentEpoch)));
        state.seekUnvalidated(tp0, initialPosition);
        assertTrue(state.awaitingValidation(tp0));

        SubscriptionState.FetchPosition updatePosition = new SubscriptionState.FetchPosition(updateOffset,
                Optional.of(updateOffsetEpoch), new Metadata.LeaderAndEpoch(Optional.of(broker1), Optional.of(currentEpoch)));
        state.seekUnvalidated(tp0, updatePosition);

        Optional<LogTruncation> truncationOpt = state.maybeCompleteValidation(tp0, initialPosition,
                new EpochEndOffset()
                    .setLeaderEpoch(initialOffsetEpoch)
                    .setEndOffset(initialOffset + 5));
        assertEquals(Optional.empty(), truncationOpt);
        assertTrue(state.awaitingValidation(tp0));
        assertEquals(updatePosition, state.position(tp0));
    }

    @Test
    public void testMaybeCompleteValidationAfterOffsetReset() {
        Node broker1 = new Node(1, "localhost", 9092);
        state.assignFromUser(Set.of(tp0));

        int currentEpoch = 10;
        long initialOffset = 10L;
        int initialOffsetEpoch = 5;

        SubscriptionState.FetchPosition initialPosition = new SubscriptionState.FetchPosition(initialOffset,
            Optional.of(initialOffsetEpoch), new Metadata.LeaderAndEpoch(Optional.of(broker1), Optional.of(currentEpoch)));
        state.seekUnvalidated(tp0, initialPosition);
        assertTrue(state.awaitingValidation(tp0));

        state.requestOffsetReset(tp0);

        Optional<LogTruncation> truncationOpt = state.maybeCompleteValidation(tp0, initialPosition,
                new EpochEndOffset()
                    .setLeaderEpoch(initialOffsetEpoch)
                    .setEndOffset(initialOffset + 5));
        assertEquals(Optional.empty(), truncationOpt);
        assertFalse(state.awaitingValidation(tp0));
        assertTrue(state.isOffsetResetNeeded(tp0));
        assertNull(state.position(tp0));
    }

    @Test
    public void testTruncationDetectionWithResetPolicy() {
        Node broker1 = new Node(1, "localhost", 9092);
        state.assignFromUser(Set.of(tp0));

        int currentEpoch = 10;
        long initialOffset = 10L;
        int initialOffsetEpoch = 5;
        long divergentOffset = 5L;
        int divergentOffsetEpoch = 7;

        SubscriptionState.FetchPosition initialPosition = new SubscriptionState.FetchPosition(initialOffset,
                Optional.of(initialOffsetEpoch), new Metadata.LeaderAndEpoch(Optional.of(broker1), Optional.of(currentEpoch)));
        state.seekUnvalidated(tp0, initialPosition);
        assertTrue(state.awaitingValidation(tp0));

        Optional<LogTruncation> truncationOpt = state.maybeCompleteValidation(tp0, initialPosition,
                new EpochEndOffset()
                    .setLeaderEpoch(divergentOffsetEpoch)
                    .setEndOffset(divergentOffset));
        assertEquals(Optional.empty(), truncationOpt);
        assertFalse(state.awaitingValidation(tp0));

        SubscriptionState.FetchPosition updatedPosition = new SubscriptionState.FetchPosition(divergentOffset,
                Optional.of(divergentOffsetEpoch), new Metadata.LeaderAndEpoch(Optional.of(broker1), Optional.of(currentEpoch)));
        assertEquals(updatedPosition, state.position(tp0));
    }

    @Test
    public void testTruncationDetectionWithoutResetPolicy() {
        Node broker1 = new Node(1, "localhost", 9092);
        state = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.NONE);
        state.assignFromUser(Set.of(tp0));

        int currentEpoch = 10;
        long initialOffset = 10L;
        int initialOffsetEpoch = 5;
        long divergentOffset = 5L;
        int divergentOffsetEpoch = 7;

        SubscriptionState.FetchPosition initialPosition = new SubscriptionState.FetchPosition(initialOffset,
                Optional.of(initialOffsetEpoch), new Metadata.LeaderAndEpoch(Optional.of(broker1), Optional.of(currentEpoch)));
        state.seekUnvalidated(tp0, initialPosition);
        assertTrue(state.awaitingValidation(tp0));

        Optional<LogTruncation> truncationOpt = state.maybeCompleteValidation(tp0, initialPosition,
                new EpochEndOffset()
                    .setLeaderEpoch(divergentOffsetEpoch)
                    .setEndOffset(divergentOffset));
        assertTrue(truncationOpt.isPresent());
        LogTruncation truncation = truncationOpt.get();

        assertEquals(Optional.of(new OffsetAndMetadata(divergentOffset, Optional.of(divergentOffsetEpoch), "")),
                truncation.divergentOffsetOpt);
        assertEquals(initialPosition, truncation.fetchPosition);
        assertTrue(state.awaitingValidation(tp0));
    }

    @Test
    public void testTruncationDetectionUnknownDivergentOffsetWithResetPolicy() {
        Node broker1 = new Node(1, "localhost", 9092);
        state = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.EARLIEST);
        state.assignFromUser(Set.of(tp0));

        int currentEpoch = 10;
        long initialOffset = 10L;
        int initialOffsetEpoch = 5;

        SubscriptionState.FetchPosition initialPosition = new SubscriptionState.FetchPosition(initialOffset,
            Optional.of(initialOffsetEpoch), new Metadata.LeaderAndEpoch(Optional.of(broker1), Optional.of(currentEpoch)));
        state.seekUnvalidated(tp0, initialPosition);
        assertTrue(state.awaitingValidation(tp0));

        Optional<LogTruncation> truncationOpt = state.maybeCompleteValidation(tp0, initialPosition,
                new EpochEndOffset()
                    .setLeaderEpoch(UNDEFINED_EPOCH)
                    .setEndOffset(UNDEFINED_EPOCH_OFFSET));
        assertEquals(Optional.empty(), truncationOpt);
        assertFalse(state.awaitingValidation(tp0));
        assertTrue(state.isOffsetResetNeeded(tp0));
        assertEquals(AutoOffsetResetStrategy.EARLIEST, state.resetStrategy(tp0));
    }

    @Test
    public void testTruncationDetectionUnknownDivergentOffsetWithoutResetPolicy() {
        Node broker1 = new Node(1, "localhost", 9092);
        state = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.NONE);
        state.assignFromUser(Set.of(tp0));

        int currentEpoch = 10;
        long initialOffset = 10L;
        int initialOffsetEpoch = 5;

        SubscriptionState.FetchPosition initialPosition = new SubscriptionState.FetchPosition(initialOffset,
            Optional.of(initialOffsetEpoch), new Metadata.LeaderAndEpoch(Optional.of(broker1), Optional.of(currentEpoch)));
        state.seekUnvalidated(tp0, initialPosition);
        assertTrue(state.awaitingValidation(tp0));

        Optional<LogTruncation> truncationOpt = state.maybeCompleteValidation(tp0, initialPosition,
                new EpochEndOffset()
                    .setLeaderEpoch(UNDEFINED_EPOCH)
                    .setEndOffset(UNDEFINED_EPOCH_OFFSET));
        assertTrue(truncationOpt.isPresent());
        LogTruncation truncation = truncationOpt.get();

        assertEquals(Optional.empty(), truncation.divergentOffsetOpt);
        assertEquals(initialPosition, truncation.fetchPosition);
        assertTrue(state.awaitingValidation(tp0));
    }

    private static class MockRebalanceListener implements ConsumerRebalanceListener {
        Collection<TopicPartition> revoked;
        public Collection<TopicPartition> assigned;
        int revokedCount = 0;
        int assignedCount = 0;

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

    @Test
    public void resetOffsetNoValidation() {
        // Check that offset reset works when we can't validate offsets (older brokers)

        Node broker1 = new Node(1, "localhost", 9092);
        state.assignFromUser(Set.of(tp0));

        // Reset offsets
        state.requestOffsetReset(tp0, AutoOffsetResetStrategy.EARLIEST);

        // Attempt to validate with older API version, should do nothing
        ApiVersions oldApis = new ApiVersions();
        oldApis.update("1", NodeApiVersions.create(ApiKeys.OFFSET_FOR_LEADER_EPOCH.id, (short) 0, (short) 2));
        assertFalse(state.maybeValidatePositionForCurrentLeader(oldApis, tp0, new Metadata.LeaderAndEpoch(
                Optional.of(broker1), Optional.empty())));
        assertFalse(state.hasValidPosition(tp0));
        assertFalse(state.awaitingValidation(tp0));
        assertTrue(state.isOffsetResetNeeded(tp0));

        // Complete the reset via unvalidated seek
        state.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(10L));
        assertTrue(state.hasValidPosition(tp0));
        assertFalse(state.awaitingValidation(tp0));
        assertFalse(state.isOffsetResetNeeded(tp0));

        // Next call to validate offsets does nothing
        assertFalse(state.maybeValidatePositionForCurrentLeader(oldApis, tp0, new Metadata.LeaderAndEpoch(
                Optional.of(broker1), Optional.empty())));
        assertTrue(state.hasValidPosition(tp0));
        assertFalse(state.awaitingValidation(tp0));
        assertFalse(state.isOffsetResetNeeded(tp0));

        // Reset again, and complete it with a seek that would normally require validation
        state.requestOffsetReset(tp0, AutoOffsetResetStrategy.EARLIEST);
        state.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(10L, Optional.of(10), new Metadata.LeaderAndEpoch(
                Optional.of(broker1), Optional.of(2))));
        // We are now in AWAIT_VALIDATION
        assertFalse(state.hasValidPosition(tp0));
        assertTrue(state.awaitingValidation(tp0));
        assertFalse(state.isOffsetResetNeeded(tp0));

        // Now ensure next call to validate clears the validation state
        assertFalse(state.maybeValidatePositionForCurrentLeader(oldApis, tp0, new Metadata.LeaderAndEpoch(
                Optional.of(broker1), Optional.of(2))));
        assertTrue(state.hasValidPosition(tp0));
        assertFalse(state.awaitingValidation(tp0));
        assertFalse(state.isOffsetResetNeeded(tp0));
    }

    @Test
    public void nullPositionLagOnNoPosition() {
        state.assignFromUser(Set.of(tp0));

        assertNull(state.partitionLag(tp0, IsolationLevel.READ_UNCOMMITTED));
        assertNull(state.partitionLag(tp0, IsolationLevel.READ_COMMITTED));

        state.updateHighWatermark(tp0, 1L);
        state.updateLastStableOffset(tp0, 1L);

        assertNull(state.partitionLag(tp0, IsolationLevel.READ_UNCOMMITTED));
        assertNull(state.partitionLag(tp0, IsolationLevel.READ_COMMITTED));
    }

    @Test
    public void testPositionOrNull() {
        state.assignFromUser(Set.of(tp0));
        final TopicPartition unassignedPartition = new TopicPartition("unassigned", 0);
        state.seek(tp0, 5);

        assertEquals(5, state.positionOrNull(tp0).offset);
        assertNull(state.positionOrNull(unassignedPartition));
    }

    @Test
    public void testTryUpdatingHighWatermark() {
        state.assignFromUser(Set.of(tp0));
        final TopicPartition unassignedPartition = new TopicPartition("unassigned", 0);

        final long highWatermark = 10L;
        assertTrue(state.tryUpdatingHighWatermark(tp0, highWatermark));
        assertEquals(highWatermark, state.partitionEndOffset(tp0, IsolationLevel.READ_UNCOMMITTED));
        assertFalse(state.tryUpdatingHighWatermark(unassignedPartition, highWatermark));
    }

    @Test
    public void testTryUpdatingLogStartOffset() {
        state.assignFromUser(Set.of(tp0));
        final TopicPartition unassignedPartition = new TopicPartition("unassigned", 0);
        final long position = 25;
        state.seek(tp0, position);

        final long logStartOffset = 10L;
        assertTrue(state.tryUpdatingLogStartOffset(tp0, logStartOffset));
        assertEquals(position - logStartOffset, state.partitionLead(tp0));
        assertFalse(state.tryUpdatingLogStartOffset(unassignedPartition, logStartOffset));
    }

    @Test
    public void testTryUpdatingLastStableOffset() {
        state.assignFromUser(Set.of(tp0));
        final TopicPartition unassignedPartition = new TopicPartition("unassigned", 0);

        final long lastStableOffset = 10L;
        assertTrue(state.tryUpdatingLastStableOffset(tp0, lastStableOffset));
        assertEquals(lastStableOffset, state.partitionEndOffset(tp0, IsolationLevel.READ_COMMITTED));
        assertFalse(state.tryUpdatingLastStableOffset(unassignedPartition, lastStableOffset));
    }

    @Test
    public void testTryUpdatingPreferredReadReplica() {
        state.assignFromUser(Set.of(tp0));
        final TopicPartition unassignedPartition = new TopicPartition("unassigned", 0);

        final int preferredReadReplicaId = 10;
        final LongSupplier expirationTimeMs = () -> System.currentTimeMillis() + 60000L;
        assertTrue(state.tryUpdatingPreferredReadReplica(tp0, preferredReadReplicaId, expirationTimeMs));
        assertEquals(Optional.of(preferredReadReplicaId), state.preferredReadReplica(tp0, System.currentTimeMillis()));
        assertFalse(state.tryUpdatingPreferredReadReplica(unassignedPartition, preferredReadReplicaId, expirationTimeMs));
        assertEquals(Optional.empty(), state.preferredReadReplica(unassignedPartition, System.currentTimeMillis()));
    }

    @Test
    public void testRequestOffsetResetIfPartitionAssigned() {
        state.assignFromUser(Set.of(tp0));
        final TopicPartition unassignedPartition = new TopicPartition("unassigned", 0);

        state.requestOffsetResetIfPartitionAssigned(tp0);

        assertTrue(state.isOffsetResetNeeded(tp0));

        state.requestOffsetResetIfPartitionAssigned(unassignedPartition);

        assertThrows(IllegalStateException.class, () -> state.isOffsetResetNeeded(unassignedPartition));
    }

    // This test ensures the "fetchablePartitions" does not run the custom predicate if the partition is not fetchable
    // This func is used in the hot path for fetching, to find fetchable partitions that are not in the buffer,
    // so it should avoid evaluating the predicate if not needed.
    @Test
    public void testFetchablePartitionsPerformsCheapChecksFirst() {
        // Setup fetchable partition and pause it
        state.assignFromUser(Set.of(tp0));
        state.seek(tp0, 100);
        assertTrue(state.isFetchable(tp0));
        state.pause(tp0);

        // Retrieve fetchable partitions with custom predicate.
        AtomicBoolean predicateEvaluated = new AtomicBoolean(false);
        Predicate<TopicPartition> isBuffered = tp -> {
            predicateEvaluated.set(true);
            return true;
        };
        List<TopicPartition> fetchablePartitions = state.fetchablePartitions(isBuffered);
        assertTrue(fetchablePartitions.isEmpty());
        assertFalse(predicateEvaluated.get(), "Custom predicate should not be evaluated when partitions are not fetchable");

        // Resume partition and retrieve fetchable again
        state.resume(tp0);
        predicateEvaluated.set(false);
        fetchablePartitions = state.fetchablePartitions(isBuffered);
        assertTrue(predicateEvaluated.get());
        assertEquals(tp0, fetchablePartitions.get(0));
    }
}
