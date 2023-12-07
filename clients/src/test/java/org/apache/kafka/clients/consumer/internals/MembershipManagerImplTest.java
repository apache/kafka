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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MembershipManagerImplTest {

    private static final String GROUP_ID = "test-group";
    private static final String MEMBER_ID = "test-member-1";
    private static final int MEMBER_EPOCH = 1;

    private SubscriptionState subscriptionState;
    private ConsumerMetadata metadata;

    private CommitRequestManager commitRequestManager;

    private ConsumerTestBuilder testBuilder;

    @BeforeEach
    public void setup() {
        testBuilder = new ConsumerTestBuilder(ConsumerTestBuilder.createDefaultGroupInformation());
        metadata = testBuilder.metadata;
        subscriptionState = testBuilder.subscriptions;
        commitRequestManager = testBuilder.commitRequestManager.get();
    }

    @AfterEach
    public void tearDown() {
        if (testBuilder != null) {
            testBuilder.close();
        }
    }

    private MembershipManagerImpl createMembershipManagerJoiningGroup() {
        MembershipManagerImpl manager = spy(new MembershipManagerImpl(
                GROUP_ID, Optional.empty(), Optional.empty(), subscriptionState, commitRequestManager,
                metadata, testBuilder.logContext));
        manager.transitionToJoining();
        return manager;
    }

    private MembershipManagerImpl createMembershipManagerJoiningGroup(String groupInstanceId,
                                                                      String serverAssignor) {
        MembershipManagerImpl manager = new MembershipManagerImpl(
                GROUP_ID, Optional.ofNullable(groupInstanceId), Optional.ofNullable(serverAssignor),
                subscriptionState, commitRequestManager, metadata, testBuilder.logContext);
        manager.transitionToJoining();
        return manager;
    }

    @Test
    public void testMembershipManagerServerAssignor() {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        assertEquals(Optional.empty(), membershipManager.serverAssignor());

        membershipManager = createMembershipManagerJoiningGroup("instance1", "Uniform");
        assertEquals(Optional.of("Uniform"), membershipManager.serverAssignor());
    }

    @Test
    public void testMembershipManagerInitSupportsEmptyGroupInstanceId() {
        createMembershipManagerJoiningGroup();
        createMembershipManagerJoiningGroup(null, null);
    }

    @Test
    public void testMembershipManagerRegistersForClusterMetadataUpdatesOnFirstJoin() {
        // First join should register to get metadata updates
        MembershipManagerImpl manager = new MembershipManagerImpl(
                GROUP_ID, Optional.empty(), Optional.empty(), subscriptionState, commitRequestManager,
                metadata, testBuilder.logContext);
        manager.transitionToJoining();
        verify(metadata).addClusterUpdateListener(manager);
        clearInvocations(metadata);

        // Following joins should not register again.
        receiveEmptyAssignment(manager);
        mockLeaveGroup();
        manager.leaveGroup();
        assertEquals(MemberState.LEAVING, manager.state());
        manager.onHeartbeatRequestSent();
        assertEquals(MemberState.UNSUBSCRIBED, manager.state());
        manager.transitionToJoining();
        verify(metadata, never()).addClusterUpdateListener(manager);
    }

    @Test
    public void testReconcilingWhenReceivingAssignmentFoundInMetadata() {
        MembershipManager membershipManager = mockJoinAndReceiveAssignment(true);
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());

        // When the ack is sent the member should go back to STABLE
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.STABLE, membershipManager.state());
    }

    @Test
    public void testTransitionToReconcilingOnlyIfAssignmentReceived() {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        assertEquals(MemberState.JOINING, membershipManager.state());

        ConsumerGroupHeartbeatResponse responseWithoutAssignment =
                createConsumerGroupHeartbeatResponse(null);
        membershipManager.onHeartbeatResponseReceived(responseWithoutAssignment.data());
        assertNotEquals(MemberState.RECONCILING, membershipManager.state());

        ConsumerGroupHeartbeatResponse responseWithAssignment =
                createConsumerGroupHeartbeatResponse(createAssignment(true));
        membershipManager.onHeartbeatResponseReceived(responseWithAssignment.data());
        assertEquals(MemberState.RECONCILING, membershipManager.state());
    }

    @Test
    public void testMemberIdAndEpochResetOnFencedMembers() {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(null);
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertEquals(MEMBER_ID, membershipManager.memberId());
        assertEquals(MEMBER_EPOCH, membershipManager.memberEpoch());

        mockMemberHasAutoAssignedPartition();

        membershipManager.transitionToFenced();
        assertEquals(MEMBER_ID, membershipManager.memberId());
        assertEquals(0, membershipManager.memberEpoch());
    }

    @Test
    public void testTransitionToFatal() {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        ConsumerGroupHeartbeatResponse heartbeatResponse =
                createConsumerGroupHeartbeatResponse(null);
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertEquals(MEMBER_ID, membershipManager.memberId());
        assertEquals(MEMBER_EPOCH, membershipManager.memberEpoch());

        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        membershipManager.transitionToFatal();
        assertEquals(MemberState.FATAL, membershipManager.state());
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
    }

    @Test
    public void testTransitionToFailedWhenTryingToJoin() {
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(
                GROUP_ID, Optional.empty(), Optional.empty(), subscriptionState, commitRequestManager, metadata,
                testBuilder.logContext);
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        membershipManager.transitionToJoining();

        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        membershipManager.transitionToFatal();
        assertEquals(MemberState.FATAL, membershipManager.state());
    }

    @Test
    public void testFencingWhenStateIsStable() {
        MembershipManager membershipManager = createMemberInStableState();
        testFencedMemberReleasesAssignmentAndTransitionsToJoining(membershipManager);
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
    }

    @Test
    public void testFencingWhenStateIsReconciling() {
        MembershipManager membershipManager = mockJoinAndReceiveAssignment(false);
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        testFencedMemberReleasesAssignmentAndTransitionsToJoining(membershipManager);
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
    }

    /**
     * This is the case where a member is stuck reconciling and transition out of the RECONCILING
     * state (due to failure). When the reconciliation completes it should not be applied because
     * it is not relevant anymore (it should not update the assignment on the member or send ack).
     */
    @Test
    public void testDelayedReconciliationResultDiscardedIfMemberNotInReconcilingStateAnymore() {
        MembershipManagerImpl membershipManager = createMemberInStableState();
        Uuid topicId1 = Uuid.randomUuid();
        String topic1 = "topic1";
        Set<TopicPartition> owned = Collections.singleton(new TopicPartition(topic1, 0));
        mockOwnedPartitionAndAssignmentReceived(topicId1, topic1, owned, true);

        // Reconciliation that does not complete stuck on revocation commit.
        CompletableFuture<Void> commitResult = mockEmptyAssignmentAndRevocationStuckOnCommit(membershipManager);

        // Member received fatal error while reconciling
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        membershipManager.transitionToFatal();
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
        clearInvocations(subscriptionState);

        // Complete commit request
        commitResult.complete(null);

        // Member should not update the subscription or send ack when the delayed reconciliation
        // completed.
        verify(subscriptionState, never()).assignFromSubscribed(anySet());
        assertNotEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
    }

    /**
     * This is the case where a member is stuck reconciling an assignment A (waiting on
     * metadata, commit or callbacks), and it rejoins (due to fence or unsubscribe/subscribe). If
     * the reconciliation of A completes it should not be applied (it should not update the
     * assignment on the member or send ack).
     */
    @Test
    public void testDelayedReconciliationResultDiscardedIfMemberRejoins() {
        MembershipManagerImpl membershipManager = createMemberInStableState();
        Uuid topicId1 = Uuid.randomUuid();
        String topic1 = "topic1";
        Set<TopicPartition> owned = Collections.singleton(new TopicPartition(topic1, 0));
        mockOwnedPartitionAndAssignmentReceived(topicId1, topic1, owned, true);

        // Reconciliation that does not complete stuck on revocation commit.
        CompletableFuture<Void> commitResult =
                mockNewAssignmentAndRevocationStuckOnCommit(membershipManager, topicId1, topic1,
                        Arrays.asList(1, 2), true);
        Set<TopicIdPartition> assignment1 = new HashSet<>();
        assignment1.add(new TopicIdPartition(topicId1, new TopicPartition(topic1, 1)));
        assignment1.add(new TopicIdPartition(topicId1, new TopicPartition(topic1, 2)));
        assertEquals(assignment1, membershipManager.assignmentReadyToReconcile());

        // Get fenced and rejoin while still reconciling. Get new assignment to reconcile after
        // rejoining.
        testFencedMemberReleasesAssignmentAndTransitionsToJoining(membershipManager);
        clearInvocations(subscriptionState);

        // Get new assignment A2 after rejoining. This should not trigger a reconciliation just
        // yet because there is another on in progress, but should keep the new assignment ready
        // to be reconciled next.
        Uuid topicId3 = Uuid.randomUuid();
        mockOwnedPartitionAndAssignmentReceived(topicId3, "topic3", owned, true);
        receiveAssignmentAfterRejoin(topicId3, Collections.singletonList(5), membershipManager);
        verifyReconciliationNotTriggered(membershipManager);
        Set<TopicIdPartition> assignmentAfterRejoin = Collections.singleton(
                new TopicIdPartition(topicId3, new TopicPartition("topic3", 5)));
        assertEquals(assignmentAfterRejoin, membershipManager.assignmentReadyToReconcile());

        // Reconciliation completes when the member has already re-joined the group. Should not
        // update the subscription state or send ack.
        commitResult.complete(null);
        verify(subscriptionState, never()).assignFromSubscribed(anyCollection());
        assertNotEquals(MemberState.ACKNOWLEDGING, membershipManager.state());

        // Assignment received after rejoining should be ready to reconcile on the next
        // reconciliation loop.
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertEquals(assignmentAfterRejoin, membershipManager.assignmentReadyToReconcile());
    }

    /**
     * This is the case where a member is stuck reconciling an assignment A (waiting on
     * metadata, commit or callbacks), and the target assignment changes (due to new topics added
     * to metadata, or new assignment received from broker). If the reconciliation of A completes
     * it should be applied (should update the assignment on the member and send ack), and then
     * the reconciliation of assignment B will be processed and applied in the next
     * reconciliation loop.
     */
    @Test
    public void testDelayedReconciliationResultAppliedWhenTargetChangedWithMetadataUpdate() {
        // Member receives and reconciles topic1-partition0
        Uuid topicId1 = Uuid.randomUuid();
        String topic1 = "topic1";
        MembershipManagerImpl membershipManager =
                mockMemberSuccessfullyReceivesAndAcksAssignment(topicId1, topic1, Arrays.asList(0));
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.STABLE, membershipManager.state());
        clearInvocations(membershipManager, subscriptionState);
        when(subscriptionState.assignedPartitions()).thenReturn(Collections.singleton(new TopicPartition(topic1, 0)));

        // New assignment revoking the partitions owned and adding a new one (not in metadata).
        // Reconciliation triggered for topic 1 (stuck on revocation commit) and topic2 waiting
        // for metadata.
        Uuid topicId2 = Uuid.randomUuid();
        String topic2 = "topic2";
        CompletableFuture<Void> commitResult =
                mockNewAssignmentAndRevocationStuckOnCommit(membershipManager, topicId2, topic2,
                        Arrays.asList(1, 2), false);
        verify(metadata).requestUpdate(anyBoolean());
        assertEquals(Collections.singleton(topicId2), membershipManager.topicsWaitingForMetadata());

        // Metadata discovered for topic2 while reconciliation in progress to revoke topic1.
        // Should not trigger a new reconciliation because there is one already in progress.
        mockTopicNameInMetadataCache(Collections.singletonMap(topicId2, topic2), true);
        membershipManager.onUpdate(null);
        assertEquals(Collections.emptySet(), membershipManager.topicsWaitingForMetadata());
        verifyReconciliationNotTriggered(membershipManager);

        // Reconciliation in progress completes. Should be applied revoking topic 1 only. Newly
        // discovered topic2 will be reconciled in the next reconciliation loop.
        commitResult.complete(null);

        // Member should update the subscription and send ack when the delayed reconciliation
        // completes.
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());

        // Pending assignment that was discovered in metadata should be ready to reconcile in the
        // next reconciliation loop.
        Set<TopicIdPartition> topic2Assignment = new HashSet<>(Arrays.asList(
                new TopicIdPartition(topicId2, new TopicPartition(topic2, 1)),
                new TopicIdPartition(topicId2, new TopicPartition(topic2, 2))));
        assertEquals(topic2Assignment, membershipManager.assignmentReadyToReconcile());
    }

    @Test
    public void testLeaveGroupWhenStateIsStable() {
        MembershipManager membershipManager = createMemberInStableState();
        testLeaveGroupReleasesAssignmentAndResetsEpochToSendLeaveGroup(membershipManager);
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
    }

    @Test
    public void testLeaveGroupWhenStateIsReconciling() {
        MembershipManager membershipManager = mockJoinAndReceiveAssignment(false);
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        testLeaveGroupReleasesAssignmentAndResetsEpochToSendLeaveGroup(membershipManager);
    }

    @Test
    public void testLeaveGroupWhenMemberAlreadyLeaving() {
        MembershipManager membershipManager = createMemberInStableState();

        // First leave attempt. Should trigger the callbacks and stay LEAVING until
        // callbacks complete and the heartbeat is sent out.
        mockLeaveGroup();
        CompletableFuture<Void> leaveResult1 = membershipManager.leaveGroup();
        assertFalse(leaveResult1.isDone());
        assertEquals(MemberState.LEAVING, membershipManager.state());
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
        clearInvocations(subscriptionState);

        // Second leave attempt while the first one has not completed yet. Should not
        // trigger any callbacks, and return a future that will complete when the ongoing first
        // leave operation completes.
        mockLeaveGroup();
        CompletableFuture<Void> leaveResult2 = membershipManager.leaveGroup();
        verify(subscriptionState, never()).rebalanceListener();
        assertFalse(leaveResult2.isDone());

        // Complete first leave group operation. Should also complete the second leave group.
        membershipManager.onHeartbeatRequestSent();
        assertTrue(leaveResult1.isDone());
        assertFalse(leaveResult1.isCompletedExceptionally());
        assertTrue(leaveResult2.isDone());
        assertFalse(leaveResult2.isCompletedExceptionally());

        // Subscription should have been updated only once with the first leave group.
        verify(subscriptionState, never()).assignFromSubscribed(Collections.emptySet());
    }

    @Test
    public void testLeaveGroupWhenMemberAlreadyLeft() {
        MembershipManager membershipManager = createMemberInStableState();

        // Leave group triggered and completed
        mockLeaveGroup();
        CompletableFuture<Void> leaveResult1 = membershipManager.leaveGroup();
        assertEquals(MemberState.LEAVING, membershipManager.state());
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        assertTrue(leaveResult1.isDone());
        assertFalse(leaveResult1.isCompletedExceptionally());
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
        clearInvocations(subscriptionState);

        // Call to leave group again, when member already left. Should be no-op (no callbacks,
        // no assignment updated)
        mockLeaveGroup();
        CompletableFuture<Void> leaveResult2 = membershipManager.leaveGroup();
        assertTrue(leaveResult2.isDone());
        assertFalse(leaveResult2.isCompletedExceptionally());
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        verify(subscriptionState, never()).rebalanceListener();
        verify(subscriptionState, never()).assignFromSubscribed(Collections.emptySet());
    }

    @Test
    public void testFatalFailureWhenStateIsUnjoined() {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        assertEquals(MemberState.JOINING, membershipManager.state());

        testStateUpdateOnFatalFailure(membershipManager);
    }

    @Test
    public void testFatalFailureWhenStateIsStable() {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(null);
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());
        assertEquals(MemberState.STABLE, membershipManager.state());

        testStateUpdateOnFatalFailure(membershipManager);
    }

    @Test
    public void testUpdateStateFailsOnResponsesWithErrors() {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        // Updating state with a heartbeat response containing errors cannot be performed and
        // should fail.
        ConsumerGroupHeartbeatResponse unknownMemberResponse =
                createConsumerGroupHeartbeatResponseWithError(Errors.UNKNOWN_MEMBER_ID);
        assertThrows(IllegalArgumentException.class,
                () -> membershipManager.onHeartbeatResponseReceived(unknownMemberResponse.data()));
    }

    /**
     * This test should be the case when an assignment is sent to the member, and it cannot find
     * it in metadata (permanently, ex. topic deleted). The member will keep the assignment as
     * waiting for metadata, but the following assignment received from the broker will not
     * contain the deleted topic. The member will discard the assignment that was pending and
     * proceed with the reconciliation of the new assignment.
     */
    @Test
    public void testNewAssignmentReplacesPreviousOneWaitingOnMetadata() {
        MembershipManagerImpl membershipManager = mockJoinAndReceiveAssignment(false);
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        // When the ack is sent the member should go back to RECONCILING
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertTrue(membershipManager.topicsWaitingForMetadata().size() > 0);

        // New target assignment received while there is another one waiting to be resolved
        // and reconciled. This assignment does not include the previous one that is waiting
        // for metadata, so the member will discard the topics that were waiting for metadata, and
        // reconcile the new assignment.
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));
        receiveAssignment(topicId, Collections.singletonList(0), membershipManager);
        Set<TopicPartition> expectedAssignment = Collections.singleton(new TopicPartition(topicName, 0));
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        verify(subscriptionState).assignFromSubscribed(expectedAssignment);

        // When ack for the reconciled assignment is sent, member should go back to STABLE
        // because the first assignment that was not resolved should have been discarded
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertTrue(membershipManager.topicsWaitingForMetadata().isEmpty());
    }

    /**
     * This test should be the case when an assignment is sent to the member, and it cannot find
     * it in metadata (temporarily). If the broker continues to send the assignment to the
     * member, this one should keep it waiting for metadata and continue to request updates.
     */
    @Test
    public void testMemberKeepsUnresolvedAssignmentWaitingForMetadataUntilResolved() {
        // Assignment with 2 topics, only 1 found in metadata
        Uuid topic1 = Uuid.randomUuid();
        String topic1Name = "topic1";
        Uuid topic2 = Uuid.randomUuid();
        ConsumerGroupHeartbeatResponseData.Assignment assignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topic1)
                                .setPartitions(Arrays.asList(0)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topic2)
                                .setPartitions(Arrays.asList(1, 3))
                ));
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topic1, topic1Name));

        // Receive assignment partly in metadata - reconcile+ack what's in metadata, keep the
        // unresolved and request metadata update.
        MembershipManagerImpl membershipManager = mockJoinAndReceiveAssignment(true, assignment);
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        verify(metadata).requestUpdate(anyBoolean());
        assertEquals(Collections.singleton(topic2), membershipManager.topicsWaitingForMetadata());

        // When the ack is sent the member should go back to RECONCILING because it still has
        // unresolved assignment to be reconciled.
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        // Target assignment received again with the same unresolved topic. Client should keep it
        // as unresolved.
        clearInvocations(subscriptionState);
        membershipManager.onHeartbeatResponseReceived(createConsumerGroupHeartbeatResponse(assignment).data());
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertEquals(Collections.singleton(topic2), membershipManager.topicsWaitingForMetadata());
        verify(subscriptionState, never()).assignFromSubscribed(anyCollection());
    }

    @Test
    public void testReconcileNewPartitionsAssignedWhenNoPartitionOwned() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";
        mockOwnedPartitionAndAssignmentReceived(topicId, topicName, Collections.emptySet(), true);

        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        receiveAssignment(topicId, Arrays.asList(0, 1), membershipManager);

        Set<TopicIdPartition> assignedPartitions = new HashSet<>(Arrays.asList(
                new TopicIdPartition(topicId, new TopicPartition(topicName, 0)),
                new TopicIdPartition(topicId, new TopicPartition(topicName, 1))));
        verifyReconciliationTriggeredAndCompleted(membershipManager, assignedPartitions);
    }

    @Test
    public void testReconcileNewPartitionsAssignedWhenOtherPartitionsOwned() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";
        TopicPartition ownedPartition = new TopicPartition(topicName, 0);
        mockOwnedPartitionAndAssignmentReceived(topicId, topicName,
                Collections.singleton(ownedPartition), true);

        MembershipManagerImpl membershipManager = createMemberInStableState();
        // New assignment received, adding partitions 1 and 2 to the previously owned partition 0.
        receiveAssignment(topicId, Arrays.asList(0, 1, 2), membershipManager);

        Set<TopicIdPartition> assignedPartitions = new HashSet<>(Arrays.asList(
                new TopicIdPartition(topicId, ownedPartition),
                new TopicIdPartition(topicId, new TopicPartition("topic1", 1)),
                new TopicIdPartition(topicId, new TopicPartition("topic1", 2))));
        verifyReconciliationTriggeredAndCompleted(membershipManager, assignedPartitions);
    }

    @Test
    public void testReconciliationSkippedWhenSameAssignmentReceived() {
        // Member stable, no assignment
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";

        // Receive assignment different from what the member owns - should reconcile
        Set<TopicPartition> owned = Collections.emptySet();
        mockOwnedPartitionAndAssignmentReceived(topicId, topicName, owned, true);
        Set<TopicIdPartition> expectedAssignmentReconciled = new HashSet<>();
        expectedAssignmentReconciled.add(new TopicIdPartition(topicId,
                new TopicPartition(topicName, 0)));
        expectedAssignmentReconciled.add(new TopicIdPartition(topicId,
                new TopicPartition(topicName, 1)));
        receiveAssignment(topicId, Arrays.asList(0, 1), membershipManager);
        verifyReconciliationTriggeredAndCompleted(membershipManager, expectedAssignmentReconciled);
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        clearInvocations(subscriptionState, membershipManager);

        // Receive same assignment again - should not trigger reconciliation
        Set<TopicPartition> expectedTopicPartitionAssignment = buildTopicPartitions(expectedAssignmentReconciled);
        mockOwnedPartitionAndAssignmentReceived(topicId, topicName, expectedTopicPartitionAssignment, true);
        receiveAssignment(topicId, Arrays.asList(0, 1), membershipManager);
        // Verify new reconciliation was not triggered
        verify(membershipManager, never()).markReconciliationInProgress();
        verify(membershipManager, never()).markReconciliationCompleted();
        verify(subscriptionState, never()).assignFromSubscribed(anyCollection());
    }

    @Test
    public void testReconcilePartitionsRevokedNoAutoCommitNoCallbacks() {
        MembershipManagerImpl membershipManager = createMemberInStableState();
        mockOwnedPartition("topic1", 0);

        mockRevocationNoCallbacks(false);

        receiveEmptyAssignment(membershipManager);

        testRevocationOfAllPartitionsCompleted(membershipManager);
    }

    @Test
    public void testReconcilePartitionsRevokedWithSuccessfulAutoCommitNoCallbacks() {
        MembershipManagerImpl membershipManager = createMemberInStableState();
        mockOwnedPartition("topic1", 0);

        CompletableFuture<Void> commitResult = mockRevocationNoCallbacks(true);

        receiveEmptyAssignment(membershipManager);

        // Member stays in RECONCILING while the commit request hasn't completed.
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        // Partitions should be still owned by the member
        verify(subscriptionState, never()).assignFromSubscribed(anyCollection());

        // Complete commit request
        commitResult.complete(null);

        testRevocationOfAllPartitionsCompleted(membershipManager);
    }

    @Test
    public void testReconcilePartitionsRevokedWithFailedAutoCommitCompletesRevocationAnyway() {
        MembershipManagerImpl membershipManager = createMemberInStableState();
        mockOwnedPartition("topic1", 0);

        CompletableFuture<Void> commitResult = mockRevocationNoCallbacks(true);

        receiveEmptyAssignment(membershipManager);

        // Member stays in RECONCILING while the commit request hasn't completed.
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        // Partitions should be still owned by the member
        verify(subscriptionState, never()).assignFromSubscribed(anyCollection());

        // Complete commit request
        commitResult.completeExceptionally(new KafkaException("Commit request failed with " +
                "non-retriable error"));

        testRevocationOfAllPartitionsCompleted(membershipManager);
    }

    @Test
    public void testReconcileNewPartitionsAssignedAndRevoked() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";
        TopicPartition ownedPartition = new TopicPartition(topicName, 0);
        mockOwnedPartitionAndAssignmentReceived(topicId, topicName, Collections.singleton(ownedPartition), true);

        mockRevocationNoCallbacks(false);

        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        // New assignment received, revoking partition 0, and assigning new partitions 1 and 2.
        receiveAssignment(topicId, Arrays.asList(1, 2), membershipManager);

        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        Set<TopicIdPartition> assignedPartitions = new HashSet<>(Arrays.asList(
                new TopicIdPartition(topicId, new TopicPartition("topic1", 1)),
                new TopicIdPartition(topicId, new TopicPartition("topic1", 2))));
        assertEquals(assignedPartitions, membershipManager.currentAssignment());
        assertFalse(membershipManager.reconciliationInProgress());

        verify(subscriptionState).assignFromSubscribed(anyCollection());
    }

    @Test
    public void testMetadataUpdatesReconcilesUnresolvedAssignments() {
        Uuid topicId = Uuid.randomUuid();

        // Assignment not in metadata
        ConsumerGroupHeartbeatResponseData.Assignment targetAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.singletonList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topicId)
                                .setPartitions(Arrays.asList(0, 1))));
        MembershipManagerImpl membershipManager = mockJoinAndReceiveAssignment(false, targetAssignment);
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        // Should not trigger reconciliation, and request a metadata update.
        verifyReconciliationNotTriggered(membershipManager);
        assertEquals(Collections.singleton(topicId), membershipManager.topicsWaitingForMetadata());
        verify(metadata).requestUpdate(anyBoolean());

        String topicName = "topic1";
        mockTopicNameInMetadataCache(Collections.singletonMap(topicId, topicName), true);

        // When metadata is updated, the member should re-trigger reconciliation
        membershipManager.onUpdate(null);
        Set<TopicIdPartition> expectedAssignmentReconciled = new HashSet<>(Arrays.asList(
                new TopicIdPartition(topicId, new TopicPartition(topicName, 0)),
                new TopicIdPartition(topicId, new TopicPartition(topicName, 1))));
        verifyReconciliationTriggeredAndCompleted(membershipManager, expectedAssignmentReconciled);
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        assertTrue(membershipManager.topicsWaitingForMetadata().isEmpty());
    }

    @Test
    public void testMetadataUpdatesRequestsAnotherUpdateIfNeeded() {
        Uuid topicId = Uuid.randomUuid();

        // Assignment not in metadata
        ConsumerGroupHeartbeatResponseData.Assignment targetAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.singletonList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topicId)
                                .setPartitions(Arrays.asList(0, 1))));
        MembershipManagerImpl membershipManager = mockJoinAndReceiveAssignment(false, targetAssignment);
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        // Should not trigger reconciliation, and request a metadata update.
        verifyReconciliationNotTriggered(membershipManager);
        assertEquals(Collections.singleton(topicId), membershipManager.topicsWaitingForMetadata());
        verify(metadata).requestUpdate(anyBoolean());

        // Metadata update received, but still without the unresolved topic in it. Should keep
        // the unresolved and request update again.
        when(metadata.topicNames()).thenReturn(Collections.emptyMap());
        membershipManager.onUpdate(null);
        verifyReconciliationNotTriggered(membershipManager);
        assertEquals(Collections.singleton(topicId), membershipManager.topicsWaitingForMetadata());
        verify(metadata, times(2)).requestUpdate(anyBoolean());
    }

    @Test
    public void testRevokePartitionsUsesTopicNamesLocalCacheWhenMetadataNotAvailable() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";

        mockOwnedPartitionAndAssignmentReceived(topicId, topicName, Collections.emptySet(), true);

        // Member received assignment to reconcile;
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        receiveAssignment(topicId, Arrays.asList(0, 1), membershipManager);

        // Member should complete reconciliation
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        Set<TopicPartition> assignedPartitions = new HashSet<>(Arrays.asList(
                new TopicPartition(topicName, 0),
                new TopicPartition(topicName, 1)));
        Set<TopicIdPartition> assignedTopicIdPartitions =
                assignedPartitions.stream().map(tp -> new TopicIdPartition(topicId, tp)).collect(Collectors.toSet());
        assertEquals(assignedTopicIdPartitions, membershipManager.currentAssignment());
        assertFalse(membershipManager.reconciliationInProgress());

        mockAckSent(membershipManager);
        when(subscriptionState.assignedPartitions()).thenReturn(assignedPartitions);

        // Revocation of topic not found in metadata cache
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        mockRevocationNoCallbacks(false);
        mockTopicNameInMetadataCache(Collections.singletonMap(topicId, topicName), false);

        // Revoke one of the 2 partitions
        receiveAssignment(topicId, Collections.singletonList(1), membershipManager);

        // Revocation should complete without requesting any metadata update given that the topic
        // received in target assignment should exist in local topic name cache.
        verify(metadata, never()).requestUpdate(anyBoolean());
        Set<TopicIdPartition> remainingAssignment = Collections.singleton(
                new TopicIdPartition(topicId, new TopicPartition(topicName, 1)));

        testRevocationCompleted(membershipManager, remainingAssignment);
    }

    @Test
    public void testOnSubscriptionUpdatedTransitionsToJoiningOnlyIfNotInGroup() {
        MembershipManagerImpl membershipManager = createMemberInStableState();
        verify(membershipManager).transitionToJoining();
        clearInvocations(membershipManager);
        membershipManager.onSubscriptionUpdated();
        verify(membershipManager, never()).transitionToJoining();
    }

    private MembershipManagerImpl mockMemberSuccessfullyReceivesAndAcksAssignment(
            Uuid topicId, String topicName, List<Integer> partitions) {
        mockOwnedPartitionAndAssignmentReceived(topicId, topicName, Collections.emptySet(), true);

        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        receiveAssignment(topicId, partitions, membershipManager);

        Set<TopicIdPartition> assignedPartitions = new HashSet<>();
        partitions.forEach(tp -> assignedPartitions.add(new TopicIdPartition(
                topicId,
                new TopicPartition(topicName, tp))));
        verifyReconciliationTriggeredAndCompleted(membershipManager, assignedPartitions);
        return membershipManager;
    }

    private CompletableFuture<Void> mockEmptyAssignmentAndRevocationStuckOnCommit(
            MembershipManagerImpl membershipManager) {
        CompletableFuture<Void> commitResult = mockRevocationNoCallbacks(true);
        receiveEmptyAssignment(membershipManager);
        verifyReconciliationTriggered(membershipManager);
        clearInvocations(membershipManager);
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        return commitResult;
    }

    private CompletableFuture<Void> mockNewAssignmentAndRevocationStuckOnCommit(
            MembershipManagerImpl membershipManager, Uuid topicId, String topicName,
            List<Integer> partitions, boolean mockMetadata) {
        CompletableFuture<Void> commitResult = mockRevocationNoCallbacks(true);
        if (mockMetadata) {
            when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));
        }
        receiveAssignment(topicId, partitions, membershipManager);
        verifyReconciliationTriggered(membershipManager);
        clearInvocations(membershipManager);
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        return commitResult;
    }

    private void verifyReconciliationTriggered(MembershipManagerImpl membershipManager) {
        verify(membershipManager).markReconciliationInProgress();
        assertEquals(MemberState.RECONCILING, membershipManager.state());
    }

    private void verifyReconciliationNotTriggered(MembershipManagerImpl membershipManager) {
        verify(membershipManager, never()).markReconciliationInProgress();
        verify(membershipManager, never()).markReconciliationCompleted();
    }

    private void verifyReconciliationTriggeredAndCompleted(MembershipManagerImpl membershipManager,
                                                           Set<TopicIdPartition> expectedAssignment) {
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        verify(membershipManager).markReconciliationInProgress();
        verify(membershipManager).markReconciliationCompleted();
        assertFalse(membershipManager.reconciliationInProgress());

        // Assignment applied
        Set<TopicPartition> expectedTopicPartitions = buildTopicPartitions(expectedAssignment);
        verify(subscriptionState).assignFromSubscribed(expectedTopicPartitions);
        // Set<TopicIdPartition> topicIdPartitions = buildTopicIdPartitions(expectedAssignment);
        assertEquals(expectedAssignment, membershipManager.currentAssignment());

        verify(commitRequestManager).resetAutoCommitTimer();
    }

    private Set<TopicPartition> buildTopicPartitions(Set<TopicIdPartition> topicIdPartitions) {
        return topicIdPartitions.stream().map(TopicIdPartition::topicPartition).collect(Collectors.toSet());
    }

    private void mockAckSent(MembershipManagerImpl membershipManager) {
        membershipManager.onHeartbeatRequestSent();
    }

    private void mockTopicNameInMetadataCache(Map<Uuid, String> topicNames, boolean isPresent) {
        if (isPresent) {
            when(metadata.topicNames()).thenReturn(topicNames);
        } else {
            when(metadata.topicNames()).thenReturn(Collections.emptyMap());
        }
    }

    private CompletableFuture<Void> mockRevocationNoCallbacks(boolean withAutoCommit) {
        doNothing().when(subscriptionState).markPendingRevocation(anySet());
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.empty()).thenReturn(Optional.empty());
        if (withAutoCommit) {
            when(commitRequestManager.autoCommitEnabled()).thenReturn(true);
            CompletableFuture<Void> commitResult = new CompletableFuture<>();
            when(commitRequestManager.maybeAutoCommitAllConsumed()).thenReturn(commitResult);
            return commitResult;
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private void mockMemberHasAutoAssignedPartition() {
        String topicName = "topic1";
        TopicPartition ownedPartition = new TopicPartition(topicName, 0);
        when(subscriptionState.assignedPartitions()).thenReturn(Collections.singleton(ownedPartition));
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.empty()).thenReturn(Optional.empty());
    }

    private void testRevocationOfAllPartitionsCompleted(MembershipManagerImpl membershipManager) {
        testRevocationCompleted(membershipManager, Collections.emptySet());
    }

    private void testRevocationCompleted(MembershipManagerImpl membershipManager,
                                         Set<TopicIdPartition> expectedCurrentAssignment) {
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        assertEquals(expectedCurrentAssignment, membershipManager.currentAssignment());
        assertFalse(membershipManager.reconciliationInProgress());

        verify(subscriptionState).markPendingRevocation(anySet());
        Set<TopicPartition> expectedTopicPartitionAssignment =
                buildTopicPartitions(expectedCurrentAssignment);
        verify(subscriptionState).assignFromSubscribed(expectedTopicPartitionAssignment);
    }

    private void mockOwnedPartitionAndAssignmentReceived(Uuid topicId,
                                                         String topicName,
                                                         Set<TopicPartition> previouslyOwned,
                                                         boolean mockMetadata) {
        when(subscriptionState.assignedPartitions()).thenReturn(previouslyOwned);
        if (mockMetadata) {
            when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));
        }
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.empty()).thenReturn(Optional.empty());
    }

    private void mockOwnedPartition(String topic, int partition) {
        TopicPartition previouslyOwned = new TopicPartition(topic, partition);
        when(subscriptionState.assignedPartitions()).thenReturn(Collections.singleton(previouslyOwned));
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
    }

    private MembershipManagerImpl mockJoinAndReceiveAssignment(boolean expectSubscriptionUpdated) {
        return mockJoinAndReceiveAssignment(expectSubscriptionUpdated, createAssignment(expectSubscriptionUpdated));
    }

    private MembershipManagerImpl mockJoinAndReceiveAssignment(boolean expectSubscriptionUpdated,
                                                               ConsumerGroupHeartbeatResponseData.Assignment assignment) {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(assignment);
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.empty()).thenReturn(Optional.empty());

        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());
        if (expectSubscriptionUpdated) {
            verify(subscriptionState).assignFromSubscribed(anyCollection());
        } else {
            verify(subscriptionState, never()).assignFromSubscribed(anyCollection());
        }

        return membershipManager;
    }

    private MembershipManagerImpl createMemberInStableState() {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(null);
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());
        assertEquals(MemberState.STABLE, membershipManager.state());
        return membershipManager;
    }

    private void receiveAssignment(Uuid topicId, List<Integer> partitions, MembershipManager membershipManager) {
        ConsumerGroupHeartbeatResponseData.Assignment targetAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.singletonList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topicId)
                                .setPartitions(partitions)));
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(targetAssignment);
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());
    }

    private void receiveAssignmentAfterRejoin(Uuid topicId, List<Integer> partitions, MembershipManager membershipManager) {
        ConsumerGroupHeartbeatResponseData.Assignment targetAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.singletonList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topicId)
                                .setPartitions(partitions)));
        ConsumerGroupHeartbeatResponse heartbeatResponse =
                createConsumerGroupHeartbeatResponseWithBumpedEpoch(targetAssignment);
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());
    }

    private void receiveEmptyAssignment(MembershipManager membershipManager) {
        // New empty assignment received, revoking owned partition.
        ConsumerGroupHeartbeatResponseData.Assignment targetAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.emptyList());
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(targetAssignment);
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());
    }

    /**
     * Fenced member should release assignment, reset epoch to 0, keep member ID, and transition
     * to JOINING to rejoin the group.
     */
    private void testFencedMemberReleasesAssignmentAndTransitionsToJoining(MembershipManager membershipManager) {
        mockMemberHasAutoAssignedPartition();

        membershipManager.transitionToFenced();

        assertEquals(MEMBER_ID, membershipManager.memberId());
        assertEquals(0, membershipManager.memberEpoch());
        assertEquals(MemberState.JOINING, membershipManager.state());
    }

    /**
     * Member that intentionally leaves the group (via unsubscribe) should release assignment,
     * reset epoch to -1, keep member ID, and transition to {@link MemberState#LEAVING} to send out a
     * heartbeat with the leave epoch. Once the heartbeat request is sent out, the member should
     * transition to {@link MemberState#UNSUBSCRIBED}
     */
    private void testLeaveGroupReleasesAssignmentAndResetsEpochToSendLeaveGroup(MembershipManager membershipManager) {
        mockLeaveGroup();

        CompletableFuture<Void> leaveResult = membershipManager.leaveGroup();

        assertEquals(MemberState.LEAVING, membershipManager.state());
        assertFalse(leaveResult.isDone(), "Leave group result should not complete until the " +
                "heartbeat request to leave is sent out.");

        membershipManager.onHeartbeatRequestSent();

        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        assertTrue(leaveResult.isDone());
        assertFalse(leaveResult.isCompletedExceptionally());
        assertEquals(MEMBER_ID, membershipManager.memberId());
        assertEquals(-1, membershipManager.memberEpoch());
        assertTrue(membershipManager.currentAssignment().isEmpty());
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
    }

    private void mockLeaveGroup() {
        mockMemberHasAutoAssignedPartition();
        doNothing().when(subscriptionState).markPendingRevocation(anySet());
    }

    private void testStateUpdateOnFatalFailure(MembershipManager membershipManager) {
        String memberId = membershipManager.memberId();
        int lastEpoch = membershipManager.memberEpoch();
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        membershipManager.transitionToFatal();
        assertEquals(MemberState.FATAL, membershipManager.state());
        // Should keep its last member id and epoch
        assertEquals(memberId, membershipManager.memberId());
        assertEquals(lastEpoch, membershipManager.memberEpoch());
    }

    private ConsumerGroupHeartbeatResponse createConsumerGroupHeartbeatResponse(
            ConsumerGroupHeartbeatResponseData.Assignment assignment) {
        return new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.NONE.code())
                .setMemberId(MEMBER_ID)
                .setMemberEpoch(MEMBER_EPOCH)
                .setAssignment(assignment));
    }

    /**
     * Create heartbeat response with the given assignment and a bumped epoch (incrementing by 1
     * as default but could be any increment). This will be used to mock when a member
     * receives a heartbeat response to the join request, and the response includes an assignment.
     */
    private ConsumerGroupHeartbeatResponse createConsumerGroupHeartbeatResponseWithBumpedEpoch(
            ConsumerGroupHeartbeatResponseData.Assignment assignment) {
        return new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.NONE.code())
                .setMemberId(MEMBER_ID)
                .setMemberEpoch(MEMBER_EPOCH + 1)
                .setAssignment(assignment));
    }

    private ConsumerGroupHeartbeatResponse createConsumerGroupHeartbeatResponseWithError(Errors error) {
        return new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(error.code())
                .setMemberId(MEMBER_ID)
                .setMemberEpoch(5));
    }

    private ConsumerGroupHeartbeatResponseData.Assignment createAssignment(boolean mockMetadata) {
        Uuid topic1 = Uuid.randomUuid();
        Uuid topic2 = Uuid.randomUuid();
        if (mockMetadata) {
            Map<Uuid, String> topicNames = new HashMap<>();
            topicNames.put(topic1, "topic1");
            topicNames.put(topic2, "topic2");
            when(metadata.topicNames()).thenReturn(topicNames);
        }
        return new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topic1)
                                .setPartitions(Arrays.asList(0, 1, 2)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topic2)
                                .setPartitions(Arrays.asList(3, 4, 5))
                ));
    }
}
