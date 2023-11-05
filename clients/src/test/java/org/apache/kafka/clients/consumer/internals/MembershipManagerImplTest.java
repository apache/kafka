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
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MembershipManagerImplTest {

    private static final String GROUP_ID = "test-group";
    private static final String MEMBER_ID = "test-member-1";
    private static final int MEMBER_EPOCH = 1;
    // private final LogContext logContext = new LogContext();

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
        MembershipManagerImpl manager = new MembershipManagerImpl(
                GROUP_ID, subscriptionState, commitRequestManager,
                metadata, testBuilder.logContext);
        manager.transitionToJoining();
        return manager;
    }

    private MembershipManagerImpl createMembershipManagerJoiningGroup(String groupInstanceId,
                                                                      String serverAssignor) {
        MembershipManagerImpl manager = new MembershipManagerImpl(
                GROUP_ID, groupInstanceId, serverAssignor, subscriptionState,
                commitRequestManager, metadata, testBuilder.logContext);
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
    public void testTransitionToReconcilingOnlyIfAssignmentReceived() {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        assertEquals(MemberState.JOINING, membershipManager.state());

        ConsumerGroupHeartbeatResponse responseWithoutAssignment =
                createConsumerGroupHeartbeatResponse(null);
        membershipManager.onHeartbeatResponseReceived(responseWithoutAssignment.data());
        assertNotEquals(MemberState.RECONCILING, membershipManager.state());

        ConsumerGroupHeartbeatResponse responseWithAssignment =
                createConsumerGroupHeartbeatResponse(createAssignment());
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
    public void testTransitionToFailure() {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        ConsumerGroupHeartbeatResponse heartbeatResponse =
                createConsumerGroupHeartbeatResponse(null);
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertEquals(MEMBER_ID, membershipManager.memberId());
        assertEquals(MEMBER_EPOCH, membershipManager.memberEpoch());

        membershipManager.transitionToFatal();
        assertEquals(MemberState.FATAL, membershipManager.state());
    }

    @Test
    public void testTransitionToFailedWhenTryingToJoin() {
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(
                GROUP_ID, subscriptionState, commitRequestManager, metadata,
                testBuilder.logContext);
        assertEquals(MemberState.NOT_IN_GROUP, membershipManager.state());
        membershipManager.transitionToJoining();

        membershipManager.transitionToFatal();
        assertEquals(MemberState.FATAL, membershipManager.state());
    }

    @Test
    public void testFencingWhenStateIsStable() {
        MembershipManager membershipManager = createMemberInStableState();
        testFencedMemberReleasesAssignmentAndResetsEpochToRejoin(membershipManager);
    }

    @Test
    public void testFencingWhenStateIsReconciling() {
        MembershipManager membershipManager = createMemberInReconcilingState();
        testFencedMemberReleasesAssignmentAndResetsEpochToRejoin(membershipManager);
    }

    @Test
    public void testLeaveGroupWhenStateIsStable() {
        MembershipManager membershipManager = createMemberInStableState();
        testLeaveGroupReleasesAssignmentAndResetsEpochToSendLeaveGroup(membershipManager);
    }

    @Test
    public void testLeaveGroupWhenStateIsReconciling() {
        MembershipManager membershipManager = createMemberInReconcilingState();
        testLeaveGroupReleasesAssignmentAndResetsEpochToSendLeaveGroup(membershipManager);
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

    @Test
    public void testMemberFailsIfAssignmentReceivedWhileAnotherOnBeingReconciled() {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        ConsumerGroupHeartbeatResponseData.Assignment newAssignment1 = createAssignment();
        membershipManager.onHeartbeatResponseReceived(createConsumerGroupHeartbeatResponse(newAssignment1).data());

        // First target assignment received should be in the process of being reconciled
        checkAssignments(membershipManager, null, newAssignment1);

        // Second target assignment received while there is another one being reconciled
        ConsumerGroupHeartbeatResponseData.Assignment newAssignment2 = createAssignment();
        assertThrows(IllegalStateException.class,
                () -> membershipManager.onHeartbeatResponseReceived(createConsumerGroupHeartbeatResponse(newAssignment2).data()));
        assertEquals(MemberState.FATAL, membershipManager.state());
    }

    @Test
    public void testReconcileNewPartitionsAssignedWhenNoPartitionOwned() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";
        mockOwnedPartitionAndAssignmentReceived(topicId, topicName, Collections.emptySet(), true);

        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        ConsumerGroupHeartbeatResponseData.Assignment targetAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.singletonList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topicId)
                                .setPartitions(Arrays.asList(0, 1))));
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(targetAssignment);
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());

        assertEquals(MemberState.SENDING_ACK_FOR_RECONCILED_ASSIGNMENT, membershipManager.state());
        Set<TopicPartition> assignedPartitions = new HashSet<>(Arrays.asList(
                new TopicPartition(topicName, 0),
                new TopicPartition(topicName, 1)));
        assertEquals(assignedPartitions, membershipManager.currentAssignment());
        assertFalse(membershipManager.targetAssignment().isPresent());

        verify(commitRequestManager).resetAutoCommitTimer();
        verify(subscriptionState).assignFromSubscribed(anyCollection());
    }

    @Test
    public void testReconcileNewPartitionsAssignedWhenOtherPartitionsOwned() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";
        TopicPartition ownedPartition = new TopicPartition(topicName, 0);
        mockOwnedPartitionAndAssignmentReceived(topicId, topicName,
                Collections.singleton(ownedPartition), true);

        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        // New assignment received, adding partitions 1 and 2 to the previously owned partition 0.
        ConsumerGroupHeartbeatResponseData.Assignment targetAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.singletonList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topicId)
                                .setPartitions(Arrays.asList(0, 1, 2))));
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(targetAssignment);
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());

        assertEquals(MemberState.SENDING_ACK_FOR_RECONCILED_ASSIGNMENT, membershipManager.state());
        Set<TopicPartition> assignedPartitions = new HashSet<>(Arrays.asList(
                ownedPartition,
                new TopicPartition("topic1", 1),
                new TopicPartition("topic1", 2)));
        assertEquals(assignedPartitions, membershipManager.currentAssignment());
        assertFalse(membershipManager.targetAssignment().isPresent());

        verify(commitRequestManager).resetAutoCommitTimer();
        verify(subscriptionState).assignFromSubscribed(anyCollection());
    }

    @Test
    public void testReconcilePartitionsRevokedNoAutoCommitNoCallbacks() {
        MembershipManagerImpl membershipManager = createMemberInStableState();
        mockOwnedPartition();

        mockRevocationNoCallbacks(false);

        receiveEmptyAssignment(membershipManager);

        testRevocationOfAllPartitionsCompleted(membershipManager);
    }

    @Test
    public void testReconcilePartitionsRevokedWithSuccessfulAutoCommitNoCallbacks() {
        MembershipManagerImpl membershipManager = createMemberInStableState();
        mockOwnedPartition();

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
        mockOwnedPartition();

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
        ConsumerGroupHeartbeatResponseData.Assignment targetAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.singletonList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topicId)
                                .setPartitions(Arrays.asList(1, 2))));
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(targetAssignment);
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());

        assertEquals(MemberState.SENDING_ACK_FOR_RECONCILED_ASSIGNMENT, membershipManager.state());
        Set<TopicPartition> assignedPartitions = new HashSet<>(Arrays.asList(
                new TopicPartition("topic1", 1),
                new TopicPartition("topic1", 2)));
        assertEquals(assignedPartitions, membershipManager.currentAssignment());
        assertFalse(membershipManager.targetAssignment().isPresent());

        verify(subscriptionState).assignFromSubscribed(anyCollection());
    }

    @Test
    public void testMemberReceivesFatalErrorWhileReconciling() {
        MembershipManager membershipManager = createMemberInStableState();
        mockOwnedPartition();

        CompletableFuture<Void> commitResult = mockRevocationNoCallbacks(true);

        receiveEmptyAssignment(membershipManager);

        // Member stays in RECONCILING while the commit request hasn't completed.
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        // Member received fatal error while reconciling
        membershipManager.transitionToFatal();

        // Complete commit request
        commitResult.complete(null);

        verify(subscriptionState, never()).assignFromSubscribed(anySet());
    }

    @Test
    public void testReconcileNewPartitionsMetadataRequestSuccess() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";

        mockOwnedPartitionAndAssignmentReceived(topicId, topicName, Collections.emptySet(), false);

        // Member received assignment to reconcile
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        ConsumerGroupHeartbeatResponseData.Assignment targetAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.singletonList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topicId)
                                .setPartitions(Arrays.asList(0, 1))));
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(targetAssignment);
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());

        // Member should request metadata once, and stay reconciling until it gets the update
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        verify(metadata, times(1)).requestUpdate(anyBoolean());
        mockTopicNameInMetadataCache(topicId, topicName, true);
        membershipManager.onUpdate(null);

        // Member should complete reconciliation
        assertEquals(MemberState.SENDING_ACK_FOR_RECONCILED_ASSIGNMENT, membershipManager.state());
        Set<TopicPartition> assignedPartitions = new HashSet<>(Arrays.asList(
                new TopicPartition(topicName, 0),
                new TopicPartition(topicName, 1)));
        assertEquals(assignedPartitions, membershipManager.currentAssignment());
        assertFalse(membershipManager.targetAssignment().isPresent());
        verify(commitRequestManager).resetAutoCommitTimer();
        verify(subscriptionState).assignFromSubscribed(anyCollection());
    }

    @Test
    public void testRevokePartitionsUsesTopicNamesLocalCacheWhenMetadataNotAvailable() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";

        mockOwnedPartitionAndAssignmentReceived(topicId, topicName, Collections.emptySet(), true);

        // Member received assignment to reconcile
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        ConsumerGroupHeartbeatResponseData.Assignment targetAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.singletonList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topicId)
                                .setPartitions(Arrays.asList(0, 1))));
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(targetAssignment);
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());

        // Member should complete reconciliation
        assertEquals(MemberState.SENDING_ACK_FOR_RECONCILED_ASSIGNMENT, membershipManager.state());
        Set<TopicPartition> assignedPartitions = new HashSet<>(Arrays.asList(
                new TopicPartition(topicName, 0),
                new TopicPartition(topicName, 1)));
        assertEquals(assignedPartitions, membershipManager.currentAssignment());
        assertFalse(membershipManager.targetAssignment().isPresent());

        mockAckSent(membershipManager);
        when(subscriptionState.assignedPartitions()).thenReturn(assignedPartitions);

        // Revocation of topic not found in metadata cache
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        mockRevocationNoCallbacks(false);
        mockTopicNameInMetadataCache(topicId, topicName, false);


        // Revoke one of the 2 partitions
        targetAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.singletonList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topicId)
                                .setPartitions(Arrays.asList(1))));
        heartbeatResponse = createConsumerGroupHeartbeatResponse(targetAssignment);
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());

        // Revocation should complete without requesting any metadata update given that the topic
        // received in target assignment should exist in local topic name cache.
        verify(metadata, never()).requestUpdate(anyBoolean());
        Set<TopicPartition> remainingAssignment = Collections.singleton(new TopicPartition(topicName, 1));

        testRevocationCompleted(membershipManager, remainingAssignment);
    }

    private void mockAckSent(MembershipManagerImpl membershipManager) {
        membershipManager.onHeartbeatRequestSent();
    }

    private void mockTopicNameInMetadataCache(Uuid topicId, String topicName, boolean isPresent) {
        if (isPresent) {
            when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));
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
                                         Set<TopicPartition> expectedCurrentAssignment) {
        assertEquals(MemberState.SENDING_ACK_FOR_RECONCILED_ASSIGNMENT, membershipManager.state());
        assertEquals(expectedCurrentAssignment, membershipManager.currentAssignment());
        assertFalse(membershipManager.targetAssignment().isPresent());

        verify(subscriptionState).markPendingRevocation(anySet());
        verify(subscriptionState).assignFromSubscribed(expectedCurrentAssignment);
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

    private void mockOwnedPartition() {
        String topicName = "topic1";
        TopicPartition previouslyOwned = new TopicPartition(topicName, 0);
        when(subscriptionState.assignedPartitions()).thenReturn(Collections.singleton(previouslyOwned));
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
    }

    private MembershipManager createMemberInReconcilingState() {
        MembershipManager membershipManager = createMembershipManagerJoiningGroup();
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(createAssignment());
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        return membershipManager;
    }

    private MembershipManagerImpl createMemberInStableState() {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(null);
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());
        assertEquals(MemberState.STABLE, membershipManager.state());
        return membershipManager;
    }

    private void receiveEmptyAssignment(MembershipManager membershipManager) {
        // New empty assignment received, revoking owned partition.
        ConsumerGroupHeartbeatResponseData.Assignment targetAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.emptyList());
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(targetAssignment);
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());
    }

    private void checkAssignments(
            MembershipManagerImpl membershipManager,
            Set<TopicPartition> expectedCurrentAssignment,
            ConsumerGroupHeartbeatResponseData.Assignment expectedTargetAssignment) {
        assertEquals(expectedCurrentAssignment, membershipManager.currentAssignment());
        assertEquals(expectedTargetAssignment, membershipManager.targetAssignment().orElse(null));
    }

    /**
     * Fenced member should release assignment, reset epoch to 0, keep member ID, and transition
     * to JOINING to rejoin the group.
     */
    private void testFencedMemberReleasesAssignmentAndResetsEpochToRejoin(MembershipManager membershipManager) {
        mockMemberHasAutoAssignedPartition();

        membershipManager.transitionToFenced();

        assertEquals(MEMBER_ID, membershipManager.memberId());
        assertEquals(0, membershipManager.memberEpoch());
        assertEquals(MemberState.JOINING, membershipManager.state());
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
    }

    /**
     * Member that intentionally leaves the group (via unsubscribe) should release assignment,
     * reset epoch to -1, keep member ID, and transition to SENDING_LEAVE_REQUEST to send out a
     * heartbeat with the leave epoch.
     */
    private void testLeaveGroupReleasesAssignmentAndResetsEpochToSendLeaveGroup(MembershipManager membershipManager) {
        mockMemberHasAutoAssignedPartition();
        doNothing().when(subscriptionState).markPendingRevocation(anySet());

        CompletableFuture<Void> leaveResult = membershipManager.leaveGroup();

        assertTrue(leaveResult.isDone());
        assertFalse(leaveResult.isCompletedExceptionally());
        assertEquals(MEMBER_ID, membershipManager.memberId());
        assertEquals(-1, membershipManager.memberEpoch());
        assertTrue(membershipManager.currentAssignment().isEmpty());
        assertEquals(MemberState.SENDING_LEAVE_REQUEST, membershipManager.state());
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
    }

    private void testStateUpdateOnFatalFailure(MembershipManager membershipManager) {
        String initialMemberId = membershipManager.memberId();
        membershipManager.transitionToFatal();
        assertEquals(MemberState.FATAL, membershipManager.state());
        // Should keep member id and reset epoch to -1 to indicate member not in the group
        assertEquals(initialMemberId, membershipManager.memberId());
        assertEquals(-1, membershipManager.memberEpoch());
    }

    private ConsumerGroupHeartbeatResponse createConsumerGroupHeartbeatResponse(ConsumerGroupHeartbeatResponseData.Assignment assignment) {
        return new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.NONE.code())
                .setMemberId(MEMBER_ID)
                .setMemberEpoch(MEMBER_EPOCH)
                .setAssignment(assignment));
    }

    private ConsumerGroupHeartbeatResponse createConsumerGroupHeartbeatResponseWithError(Errors error) {
        return new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(error.code())
                .setMemberId(MEMBER_ID)
                .setMemberEpoch(5));
    }

    private ConsumerGroupHeartbeatResponseData.Assignment createAssignment() {
        return new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(Uuid.randomUuid())
                                .setPartitions(Arrays.asList(0, 1, 2)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(Uuid.randomUuid())
                                .setPartitions(Arrays.asList(3, 4, 5))
                ));
    }
}
