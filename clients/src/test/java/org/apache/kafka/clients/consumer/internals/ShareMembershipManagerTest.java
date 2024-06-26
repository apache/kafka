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

import org.apache.kafka.clients.consumer.internals.metrics.ShareRebalanceMetricsManager;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData.Assignment;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ShareGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ShareGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_SHARE_METRIC_GROUP_PREFIX;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSortedSet;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShareMembershipManagerTest {

    private static final String GROUP_ID = "test-group";
    private static final String MEMBER_ID = "test-member-1";
    private static final String RACK_ID = null;
    private static final int MEMBER_EPOCH = 1;

    private final LogContext logContext = new LogContext();
    private SubscriptionState subscriptionState;
    private ConsumerMetadata metadata;

    private ShareConsumerTestBuilder testBuilder;
    private Time time;
    private ShareRebalanceMetricsManager shareRebalanceMetricsManager;
    private Metrics metrics;

    @BeforeEach
    public void setup() {
        testBuilder = new ShareConsumerTestBuilder(ShareConsumerTestBuilder.createDefaultGroupInformation());
        metadata = testBuilder.metadata;
        subscriptionState = testBuilder.subscriptions;
        time = testBuilder.time;
        metrics = new Metrics(time);
        shareRebalanceMetricsManager = new ShareRebalanceMetricsManager(metrics);
    }

    @AfterEach
    public void tearDown() {
        if (testBuilder != null) {
            testBuilder.close();
        }
    }

    private ShareMembershipManager createMembershipManagerJoiningGroup() {
        ShareMembershipManager manager = spy(new ShareMembershipManager(
                logContext, GROUP_ID, RACK_ID, subscriptionState, metadata,
                Optional.empty(), time, shareRebalanceMetricsManager));
        manager.transitionToJoining();
        return manager;
    }

    @Test
    public void testMembershipManagerRegistersForClusterMetadataUpdatesOnFirstJoin() {
        // First join should register to get metadata updates
        ShareMembershipManager manager = new ShareMembershipManager(
                logContext, GROUP_ID, RACK_ID, subscriptionState, metadata,
                Optional.empty(), time, shareRebalanceMetricsManager);
        manager.transitionToJoining();
        clearInvocations(metadata);

        // Following joins should not register again.
        receiveEmptyAssignment(manager);
        mockLeaveGroup();
        manager.leaveGroup();
        assertEquals(MemberState.LEAVING, manager.state());
        manager.onHeartbeatRequestSent();
        assertEquals(MemberState.UNSUBSCRIBED, manager.state());
        manager.transitionToJoining();
    }

    @Test
    public void testReconcilingWhenReceivingAssignmentFoundInMetadata() {
        ShareMembershipManager membershipManager = mockJoinAndReceiveAssignment(true);
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());

        // When the ack is sent the member should go back to STABLE
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.STABLE, membershipManager.state());
    }

    @Test
    public void testTransitionToAcknowledgingOnlyIfAssignmentReceived() {
        ShareMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        assertEquals(MemberState.JOINING, membershipManager.state());

        ShareGroupHeartbeatResponse responseWithoutAssignment = createShareGroupHeartbeatResponse(new Assignment());
        membershipManager.onHeartbeatSuccess(responseWithoutAssignment.data());
        assertNotEquals(MemberState.RECONCILING, membershipManager.state());

        ShareGroupHeartbeatResponse responseWithAssignment =
                createShareGroupHeartbeatResponse(createAssignment(true));
        membershipManager.onHeartbeatSuccess(responseWithAssignment.data());
        assertEquals(MemberState.RECONCILING, membershipManager.state());
    }

    @Test
    public void testMemberIdAndEpochResetOnFencedMembers() {
        ShareMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        ShareGroupHeartbeatResponse heartbeatResponse = createShareGroupHeartbeatResponse(new Assignment());
        membershipManager.onHeartbeatSuccess(heartbeatResponse.data());
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
        ShareMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        ShareGroupHeartbeatResponse heartbeatResponse = createShareGroupHeartbeatResponse(new Assignment());
        membershipManager.onHeartbeatSuccess(heartbeatResponse.data());
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
        ShareMembershipManager membershipManager = new ShareMembershipManager(
                logContext, GROUP_ID, RACK_ID, subscriptionState, metadata,
                Optional.empty(), time, shareRebalanceMetricsManager);
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        membershipManager.transitionToJoining();

        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        membershipManager.transitionToFatal();
        assertEquals(MemberState.FATAL, membershipManager.state());
    }

    @Test
    public void testFencingWhenStateIsStable() {
        ShareMembershipManager membershipManager = createMemberInStableState();
        testFencedMemberReleasesAssignmentAndTransitionsToJoining(membershipManager);
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
    }

    @Test
    public void testListenersGetNotifiedOnTransitionsToFatal() {
        ShareMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        subscriptionState.subscribe(Collections.singleton("topic1"), Optional.empty());
        MemberStateListener listener = mock(MemberStateListener.class);
        membershipManager.registerStateListener(listener);
        mockStableMember(membershipManager);
        verify(listener).onMemberEpochUpdated(Optional.of(MEMBER_EPOCH), Optional.of(MEMBER_ID));
        clearInvocations(listener);

        // Transition to FAILED before getting member ID/epoch
        membershipManager.transitionToFatal();
        assertEquals(MemberState.FATAL, membershipManager.state());
        verify(listener).onMemberEpochUpdated(Optional.empty(), Optional.empty());
    }

    @Test
    public void testListenersGetNotifiedOnTransitionsToLeavingGroup() {
        ShareMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        MemberStateListener listener = mock(MemberStateListener.class);
        membershipManager.registerStateListener(listener);
        mockStableMember(membershipManager);
        verify(listener).onMemberEpochUpdated(Optional.of(MEMBER_EPOCH), Optional.of(MEMBER_ID));
        clearInvocations(listener);

        mockLeaveGroup();
        membershipManager.leaveGroup();
        assertEquals(MemberState.LEAVING, membershipManager.state());
        verify(listener).onMemberEpochUpdated(Optional.empty(), Optional.empty());
    }

    @Test
    public void testListenersGetNotifiedOfMemberEpochUpdatesOnlyIfItChanges() {
        ShareMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        MemberStateListener listener = mock(MemberStateListener.class);
        membershipManager.registerStateListener(listener);
        int epoch = 5;

        membershipManager.onHeartbeatSuccess(new ShareGroupHeartbeatResponseData()
                .setErrorCode(Errors.NONE.code())
                .setMemberId(MEMBER_ID)
                .setMemberEpoch(epoch));

        verify(listener).onMemberEpochUpdated(Optional.of(epoch), Optional.of(MEMBER_ID));
        clearInvocations(listener);

        membershipManager.onHeartbeatSuccess(new ShareGroupHeartbeatResponseData()
                .setErrorCode(Errors.NONE.code())
                .setMemberId(MEMBER_ID)
                .setMemberEpoch(epoch));
        verify(listener, never()).onMemberEpochUpdated(any(), any());
    }

    private void mockStableMember(ShareMembershipManager membershipManager) {
        ShareGroupHeartbeatResponse heartbeatResponse = createShareGroupHeartbeatResponse(new Assignment());
        membershipManager.onHeartbeatSuccess(heartbeatResponse.data());
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertEquals(MEMBER_ID, membershipManager.memberId());
        assertEquals(MEMBER_EPOCH, membershipManager.memberEpoch());
    }

    @Test
    public void testFencingWhenStateIsReconciling() {
        ShareMembershipManager membershipManager = mockJoinAndReceiveAssignment(false);
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        testFencedMemberReleasesAssignmentAndTransitionsToJoining(membershipManager);
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
    }

    /**
     * Members keep sending heartbeats while preparing to leave, so they could get fenced. This
     * tests ensures that if a member gets fenced while preparing to leave, it will directly
     * transition to UNSUBSCRIBE (rejoin as in regular fencing scenarios), and when the
     * LEAVING completes it remains UNSUBSCRIBED (no last heartbeat sent).
     */
    @Test
    public void testFencingWhenStateIsLeaving() {
        ShareMembershipManager membershipManager = createMemberInStableState();
        mockPrepareLeaving(membershipManager);
        assertEquals(MemberState.LEAVING, membershipManager.state());

        // Get fenced while preparing to leave the group. Member should ignore the fence
        // (no rejoin) and should continue leaving the group as it was before getting fenced.
        clearInvocations(subscriptionState);
        membershipManager.transitionToFenced();
        testFenceIsNoOp(membershipManager);
        assertEquals(MemberState.LEAVING, membershipManager.state());

        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());

        // Last heartbeat sent is expected to fail, leading to a call to transitionToFenced. That
        // should be no-op because the member already left.
        clearInvocations(subscriptionState);
        membershipManager.transitionToFenced();
        testFenceIsNoOp(membershipManager);
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
    }

    @Test
    public void testLeaveGroupEpoch() {
        // Member should leave the group with epoch -1.
        ShareMembershipManager membershipManager = createMemberInStableState();
        mockLeaveGroup();
        membershipManager.leaveGroup();
        assertEquals(MemberState.LEAVING, membershipManager.state());
        assertEquals(ShareGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH,
                membershipManager.memberEpoch());
    }

    // Tests the case where topic metadata is not available at the time of the assignment,
    // but is made available later.
    @Test
    public void testDelayedMetadataUsedToCompleteAssignment() {

        Uuid topicId1 = Uuid.randomUuid();
        String topic1 = "topic1";
        final TopicIdPartition topicId1Partition0 = new TopicIdPartition(topicId1, new TopicPartition(topic1, 0));

        Uuid topicId2 = Uuid.randomUuid();
        String topic2 = "topic2";
        final TopicIdPartition topicId2Partition0 = new TopicIdPartition(topicId2, new TopicPartition(topic2, 0));

        // Receive assignment with only topic1-0, entering STABLE state.
        ShareMembershipManager membershipManager =
                mockMemberSuccessfullyReceivesAndAcknowledgesAssignment(topicId1, topic1, Collections.singletonList(0));

        membershipManager.onHeartbeatRequestSent();

        assertEquals(MemberState.STABLE, membershipManager.state());
        when(subscriptionState.assignedPartitions()).thenReturn(getTopicPartitions(Collections.singleton(topicId1Partition0)));
        clearInvocations(membershipManager, subscriptionState);

        // New assignment adding a new topic2-0 (not in metadata).
        // No reconciliation triggered, because new topic in assignment is waiting for metadata.

        Map<Uuid, SortedSet<Integer>> newAssignment =
                mkMap(
                        mkEntry(topicId1, mkSortedSet(0)),
                        mkEntry(topicId2, mkSortedSet(0))
                );

        receiveAssignment(newAssignment, membershipManager);
        membershipManager.poll(time.milliseconds());

        verifyReconciliationNotTriggered(membershipManager);
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertEquals(Collections.singleton(topicId2), membershipManager.topicsAwaitingReconciliation());
        verify(metadata).requestUpdate(anyBoolean());
        clearInvocations(membershipManager);

        // Metadata discovered for topic2. Should trigger reconciliation to complete the assignment,
        // with membership manager entering ACKNOWLEDGING state.

        Map<Uuid, String> fullTopicMetadata = mkMap(
                mkEntry(topicId1, topic1),
                mkEntry(topicId2, topic2)
        );
        when(metadata.topicNames()).thenReturn(fullTopicMetadata);

        membershipManager.poll(time.milliseconds());

        verifyReconciliationTriggeredAndCompleted(membershipManager, Arrays.asList(topicId1Partition0, topicId2Partition0));
    }

    @Test
    public void testLeaveGroupWhenStateIsStable() {
        ShareMembershipManager membershipManager = createMemberInStableState();
        testLeaveGroupReleasesAssignmentAndResetsEpochToSendLeaveGroup(membershipManager);
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
    }

    @Test
    public void testLeaveGroupWhenStateIsReconciling() {
        ShareMembershipManager membershipManager = mockJoinAndReceiveAssignment(false);
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        testLeaveGroupReleasesAssignmentAndResetsEpochToSendLeaveGroup(membershipManager);
    }

    @Test
    public void testIgnoreHeartbeatWhenLeavingGroup() {
        ShareMembershipManager membershipManager = createMemberInStableState();
        mockLeaveGroup();

        CompletableFuture<Void> leaveResult = membershipManager.leaveGroup();

        membershipManager.onHeartbeatSuccess(createShareGroupHeartbeatResponse(createAssignment(true)).data());

        assertEquals(MemberState.LEAVING, membershipManager.state());
        assertEquals(-1, membershipManager.memberEpoch());
        assertEquals(MEMBER_ID, membershipManager.memberId());
        assertTrue(membershipManager.currentAssignment().isEmpty());
        assertFalse(leaveResult.isDone(), "Leave group result should not complete until the " +
                "heartbeat request to leave is sent out.");
    }

    @Test
    public void testLeaveGroupWhenMemberOwnsAssignment() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";
        ShareMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId, topicName, Collections.emptyList());

        receiveAssignment(topicId, Arrays.asList(0, 1), membershipManager);

        verifyReconciliationNotTriggered(membershipManager);
        membershipManager.poll(time.milliseconds());

        List<TopicIdPartition> assignedPartitions = Arrays.asList(
                new TopicIdPartition(topicId, new TopicPartition(topicName, 0)),
                new TopicIdPartition(topicId, new TopicPartition(topicName, 1)));
        verifyReconciliationTriggeredAndCompleted(membershipManager, assignedPartitions);

        assertEquals(1, membershipManager.currentAssignment().size());

        testLeaveGroupReleasesAssignmentAndResetsEpochToSendLeaveGroup(membershipManager);
    }

    @Test
    public void testLeaveGroupWhenMemberAlreadyLeaving() {
        ShareMembershipManager membershipManager = createMemberInStableState();

        // First leave attempt. Should stay LEAVING until the heartbeat is sent out.
        mockLeaveGroup();
        CompletableFuture<Void> leaveResult1 = membershipManager.leaveGroup();
        assertEquals(MemberState.LEAVING, membershipManager.state());
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
        clearInvocations(subscriptionState);

        // Second leave attempt while the first one has not completed yet.
        mockLeaveGroup();
        CompletableFuture<Void> leaveResult2 = membershipManager.leaveGroup();
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
        ShareMembershipManager membershipManager = createMemberInStableState();

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

        // Call to leave group again, when member already left. Should be no-op (no assignment updated)
        mockLeaveGroup();
        CompletableFuture<Void> leaveResult2 = membershipManager.leaveGroup();
        assertTrue(leaveResult2.isDone());
        assertFalse(leaveResult2.isCompletedExceptionally());
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        verify(subscriptionState, never()).assignFromSubscribed(Collections.emptySet());
    }

    @Test
    public void testFatalFailureWhenStateIsNotJoined() {
        ShareMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        assertEquals(MemberState.JOINING, membershipManager.state());

        testStateUpdateOnFatalFailure(membershipManager);
    }

    @Test
    public void testFatalFailureWhenStateIsStable() {
        ShareMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        ShareGroupHeartbeatResponse heartbeatResponse = createShareGroupHeartbeatResponse(new Assignment());
        membershipManager.onHeartbeatSuccess(heartbeatResponse.data());
        assertEquals(MemberState.STABLE, membershipManager.state());

        testStateUpdateOnFatalFailure(membershipManager);
    }

    @Test
    public void testFatalFailureWhenStateIsLeaving() {
        ShareMembershipManager membershipManager = createMemberInStableState();

        // Start leaving group.
        mockLeaveGroup();
        membershipManager.leaveGroup();
        assertEquals(MemberState.LEAVING, membershipManager.state());

        // Get fatal failure while waiting to send the heartbeat to leave. Member should
        // transition to FATAL, so the last heartbeat for leaving won't be sent because the member
        // already failed.
        testStateUpdateOnFatalFailure(membershipManager);

        assertEquals(MemberState.FATAL, membershipManager.state());
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.FATAL, membershipManager.state());
    }

    @Test
    public void testFatalFailureWhenMemberAlreadyLeft() {
        ShareMembershipManager membershipManager = createMemberInStableState();

        // Start leaving group.
        mockLeaveGroup();
        membershipManager.leaveGroup();
        assertEquals(MemberState.LEAVING, membershipManager.state());

        // Last heartbeat sent.
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());

        // Fatal error received in response for the last heartbeat. Member should remain in FATAL
        // state but the member already left the group.
        membershipManager.transitionToFatal();

        assertEquals(MemberState.FATAL, membershipManager.state());
    }

    @Test
    public void testUpdateStateFailsOnResponsesWithErrors() {
        ShareMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        // Updating state with a heartbeat response containing errors cannot be performed and
        // should fail.
        ShareGroupHeartbeatResponse unknownMemberResponse = createShareGroupHeartbeatResponseWithError();
        assertThrows(IllegalArgumentException.class,
                () -> membershipManager.onHeartbeatSuccess(unknownMemberResponse.data()));
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
        ShareMembershipManager membershipManager = mockJoinAndReceiveAssignment(false);
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertFalse(membershipManager.topicsAwaitingReconciliation().isEmpty());

        // When the ack is sent nothing should change. Member still has nothing to reconcile,
        // only topics waiting for metadata.
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertFalse(membershipManager.topicsAwaitingReconciliation().isEmpty());

        // New target assignment received while there is another one waiting to be resolved
        // and reconciled. This assignment does not include the previous one that is waiting
        // for metadata, so the member will discard the topics that were waiting for metadata, and
        // reconcile the new assignment.
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));
        receiveAssignment(topicId, Collections.singletonList(0), membershipManager);

        verifyReconciliationNotTriggered(membershipManager);
        membershipManager.poll(time.milliseconds());

        Set<TopicPartition> expectedAssignment = Collections.singleton(new TopicPartition(topicName, 0));
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        verify(subscriptionState).assignFromSubscribed(expectedAssignment);

        // When ack for the reconciled assignment is sent, member should go back to STABLE
        // because the first assignment that was not resolved should have been discarded
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertTrue(membershipManager.topicsAwaitingReconciliation().isEmpty());
    }

    /**
     * This test ensures that member goes back to STABLE when the broker sends assignment that
     * removes the unresolved target the client has, without triggering a reconciliation. In this
     * case the member should discard the assignment that was unresolved and go back to STABLE with
     * nothing to reconcile.
     */
    @Test
    public void testNewEmptyAssignmentReplacesPreviousOneWaitingOnMetadata() {
        ShareMembershipManager membershipManager = mockJoinAndReceiveAssignment(false);
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertFalse(membershipManager.topicsAwaitingReconciliation().isEmpty());

        // When the ack is sent nothing should change. Member still has nothing to reconcile,
        // only topics waiting for metadata.
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertFalse(membershipManager.topicsAwaitingReconciliation().isEmpty());

        // New target assignment received while there is another one waiting to be resolved
        // and reconciled. This assignment does not include the previous one that is waiting
        // for metadata, so the member will discard the topics that were waiting for metadata, and
        // reconcile the new assignment.
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));
        receiveEmptyAssignment(membershipManager);
        assertEquals(MemberState.STABLE, membershipManager.state());
        verify(subscriptionState, never()).assignFromSubscribed(any());
    }

    @Test
    public void testNewAssignmentNotInMetadataReplacesPreviousOneWaitingOnMetadata() {
        ShareMembershipManager membershipManager = mockJoinAndReceiveAssignment(false);
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertFalse(membershipManager.topicsAwaitingReconciliation().isEmpty());

        // New target assignment (not found in metadata) received while there is another one
        // waiting to be resolved and reconciled. This assignment does not include the previous
        // one that is waiting for metadata, so the member will discard the topics that were
        // waiting for metadata, and just keep the new one as unresolved.
        Uuid topicId = Uuid.randomUuid();
        when(metadata.topicNames()).thenReturn(Collections.emptyMap());
        receiveAssignment(topicId, Collections.singletonList(0), membershipManager);
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertFalse(membershipManager.topicsAwaitingReconciliation().isEmpty());
        assertEquals(topicId, membershipManager.topicsAwaitingReconciliation().iterator().next());
    }

    /**
     *  This ensures that the client reconciles target assignments as soon as they are discovered
     *  in metadata, without depending on the broker to re-send the assignment.
     */
    @Test
    public void testUnresolvedTargetAssignmentIsReconciledWhenMetadataReceived() {
        ShareMembershipManager membershipManager = createMemberInStableState();
        // Assignment not in metadata. Member cannot reconcile it yet, but keeps it to be
        // reconciled when metadata is discovered.
        Uuid topicId = Uuid.randomUuid();
        receiveAssignment(topicId, Collections.singletonList(1), membershipManager);
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertFalse(membershipManager.topicsAwaitingReconciliation().isEmpty());

        // Metadata update received, including the missing topic name.
        String topicName = "topic1";
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);

        assertEquals(0.0d, getMetric("rebalance-total").metricValue());
        assertEquals(0.0d, getMetric("rebalance-rate-per-hour").metricValue());
        membershipManager.poll(time.milliseconds());

        // Assignment should have been reconciled.
        Set<TopicPartition> expectedAssignment = Collections.singleton(new TopicPartition(topicName, 1));
        verify(subscriptionState).assignFromSubscribed(expectedAssignment);
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        assertTrue(membershipManager.topicsAwaitingReconciliation().isEmpty());
        assertEquals(1.0d, getMetric("rebalance-total").metricValue());
        assertEquals(120.d, (double) getMetric("rebalance-rate-per-hour").metricValue(), 0.2d);
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
        ShareGroupHeartbeatResponseData.Assignment assignment = new ShareGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Arrays.asList(
                        new ShareGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topic1)
                                .setPartitions(Collections.singletonList(0)),
                        new ShareGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topic2)
                                .setPartitions(Arrays.asList(1, 3))
                ));
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topic1, topic1Name));

        // Receive assignment partly in metadata - reconcile+ack what's in metadata, keep the
        // unresolved and request metadata update.
        ShareMembershipManager membershipManager = mockJoinAndReceiveAssignment(true, assignment);
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        verify(metadata).requestUpdate(anyBoolean());
        assertEquals(Collections.singleton(topic2), membershipManager.topicsAwaitingReconciliation());

        // When the ack is sent the member should go back to RECONCILING because it still has
        // unresolved assignment to be reconciled.
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        // Target assignment received again with the same unresolved topic. Client should keep it
        // as unresolved.
        clearInvocations(subscriptionState);
        membershipManager.onHeartbeatSuccess(createShareGroupHeartbeatResponse(assignment).data());
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertEquals(Collections.singleton(topic2), membershipManager.topicsAwaitingReconciliation());
        verify(subscriptionState, never()).assignFromSubscribed(anyCollection());
    }

    @Test
    public void testReconcileNewPartitionsAssignedWhenNoPartitionOwned() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";
        ShareMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId, topicName, Collections.emptyList());

        receiveAssignment(topicId, Arrays.asList(0, 1), membershipManager);

        verifyReconciliationNotTriggered(membershipManager);
        membershipManager.poll(time.milliseconds());

        List<TopicIdPartition> assignedPartitions = topicIdPartitions(topicId, topicName, 0, 1);
        verifyReconciliationTriggeredAndCompleted(membershipManager, assignedPartitions);
    }

    @Test
    public void testReconcileNewPartitionsAssignedWhenOtherPartitionsOwned() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";
        TopicIdPartition ownedPartition = new TopicIdPartition(topicId, new TopicPartition(topicName, 0));
        ShareMembershipManager membershipManager = createMemberInStableState();
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId, topicName,
                Collections.singletonList(ownedPartition));

        // New assignment received, adding partitions 1 and 2 to the previously owned partition 0.
        receiveAssignment(topicId, Arrays.asList(0, 1, 2), membershipManager);

        verifyReconciliationNotTriggered(membershipManager);
        membershipManager.poll(time.milliseconds());

        List<TopicIdPartition> assignedPartitions = new ArrayList<>();
        assignedPartitions.add(ownedPartition);
        assignedPartitions.addAll(topicIdPartitions(topicId, topicName, 1, 2));
        verifyReconciliationTriggeredAndCompleted(membershipManager, assignedPartitions);
    }

    @Test
    public void testReconciliationSkippedWhenSameAssignmentReceived() {
        // Member stable, no assignment
        ShareMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";

        // Receive assignment different from what the member owns - should reconcile
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId, topicName, Collections.emptyList());
        List<TopicIdPartition> expectedAssignmentReconciled = topicIdPartitions(topicId, topicName, 0, 1);
        receiveAssignment(topicId, Arrays.asList(0, 1), membershipManager);

        verifyReconciliationNotTriggered(membershipManager);
        membershipManager.poll(time.milliseconds());

        verifyReconciliationTriggeredAndCompleted(membershipManager, expectedAssignmentReconciled);
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        clearInvocations(subscriptionState, membershipManager);

        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.STABLE, membershipManager.state());

        // Receive same assignment again - should not trigger reconciliation
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId, topicName, expectedAssignmentReconciled);
        receiveAssignment(topicId, Arrays.asList(0, 1), membershipManager);
        // Verify new reconciliation was not triggered
        verify(membershipManager, never()).markReconciliationInProgress();
        verify(membershipManager, never()).markReconciliationCompleted();
        verify(subscriptionState, never()).assignFromSubscribed(anyCollection());

        assertEquals(MemberState.STABLE, membershipManager.state());
    }

    @Test
    public void testReconcileNewPartitionsAssignedAndRevoked() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";
        TopicIdPartition ownedPartition = new TopicIdPartition(topicId,
                new TopicPartition(topicName, 0));
        ShareMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId, topicName,
                Collections.singletonList(ownedPartition));

        mockRevocation();

        // New assignment received, revoking partition 0, and assigning new partitions 1 and 2.
        receiveAssignment(topicId, Arrays.asList(1, 2), membershipManager);

        verifyReconciliationNotTriggered(membershipManager);
        membershipManager.poll(time.milliseconds());

        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        assertEquals(topicIdPartitionsMap(topicId, 1, 2), membershipManager.currentAssignment());
        assertFalse(membershipManager.reconciliationInProgress());

        verify(subscriptionState).assignFromSubscribed(anyCollection());
    }

    @Test
    public void testMetadataUpdatesReconcilesUnresolvedAssignments() {
        Uuid topicId = Uuid.randomUuid();

        // Assignment not in metadata
        ShareGroupHeartbeatResponseData.Assignment targetAssignment = new ShareGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.singletonList(
                        new ShareGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topicId)
                                .setPartitions(Arrays.asList(0, 1))));
        ShareMembershipManager membershipManager = mockJoinAndReceiveAssignment(false, targetAssignment);
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        // Should not trigger reconciliation, and request a metadata update.
        verifyReconciliationNotTriggered(membershipManager);
        assertEquals(Collections.singleton(topicId), membershipManager.topicsAwaitingReconciliation());
        verify(metadata).requestUpdate(anyBoolean());

        String topicName = "topic1";
        mockTopicNameInMetadataCache(Collections.singletonMap(topicId, topicName), true);

        // When metadata is updated, the member should re-trigger reconciliation
        membershipManager.poll(time.milliseconds());
        List<TopicIdPartition> expectedAssignmentReconciled = topicIdPartitions(topicId, topicName, 0, 1);
        verifyReconciliationTriggeredAndCompleted(membershipManager, expectedAssignmentReconciled);
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        assertTrue(membershipManager.topicsAwaitingReconciliation().isEmpty());
    }

    @Test
    public void testMetadataUpdatesRequestsAnotherUpdateIfNeeded() {
        Uuid topicId = Uuid.randomUuid();

        // Assignment not in metadata
        ShareGroupHeartbeatResponseData.Assignment targetAssignment = new ShareGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.singletonList(
                        new ShareGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topicId)
                                .setPartitions(Arrays.asList(0, 1))));
        ShareMembershipManager membershipManager = mockJoinAndReceiveAssignment(false, targetAssignment);
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        // Should not trigger reconciliation, and request a metadata update.
        verifyReconciliationNotTriggered(membershipManager);
        assertEquals(Collections.singleton(topicId), membershipManager.topicsAwaitingReconciliation());
        verify(metadata).requestUpdate(anyBoolean());

        // Next poll is run, but metadata still without the unresolved topic in it. Should keep
        // the unresolved and request update again.
        when(metadata.topicNames()).thenReturn(Collections.emptyMap());
        membershipManager.poll(time.milliseconds());
        verifyReconciliationNotTriggered(membershipManager);
        assertEquals(Collections.singleton(topicId), membershipManager.topicsAwaitingReconciliation());
        verify(metadata, times(2)).requestUpdate(anyBoolean());
    }

    @Test
    public void testRevokePartitionsUsesTopicNamesLocalCacheWhenMetadataNotAvailable() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";

        ShareMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId, topicName, Collections.emptyList());

        // Member received assignment to reconcile;

        receiveAssignment(topicId, Arrays.asList(0, 1), membershipManager);

        verifyReconciliationNotTriggered(membershipManager);
        membershipManager.poll(time.milliseconds());

        // Member should complete reconciliation
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        List<Integer> partitions = Arrays.asList(0, 1);
        Set<TopicPartition> assignedPartitions =
                partitions.stream().map(p -> new TopicPartition(topicName, p)).collect(Collectors.toSet());
        Map<Uuid, SortedSet<Integer>> assignedTopicIdPartitions = Collections.singletonMap(topicId,
                new TreeSet<>(partitions));
        assertEquals(assignedTopicIdPartitions, membershipManager.currentAssignment());
        assertFalse(membershipManager.reconciliationInProgress());

        mockAckSent(membershipManager);
        when(subscriptionState.assignedPartitions()).thenReturn(assignedPartitions);

        // Revocation of topic not found in metadata cache
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        mockRevocation();
        mockTopicNameInMetadataCache(Collections.singletonMap(topicId, topicName), false);

        // Revoke one of the 2 partitions
        receiveAssignment(topicId, Collections.singletonList(1), membershipManager);

        membershipManager.poll(time.milliseconds());

        // Revocation should complete without requesting any metadata update given that the topic
        // received in target assignment should exist in local topic name cache.
        verify(metadata, never()).requestUpdate(anyBoolean());
        List<TopicIdPartition> remainingAssignment = topicIdPartitions(topicId, topicName, 1);

        testRevocationCompleted(membershipManager, remainingAssignment);
    }

    @Test
    public void testOnSubscriptionUpdatedTransitionsToJoiningOnlyIfNotInGroup() {
        ShareMembershipManager membershipManager = createMemberInStableState();
        verify(membershipManager).transitionToJoining();
        clearInvocations(membershipManager);
        membershipManager.onSubscriptionUpdated();
        verify(membershipManager, never()).transitionToJoining();
    }

    private SortedSet<TopicIdPartition> topicIdPartitionsSet(Uuid topicId, String topicName, int... partitions) {
        SortedSet<TopicIdPartition> topicIdPartitions = new TreeSet<>(new Utils.TopicIdPartitionComparator());

        for (int partition : partitions)
            topicIdPartitions.add(new TopicIdPartition(topicId, new TopicPartition(topicName, partition)));

        return topicIdPartitions;
    }

    private List<TopicIdPartition> topicIdPartitions(Uuid topicId, String topicName, int... partitions) {
        return new ArrayList<>(topicIdPartitionsSet(topicId, topicName, partitions));
    }

    private Map<Uuid, SortedSet<Integer>> topicIdPartitionsMap(Uuid topicId, int... partitions) {
        SortedSet<Integer> topicIdPartitions = new TreeSet<>();

        for (int partition : partitions)
            topicIdPartitions.add(partition);

        return Collections.singletonMap(topicId, topicIdPartitions);
    }

    private void testFenceIsNoOp(ShareMembershipManager membershipManager) {
        assertNotEquals(0, membershipManager.memberEpoch());
        verify(subscriptionState, never()).rebalanceListener();
    }

    private void assertStaleMemberLeavesGroupAndClearsAssignment(ShareMembershipManager membershipManager) {
        assertEquals(MemberState.STALE, membershipManager.state());

        // Should reset epoch to leave the group and release the assignment (right away because
        // there is no onPartitionsLost callback defined)
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
        assertTrue(membershipManager.currentAssignment().isEmpty());
        assertTrue(membershipManager.topicsAwaitingReconciliation().isEmpty());
        assertEquals(ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH, membershipManager.memberEpoch());
    }

    @Test
    public void testMemberJoiningTransitionsToStableWhenReceivingEmptyAssignment() {
        ShareMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        assertEquals(MemberState.JOINING, membershipManager.state());
        receiveEmptyAssignment(membershipManager);
        assertEquals(MemberState.STABLE, membershipManager.state());
    }

    private ShareMembershipManager mockMemberSuccessfullyReceivesAndAcknowledgesAssignment(
            Uuid topicId, String topicName, List<Integer> partitions) {
        ShareMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId, topicName,
                Collections.emptyList());

        receiveAssignment(topicId, partitions, membershipManager);

        verifyReconciliationNotTriggered(membershipManager);
        membershipManager.poll(time.milliseconds());

        List<TopicIdPartition> assignedPartitions =
                partitions.stream().map(tp -> new TopicIdPartition(topicId,
                        new TopicPartition(topicName, tp))).collect(Collectors.toList());
        verifyReconciliationTriggeredAndCompleted(membershipManager, assignedPartitions);
        return membershipManager;
    }

    private void verifyReconciliationNotTriggered(ShareMembershipManager membershipManager) {
        verify(membershipManager, never()).markReconciliationInProgress();
        verify(membershipManager, never()).markReconciliationCompleted();
    }

    private void verifyReconciliationTriggeredAndCompleted(ShareMembershipManager membershipManager,
                                                           List<TopicIdPartition> expectedAssignment) {
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        verify(membershipManager).markReconciliationInProgress();
        verify(membershipManager).markReconciliationCompleted();
        assertFalse(membershipManager.reconciliationInProgress());

        // Assignment applied
        List<TopicPartition> expectedTopicPartitions = buildTopicPartitions(expectedAssignment);
        verify(subscriptionState).assignFromSubscribed(new HashSet<>(expectedTopicPartitions));
        Map<Uuid, SortedSet<Integer>> assignmentByTopicId = assignmentByTopicId(expectedAssignment);
        assertEquals(assignmentByTopicId, membershipManager.currentAssignment());
    }

    private List<TopicPartition> buildTopicPartitions(List<TopicIdPartition> topicIdPartitions) {
        return topicIdPartitions.stream().map(TopicIdPartition::topicPartition).collect(Collectors.toList());
    }

    private void mockAckSent(ShareMembershipManager membershipManager) {
        membershipManager.onHeartbeatRequestSent();
    }

    private void mockTopicNameInMetadataCache(Map<Uuid, String> topicNames, boolean isPresent) {
        if (isPresent) {
            when(metadata.topicNames()).thenReturn(topicNames);
        } else {
            when(metadata.topicNames()).thenReturn(Collections.emptyMap());
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    private CompletableFuture<Void> mockRevocation() {
        doNothing().when(subscriptionState).markPendingRevocation(anySet());
        return CompletableFuture.completedFuture(null);
    }

    private void mockMemberHasAutoAssignedPartition() {
        String topicName = "topic1";
        TopicPartition ownedPartition = new TopicPartition(topicName, 0);
        when(subscriptionState.assignedPartitions()).thenReturn(Collections.singleton(ownedPartition));
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
    }

    private void testRevocationCompleted(ShareMembershipManager membershipManager,
                                         List<TopicIdPartition> expectedCurrentAssignment) {
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        Map<Uuid, SortedSet<Integer>> assignmentByTopicId = assignmentByTopicId(expectedCurrentAssignment);
        assertEquals(assignmentByTopicId, membershipManager.currentAssignment());
        assertFalse(membershipManager.reconciliationInProgress());

        verify(subscriptionState).markPendingRevocation(anySet());
        List<TopicPartition> expectedTopicPartitionAssignment =
                buildTopicPartitions(expectedCurrentAssignment);
        verify(subscriptionState).assignFromSubscribed(new HashSet<>(expectedTopicPartitionAssignment));
    }

    private Map<Uuid, SortedSet<Integer>> assignmentByTopicId(List<TopicIdPartition> topicIdPartitions) {
        Map<Uuid, SortedSet<Integer>> assignmentByTopicId = new HashMap<>();
        topicIdPartitions.forEach(topicIdPartition -> {
            Uuid topicId = topicIdPartition.topicId();
            assignmentByTopicId.computeIfAbsent(topicId, k -> new TreeSet<>()).add(topicIdPartition.partition());
        });
        return assignmentByTopicId;
    }

    private void mockOwnedPartitionAndAssignmentReceived(ShareMembershipManager membershipManager,
                                                         Uuid topicId,
                                                         String topicName,
                                                         Collection<TopicIdPartition> previouslyOwned) {
        when(subscriptionState.assignedPartitions()).thenReturn(getTopicPartitions(previouslyOwned));
        membershipManager.updateAssignment(new HashSet<>(previouslyOwned));
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
    }

    private Set<TopicPartition> getTopicPartitions(Collection<TopicIdPartition> topicIdPartitions) {
        return topicIdPartitions.stream().map(topicIdPartition ->
                        new TopicPartition(topicIdPartition.topic(), topicIdPartition.partition()))
                .collect(Collectors.toSet());
    }

    private ShareMembershipManager mockJoinAndReceiveAssignment(boolean expectSubscriptionUpdated) {
        return mockJoinAndReceiveAssignment(expectSubscriptionUpdated, createAssignment(expectSubscriptionUpdated));
    }

    private ShareMembershipManager mockJoinAndReceiveAssignment(boolean expectSubscriptionUpdated,
                                                                ShareGroupHeartbeatResponseData.Assignment assignment) {
        ShareMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        ShareGroupHeartbeatResponse heartbeatResponse = createShareGroupHeartbeatResponse(assignment);
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);

        membershipManager.onHeartbeatSuccess(heartbeatResponse.data());
        membershipManager.poll(time.milliseconds());

        if (expectSubscriptionUpdated) {
            verify(subscriptionState).assignFromSubscribed(anyCollection());
        } else {
            verify(subscriptionState, never()).assignFromSubscribed(anyCollection());
        }

        return membershipManager;
    }

    private ShareMembershipManager createMemberInStableState() {
        ShareMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        ShareGroupHeartbeatResponse heartbeatResponse = createShareGroupHeartbeatResponse(new Assignment());
        membershipManager.onHeartbeatSuccess(heartbeatResponse.data());
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.empty());
        membershipManager.onHeartbeatSuccess(heartbeatResponse.data());
        assertEquals(MemberState.STABLE, membershipManager.state());
        return membershipManager;
    }

    private void receiveAssignment(Map<Uuid, SortedSet<Integer>> topicIdPartitionList, ShareMembershipManager membershipManager) {
        ShareGroupHeartbeatResponseData.Assignment targetAssignment = new ShareGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(topicIdPartitionList.entrySet().stream().map(tp ->
                        new ShareGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(tp.getKey())
                                .setPartitions(new ArrayList<>(tp.getValue()))).collect(Collectors.toList()));
        ShareGroupHeartbeatResponse heartbeatResponse = createShareGroupHeartbeatResponse(targetAssignment);
        membershipManager.onHeartbeatSuccess(heartbeatResponse.data());
    }

    private void receiveAssignment(Uuid topicId, List<Integer> partitions, ShareMembershipManager membershipManager) {
        ShareGroupHeartbeatResponseData.Assignment targetAssignment = new ShareGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.singletonList(
                        new ShareGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topicId)
                                .setPartitions(partitions)));
        ShareGroupHeartbeatResponse heartbeatResponse = createShareGroupHeartbeatResponse(targetAssignment);
        membershipManager.onHeartbeatSuccess(heartbeatResponse.data());
    }

    private void receiveEmptyAssignment(ShareMembershipManager membershipManager) {
        // New empty assignment received, revoking owned partition.
        ShareGroupHeartbeatResponseData.Assignment targetAssignment = new ShareGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.emptyList());
        ShareGroupHeartbeatResponse heartbeatResponse = createShareGroupHeartbeatResponse(targetAssignment);
        membershipManager.onHeartbeatSuccess(heartbeatResponse.data());
    }

    /**
     * Fenced member should release assignment, reset epoch to 0, keep member ID, and transition
     * to JOINING to rejoin the group.
     */
    private void testFencedMemberReleasesAssignmentAndTransitionsToJoining(ShareMembershipManager membershipManager) {
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
    private void testLeaveGroupReleasesAssignmentAndResetsEpochToSendLeaveGroup(ShareMembershipManager membershipManager) {
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

    @Test
    public void testStaleMemberDoesNotSendHeartbeatAndAllowsTransitionToJoiningToRecover() {
        ShareMembershipManager membershipManager = createMemberInStableState();
        doNothing().when(subscriptionState).assignFromSubscribed(any());
        membershipManager.transitionToSendingLeaveGroup(true);
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.STALE, membershipManager.state());
        assertTrue(membershipManager.shouldSkipHeartbeat(), "Stale member should not send " +
                "heartbeats");
        // Check that a transition to joining is allowed, which is what is expected to happen
        // when the poll timer is reset.
        assertDoesNotThrow(membershipManager::maybeRejoinStaleMember);
    }

    @Test
    public void testStaleMemberRejoinsWhenTimerResetsNoCallbacks() {
        ShareMembershipManager membershipManager = mockStaleMember();
        assertStaleMemberLeavesGroupAndClearsAssignment(membershipManager);

        membershipManager.maybeRejoinStaleMember();
        assertEquals(MemberState.JOINING, membershipManager.state());
    }

    private ShareMembershipManager mockStaleMember() {
        ShareMembershipManager membershipManager = createMemberInStableState();
        doNothing().when(subscriptionState).assignFromSubscribed(any());
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        membershipManager.transitionToSendingLeaveGroup(true);
        membershipManager.onHeartbeatRequestSent();
        return membershipManager;
    }

    private void mockLeaveGroup() {
        mockMemberHasAutoAssignedPartition();
        doNothing().when(subscriptionState).markPendingRevocation(anySet());
    }

    private void mockPrepareLeaving(ShareMembershipManager membershipManager) {
        String topicName = "topic1";
        TopicPartition ownedPartition = new TopicPartition(topicName, 0);

        // Start leaving group, blocked waiting for callback to complete.
        when(subscriptionState.assignedPartitions()).thenReturn(Collections.singleton(ownedPartition));
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        doNothing().when(subscriptionState).markPendingRevocation(anySet());
        membershipManager.leaveGroup();
    }

    private void testStateUpdateOnFatalFailure(ShareMembershipManager membershipManager) {
        String memberId = membershipManager.memberId();
        int lastEpoch = membershipManager.memberEpoch();
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        membershipManager.transitionToFatal();
        assertEquals(MemberState.FATAL, membershipManager.state());
        // Should keep its last member id and epoch.
        assertEquals(memberId, membershipManager.memberId());
        assertEquals(lastEpoch, membershipManager.memberEpoch());
    }

    private ShareGroupHeartbeatResponse createShareGroupHeartbeatResponse(
            ShareGroupHeartbeatResponseData.Assignment assignment) {
        return new ShareGroupHeartbeatResponse(new ShareGroupHeartbeatResponseData()
                .setErrorCode(Errors.NONE.code())
                .setMemberId(MEMBER_ID)
                .setMemberEpoch(MEMBER_EPOCH)
                .setAssignment(assignment));
    }

    private ShareGroupHeartbeatResponse createShareGroupHeartbeatResponseWithError() {
        return new ShareGroupHeartbeatResponse(new ShareGroupHeartbeatResponseData()
                .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                .setMemberId(MEMBER_ID)
                .setMemberEpoch(5));
    }

    private ShareGroupHeartbeatResponseData.Assignment createAssignment(boolean mockMetadata) {
        Uuid topic1 = Uuid.randomUuid();
        Uuid topic2 = Uuid.randomUuid();
        if (mockMetadata) {
            Map<Uuid, String> topicNames = new HashMap<>();
            topicNames.put(topic1, "topic1");
            topicNames.put(topic2, "topic2");
            when(metadata.topicNames()).thenReturn(topicNames);
        }
        return new ShareGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Arrays.asList(
                        new ShareGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topic1)
                                .setPartitions(Arrays.asList(0, 1, 2)),
                        new ShareGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topic2)
                                .setPartitions(Arrays.asList(3, 4, 5))
                ));
    }

    private KafkaMetric getMetric(final String name) {
        return metrics.metrics().get(metrics.metricName(name, CONSUMER_SHARE_METRIC_GROUP_PREFIX + "-coordinator-metrics"));
    }
}
