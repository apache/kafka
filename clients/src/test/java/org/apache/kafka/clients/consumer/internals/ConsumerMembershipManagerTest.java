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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ConsumerRebalanceListenerCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.ConsumerRebalanceListenerCallbackNeededEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.clients.consumer.internals.metrics.ConsumerRebalanceMetricsManager;
import org.apache.kafka.clients.consumer.internals.metrics.RebalanceCallbackMetricsManager;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData.Assignment;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData.TopicPartitions;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.clients.consumer.internals.AbstractMembershipManager.TOPIC_PARTITION_COMPARATOR;
import static org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer.invokeRebalanceCallbacks;
import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSortedSet;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsumerMembershipManagerTest {

    private static final String GROUP_ID = "test-group";
    private static final int REBALANCE_TIMEOUT = 100;
    private static final int MEMBER_EPOCH = 1;
    private static final LogContext LOG_CONTEXT = new LogContext();

    private SubscriptionState subscriptionState;
    private ConsumerMetadata metadata;
    private CommitRequestManager commitRequestManager;
    private BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private BackgroundEventHandler backgroundEventHandler;
    private Time time;
    private Metrics metrics;
    private ConsumerRebalanceMetricsManager rebalanceMetricsManager;

    @BeforeEach
    public void setup() {
        metadata = mock(ConsumerMetadata.class);
        subscriptionState = mock(SubscriptionState.class);
        commitRequestManager = mock(CommitRequestManager.class);
        backgroundEventQueue = new LinkedBlockingQueue<>();
        time = new MockTime(0);
        backgroundEventHandler = new BackgroundEventHandler(backgroundEventQueue, time, mock(AsyncConsumerMetrics.class));
        metrics = new Metrics(time);
        rebalanceMetricsManager = new ConsumerRebalanceMetricsManager(metrics);

        when(commitRequestManager.maybeAutoCommitSyncBeforeRevocation(anyLong())).thenReturn(CompletableFuture.completedFuture(null));
    }

    private ConsumerMembershipManager createMembershipManagerJoiningGroup() {
        return createMembershipManagerJoiningGroup(null);
    }

    private ConsumerMembershipManager createMembershipManagerJoiningGroup(String groupInstanceId) {
        ConsumerMembershipManager manager = createMembershipManager(groupInstanceId);
        manager.transitionToJoining();
        return manager;
    }

    private ConsumerMembershipManager createMembershipManager(String groupInstanceId) {
        ConsumerMembershipManager manager = spy(new ConsumerMembershipManager(
            GROUP_ID, Optional.ofNullable(groupInstanceId), REBALANCE_TIMEOUT, Optional.empty(),
            subscriptionState, commitRequestManager, metadata, LOG_CONTEXT,
            backgroundEventHandler, time, rebalanceMetricsManager));
        assertMemberIdIsGenerated(manager.memberId());
        return manager;
    }

    private ConsumerMembershipManager createMembershipManagerJoiningGroup(String groupInstanceId,
                                                                      String serverAssignor) {
        ConsumerMembershipManager manager = spy(new ConsumerMembershipManager(
                GROUP_ID, Optional.ofNullable(groupInstanceId), REBALANCE_TIMEOUT,
                Optional.ofNullable(serverAssignor), subscriptionState, commitRequestManager,
                metadata, LOG_CONTEXT, backgroundEventHandler, time, rebalanceMetricsManager));
        assertMemberIdIsGenerated(manager.memberId());
        manager.transitionToJoining();
        return manager;
    }

    @Test
    public void testMembershipManagerServerAssignor() {
        ConsumerMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        assertEquals(Optional.empty(), membershipManager.serverAssignor());

        membershipManager = createMembershipManagerJoiningGroup("instance1", "Uniform");
        assertEquals(Optional.of("Uniform"), membershipManager.serverAssignor());
    }

    @Test
    public void testMembershipManagerInitSupportsEmptyGroupInstanceId() {
        createMembershipManagerJoiningGroup();
    }

    @Test
    public void testReconcilingWhenReceivingAssignmentFoundInMetadata() {
        ConsumerMembershipManager membershipManager = mockJoinAndReceiveAssignment(true);
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());

        // When the ack is sent the member should go back to STABLE
        membershipManager.onHeartbeatRequestGenerated();
        assertEquals(MemberState.STABLE, membershipManager.state());
    }

    @Test
    public void testTransitionToReconcilingIfEmptyAssignmentReceived() {
        ConsumerMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        assertEquals(MemberState.JOINING, membershipManager.state());

        ConsumerGroupHeartbeatResponse responseWithoutAssignment =
                createConsumerGroupHeartbeatResponse(new Assignment(), membershipManager.memberId());
        membershipManager.onHeartbeatSuccess(responseWithoutAssignment);
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        ConsumerGroupHeartbeatResponse responseWithAssignment =
                createConsumerGroupHeartbeatResponse(createAssignment(true), membershipManager.memberId());
        membershipManager.onHeartbeatSuccess(responseWithAssignment);
        assertEquals(MemberState.RECONCILING, membershipManager.state());
    }

    @Test
    public void testMemberIdAndEpochResetOnFencedMembers() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        String originalMemberId = membershipManager.memberId();
        assertNotNull(originalMemberId);
        assertFalse(originalMemberId.isEmpty());
        assertEquals(MEMBER_EPOCH, membershipManager.memberEpoch());

        mockMemberHasAutoAssignedPartition();

        membershipManager.transitionToFenced();
        assertEquals(0, membershipManager.memberEpoch());
    }

    @Test
    public void testTransitionToFatal() {
        ConsumerMembershipManager membershipManager = createMemberInStableState(null);
        assertEquals(MEMBER_EPOCH, membershipManager.memberEpoch());

        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        membershipManager.transitionToFatal();
        assertEquals(MemberState.FATAL, membershipManager.state());
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());

        membershipManager.leaveGroup();
        verify(subscriptionState).unsubscribe();
    }

    @Test
    public void testTransitionToFailedWhenTryingToJoin() {
        ConsumerMembershipManager membershipManager = new ConsumerMembershipManager(
                GROUP_ID, Optional.empty(), REBALANCE_TIMEOUT, Optional.empty(),
                subscriptionState, commitRequestManager, metadata, LOG_CONTEXT,
            backgroundEventHandler, time, rebalanceMetricsManager);
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        membershipManager.transitionToJoining();

        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        membershipManager.transitionToFatal();
        assertEquals(MemberState.FATAL, membershipManager.state());
    }

    @Test
    public void testFencingWhenStateIsStable() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        testFencedMemberReleasesAssignmentAndTransitionsToJoining(membershipManager);
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
    }

    @Test
    public void testListenersGetNotifiedOnTransitionsToFatal() {
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.empty());
        ConsumerMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        MemberStateListener listener = mock(MemberStateListener.class);
        membershipManager.registerStateListener(listener);
        mockStableMember(membershipManager);
        verify(listener).onMemberEpochUpdated(Optional.of(MEMBER_EPOCH), membershipManager.memberId);
        clearInvocations(listener);

        // Transition to FAILED before getting member ID/epoch
        membershipManager.transitionToFatal();
        assertEquals(MemberState.FATAL, membershipManager.state());
        verify(listener).onMemberEpochUpdated(Optional.empty(), membershipManager.memberId);
    }

    @Test
    public void testListenersGetNotifiedOnTransitionsToLeavingGroup() {
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.empty());
        ConsumerMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        MemberStateListener listener = mock(MemberStateListener.class);
        membershipManager.registerStateListener(listener);
        mockStableMember(membershipManager);
        verify(listener).onMemberEpochUpdated(Optional.of(MEMBER_EPOCH), membershipManager.memberId);
        clearInvocations(listener);

        mockLeaveGroup();
        membershipManager.leaveGroup();
        verify(subscriptionState).unsubscribe();
        assertEquals(MemberState.LEAVING, membershipManager.state());
        verify(listener).onMemberEpochUpdated(Optional.empty(), membershipManager.memberId);
    }

    @Test
    public void testListenersGetNotifiedOfMemberEpochUpdatesOnlyIfItChanges() {
        ConsumerMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        String memberId = membershipManager.memberId();
        MemberStateListener listener = mock(MemberStateListener.class);
        membershipManager.registerStateListener(listener);
        int epoch = 5;

        membershipManager.onHeartbeatSuccess(createConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.NONE.code())
                .setMemberId(memberId)
                .setMemberEpoch(epoch)));

        verify(listener).onMemberEpochUpdated(Optional.of(epoch), memberId);
        clearInvocations(listener);

        membershipManager.onHeartbeatSuccess(createConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.NONE.code())
                .setMemberId(memberId)
                .setMemberEpoch(epoch)));
        verify(listener, never()).onMemberEpochUpdated(any(), any());
    }

    private void mockStableMember(ConsumerMembershipManager membershipManager) {
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(new Assignment(),
            membershipManager.memberId());
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        membershipManager.onHeartbeatSuccess(heartbeatResponse);
        membershipManager.poll(time.milliseconds());
        membershipManager.onHeartbeatRequestGenerated();
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertEquals(MEMBER_EPOCH, membershipManager.memberEpoch());
    }

    @Test
    public void testFencingWhenStateIsReconciling() {
        ConsumerMembershipManager membershipManager = mockJoinAndReceiveAssignment(false);
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        testFencedMemberReleasesAssignmentAndTransitionsToJoining(membershipManager);
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
    }

    /**
     * Members keep sending heartbeats while preparing to leave, so they could get fenced. This
     * tests ensures that if a member gets fenced while preparing to leave, it will directly
     * transition to UNSUBSCRIBE (no callbacks triggered or rejoin as in regular fencing
     * scenarios), and when the PREPARE_LEAVING completes it remains UNSUBSCRIBED (no last
     * heartbeat sent).
     */
    @Test
    public void testFencingWhenStateIsPrepareLeaving() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        ConsumerRebalanceListenerInvoker invoker = consumerRebalanceListenerInvoker();
        ConsumerRebalanceListenerCallbackCompletedEvent callbackEvent =
                mockPrepareLeavingStuckOnUserCallback(membershipManager, invoker);
        assertEquals(MemberState.PREPARE_LEAVING, membershipManager.state());

        // Get fenced while preparing to leave the group. Member should ignore the fence
        // (no callbacks or rejoin) and should transition to UNSUBSCRIBED to
        // effectively stop sending heartbeats.
        clearInvocations(subscriptionState);
        membershipManager.transitionToFenced();
        testFenceIsNoOp(membershipManager);
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());

        // When PREPARE_LEAVING completes, the member should remain UNSUBSCRIBED (no last HB sent
        // because member is already out of the group in the broker).
        completeCallback(callbackEvent, membershipManager);
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        assertEquals(ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH, membershipManager.memberEpoch());
        verify(membershipManager).notifyEpochChange(Optional.empty());
        assertTrue(membershipManager.shouldSkipHeartbeat());
    }

    @Test
    public void testFencingWhenStateIsPrepareLeavingCompletesTheLeaveOperation() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        mockPrepareLeaving(new TopicPartition("topic1", 0));
        CompletableFuture<Void> leaveOperation = membershipManager.leaveGroup();
        assertEquals(MemberState.PREPARE_LEAVING, membershipManager.state());
        assertFalse(leaveOperation.isDone());
        clearInvocations(subscriptionState);

        membershipManager.transitionToFenced();

        testFenceIsNoOp(membershipManager);
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        assertTrue(leaveOperation.isDone(), "Fenced member should complete the ongoing leave operation");
    }

    @Test
    public void testNewAssignmentIgnoredWhenStateIsPrepareLeaving() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        ConsumerRebalanceListenerInvoker invoker = consumerRebalanceListenerInvoker();
        ConsumerRebalanceListenerCallbackCompletedEvent callbackEvent =
            mockPrepareLeavingStuckOnUserCallback(membershipManager, invoker);
        assertEquals(MemberState.PREPARE_LEAVING, membershipManager.state());

        // Get new assignment while preparing to leave the group. Member should continue leaving
        // the group, ignoring the new assignment received.
        Uuid topicId = Uuid.randomUuid();
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId, "topic1",
            Collections.emptyList());
        receiveAssignment(topicId, Arrays.asList(0, 1), membershipManager);
        assertEquals(MemberState.PREPARE_LEAVING, membershipManager.state());
        assertTrue(membershipManager.topicsAwaitingReconciliation().isEmpty());
        verify(membershipManager, never()).markReconciliationInProgress();

        // When callback completes member should transition to LEAVING.
        completeCallback(callbackEvent, membershipManager);
        assertEquals(MemberState.LEAVING, membershipManager.state());
    }

    @Test
    public void testFencingWhenStateIsLeaving() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();

        // Start leaving group.
        mockLeaveGroup();
        CompletableFuture<Void> leaveOperation = membershipManager.leaveGroup();
        verify(subscriptionState).unsubscribe();
        assertEquals(MemberState.LEAVING, membershipManager.state());

        // Get fenced while leaving. Member should not trigger any callback or try to
        // rejoin, and should continue leaving the group as it was before getting fenced.
        clearInvocations(subscriptionState);
        membershipManager.transitionToFenced();
        testFenceIsNoOp(membershipManager);
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state(), "Member should " +
            "transition from LEAVING to UNSUBSCRIBED when getting fenced (it does not need to " +
            "send leave request if fenced");
        assertTrue(leaveOperation.isDone(), "Fenced member should complete the ongoing leave operation");
    }

    private void assertTransitionToUnsubscribeOnHBSentAndWaitForResponseToCompleteLeave(ConsumerMembershipManager membershipManager, CompletableFuture<Void> sendLeave) {
        assertEquals(MemberState.LEAVING, membershipManager.state());
        membershipManager.onHeartbeatRequestGenerated();
        assertFalse(sendLeave.isDone(), "Send leave operation should not complete until a response is received");
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());

        membershipManager.onHeartbeatSuccess(createConsumerGroupHeartbeatResponse(new Assignment(), membershipManager.memberId()));

        assertSendLeaveCompleted(membershipManager, sendLeave);
    }

    @Test
    public void testLeaveGroupEpoch() {
        // Static member should leave the group with epoch -2.
        ConsumerMembershipManager membershipManager = createMemberInStableState("instance1");
        mockLeaveGroup();
        membershipManager.leaveGroup();
        verify(subscriptionState).unsubscribe();
        assertEquals(MemberState.LEAVING, membershipManager.state());
        assertEquals(ConsumerGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH,
                membershipManager.memberEpoch());

        // Dynamic member should leave the group with epoch -1.
        membershipManager = createMemberInStableState(null);
        mockLeaveGroup();
        membershipManager.leaveGroup();
        verify(subscriptionState).unsubscribe();
        assertEquals(MemberState.LEAVING, membershipManager.state());
        assertEquals(ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH,
                membershipManager.memberEpoch());
    }

    /**
     * This is the case where a member is stuck reconciling and transitions out of the RECONCILING
     * state (due to failure). When the reconciliation completes it should not be applied because
     * it is not relevant anymore (it should not update the assignment on the member or send ack).
     */
    @Test
    public void testDelayedReconciliationResultDiscardedIfMemberNotInReconcilingStateAnymore() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        Uuid topicId1 = Uuid.randomUuid();
        String topic1 = "topic1";
        List<TopicIdPartition> owned = Collections.singletonList(
            new TopicIdPartition(topicId1, new TopicPartition(topic1, 0)));
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId1, topic1, owned);

        // Reconciliation that does not complete stuck on revocation commit.
        CompletableFuture<Void> commitResult = mockEmptyAssignmentAndRevocationStuckOnCommit(membershipManager);

        // Member received fatal error while reconciling.
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        membershipManager.transitionToFatal();
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
        clearInvocations(subscriptionState);

        // Complete commit request.
        commitResult.complete(null);

        // Member should not update the subscription or send ack when the delayed reconciliation
        // completes.
        verify(subscriptionState, never()).assignFromSubscribed(anySet());
        assertNotEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
    }

    /**
     * This is the case where a member is stuck reconciling an assignment A (waiting for commit
     * to complete), and it rejoins (due to fence or unsubscribe/subscribe). If the
     * reconciliation of A completes it should be interrupted, and it should not update the
     * assignment on the member or send ack.
     */
    @Test
    public void testDelayedReconciliationResultDiscardedAfterCommitIfMemberRejoins() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        Uuid topicId1 = Uuid.randomUuid();
        String topic1 = "topic1";
        List<TopicIdPartition> owned = Collections.singletonList(new TopicIdPartition(topicId1,
            new TopicPartition(topic1, 0)));
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId1, topic1, owned);

        // Reconciliation that does not complete stuck on revocation commit.
        CompletableFuture<Void> commitResult =
                mockNewAssignmentAndRevocationStuckOnCommit(membershipManager, topicId1, topic1,
                        Arrays.asList(1, 2), true);
        Map<Uuid, SortedSet<Integer>> assignment1 = topicIdPartitionsMap(topicId1,  1, 2);
        assertEquals(assignment1, membershipManager.topicPartitionsAwaitingReconciliation());

        // Get fenced and rejoin while still reconciling. Get new assignment to reconcile after
        // rejoining.
        testFencedMemberReleasesAssignmentAndTransitionsToJoining(membershipManager);
        clearInvocations(subscriptionState);

        Map<Uuid, SortedSet<Integer>> assignmentAfterRejoin = receiveAssignmentAfterRejoin(
            Collections.singletonList(5), membershipManager, owned);

        // Reconciliation completes when the member has already re-joined the group. Should not
        // proceed with the revocation, update the subscription state or send ack.
        commitResult.complete(null);
        assertInitialReconciliationDiscardedAfterRejoin(membershipManager, assignmentAfterRejoin);
    }

    /**
     * This is the case where a member is stuck reconciling an assignment A (waiting for
     * onPartitionsRevoked callback to complete), and it rejoins (due to fence or
     * unsubscribe/subscribe). When the reconciliation of A completes it should be interrupted,
     * and it should not update the assignment on the member or send ack.
     */
    @Test
    public void testDelayedReconciliationResultDiscardedAfterPartitionsRevokedCallbackIfMemberRejoins() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        Uuid topicId1 = Uuid.randomUuid();
        String topic1 = "topic1";
        List<TopicIdPartition> owned = Collections.singletonList(new TopicIdPartition(topicId1,
            new TopicPartition(topic1, 0)));
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId1, topic1, owned);

        // Reconciliation that does not complete stuck on onPartitionsRevoked callback
        ConsumerRebalanceListenerInvoker invoker = consumerRebalanceListenerInvoker();
        ConsumerRebalanceListenerCallbackCompletedEvent callbackCompletedEvent =
            mockNewAssignmentStuckOnPartitionsRevokedCallback(membershipManager, topicId1, topic1,
            Arrays.asList(1, 2), owned.get(0).topicPartition(), invoker);
        Map<Uuid, SortedSet<Integer>> assignment1 = topicIdPartitionsMap(topicId1,  1, 2);
        assertEquals(assignment1, membershipManager.topicPartitionsAwaitingReconciliation());

        // Get fenced and rejoin while still reconciling. Get new assignment to reconcile after rejoining.
        testFencedMemberReleasesAssignmentAndTransitionsToJoining(membershipManager);
        clearInvocations(subscriptionState);

        Map<Uuid, SortedSet<Integer>> assignmentAfterRejoin = receiveAssignmentAfterRejoin(
            Collections.singletonList(5), membershipManager, owned);

        // onPartitionsRevoked callback completes when the member has already re-joined the group.
        // Should not proceed with the assignment, update the subscription state or send ack.
        completeCallback(callbackCompletedEvent, membershipManager);
        assertInitialReconciliationDiscardedAfterRejoin(membershipManager, assignmentAfterRejoin);
    }

    /**
     * This is the case where a member is stuck reconciling an assignment A (waiting for
     * onPartitionsAssigned callback to complete), and it rejoins (due to fence or
     * unsubscribe/subscribe). If the reconciliation of A completes it should be interrupted, and it
     * should not update the assignment on the member or send ack.
     */
    @Test
    public void testDelayedReconciliationResultDiscardedAfterPartitionsAssignedCallbackIfMemberRejoins() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        Uuid topicId1 = Uuid.randomUuid();
        String topic1 = "topic1";

        // Reconciliation that does not complete stuck on onPartitionsAssigned callback
        ConsumerRebalanceListenerInvoker invoker = consumerRebalanceListenerInvoker();
        int newPartition = 1;
        ConsumerRebalanceListenerCallbackCompletedEvent callbackCompletedEvent =
            mockNewAssignmentStuckOnPartitionsAssignedCallback(membershipManager, topicId1,
                topic1, newPartition, invoker);
        Map<Uuid, SortedSet<Integer>> assignment1 = topicIdPartitionsMap(topicId1,  newPartition);
        assertEquals(assignment1, membershipManager.topicPartitionsAwaitingReconciliation());

        // Get fenced and rejoin while still reconciling. Get new assignment to reconcile after rejoining.
        testFencedMemberReleasesAssignmentAndTransitionsToJoining(membershipManager);
        clearInvocations(subscriptionState);

        Map<Uuid, SortedSet<Integer>> assignmentAfterRejoin = receiveAssignmentAfterRejoin(
            Collections.singletonList(5), membershipManager, Collections.emptyList());

        // onPartitionsAssigned callback completes when the member has already re-joined the group.
        // Should not update the subscription state or send ack.
        completeCallback(callbackCompletedEvent, membershipManager);
        assertInitialReconciliationDiscardedAfterRejoin(membershipManager, assignmentAfterRejoin);
    }

    /**
     * This is the case where a member rejoins (due to fence). If
     * the member gets the same assignment again, it should still reconcile and ack the assignment.
     */
    @Test
    public void testSameAssignmentReconciledAgainWhenFenced() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        Uuid topic1 = Uuid.randomUuid();
        final Assignment assignment1 = new ConsumerGroupHeartbeatResponseData.Assignment();
        final Assignment assignment2 = new ConsumerGroupHeartbeatResponseData.Assignment()
            .setTopicPartitions(Collections.singletonList(
                new TopicPartitions()
                    .setTopicId(topic1)
                    .setPartitions(Arrays.asList(0, 1, 2))
            ));
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topic1, "topic1"));
        assertEquals(toTopicIdPartitionMap(assignment1), membershipManager.currentAssignment().partitions);

        // Receive assignment, wait on commit
        membershipManager.onHeartbeatSuccess(createConsumerGroupHeartbeatResponse(assignment2, membershipManager.memberId()));
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        CompletableFuture<Void> commitResult = new CompletableFuture<>();
        when(commitRequestManager.maybeAutoCommitSyncBeforeRevocation(anyLong())).thenReturn(commitResult);
        membershipManager.poll(time.milliseconds());

        // Get fenced, commit completes
        membershipManager.transitionToFenced();

        assertEquals(MemberState.JOINING, membershipManager.state());
        assertTrue(membershipManager.currentAssignment().isNone());
        assertTrue(subscriptionState.assignedPartitions().isEmpty());

        commitResult.complete(null);

        assertEquals(MemberState.JOINING, membershipManager.state());
        assertTrue(membershipManager.currentAssignment().isNone());
        assertTrue(subscriptionState.assignedPartitions().isEmpty());

        // We have to reconcile & ack the assignment again
        membershipManager.onHeartbeatSuccess(createConsumerGroupHeartbeatResponse(assignment1, membershipManager.memberId()));
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        membershipManager.poll(time.milliseconds());
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        membershipManager.onHeartbeatRequestGenerated();
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertEquals(toTopicIdPartitionMap(assignment1), membershipManager.currentAssignment().partitions);
    }

    /**
     * This is the case where we receive a new assignment while reconciling an existing one. The intermediate assignment
     * is not applied, and a new assignment containing the same partitions is received and reconciled. In all assignments,
     * one topic is not resolvable.
     *
     * We need to make sure that the last assignment is acked and applied, even though the set of partitions does not change.
     * In this case, no rebalance listeners are run.
     */
    @Test
    public void testSameAssignmentReconciledAgainWithMissingTopic() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        Uuid topic1 = Uuid.randomUuid();
        Uuid topic2 = Uuid.randomUuid();
        final Assignment assignment1 = new ConsumerGroupHeartbeatResponseData.Assignment()
            .setTopicPartitions(Arrays.asList(
                new TopicPartitions().setTopicId(topic1).setPartitions(Collections.singletonList(0)),
                new TopicPartitions().setTopicId(topic2).setPartitions(Collections.singletonList(0))
            ));
        final Assignment assignment2 = new ConsumerGroupHeartbeatResponseData.Assignment()
            .setTopicPartitions(Arrays.asList(
                new TopicPartitions().setTopicId(topic1).setPartitions(Arrays.asList(0, 1)),
                new TopicPartitions().setTopicId(topic2).setPartitions(Collections.singletonList(0))
            ));
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topic1, "topic1"));

        // Receive assignment - full reconciliation triggered
        // stay in RECONCILING state, since an unresolved topic is assigned
        membershipManager.onHeartbeatSuccess(createConsumerGroupHeartbeatResponse(assignment1, membershipManager.memberId()));
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        membershipManager.poll(time.milliseconds());
        verifyReconciliationTriggeredAndCompleted(membershipManager,
            Collections.singletonList(new TopicIdPartition(topic1, new TopicPartition("topic1", 0)))
        );
        membershipManager.onHeartbeatRequestGenerated();
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        clearInvocations(membershipManager);

        // Receive extended assignment - assignment received but no reconciliation triggered
        membershipManager.onHeartbeatSuccess(createConsumerGroupHeartbeatResponse(assignment2, membershipManager.memberId()));
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        verifyReconciliationNotTriggered(membershipManager);

        // Receive original assignment again - full reconciliation not triggered but assignment is acked again
        membershipManager.onHeartbeatSuccess(createConsumerGroupHeartbeatResponse(assignment1, membershipManager.memberId()));
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        membershipManager.poll(time.milliseconds());
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        verifyReconciliationNotTriggered(membershipManager);
        assertEquals(Collections.singletonMap(topic1, mkSortedSet(0)), membershipManager.currentAssignment().partitions);
        assertEquals(Set.of(topic2), membershipManager.topicsAwaitingReconciliation());
    }

    private Map<Uuid, SortedSet<Integer>> toTopicIdPartitionMap(final Assignment assignment) {
        Map<Uuid, SortedSet<Integer>> result = new HashMap<>();
        for (TopicPartitions topicPartitions : assignment.topicPartitions()) {
            result.put(topicPartitions.topicId(), new TreeSet<>(topicPartitions.partitions()));
        }
        return result;
    }

    /**
     * This is the case where a member is stuck reconciling an assignment A (waiting on metadata, commit or callbacks), and the target
     * assignment changes due to new topics. If the reconciliation of A completes it should be applied (should update the assignment
     * on the member and send ack), and then the reconciliation of assignment B will be processed and applied in the next reconciliation
     * loop.
     */
    @Test
    public void testDelayedReconciliationResultAppliedWhenTargetChangedWithMetadataUpdate() {
        // Member receives and reconciles topic1-partition0
        Uuid topicId1 = Uuid.randomUuid();
        String topic1 = "topic1";
        ConsumerMembershipManager membershipManager =
                mockMemberSuccessfullyReceivesAndAcksAssignment(topicId1, topic1, Collections.singletonList(0));
        membershipManager.onHeartbeatRequestGenerated();
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
        assertEquals(Collections.singleton(topicId2), membershipManager.topicsAwaitingReconciliation());

        // Metadata discovered for topic2 while reconciliation in progress to revoke topic1.
        // Should not trigger a new reconciliation because there is one already in progress.
        final Map<Uuid, String> topic2Metadata = Collections.singletonMap(topicId2, topic2);
        mockTopicNameInMetadataCache(topic2Metadata, true);
        assertEquals(Collections.singleton(topicId2), membershipManager.topicsAwaitingReconciliation());
        verifyReconciliationNotTriggered(membershipManager);

        // Reconciliation in progress completes. Should be applied revoking topic 1 only. Newly
        // discovered topic2 will be reconciled in the next reconciliation loop.
        commitResult.complete(null);

        // Member should update the subscription and send ack when the delayed reconciliation
        // completes.
        verify(subscriptionState).assignFromSubscribedAwaitingCallback(Collections.emptySet(), Collections.emptySet());
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());

        // Pending assignment that was discovered in metadata should be ready to reconcile in the
        // next reconciliation loop.
        Map<Uuid, SortedSet<Integer>> topic2Assignment = topicIdPartitionsMap(topicId2,  1, 2);
        assertEquals(topic2Assignment, membershipManager.topicPartitionsAwaitingReconciliation());

        // After acknowledging the assignment, we should be back to RECONCILING, because we have not
        // yet reached the target assignment.
        membershipManager.onHeartbeatRequestGenerated();

        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertEquals(topic2Assignment, membershipManager.topicPartitionsAwaitingReconciliation());

        // Next reconciliation triggered in poll
        membershipManager.poll(time.milliseconds());

        assertEquals(Collections.emptySet(), membershipManager.topicsAwaitingReconciliation());
        verify(subscriptionState).assignFromSubscribedAwaitingCallback(topicPartitions(topic2Assignment, topic2Metadata), topicPartitions(topic2Assignment, topic2Metadata));
    }

    /**
     * This is the case where a member is stuck reconciling an assignment A (waiting on metadata, commit or callbacks), and the target
     * assignment changes due to a new assignment received from broker. If the reconciliation of A completes it should be applied (should
     * update the assignment on the member and send ack), and then the reconciliation of assignment B will be processed and applied in the
     * next reconciliation loop.
     */
    @Test
    public void testDelayedReconciliationResultAppliedWhenTargetChangedWithNewAssignment() {

        Uuid topicId1 = Uuid.randomUuid();
        String topic1 = "topic1";
        final TopicIdPartition topicId1Partition0 = new TopicIdPartition(topicId1, new TopicPartition(topic1, 0));

        Uuid topicId2 = Uuid.randomUuid();
        String topic2 = "topic2";
        final TopicIdPartition topicId2Partition0 = new TopicIdPartition(topicId2, new TopicPartition(topic2, 0));

        ConsumerMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        when(metadata.topicNames()).thenReturn(
            mkMap(
                mkEntry(topicId1, topic1),
                mkEntry(topicId2, topic2)
            )
        );

        // Receive assignment with only topic1-0, getting stuck during commit.
        final CompletableFuture<Void> commitFuture = mockNewAssignmentAndRevocationStuckOnCommit(membershipManager, topicId1,
            topic1, Collections.singletonList(0), false);

        // New assignment adding a new topic2-0 (not in metadata).
        // No reconciliation triggered, because another reconciliation is in progress.
        Map<Uuid, SortedSet<Integer>> newAssignment =
            mkMap(
                mkEntry(topicId1, mkSortedSet(0)),
                mkEntry(topicId2, mkSortedSet(0))
            );

        receiveAssignment(newAssignment, membershipManager);
        membershipManager.poll(time.milliseconds());

        verifyReconciliationNotTriggered(membershipManager);
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertEquals(Set.of(topicId1, topicId2), membershipManager.topicsAwaitingReconciliation());
        clearInvocations(membershipManager, commitRequestManager);

        // First reconciliation completes. Should trigger follow-up reconciliation to complete the assignment,
        // with membership manager entering ACKNOWLEDGING state.

        commitFuture.complete(null);

        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        assertEquals(Set.of(topicId2), membershipManager.topicsAwaitingReconciliation());

        // After acknowledging the assignment, we should be back to RECONCILING, because we have not
        // yet reached the target assignment.
        membershipManager.onHeartbeatRequestGenerated();

        assertEquals(MemberState.RECONCILING, membershipManager.state());
        clearInvocations(membershipManager, commitRequestManager);

        // Next poll should trigger final reconciliation
        membershipManager.poll(time.milliseconds());

        verifyReconciliationTriggeredAndCompleted(membershipManager, Arrays.asList(topicId1Partition0, topicId2Partition0));
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
        ConsumerMembershipManager membershipManager =
            mockMemberSuccessfullyReceivesAndAcksAssignment(topicId1, topic1, Collections.singletonList(0));

        membershipManager.onHeartbeatRequestGenerated();

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

        // No full reconciliation triggered, but assignment needs to be acknowledged.
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        assertTrue(membershipManager.shouldHeartbeatNow());

        membershipManager.onHeartbeatRequestGenerated();

        verifyReconciliationNotTriggered(membershipManager);
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertEquals(Collections.singleton(topicId2), membershipManager.topicsAwaitingReconciliation());
        verify(metadata).requestUpdate(anyBoolean());
        clearInvocations(membershipManager, commitRequestManager);

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
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        testLeaveGroupReleasesAssignmentAndResetsEpochToSendLeaveGroup(membershipManager);
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
    }

    @Test
    public void testHeartbeatSuccessfulResponseWhenLeavingGroupCompletesLeave() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        mockLeaveGroup();

        CompletableFuture<Void> leaveResult = membershipManager.leaveGroup();
        verify(subscriptionState).unsubscribe();
        assertEquals(MemberState.LEAVING, membershipManager.state());
        assertFalse(leaveResult.isDone());

        membershipManager.onHeartbeatRequestGenerated();
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        assertFalse(leaveResult.isDone());

        membershipManager.onHeartbeatSuccess(createConsumerGroupHeartbeatResponse(createAssignment(true), membershipManager.memberId()));
        assertSendLeaveCompleted(membershipManager, leaveResult);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testHeartbeatFailedResponseWhenLeavingGroupCompletesLeave(boolean retriableResponseError) {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        mockLeaveGroup();

        CompletableFuture<Void> leaveResult = membershipManager.leaveGroup();
        assertEquals(MemberState.LEAVING, membershipManager.state());
        assertFalse(leaveResult.isDone());

        membershipManager.onHeartbeatRequestGenerated();
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        assertFalse(leaveResult.isDone());

        membershipManager.onHeartbeatFailure(retriableResponseError);
        assertSendLeaveCompleted(membershipManager, leaveResult);
    }

    private void assertSendLeaveCompleted(ConsumerMembershipManager membershipManager,
                                          CompletableFuture<Void> sendLeave) {
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state(), "Member should " +
            "remain UNSUBSCRIBED after receiving the response to the HB to leave");
        assertEquals(-1, membershipManager.memberEpoch());
        assertTrue(membershipManager.currentAssignment().isNone());
        assertTrue(sendLeave.isDone(), "Leave group result should complete when the response to" +
            " the heartbeat request to leave is received.");
        assertFalse(sendLeave.isCompletedExceptionally());
    }

    @ParameterizedTest
    @MethodSource("notInGroupStates")
    public void testIgnoreHeartbeatResponseWhenNotInGroup(MemberState state) {
        ConsumerMembershipManager membershipManager = createMembershipManager(null);
        when(membershipManager.state()).thenReturn(state);
        ConsumerGroupHeartbeatResponseData responseData = mock(ConsumerGroupHeartbeatResponseData.class);

        membershipManager.onHeartbeatSuccess(createConsumerGroupHeartbeatResponse(responseData));

        assertEquals(state, membershipManager.state());
        verify(responseData, never()).memberId();
        verify(responseData, never()).memberEpoch();
        verify(responseData, never()).assignment();
    }

    @Test
    public void testLeaveGroupWhenStateIsReconciling() {
        ConsumerMembershipManager membershipManager = mockJoinAndReceiveAssignment(false);
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        testLeaveGroupReleasesAssignmentAndResetsEpochToSendLeaveGroup(membershipManager);
    }

    @Test
    public void testLeaveGroupWhenMemberOwnsAssignment() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";
        ConsumerMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId, topicName, Collections.emptyList());

        receiveAssignment(topicId, Arrays.asList(0, 1), membershipManager);

        verifyReconciliationNotTriggered(membershipManager);
        membershipManager.poll(time.milliseconds());

        List<TopicIdPartition> assignedPartitions = Arrays.asList(
            new TopicIdPartition(topicId, new TopicPartition(topicName, 0)),
            new TopicIdPartition(topicId, new TopicPartition(topicName, 1)));
        verifyReconciliationTriggeredAndCompleted(membershipManager, assignedPartitions);

        assertEquals(1, membershipManager.currentAssignment().partitions.size());

        testLeaveGroupReleasesAssignmentAndResetsEpochToSendLeaveGroup(membershipManager);
    }

    @Test
    public void testFencedWhenAssignmentEmpty() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();

        // Clear the assignment
        when(subscriptionState.assignedPartitions()).thenReturn(Collections.emptySet());
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(false);

        membershipManager.transitionToFenced();

        // Make sure to never call `assignFromSubscribed` again
        verify(subscriptionState, never()).assignFromSubscribed(Collections.emptySet());
    }

    @Test
    public void testLeaveGroupWhenMemberAlreadyLeaving() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();

        // First leave attempt. Should trigger the callbacks and stay LEAVING until
        // callbacks complete and the heartbeat is sent out.
        mockLeaveGroup();
        CompletableFuture<Void> leaveResult1 = membershipManager.leaveGroup();
        verify(subscriptionState).unsubscribe();
        assertFalse(leaveResult1.isDone());
        assertEquals(MemberState.LEAVING, membershipManager.state());
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
        clearInvocations(subscriptionState);

        // Second leave attempt while the first one has not completed yet. Should not
        // trigger any callbacks, and return a future that will complete when the ongoing first
        // leave operation completes.
        mockLeaveGroup();
        CompletableFuture<Void> leaveResult2 = membershipManager.leaveGroup();
        verify(subscriptionState, never()).unsubscribe();
        verify(subscriptionState, never()).rebalanceListener();
        assertFalse(leaveResult2.isDone());

        // Complete first leave group operation. Should also complete the second leave group.
        assertTransitionToUnsubscribeOnHBSentAndWaitForResponseToCompleteLeave(membershipManager, leaveResult1);
        assertTrue(leaveResult2.isDone());
        assertFalse(leaveResult2.isCompletedExceptionally());

        // Subscription should have been updated only once with the first leave group.
        verify(subscriptionState, never()).assignFromSubscribed(Collections.emptySet());
    }

    @Test
    public void testLeaveGroupWhenMemberAlreadyLeft() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();

        // Leave group triggered and completed
        mockLeaveGroup();
        CompletableFuture<Void> leaveResult1 = membershipManager.leaveGroup();
        verify(subscriptionState).unsubscribe();
        assertEquals(MemberState.LEAVING, membershipManager.state());
        assertTransitionToUnsubscribeOnHBSentAndWaitForResponseToCompleteLeave(membershipManager, leaveResult1);
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
        clearInvocations(subscriptionState);

        // Call to leave group again, when member already left. Should be no-op (no callbacks,
        // no assignment updated)
        mockLeaveGroup();
        CompletableFuture<Void> leaveResult2 = membershipManager.leaveGroup();
        verify(subscriptionState).unsubscribe();
        assertTrue(leaveResult2.isDone());
        assertFalse(leaveResult2.isCompletedExceptionally());
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        verify(subscriptionState, never()).rebalanceListener();
        verify(subscriptionState, never()).assignFromSubscribed(Collections.emptySet());
    }

    @Test
    public void testLeaveGroupWhenMemberFenced() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        ConsumerRebalanceListenerInvoker invoker = consumerRebalanceListenerInvoker();
        ConsumerRebalanceListenerCallbackCompletedEvent callbackEvent = mockFencedMemberStuckOnUserCallback(membershipManager, invoker);
        assertEquals(MemberState.FENCED, membershipManager.state());

        mockLeaveGroup();
        CompletableFuture<Void> leaveOperation = membershipManager.leaveGroup();
        verify(subscriptionState).unsubscribe();
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        assertTrue(leaveOperation.isDone());
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());

        completeCallback(callbackEvent, membershipManager);
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
    }

    @Test
    public void testLeaveGroupWhenMemberIsStale() {
        ConsumerMembershipManager membershipManager = mockStaleMember();
        assertEquals(MemberState.STALE, membershipManager.state());

        mockLeaveGroup();
        CompletableFuture<Void> leaveResult1 = membershipManager.leaveGroup();
        verify(subscriptionState).unsubscribe();
        assertTrue(leaveResult1.isDone());
        assertEquals(MemberState.STALE, membershipManager.state());
    }

    @Test
    public void testFatalFailureWhenStateIsUnjoined() {
        ConsumerMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        assertEquals(MemberState.JOINING, membershipManager.state());

        testStateUpdateOnFatalFailure(membershipManager);
    }

    @Test
    public void testFatalFailureWhenStateIsStable() {
        ConsumerMembershipManager membershipManager = createMemberInStableState(null);

        testStateUpdateOnFatalFailure(membershipManager);
    }

    @Test
    public void testFatalFailureWhenStateIsPrepareLeaving() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        ConsumerRebalanceListenerInvoker invoker = consumerRebalanceListenerInvoker();
        ConsumerRebalanceListenerCallbackCompletedEvent callbackEvent =
            mockPrepareLeavingStuckOnUserCallback(membershipManager, invoker);
        assertEquals(MemberState.PREPARE_LEAVING, membershipManager.state());

        testStateUpdateOnFatalFailure(membershipManager);

        // When callback completes member should abort the leave operation and remain in FATAL.
        completeCallback(callbackEvent, membershipManager);
        assertEquals(MemberState.FATAL, membershipManager.state());
    }

    @Test
    public void testFatalFailureWhenStateIsLeaving() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();

        // Start leaving group.
        mockLeaveGroup();
        CompletableFuture<Void> leaveOperation = membershipManager.leaveGroup();
        verify(subscriptionState).unsubscribe();
        assertEquals(MemberState.LEAVING, membershipManager.state());
        assertFalse(leaveOperation.isDone());

        // Get fatal failure while waiting to send the heartbeat to leave. Member should
        // transition to FATAL, so the last heartbeat for leaving won't be sent because the member
        // already failed.
        testStateUpdateOnFatalFailure(membershipManager);

        assertEquals(MemberState.FATAL, membershipManager.state());
        assertTrue(leaveOperation.isDone(), "Member should complete the ongoing leave operation when transitioning to FATAL state");
        membershipManager.onHeartbeatRequestGenerated();
        assertEquals(MemberState.FATAL, membershipManager.state());
    }

    @Test
    public void testFatalFailureWhenMemberAlreadyLeft() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();

        // Start leaving group.
        mockLeaveGroup();
        CompletableFuture<Void> sendLeave = membershipManager.leaveGroup();
        verify(subscriptionState).unsubscribe();
        assertEquals(MemberState.LEAVING, membershipManager.state());

        // Last heartbeat sent.
        membershipManager.onHeartbeatRequestGenerated();
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());

        // Fatal error received in response for the last heartbeat. Member should remain in FATAL
        // state but no callbacks should be triggered because the member already left the group.
        MockRebalanceListener rebalanceListener = new MockRebalanceListener();
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.of(rebalanceListener));
        membershipManager.onHeartbeatFailure(false);
        membershipManager.transitionToFatal();
        assertEquals(0, rebalanceListener.lostCount);

        assertEquals(MemberState.FATAL, membershipManager.state());

        assertTrue(sendLeave.isDone());
        assertFalse(sendLeave.isCompletedExceptionally(), "Send leave should complete when a " +
                "response to the leave group is received, even if it contains errors.");
    }

    @Test
    public void testUpdateStateFailsOnResponsesWithErrors() {
        ConsumerMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        // Updating state with a heartbeat response containing errors cannot be performed and
        // should fail.
        ConsumerGroupHeartbeatResponse unknownMemberResponse =
                createConsumerGroupHeartbeatResponseWithError(Errors.UNKNOWN_MEMBER_ID, membershipManager.memberId());
        assertThrows(IllegalArgumentException.class,
                () -> membershipManager.onHeartbeatSuccess(unknownMemberResponse));
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
        ConsumerMembershipManager membershipManager = mockJoinAndReceiveAssignment(false);
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertFalse(membershipManager.topicsAwaitingReconciliation().isEmpty());

        // When the ack is sent nothing should change. Member still has nothing to reconcile,
        // only topics waiting for metadata.
        membershipManager.onHeartbeatRequestGenerated();
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
        verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedAssignment, expectedAssignment);

        // When ack for the reconciled assignment is sent, member should go back to STABLE
        // because the first assignment that was not resolved should have been discarded
        membershipManager.onHeartbeatRequestGenerated();
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
        ConsumerMembershipManager membershipManager = mockJoinAndReceiveAssignment(false);
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertFalse(membershipManager.topicsAwaitingReconciliation().isEmpty());

        // When the ack is sent nothing should change. Member still has nothing to reconcile,
        // only topics waiting for metadata.
        membershipManager.onHeartbeatRequestGenerated();
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

        verifyReconciliationNotTriggered(membershipManager);

        membershipManager.poll(time.milliseconds());

        verifyReconciliationTriggeredAndCompleted(membershipManager, Collections.emptyList());

        membershipManager.onHeartbeatRequestGenerated();

        assertEquals(MemberState.STABLE, membershipManager.state());
    }

    @Test
    public void testNewAssignmentNotInMetadataReplacesPreviousOneWaitingOnMetadata() {
        ConsumerMembershipManager membershipManager = mockJoinAndReceiveAssignment(false);
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertFalse(membershipManager.topicsAwaitingReconciliation().isEmpty());

        // New target assignment (not found in metadata) received while there is another one
        // waiting to be resolved and reconciled. This assignment does not include the previous
        // one that is waiting for metadata, so the member will discard the topics that were
        // waiting for metadata, and just keep the new one as unresolved.
        Uuid topicId = Uuid.randomUuid();
        when(metadata.topicNames()).thenReturn(Collections.emptyMap());
        receiveAssignment(topicId, Collections.singletonList(0), membershipManager);

        verifyReconciliationNotTriggered(membershipManager);
        membershipManager.poll(time.milliseconds());
        membershipManager.onHeartbeatRequestGenerated();

        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertFalse(membershipManager.topicsAwaitingReconciliation().isEmpty());
        assertEquals(topicId, membershipManager.topicsAwaitingReconciliation().iterator().next());
    }

    /**
     * This ensures that the client reconciles target assignments as soon as they are discovered
     * in metadata, without depending on the broker to re-send the assignment.
     */
    @Test
    public void testUnresolvedTargetAssignmentIsReconciledWhenMetadataReceived() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
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
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.empty());

        membershipManager.poll(time.milliseconds());

        // Assignment should have been reconciled.
        Set<TopicPartition> expectedAssignment = Collections.singleton(new TopicPartition(topicName, 1));
        verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedAssignment, expectedAssignment);
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        assertTrue(membershipManager.topicsAwaitingReconciliation().isEmpty());
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
                                .setPartitions(Collections.singletonList(0)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topic2)
                                .setPartitions(Arrays.asList(1, 3))
                ));
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topic1, topic1Name));

        // Receive assignment partly in metadata - reconcile+ack what's in metadata, keep the
        // unresolved and request metadata update.
        ConsumerMembershipManager membershipManager = mockJoinAndReceiveAssignment(true, assignment);
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        verify(metadata).requestUpdate(anyBoolean());
        assertEquals(Collections.singleton(topic2), membershipManager.topicsAwaitingReconciliation());

        // When the ack is sent the member should go back to RECONCILING because it still has
        // unresolved assignment to be reconciled.
        membershipManager.onHeartbeatRequestGenerated();
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        // Target assignment received again with the same unresolved topic. Client should keep it
        // as unresolved.
        clearInvocations(subscriptionState);
        membershipManager.onHeartbeatSuccess(createConsumerGroupHeartbeatResponse(assignment, membershipManager.memberId()));
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertEquals(Collections.singleton(topic2), membershipManager.topicsAwaitingReconciliation());
        verify(subscriptionState, never()).assignFromSubscribed(anyCollection());
    }

    @Test
    public void testReconcileNewPartitionsAssignedWhenNoPartitionOwned() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";
        ConsumerMembershipManager membershipManager = createMembershipManagerJoiningGroup();
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
        ConsumerMembershipManager membershipManager = createMemberInStableState();
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
        ConsumerMembershipManager membershipManager = createMembershipManagerJoiningGroup();
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

        membershipManager.onHeartbeatRequestGenerated();
        assertEquals(MemberState.STABLE, membershipManager.state());

        // Receive same assignment again - should not trigger reconciliation
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId, topicName, expectedAssignmentReconciled);
        receiveAssignment(topicId, Arrays.asList(0, 1), membershipManager);
        // Verify new reconciliation was not triggered
        verify(membershipManager, never()).markReconciliationInProgress();
        verify(membershipManager, never()).markReconciliationCompleted();
        verify(subscriptionState, never()).assignFromSubscribed(anyCollection());

        assertEquals(MemberState.STABLE, membershipManager.state());

        assertEquals(1.0d, getMetricValue(metrics, rebalanceMetricsManager.rebalanceTotal));
        assertEquals(0.0d, getMetricValue(metrics, rebalanceMetricsManager.failedRebalanceTotal));
    }

    @Test
    public void testReconcilePartitionsRevokedNoAutoCommitNoCallbacks() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        mockOwnedPartition(membershipManager, Uuid.randomUuid(), "topic1");

        mockRevocationNoCallbacks(false);

        receiveEmptyAssignment(membershipManager);

        verifyReconciliationNotTriggered(membershipManager);
        membershipManager.poll(time.milliseconds());

        testRevocationOfAllPartitionsCompleted(membershipManager);
        verify(subscriptionState, times(2)).markPendingRevocation(Set.of(new TopicPartition("topic1", 0)));
    }

    @Test
    public void testReconcilePartitionsRevokedWithSuccessfulAutoCommitNoCallbacks() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        mockOwnedPartition(membershipManager, Uuid.randomUuid(), "topic1");

        CompletableFuture<Void> commitResult = mockRevocationNoCallbacks(true);

        receiveEmptyAssignment(membershipManager);

        verifyReconciliationNotTriggered(membershipManager);
        when(commitRequestManager.maybeAutoCommitSyncBeforeRevocation(anyLong())).thenReturn(commitResult);
        membershipManager.poll(time.milliseconds());

        // Member stays in RECONCILING while the commit request hasn't completed.
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        // Partitions should be still owned by the member
        verify(subscriptionState, never()).assignFromSubscribed(anyCollection());

        // Complete commit request
        commitResult.complete(null);
        InOrder inOrder = inOrder(subscriptionState, commitRequestManager);
        inOrder.verify(subscriptionState).markPendingRevocation(Set.of(new TopicPartition("topic1", 0)));
        inOrder.verify(commitRequestManager).maybeAutoCommitSyncBeforeRevocation(anyLong());
        inOrder.verify(subscriptionState).markPendingRevocation(Set.of(new TopicPartition("topic1", 0)));

        testRevocationOfAllPartitionsCompleted(membershipManager);
    }

    @Test
    public void testReconcilePartitionsRevokedWithFailedAutoCommitCompletesRevocationAnyway() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        mockOwnedPartition(membershipManager, Uuid.randomUuid(), "topic1");

        CompletableFuture<Void> commitResult = mockRevocationNoCallbacks(true);

        receiveEmptyAssignment(membershipManager);

        verifyReconciliationNotTriggered(membershipManager);
        membershipManager.poll(time.milliseconds());

        // Member stays in RECONCILING while the commit request hasn't completed.
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        // Partitions should be still owned by the member
        verify(subscriptionState, never()).assignFromSubscribed(anyCollection());

        // Complete commit request
        commitResult.completeExceptionally(new KafkaException("Commit request failed with " +
                "non-retriable error"));
        verify(subscriptionState, times(2)).markPendingRevocation(Set.of(new TopicPartition("topic1", 0)));

        testRevocationOfAllPartitionsCompleted(membershipManager);
    }

    @Test
    public void testReconcileNewPartitionsAssignedAndRevoked() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";
        TopicIdPartition ownedPartition = new TopicIdPartition(topicId,
            new TopicPartition(topicName, 0));
        ConsumerMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId, topicName,
            Collections.singletonList(ownedPartition));

        mockRevocationNoCallbacks(false);

        // New assignment received, revoking partition 0, and assigning new partitions 1 and 2.
        receiveAssignment(topicId, Arrays.asList(1, 2), membershipManager);

        verifyReconciliationNotTriggered(membershipManager);
        membershipManager.poll(time.milliseconds());

        TreeSet<TopicPartition> expectedSet = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        expectedSet.add(new TopicPartition(topicName, 1));
        expectedSet.add(new TopicPartition(topicName, 2));
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        assertEquals(topicIdPartitionsMap(topicId, 1, 2), membershipManager.currentAssignment().partitions);
        assertFalse(membershipManager.reconciliationInProgress());

        verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedSet, expectedSet);
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
        ConsumerMembershipManager membershipManager = mockJoinAndReceiveAssignment(true, targetAssignment);
        membershipManager.onHeartbeatRequestGenerated();
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        // Should not trigger reconciliation, and request a metadata update.
        verifyReconciliationNotTriggered(membershipManager);
        assertEquals(Collections.singleton(topicId), membershipManager.topicsAwaitingReconciliation());
        verify(metadata).requestUpdate(anyBoolean());

        String topicName = "topic1";
        mockTopicNameInMetadataCache(Collections.singletonMap(topicId, topicName), true);

        // When the next poll is run, the member should re-trigger reconciliation
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
        ConsumerGroupHeartbeatResponseData.Assignment targetAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.singletonList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topicId)
                                .setPartitions(Arrays.asList(0, 1))));
        ConsumerMembershipManager membershipManager = mockJoinAndReceiveAssignment(true, targetAssignment);
        membershipManager.onHeartbeatRequestGenerated();
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

        ConsumerMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId, topicName, Collections.emptyList());

        // Member received assignment to reconcile;
        receiveAssignment(topicId, Arrays.asList(0, 1), membershipManager);

        verifyReconciliationNotTriggered(membershipManager);
        membershipManager.poll(time.milliseconds());
        verify(subscriptionState).markPendingRevocation(Set.of());

        // Member should complete reconciliation
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        List<Integer> partitions = Arrays.asList(0, 1);
        Set<TopicPartition> assignedPartitions =
            partitions.stream().map(p -> new TopicPartition(topicName, p)).collect(Collectors.toSet());
        Map<Uuid, SortedSet<Integer>> assignedTopicIdPartitions = Collections.singletonMap(topicId,
            new TreeSet<>(partitions));
        assertEquals(assignedTopicIdPartitions, membershipManager.currentAssignment().partitions);
        assertFalse(membershipManager.reconciliationInProgress());

        mockAckSent(membershipManager);
        when(subscriptionState.assignedPartitions()).thenReturn(assignedPartitions);

        // Revocation of topic not found in metadata cache
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        mockRevocationNoCallbacks(false);
        mockTopicNameInMetadataCache(Collections.singletonMap(topicId, topicName), false);

        // Revoke one of the 2 partitions
        receiveAssignment(topicId, Collections.singletonList(1), membershipManager);

        membershipManager.poll(time.milliseconds());
        verify(subscriptionState, times(2)).markPendingRevocation(Set.of(new TopicPartition(topicName, 0)));

        // Revocation should complete without requesting any metadata update given that the topic
        // received in target assignment should exist in local topic name cache.
        verify(metadata, never()).requestUpdate(anyBoolean());
        List<TopicIdPartition> remainingAssignment = topicIdPartitions(topicId, topicName, 1);

        testRevocationCompleted(membershipManager, remainingAssignment);
    }

    @Test
    public void testOnSubscriptionUpdatedDoesNotTransitionToJoiningIfInGroup() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        membershipManager.onSubscriptionUpdated();
        assertTrue(membershipManager.subscriptionUpdated());
        membershipManager.onConsumerPoll();
        verify(membershipManager, never()).transitionToJoining();
        assertFalse(membershipManager.subscriptionUpdated());
    }

    @Test
    public void testOnSubscriptionUpdatedTransitionsToJoiningOnPollIfNotInGroup() {
        ConsumerMembershipManager membershipManager = createMembershipManager(null);
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        membershipManager.onSubscriptionUpdated();
        verify(membershipManager, never()).transitionToJoining();
        assertTrue(membershipManager.subscriptionUpdated());
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        membershipManager.onConsumerPoll();
        verify(membershipManager).transitionToJoining();
    }

    @Test
    public void testListenerCallbacksBasic() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        CounterConsumerRebalanceListener listener = new CounterConsumerRebalanceListener();
        ConsumerRebalanceListenerInvoker invoker = consumerRebalanceListenerInvoker();

        String topicName = "topic1";
        Uuid topicId = Uuid.randomUuid();

        when(subscriptionState.assignedPartitions()).thenReturn(Collections.emptySet());
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.of(listener));
        doNothing().when(subscriptionState).markPendingRevocation(anySet());
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));

        // Step 2: put the state machine into the appropriate... state
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));
        receiveAssignment(topicId, Arrays.asList(0, 1), membershipManager);

        verifyReconciliationNotTriggered(membershipManager);
        membershipManager.poll(time.milliseconds());

        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertTrue(membershipManager.reconciliationInProgress());
        assertEquals(0, listener.revokedCount());
        assertEquals(0, listener.assignedCount());
        assertEquals(0, listener.lostCount());

        assertTrue(membershipManager.reconciliationInProgress());

        // Step 3: assign partitions
        performCallback(
                membershipManager,
                invoker,
                ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED,
                topicPartitions(topicName, 0, 1),
                true
        );

        assertFalse(membershipManager.reconciliationInProgress());

        // Step 4: Send ack and make sure we're done and our listener was called appropriately
        membershipManager.onHeartbeatRequestGenerated();
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertEquals(topicIdPartitionsMap(topicId, 0, 1), membershipManager.currentAssignment().partitions);

        assertEquals(0, listener.revokedCount());
        assertEquals(1, listener.assignedCount());
        assertEquals(0, listener.lostCount());

        // Step 5: receive an empty assignment, which means we should call revoke
        when(subscriptionState.assignedPartitions()).thenReturn(topicPartitions(topicName, 0, 1));
        receiveEmptyAssignment(membershipManager);
        membershipManager.poll(time.milliseconds());

        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertTrue(membershipManager.reconciliationInProgress());

        // Step 6: revoke partitions
        performCallback(
                membershipManager,
                invoker,
                ConsumerRebalanceListenerMethodName.ON_PARTITIONS_REVOKED,
                topicPartitions(topicName, 0, 1),
                true
        );
        assertTrue(membershipManager.reconciliationInProgress());

        // Step 7: assign partitions should still be called, even though it's empty
        performCallback(
                membershipManager,
                invoker,
                ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED,
                Collections.emptySortedSet(),
                true
        );
        assertFalse(membershipManager.reconciliationInProgress());

        // Step 8: Send ack and make sure we're done and our listener was called appropriately
        membershipManager.onHeartbeatRequestGenerated();
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertFalse(membershipManager.reconciliationInProgress());

        assertEquals(1, listener.revokedCount());
        assertEquals(2, listener.assignedCount());
        assertEquals(0, listener.lostCount());
    }

    @Test
    public void testListenerCallbacksThrowsErrorOnPartitionsRevoked() {
        // Step 1: set up mocks
        String topicName = "topic1";
        Uuid topicId = Uuid.randomUuid();

        ConsumerMembershipManager membershipManager = createMemberInStableState();
        mockOwnedPartition(membershipManager, topicId, topicName);
        CounterConsumerRebalanceListener listener = new CounterConsumerRebalanceListener(
                Optional.of(new IllegalArgumentException("Intentional onPartitionsRevoked() error")),
                Optional.empty(),
                Optional.empty()
        );
        ConsumerRebalanceListenerInvoker invoker = consumerRebalanceListenerInvoker();

        when(subscriptionState.rebalanceListener()).thenReturn(Optional.of(listener));
        doNothing().when(subscriptionState).markPendingRevocation(anySet());

        // Step 2: put the state machine into the appropriate... state
        receiveEmptyAssignment(membershipManager);

        verifyReconciliationNotTriggered(membershipManager);
        membershipManager.poll(time.milliseconds());

        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertEquals(topicIdPartitionsMap(topicId, 0), membershipManager.currentAssignment().partitions);
        assertTrue(membershipManager.reconciliationInProgress());
        assertEquals(0, listener.revokedCount());
        assertEquals(0, listener.assignedCount());
        assertEquals(0, listener.lostCount());

        assertTrue(membershipManager.reconciliationInProgress());

        // Step 3: revoke partitions
        performCallback(
                membershipManager,
                invoker,
                ConsumerRebalanceListenerMethodName.ON_PARTITIONS_REVOKED,
                topicPartitions(topicName, 0),
                true
        );

        assertFalse(membershipManager.reconciliationInProgress());
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        // Step 4: Send ack and make sure we're done and our listener was called appropriately
        membershipManager.onHeartbeatRequestGenerated();
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        assertEquals(1, listener.revokedCount());
        assertEquals(0, listener.assignedCount());
        assertEquals(0, listener.lostCount());
    }

    @Test
    public void testListenerCallbacksThrowsErrorOnPartitionsAssigned() {
        // Step 1: set up mocks
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        String topicName = "topic1";
        Uuid topicId = Uuid.randomUuid();
        mockOwnedPartition(membershipManager, topicId, topicName);
        CounterConsumerRebalanceListener listener = new CounterConsumerRebalanceListener(
                Optional.empty(),
                Optional.of(new IllegalArgumentException("Intentional onPartitionsAssigned() error")),
                Optional.empty()
        );
        ConsumerRebalanceListenerInvoker invoker = consumerRebalanceListenerInvoker();

        when(subscriptionState.rebalanceListener()).thenReturn(Optional.of(listener));
        doNothing().when(subscriptionState).markPendingRevocation(anySet());

        // Step 2: put the state machine into the appropriate... state
        receiveEmptyAssignment(membershipManager);

        verifyReconciliationNotTriggered(membershipManager);
        membershipManager.poll(time.milliseconds());

        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertEquals(topicIdPartitionsMap(topicId, 0), membershipManager.currentAssignment().partitions);
        assertTrue(membershipManager.reconciliationInProgress());
        assertEquals(0, listener.revokedCount());
        assertEquals(0, listener.assignedCount());
        assertEquals(0, listener.lostCount());

        assertTrue(membershipManager.reconciliationInProgress());

        // Step 3: revoke partitions
        performCallback(
                membershipManager,
                invoker,
                ConsumerRebalanceListenerMethodName.ON_PARTITIONS_REVOKED,
                topicPartitions("topic1", 0),
                true
        );

        assertTrue(membershipManager.reconciliationInProgress());

        // Step 4: assign partitions
        performCallback(
                membershipManager,
                invoker,
                ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED,
                Collections.emptySortedSet(),
                true
        );

        assertFalse(membershipManager.reconciliationInProgress());
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        // Step 5: Send ack and make sure we're done and our listener was called appropriately
        membershipManager.onHeartbeatRequestGenerated();
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        assertEquals(1, listener.revokedCount());
        assertEquals(1, listener.assignedCount());
        assertEquals(0, listener.lostCount());
    }

    @Test
    public void testAddedPartitionsTemporarilyDisabledAwaitingOnPartitionsAssignedCallback() {
        ConsumerMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        String topicName = "topic1";
        ConsumerRebalanceListenerInvoker invoker = consumerRebalanceListenerInvoker();
        int partitionOwned = 0;
        int partitionAdded = 1;
        SortedSet<TopicPartition> assignedPartitions = topicPartitions(topicName, partitionOwned,
            partitionAdded);
        SortedSet<TopicPartition> addedPartitions = topicPartitions(topicName, partitionAdded);
        mockPartitionOwnedAndNewPartitionAdded(topicName, partitionOwned, partitionAdded,
            new CounterConsumerRebalanceListener(), membershipManager);

        membershipManager.poll(time.milliseconds());

        verify(subscriptionState).assignFromSubscribedAwaitingCallback(assignedPartitions, addedPartitions);

        performCallback(
            membershipManager,
            invoker,
            ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED,
            addedPartitions,
            true
        );

        verify(subscriptionState).enablePartitionsAwaitingCallback(addedPartitions);
    }

    @Test
    public void testAddedPartitionsNotEnabledAfterFailedOnPartitionsAssignedCallback() {
        ConsumerMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        String topicName = "topic1";
        ConsumerRebalanceListenerInvoker invoker = consumerRebalanceListenerInvoker();
        int partitionOwned = 0;
        int partitionAdded = 1;
        SortedSet<TopicPartition> assignedPartitions = topicPartitions(topicName, partitionOwned,
            partitionAdded);
        SortedSet<TopicPartition> addedPartitions = topicPartitions(topicName, partitionAdded);
        CounterConsumerRebalanceListener listener =
            new CounterConsumerRebalanceListener(Optional.empty(),
                Optional.of(new RuntimeException("onPartitionsAssigned failed!")),
                Optional.empty());
        mockPartitionOwnedAndNewPartitionAdded(topicName, partitionOwned, partitionAdded,
            listener, membershipManager);

        membershipManager.poll(time.milliseconds());

        verify(subscriptionState).assignFromSubscribedAwaitingCallback(assignedPartitions, addedPartitions);

        performCallback(
            membershipManager,
            invoker,
            ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED,
            addedPartitions,
            true
        );
        verify(subscriptionState, never()).enablePartitionsAwaitingCallback(any());
    }

    @Test
    public void testOnPartitionsLostNoError() {
        testOnPartitionsLost(Optional.empty());
    }

    @Test
    public void testOnPartitionsLostError() {
        testOnPartitionsLost(Optional.of(new KafkaException("Intentional error for test")));
    }

    private void assertLeaveGroupDueToExpiredPollAndTransitionToStale(ConsumerMembershipManager membershipManager) {
        assertDoesNotThrow(() -> membershipManager.transitionToSendingLeaveGroup(true));
        assertEquals(LEAVE_GROUP_MEMBER_EPOCH, membershipManager.memberEpoch());
        membershipManager.onHeartbeatRequestGenerated();
        assertStaleMemberLeavesGroupAndClearsAssignment(membershipManager);
    }

    @Test
    public void testTransitionToLeavingWhileReconcilingDueToStaleMember() {
        ConsumerMembershipManager membershipManager = memberJoinWithAssignment();
        clearInvocations(subscriptionState);
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertLeaveGroupDueToExpiredPollAndTransitionToStale(membershipManager);
    }

    @Test
    public void testTransitionToLeavingWhileJoiningDueToStaleMember() {
        ConsumerMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        doNothing().when(subscriptionState).assignFromSubscribed(any());
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        assertEquals(MemberState.JOINING, membershipManager.state());
        assertLeaveGroupDueToExpiredPollAndTransitionToStale(membershipManager);
    }

    @Test
    public void testTransitionToLeavingWhileStableDueToStaleMember() {
        ConsumerMembershipManager membershipManager = createMemberInStableState(null);
        assertLeaveGroupDueToExpiredPollAndTransitionToStale(membershipManager);
    }

    @Test
    public void testTransitionToLeavingWhileAcknowledgingDueToStaleMember() {
        ConsumerMembershipManager membershipManager = mockJoinAndReceiveAssignment(true);
        doNothing().when(subscriptionState).assignFromSubscribed(any());
        clearInvocations(subscriptionState);
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        assertLeaveGroupDueToExpiredPollAndTransitionToStale(membershipManager);
    }

    @Test
    public void testStaleMemberDoesNotSendHeartbeatAndAllowsTransitionToJoiningToRecover() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        doNothing().when(subscriptionState).assignFromSubscribed(any());
        membershipManager.transitionToSendingLeaveGroup(true);
        membershipManager.onHeartbeatRequestGenerated();
        assertEquals(MemberState.STALE, membershipManager.state());
        assertTrue(membershipManager.shouldSkipHeartbeat(), "Stale member should not send heartbeats");
        // Check that a transition to joining is allowed, which is what is expected to happen
        // when the poll timer is reset.
        assertDoesNotThrow(membershipManager::maybeRejoinStaleMember);
    }

    @Test
    public void testStaleMemberRejoinsWhenTimerResetsNoCallbacks() {
        ConsumerMembershipManager membershipManager = mockStaleMember();
        assertStaleMemberLeavesGroupAndClearsAssignment(membershipManager);

        membershipManager.maybeRejoinStaleMember();
        assertEquals(MemberState.JOINING, membershipManager.state());
    }

    @Test
    public void testStaleMemberWaitsForCallbackToRejoinWhenTimerReset() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";
        int ownedPartition = 0;
        TopicPartition tp = new TopicPartition(topicName, ownedPartition);
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId, topicName,
            Collections.singletonList(new TopicIdPartition(topicId, tp)));
        CounterConsumerRebalanceListener listener = new CounterConsumerRebalanceListener();
        ConsumerRebalanceListenerInvoker invoker = consumerRebalanceListenerInvoker();
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.of(listener));

        membershipManager.transitionToSendingLeaveGroup(true);
        membershipManager.onHeartbeatRequestGenerated();

        assertEquals(MemberState.STALE, membershipManager.state());
        assertFalse(backgroundEventQueue.isEmpty());
        assertInstanceOf(ConsumerRebalanceListenerCallbackNeededEvent.class, backgroundEventQueue.peek());

        // Stale member triggers onPartitionLost callback that will not complete just yet
        ConsumerRebalanceListenerCallbackCompletedEvent callbackEvent = performCallback(
            membershipManager,
            invoker,
            ConsumerRebalanceListenerMethodName.ON_PARTITIONS_LOST,
            topicPartitions(topicName, ownedPartition),
            false
        );

        // Timer reset while callback hasn't completed. Member should stay in STALE while it
        // completes releasing its assignment, and then transition to joining.
        membershipManager.maybeRejoinStaleMember();
        assertEquals(MemberState.STALE, membershipManager.state(), "Member should not transition " +
            "out of the STALE state when the timer is reset if the callback has not completed.");
        // Member should not clear its assignment to rejoin until the callback completes
        verify(subscriptionState, never()).assignFromSubscribed(any());

        completeCallback(callbackEvent, membershipManager);
        assertEquals(MemberState.JOINING, membershipManager.state());
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
    }

    private ConsumerMembershipManager mockStaleMember() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        doNothing().when(subscriptionState).assignFromSubscribed(any());
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        membershipManager.transitionToSendingLeaveGroup(true);
        membershipManager.onHeartbeatRequestGenerated();
        return membershipManager;
    }

    private void mockPartitionOwnedAndNewPartitionAdded(String topicName,
                                                        int partitionOwned,
                                                        int partitionAdded,
                                                        CounterConsumerRebalanceListener listener,
                                                        ConsumerMembershipManager membershipManager) {
        Uuid topicId = Uuid.randomUuid();
        TopicPartition owned = new TopicPartition(topicName, partitionOwned);
        when(subscriptionState.assignedPartitions()).thenReturn(Collections.singleton(owned));
        membershipManager.updateAssignment(Collections.singletonMap(topicId, mkSortedSet(partitionOwned)));
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.ofNullable(listener));

        // Receive assignment adding a new partition
        receiveAssignment(topicId, Arrays.asList(partitionOwned, partitionAdded), membershipManager);
    }

    private void testOnPartitionsLost(Optional<RuntimeException> lostError) {
        // Step 1: set up mocks
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        String topicName = "topic1";
        Uuid topicId = Uuid.randomUuid();
        mockOwnedPartition(membershipManager, topicId, topicName);
        CounterConsumerRebalanceListener listener = new CounterConsumerRebalanceListener(
                Optional.empty(),
                Optional.empty(),
                lostError
        );
        ConsumerRebalanceListenerInvoker invoker = consumerRebalanceListenerInvoker();

        when(subscriptionState.rebalanceListener()).thenReturn(Optional.of(listener));
        doNothing().when(subscriptionState).markPendingRevocation(anySet());

        // Step 2: put the state machine into the appropriate... state
        membershipManager.transitionToFenced();
        assertEquals(MemberState.FENCED, membershipManager.state());
        assertEquals(0, listener.revokedCount());
        assertEquals(0, listener.assignedCount());
        assertEquals(0, listener.lostCount());

        assertTrue(membershipManager.shouldSkipHeartbeat(), "Member should not send heartbeat while fenced");

        // Step 3: invoke the callback
        performCallback(
                membershipManager,
                invoker,
                ConsumerRebalanceListenerMethodName.ON_PARTITIONS_LOST,
                topicPartitions("topic1", 0),
                true
        );

        assertTrue(membershipManager.currentAssignment().isNone());

        // Step 4: Receive ack and make sure we're done and our listener was called appropriately
        membershipManager.onHeartbeatRequestGenerated();
        assertEquals(MemberState.JOINING, membershipManager.state());

        assertEquals(0, listener.revokedCount());
        assertEquals(0, listener.assignedCount());
        assertEquals(1, listener.lostCount());
    }

    private ConsumerRebalanceListenerInvoker consumerRebalanceListenerInvoker() {
        return new ConsumerRebalanceListenerInvoker(
                new LogContext(),
                subscriptionState,
                time,
                new RebalanceCallbackMetricsManager(new Metrics(time))
        );
    }

    private static SortedSet<TopicPartition> topicPartitions(Map<Uuid, SortedSet<Integer>> topicIdMap, Map<Uuid, String> topicIdNames) {
        SortedSet<TopicPartition> topicPartitions = new TreeSet<>(new Utils.TopicPartitionComparator());

        for (Uuid topicId : topicIdMap.keySet()) {
            for (int partition : topicIdMap.get(topicId)) {
                topicPartitions.add(new TopicPartition(topicIdNames.get(topicId), partition));
            }
        }

        return topicPartitions;
    }

    private SortedSet<TopicPartition> topicPartitions(String topicName, int... partitions) {
        SortedSet<TopicPartition> topicPartitions = new TreeSet<>(new Utils.TopicPartitionComparator());

        for (int partition : partitions)
            topicPartitions.add(new TopicPartition(topicName, partition));

        return topicPartitions;
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

    private ConsumerRebalanceListenerCallbackCompletedEvent performCallback(ConsumerMembershipManager membershipManager,
                                                                            ConsumerRebalanceListenerInvoker invoker,
                                                                            ConsumerRebalanceListenerMethodName expectedMethodName,
                                                                            SortedSet<TopicPartition> expectedPartitions,
                                                                            boolean complete) {
        // We expect only our enqueued event in the background queue.
        assertEquals(1, backgroundEventQueue.size());
        assertNotNull(backgroundEventQueue.peek());
        assertInstanceOf(ConsumerRebalanceListenerCallbackNeededEvent.class, backgroundEventQueue.peek());
        ConsumerRebalanceListenerCallbackNeededEvent neededEvent = (ConsumerRebalanceListenerCallbackNeededEvent) backgroundEventQueue.poll();
        assertNotNull(neededEvent);
        assertEquals(expectedMethodName, neededEvent.methodName());
        assertEquals(expectedPartitions, neededEvent.partitions());

        ConsumerRebalanceListenerCallbackCompletedEvent invokedEvent = invokeRebalanceCallbacks(
                invoker,
                neededEvent.methodName(),
                neededEvent.partitions(),
                neededEvent.future()
        );

        if (complete) {
            completeCallback(invokedEvent, membershipManager);
        }
        return invokedEvent;
    }

    private void completeCallback(ConsumerRebalanceListenerCallbackCompletedEvent callbackCompletedEvent,
                                  ConsumerMembershipManager membershipManager) {
        membershipManager.consumerRebalanceListenerCallbackCompleted(callbackCompletedEvent);
    }

    private void testFenceIsNoOp(ConsumerMembershipManager membershipManager) {
        assertNotEquals(0, membershipManager.memberEpoch());
        verify(subscriptionState, never()).rebalanceListener();
    }

    private void assertStaleMemberLeavesGroupAndClearsAssignment(ConsumerMembershipManager membershipManager) {
        assertEquals(MemberState.STALE, membershipManager.state());

        // Should reset epoch to leave the group and release the assignment (right away because
        // there is no onPartitionsLost callback defined)
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
        assertTrue(membershipManager.currentAssignment().isNone());
        assertTrue(membershipManager.topicsAwaitingReconciliation().isEmpty());
        assertEquals(LEAVE_GROUP_MEMBER_EPOCH, membershipManager.memberEpoch());
        verify(subscriptionState, never()).unsubscribe();
    }

    @Test
    public void testMemberJoiningTransitionsToStableWhenReceivingEmptyAssignment() {
        ConsumerMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.empty());
        assertEquals(MemberState.JOINING, membershipManager.state());
        receiveEmptyAssignment(membershipManager);

        verifyReconciliationNotTriggered(membershipManager);
        membershipManager.poll(time.milliseconds());
        membershipManager.onHeartbeatRequestGenerated();

        assertEquals(MemberState.STABLE, membershipManager.state());
    }

    @Test
    public void testMemberJoiningCallsRebalanceListenerWhenReceivingEmptyAssignment() {
        CounterConsumerRebalanceListener listener = new CounterConsumerRebalanceListener();
        ConsumerRebalanceListenerInvoker invoker = consumerRebalanceListenerInvoker();

        when(subscriptionState.rebalanceListener()).thenReturn(Optional.of(listener));
        ConsumerMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        receiveEmptyAssignment(membershipManager);

        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertEquals(0, listener.assignedCount());
        assertEquals(0, listener.revokedCount());
        assertEquals(0, listener.lostCount());

        verifyReconciliationNotTriggered(membershipManager);
        membershipManager.poll(time.milliseconds());
        performCallback(
            membershipManager,
            invoker,
            ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED,
            new TreeSet<>(Collections.emptyList()),
            true
        );

        assertEquals(1, listener.assignedCount());
        assertEquals(0, listener.revokedCount());
        assertEquals(0, listener.lostCount());
    }

    @Test
    public void testMetricsWhenHeartbeatFailed() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        membershipManager.onHeartbeatFailure(false);

        // Not expecting rebalance failures with only the empty assignment being reconciled.
        assertEquals(1.0d, getMetricValue(metrics, rebalanceMetricsManager.rebalanceTotal));
        assertEquals(0.0d, getMetricValue(metrics, rebalanceMetricsManager.failedRebalanceTotal));
    }

    @Test
    public void testRebalanceMetricsOnSuccessfulRebalance() {
        ConsumerMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(new Assignment(), membershipManager.memberId());
        membershipManager.onHeartbeatSuccess(heartbeatResponse);
        mockOwnedPartition(membershipManager, Uuid.randomUuid(), "topic1");

        CompletableFuture<Void> commitResult = mockRevocationNoCallbacks(true);

        receiveEmptyAssignment(membershipManager);
        long reconciliationDurationMs = 1234;
        time.sleep(reconciliationDurationMs);

        membershipManager.poll(time.milliseconds());
        // Complete commit request to complete the callback invocation
        commitResult.complete(null);

        assertEquals((double) reconciliationDurationMs, getMetricValue(metrics, rebalanceMetricsManager.rebalanceLatencyTotal));
        assertEquals((double) reconciliationDurationMs, getMetricValue(metrics, rebalanceMetricsManager.rebalanceLatencyAvg));
        assertEquals((double) reconciliationDurationMs, getMetricValue(metrics, rebalanceMetricsManager.rebalanceLatencyMax));
        assertEquals(1d, getMetricValue(metrics, rebalanceMetricsManager.rebalanceTotal));
        assertEquals(120d, 1d, (double) getMetricValue(metrics, rebalanceMetricsManager.rebalanceRatePerHour));
        assertEquals(0d, getMetricValue(metrics, rebalanceMetricsManager.failedRebalanceRate));
        assertEquals(0d, getMetricValue(metrics, rebalanceMetricsManager.failedRebalanceTotal));
        assertEquals(0d, getMetricValue(metrics, rebalanceMetricsManager.lastRebalanceSecondsAgo));
    }

    @Test
    public void testRebalanceMetricsForMultipleReconciliations() {
        ConsumerMembershipManager membershipManager = createMemberInStableState();
        ConsumerRebalanceListenerInvoker invoker = consumerRebalanceListenerInvoker();

        String topicName = "topic1";
        Uuid topicId = Uuid.randomUuid();

        SleepyRebalanceListener listener = new SleepyRebalanceListener(1453, time);
        when(subscriptionState.assignedPartitions()).thenReturn(Collections.emptySet());
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.of(listener));
        doNothing().when(subscriptionState).markPendingRevocation(anySet());
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));

        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));
        receiveAssignment(topicId, Arrays.asList(0, 1), membershipManager);

        membershipManager.poll(time.milliseconds());

        // assign partitions
        performCallback(
            membershipManager,
            invoker,
            ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED,
            topicPartitions(topicName, 0, 1),
            true
        );

        long firstRebalanaceTimesMs = listener.sleepMs;
        listener.reset();

        // ack
        membershipManager.onHeartbeatRequestGenerated();

        // revoke all
        when(subscriptionState.assignedPartitions()).thenReturn(topicPartitions(topicName, 0, 1));
        receiveAssignment(topicId, Collections.singletonList(2), membershipManager);

        membershipManager.poll(time.milliseconds());

        performCallback(
            membershipManager,
            invoker,
            ConsumerRebalanceListenerMethodName.ON_PARTITIONS_REVOKED,
            topicPartitions(topicName, 0, 1),
            true
        );

        // assign new partition 2
        performCallback(
            membershipManager,
            invoker,
            ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED,
            topicPartitions(topicName, 2),
            true
        );
        membershipManager.onHeartbeatRequestGenerated();

        long secondRebalanceMs = listener.sleepMs;
        long total = firstRebalanaceTimesMs + secondRebalanceMs;
        double avg = total / 3.0d;
        long max = Math.max(firstRebalanaceTimesMs, secondRebalanceMs);
        assertEquals((double) total, getMetricValue(metrics, rebalanceMetricsManager.rebalanceLatencyTotal));
        assertEquals(avg, (double) getMetricValue(metrics, rebalanceMetricsManager.rebalanceLatencyAvg), 1d);
        assertEquals((double) max, getMetricValue(metrics, rebalanceMetricsManager.rebalanceLatencyMax));
        assertEquals(3d, getMetricValue(metrics, rebalanceMetricsManager.rebalanceTotal));
        // rate is not tested because it is subject to Rate implementation
        assertEquals(0d, getMetricValue(metrics, rebalanceMetricsManager.failedRebalanceRate));
        assertEquals(0d, getMetricValue(metrics, rebalanceMetricsManager.failedRebalanceTotal));
        assertEquals(0d, getMetricValue(metrics, rebalanceMetricsManager.lastRebalanceSecondsAgo));

    }

    @Test
    public void testRebalanceMetricsOnFailedRebalance() {
        ConsumerMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(new Assignment(), membershipManager.memberId());
        membershipManager.onHeartbeatSuccess(heartbeatResponse);

        Uuid topicId = Uuid.randomUuid();

        receiveAssignment(topicId, Arrays.asList(0, 1), membershipManager);

        // sleep for an arbitrary amount
        time.sleep(2300);

        assertTrue(rebalanceMetricsManager.rebalanceStarted());
        membershipManager.onHeartbeatFailure(false);

        assertEquals((double) 0, getMetricValue(metrics, rebalanceMetricsManager.rebalanceLatencyTotal));
        assertEquals(0d, getMetricValue(metrics, rebalanceMetricsManager.rebalanceTotal));
        assertEquals(120d, getMetricValue(metrics, rebalanceMetricsManager.failedRebalanceRate));
        assertEquals(1d, getMetricValue(metrics, rebalanceMetricsManager.failedRebalanceTotal));
        assertEquals(-1d, getMetricValue(metrics, rebalanceMetricsManager.lastRebalanceSecondsAgo));
    }

    private Object getMetricValue(Metrics metrics, MetricName name) {
        return metrics.metrics().get(name).metricValue();
    }

    private ConsumerMembershipManager mockMemberSuccessfullyReceivesAndAcksAssignment(
            Uuid topicId, String topicName, List<Integer> partitions) {
        ConsumerMembershipManager membershipManager = createMembershipManagerJoiningGroup();
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

    private CompletableFuture<Void> mockEmptyAssignmentAndRevocationStuckOnCommit(
            ConsumerMembershipManager membershipManager) {
        CompletableFuture<Void> commitResult = mockRevocationNoCallbacks(true);
        receiveEmptyAssignment(membershipManager);

        verifyReconciliationNotTriggered(membershipManager);
        membershipManager.poll(time.milliseconds());

        verifyReconciliationTriggered(membershipManager);
        clearInvocations(membershipManager);
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        return commitResult;
    }

    private CompletableFuture<Void> mockNewAssignmentAndRevocationStuckOnCommit(
            ConsumerMembershipManager membershipManager, Uuid topicId, String topicName,
            List<Integer> partitions, boolean mockMetadata) {
        CompletableFuture<Void> commitResult = mockRevocationNoCallbacks(true);
        if (mockMetadata) {
            when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));
        }
        receiveAssignment(topicId, partitions, membershipManager);

        verifyReconciliationNotTriggered(membershipManager);
        membershipManager.poll(time.milliseconds());

        verifyReconciliationTriggered(membershipManager);
        clearInvocations(membershipManager);
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        return commitResult;
    }

    private ConsumerRebalanceListenerCallbackCompletedEvent mockNewAssignmentStuckOnPartitionsRevokedCallback(
            ConsumerMembershipManager membershipManager, Uuid topicId, String topicName,
            List<Integer> partitions, TopicPartition ownedPartition, ConsumerRebalanceListenerInvoker invoker) {
        doNothing().when(subscriptionState).markPendingRevocation(anySet());
        CounterConsumerRebalanceListener listener = new CounterConsumerRebalanceListener();
        when(subscriptionState.assignedPartitions()).thenReturn(Collections.singleton(ownedPartition));
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.of(listener));
        when(commitRequestManager.autoCommitEnabled()).thenReturn(false);

        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));
        receiveAssignment(topicId, partitions, membershipManager);
        membershipManager.poll(time.milliseconds());
        verifyReconciliationTriggered(membershipManager);
        clearInvocations(membershipManager);
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        return performCallback(
            membershipManager,
            invoker,
            ConsumerRebalanceListenerMethodName.ON_PARTITIONS_REVOKED,
            topicPartitions(ownedPartition.topic(), ownedPartition.partition()),
            false
        );
    }

    private ConsumerRebalanceListenerCallbackCompletedEvent mockNewAssignmentStuckOnPartitionsAssignedCallback(
            ConsumerMembershipManager membershipManager, Uuid topicId, String topicName, int newPartition,
            ConsumerRebalanceListenerInvoker invoker) {
        CounterConsumerRebalanceListener listener = new CounterConsumerRebalanceListener();
        when(subscriptionState.assignedPartitions()).thenReturn(Collections.emptySet());
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.of(listener));
        when(commitRequestManager.autoCommitEnabled()).thenReturn(false);

        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));
        receiveAssignment(topicId, Collections.singletonList(newPartition), membershipManager);
        membershipManager.poll(time.milliseconds());
        verifyReconciliationTriggered(membershipManager);
        clearInvocations(membershipManager);
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        return performCallback(
            membershipManager,
            invoker,
            ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED,
            topicPartitions(topicName, newPartition),
            false
        );
    }

    private void verifyReconciliationTriggered(ConsumerMembershipManager membershipManager) {
        verify(membershipManager).markReconciliationInProgress();
        assertEquals(MemberState.RECONCILING, membershipManager.state());
    }

    private void verifyReconciliationNotTriggered(ConsumerMembershipManager membershipManager) {
        verify(membershipManager, never()).markReconciliationInProgress();
        verify(membershipManager, never()).markReconciliationCompleted();
    }

    private void verifyReconciliationTriggeredAndCompleted(ConsumerMembershipManager membershipManager,
                                                           List<TopicIdPartition> expectedAssignment) {
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        verify(membershipManager).markReconciliationInProgress();
        verify(membershipManager).markReconciliationCompleted();
        assertFalse(membershipManager.reconciliationInProgress());

        // Assignment applied
        List<TopicPartition> expectedTopicPartitions = buildTopicPartitions(expectedAssignment);
        verify(subscriptionState).assignFromSubscribedAwaitingCallback(eq(new HashSet<>(expectedTopicPartitions)), any());
        Map<Uuid, SortedSet<Integer>> assignmentByTopicId = assignmentByTopicId(expectedAssignment);
        assertEquals(assignmentByTopicId, membershipManager.currentAssignment().partitions);

        // The auto-commit interval should be reset (only once), when the reconciliation completes
        verify(commitRequestManager).resetAutoCommitTimer();
    }

    private List<TopicPartition> buildTopicPartitions(List<TopicIdPartition> topicIdPartitions) {
        return topicIdPartitions.stream().map(TopicIdPartition::topicPartition).collect(Collectors.toList());
    }

    private void mockAckSent(ConsumerMembershipManager membershipManager) {
        membershipManager.onHeartbeatRequestGenerated();
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
            when(commitRequestManager.maybeAutoCommitSyncBeforeRevocation(anyLong())).thenReturn(commitResult);
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

    private void testRevocationOfAllPartitionsCompleted(ConsumerMembershipManager membershipManager) {
        testRevocationCompleted(membershipManager, Collections.emptyList());
    }

    private void testRevocationCompleted(ConsumerMembershipManager membershipManager,
                                         List<TopicIdPartition> expectedCurrentAssignment) {
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        Map<Uuid, SortedSet<Integer>> assignmentByTopicId = assignmentByTopicId(expectedCurrentAssignment);
        assertEquals(assignmentByTopicId, membershipManager.currentAssignment().partitions);
        assertFalse(membershipManager.reconciliationInProgress());

        List<TopicPartition> expectedTopicPartitionAssignment =
                buildTopicPartitions(expectedCurrentAssignment);
        HashSet<TopicPartition> expectedSet = new HashSet<>(expectedTopicPartitionAssignment);
        verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedSet, Collections.emptySet());
    }

    private Map<Uuid, SortedSet<Integer>> assignmentByTopicId(List<TopicIdPartition> topicIdPartitions) {
        Map<Uuid, SortedSet<Integer>> assignmentByTopicId = new HashMap<>();
        topicIdPartitions.forEach(topicIdPartition -> {
            Uuid topicId = topicIdPartition.topicId();
            assignmentByTopicId.computeIfAbsent(topicId, k -> new TreeSet<>()).add(topicIdPartition.partition());
        });
        return assignmentByTopicId;
    }

    private void mockOwnedPartitionAndAssignmentReceived(ConsumerMembershipManager membershipManager,
                                                         Uuid topicId,
                                                         String topicName,
                                                         Collection<TopicIdPartition> previouslyOwned) {
        when(subscriptionState.assignedPartitions()).thenReturn(getTopicPartitions(previouslyOwned));
        HashMap<Uuid, SortedSet<Integer>> partitionsByTopicId = new HashMap<>();
        partitionsByTopicId.put(topicId, new TreeSet<>(previouslyOwned.stream().map(TopicIdPartition::partition).collect(Collectors.toSet())));
        membershipManager.updateAssignment(partitionsByTopicId);
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.empty()).thenReturn(Optional.empty());
    }

    private Set<TopicPartition> getTopicPartitions(Collection<TopicIdPartition> topicIdPartitions) {
        return topicIdPartitions.stream().map(topicIdPartition ->
                new TopicPartition(topicIdPartition.topic(), topicIdPartition.partition()))
            .collect(Collectors.toSet());
    }

    private void mockOwnedPartition(ConsumerMembershipManager membershipManager, Uuid topicId, String topic) {
        int partition = 0;
        TopicPartition previouslyOwned = new TopicPartition(topic, partition);
        membershipManager.updateAssignment(mkMap(mkEntry(topicId, new TreeSet<>(Collections.singletonList(partition)))));
        when(subscriptionState.assignedPartitions()).thenReturn(Collections.singleton(previouslyOwned));
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
    }

    private ConsumerMembershipManager mockJoinAndReceiveAssignment(boolean triggerReconciliation) {
        return mockJoinAndReceiveAssignment(triggerReconciliation, createAssignment(triggerReconciliation));
    }

    private ConsumerMembershipManager mockJoinAndReceiveAssignment(boolean triggerReconciliation,
                                                               ConsumerGroupHeartbeatResponseData.Assignment assignment) {
        ConsumerMembershipManager membershipManager = createMembershipManagerJoiningGroup();
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(assignment, membershipManager.memberId());
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.empty()).thenReturn(Optional.empty());

        membershipManager.onHeartbeatSuccess(heartbeatResponse);

        if (triggerReconciliation) {
            membershipManager.poll(time.milliseconds());
            verify(subscriptionState).assignFromSubscribedAwaitingCallback(anyCollection(), anyCollection());
        } else {
            verify(subscriptionState, never()).assignFromSubscribed(anyCollection());
        }

        clearInvocations(membershipManager, commitRequestManager);
        return membershipManager;
    }

    private ConsumerMembershipManager createMemberInStableState() {
        return createMemberInStableState(null);
    }

    private ConsumerMembershipManager createMemberInStableState(String groupInstanceId) {
        ConsumerMembershipManager membershipManager = createMembershipManagerJoiningGroup(groupInstanceId, null);
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(new Assignment(), membershipManager.memberId());
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.empty());
        membershipManager.onHeartbeatSuccess(heartbeatResponse);
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        membershipManager.poll(time.milliseconds());
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        membershipManager.onHeartbeatRequestGenerated();
        assertEquals(MemberState.STABLE, membershipManager.state());

        clearInvocations(subscriptionState, membershipManager, commitRequestManager);
        return membershipManager;
    }

    private void receiveAssignment(Map<Uuid, SortedSet<Integer>> topicIdPartitionList, ConsumerMembershipManager membershipManager) {
        ConsumerGroupHeartbeatResponseData.Assignment targetAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
            .setTopicPartitions(topicIdPartitionList.entrySet().stream().map(tp ->
                new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                    .setTopicId(tp.getKey())
                    .setPartitions(new ArrayList<>(tp.getValue()))).collect(Collectors.toList()));
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(targetAssignment, membershipManager.memberId());
        membershipManager.onHeartbeatSuccess(heartbeatResponse);
    }

    private void receiveAssignment(Uuid topicId, List<Integer> partitions, ConsumerMembershipManager membershipManager) {
        ConsumerGroupHeartbeatResponseData.Assignment targetAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.singletonList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topicId)
                                .setPartitions(partitions)));
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(targetAssignment, membershipManager.memberId());
        membershipManager.onHeartbeatSuccess(heartbeatResponse);
    }

    private Map<Uuid, SortedSet<Integer>> receiveAssignmentAfterRejoin(List<Integer> partitions,
                                                                       ConsumerMembershipManager membershipManager,
                                                                       Collection<TopicIdPartition> owned) {
        // Get new assignment after rejoining. This should not trigger a reconciliation just
        // yet because there is another one in progress, but should keep the new assignment ready
        // to be reconciled next.
        Uuid topicId = Uuid.randomUuid();
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId, "topic3", owned);

        ConsumerGroupHeartbeatResponseData.Assignment targetAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.singletonList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topicId)
                                .setPartitions(partitions)));
        ConsumerGroupHeartbeatResponse heartbeatResponse =
                createConsumerGroupHeartbeatResponseWithBumpedEpoch(targetAssignment, membershipManager.memberId());
        membershipManager.onHeartbeatSuccess(heartbeatResponse);

        verifyReconciliationNotTriggered(membershipManager);
        Map<Uuid, SortedSet<Integer>> assignmentAfterRejoin = topicIdPartitionsMap(topicId, 5);
        assertEquals(assignmentAfterRejoin, membershipManager.topicPartitionsAwaitingReconciliation());
        return assignmentAfterRejoin;
    }

    private void assertInitialReconciliationDiscardedAfterRejoin(
            ConsumerMembershipManager membershipManager,
            Map<Uuid, SortedSet<Integer>> assignmentAfterRejoin) {
        verify(subscriptionState, never()).markPendingRevocation(any());
        verify(subscriptionState, never()).assignFromSubscribed(anyCollection());
        assertNotEquals(MemberState.ACKNOWLEDGING, membershipManager.state());

        // Assignment received after rejoining should be ready to reconcile on the next
        // reconciliation loop.
        assertEquals(assignmentAfterRejoin, membershipManager.topicPartitionsAwaitingReconciliation());

        // Stale reconciliation should have been aborted and a new one should be triggered on the next poll.
        assertFalse(membershipManager.reconciliationInProgress());
        clearInvocations(membershipManager);
        membershipManager.poll(time.milliseconds());
        verify(membershipManager).markReconciliationInProgress();
    }

    private void receiveEmptyAssignment(ConsumerMembershipManager membershipManager) {
        // New empty assignment received, revoking owned partition.
        ConsumerGroupHeartbeatResponseData.Assignment targetAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.emptyList());
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(targetAssignment, membershipManager.memberId());
        membershipManager.onHeartbeatSuccess(heartbeatResponse);
    }

    /**
     * Fenced member should release assignment, reset epoch to 0, keep member ID, and transition
     * to JOINING to rejoin the group.
     */
    private void testFencedMemberReleasesAssignmentAndTransitionsToJoining(ConsumerMembershipManager membershipManager) {
        mockMemberHasAutoAssignedPartition();

        membershipManager.transitionToFenced();
        assertEquals(0, membershipManager.memberEpoch());
        assertEquals(MemberState.JOINING, membershipManager.state());
    }

    /**
     * Member that intentionally leaves the group (via unsubscribe) should release assignment,
     * reset epoch to -1, keep member ID, and transition to {@link MemberState#LEAVING} to send out a
     * heartbeat with the leave epoch. Once the heartbeat request is sent out, the member should
     * transition to {@link MemberState#UNSUBSCRIBED}
     */
    private void testLeaveGroupReleasesAssignmentAndResetsEpochToSendLeaveGroup(ConsumerMembershipManager membershipManager) {
        mockLeaveGroup();

        CompletableFuture<Void> leaveResult = membershipManager.leaveGroup();
        verify(subscriptionState).unsubscribe();

        assertEquals(MemberState.LEAVING, membershipManager.state());
        assertFalse(leaveResult.isDone(), "Leave group result should not complete until the " +
                "heartbeat request to leave is sent out.");

        assertTransitionToUnsubscribeOnHBSentAndWaitForResponseToCompleteLeave(membershipManager, leaveResult);
        assertEquals(-1, membershipManager.memberEpoch());
        assertTrue(membershipManager.currentAssignment().isNone());
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
    }

    private void mockLeaveGroup() {
        mockMemberHasAutoAssignedPartition();
        doNothing().when(subscriptionState).markPendingRevocation(anySet());
    }

    private ConsumerRebalanceListenerCallbackCompletedEvent mockPrepareLeavingStuckOnUserCallback(
            ConsumerMembershipManager membershipManager,
            ConsumerRebalanceListenerInvoker invoker) {
        String topicName = "topic1";
        TopicPartition ownedPartition = new TopicPartition(topicName, 0);
        mockPrepareLeaving(ownedPartition);
        membershipManager.leaveGroup();
        return performCallback(
            membershipManager,
            invoker,
            ConsumerRebalanceListenerMethodName.ON_PARTITIONS_REVOKED,
            topicPartitions(ownedPartition.topic(), ownedPartition.partition()),
            false
        );
    }

    private void mockPrepareLeaving(TopicPartition ownedPartition) {
        // Start leaving group, blocked waiting for callback to complete.
        CounterConsumerRebalanceListener listener = new CounterConsumerRebalanceListener();
        when(subscriptionState.assignedPartitions()).thenReturn(Collections.singleton(ownedPartition));
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.of(listener));
        doNothing().when(subscriptionState).markPendingRevocation(anySet());
        when(commitRequestManager.autoCommitEnabled()).thenReturn(false);
    }

    private ConsumerRebalanceListenerCallbackCompletedEvent mockFencedMemberStuckOnUserCallback(
            ConsumerMembershipManager membershipManager,
            ConsumerRebalanceListenerInvoker invoker) {
        String topicName = "topic1";
        TopicPartition ownedPartition = new TopicPartition(topicName, 0);

        // Fence member and block waiting for onPartitionsLost callback to complete
        CounterConsumerRebalanceListener listener = new CounterConsumerRebalanceListener();
        when(subscriptionState.assignedPartitions()).thenReturn(Collections.singleton(ownedPartition));
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.of(listener));
        when(commitRequestManager.autoCommitEnabled()).thenReturn(false);
        membershipManager.transitionToFenced();
        return performCallback(
            membershipManager,
            invoker,
            ConsumerRebalanceListenerMethodName.ON_PARTITIONS_LOST,
            topicPartitions(ownedPartition.topic(), ownedPartition.partition()),
            false
        );
    }

    private void testStateUpdateOnFatalFailure(ConsumerMembershipManager membershipManager) {
        String memberId = membershipManager.memberId();
        int lastEpoch = membershipManager.memberEpoch();
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        membershipManager.onHeartbeatFailure(false);
        membershipManager.transitionToFatal();
        assertEquals(MemberState.FATAL, membershipManager.state());
        // Should keep its last member id and epoch.
        assertEquals(memberId, membershipManager.memberId());
        assertEquals(lastEpoch, membershipManager.memberEpoch());
    }

    private ConsumerGroupHeartbeatResponse createConsumerGroupHeartbeatResponse(
            ConsumerGroupHeartbeatResponseData data) {
        return new ConsumerGroupHeartbeatResponse(data);
    }

    private ConsumerGroupHeartbeatResponse createConsumerGroupHeartbeatResponse(
            ConsumerGroupHeartbeatResponseData.Assignment assignment, String memberId) {
        return new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.NONE.code())
                .setMemberId(memberId)
                .setMemberEpoch(MEMBER_EPOCH)
                .setAssignment(assignment));
    }

    /**
     * Create heartbeat response with the given assignment and a bumped epoch (incrementing by 1
     * as default but could be any increment). This will be used to mock when a member
     * receives a heartbeat response to the join request, and the response includes an assignment.
     */
    private ConsumerGroupHeartbeatResponse createConsumerGroupHeartbeatResponseWithBumpedEpoch(
            ConsumerGroupHeartbeatResponseData.Assignment assignment, String memberId) {
        return new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.NONE.code())
                .setMemberId(memberId)
                .setMemberEpoch(MEMBER_EPOCH + 1)
                .setAssignment(assignment));
    }

    private ConsumerGroupHeartbeatResponse createConsumerGroupHeartbeatResponseWithError(Errors error, String memberId) {
        return new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(error.code())
                .setMemberId(memberId)
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

    private ConsumerMembershipManager memberJoinWithAssignment() {
        Uuid topicId = Uuid.randomUuid();
        ConsumerMembershipManager membershipManager = mockJoinAndReceiveAssignment(true);
        membershipManager.onHeartbeatRequestGenerated();
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, "topic"));
        receiveAssignment(topicId, Collections.singletonList(0), membershipManager);
        membershipManager.onHeartbeatRequestGenerated();
        assertFalse(membershipManager.currentAssignment().isNone());
        return membershipManager;
    }

    private void assertMemberIdIsGenerated(String memberId) {
        assertNotNull(memberId, "Member Id should be generated at startup");
        assertFalse(memberId.isEmpty(), "Member Id should be generated at startup");
    }

    /**
     * @return States where the member is not part of the group.
     */
    private static Stream<Arguments> notInGroupStates() {
        return Stream.of(
            Arguments.of(MemberState.UNSUBSCRIBED),
            Arguments.of(MemberState.FENCED),
            Arguments.of(MemberState.FATAL),
            Arguments.of(MemberState.STALE));
    }

    private static class SleepyRebalanceListener implements ConsumerRebalanceListener {
        private long sleepMs;
        private final long sleepDurationMs;
        private final Time time;
        SleepyRebalanceListener(long sleepDurationMs, Time time) {
            this.sleepDurationMs = sleepDurationMs;
            this.time = time;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            sleepMs += sleepDurationMs;
            time.sleep(sleepDurationMs);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            sleepMs += sleepDurationMs;
            time.sleep(sleepDurationMs);
        }

        public void reset() {
            sleepMs = 0;
        }
    }
}
