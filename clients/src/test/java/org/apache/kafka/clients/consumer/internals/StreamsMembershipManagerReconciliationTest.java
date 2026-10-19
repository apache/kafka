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

import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnAllTasksLostCallbackNeededEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksRevokedCallbackNeededEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsTasksAssignedEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class StreamsMembershipManagerReconciliationTest {

    private static final String SUBTOPOLOGY_ID = "subtopology";
    private static final String TOPIC = "topic";
    private static final int TASK_A = 0;
    private static final int TASK_B = 1;
    private static final int TASK_C = 2;
    private static final int TASK_D = 3;

    private final Time time = new MockTime();
    private final Metrics metrics = new Metrics(time);
    private final BlockingQueue<BackgroundEvent> backgroundEvents = new LinkedBlockingQueue<>();
    private final SubscriptionState subscriptionState =
        spy(new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.EARLIEST));

    private StreamsMembershipManager membershipManager;

    @BeforeEach
    public void setup() {
        final StreamsRebalanceData rebalanceData = new StreamsRebalanceData(
            UUID.randomUUID(),
            Optional.empty(),
            Optional.empty(),
            Map.of(SUBTOPOLOGY_ID, new StreamsRebalanceData.Subtopology(
                Set.of(TOPIC), Set.of(), Map.of(), Map.of(), List.of()
            )),
            Map.of(),
            Map::of,
            Map::of
        );
        final BackgroundEventHandler backgroundEventHandler = new BackgroundEventHandler(
            backgroundEvents, time, new AsyncConsumerMetrics(metrics, CONSUMER_METRIC_GROUP_PREFIX)
        );
        membershipManager = new StreamsMembershipManager(
            "group", Optional.empty(), rebalanceData, subscriptionState,
            backgroundEventHandler, new LogContext(), time, metrics
        );
        subscriptionState.subscribe(Set.of(TOPIC));
        membershipManager.onSubscriptionUpdated();
        membershipManager.onConsumerPoll();
        assertEquals(MemberState.JOINING, membershipManager.state());
    }

    @AfterEach
    public void tearDown() {
        metrics.close();
    }

    @Test
    public void testRetrySameTargetAfterAssignmentCallbackFailure() {
        establishAssignment(TASK_A);
        reconcileTo(TASK_A, TASK_B);
        final StreamsTasksAssignedEvent failedAssignment = nextAssignment(TASK_A, TASK_B);
        assertEquals(partitions(TASK_B), failedAssignment.addedPartitions());
        failAssignmentCallback(failedAssignment);
        assertEquals(partitions(TASK_A, TASK_B), subscriptionState.assignedPartitions());

        // Retry on the next network poll, without another heartbeat response.
        membershipManager.poll(time.milliseconds());

        final StreamsTasksAssignedEvent retry = nextAssignment(TASK_A, TASK_B);
        assertTrue(retry.addedPartitions().isEmpty());
        completeAssignment(retry);
        assertReconciledAndFetchable(TASK_A, TASK_B);
    }

    @Test
    public void testReconcileNewTargetAfterAssignmentCallbackFailure() {
        establishAssignment(TASK_A, TASK_B);
        reconcileTo(TASK_B, TASK_C);
        nextRevocation(TASK_A).future().complete(null);
        final StreamsTasksAssignedEvent failedAssignment = nextAssignment(TASK_B, TASK_C);
        failAssignmentCallback(failedAssignment);
        assertEquals(partitions(TASK_B, TASK_C), subscriptionState.assignedPartitions());

        reconcileTo(TASK_C, TASK_D);

        // A has already been removed from the consumer; only B must be revoked.
        nextRevocation(TASK_B).future().complete(null);
        final StreamsTasksAssignedEvent assignment = nextAssignment(TASK_C, TASK_D);
        assertEquals(partitions(TASK_D), assignment.addedPartitions());
        completeAssignment(assignment);
        // C was retained from the failed callback and must become fetchable as well.
        assertReconciledAndFetchable(TASK_C, TASK_D);
    }

    @Test
    public void testRetrySameTargetAfterRevocationCallbackFailure() {
        establishAssignment(TASK_A, TASK_B);
        reconcileTo(TASK_B, TASK_C);
        failRevocationCallback(nextRevocation(TASK_A));
        assertEquals(partitions(TASK_A, TASK_B), subscriptionState.assignedPartitions());

        membershipManager.poll(time.milliseconds());

        final StreamsOnTasksRevokedCallbackNeededEvent retry = nextRevocation(TASK_A);
        assertTrue(backgroundEvents.isEmpty(), "Assignment must wait for successful revocation");
        retry.future().complete(null);
        completeAssignment(nextAssignment(TASK_B, TASK_C));
        assertReconciledAndFetchable(TASK_B, TASK_C);
    }

    @Test
    public void testReconcileNewTargetAfterRevocationCallbackFailure() {
        establishAssignment(TASK_A, TASK_B);
        reconcileTo(TASK_B, TASK_C);
        failRevocationCallback(nextRevocation(TASK_A));
        assertEquals(partitions(TASK_A, TASK_B), subscriptionState.assignedPartitions());

        reconcileTo(TASK_B, TASK_D);

        final StreamsOnTasksRevokedCallbackNeededEvent retry = nextRevocation(TASK_A);
        assertTrue(backgroundEvents.isEmpty(), "Neither the old nor new target may be assigned before revocation");
        retry.future().complete(null);
        completeAssignment(nextAssignment(TASK_B, TASK_D));
        assertReconciledAndFetchable(TASK_B, TASK_D);
    }

    @Test
    public void testDoNotRepeatSuccessfulRevocationAfterAssignmentApplicationFailure() {
        establishAssignment(TASK_A, TASK_B);
        reconcileTo(TASK_B, TASK_C);
        nextRevocation(TASK_A).future().complete(null);
        failAssignmentApplication(nextAssignment(TASK_B, TASK_C));
        assertEquals(partitions(TASK_A, TASK_B), subscriptionState.assignedPartitions());

        membershipManager.poll(time.milliseconds());

        // The next event must be assignment, not another revocation of A.
        final StreamsTasksAssignedEvent retry = nextAssignment(TASK_B, TASK_C);
        assertEquals(partitions(TASK_C), retry.addedPartitions());
        completeAssignment(retry);
        assertReconciledAndFetchable(TASK_B, TASK_C);
    }

    @Test
    public void testRevokeReassignedTaskAfterAssignmentApplicationFailure() {
        establishAssignment(TASK_A, TASK_B);
        reconcileTo(TASK_B, TASK_C);
        nextRevocation(TASK_A).future().complete(null);
        failAssignmentApplication(nextAssignment(TASK_B, TASK_C));
        assertEquals(partitions(TASK_A, TASK_B), subscriptionState.assignedPartitions());

        // A is assigned again while its consumer partition state still exists.
        reconcileTo(TASK_A, TASK_B);
        final StreamsTasksAssignedEvent assignment = nextAssignment(TASK_A, TASK_B);
        assertTrue(assignment.addedPartitions().isEmpty());
        completeAssignment(assignment);
        assertReconciledAndFetchable(TASK_A, TASK_B);

        reconcileTo(TASK_B);

        // The earlier successful revocation must not suppress revocation after reassignment.
        final StreamsOnTasksRevokedCallbackNeededEvent revocation = nextRevocation(TASK_A);
        assertTrue(backgroundEvents.isEmpty(), "Assignment must wait for the new revocation");
        revocation.future().complete(null);
        completeAssignment(nextAssignment(TASK_B));
        assertReconciledAndFetchable(TASK_B);
    }

    @Test
    public void testUseAppliedAssignmentWhenTargetChangesBeforeAssignmentApplication() {
        establishAssignment(TASK_A);
        reconcileTo(TASK_A, TASK_B);
        final StreamsTasksAssignedEvent oldAssignment = nextAssignment(TASK_A, TASK_B);

        // The app thread has not applied A+B when the network thread receives A+C.
        reconcileTo(TASK_A, TASK_C);
        assertTrue(backgroundEvents.isEmpty(), "The ongoing reconciliation must finish first");
        failAssignmentCallback(oldAssignment);
        assertEquals(partitions(TASK_A, TASK_B), subscriptionState.assignedPartitions());

        membershipManager.poll(time.milliseconds());

        nextRevocation(TASK_B).future().complete(null);
        final StreamsTasksAssignedEvent assignment = nextAssignment(TASK_A, TASK_C);
        assertEquals(partitions(TASK_C), assignment.addedPartitions());
        completeAssignment(assignment);
        assertReconciledAndFetchable(TASK_A, TASK_C);
    }

    @Test
    public void testClearPendingRevocationWhenNewTargetRetainsPartition() {
        establishAssignment(TASK_A);
        reconcileTo(TASK_A, TASK_B);
        failAssignmentCallback(nextAssignment(TASK_A, TASK_B));
        reconcileTo(TASK_A);
        failRevocationCallback(nextRevocation(TASK_B));

        // Exercise the client receiving B again while its consumer partition state still exists.
        // Coordinator decisions about when to issue this target are outside this unit test.
        reconcileTo(TASK_A, TASK_B, TASK_C);

        final StreamsTasksAssignedEvent assignment = nextAssignment(TASK_A, TASK_B, TASK_C);
        assertEquals(partitions(TASK_C), assignment.addedPartitions());
        completeAssignment(assignment);
        // Both flags left by B's failed assignment and revocation callbacks must be cleared.
        assertReconciledAndFetchable(TASK_A, TASK_B, TASK_C);
    }

    @Test
    public void testObsoleteAssignmentCallbackDoesNotEnableFetchingAfterFencing() {
        establishAssignment(TASK_A);
        reconcileTo(TASK_A, TASK_B);
        final StreamsTasksAssignedEvent assignment = nextAssignment(TASK_A, TASK_B);
        applyAssignment(assignment);
        subscriptionState.seek(new TopicPartition(TOPIC, TASK_B), 0);

        membershipManager.onFenced();
        final StreamsOnAllTasksLostCallbackNeededEvent lost =
            nextEvent(StreamsOnAllTasksLostCallbackNeededEvent.class);
        assignment.future().complete(null);

        assertEquals(MemberState.FENCED, membershipManager.state());
        assertEquals(partitions(TASK_A, TASK_B), subscriptionState.assignedPartitions());
        for (final TopicPartition partition : partitions(TASK_A, TASK_B)) {
            assertTrue(subscriptionState.hasValidPosition(partition));
            assertFalse(subscriptionState.isFetchable(partition), "Fenced partition became fetchable: " + partition);
        }
        assertTrue(backgroundEvents.isEmpty());

        lost.future().complete(null);
        assertEquals(MemberState.JOINING, membershipManager.state());
        assertTrue(subscriptionState.assignedPartitions().isEmpty());
        establishAssignment(TASK_A, TASK_B);
    }

    @Test
    public void testLeaveGroupReleasesAppliedTasksAfterAssignmentCallbackFailure() {
        establishAssignment(TASK_A, TASK_B);
        reconcileTo(TASK_B, TASK_C);
        nextRevocation(TASK_A).future().complete(null);
        failAssignmentCallback(nextAssignment(TASK_B, TASK_C));

        final CompletableFuture<Void> leave = membershipManager.leaveGroup();

        assertEquals(MemberState.PREPARE_LEAVING, membershipManager.state());
        assertFalse(leave.isDone());
        nextRevocation(TASK_B, TASK_C).future().complete(null);
        assertEquals(MemberState.LEAVING, membershipManager.state());
        assertTrue(subscriptionState.assignedPartitions().isEmpty());
        assertTrue(subscriptionState.subscription().isEmpty());
        assertTrue(backgroundEvents.isEmpty());
        membershipManager.onHeartbeatRequestGenerated();
        membershipManager.onHeartbeatSuccess(heartbeatResponse(LEAVE_GROUP_MEMBER_EPOCH));
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        assertTrue(leave.isDone());
        assertFalse(leave.isCompletedExceptionally());
    }

    @Test
    public void testLeaveGroupDoesNotRepeatSuccessfulRevocationAfterAssignmentApplicationFailure() {
        establishAssignment(TASK_A, TASK_B);
        reconcileTo(TASK_B, TASK_C);
        nextRevocation(TASK_A).future().complete(null);
        failAssignmentApplication(nextAssignment(TASK_B, TASK_C));
        assertEquals(partitions(TASK_A, TASK_B), subscriptionState.assignedPartitions());

        final CompletableFuture<Void> leave = membershipManager.leaveGroup();

        assertEquals(MemberState.PREPARE_LEAVING, membershipManager.state());
        assertFalse(leave.isDone());
        // A remains assigned to the consumer, but its revocation callback already succeeded.
        final StreamsOnTasksRevokedCallbackNeededEvent revocation = nextRevocation(TASK_B);
        assertTrue(backgroundEvents.isEmpty(), "Leaving must not revoke A again");
        revocation.future().complete(null);
        assertEquals(MemberState.LEAVING, membershipManager.state());
        assertTrue(subscriptionState.assignedPartitions().isEmpty());
        assertTrue(subscriptionState.subscription().isEmpty());
        assertTrue(backgroundEvents.isEmpty());
        membershipManager.onHeartbeatRequestGenerated();
        membershipManager.onHeartbeatSuccess(heartbeatResponse(LEAVE_GROUP_MEMBER_EPOCH));
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        assertTrue(leave.isDone());
        assertFalse(leave.isCompletedExceptionally());
    }

    @Test
    public void testRetryAfterSynchronousReconciliationFailure() {
        establishAssignment(TASK_A);
        membershipManager.onHeartbeatSuccess(heartbeatResponse(1, TASK_A, TASK_B));
        final KafkaException failure = new KafkaException("Failed to read consumer assignment");
        doThrow(failure).doCallRealMethod().when(subscriptionState).assignedPartitions();

        assertSame(failure, assertThrows(KafkaException.class, () -> membershipManager.poll(time.milliseconds())));
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertTrue(backgroundEvents.isEmpty());

        membershipManager.poll(time.milliseconds());

        completeAssignment(nextAssignment(TASK_A, TASK_B));
        assertReconciledAndFetchable(TASK_A, TASK_B);
    }

    private void establishAssignment(final int... tasks) {
        reconcileTo(tasks);
        completeAssignment(nextAssignment(tasks));
        assertReconciledAndFetchable(tasks);
    }

    private void reconcileTo(final int... tasks) {
        membershipManager.onHeartbeatSuccess(heartbeatResponse(1, tasks));
        membershipManager.poll(time.milliseconds());
        assertEquals(MemberState.RECONCILING, membershipManager.state());
    }

    private StreamsGroupHeartbeatResponse heartbeatResponse(final int memberEpoch, final int... tasks) {
        return new StreamsGroupHeartbeatResponse(new StreamsGroupHeartbeatResponseData()
            .setMemberId(membershipManager.memberId())
            .setMemberEpoch(memberEpoch)
            .setActiveTasks(List.of(new StreamsGroupHeartbeatResponseData.TaskIds()
                .setSubtopologyId(SUBTOPOLOGY_ID)
                .setPartitions(Arrays.stream(tasks).boxed().collect(Collectors.toList()))))
            .setStandbyTasks(List.of())
            .setWarmupTasks(List.of()));
    }

    private <T extends BackgroundEvent> T nextEvent(final Class<T> eventClass) {
        return assertInstanceOf(eventClass, backgroundEvents.poll());
    }

    private StreamsTasksAssignedEvent nextAssignment(final int... tasks) {
        final StreamsTasksAssignedEvent event = nextEvent(StreamsTasksAssignedEvent.class);
        final Set<StreamsRebalanceData.TaskId> activeTasks = Arrays.stream(tasks)
            .mapToObj(task -> new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID, task))
            .collect(Collectors.toSet());
        assertEquals(new StreamsRebalanceData.Assignment(activeTasks, Set.of(), Set.of(), true), event.assignment());
        assertEquals(partitions(tasks), event.assignedPartitions());
        assertTrue(backgroundEvents.isEmpty(), "Unexpected additional callback event");
        return event;
    }

    private StreamsOnTasksRevokedCallbackNeededEvent nextRevocation(final int... tasks) {
        final StreamsOnTasksRevokedCallbackNeededEvent event = nextEvent(StreamsOnTasksRevokedCallbackNeededEvent.class);
        final Set<StreamsRebalanceData.TaskId> activeTasks = Arrays.stream(tasks)
            .mapToObj(task -> new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID, task))
            .collect(Collectors.toSet());
        assertEquals(activeTasks, event.activeTasksToRevoke());
        return event;
    }

    private void applyAssignment(final StreamsTasksAssignedEvent event) {
        membershipManager.applyAssignment(event.assignedPartitions(), event.addedPartitions());
    }

    private void completeAssignment(final StreamsTasksAssignedEvent event) {
        applyAssignment(event);
        event.future().complete(null);
    }

    private void failAssignmentApplication(final StreamsTasksAssignedEvent event) {
        final KafkaException failure = new KafkaException("Assignment application failed");
        doThrow(failure).doCallRealMethod().when(subscriptionState).assignFromSubscribedAwaitingCallback(
            event.assignedPartitions(), event.addedPartitions()
        );

        // Simulate ApplyAssignmentEvent failing before changing consumer assignment.
        assertSame(failure, assertThrows(KafkaException.class, () -> applyAssignment(event)));
        event.future().completeExceptionally(failure);
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertTrue(backgroundEvents.isEmpty(), "Reconciliation must stop after assignment application failure");
    }

    private void failAssignmentCallback(final StreamsTasksAssignedEvent event) {
        applyAssignment(event);
        event.future().completeExceptionally(new KafkaException("Assignment callback failed"));
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertTrue(backgroundEvents.isEmpty(), "Reconciliation must stop after callback failure");
    }

    private void failRevocationCallback(final StreamsOnTasksRevokedCallbackNeededEvent event) {
        event.future().completeExceptionally(new KafkaException("Revocation callback failed"));
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertTrue(backgroundEvents.isEmpty(), "Assignment must not follow a failed revocation");
    }

    private void assertReconciledAndFetchable(final int... tasks) {
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        final Set<TopicPartition> expectedPartitions = partitions(tasks);
        assertEquals(expectedPartitions, subscriptionState.assignedPartitions());
        for (final TopicPartition partition : expectedPartitions) {
            if (!subscriptionState.hasValidPosition(partition)) {
                subscriptionState.seek(partition, 0);
            }
            assertTrue(subscriptionState.isFetchable(partition), "Partition is not fetchable: " + partition);
        }
        membershipManager.onHeartbeatRequestGenerated();
        assertEquals(MemberState.STABLE, membershipManager.state());
        membershipManager.poll(time.milliseconds());
        assertTrue(backgroundEvents.isEmpty(), "Completed target must not be reconciled again");
    }

    private Set<TopicPartition> partitions(final int... tasks) {
        return Arrays.stream(tasks)
            .mapToObj(task -> new TopicPartition(TOPIC, task))
            .collect(Collectors.toSet());
    }
}
