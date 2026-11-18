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

import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnAllTasksLostCallbackNeededEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksRevokedCallbackNeededEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsTasksAssignedEvent;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StreamsMembershipManagerReconciliationTest {

    private static final String GROUP_ID = "test-group";
    private static final int MEMBER_EPOCH = 1;
    private static final String SUBTOPOLOGY_ID_0 = "subtopology-0";
    private static final String SUBTOPOLOGY_ID_1 = "subtopology-1";
    private static final String TOPIC_0 = "topic-0";
    private static final String TOPIC_1 = "topic-1";
    private static final int PARTITION_0 = 0;
    private static final int PARTITION_1 = 1;
    private static final int PARTITION_2 = 2;

    private final Time time = new MockTime(0);
    private final Metrics metrics = new Metrics(time);

    private StreamsMembershipManager membershipManager;
    private ConsumerMetadata metadata;

    @Mock
    private ConsumerSubscriptionState subscriptionState;

    @Mock
    private BackgroundEventHandler backgroundEventHandler;

    @Mock
    private StreamsRebalanceData streamsRebalanceData;

    @Mock
    private MemberStateListener memberStateListener;

    @Captor
    private ArgumentCaptor<StreamsTasksAssignedEvent> tasksAssignedEventCaptor;
    private int tasksAssignedAddCount = 0;

    @Captor
    private ArgumentCaptor<StreamsOnTasksRevokedCallbackNeededEvent> onTasksRevokedCallbackNeededEventCaptor;

    @Captor
    private ArgumentCaptor<StreamsOnAllTasksLostCallbackNeededEvent> onAllTasksLostCallbackNeededEventCaptor;

    @BeforeEach
    public void setup() {
        metadata = mock(ConsumerMetadata.class);
        membershipManager = new StreamsMembershipManager(
            GROUP_ID,
            Optional.empty(),
            streamsRebalanceData, subscriptionState, backgroundEventHandler,
            new LogContext("test"),
            time,
            metrics,
            metadata
        );
        membershipManager.registerStateListener(memberStateListener);
        verifyInStateUnsubscribed(membershipManager);
    }

    @AfterEach
    public void tearDown() {
        metrics.close();
    }

    @Test
    public void testReconcilingActiveTaskWithDifferentSourceTopicPartitionCounts() {
        when(streamsRebalanceData.subtopologies()).thenReturn(
            Map.of(
                SUBTOPOLOGY_ID_0,
                new StreamsRebalanceData.Subtopology(
                    Set.of(TOPIC_0, TOPIC_1),
                    Set.of(),
                    Map.of(),
                    Map.of(),
                    List.of()
                )
            )
        );
        when(metadata.fetch()).thenReturn(
            TestUtils.clusterWith(1, Map.of(TOPIC_0, 1, TOPIC_1, 3))
        );
        when(metadata.updateVersion()).thenReturn(1);
        joining();

        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_2)));
        final Set<StreamsRebalanceData.TaskId> activeTasks = Set.of(new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_2));
        final Set<TopicPartition> expectedFullPartitionsToAssign = Set.of(new TopicPartition(TOPIC_1, PARTITION_2));
        final StreamsTasksAssignedEvent onTasksAssignedCallbackExecuted =
            verifyOnTasksAssignedCallbackNeededEventAddedToBackgroundEventHandler(activeTasks, Set.of(), Set.of(), expectedFullPartitionsToAssign);
        assertEquals(expectedFullPartitionsToAssign, onTasksAssignedCallbackExecuted.assignedPartitions());
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedFullPartitionsToAssign);

        completeAssignment(onTasksAssignedCallbackExecuted);

        verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedFullPartitionsToAssign, expectedFullPartitionsToAssign);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();

        membershipManager.onHeartbeatRequestGenerated();

        verifyInStateStable(membershipManager);
    }

    @Test
    public void testReconcilingActiveTaskWithNoMatchingInputPartitions() {
        setupStreamsRebalanceDataWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        when(metadata.fetch()).thenReturn(
            TestUtils.clusterWith(1, Map.of(TOPIC_0, 1))
        );
        when(metadata.updateVersion()).thenReturn(1);
        joining();

        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_2)));
        final StreamsTasksAssignedEvent onTasksAssignedCallbackExecuted =
            verifyOnTasksAssignedCallbackNeededEventAddedToBackgroundEventHandler(Set.of(), Set.of(), Set.of(), Set.of());
        assertEquals(Set.of(), onTasksAssignedCallbackExecuted.assignedPartitions());
        assertEquals(Set.of(), onTasksAssignedCallbackExecuted.addedPartitions());
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(Set.of(), Set.of());

        completeAssignment(onTasksAssignedCallbackExecuted);

        verify(subscriptionState).assignFromSubscribedAwaitingCallback(Set.of(), Set.of());
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(Set.of());
        verify(metadata).requestUpdate(true);
        verifyThatNoTasksHaveBeenRevoked();

        membershipManager.onHeartbeatRequestGenerated();

        verifyInStateReconciling(membershipManager);
        assertTrue(membershipManager.isPartialAssignmentReconciled());

        when(metadata.fetch()).thenReturn(
            TestUtils.clusterWith(1, Map.of(TOPIC_0, 3))
        );
        when(metadata.updateVersion()).thenReturn(3);

        membershipManager.poll(time.milliseconds());

        final Set<StreamsRebalanceData.TaskId> activeTasks =
            Set.of(new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_2));
        final Set<TopicPartition> expectedFullPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_2));
        final StreamsTasksAssignedEvent resolvedAssignment =
            verifyOnTasksAssignedCallbackNeededEventAddedToBackgroundEventHandler(activeTasks, Set.of(), Set.of(), expectedFullPartitionsToAssign);
        assertEquals(expectedFullPartitionsToAssign, resolvedAssignment.assignedPartitions());
        assertEquals(expectedFullPartitionsToAssign, resolvedAssignment.addedPartitions());
        verifyInStateReconciling(membershipManager);
        assertTrue(membershipManager.isReconciliationInProgress());

        completeAssignment(resolvedAssignment);

        verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedFullPartitionsToAssign, expectedFullPartitionsToAssign);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
        assertFalse(membershipManager.isReconciliationInProgress());
        assertFalse(membershipManager.isPartialAssignmentReconciled());
        verifyThatNoTasksHaveBeenRevoked();

        membershipManager.onHeartbeatRequestGenerated();

        verifyInStateStable(membershipManager);
    }

    @Test
    public void testReconcilingActiveTaskAfterMissingMetadataIsUpdated() {
        setupStreamsRebalanceDataWithOneSubtopologyTwoSourceTopics(SUBTOPOLOGY_ID_0, TOPIC_0, TOPIC_1);
        when(metadata.fetch()).thenReturn(Cluster.empty());
        when(metadata.updateVersion()).thenReturn(1);

        joining();

        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_2)));

        final StreamsTasksAssignedEvent partialAssignment =
            verifyOnTasksAssignedCallbackNeededEventAddedToBackgroundEventHandler(Set.of(), Set.of(), Set.of(), Set.of());
        assertEquals(Set.of(), partialAssignment.assignedPartitions());
        assertTrue(membershipManager.isReconciliationInProgress());
        assertFalse(membershipManager.isPartialAssignmentReconciled());

        acknowledging(partialAssignment);
        verify(metadata).requestUpdate(true);

        assertFalse(membershipManager.isReconciliationInProgress());
        assertTrue(membershipManager.isPartialAssignmentReconciled());

        membershipManager.onHeartbeatRequestGenerated();

        verifyInStateReconciling(membershipManager);

        membershipManager.poll(time.milliseconds());

        verifyNoAdditionalTasksAssigned();
        verify(metadata, times(2)).requestUpdate(true);
        verifyInStateReconciling(membershipManager);

        when(metadata.fetch()).thenReturn(
            TestUtils.clusterWith(1, Map.of(TOPIC_0, 1, TOPIC_1, 3))
        );
        when(metadata.updateVersion()).thenReturn(2);

        membershipManager.poll(time.milliseconds());

        final Set<TopicPartition> expectedFullPartitionsToAssign = Set.of(new TopicPartition(TOPIC_1, PARTITION_2));
        final Set<StreamsRebalanceData.TaskId> activeTasks = Set.of(new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_2));
        final StreamsTasksAssignedEvent onTasksAssignedCallbackExecuted =
            verifyOnTasksAssignedCallbackNeededEventAddedToBackgroundEventHandler(activeTasks, Set.of(), Set.of(), expectedFullPartitionsToAssign);
        assertEquals(expectedFullPartitionsToAssign, onTasksAssignedCallbackExecuted.assignedPartitions());
        assertEquals(expectedFullPartitionsToAssign, onTasksAssignedCallbackExecuted.addedPartitions());
        completeAssignment(onTasksAssignedCallbackExecuted);
        verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedFullPartitionsToAssign, expectedFullPartitionsToAssign);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();

        membershipManager.onHeartbeatRequestGenerated();

        verifyInStateStable(membershipManager);
    }

    @Test
    public void testReconcilingActiveTaskWaitsForAllSourceTopicPartitionCounts() {
        setupStreamsRebalanceDataWithOneSubtopologyTwoSourceTopics(SUBTOPOLOGY_ID_0, TOPIC_0, TOPIC_1);
        when(metadata.fetch()).thenReturn(
            TestUtils.clusterWith(1, Map.of(TOPIC_1, 3))
        );
        when(metadata.updateVersion()).thenReturn(1);
        joining();

        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_2)));

        final StreamsTasksAssignedEvent partialAssignment =
            verifyOnTasksAssignedCallbackNeededEventAddedToBackgroundEventHandler(Set.of(), Set.of(), Set.of(), Set.of());
        assertEquals(Set.of(), partialAssignment.assignedPartitions());

        assertTrue(membershipManager.isReconciliationInProgress());
        assertFalse(membershipManager.isPartialAssignmentReconciled());

        acknowledging(partialAssignment);

        assertFalse(membershipManager.isReconciliationInProgress());
        assertTrue(membershipManager.isPartialAssignmentReconciled());

        verify(metadata).requestUpdate(true);
        membershipManager.onHeartbeatRequestGenerated();
        verifyInStateReconciling(membershipManager);

        when(metadata.fetch()).thenReturn(
            TestUtils.clusterWith(1, Map.of(TOPIC_0, 1, TOPIC_1, 3))
        );
        when(metadata.updateVersion()).thenReturn(2);

        membershipManager.poll(time.milliseconds());

        final Set<TopicPartition> expectedFullPartitionsToAssign = Set.of(new TopicPartition(TOPIC_1, PARTITION_2));
        final Set<StreamsRebalanceData.TaskId> activeTasks = Set.of(new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_2));
        final StreamsTasksAssignedEvent onTasksAssignedCallbackExecuted =
            verifyOnTasksAssignedCallbackNeededEventAddedToBackgroundEventHandler(activeTasks, Set.of(), Set.of(), expectedFullPartitionsToAssign);
        assertEquals(expectedFullPartitionsToAssign, onTasksAssignedCallbackExecuted.assignedPartitions());

        acknowledging(onTasksAssignedCallbackExecuted);

        verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedFullPartitionsToAssign, expectedFullPartitionsToAssign);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();

        membershipManager.onHeartbeatRequestGenerated();

        verifyInStateStable(membershipManager);
    }

    @Test
    public void testPartialReconciliationIncludesPreviouslyAssignedActiveTasks() {
        setupStreamsReabalanceDataWithTwoSubtopologies(SUBTOPOLOGY_ID_0, TOPIC_0, SUBTOPOLOGY_ID_1, TOPIC_1);
        when(metadata.fetch()).thenReturn(
            TestUtils.clusterWith(1, Map.of(TOPIC_0, 1))
        );
        when(metadata.updateVersion()).thenReturn(1);
        final StreamsRebalanceData.TaskId previouslyAssignedTask = new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0);
        final StreamsRebalanceData.TaskId newlyAssignedTask = new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_1, PARTITION_0);
        final Set<TopicPartition> previouslyAssignedPartitions = Set.of(new TopicPartition(TOPIC_0, PARTITION_0));
        joining();

        reconcile(makeHeartbeatResponseWithActiveTasks(
            SUBTOPOLOGY_ID_0, List.of(PARTITION_0),
            SUBTOPOLOGY_ID_1, List.of(PARTITION_0)
        ));

        final StreamsTasksAssignedEvent partialAssignment =
            verifyOnTasksAssignedCallbackNeededEventAddedToBackgroundEventHandler(Set.of(previouslyAssignedTask), Set.of(), Set.of(), previouslyAssignedPartitions);
        assertEquals(previouslyAssignedPartitions, partialAssignment.assignedPartitions());

        assertTrue(membershipManager.isReconciliationInProgress());
        assertFalse(membershipManager.isPartialAssignmentReconciled());

        acknowledging(partialAssignment);

        assertFalse(membershipManager.isReconciliationInProgress());
        assertTrue(membershipManager.isPartialAssignmentReconciled());

        when(subscriptionState.assignedPartitions()).thenReturn(previouslyAssignedPartitions);
        membershipManager.onHeartbeatRequestGenerated();
        verifyInStateReconciling(membershipManager);

        when(metadata.fetch()).thenReturn(
            TestUtils.clusterWith(1, Map.of(TOPIC_0, 1, TOPIC_1, 1))
        );
        when(metadata.updateVersion()).thenReturn(2);

        membershipManager.poll(time.milliseconds());

        final Set<TopicPartition> expectedFullPartitionsToAssign = Set.of(
            new TopicPartition(TOPIC_0, PARTITION_0),
            new TopicPartition(TOPIC_1, PARTITION_0)
        );
        final Set<TopicPartition> expectedNewPartitionsToAssign = Set.of(new TopicPartition(TOPIC_1, PARTITION_0));
        final StreamsTasksAssignedEvent onTasksAssignedCallbackExecuted =
            verifyOnTasksAssignedCallbackNeededEventAddedToBackgroundEventHandler(
                Set.of(previouslyAssignedTask, newlyAssignedTask), Set.of(), Set.of(), expectedFullPartitionsToAssign
            );
        assertEquals(expectedFullPartitionsToAssign, onTasksAssignedCallbackExecuted.assignedPartitions());
        assertEquals(expectedNewPartitionsToAssign, onTasksAssignedCallbackExecuted.addedPartitions());

        acknowledging(onTasksAssignedCallbackExecuted);

        verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();

        membershipManager.onHeartbeatRequestGenerated();

        verifyInStateStable(membershipManager);
    }

    @Test
    public void testPartialReconciliationUsesCachedPartitionCountWhenTopicMetadataIsMissing() {
        setupStreamsReabalanceDataWithTwoSubtopologies(SUBTOPOLOGY_ID_0, TOPIC_0, SUBTOPOLOGY_ID_1, TOPIC_1);
        when(metadata.fetch()).thenReturn(
            TestUtils.clusterWith(1, Map.of(TOPIC_0, 3))
        );
        when(metadata.updateVersion()).thenReturn(1);
        final StreamsRebalanceData.TaskId previouslyAssignedTask = new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_2);
        final StreamsRebalanceData.TaskId newlyAssignedTask = new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_1, PARTITION_0);
        final Set<TopicPartition> previouslyAssignedPartitions = Set.of(new TopicPartition(TOPIC_0, PARTITION_2));
        joining();

        reconcile(makeHeartbeatResponseWithActiveTasks(
            SUBTOPOLOGY_ID_0, List.of(PARTITION_2),
            SUBTOPOLOGY_ID_1, List.of(PARTITION_0)
        ));

        final StreamsTasksAssignedEvent partialAssignment =
            verifyOnTasksAssignedCallbackNeededEventAddedToBackgroundEventHandler(Set.of(previouslyAssignedTask), Set.of(), Set.of(), previouslyAssignedPartitions);
        assertEquals(previouslyAssignedPartitions, partialAssignment.assignedPartitions());

        assertTrue(membershipManager.isReconciliationInProgress());
        assertFalse(membershipManager.isPartialAssignmentReconciled());

        acknowledging(partialAssignment);

        assertFalse(membershipManager.isReconciliationInProgress());
        assertTrue(membershipManager.isPartialAssignmentReconciled());
        verify(subscriptionState).assignFromSubscribedAwaitingCallback(previouslyAssignedPartitions, previouslyAssignedPartitions);

        when(subscriptionState.assignedPartitions()).thenReturn(previouslyAssignedPartitions);
        membershipManager.onHeartbeatRequestGenerated();
        verifyInStateReconciling(membershipManager);

        // TOPIC_0 is missing from the snapshot, so retain its cached partition count of 3.
        final Map<String, Integer> updatedPartitionCounts = Map.of(TOPIC_1, 1);
        when(metadata.fetch()).thenReturn(TestUtils.clusterWith(1, updatedPartitionCounts));
        when(metadata.updateVersion()).thenReturn(2);

        membershipManager.poll(time.milliseconds());

        final Set<TopicPartition> expectedFullPartitionsToAssign = Set.of(
            new TopicPartition(TOPIC_0, PARTITION_2),
            new TopicPartition(TOPIC_1, PARTITION_0)
        );
        final Set<TopicPartition> expectedNewPartitionsToAssign = Set.of(new TopicPartition(TOPIC_1, PARTITION_0));
        final StreamsTasksAssignedEvent onTasksAssignedCallbackExecuted =
            verifyOnTasksAssignedCallbackNeededEventAddedToBackgroundEventHandler(
                Set.of(previouslyAssignedTask, newlyAssignedTask), Set.of(), Set.of(), expectedFullPartitionsToAssign
            );
        assertEquals(expectedFullPartitionsToAssign, onTasksAssignedCallbackExecuted.assignedPartitions());
        assertEquals(expectedNewPartitionsToAssign, onTasksAssignedCallbackExecuted.addedPartitions());

        acknowledging(onTasksAssignedCallbackExecuted);

        verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();

        membershipManager.onHeartbeatRequestGenerated();

        verifyInStateStable(membershipManager);
    }

    @Test
    public void testPartialReconciliationKeepsHigherCachedPartitionCount() {
        setupStreamsReabalanceDataWithTwoSubtopologies(SUBTOPOLOGY_ID_0, TOPIC_0, SUBTOPOLOGY_ID_1, TOPIC_1);
        when(metadata.fetch()).thenReturn(
            TestUtils.clusterWith(1, Map.of(TOPIC_0, 3))
        );
        when(metadata.updateVersion()).thenReturn(1);
        final StreamsRebalanceData.TaskId previouslyAssignedTask = new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_2);
        final StreamsRebalanceData.TaskId newlyAssignedTask = new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_1, PARTITION_0);
        final Set<TopicPartition> previouslyAssignedPartitions = Set.of(new TopicPartition(TOPIC_0, PARTITION_2));
        joining();

        reconcile(makeHeartbeatResponseWithActiveTasks(
            SUBTOPOLOGY_ID_0, List.of(PARTITION_2),
            SUBTOPOLOGY_ID_1, List.of(PARTITION_0)
        ));

        final StreamsTasksAssignedEvent partialAssignment =
            verifyOnTasksAssignedCallbackNeededEventAddedToBackgroundEventHandler(Set.of(previouslyAssignedTask), Set.of(), Set.of(), previouslyAssignedPartitions);
        assertEquals(previouslyAssignedPartitions, partialAssignment.assignedPartitions());

        assertTrue(membershipManager.isReconciliationInProgress());
        assertFalse(membershipManager.isPartialAssignmentReconciled());

        acknowledging(partialAssignment);

        verify(subscriptionState).assignFromSubscribedAwaitingCallback(previouslyAssignedPartitions, previouslyAssignedPartitions);
        assertFalse(membershipManager.isReconciliationInProgress());
        assertTrue(membershipManager.isPartialAssignmentReconciled());

        when(subscriptionState.assignedPartitions()).thenReturn(previouslyAssignedPartitions);
        membershipManager.onHeartbeatRequestGenerated();
        verifyInStateReconciling(membershipManager);

        // The snapshot reports 1 partition for TOPIC_0, but its cached count is 3.
        final Map<String, Integer> updatedPartitionCounts = Map.of(TOPIC_0, 1, TOPIC_1, 1);
        when(metadata.fetch()).thenReturn(TestUtils.clusterWith(1, updatedPartitionCounts));
        when(metadata.updateVersion()).thenReturn(2);

        membershipManager.poll(time.milliseconds());

        final Set<TopicPartition> expectedFullPartitionsToAssign = Set.of(
            new TopicPartition(TOPIC_0, PARTITION_2),
            new TopicPartition(TOPIC_1, PARTITION_0)
        );
        final Set<TopicPartition> expectedNewPartitionsToAssign = Set.of(new TopicPartition(TOPIC_1, PARTITION_0));
        final StreamsTasksAssignedEvent onTasksAssignedCallbackExecuted =
            verifyOnTasksAssignedCallbackNeededEventAddedToBackgroundEventHandler(
                Set.of(previouslyAssignedTask, newlyAssignedTask), Set.of(), Set.of(), expectedFullPartitionsToAssign
            );
        assertEquals(expectedFullPartitionsToAssign, onTasksAssignedCallbackExecuted.assignedPartitions());
        assertEquals(expectedNewPartitionsToAssign, onTasksAssignedCallbackExecuted.addedPartitions());
        acknowledging(onTasksAssignedCallbackExecuted);
        verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();

        membershipManager.onHeartbeatRequestGenerated();

        verifyInStateStable(membershipManager);
    }

    @Test
    public void testAssignmentCallbackSuccessAfterFencingDoesEnablePartitions() {
        setupStreamsRebalanceDataWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        when(metadata.fetch()).thenReturn(
            TestUtils.clusterWith(1, Map.of(TOPIC_0, 2))
        );
        when(metadata.updateVersion()).thenReturn(1);
        final Set<StreamsRebalanceData.TaskId> activeTasks = Set.of(new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0));
        final Set<StreamsRebalanceData.TaskId> standbyTasks = Set.of(new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1));
        final Set<TopicPartition> assignedPartitions = Set.of(new TopicPartition(TOPIC_0, PARTITION_0));
        joining();

        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        final StreamsTasksAssignedEvent initialAssignment =
            verifyOnTasksAssignedCallbackNeededEventAddedToBackgroundEventHandler(activeTasks, Set.of(), Set.of(), assignedPartitions);
        acknowledging(initialAssignment);
        when(subscriptionState.assignedPartitions()).thenReturn(assignedPartitions);
        verify(subscriptionState).enablePartitionsAwaitingCallback(assignedPartitions);
        verify(subscriptionState, times(1)).enablePartitionsAwaitingCallback(any());
        verify(subscriptionState, never()).assignFromSubscribed(Set.of());

        membershipManager.onHeartbeatRequestGenerated();
        verifyInStateStable(membershipManager);

        reconcile(makeHeartbeatResponse(
            List.of(taskIds(SUBTOPOLOGY_ID_0, List.of(PARTITION_0))),
            List.of(taskIds(SUBTOPOLOGY_ID_0, List.of(PARTITION_1))),
            List.of()
        ));

        final StreamsTasksAssignedEvent pendingAssignment =
            verifyOnTasksAssignedCallbackNeededEventAddedToBackgroundEventHandler(activeTasks, standbyTasks, Set.of(), assignedPartitions);
        membershipManager.applyAssignment(pendingAssignment.assignedPartitions(), pendingAssignment.addedPartitions());

        membershipManager.onFenced();

        final CompletableFuture<Void> onAllTasksLostCallbackExecuted =
            verifyOnAllTasksLostCallbackNeededEventAddedToBackgroundEventHandler();
        verifyInStateFenced(membershipManager);
        verify(subscriptionState).markPendingRevocation(assignedPartitions);

        pendingAssignment.future().complete(null);

        assertFalse(onAllTasksLostCallbackExecuted.isDone());
        verify(subscriptionState, times(2)).enablePartitionsAwaitingCallback(any());
        verify(subscriptionState, never()).assignFromSubscribed(Set.of());
        verifyInStateFenced(membershipManager);

        onAllTasksLostCallbackExecuted.complete(null);

        verify(subscriptionState).assignFromSubscribed(Set.of());
        verifyInStateJoining(membershipManager);
    }

    @Test
    public void testSameTasksAndMemberEpochDoNotTriggerReconciliationAfterMetadataUpdate() {
        setupStreamsRebalanceDataWithOneSubtopologyTwoSourceTopics(SUBTOPOLOGY_ID_0, TOPIC_0, TOPIC_1);
        when(metadata.fetch()).thenReturn(
            TestUtils.clusterWith(1, Map.of(TOPIC_0, 1, TOPIC_1, 3))
        );
        when(metadata.updateVersion()).thenReturn(1);
        final Set<StreamsRebalanceData.TaskId> activeTasks =
            Set.of(new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_2));
        final Set<TopicPartition> initialPartitions = Set.of(new TopicPartition(TOPIC_1, PARTITION_2));
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_2)));
        final StreamsTasksAssignedEvent initialAssignment =
            verifyOnTasksAssignedCallbackNeededEventAddedToBackgroundEventHandler(activeTasks, Set.of(), Set.of(), initialPartitions);
        assertEquals(initialPartitions, initialAssignment.assignedPartitions());
        acknowledging(initialAssignment);
        verify(subscriptionState).assignFromSubscribedAwaitingCallback(initialPartitions, initialPartitions);
        membershipManager.onHeartbeatRequestGenerated();
        verifyInStateStable(membershipManager);

        // The updated metadata should not be read unless a new reconciliation starts.
        lenient().when(metadata.fetch()).thenReturn(
            TestUtils.clusterWith(1, Map.of(TOPIC_0, 3, TOPIC_1, 3))
        );
        lenient().when(metadata.updateVersion()).thenReturn(2);

        membershipManager.poll(time.milliseconds());

        verifyNoAdditionalTasksAssigned();
        verifyInStateStable(membershipManager);
        verify(subscriptionState, times(1)).assignFromSubscribedAwaitingCallback(any(), any());

        membershipManager.onHeartbeatSuccess(
            makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_2), MEMBER_EPOCH)
        );
        membershipManager.poll(time.milliseconds());

        assertEquals(MEMBER_EPOCH, membershipManager.memberEpoch());
        verifyNoAdditionalTasksAssigned();
        verifyInStateStable(membershipManager);
        verify(subscriptionState, times(1)).assignFromSubscribedAwaitingCallback(any(), any());
        verify(backgroundEventHandler, never()).add(any(StreamsOnTasksRevokedCallbackNeededEvent.class));
    }

    @Test
    public void testSameTasksWithHigherMemberEpochDoNotTriggerReconciliationAfterMetadataUpdate() {
        setupStreamsRebalanceDataWithOneSubtopologyTwoSourceTopics(SUBTOPOLOGY_ID_0, TOPIC_0, TOPIC_1);
        when(metadata.fetch()).thenReturn(
            TestUtils.clusterWith(1, Map.of(TOPIC_0, 1, TOPIC_1, 3))
        );
        when(metadata.updateVersion()).thenReturn(1);
        final Set<StreamsRebalanceData.TaskId> activeTasks =
            Set.of(new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_2));
        final Set<TopicPartition> initialPartitions = Set.of(new TopicPartition(TOPIC_1, PARTITION_2));
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_2)));
        final StreamsTasksAssignedEvent initialAssignment =
            verifyOnTasksAssignedCallbackNeededEventAddedToBackgroundEventHandler(activeTasks, Set.of(), Set.of(), initialPartitions);
        assertEquals(initialPartitions, initialAssignment.assignedPartitions());
        acknowledging(initialAssignment);
        verify(subscriptionState).assignFromSubscribedAwaitingCallback(initialPartitions, initialPartitions);
        membershipManager.onHeartbeatRequestGenerated();
        verifyInStateStable(membershipManager);

        // The updated metadata should not be read unless a new reconciliation starts.
        lenient().when(metadata.fetch()).thenReturn(
            TestUtils.clusterWith(1, Map.of(TOPIC_0, 3, TOPIC_1, 3))
        );
        lenient().when(metadata.updateVersion()).thenReturn(2);

        membershipManager.poll(time.milliseconds());

        verifyNoAdditionalTasksAssigned();
        verifyInStateStable(membershipManager);
        verify(subscriptionState, times(1)).assignFromSubscribedAwaitingCallback(any(), any());

        membershipManager.onHeartbeatSuccess(
            makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_2), MEMBER_EPOCH + 1)
        );
        membershipManager.poll(time.milliseconds());

        assertEquals(MEMBER_EPOCH + 1, membershipManager.memberEpoch());
        verifyNoAdditionalTasksAssigned();
        verifyInStateStable(membershipManager);
        verify(subscriptionState, times(1)).assignFromSubscribedAwaitingCallback(any(), any());
        verify(backgroundEventHandler, never()).add(any(StreamsOnTasksRevokedCallbackNeededEvent.class));
    }

    @Test
    public void testAdditionalTaskTriggersReconciliationWithUpdatedMetadata() {
        setupStreamsRebalanceDataWithOneSubtopologyTwoSourceTopics(SUBTOPOLOGY_ID_0, TOPIC_0, TOPIC_1);
        when(metadata.fetch()).thenReturn(
            TestUtils.clusterWith(1, Map.of(TOPIC_0, 1, TOPIC_1, 3))
        );
        when(metadata.updateVersion()).thenReturn(1);
        final Set<StreamsRebalanceData.TaskId> activeTasks =
            Set.of(new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_2));
        final Set<TopicPartition> initialPartitions = Set.of(new TopicPartition(TOPIC_1, PARTITION_2));
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_2)));
        final StreamsTasksAssignedEvent initialAssignment =
            verifyOnTasksAssignedCallbackNeededEventAddedToBackgroundEventHandler(activeTasks, Set.of(), Set.of(), initialPartitions);
        assertEquals(initialPartitions, initialAssignment.assignedPartitions());
        acknowledging(initialAssignment);
        when(subscriptionState.assignedPartitions()).thenReturn(initialPartitions);
        verify(subscriptionState).assignFromSubscribedAwaitingCallback(initialPartitions, initialPartitions);
        membershipManager.onHeartbeatRequestGenerated();
        verifyInStateStable(membershipManager);

        // The updated metadata should not be read unless a new reconciliation starts.
        when(metadata.fetch()).thenReturn(
            TestUtils.clusterWith(1, Map.of(TOPIC_0, 3, TOPIC_1, 3))
        );
        when(metadata.updateVersion()).thenReturn(2);

        membershipManager.poll(time.milliseconds());

        verifyNoAdditionalTasksAssigned();
        verifyInStateStable(membershipManager);
        verify(subscriptionState, times(1)).assignFromSubscribedAwaitingCallback(any(), any());

        membershipManager.onHeartbeatSuccess(
            makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0, PARTITION_2), MEMBER_EPOCH + 1)
        );
        membershipManager.poll(time.milliseconds());

        assertEquals(MEMBER_EPOCH + 1, membershipManager.memberEpoch());
        verifyInStateReconciling(membershipManager);
        final Set<StreamsRebalanceData.TaskId> expectedActiveTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0),
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_2)
        );
        final Set<TopicPartition> expectedFullPartitionsToAssign = Set.of(
            new TopicPartition(TOPIC_0, PARTITION_0),
            new TopicPartition(TOPIC_1, PARTITION_0),
            new TopicPartition(TOPIC_0, PARTITION_2),
            new TopicPartition(TOPIC_1, PARTITION_2)
        );
        final Set<TopicPartition> expectedNewPartitionsToAssign = Set.of(
            new TopicPartition(TOPIC_0, PARTITION_0),
            new TopicPartition(TOPIC_1, PARTITION_0),
            new TopicPartition(TOPIC_0, PARTITION_2)
        );
        final StreamsTasksAssignedEvent onTasksAssignedCallbackExecuted =
            verifyOnTasksAssignedCallbackNeededEventAddedToBackgroundEventHandler(expectedActiveTasks, Set.of(), Set.of(), expectedFullPartitionsToAssign);
        assertEquals(expectedFullPartitionsToAssign, onTasksAssignedCallbackExecuted.assignedPartitions());
        assertEquals(expectedNewPartitionsToAssign, onTasksAssignedCallbackExecuted.addedPartitions());
        verify(subscriptionState, times(1)).assignFromSubscribedAwaitingCallback(any(), any());

        acknowledging(onTasksAssignedCallbackExecuted);

        verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        verify(subscriptionState, times(2)).assignFromSubscribedAwaitingCallback(any(), any());
        verify(subscriptionState).enablePartitionsAwaitingCallback(expectedFullPartitionsToAssign);
        verify(backgroundEventHandler, never()).add(any(StreamsOnTasksRevokedCallbackNeededEvent.class));
        membershipManager.onHeartbeatRequestGenerated();
        verifyInStateStable(membershipManager);
    }

    @Test
    public void testReconcilingNewTargetAfterPreviousAssignmentCallbackCompletes() {
        setupStreamsRebalanceDataWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        when(metadata.fetch()).thenReturn(
            TestUtils.clusterWith(1, Map.of(TOPIC_0, 2))
        );
        when(metadata.updateVersion()).thenReturn(1);
        final Set<StreamsRebalanceData.TaskId> initialActiveTasks =
            Set.of(new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0));
        final Set<StreamsRebalanceData.TaskId> nextActiveTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0),
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1)
        );
        final Set<TopicPartition> initialPartitions = Set.of(new TopicPartition(TOPIC_0, PARTITION_0));
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        final StreamsTasksAssignedEvent initialAssignment =
            verifyOnTasksAssignedCallbackNeededEventAddedToBackgroundEventHandler(initialActiveTasks, Set.of(), Set.of(), initialPartitions);
        membershipManager.applyAssignment(initialAssignment.assignedPartitions(), initialAssignment.addedPartitions());
        when(subscriptionState.assignedPartitions()).thenReturn(initialPartitions);

        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0, PARTITION_1)));

        verifyNoAdditionalTasksAssigned();
        assertFalse(initialAssignment.future().isDone());

        initialAssignment.future().complete(null);

        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(initialPartitions);
        membershipManager.onHeartbeatRequestGenerated();
        verifyInStateReconciling(membershipManager);

        membershipManager.poll(time.milliseconds());

        final Set<TopicPartition> expectedFullPartitionsToAssign = Set.of(
            new TopicPartition(TOPIC_0, PARTITION_0),
            new TopicPartition(TOPIC_0, PARTITION_1)
        );
        final Set<TopicPartition> expectedNewPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_1));
        final StreamsTasksAssignedEvent onTasksAssignedCallbackExecuted =
            verifyOnTasksAssignedCallbackNeededEventAddedToBackgroundEventHandler(nextActiveTasks, Set.of(), Set.of(), expectedFullPartitionsToAssign);
        assertEquals(expectedFullPartitionsToAssign, onTasksAssignedCallbackExecuted.assignedPartitions());
        assertEquals(expectedNewPartitionsToAssign, onTasksAssignedCallbackExecuted.addedPartitions());
        completeAssignment(onTasksAssignedCallbackExecuted);
        verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();

        membershipManager.onHeartbeatRequestGenerated();

        verifyInStateStable(membershipManager);
    }

    private void setupStreamsRebalanceDataWithOneSubtopologyTwoSourceTopics(final String subtopologyId,
                                                                            final String sourceTopic,
                                                                            final String otherSourceTopic) {
        when(streamsRebalanceData.subtopologies()).thenReturn(
            Map.of(
                subtopologyId,
                new StreamsRebalanceData.Subtopology(
                    Set.of(sourceTopic, otherSourceTopic),
                    Set.of(),
                    Map.of(),
                    Map.of(),
                    List.of()
                )
            )
        );
    }

    private void verifyThatNoTasksHaveBeenRevoked() {
        verify(backgroundEventHandler, never()).add(any(StreamsOnTasksRevokedCallbackNeededEvent.class));
        verify(subscriptionState, never()).markPendingRevocation(any());
    }

    private void verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(Set<TopicPartition> expectedAllPartitionsToAssign,
                                                                              Set<TopicPartition> expectedNewPartitionsToAssign) {
        // Assignment is now deferred to app thread via StreamsTasksAssignedEvent,
        // so assignFromSubscribedAwaitingCallback should NOT be called directly from background thread
        verify(subscriptionState, never()).assignFromSubscribedAwaitingCallback(expectedAllPartitionsToAssign, expectedNewPartitionsToAssign);
        verify(memberStateListener, never()).onGroupAssignmentUpdated(expectedAllPartitionsToAssign);
        verify(subscriptionState, never()).enablePartitionsAwaitingCallback(expectedNewPartitionsToAssign);
        verifyInStateReconciling(membershipManager);
    }

    private void verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(Collection<TopicPartition> expectedNewPartitionsToAssign) {
        verify(subscriptionState).enablePartitionsAwaitingCallback(expectedNewPartitionsToAssign);
        verifyInStateAcknowledging(membershipManager);
    }

    private static void verifyInStateReconciling(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertFalse(membershipManager.shouldNotWaitForHeartbeatInterval());
        assertFalse(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateAcknowledging(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        assertTrue(membershipManager.shouldNotWaitForHeartbeatInterval());
        assertFalse(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateLeaving(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.LEAVING, membershipManager.state());
        assertTrue(membershipManager.shouldNotWaitForHeartbeatInterval());
        assertFalse(membershipManager.shouldSkipHeartbeat());
        assertTrue(membershipManager.isLeavingGroup());
    }

    private static void verifyInStatePrepareLeaving(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.PREPARE_LEAVING, membershipManager.state());
        assertFalse(membershipManager.shouldNotWaitForHeartbeatInterval());
        assertFalse(membershipManager.shouldSkipHeartbeat());
        assertTrue(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateUnsubscribed(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        assertFalse(membershipManager.shouldNotWaitForHeartbeatInterval());
        assertTrue(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateJoining(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.JOINING, membershipManager.state());
        assertTrue(membershipManager.shouldNotWaitForHeartbeatInterval());
        assertFalse(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateStable(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertFalse(membershipManager.shouldNotWaitForHeartbeatInterval());
        assertFalse(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateFenced(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.FENCED, membershipManager.state());
        assertFalse(membershipManager.shouldNotWaitForHeartbeatInterval());
        assertTrue(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private StreamsTasksAssignedEvent verifyOnTasksAssignedCallbackNeededEventAddedToBackgroundEventHandler(final Set<StreamsRebalanceData.TaskId> activeTasks,
                                                                                                            final Set<StreamsRebalanceData.TaskId> standbyTasks,
                                                                                                            final Set<StreamsRebalanceData.TaskId> warmupTasks,
                                                                                                            final Set<TopicPartition> assignedPartitions
                                                                                                            ) {
        return verifyOnTasksAssignedCallbackNeededEventAddedToBackgroundEventHandler(activeTasks, standbyTasks, warmupTasks, assignedPartitions, true);
    }

    private StreamsTasksAssignedEvent verifyOnTasksAssignedCallbackNeededEventAddedToBackgroundEventHandler(final Set<StreamsRebalanceData.TaskId> activeTasks,
                                                                                                            final Set<StreamsRebalanceData.TaskId> standbyTasks,
                                                                                                            final Set<StreamsRebalanceData.TaskId> warmupTasks,
                                                                                                            final Set<TopicPartition> assignedPartitions,
                                                                                                            final boolean isGroupReady) {
        verify(backgroundEventHandler, times(++tasksAssignedAddCount)).add(tasksAssignedEventCaptor.capture());
        final StreamsTasksAssignedEvent streamsTasksAssignedEvent = tasksAssignedEventCaptor.getValue();
        assertEquals(makeTaskAssignment(activeTasks, standbyTasks, warmupTasks, assignedPartitions, isGroupReady), streamsTasksAssignedEvent.assignment());
        return streamsTasksAssignedEvent;
    }

    private void completeAssignment(final StreamsTasksAssignedEvent event) {
        membershipManager.applyAssignment(event.assignedPartitions(), event.addedPartitions());
        event.future().complete(null);
    }

    private CompletableFuture<Void> verifyOnTasksRevokedCallbackNeededEventAddedToBackgroundEventHandler(final Set<StreamsRebalanceData.TaskId> activeTasksToRevoke) {
        verify(backgroundEventHandler).add(onTasksRevokedCallbackNeededEventCaptor.capture());
        final StreamsOnTasksRevokedCallbackNeededEvent onTasksRevokedCallbackNeededEvent = onTasksRevokedCallbackNeededEventCaptor.getValue();
        assertEquals(
            activeTasksToRevoke,
            onTasksRevokedCallbackNeededEvent.activeTasksToRevoke()
        );
        return onTasksRevokedCallbackNeededEvent.future();
    }

    private CompletableFuture<Void> verifyOnAllTasksLostCallbackNeededEventAddedToBackgroundEventHandler() {
        verify(backgroundEventHandler).add(onAllTasksLostCallbackNeededEventCaptor.capture());
        final StreamsOnAllTasksLostCallbackNeededEvent onAllTasksLostCallbackNeededEvent = onAllTasksLostCallbackNeededEventCaptor.getValue();
        return onAllTasksLostCallbackNeededEvent.future();
    }

    private void verifyNoAdditionalTasksAssigned() {
        verify(backgroundEventHandler, times(tasksAssignedAddCount))
            .add(any(StreamsTasksAssignedEvent.class));
    }

    private void setupStreamsRebalanceDataWithOneSubtopologyOneSourceTopic(final String subtopologyId,
                                                                           final String topicName) {
        lenient().when(streamsRebalanceData.subtopologies()).thenReturn(
            Map.of(
                subtopologyId,
                new StreamsRebalanceData.Subtopology(
                    Set.of(topicName),
                    Set.of(),
                    Map.of(),
                    Map.of(),
                    List.of()
                )
            )
        );
    }

    private void setupStreamsReabalanceDataWithTwoSubtopologies(final String subtopologyId1,
                                                                final String topicName1,
                                                                final String subtopologyId2,
                                                                final String topicName2) {
        lenient().when(streamsRebalanceData.subtopologies()).thenReturn(
            Map.of(
                subtopologyId1,
                new StreamsRebalanceData.Subtopology(
                    Set.of(topicName1),
                    Set.of(),
                    Map.of(),
                    Map.of(),
                    List.of()
                ),
                subtopologyId2,
                new StreamsRebalanceData.Subtopology(
                    Set.of(topicName2),
                    Set.of(),
                    Map.of(),
                    Map.of(),
                    List.of()
                )
            )
        );
    }

    private static StreamsGroupHeartbeatResponseData.TaskIds taskIds(
        final String subtopologyId,
        final List<Integer> partitions
    ) {
        return new StreamsGroupHeartbeatResponseData.TaskIds()
            .setSubtopologyId(subtopologyId)
            .setPartitions(partitions);
    }

    private StreamsRebalanceData.Assignment makeTaskAssignment(final Set<StreamsRebalanceData.TaskId> activeTasks,
                                                               final Set<StreamsRebalanceData.TaskId> standbyTasks,
                                                               final Set<StreamsRebalanceData.TaskId> warmupTasks,
                                                               final Set<TopicPartition> assignedPartitions,
                                                               final boolean isGroupReady) {
        return new StreamsRebalanceData.Assignment(
            activeTasks,
            standbyTasks,
            warmupTasks,
            assignedPartitions,
            isGroupReady
        );
    }

    private void reconcile(final StreamsGroupHeartbeatResponse response) {
        membershipManager.onHeartbeatSuccess(response);
        membershipManager.poll(time.milliseconds());
        verifyInStateReconciling(membershipManager);
    }

    private void acknowledging(final StreamsTasksAssignedEvent event) {
        completeAssignment(event);
        verifyInStateAcknowledging(membershipManager);
    }

    private StreamsGroupHeartbeatResponse makeHeartbeatResponseWithActiveTasks(final String subtopologyId,
                                                                               final List<Integer> partitions) {
        return makeHeartbeatResponse(
            List.of(taskIds(subtopologyId, partitions)),
            List.of(),
            List.of()
        );
    }

    private StreamsGroupHeartbeatResponse makeHeartbeatResponseWithActiveTasks(final String subtopologyId,
                                                                               final List<Integer> partitions,
                                                                               final int memberEpoch) {
        return makeHeartbeatResponse(
            List.of(taskIds(subtopologyId, partitions)),
            List.of(),
            List.of(),
            memberEpoch
        );
    }

    private StreamsGroupHeartbeatResponse makeHeartbeatResponseWithActiveTasks(final String subtopologyId0,
                                                                               final List<Integer> partitions0,
                                                                               final String subtopologyId1,
                                                                               final List<Integer> partitions1) {
        return makeHeartbeatResponse(
            List.of(taskIds(subtopologyId0, partitions0), taskIds(subtopologyId1, partitions1)),
            List.of(),
            List.of()
        );
    }

    private StreamsGroupHeartbeatResponse makeHeartbeatResponse(final List<StreamsGroupHeartbeatResponseData.TaskIds> activeTasks,
                                                                final List<StreamsGroupHeartbeatResponseData.TaskIds> standbyTasks,
                                                                final List<StreamsGroupHeartbeatResponseData.TaskIds> warmupTasks) {
        return makeHeartbeatResponse(activeTasks, standbyTasks, warmupTasks, MEMBER_EPOCH);
    }

    private StreamsGroupHeartbeatResponse makeHeartbeatResponse(final List<StreamsGroupHeartbeatResponseData.TaskIds> activeTasks,
                                                                final List<StreamsGroupHeartbeatResponseData.TaskIds> standbyTasks,
                                                                final List<StreamsGroupHeartbeatResponseData.TaskIds> warmupTasks,
                                                                final int memberEpoch) {
        final StreamsGroupHeartbeatResponseData responseData = new StreamsGroupHeartbeatResponseData()
            .setErrorCode(Errors.NONE.code())
            .setMemberId(membershipManager.memberId())
            .setMemberEpoch(memberEpoch)
            .setActiveTasks(activeTasks)
            .setStandbyTasks(standbyTasks)
            .setWarmupTasks(warmupTasks);
        return new StreamsGroupHeartbeatResponse(responseData);
    }

    private void joining() {
        membershipManager.onSubscriptionUpdated();
        membershipManager.onConsumerPoll();
        verifyInStateJoining(membershipManager);
    }

}
