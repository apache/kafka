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

import org.apache.kafka.clients.consumer.internals.events.StreamsOnAllTasksLostCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksAssignedCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksRevokedCallbackCompletedEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.COORDINATOR_METRICS_SUFFIX;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StreamsMembershipManagerTest {

    private static final String GROUP_ID = "test-group";
    private static final int MEMBER_EPOCH = 1;

    private static final String SUBTOPOLOGY_ID_0 = "subtopology-0";
    private static final String SUBTOPOLOGY_ID_1 = "subtopology-1";

    private static final String TOPIC_0 = "topic-0";
    private static final String TOPIC_1 = "topic-1";

    private static final int PARTITION_0 = 0;
    private static final int PARTITION_1 = 1;

    private final Time time = new MockTime(0);
    private final Metrics metrics = new Metrics(time);

    private StreamsMembershipManager membershipManager;

    @Mock
    private SubscriptionState subscriptionState;

    @Mock
    private StreamsRebalanceEventsProcessor streamsRebalanceEventsProcessor;

    @Mock
    private StreamsRebalanceData streamsRebalanceData;

    @Mock
    private MemberStateListener memberStateListener;

    @BeforeEach
    public void setup() {
        membershipManager = new StreamsMembershipManager(
            GROUP_ID,
            streamsRebalanceEventsProcessor,
            streamsRebalanceData,
            subscriptionState,
            new LogContext("test"),
            time,
            metrics
        );
        membershipManager.registerStateListener(memberStateListener);
        verifyInStateUnsubscribed(membershipManager);
    }

    @Test
    public void testUnexpectedErrorInHeartbeatResponse() {
        final String errorMessage = "Nobody expects the Spanish Inquisition!";
        final StreamsGroupHeartbeatResponseData responseData = new StreamsGroupHeartbeatResponseData()
            .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code())
            .setErrorMessage(errorMessage);
        final StreamsGroupHeartbeatResponse response = new StreamsGroupHeartbeatResponse(responseData);

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> membershipManager.onHeartbeatSuccess(response)
        );

        assertEquals(
            "Unexpected error in Heartbeat response. Expected no error, but received: "
                + Errors.GROUP_AUTHORIZATION_FAILED.name()
                + " with message: '" + errorMessage + "'",
            exception.getMessage()
        );
    }

    @Test
    public void testActiveTasksAreNullInHeartbeatResponse() {
        testTasksAreNullInHeartbeatResponse(null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void testStandbyTasksAreNullInHeartbeatResponse() {
        testTasksAreNullInHeartbeatResponse(Collections.emptyList(), null, Collections.emptyList());
    }

    @Test
    public void testWarmupTasksAreNullInHeartbeatResponse() {
        testTasksAreNullInHeartbeatResponse(Collections.emptyList(), Collections.emptyList(), null);
    }

    private void testTasksAreNullInHeartbeatResponse(final List<StreamsGroupHeartbeatResponseData.TaskIds> activeTasks,
                                                     final List<StreamsGroupHeartbeatResponseData.TaskIds> standbyTasks,
                                                     final List<StreamsGroupHeartbeatResponseData.TaskIds> warmupTasks) {
        joining();
        final StreamsGroupHeartbeatResponse response = makeHeartbeatResponse(activeTasks, standbyTasks, warmupTasks);

        final IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> membershipManager.onHeartbeatSuccess(response)
        );

        assertEquals(
            "Invalid response data, task collections must be all null or all non-null: " + response.data(),
            exception.getMessage()
        );
    }

    @Test
    public void testJoining() {
        joining();

        verifyInStateJoining(membershipManager);
        assertEquals(StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH, membershipManager.memberEpoch());
    }

    @Test
    public void testReconcilingEmptyToSingleActiveTask() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsRebalanceData.TaskId> activeTasks =
            Set.of(new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecuted);
        joining();

        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));

        final Set<TopicPartition> expectedFullPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_0));
        final Set<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingActiveTaskToDifferentActiveTask() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksRevokedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> activeTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsRebalanceData.TaskId> activeTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1)
        );
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(streamsRebalanceEventsProcessor.requestOnTasksRevokedCallbackInvocation(activeTasksSetup))
            .thenReturn(onTasksRevokedCallbackExecuted);
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecuted);
        when(subscriptionState.assignedPartitions())
            .thenReturn(Collections.emptySet())
            .thenReturn(Set.of(new TopicPartition(TOPIC_0, PARTITION_0)));
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Set<TopicPartition> expectedPartitionsToRevoke = Set.of(new TopicPartition(TOPIC_0, PARTITION_0));
        final Set<TopicPartition> expectedFullPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_1));
        final Set<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskRevokedCallbackExecuted(
            expectedPartitionsToRevoke,
            expectedFullPartitionsToAssign,
            expectedNewPartitionsToAssign
        );
        onTasksRevokedCallbackExecuted.complete(null);
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
    }

    @Test
    public void testReconcilingSingleActiveTaskToAdditionalActiveTask() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> activeTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsRebalanceData.TaskId> activeTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0),
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1)
        );
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecuted);
        when(subscriptionState.assignedPartitions())
            .thenReturn(Collections.emptySet())
            .thenReturn(Set.of(new TopicPartition(TOPIC_0, PARTITION_0)));
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0, PARTITION_1)));

        final Set<TopicPartition> expectedFullPartitionsToAssign = Set.of(
            new TopicPartition(TOPIC_0, PARTITION_0),
            new TopicPartition(TOPIC_0, PARTITION_1)
        );
        final Set<TopicPartition> expectedNewPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_1));
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingMultipleActiveTaskToSingleActiveTask() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksRevokedCallbackExecuted = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> activeTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0),
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1)
        );
        final Set<StreamsRebalanceData.TaskId> activeTasksToRevoke = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsRebalanceData.TaskId> activeTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1)
        );
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(streamsRebalanceEventsProcessor.requestOnTasksRevokedCallbackInvocation(activeTasksToRevoke))
            .thenReturn(onTasksRevokedCallbackExecuted);
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecuted);
        when(subscriptionState.assignedPartitions())
            .thenReturn(Collections.emptySet())
            .thenReturn(Set.of(new TopicPartition(TOPIC_0, PARTITION_0), new TopicPartition(TOPIC_0, PARTITION_1)));
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0, PARTITION_1)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Set<TopicPartition> expectedPartitionsToRevoke = Set.of(new TopicPartition(TOPIC_0, PARTITION_0));
        final Set<TopicPartition> expectedFullPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_1));
        final Set<TopicPartition> expectedNewPartitionsToAssign = Collections.emptySet();
        verifyInStateReconcilingBeforeOnTaskRevokedCallbackExecuted(
            expectedPartitionsToRevoke,
            expectedFullPartitionsToAssign,
            expectedNewPartitionsToAssign
        );
        onTasksRevokedCallbackExecuted.complete(null);
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
    }

    @Test
    public void testReconcilingEmptyToMultipleActiveTaskOfDifferentSubtopologies() {
        setupStreamsAssignmentInterfaceWithTwoSubtopologies(
            SUBTOPOLOGY_ID_0, TOPIC_0,
            SUBTOPOLOGY_ID_1, TOPIC_1
        );
        final Set<StreamsRebalanceData.TaskId> activeTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0),
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_1, PARTITION_0)
        );
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        when(
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecuted);
        joining();

        reconcile(makeHeartbeatResponseWithActiveTasks(
            SUBTOPOLOGY_ID_0, List.of(PARTITION_0),
            SUBTOPOLOGY_ID_1, List.of(PARTITION_0))
        );

        final Set<TopicPartition> expectedFullPartitionsToAssign = Set.of(
            new TopicPartition(TOPIC_0, PARTITION_0),
            new TopicPartition(TOPIC_1, PARTITION_0)
        );
        final Set<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingActiveTaskToStandbyTask() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksRevokedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> activeTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsRebalanceData.TaskId> standbyTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1)
        );
        when(
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(streamsRebalanceEventsProcessor.requestOnTasksRevokedCallbackInvocation(activeTasksSetup))
            .thenReturn(onTasksRevokedCallbackExecuted);
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
            makeTaskAssignment(Collections.emptySet(), standbyTasks, Collections.emptySet()))
        ).thenReturn(onTasksAssignedCallbackExecuted);
        when(subscriptionState.assignedPartitions())
            .thenReturn(Collections.emptySet())
            .thenReturn(Set.of(new TopicPartition(TOPIC_0, PARTITION_0)))
            .thenReturn(Collections.emptySet());
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        reconcile(makeHeartbeatResponseWithStandbyTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Set<TopicPartition> expectedPartitionsToRevoke = Set.of(new TopicPartition(TOPIC_0, PARTITION_0));
        final Set<TopicPartition> expectedFullPartitionsToAssign = Collections.emptySet();
        final Set<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskRevokedCallbackExecuted(
            expectedPartitionsToRevoke,
            expectedFullPartitionsToAssign,
            expectedNewPartitionsToAssign
        );
        onTasksRevokedCallbackExecuted.complete(null);
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
    }

    @Test
    public void testReconcilingActiveTaskToWarmupTask() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksRevokedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> activeTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsRebalanceData.TaskId> warmupTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1)
        );
        when(
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(streamsRebalanceEventsProcessor.requestOnTasksRevokedCallbackInvocation(activeTasksSetup))
            .thenReturn(onTasksRevokedCallbackExecuted);
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
            makeTaskAssignment(Collections.emptySet(), Collections.emptySet(), warmupTasks))
        ).thenReturn(onTasksAssignedCallbackExecuted);
        when(subscriptionState.assignedPartitions())
            .thenReturn(Collections.emptySet())
            .thenReturn(Set.of(new TopicPartition(TOPIC_0, PARTITION_0)))
            .thenReturn(Collections.emptySet());
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        reconcile(makeHeartbeatResponseWithWarmupTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Set<TopicPartition> expectedPartitionsToRevoke = Set.of(new TopicPartition(TOPIC_0, PARTITION_0));
        final Set<TopicPartition> expectedFullPartitionsToAssign = Collections.emptySet();
        final Set<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskRevokedCallbackExecuted(
            expectedPartitionsToRevoke,
            expectedFullPartitionsToAssign,
            expectedNewPartitionsToAssign
        );
        onTasksRevokedCallbackExecuted.complete(null);
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
    }

    @Test
    public void testReconcilingEmptyToSingleStandbyTask() {
        final Set<StreamsRebalanceData.TaskId> standbyTasks =
            Set.of(new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        when(
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), standbyTasks, Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecuted);
        joining();

        reconcile(makeHeartbeatResponseWithStandbyTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));

        final Set<TopicPartition> expectedFullPartitionsToAssign = Collections.emptySet();
        final Set<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingStandbyTaskToDifferentStandbyTask() {
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> standbyTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsRebalanceData.TaskId> standbyTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1)
        );
        when(
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), standbyTasksSetup, Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), standbyTasks, Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithStandbyTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        Mockito.reset(subscriptionState);
        Mockito.reset(memberStateListener);

        reconcile(makeHeartbeatResponseWithStandbyTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Set<TopicPartition> expectedFullPartitionsToAssign = Collections.emptySet();
        final Set<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingSingleStandbyTaskToAdditionalStandbyTask() {
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> standbyTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsRebalanceData.TaskId> standbyTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0),
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1)
        );
        when(
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), standbyTasksSetup, Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), standbyTasks, Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithStandbyTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        Mockito.reset(subscriptionState);
        Mockito.reset(memberStateListener);

        reconcile(makeHeartbeatResponseWithStandbyTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0, PARTITION_1)));

        final Set<TopicPartition> expectedFullPartitionsToAssign = Collections.emptySet();
        final Set<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingMultipleStandbyTaskToSingleStandbyTask() {
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> standbyTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0),
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1)
        );
        final Set<StreamsRebalanceData.TaskId> standbyTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1)
        );
        when(
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), standbyTasksSetup, Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), standbyTasks, Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithStandbyTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0, PARTITION_1)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        Mockito.reset(subscriptionState);
        Mockito.reset(memberStateListener);

        reconcile(makeHeartbeatResponseWithStandbyTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Set<TopicPartition> expectedFullPartitionsToAssign = Collections.emptySet();
        final Set<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingStandbyTaskToActiveTask() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> standbyTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsRebalanceData.TaskId> activeTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1)
        );
        when(
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), standbyTasksSetup, Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecuted);
        when(subscriptionState.assignedPartitions())
            .thenReturn(Collections.emptySet())
            .thenReturn(Collections.emptySet())
            .thenReturn(Set.of(new TopicPartition(TOPIC_0, PARTITION_0)));
        joining();
        reconcile(makeHeartbeatResponseWithStandbyTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Set<TopicPartition> expectedFullPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_1));
        final Set<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingStandbyTaskToWarmupTask() {
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> standbyTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsRebalanceData.TaskId> warmupTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1)
        );
        when(
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), standbyTasksSetup, Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
            makeTaskAssignment(Collections.emptySet(), Collections.emptySet(), warmupTasks))
        ).thenReturn(onTasksAssignedCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithStandbyTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        Mockito.reset(subscriptionState);
        Mockito.reset(memberStateListener);

        reconcile(makeHeartbeatResponseWithWarmupTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Set<TopicPartition> expectedFullPartitionsToAssign = Collections.emptySet();
        final Set<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingEmptyToSingleWarmupTask() {
        final Set<StreamsRebalanceData.TaskId> warmupTasks =
            Set.of(new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        when(
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), Collections.emptySet(), warmupTasks)
            )
        ).thenReturn(onTasksAssignedCallbackExecuted);
        joining();

        reconcile(makeHeartbeatResponseWithWarmupTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));

        final Set<TopicPartition> expectedFullPartitionsToAssign = Collections.emptySet();
        final Set<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingWarmupTaskToDifferentWarmupTask() {
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> warmupTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsRebalanceData.TaskId> warmupTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1)
        );
        when(
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), Collections.emptySet(), warmupTasksSetup)
            )
        ).thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), Collections.emptySet(), warmupTasks)
            )
        ).thenReturn(onTasksAssignedCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithWarmupTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        Mockito.reset(subscriptionState);
        Mockito.reset(memberStateListener);

        reconcile(makeHeartbeatResponseWithWarmupTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Set<TopicPartition> expectedFullPartitionsToAssign = Collections.emptySet();
        final Set<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingSingleWarmupTaskToAdditionalWarmupTask() {
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> warmupTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsRebalanceData.TaskId> warmupTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0),
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1)
        );
        when(
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), Collections.emptySet(), warmupTasksSetup)
            )
        ).thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), Collections.emptySet(), warmupTasks)
            )
        ).thenReturn(onTasksAssignedCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithWarmupTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        Mockito.reset(subscriptionState);
        Mockito.reset(memberStateListener);

        reconcile(makeHeartbeatResponseWithWarmupTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0, PARTITION_1)));

        final Set<TopicPartition> expectedFullPartitionsToAssign = Collections.emptySet();
        final Set<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingMultipleWarmupTaskToSingleWarmupTask() {
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> warmupTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0),
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1)
        );
        final Set<StreamsRebalanceData.TaskId> warmupTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1)
        );
        when(
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), Collections.emptySet(), warmupTasksSetup)
            )
        ).thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), Collections.emptySet(), warmupTasks)
            )
        ).thenReturn(onTasksAssignedCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithWarmupTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0, PARTITION_1)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        Mockito.reset(subscriptionState);
        Mockito.reset(memberStateListener);

        reconcile(makeHeartbeatResponseWithWarmupTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Set<TopicPartition> expectedFullPartitionsToAssign = Collections.emptySet();
        final Set<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingWarmupTaskToActiveTask() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> warmupTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsRebalanceData.TaskId> activeTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1)
        );
        when(
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), Collections.emptySet(), warmupTasksSetup)
            )
        ).thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecuted);
        when(subscriptionState.assignedPartitions())
            .thenReturn(Collections.emptySet())
            .thenReturn(Collections.emptySet())
            .thenReturn(Set.of(new TopicPartition(TOPIC_0, PARTITION_1)));
        joining();
        reconcile(makeHeartbeatResponseWithWarmupTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Set<TopicPartition> expectedFullPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_1));
        final Set<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingWarmupTaskToStandbyTask() {
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> warmupTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsRebalanceData.TaskId> standbyTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1)
        );
        when(
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), Collections.emptySet(), warmupTasksSetup)
            )
        ).thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(
            streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), standbyTasks, Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithWarmupTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        Mockito.reset(subscriptionState);
        Mockito.reset(memberStateListener);

        reconcile(makeHeartbeatResponseWithStandbyTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Set<TopicPartition> expectedFullPartitionsToAssign = Collections.emptySet();
        final Set<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingAndAssignmentCallbackFails() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsRebalanceData.TaskId> activeTasks =
            Set.of(new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecuted);
        joining();

        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));

        final Set<TopicPartition> expectedFullPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_0));
        final Set<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);

        onTasksAssignedCallbackExecuted.completeExceptionally(new RuntimeException("KABOOM!"));

        verifyInStateReconciling(membershipManager);
        verify(subscriptionState, never()).enablePartitionsAwaitingCallback(any());
    }

    @Test
    public void testReconcilingAndRevocationCallbackFails() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksRevokedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> activeTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsRebalanceData.TaskId> activeTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1)
        );
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(streamsRebalanceEventsProcessor.requestOnTasksRevokedCallbackInvocation(activeTasksSetup))
            .thenReturn(onTasksRevokedCallbackExecuted);
        when(subscriptionState.assignedPartitions())
            .thenReturn(Collections.emptySet())
            .thenReturn(Set.of(new TopicPartition(TOPIC_0, PARTITION_0)));
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Set<TopicPartition> partitionsToAssignAtSetup = Set.of(new TopicPartition(TOPIC_0, PARTITION_0));
        final Set<TopicPartition> expectedPartitionsToRevoke = partitionsToAssignAtSetup;
        final Set<TopicPartition> expectedFullPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_1));
        final Set<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskRevokedCallbackExecuted(
            expectedPartitionsToRevoke,
            expectedFullPartitionsToAssign,
            expectedNewPartitionsToAssign
        );

        onTasksRevokedCallbackExecuted.completeExceptionally(new RuntimeException("KABOOM!"));

        verify(subscriptionState).markPendingRevocation(expectedPartitionsToRevoke);
        verify(subscriptionState, never()).assignFromSubscribedAwaitingCallback(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        verify(memberStateListener, never()).onGroupAssignmentUpdated(expectedFullPartitionsToAssign);
        verify(subscriptionState, never())
            .enablePartitionsAwaitingCallback(argThat(a -> !a.equals(partitionsToAssignAtSetup)));
        verifyInStateReconciling(membershipManager);
        verify(streamsRebalanceEventsProcessor, never()).requestOnTasksAssignedCallbackInvocation(
            makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())
        );
        verifyInStateReconciling(membershipManager);
    }

    @Test
    public void testReconcilingWhenReconciliationAbortedBeforeAssignmentDueToRejoin() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksRevokedCallbackExecuted = new CompletableFuture<>();
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> activeTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsRebalanceData.TaskId> activeTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1)
        );
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(streamsRebalanceEventsProcessor.requestOnTasksRevokedCallbackInvocation(activeTasksSetup))
            .thenReturn(onTasksRevokedCallbackExecuted);
        when(streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        when(subscriptionState.assignedPartitions())
            .thenReturn(Collections.emptySet())
            .thenReturn(Set.of(new TopicPartition(TOPIC_0, PARTITION_0)));
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_1)));
        final Set<TopicPartition> partitionsToAssignAtSetup = Set.of(new TopicPartition(TOPIC_0, PARTITION_0));
        final Set<TopicPartition> expectedPartitionsToRevoke = partitionsToAssignAtSetup;
        final Set<TopicPartition> expectedFullPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_1));
        final Set<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskRevokedCallbackExecuted(
            expectedPartitionsToRevoke,
            expectedFullPartitionsToAssign,
            expectedNewPartitionsToAssign
        );
        membershipManager.onPollTimerExpired();
        membershipManager.onHeartbeatRequestGenerated();
        onAllTasksLostCallbackExecuted.complete(null);
        membershipManager.maybeRejoinStaleMember();

        onTasksRevokedCallbackExecuted.complete(null);

        verify(subscriptionState, never()).assignFromSubscribedAwaitingCallback(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        verify(memberStateListener, never()).onGroupAssignmentUpdated(expectedFullPartitionsToAssign);
        verify(subscriptionState, never())
            .enablePartitionsAwaitingCallback(argThat(a -> !a.equals(partitionsToAssignAtSetup)));
        verify(streamsRebalanceEventsProcessor, never()).requestOnTasksAssignedCallbackInvocation(
            makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())
        );
        verifyInStateJoining(membershipManager);
    }

    @Test
    public void testReconcilingWhenReconciliationAbortedBeforeAssignmentDueToNotInReconciling() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksRevokedCallbackExecuted = new CompletableFuture<>();
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> activeTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsRebalanceData.TaskId> activeTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1)
        );
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(streamsRebalanceEventsProcessor.requestOnTasksRevokedCallbackInvocation(activeTasksSetup))
            .thenReturn(onTasksRevokedCallbackExecuted);
        when(streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        when(subscriptionState.assignedPartitions())
            .thenReturn(Collections.emptySet())
            .thenReturn(Set.of(new TopicPartition(TOPIC_0, PARTITION_0)));
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_1)));
        final Set<TopicPartition> partitionsToAssignAtSetup = Set.of(new TopicPartition(TOPIC_0, PARTITION_0));
        final Set<TopicPartition> expectedPartitionsToRevoke = partitionsToAssignAtSetup;
        final Set<TopicPartition> expectedFullPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_1));
        final Set<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskRevokedCallbackExecuted(
            expectedPartitionsToRevoke,
            expectedFullPartitionsToAssign,
            expectedNewPartitionsToAssign
        );
        membershipManager.transitionToFatal();
        onAllTasksLostCallbackExecuted.complete(null);

        onTasksRevokedCallbackExecuted.complete(null);

        verify(subscriptionState, never()).assignFromSubscribedAwaitingCallback(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        verify(memberStateListener, never()).onGroupAssignmentUpdated(expectedFullPartitionsToAssign);
        verify(subscriptionState, never())
            .enablePartitionsAwaitingCallback(argThat(a -> !a.equals(partitionsToAssignAtSetup)));
        verify(streamsRebalanceEventsProcessor, never()).requestOnTasksAssignedCallbackInvocation(
            makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())
        );
        verifyInStateFatal(membershipManager);
    }

    @Test
    public void testReconcilingWhenReconciliationAbortedAfterAssignmentDueToRejoin() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksRevokedCallbackExecuted = new CompletableFuture<>();
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> activeTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsRebalanceData.TaskId> activeTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1)
        );
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecuted);
        when(streamsRebalanceEventsProcessor.requestOnTasksRevokedCallbackInvocation(activeTasksSetup))
            .thenReturn(onTasksRevokedCallbackExecuted);
        when(streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        when(subscriptionState.assignedPartitions())
            .thenReturn(Collections.emptySet())
            .thenReturn(Set.of(new TopicPartition(TOPIC_0, PARTITION_0)));
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_1)));
        final Set<TopicPartition> partitionsToAssignAtSetup = Set.of(new TopicPartition(TOPIC_0, PARTITION_0));
        final Set<TopicPartition> expectedPartitionsToRevoke = partitionsToAssignAtSetup;
        final Set<TopicPartition> expectedFullPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_1));
        final Set<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskRevokedCallbackExecuted(
            expectedPartitionsToRevoke,
            expectedFullPartitionsToAssign,
            expectedNewPartitionsToAssign
        );
        onTasksRevokedCallbackExecuted.complete(null);
        membershipManager.onPollTimerExpired();
        membershipManager.onHeartbeatRequestGenerated();
        onAllTasksLostCallbackExecuted.complete(null);
        membershipManager.maybeRejoinStaleMember();

        onTasksAssignedCallbackExecuted.complete(null);

        assertNotEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
    }

    @Test
    public void testReconcilingWhenReconciliationAbortedAfterAssignmentDueToNotInReconciling() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksRevokedCallbackExecuted = new CompletableFuture<>();
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> activeTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsRebalanceData.TaskId> activeTasks = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_1)
        );
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecuted);
        when(streamsRebalanceEventsProcessor.requestOnTasksRevokedCallbackInvocation(activeTasksSetup))
            .thenReturn(onTasksRevokedCallbackExecuted);
        when(streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        when(subscriptionState.assignedPartitions())
            .thenReturn(Collections.emptySet())
            .thenReturn(Set.of(new TopicPartition(TOPIC_0, PARTITION_0)));
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_1)));
        final Set<TopicPartition> partitionsToAssignAtSetup = Set.of(new TopicPartition(TOPIC_0, PARTITION_0));
        final Set<TopicPartition> expectedPartitionsToRevoke = partitionsToAssignAtSetup;
        final Set<TopicPartition> expectedFullPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_1));
        final Set<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskRevokedCallbackExecuted(
            expectedPartitionsToRevoke,
            expectedFullPartitionsToAssign,
            expectedNewPartitionsToAssign
        );
        onTasksRevokedCallbackExecuted.complete(null);
        membershipManager.transitionToFatal();
        onAllTasksLostCallbackExecuted.complete(null);

        onTasksAssignedCallbackExecuted.complete(null);

        assertNotEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
    }

    @Test
    public void testLeaveGroupWhenNotInGroup() {
        testLeaveGroupWhenNotInGroup(membershipManager::leaveGroup);
    }

    @Test
    public void testLeaveGroupOnCloseWhenNotInGroup() {
        testLeaveGroupWhenNotInGroup(membershipManager::leaveGroupOnClose);
    }

    private void testLeaveGroupWhenNotInGroup(final Supplier<CompletableFuture<Void>> leaveGroup) {
        final CompletableFuture<Void> future = leaveGroup.get();

        assertFalse(membershipManager.isLeavingGroup());
        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertFalse(future.isCompletedExceptionally());
        verify(subscriptionState).unsubscribe();
        verify(memberStateListener).onGroupAssignmentUpdated(Collections.emptySet());
        verifyInStateUnsubscribed(membershipManager);
    }

    @Test
    public void testLeaveGroupWhenNotInGroupAndFenced() {
        testLeaveGroupOnCloseWhenNotInGroupAndFenced(membershipManager::leaveGroup);
    }

    @Test
    public void testLeaveGroupOnCloseWhenNotInGroupAndFenced() {
        testLeaveGroupOnCloseWhenNotInGroupAndFenced(membershipManager::leaveGroupOnClose);
    }

    private void testLeaveGroupOnCloseWhenNotInGroupAndFenced(final Supplier<CompletableFuture<Void>> leaveGroup) {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();
        fenced();
        final CompletableFuture<Void> future = leaveGroup.get();

        assertFalse(membershipManager.isLeavingGroup());
        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertFalse(future.isCompletedExceptionally());
        verify(subscriptionState).unsubscribe();
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
        verify(memberStateListener, times(2)).onGroupAssignmentUpdated(Collections.emptySet());
        verifyInStateUnsubscribed(membershipManager);
    }

    @Test
    public void testLeaveGroupWhenInGroupWithAssignment() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsRebalanceData.TaskId> activeTasks =
            Set.of(new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        final CompletableFuture<Void> onTasksRevokedCallbackExecuted = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnTasksRevokedCallbackInvocation(activeTasks))
            .thenReturn(onTasksRevokedCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        final CompletableFuture<Void> onGroupLeft = membershipManager.leaveGroup();

        assertFalse(onGroupLeft.isDone());
        verify(subscriptionState, never()).unsubscribe();
        verifyInStatePrepareLeaving(membershipManager);
        final CompletableFuture<Void> onGroupLeftBeforeRevocationCallback = membershipManager.leaveGroup();
        assertEquals(onGroupLeft, onGroupLeftBeforeRevocationCallback);
        final CompletableFuture<Void> onGroupLeftOnCloseBeforeRevocationCallback = membershipManager.leaveGroupOnClose();
        assertEquals(onGroupLeft, onGroupLeftOnCloseBeforeRevocationCallback);
        onTasksRevokedCallbackExecuted.complete(null);
        verify(memberStateListener).onGroupAssignmentUpdated(Collections.emptySet());
        verify(subscriptionState).unsubscribe();
        assertFalse(onGroupLeft.isDone());
        verifyInStateLeaving(membershipManager);
        final CompletableFuture<Void> onGroupLeftAfterRevocationCallback = membershipManager.leaveGroup();
        assertEquals(onGroupLeft, onGroupLeftAfterRevocationCallback);
        membershipManager.onHeartbeatRequestGenerated();
        verifyInStateUnsubscribed(membershipManager);
        membershipManager.onHeartbeatSuccess(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0), MEMBER_EPOCH + 1));
        assertTrue(onGroupLeft.isDone());
        assertFalse(onGroupLeft.isCompletedExceptionally());
        verify(memberStateListener, never()).onMemberEpochUpdated(Optional.of(MEMBER_EPOCH + 1), membershipManager.memberId());
    }

    @Test
    public void testLeaveGroupOnCloseWhenInGroupWithAssignment() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsRebalanceData.TaskId> activeTasks =
            Set.of(new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        final CompletableFuture<Void> onGroupLeft = membershipManager.leaveGroupOnClose();

        assertFalse(onGroupLeft.isDone());
        verifyInStateLeaving(membershipManager);
        verify(subscriptionState).unsubscribe();
        verify(memberStateListener).onGroupAssignmentUpdated(Collections.emptySet());
        verify(streamsRebalanceEventsProcessor, never()).requestOnTasksRevokedCallbackInvocation(any());
        final CompletableFuture<Void> onGroupLeftBeforeHeartbeatRequestGenerated = membershipManager.leaveGroup();
        assertEquals(onGroupLeft, onGroupLeftBeforeHeartbeatRequestGenerated);
        final CompletableFuture<Void> onGroupLeftOnCloseBeforeHeartbeatRequestGenerated = membershipManager.leaveGroupOnClose();
        assertEquals(onGroupLeft, onGroupLeftOnCloseBeforeHeartbeatRequestGenerated);
        assertFalse(onGroupLeft.isDone());
        membershipManager.onHeartbeatRequestGenerated();
        verifyInStateUnsubscribed(membershipManager);
        membershipManager.onHeartbeatSuccess(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0), MEMBER_EPOCH + 1));
        assertTrue(onGroupLeft.isDone());
        assertFalse(onGroupLeft.isCompletedExceptionally());
        verify(memberStateListener, never()).onMemberEpochUpdated(Optional.of(MEMBER_EPOCH + 1), membershipManager.memberId());
    }

    @Test
    public void testOnHeartbeatRequestSkippedWhenInLeaving() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, "topic");
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> activeTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0)
        );
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        final CompletableFuture<Void> onAllTasksRevokedCallbackExecuted = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnTasksRevokedCallbackInvocation(activeTasksSetup))
            .thenReturn(onAllTasksRevokedCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        CompletableFuture<Void> future = leaving(onAllTasksRevokedCallbackExecuted);

        membershipManager.onHeartbeatRequestSkipped();

        verifyInStateUnsubscribed(membershipManager);
        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertFalse(future.isCompletedExceptionally());
    }

    @Test
    public void testOnHeartbeatSuccessWhenInLeaving() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, "topic");
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> activeTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0)
        );
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        final CompletableFuture<Void> onTasksRevokedCallbackExecuted = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnTasksRevokedCallbackInvocation(activeTasksSetup))
            .thenReturn(onTasksRevokedCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        CompletableFuture<Void> future = leaving(onTasksRevokedCallbackExecuted);

        membershipManager.onHeartbeatSuccess(makeHeartbeatResponseWithActiveTasks(
            SUBTOPOLOGY_ID_0, List.of(PARTITION_0),
            membershipManager.memberEpoch() + 1
        ));

        verifyInStateLeaving(membershipManager);
        assertFalse(future.isDone());
        assertFalse(future.isCancelled());
        assertFalse(future.isCompletedExceptionally());
        verify(memberStateListener, never()).onMemberEpochUpdated(Optional.of(MEMBER_EPOCH + 1), membershipManager.memberId());
    }

    @Test
    public void testOnHeartbeatSuccessWhenInUnsubscribeLeaveNotInProgress() {
        membershipManager.onHeartbeatSuccess(makeHeartbeatResponseWithActiveTasks(
            SUBTOPOLOGY_ID_0, List.of(PARTITION_0),
            MEMBER_EPOCH
        ));

        verify(memberStateListener, never()).onMemberEpochUpdated(Optional.of(MEMBER_EPOCH), membershipManager.memberId());
    }

    @Test
    public void testOnHeartbeatSuccessWhenInFenced() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();
        fenced();

        membershipManager.onHeartbeatSuccess(makeHeartbeatResponseWithActiveTasks(
            SUBTOPOLOGY_ID_0, List.of(PARTITION_0),
            MEMBER_EPOCH
        ));

        verify(memberStateListener, never()).onMemberEpochUpdated(Optional.of(MEMBER_EPOCH), membershipManager.memberId());
    }

    @Test
    public void testOnHeartbeatSuccessWhenInFatal() {
        membershipManager.transitionToFatal();

        membershipManager.onHeartbeatSuccess(makeHeartbeatResponseWithActiveTasks(
            SUBTOPOLOGY_ID_0, List.of(PARTITION_0),
            MEMBER_EPOCH
        ));

        verify(memberStateListener, never()).onMemberEpochUpdated(Optional.of(MEMBER_EPOCH), membershipManager.memberId());
    }

    @Test
    public void testOnHeartbeatSuccessWhenInStale() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();
        membershipManager.onPollTimerExpired();
        membershipManager.onHeartbeatRequestGenerated();

        membershipManager.onHeartbeatSuccess(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0), MEMBER_EPOCH + 1));

        verify(memberStateListener, never()).onMemberEpochUpdated(Optional.of(MEMBER_EPOCH + 1), membershipManager.memberId());
    }

    @Test
    public void testOnHeartbeatSuccessWhenInReconciling() {
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(Set.of(), Set.of(), Set.of())))
            .thenReturn(onTasksAssignedCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(List.of(), MEMBER_EPOCH));
        onTasksAssignedCallbackExecuted.complete(null);
        membershipManager.onHeartbeatRequestGenerated();

        membershipManager.onHeartbeatSuccess(makeHeartbeatResponseWithActiveTasks(List.of(), MEMBER_EPOCH));

        verify(memberStateListener).onMemberEpochUpdated(Optional.of(MEMBER_EPOCH), membershipManager.memberId());
        verifyInStateStable(membershipManager);
    }

    @Test
    public void testOnPollTimerExpired() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsRebalanceData.TaskId> activeTasks =
            Set.of(new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        membershipManager.onPollTimerExpired();

        verifyInStateLeaving(membershipManager);
        assertEquals(StreamsGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH, membershipManager.memberEpoch());
    }

    @Test
    public void testOnPollTimerExpiredWhenInFatal() {
        membershipManager.transitionToFatal();

        membershipManager.onPollTimerExpired();

        verifyInStateFatal(membershipManager);
    }

    @Test
    public void testOnPollTimerExpiredWhenInUnsubscribe() {
        membershipManager.onPollTimerExpired();

        verifyInStateUnsubscribed(membershipManager);
    }

    @Test
    public void testOnHeartbeatRequestGeneratedWhenInAcknowleding() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsRebalanceData.TaskId> activeTasks =
            Set.of(new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        membershipManager.onHeartbeatRequestGenerated();

        verifyInStateStable(membershipManager);
    }

    @Test
    public void testOnHeartbeatRequestGeneratedWhenInAcknowledgingAndNewTargetAssignment() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsRebalanceData.TaskId> activeTasks =
            Set.of(new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_1)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        membershipManager.onHeartbeatRequestGenerated();

        verifyInStateReconciling(membershipManager);
    }

    @Test
    public void testOnHeartbeatRequestGeneratedWhenInLeaving() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();
        leaving(onAllTasksLostCallbackExecuted);

        membershipManager.onHeartbeatRequestGenerated();

        verifyInStateUnsubscribed(membershipManager);
    }

    @Test
    public void testOnHeartbeatRequestGeneratedWhenInLeavingAndPollTimerExpired() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();
        membershipManager.onPollTimerExpired();

        membershipManager.onHeartbeatRequestGenerated();

        verifyInStateStale(membershipManager);
        verify(subscriptionState, never()).assignFromSubscribed(Collections.emptySet());
        onAllTasksLostCallbackExecuted.complete(null);
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
        verify(memberStateListener).onGroupAssignmentUpdated(Collections.emptySet());
    }

    @Test
    public void testOnHeartbeatFailureAfterLeaveRequestGenerated() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();
        final CompletableFuture<Void> groupLeft = leaving(onAllTasksLostCallbackExecuted);
        membershipManager.onHeartbeatRequestGenerated();
        assertFalse(groupLeft.isDone());

        membershipManager.onHeartbeatFailure(true);

        assertTrue(groupLeft.isDone());
    }

    @Test
    public void testOnHeartbeatFatalFailure() {
        testOnHeartbeatFailure(false);
    }

    @Test
    public void testOnHeartbeatRetriableFailure() {
        testOnHeartbeatFailure(true);
    }

    private void testOnHeartbeatFailure(boolean retriable) {
        final MetricName failedRebalanceTotalMetricName = metrics.metricName(
            "failed-rebalance-total",
            CONSUMER_METRIC_GROUP_PREFIX + COORDINATOR_METRICS_SUFFIX
        );
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsRebalanceData.TaskId> activeTasks =
            Set.of(new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecuted);
        joining();
        time.sleep(1);
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        final double failedRebalancesTotalBefore = (double) metrics.metric(failedRebalanceTotalMetricName).metricValue();
        assertEquals(0L, failedRebalancesTotalBefore);

        membershipManager.onHeartbeatFailure(retriable);

        final double failedRebalancesTotalAfter = (double) metrics.metric(failedRebalanceTotalMetricName).metricValue();
        assertEquals(retriable ? 0L : 1L, failedRebalancesTotalAfter);
    }

    @Test
    public void testOnFencedWhenInJoining() {
        joining();

        testOnFencedWhenInJoiningOrReconcilingOrAcknowledgingOrStable();
    }

    @Test
    public void testOnFencedWhenInReconciling() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsRebalanceData.TaskId> activeTasks =
            Set.of(new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));

        testOnFencedWhenInJoiningOrReconcilingOrAcknowledgingOrStable();
    }

    @Test
    public void testOnFencedWhenInAcknowledging() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsRebalanceData.TaskId> activeTasks =
            Set.of(new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        testOnFencedWhenInJoiningOrReconcilingOrAcknowledgingOrStable();
    }

    @Test
    public void testOnFencedWhenInStable() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsRebalanceData.TaskId> activeTasks =
            Set.of(new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        stable();

        testOnFencedWhenInJoiningOrReconcilingOrAcknowledgingOrStable();
    }

    private void testOnFencedWhenInJoiningOrReconcilingOrAcknowledgingOrStable() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);

        membershipManager.onFenced();

        verifyInStateFenced(membershipManager);
        assertEquals(StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH, membershipManager.memberEpoch());
        verify(subscriptionState, never()).assignFromSubscribed(Collections.emptySet());
        onAllTasksLostCallbackExecuted.complete(null);
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
        verify(memberStateListener).onGroupAssignmentUpdated(Collections.emptySet());
        verifyInStateJoining(membershipManager);
    }

    @Test
    public void testOnFencedWhenInPrepareLeaving() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();

        testOnFencedWhenInPrepareLeavingOrLeaving(prepareLeaving());
    }

    @Test
    public void testOnFencedWhenInLeaving() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();

        testOnFencedWhenInPrepareLeavingOrLeaving(leaving(onAllTasksLostCallbackExecuted));
    }

    private void testOnFencedWhenInPrepareLeavingOrLeaving(final CompletableFuture<Void> onGroupLeft) {
        membershipManager.onFenced();

        verifyInStateUnsubscribed(membershipManager);
        assertEquals(StreamsGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH, membershipManager.memberEpoch());
        assertTrue(onGroupLeft.isDone());
        assertFalse(onGroupLeft.isCancelled());
        assertFalse(onGroupLeft.isCompletedExceptionally());
    }

    @Test
    public void testTransitionToFatalWhenInPrepareLeaving() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();

        testTransitionToFatalWhenInPrepareLeavingOrLeaving(prepareLeaving());

        verify(memberStateListener).onMemberEpochUpdated(Optional.empty(), membershipManager.memberId());
    }

    @Test
    public void testTransitionToFatalWhenInLeaving() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();

        testTransitionToFatalWhenInPrepareLeavingOrLeaving(leaving(onAllTasksLostCallbackExecuted));
        verify(memberStateListener, times(2)).onMemberEpochUpdated(Optional.empty(), membershipManager.memberId());
    }

    private void testTransitionToFatalWhenInPrepareLeavingOrLeaving(final CompletableFuture<Void> onGroupLeft) {
        membershipManager.transitionToFatal();

        verifyInStateFatal(membershipManager);
        assertTrue(onGroupLeft.isDone());
        assertFalse(onGroupLeft.isCancelled());
        assertFalse(onGroupLeft.isCompletedExceptionally());
    }

    @Test
    public void testTransitionToFatalWhenInJoining() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();

        testTransitionToFatalWhenInJoiningOrReconcilingOrAcknowledgingOrStable(onAllTasksLostCallbackExecuted);
    }

    @Test
    public void testTransitionToFatalWhenInReconciling() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> activeTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0)
        );
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));

        testTransitionToFatalWhenInJoiningOrReconcilingOrAcknowledgingOrStable(onAllTasksLostCallbackExecuted);
    }

    @Test
    public void testTransitionToFatalWhenInAcknowledging() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> activeTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0)
        );
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        testTransitionToFatalWhenInJoiningOrReconcilingOrAcknowledgingOrStable(onAllTasksLostCallbackExecuted);
    }

    @Test
    public void testTransitionToFatalWhenInStable() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> activeTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0)
        );
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        stable();

        testTransitionToFatalWhenInJoiningOrReconcilingOrAcknowledgingOrStable(onAllTasksLostCallbackExecuted);
    }

    private void testTransitionToFatalWhenInJoiningOrReconcilingOrAcknowledgingOrStable(final CompletableFuture<Void> future) {
        membershipManager.transitionToFatal();

        verify(subscriptionState, never()).assignFromSubscribed(Collections.emptySet());
        future.complete(null);
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
        verifyInStateFatal(membershipManager);
        verify(memberStateListener).onMemberEpochUpdated(Optional.empty(), membershipManager.memberId());
        verify(memberStateListener).onGroupAssignmentUpdated(Collections.emptySet());
    }

    @Test
    public void testTransitionToFatalWhenInUnsubscribe() {
        membershipManager.transitionToFatal();

        verifyInStateFatal(membershipManager);
        verify(memberStateListener).onMemberEpochUpdated(Optional.empty(), membershipManager.memberId());
        verify(streamsRebalanceEventsProcessor, never()).requestOnAllTasksLostCallbackInvocation();
        verify(streamsRebalanceEventsProcessor, never()).requestOnAllTasksLostCallbackInvocation();
        verify(subscriptionState, never()).assignFromSubscribed(Collections.emptySet());
    }

    @Test
    public void testOnTasksAssignedCallbackCompleted() {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        final StreamsOnTasksAssignedCallbackCompletedEvent event = new StreamsOnTasksAssignedCallbackCompletedEvent(
            future,
            Optional.empty()
        );

        membershipManager.onTasksAssignedCallbackCompleted(event);

        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertFalse(future.isCompletedExceptionally());
    }

    @Test
    public void testOnTasksAssignedCallbackCompletedWhenCallbackFails() {
        final String errorMessage = "KABOOM!";
        final CompletableFuture<Void> future = new CompletableFuture<>();
        final StreamsOnTasksAssignedCallbackCompletedEvent event = new StreamsOnTasksAssignedCallbackCompletedEvent(
            future,
            Optional.of(new KafkaException(errorMessage))
        );

        membershipManager.onTasksAssignedCallbackCompleted(event);

        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertTrue(future.isCompletedExceptionally());
        final ExecutionException executionException = assertThrows(ExecutionException.class, future::get);
        assertInstanceOf(KafkaException.class, executionException.getCause());
        assertEquals(errorMessage, executionException.getCause().getMessage());

        final SortedSet<StreamsRebalanceData.TaskId> activeTasksToAssign = new TreeSet<>();
        activeTasksToAssign.add(new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0));
        System.out.println(activeTasksToAssign.stream()
            .map(StreamsRebalanceData.TaskId::toString)
            .collect(Collectors.joining(", ")));
    }

    @Test
    public void testOnTasksRevokedCallbackCompleted() {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        final StreamsOnTasksRevokedCallbackCompletedEvent event = new StreamsOnTasksRevokedCallbackCompletedEvent(
            future,
            Optional.empty()
        );

        membershipManager.onTasksRevokedCallbackCompleted(event);

        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertFalse(future.isCompletedExceptionally());
    }

    @Test
    public void testOnTasksRevokedCallbackCompletedWhenCallbackFails() {
        final String errorMessage = "KABOOM!";
        final CompletableFuture<Void> future = new CompletableFuture<>();
        final StreamsOnTasksRevokedCallbackCompletedEvent event = new StreamsOnTasksRevokedCallbackCompletedEvent(
            future,
            Optional.of(new KafkaException(errorMessage))
        );

        membershipManager.onTasksRevokedCallbackCompleted(event);

        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertTrue(future.isCompletedExceptionally());
        final ExecutionException executionException = assertThrows(ExecutionException.class, future::get);
        assertInstanceOf(KafkaException.class, executionException.getCause());
        assertEquals(errorMessage, executionException.getCause().getMessage());
    }

    @Test
    public void testOnAllTasksLostCallbackCompleted() {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        final StreamsOnAllTasksLostCallbackCompletedEvent event = new StreamsOnAllTasksLostCallbackCompletedEvent(
            future,
            Optional.empty()
        );

        membershipManager.onAllTasksLostCallbackCompleted(event);

        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertFalse(future.isCompletedExceptionally());
    }

    @Test
    public void testOnAllTasksLostCallbackCompletedWhenCallbackFails() {
        final String errorMessage = "KABOOM!";
        final CompletableFuture<Void> future = new CompletableFuture<>();
        final StreamsOnAllTasksLostCallbackCompletedEvent event = new StreamsOnAllTasksLostCallbackCompletedEvent(
            future,
            Optional.of(new KafkaException(errorMessage))
        );

        membershipManager.onAllTasksLostCallbackCompleted(event);

        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertTrue(future.isCompletedExceptionally());
        final ExecutionException executionException = assertThrows(ExecutionException.class, future::get);
        assertInstanceOf(KafkaException.class, executionException.getCause());
        assertEquals(errorMessage, executionException.getCause().getMessage());
    }

    @Test
    public void testMaybeRejoinStaleMember() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsRebalanceEventsProcessor.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();
        membershipManager.onPollTimerExpired();
        membershipManager.onHeartbeatRequestGenerated();
        verifyInStateStale(membershipManager);

        membershipManager.maybeRejoinStaleMember();

        verifyInStateStale(membershipManager);
        onAllTasksLostCallbackExecuted.complete(null);
        verifyInStateJoining(membershipManager);
        assertEquals(StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH, membershipManager.memberEpoch());
    }

    @Test
    public void testForDuplicateRegistrationOfSameStateListener() {
        final MemberStateListener listener1 = new MemberStateListener() {

            @Override
            public void onMemberEpochUpdated(Optional<Integer> memberEpoch, String memberId) {
            }
        };
        final MemberStateListener listener2 = new MemberStateListener() {

            @Override
            public void onMemberEpochUpdated(Optional<Integer> memberEpoch, String memberId) {
            }
        };

        membershipManager.registerStateListener(listener1);
        membershipManager.registerStateListener(listener2);
        final Exception exception =
            assertThrows(IllegalArgumentException.class, () -> membershipManager.registerStateListener(listener1));
        assertEquals("Listener is already registered.", exception.getMessage());
    }

    @Test
    public void testConsumerPollWhenNotJoining() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUBTOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final Set<StreamsRebalanceData.TaskId> activeTasksSetup = Set.of(
            new StreamsRebalanceData.TaskId(SUBTOPOLOGY_ID_0, PARTITION_0)
        );
        when(streamsRebalanceEventsProcessor.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUBTOPOLOGY_ID_0, List.of(PARTITION_0)));
        membershipManager.onSubscriptionUpdated();

        membershipManager.onConsumerPoll();

        verifyInStateReconciling(membershipManager);
    }

    @Test
    public void testConsumerPollWhenSubscriptionNotUpdated() {
        membershipManager.onConsumerPoll();

        verifyInStateUnsubscribed(membershipManager);
    }

    private void verifyThatNoTasksHaveBeenRevoked() {
        verify(streamsRebalanceEventsProcessor, never()).requestOnTasksRevokedCallbackInvocation(any());
        verify(subscriptionState, never()).markPendingRevocation(any());
    }

    private void verifyInStateReconcilingBeforeOnTaskRevokedCallbackExecuted(Set<TopicPartition> expectedPartitionsToRevoke,
                                                                             Set<TopicPartition> expectedAllPartitionsToAssign,
                                                                             Set<TopicPartition> expectedNewPartitionsToAssign) {
        verify(subscriptionState).markPendingRevocation(expectedPartitionsToRevoke);
        verify(subscriptionState, never()).assignFromSubscribedAwaitingCallback(expectedAllPartitionsToAssign, expectedNewPartitionsToAssign);
        verifyInStateReconciling(membershipManager);
    }

    private void verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(Set<TopicPartition> expectedAllPartitionsToAssign,
                                                                              Set<TopicPartition> expectedNewPartitionsToAssign) {
        verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedAllPartitionsToAssign, expectedNewPartitionsToAssign);
        verify(memberStateListener).onGroupAssignmentUpdated(expectedAllPartitionsToAssign);
        verify(subscriptionState, never()).enablePartitionsAwaitingCallback(expectedNewPartitionsToAssign);
        verifyInStateReconciling(membershipManager);
    }

    private void verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(Collection<TopicPartition> expectedNewPartitionsToAssign) {
        verify(subscriptionState).enablePartitionsAwaitingCallback(expectedNewPartitionsToAssign);
        verifyInStateAcknowledging(membershipManager);
    }

    private static void verifyInStateReconciling(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertFalse(membershipManager.shouldHeartbeatNow());
        assertFalse(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateAcknowledging(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        assertTrue(membershipManager.shouldHeartbeatNow());
        assertFalse(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateLeaving(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.LEAVING, membershipManager.state());
        assertTrue(membershipManager.shouldHeartbeatNow());
        assertFalse(membershipManager.shouldSkipHeartbeat());
        assertTrue(membershipManager.isLeavingGroup());
    }

    private static void verifyInStatePrepareLeaving(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.PREPARE_LEAVING, membershipManager.state());
        assertFalse(membershipManager.shouldHeartbeatNow());
        assertFalse(membershipManager.shouldSkipHeartbeat());
        assertTrue(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateUnsubscribed(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        assertFalse(membershipManager.shouldHeartbeatNow());
        assertTrue(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateJoining(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.JOINING, membershipManager.state());
        assertTrue(membershipManager.shouldHeartbeatNow());
        assertFalse(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateStable(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertFalse(membershipManager.shouldHeartbeatNow());
        assertFalse(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateFenced(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.FENCED, membershipManager.state());
        assertFalse(membershipManager.shouldHeartbeatNow());
        assertTrue(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateFatal(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.FATAL, membershipManager.state());
        assertFalse(membershipManager.shouldHeartbeatNow());
        assertTrue(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateStale(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.STALE, membershipManager.state());
        assertFalse(membershipManager.shouldHeartbeatNow());
        assertTrue(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private void setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(final String subtopologyId,
                                                                                 final String topicName) {
        when(streamsRebalanceData.subtopologies()).thenReturn(
            mkMap(
                mkEntry(
                    subtopologyId,
                    new StreamsRebalanceData.Subtopology(
                        Set.of(topicName),
                        Collections.emptySet(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyList()
                    )
                )
            )
        );
    }

    private void setupStreamsAssignmentInterfaceWithTwoSubtopologies(final String subtopologyId1,
                                                                     final String topicName1,
                                                                     final String subtopologyId2,
                                                                     final String topicName2) {
        when(streamsRebalanceData.subtopologies()).thenReturn(
            mkMap(
                mkEntry(
                    subtopologyId1,
                    new StreamsRebalanceData.Subtopology(
                        Set.of(topicName1),
                        Collections.emptySet(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyList()
                    )
                ),
                mkEntry(
                    subtopologyId2,
                    new StreamsRebalanceData.Subtopology(
                        Set.of(topicName2),
                        Collections.emptySet(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyList()
                    )
                )
            )
        );
    }

    private StreamsGroupHeartbeatResponse makeHeartbeatResponseWithActiveTasks(final String subtopologyId,
                                                                               final List<Integer> partitions) {
        return makeHeartbeatResponseWithActiveTasks(List.of(
            new StreamsGroupHeartbeatResponseData.TaskIds()
                .setSubtopologyId(subtopologyId)
                .setPartitions(partitions)
            ),
            MEMBER_EPOCH
        );
    }

    private StreamsGroupHeartbeatResponse makeHeartbeatResponseWithActiveTasks(final String subtopologyId,
                                                                               final List<Integer> partitions,
                                                                               final int memberEpoch) {
        return makeHeartbeatResponseWithActiveTasks(List.of(
            new StreamsGroupHeartbeatResponseData.TaskIds()
                .setSubtopologyId(subtopologyId)
                .setPartitions(partitions)
            ),
            memberEpoch
        );
    }

    private StreamsGroupHeartbeatResponse makeHeartbeatResponseWithStandbyTasks(final String subtopologyId,
                                                                                final List<Integer> partitions) {
        return makeHeartbeatResponse(
            Collections.emptyList(),
            List.of(
                new StreamsGroupHeartbeatResponseData.TaskIds()
                    .setSubtopologyId(subtopologyId)
                    .setPartitions(partitions)
            ),
            Collections.emptyList(),
            MEMBER_EPOCH
        );
    }

    private StreamsGroupHeartbeatResponse makeHeartbeatResponseWithWarmupTasks(final String subtopologyId,
                                                                               final List<Integer> partitions) {
        return makeHeartbeatResponse(
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(
                new StreamsGroupHeartbeatResponseData.TaskIds()
                    .setSubtopologyId(subtopologyId)
                    .setPartitions(partitions)
            ),
            MEMBER_EPOCH
        );
    }

    private StreamsGroupHeartbeatResponse makeHeartbeatResponseWithActiveTasks(final String subtopologyId0,
                                                                               final List<Integer> partitions0,
                                                                               final String subtopologyId1,
                                                                               final List<Integer> partitions1) {
        return makeHeartbeatResponseWithActiveTasks(List.of(
            new StreamsGroupHeartbeatResponseData.TaskIds()
                .setSubtopologyId(subtopologyId0)
                .setPartitions(partitions0),
            new StreamsGroupHeartbeatResponseData.TaskIds()
                .setSubtopologyId(subtopologyId1)
                .setPartitions(partitions1)),
            MEMBER_EPOCH
        );
    }

    private StreamsGroupHeartbeatResponse makeHeartbeatResponseWithActiveTasks(final List<StreamsGroupHeartbeatResponseData.TaskIds> activeTasks,
                                                                               final int memberEpoch) {
        return makeHeartbeatResponse(activeTasks, Collections.emptyList(), Collections.emptyList(), memberEpoch);
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

    private StreamsRebalanceData.Assignment makeTaskAssignment(final Set<StreamsRebalanceData.TaskId> activeTasks,
                                                               final Set<StreamsRebalanceData.TaskId> standbyTasks,
                                                               final Set<StreamsRebalanceData.TaskId> warmupTasks) {
        return new StreamsRebalanceData.Assignment(
            activeTasks,
            standbyTasks,
            warmupTasks
        );
    }

    private void joining() {
        membershipManager.onSubscriptionUpdated();
        membershipManager.onConsumerPoll();
        verifyInStateJoining(membershipManager);
    }

    private void reconcile(final StreamsGroupHeartbeatResponse response) {
        membershipManager.onHeartbeatSuccess(response);
        membershipManager.poll(time.milliseconds());
        verifyInStateReconciling(membershipManager);
    }

    private void acknowledging(final CompletableFuture<Void> future) {
        future.complete(null);
        verifyInStateAcknowledging(membershipManager);
    }

    private CompletableFuture<Void> prepareLeaving() {
        final CompletableFuture<Void> onGroupLeft = membershipManager.leaveGroup();
        verifyInStatePrepareLeaving(membershipManager);
        return onGroupLeft;
    }

    private CompletableFuture<Void> leaving(final CompletableFuture<Void> onAllTasksRevokedCallbackExecuted) {
        final CompletableFuture<Void> future = prepareLeaving();
        onAllTasksRevokedCallbackExecuted.complete(null);
        verifyInStateLeaving(membershipManager);
        return future;
    }

    private void stable() {
        membershipManager.onHeartbeatRequestGenerated();
    }

    private void fenced() {
        membershipManager.onFenced();
    }
}
