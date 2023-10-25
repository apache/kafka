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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.BeforeEach;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.Optional;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MembershipManagerImplTest {

    private static final String TOPIC_NAME = "test-topic";
    private static final Uuid TOPIC_ID = Uuid.randomUuid();
    private static final String GROUP_ID = "test-group";
    private static final String MEMBER_ID = "test-member-1";
    private static final int MEMBER_EPOCH = 1;

    private ConsumerMetadata metadata;
    private AssignmentReconciler assignmentReconciler;

    @BeforeEach
    public void setup() {
        Map<String, Uuid> topics = Collections.singletonMap(TOPIC_NAME, TOPIC_ID);
        LogContext logContext = new LogContext();

        // Create our subscriptions and subscribe to the topics.
        SubscriptionState subscriptions = new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST);
        subscriptions.subscribe(topics.keySet(), new NoOpConsumerRebalanceListener());

        // Create our metadata and ensure at has our topic name to topic ID mapping.
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWithIds(
                "dummy",
                1,
                Collections.emptyMap(),
                topics.keySet().stream().collect(Collectors.toMap(t -> t, t -> 3)),
                tp -> 0,
                topics
        );
        Properties props = new Properties();
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        ConsumerConfig config = new ConsumerConfig(props);
        metadata = new ConsumerMetadata(
                config,
                subscriptions,
                logContext,
                new ClusterResourceListeners()
        );
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 0L);

        BlockingQueue<BackgroundEvent> backgroundEventQueue = new LinkedBlockingQueue<>();
        assignmentReconciler = new AssignmentReconciler(logContext, subscriptions, backgroundEventQueue);
    }

    @Test
    public void testMembershipManagerDefaultAssignor() {
        MembershipManagerImpl membershipManager = createMembershipManager(GROUP_ID);
        assertEquals(AssignorSelection.defaultAssignor(), membershipManager.assignorSelection());

        membershipManager = createMembershipManager(GROUP_ID, "instance1", null);
        assertEquals(AssignorSelection.defaultAssignor(), membershipManager.assignorSelection());
    }

    @Test
    public void testMembershipManagerAssignorSelectionUpdate() {
        AssignorSelection firstAssignorSelection = AssignorSelection.newServerAssignor("uniform");
        MembershipManagerImpl membershipManager = createMembershipManager(
                GROUP_ID,
                "instance1",
                firstAssignorSelection
        );
        assertEquals(firstAssignorSelection, membershipManager.assignorSelection());

        AssignorSelection secondAssignorSelection = AssignorSelection.newServerAssignor("range");
        membershipManager.setAssignorSelection(secondAssignorSelection);
        assertEquals(secondAssignorSelection, membershipManager.assignorSelection());

        assertThrows(IllegalArgumentException.class,
                () -> membershipManager.setAssignorSelection(null));
    }

    @Test
    public void testMembershipManagerServerAssignor() {
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(GROUP_ID, logContext);
        assertEquals(Optional.empty(), membershipManager.serverAssignor());

        membershipManager = new MembershipManagerImpl(GROUP_ID, "instance1", "Uniform", logContext);
        assertEquals(Optional.of("Uniform"), membershipManager.serverAssignor());
    }

    @Test
    public void testMembershipManagerInitSupportsEmptyGroupInstanceId() {
        createMembershipManager(GROUP_ID);
        createMembershipManager(GROUP_ID, null, AssignorSelection.defaultAssignor());
    }

    @Test
    public void testTransitionToReconcilingOnlyIfAssignmentReceived() {
        MembershipManagerImpl membershipManager = createMembershipManager(GROUP_ID);
        assertEquals(MemberState.UNJOINED, membershipManager.state());

        ConsumerGroupHeartbeatResponse responseWithoutAssignment =
                createConsumerGroupHeartbeatResponse(null);
        membershipManager.updateState(responseWithoutAssignment.data());
        assertNotEquals(MemberState.RECONCILING, membershipManager.state());

        ConsumerGroupHeartbeatResponse responseWithAssignment =
                createConsumerGroupHeartbeatResponse(createAssignment());
        membershipManager.updateState(responseWithAssignment.data());
        assertEquals(MemberState.RECONCILING, membershipManager.state());
    }

    @Test
    public void testMemberIdAndEpochResetOnFencedMembers() {
        MembershipManagerImpl membershipManager = createMembershipManager(GROUP_ID);
        ConsumerGroupHeartbeatResponse heartbeatResponse =
                createConsumerGroupHeartbeatResponse(null);
        membershipManager.updateState(heartbeatResponse.data());
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertEquals(MEMBER_ID, membershipManager.memberId());
        assertEquals(MEMBER_EPOCH, membershipManager.memberEpoch());

        membershipManager.transitionToFenced();
        assertFalse(membershipManager.memberId().isEmpty());
        assertEquals(0, membershipManager.memberEpoch());
    }

    @Test
    public void testTransitionToFailure() {
        MembershipManagerImpl membershipManager = createMembershipManager(GROUP_ID);
        ConsumerGroupHeartbeatResponse heartbeatResponse =
                createConsumerGroupHeartbeatResponse(null);
        membershipManager.updateState(heartbeatResponse.data());
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertEquals(MEMBER_ID, membershipManager.memberId());
        assertEquals(MEMBER_EPOCH, membershipManager.memberEpoch());

        membershipManager.transitionToFailed();
        assertEquals(MemberState.FAILED, membershipManager.state());
    }

    @Test
    public void testFencingWhenStateIsStable() {
        MembershipManagerImpl membershipManager = createMembershipManager(GROUP_ID);
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(null);
        membershipManager.updateState(heartbeatResponse.data());
        assertEquals(MemberState.STABLE, membershipManager.state());

        testStateUpdateOnFenceError(membershipManager);
    }

    @Test
    public void testFencingWhenStateIsReconciling() {
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(GROUP_ID, logContext);
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(createAssignment());
        membershipManager.updateState(heartbeatResponse.data());
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        testStateUpdateOnFenceError(membershipManager);
    }

    @Test
    public void testFatalFailureWhenStateIsUnjoined() {
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(GROUP_ID, logContext);
        assertEquals(MemberState.UNJOINED, membershipManager.state());

        testStateUpdateOnFatalFailure(membershipManager);
    }

    @Test
    public void testFatalFailureWhenStateIsStable() {
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(GROUP_ID, logContext);
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(null);
        membershipManager.updateState(heartbeatResponse.data());
        assertEquals(MemberState.STABLE, membershipManager.state());

        testStateUpdateOnFatalFailure(membershipManager);
    }

    @Test
    public void testFencingShouldNotHappenWhenStateIsUnjoined() {
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(GROUP_ID, logContext);
        assertEquals(MemberState.UNJOINED, membershipManager.state());

        // Getting fenced when the member is not part of the group is not expected and should
        // fail with invalid transition.
        assertThrows(IllegalStateException.class, membershipManager::transitionToFenced);
    }

    @Test
    public void testUpdateStateFailsOnResponsesWithErrors() {
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(GROUP_ID, logContext);
        // Updating state with a heartbeat response containing errors cannot be performed and
        // should fail.
        ConsumerGroupHeartbeatResponse unknownMemberResponse =
                createConsumerGroupHeartbeatResponseWithError(Errors.UNKNOWN_MEMBER_ID);
        assertThrows(IllegalArgumentException.class,
                () -> membershipManager.updateState(unknownMemberResponse.data()));
    }

    @Test
    public void testAssignmentUpdatedAsReceivedAndProcessed() {
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(GROUP_ID, logContext);
        ConsumerGroupHeartbeatResponseData.Assignment newAssignment = createAssignment();
        ConsumerGroupHeartbeatResponse heartbeatResponse =
                createConsumerGroupHeartbeatResponse(newAssignment);
        membershipManager.updateState(heartbeatResponse.data());

        // Target assignment should be in the process of being reconciled
        checkAssignments(membershipManager, null, newAssignment);
        // Mark assignment processing completed
        membershipManager.onTargetAssignmentProcessComplete(newAssignment);
        // Target assignment should now be the current assignment
        checkAssignments(membershipManager, newAssignment, null);
    }

    @Test
    public void testMemberFailsIfAssignmentReceivedWhileAnotherOnBeingReconciled() {
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(GROUP_ID, logContext);
        ConsumerGroupHeartbeatResponseData.Assignment newAssignment1 = createAssignment();
        membershipManager.updateState(createConsumerGroupHeartbeatResponse(newAssignment1).data());

        // First target assignment received should be in the process of being reconciled
        checkAssignments(membershipManager, null, newAssignment1);

        // Second target assignment received while there is another one being reconciled
        ConsumerGroupHeartbeatResponseData.Assignment newAssignment2 = createAssignment();
        assertThrows(IllegalStateException.class,
                () -> membershipManager.updateState(createConsumerGroupHeartbeatResponse(newAssignment2).data()));
        assertEquals(MemberState.FAILED, membershipManager.state());
    }

    @Test
    public void testAssignmentUpdatedFailsIfAssignmentReconciledDoesNotMatchTargetAssignment() {
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(GROUP_ID, logContext);
        ConsumerGroupHeartbeatResponseData.Assignment targetAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.singletonList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(Uuid.randomUuid())
                                .setPartitions(Arrays.asList(0, 1, 2))));
        ConsumerGroupHeartbeatResponse heartbeatResponse =
                createConsumerGroupHeartbeatResponse(targetAssignment);
        membershipManager.updateState(heartbeatResponse.data());

        // Target assignment should be in the process of being reconciled
        checkAssignments(membershipManager, null, targetAssignment);
        // Mark assignment processing completed
        ConsumerGroupHeartbeatResponseData.Assignment reconciled =
                new ConsumerGroupHeartbeatResponseData.Assignment()
                        .setTopicPartitions(Collections.singletonList(
                                new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                        .setTopicId(Uuid.randomUuid())
                                        .setPartitions(Collections.singletonList(0))));
        assertThrows(IllegalStateException.class, () -> membershipManager.onTargetAssignmentProcessComplete(reconciled));
    }

    private void checkAssignments(
            MembershipManagerImpl membershipManager,
            ConsumerGroupHeartbeatResponseData.Assignment expectedCurrentAssignment,
            ConsumerGroupHeartbeatResponseData.Assignment expectedTargetAssignment) {
        assertEquals(expectedCurrentAssignment, membershipManager.currentAssignment());
        assertEquals(expectedTargetAssignment, membershipManager.targetAssignment().orElse(null));
    }

    private void testStateUpdateOnFenceError(MembershipManager membershipManager) {
        membershipManager.transitionToFenced();
        assertEquals(MemberState.FENCED, membershipManager.state());
        // Should reset member epoch and keep member id
        assertFalse(membershipManager.memberId().isEmpty());
        assertEquals(0, membershipManager.memberEpoch());
    }

    private void testStateUpdateOnFatalFailure(MembershipManager membershipManager) {
        String initialMemberId = membershipManager.memberId();
        int initialMemberEpoch = membershipManager.memberEpoch();
        membershipManager.transitionToFailed();
        assertEquals(MemberState.FAILED, membershipManager.state());
        // Should not reset member id or epoch
        assertEquals(initialMemberId, membershipManager.memberId());
        assertEquals(initialMemberEpoch, membershipManager.memberEpoch());
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
                                .setTopicId(TOPIC_ID)
                                .setPartitions(Arrays.asList(0, 1, 2)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(TOPIC_ID)
                                .setPartitions(Arrays.asList(3, 4, 5))
                ));
    }

    private MembershipManagerImpl createMembershipManager(String groupId) {
        return new MembershipManagerImpl(metadata, groupId, assignmentReconciler);
    }

    private MembershipManagerImpl createMembershipManager(String groupId,
                                                          String instanceId,
                                                          AssignorSelection assignorSelection) {
        return new MembershipManagerImpl(metadata, groupId, instanceId, assignorSelection, assignmentReconciler);
    }
}
