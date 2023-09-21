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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MembershipManagerImplTest {

    private static final String GROUP_ID = "test-group";
    private static final String MEMBER_ID = "test-member-1";
    private static final int MEMBER_EPOCH = 1;
    private final LogContext logContext = new LogContext();

    @Test
    public void testMembershipManagerAssignorSelectionUpdate() {
        AssignorSelection firstAssignorSelection = AssignorSelection.newServerAssignor("uniform");
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(GROUP_ID, "instance1",
                firstAssignorSelection, logContext);
        assertTrue(membershipManager.assignorSelection().isPresent());
        assertEquals(firstAssignorSelection, membershipManager.assignorSelection().get());

        AssignorSelection secondAssignorSelection = AssignorSelection.newServerAssignor("range");
        membershipManager.setAssignorSelection(Optional.of(secondAssignorSelection));
        assertTrue(membershipManager.assignorSelection().isPresent());
        assertEquals(secondAssignorSelection, membershipManager.assignorSelection().get());

        assertThrows(IllegalArgumentException.class,
                () -> membershipManager.setAssignorSelection(null));
    }

    @Test
    public void testMembershipManagerInitSupportsNullGroupInstanceIdAndAssignor() {
        new MembershipManagerImpl(GROUP_ID, logContext);
        new MembershipManagerImpl(GROUP_ID, null, null, logContext);
    }

    @Test
    public void testTransitionToReconcilingOnlyIfAssignmentReceived() {
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(GROUP_ID, logContext);
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
    public void testFencingWhenStateIsStable() {
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(GROUP_ID, logContext);
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
        assertThrows(IllegalStateException.class, membershipManager::fenceMember);
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
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(newAssignment);
        membershipManager.updateState(heartbeatResponse.data());

        // Target assignment should be in the process of being reconciled
        checkAssignments(membershipManager, null, newAssignment);
        // Mark assignment processing completed
        membershipManager.onTargetAssignmentProcessComplete(Optional.empty());
        // Target assignment should now be the current assignment
        checkAssignments(membershipManager, newAssignment, null);
    }

    @Test
    public void testUpdateAssignmentReceivingAssignmentWhileAnotherInProcess() {
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(GROUP_ID, logContext);
        ConsumerGroupHeartbeatResponseData.Assignment newAssignment1 = createAssignment();
        membershipManager.updateState(createConsumerGroupHeartbeatResponse(newAssignment1).data());

        // First target assignment received should be in the process of being reconciled
        checkAssignments(membershipManager, null, newAssignment1);

        // Second target assignment received while there is another one being reconciled
        ConsumerGroupHeartbeatResponseData.Assignment newAssignment2 = createAssignment();
        membershipManager.updateState(createConsumerGroupHeartbeatResponse(newAssignment2).data());
        checkAssignments(membershipManager, null, newAssignment1);
    }

    private void checkAssignments(
            MembershipManagerImpl membershipManager,
            ConsumerGroupHeartbeatResponseData.Assignment expectedCurrentAssignment,
            ConsumerGroupHeartbeatResponseData.Assignment expectedTargetAssignment) {
        assertEquals(expectedCurrentAssignment, membershipManager.currentAssignment());
        assertEquals(expectedTargetAssignment, membershipManager.targetAssignment().orElse(null));
    }

    private void testStateUpdateOnFenceError(MembershipManager membershipManager) {
        membershipManager.fenceMember();
        assertEquals(MemberState.FENCED, membershipManager.state());
        // Should reset member epoch and keep member id
        assertFalse(membershipManager.memberId().isEmpty());
        assertEquals(0, membershipManager.memberEpoch());
    }

    private void testStateUpdateOnFatalFailure(MembershipManager membershipManager) {
        String initialMemberId = membershipManager.memberId();
        int initialMemberEpoch = membershipManager.memberEpoch();
        membershipManager.failMember();
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
                .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(Uuid.randomUuid())
                                .setPartitions(Arrays.asList(0, 1, 2)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(Uuid.randomUuid())
                                .setPartitions(Arrays.asList(3, 4, 5))
                ));
    }
}