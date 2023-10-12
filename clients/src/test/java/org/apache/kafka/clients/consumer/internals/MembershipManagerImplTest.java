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
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MembershipManagerImplTest {

    private static final String GROUP_ID = "test-group";
    private static final String MEMBER_ID = "test-member-1";
    private static final int MEMBER_EPOCH = 1;

    @Test
    public void testMembershipManagerDefaultAssignor() {
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(GROUP_ID);
        assertEquals(AssignorSelection.defaultAssignor(), membershipManager.assignorSelection());

        membershipManager = new MembershipManagerImpl(GROUP_ID, "instance1", null);
        assertEquals(AssignorSelection.defaultAssignor(), membershipManager.assignorSelection());
    }

    @Test
    public void testMembershipManagerAssignorSelectionUpdate() {
        AssignorSelection firstAssignorSelection = AssignorSelection.newServerAssignor("uniform");
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(GROUP_ID, "instance1",
                firstAssignorSelection);
        assertEquals(firstAssignorSelection, membershipManager.assignorSelection());

        AssignorSelection secondAssignorSelection = AssignorSelection.newServerAssignor("range");
        membershipManager.setAssignorSelection(secondAssignorSelection);
        assertEquals(secondAssignorSelection, membershipManager.assignorSelection());

        assertThrows(IllegalArgumentException.class,
                () -> membershipManager.setAssignorSelection(null));
    }

    @Test
    public void testMembershipManagerInitSupportsEmptyGroupInstanceId() {
        new MembershipManagerImpl(GROUP_ID);
        new MembershipManagerImpl(GROUP_ID, null, AssignorSelection.defaultAssignor());
    }

    @Test
    public void testTransitionToReconcilingOnlyIfAssignmentReceived() {
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(GROUP_ID);
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
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(GROUP_ID);
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
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(GROUP_ID);
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
    public void testUpdateAssignment() {
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(GROUP_ID);
        ConsumerGroupHeartbeatResponseData.Assignment newAssignment = createAssignment();
        ConsumerGroupHeartbeatResponse heartbeatResponse =
                createConsumerGroupHeartbeatResponse(newAssignment);
        membershipManager.updateState(heartbeatResponse.data());

        // Target assignment should be in the process of being reconciled
        checkAssignments(membershipManager, null, newAssignment, null);
    }

    @Test
    public void testUpdateAssignmentReceivingAssignmentWhileAnotherInProcess() {
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(GROUP_ID);
        ConsumerGroupHeartbeatResponseData.Assignment newAssignment1 = createAssignment();
        membershipManager.updateState(createConsumerGroupHeartbeatResponse(newAssignment1).data());

        // First target assignment received should be in the process of being reconciled
        checkAssignments(membershipManager, null, newAssignment1, null);

        // Second target assignment received while there is another one being reconciled
        ConsumerGroupHeartbeatResponseData.Assignment newAssignment2 = createAssignment();
        membershipManager.updateState(createConsumerGroupHeartbeatResponse(newAssignment2).data());
        checkAssignments(membershipManager, null, newAssignment1, newAssignment2);
    }

    @Test
    public void testNextTargetAssignmentHoldsLatestAssignmentReceivedWhileAnotherInProcess() {
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(GROUP_ID);
        ConsumerGroupHeartbeatResponseData.Assignment newAssignment1 = createAssignment();
        membershipManager.updateState(createConsumerGroupHeartbeatResponse(newAssignment1).data());

        // First target assignment received, remains in the process of being reconciled
        checkAssignments(membershipManager, null, newAssignment1, null);

        // Second target assignment received while there is another one being reconciled
        ConsumerGroupHeartbeatResponseData.Assignment newAssignment2 = createAssignment();
        membershipManager.updateState(createConsumerGroupHeartbeatResponse(newAssignment2).data());
        checkAssignments(membershipManager, null, newAssignment1, newAssignment2);

        // If more assignments are received while there is one being reconciled, the most recent
        // assignment received is kept as nextTargetAssignment
        ConsumerGroupHeartbeatResponseData.Assignment newAssignment3 = createAssignment();
        membershipManager.updateState(createConsumerGroupHeartbeatResponse(newAssignment3).data());
        checkAssignments(membershipManager, null, newAssignment1, newAssignment3);
    }

    private void checkAssignments(
            MembershipManagerImpl membershipManager,
            ConsumerGroupHeartbeatResponseData.Assignment expectedCurrentAssignment,
            ConsumerGroupHeartbeatResponseData.Assignment expectedTargetAssignment,
            ConsumerGroupHeartbeatResponseData.Assignment expectedNextTargetAssignment) {
        assertEquals(expectedCurrentAssignment, membershipManager.assignment());
        assertEquals(expectedTargetAssignment, membershipManager.targetAssignment().orElse(null));
        assertEquals(expectedNextTargetAssignment, membershipManager.nextTargetAssignment().orElse(null));

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
