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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @ParameterizedTest
    @EnumSource(Errors.class)
    public void testMemberIdAndEpochResetOnErrors(Errors error) {
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(GROUP_ID);
        ConsumerGroupHeartbeatResponse heartbeatResponse =
                createConsumerGroupHeartbeatResponse(null);
        membershipManager.updateState(heartbeatResponse.data());
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertEquals(MEMBER_ID, membershipManager.memberId());
        assertEquals(MEMBER_EPOCH, membershipManager.memberEpoch());

        if (error == Errors.UNKNOWN_MEMBER_ID) {
            // Should reset member id and epoch
            ConsumerGroupHeartbeatResponse heartbeatResponseWithMemberIdError =
                    createConsumerGroupHeartbeatResponseWithError(Errors.UNKNOWN_MEMBER_ID);
            membershipManager.updateState(heartbeatResponseWithMemberIdError.data());

            assertTrue(membershipManager.memberId().isEmpty());
            assertEquals(0, membershipManager.memberEpoch());
        } else if (error == Errors.FENCED_MEMBER_EPOCH) {
            // Should reset member epoch and keep member id
            ConsumerGroupHeartbeatResponse heartbeatResponseWithMemberIdError =
                    createConsumerGroupHeartbeatResponseWithError(Errors.FENCED_MEMBER_EPOCH);
            membershipManager.updateState(heartbeatResponseWithMemberIdError.data());

            assertFalse(membershipManager.memberId().isEmpty());
            assertEquals(0, membershipManager.memberEpoch());
        } else {
            // Should not reset member id or epoch
            ConsumerGroupHeartbeatResponse heartbeatResponseWithError =
                    createConsumerGroupHeartbeatResponseWithError(error);
            membershipManager.updateState(heartbeatResponseWithError.data());

            assertFalse(membershipManager.memberId().isEmpty());
            assertNotEquals(0, membershipManager.memberEpoch());
        }
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
