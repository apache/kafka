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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MembershipManagerImplTest {

    private static final String GROUP_ID = "test-group";

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

    private ConsumerGroupHeartbeatResponse createConsumerGroupHeartbeatResponse(ConsumerGroupHeartbeatResponseData.Assignment assignment) {
        return new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.NONE.code())
                .setMemberId("testMember1")
                .setMemberEpoch(1)
                .setAssignment(assignment));
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
