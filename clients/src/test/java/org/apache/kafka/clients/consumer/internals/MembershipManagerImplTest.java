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

import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.apache.kafka.clients.consumer.internals.ConsumerTestBuilder.DEFAULT_GROUP_ID;
import static org.apache.kafka.clients.consumer.internals.ConsumerTestBuilder.DEFAULT_MEMBER_ID;
import static org.apache.kafka.clients.consumer.internals.ConsumerTestBuilder.DEFAULT_TOPIC_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MembershipManagerImplTest {

    private static final int MEMBER_EPOCH = 1;
    private ConsumerTestBuilder testBuilder;
    private MembershipManagerImpl membershipManager;

    @BeforeEach
    public void setup() {
        testBuilder = new ConsumerTestBuilder(ConsumerTestBuilder.createDefaultGroupInformation());
        membershipManager = testBuilder.membershipManager.orElseThrow(IllegalStateException::new);
    }

    @AfterEach
    public void tearDown() {
        if (testBuilder != null) {
            testBuilder.close();
        }
    }

    @Test
    public void testMembershipManagerServerAssignor() {
        assertEquals(Optional.empty(), membershipManager.serverAssignor());

        membershipManager = new MembershipManagerImpl(
                testBuilder.logContext,
                testBuilder.assignmentReconciler.orElseThrow(IllegalStateException::new),
                testBuilder.metadata,
                DEFAULT_GROUP_ID,
                Optional.of("instance1"),
                Optional.of("Uniform")
        );
        assertEquals(Optional.of("Uniform"), membershipManager.serverAssignor());
    }

    @Test
    public void testTransitionToReconcilingOnlyIfAssignmentReceived() {
        assertEquals(MemberState.UNJOINED, membershipManager.state());

        ConsumerGroupHeartbeatResponse responseWithoutAssignment = createHeartbeatResponse();
        membershipManager.updateState(responseWithoutAssignment.data());
        assertNotEquals(MemberState.RECONCILING, membershipManager.state());

        ConsumerGroupHeartbeatResponse responseWithAssignment = createHeartbeatResponse(createAssignment());
        membershipManager.updateState(responseWithAssignment.data());
        assertEquals(MemberState.RECONCILING, membershipManager.state());
    }

    @Test
    public void testMemberIdAndEpochResetOnFencedMembers() {
        ConsumerGroupHeartbeatResponse heartbeatResponse = createHeartbeatResponse();
        membershipManager.updateState(heartbeatResponse.data());
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertEquals(DEFAULT_MEMBER_ID, membershipManager.memberId());
        assertEquals(MEMBER_EPOCH, membershipManager.memberEpoch());

        membershipManager.transitionToFenced();
        assertFalse(membershipManager.memberId().isEmpty());
        assertEquals(0, membershipManager.memberEpoch());
    }

    @Test
    public void testTransitionToFailure() {
        ConsumerGroupHeartbeatResponse heartbeatResponse = createHeartbeatResponse();
        membershipManager.updateState(heartbeatResponse.data());
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertEquals(DEFAULT_MEMBER_ID, membershipManager.memberId());
        assertEquals(MEMBER_EPOCH, membershipManager.memberEpoch());

        membershipManager.transitionToFailed();
        assertEquals(MemberState.FAILED, membershipManager.state());
    }

    @Test
    public void testFencingWhenStateIsStable() {
        ConsumerGroupHeartbeatResponse heartbeatResponse = createHeartbeatResponse();
        membershipManager.updateState(heartbeatResponse.data());
        assertEquals(MemberState.STABLE, membershipManager.state());

        testStateUpdateOnFenceError();
    }

    @Test
    public void testFencingWhenStateIsReconciling() {
        ConsumerGroupHeartbeatResponse heartbeatResponse = createHeartbeatResponse(createAssignment());
        membershipManager.updateState(heartbeatResponse.data());
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        testStateUpdateOnFenceError();
    }

    @Test
    public void testFatalFailureWhenStateIsUnjoined() {
        assertEquals(MemberState.UNJOINED, membershipManager.state());

        testStateUpdateOnFatalFailure();
    }

    @Test
    public void testFatalFailureWhenStateIsStable() {
        ConsumerGroupHeartbeatResponse heartbeatResponse = createHeartbeatResponse();
        membershipManager.updateState(heartbeatResponse.data());
        assertEquals(MemberState.STABLE, membershipManager.state());

        testStateUpdateOnFatalFailure();
    }

    @Test
    public void testFencingShouldNotHappenWhenStateIsUnjoined() {
        assertEquals(MemberState.UNJOINED, membershipManager.state());

        // Getting fenced when the member is not part of the group is not expected and should
        // fail with invalid transition.
        assertThrows(IllegalStateException.class, membershipManager::transitionToFenced);
    }

    @Test
    public void testUpdateStateFailsOnResponsesWithErrors() {
        // Updating state with a heartbeat response containing errors cannot be performed and
        // should fail.
        ConsumerGroupHeartbeatResponse unknownMemberResponse = createHeartbeatResponse(Errors.UNKNOWN_MEMBER_ID);
        assertThrows(IllegalArgumentException.class, () -> membershipManager.updateState(unknownMemberResponse.data()));
    }

    @Test
    public void testAssignmentUpdatedAsReceivedAndProcessed() {
        ConsumerGroupHeartbeatResponseData.Assignment newAssignment = createAssignment();
        ConsumerGroupHeartbeatResponse heartbeatResponse = createHeartbeatResponse(newAssignment);
        membershipManager.updateState(heartbeatResponse.data());

        // Target assignment should be in the process of being reconciled
        checkAssignments(null, newAssignment);
        // Mark assignment processing completed
        membershipManager.onTargetAssignmentProcessComplete(newAssignment);
        // Target assignment should now be the current assignment
        checkAssignments(newAssignment, null);
    }

    @Test
    public void testMemberFailsIfAssignmentReceivedWhileAnotherOnBeingReconciled() {
        ConsumerGroupHeartbeatResponseData.Assignment newAssignment1 = createAssignment();
        ConsumerGroupHeartbeatResponseData response1 = createHeartbeatResponse(newAssignment1).data();
        membershipManager.updateState(response1);

        // First target assignment received should be in the process of being reconciled
        checkAssignments(null, newAssignment1);

        // Second target assignment received while there is another one being reconciled
        ConsumerGroupHeartbeatResponseData.Assignment newAssignment2 = createAssignment();
        ConsumerGroupHeartbeatResponseData response2 = createHeartbeatResponse(newAssignment2).data();
        assertThrows(IllegalStateException.class, () -> membershipManager.updateState(response2));
        assertEquals(MemberState.FAILED, membershipManager.state());
    }

    @Test
    public void testAssignmentUpdatedFailsIfAssignmentReconciledDoesNotMatchTargetAssignment() {
        ConsumerGroupHeartbeatResponseData.Assignment targetAssignment = createAssignment(0, 1, 2);
        ConsumerGroupHeartbeatResponse heartbeatResponse = createHeartbeatResponse(targetAssignment);
        membershipManager.updateState(heartbeatResponse.data());

        // Target assignment should be in the process of being reconciled
        checkAssignments(null, targetAssignment);
        // Mark assignment processing completed
        ConsumerGroupHeartbeatResponseData.Assignment reconciled = createAssignment(0);
        assertThrows(IllegalStateException.class, () -> membershipManager.onTargetAssignmentProcessComplete(reconciled));
    }

    private void checkAssignments(ConsumerGroupHeartbeatResponseData.Assignment expectedCurrentAssignment,
                                  ConsumerGroupHeartbeatResponseData.Assignment expectedTargetAssignment) {
        assertEquals(expectedCurrentAssignment, membershipManager.currentAssignment());
        assertEquals(expectedTargetAssignment, membershipManager.targetAssignment().orElse(null));
    }

    private void testStateUpdateOnFenceError() {
        membershipManager.transitionToFenced();
        assertEquals(MemberState.FENCED, membershipManager.state());
        // Should reset member epoch and keep member id
        assertFalse(membershipManager.memberId().isEmpty());
        assertEquals(0, membershipManager.memberEpoch());
    }

    private void testStateUpdateOnFatalFailure() {
        String initialMemberId = membershipManager.memberId();
        int initialMemberEpoch = membershipManager.memberEpoch();
        membershipManager.transitionToFailed();
        assertEquals(MemberState.FAILED, membershipManager.state());
        // Should not reset member id or epoch
        assertEquals(initialMemberId, membershipManager.memberId());
        assertEquals(initialMemberEpoch, membershipManager.memberEpoch());
    }

    private ConsumerGroupHeartbeatResponse createHeartbeatResponse() {
        return new ConsumerGroupHeartbeatResponse(createHeartbeatResponseData());
    }

    private ConsumerGroupHeartbeatResponse createHeartbeatResponse(ConsumerGroupHeartbeatResponseData.Assignment assignment) {
        ConsumerGroupHeartbeatResponseData data = createHeartbeatResponseData()
                .setAssignment(assignment);
        return new ConsumerGroupHeartbeatResponse(data);
    }

    private ConsumerGroupHeartbeatResponse createHeartbeatResponse(Errors error) {
        ConsumerGroupHeartbeatResponseData data = createHeartbeatResponseData()
                .setErrorCode(error.code())
                .setMemberEpoch(5);
        return new ConsumerGroupHeartbeatResponse(data);
    }

    private ConsumerGroupHeartbeatResponseData createHeartbeatResponseData() {
        return new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.NONE.code())
                .setMemberId(DEFAULT_MEMBER_ID)
                .setMemberEpoch(MEMBER_EPOCH);
    }

    private ConsumerGroupHeartbeatResponseData.Assignment createAssignment() {
        return createAssignment(
                createTopicPartitions(0, 1, 2),
                createTopicPartitions(3, 4, 5)
        );
    }

    private ConsumerGroupHeartbeatResponseData.Assignment createAssignment(ConsumerGroupHeartbeatResponseData.TopicPartitions... partitions) {
        return new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Arrays.asList(partitions));
    }

    private ConsumerGroupHeartbeatResponseData.Assignment createAssignment(Integer... partitions) {
        return new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.singletonList(createTopicPartitions(partitions)));
    }

    private ConsumerGroupHeartbeatResponseData.TopicPartitions createTopicPartitions(Integer... partitions) {
        return new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                .setTopicId(DEFAULT_TOPIC_ID)
                .setPartitions(Arrays.asList(partitions));
    }
}
