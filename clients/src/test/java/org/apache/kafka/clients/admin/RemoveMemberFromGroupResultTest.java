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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RemoveMemberFromGroupResultTest {

    private String instanceOne = "instance-1";
    private String instanceTwo = "instance-2";
    private List<MemberIdentity> membersToRemove;

    private List<MemberResponse> memberResponses;

    @Before
    public void setUp() {
        membersToRemove = Arrays.asList(
            new MemberIdentity()
                .setGroupInstanceId(instanceOne),
            new MemberIdentity()
                .setGroupInstanceId(instanceTwo)
        );

        memberResponses = Arrays.asList(
            new MemberResponse()
                .setGroupInstanceId(instanceOne),
            new MemberResponse()
                .setGroupInstanceId(instanceTwo)
        );
    }

    @Test
    public void testTopLevelErrorConstructor() {
        RemoveMemberFromGroupResult topLevelErrorResult =
            new RemoveMemberFromGroupResult(new LeaveGroupResponse(
                new LeaveGroupResponseData()
                    .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code())
                    .setMembers(memberResponses)), membersToRemove);

        assertTrue(topLevelErrorResult.hasError());
        assertEquals(Errors.GROUP_AUTHORIZATION_FAILED, topLevelErrorResult.topLevelError());

        Map<MemberIdentity, KafkaFuture<Void>> memberFutures = topLevelErrorResult.memberFutures();
        assertEquals(2, memberFutures.size());
        for (Map.Entry<MemberIdentity, KafkaFuture<Void>> entry : memberFutures.entrySet()) {
            KafkaFuture<Void> memberFuture = entry.getValue();
            assertTrue(memberFuture.isCompletedExceptionally());
            try {
                memberFuture.get();
                fail("get() should throw ExecutionException");
            } catch (ExecutionException | InterruptedException e0) {
                assertTrue(e0.getCause() instanceof GroupAuthorizationException);
            }
        }
    }

    @Test
    public void testMemberLevelErrorConstructor() {
        MemberResponse responseOne = new MemberResponse()
                                         .setGroupInstanceId(instanceOne)
                                         .setErrorCode(Errors.FENCED_INSTANCE_ID.code());
        MemberResponse responseTwo = new MemberResponse()
                                         .setGroupInstanceId(instanceTwo)
                                         .setErrorCode(Errors.NONE.code());

        RemoveMemberFromGroupResult memberLevelErrorResult = new RemoveMemberFromGroupResult(
            new LeaveGroupResponse(new LeaveGroupResponseData()
                                       .setMembers(Arrays.asList(responseOne, responseTwo))),
            membersToRemove);
        assertTrue(memberLevelErrorResult.hasError());
        assertEquals(Errors.NONE, memberLevelErrorResult.topLevelError());

        Map<MemberIdentity, KafkaFuture<Void>> memberFutures = memberLevelErrorResult.memberFutures();
        assertEquals(2, memberFutures.size());
        for (Map.Entry<MemberIdentity, KafkaFuture<Void>> entry : memberFutures.entrySet()) {
            KafkaFuture<Void> memberFuture = entry.getValue();
            if (entry.getKey().groupInstanceId().equals(instanceOne)) {
                assertTrue(memberFuture.isCompletedExceptionally());
                try {
                    memberFuture.get();
                    fail("get() should throw ExecutionException");
                } catch (ExecutionException | InterruptedException e0) {
                    assertTrue(e0.getCause() instanceof FencedInstanceIdException);
                }
            } else {
                assertFalse(memberFuture.isCompletedExceptionally());
                try {
                    memberFuture.get();
                } catch (ExecutionException | InterruptedException e0) {
                    fail("get() shouldn't throw exception");
                }
            }
        }
    }

    @Test
    public void testNoErrorConstructor() {
        MemberResponse responseOne = new MemberResponse()
                                         .setGroupInstanceId(instanceOne)
                                         .setErrorCode(Errors.NONE.code());
        MemberResponse responseTwo = new MemberResponse()
                                         .setGroupInstanceId(instanceTwo)
                                         .setErrorCode(Errors.NONE.code());
        // If no error is specified, failed members are not visible.
        RemoveMemberFromGroupResult noErrorResult = new RemoveMemberFromGroupResult(
            new LeaveGroupResponse(new LeaveGroupResponseData()
                                       .setMembers(Arrays.asList(responseOne, responseTwo))),
            membersToRemove);
        assertFalse(noErrorResult.hasError());
        assertEquals(Errors.NONE, noErrorResult.topLevelError());
        Map<MemberIdentity, KafkaFuture<Void>> memberFutures = noErrorResult.memberFutures();
        assertEquals(2, memberFutures.size());
        for (Map.Entry<MemberIdentity, KafkaFuture<Void>> entry : memberFutures.entrySet()) {
            try {
                entry.getValue().get();
            } catch (ExecutionException | InterruptedException e0) {
                fail("get() shouldn't throw exception");
            }
        }
    }
}
