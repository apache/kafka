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

import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RemoveMembersFromConsumerGroupResultTest {

    private final MemberToRemove instanceOne = new MemberToRemove("instance-1");
    private final MemberToRemove instanceTwo = new MemberToRemove("instance-2");
    private Set<MemberToRemove> membersToRemove;
    private Map<MemberIdentity, Errors> errorsMap;

    private KafkaFutureImpl<Map<MemberIdentity, Errors>> memberFutures;

    @BeforeEach
    public void setUp() {
        memberFutures = new KafkaFutureImpl<>();
        membersToRemove = new HashSet<>();
        membersToRemove.add(instanceOne);
        membersToRemove.add(instanceTwo);

        errorsMap = new HashMap<>();
        errorsMap.put(instanceOne.toMemberIdentity(), Errors.NONE);
        errorsMap.put(instanceTwo.toMemberIdentity(), Errors.FENCED_INSTANCE_ID);
    }

    @Test
    public void testTopLevelErrorConstructor() throws InterruptedException {
        memberFutures.completeExceptionally(Errors.GROUP_AUTHORIZATION_FAILED.exception());
        RemoveMembersFromConsumerGroupResult topLevelErrorResult =
            new RemoveMembersFromConsumerGroupResult(memberFutures, membersToRemove);
        TestUtils.assertFutureThrows(topLevelErrorResult.all(), GroupAuthorizationException.class);
    }

    @Test
    public void testMemberLevelErrorConstructor() throws InterruptedException, ExecutionException {
        createAndVerifyMemberLevelError();
    }

    @Test
    public void testMemberMissingErrorInRequestConstructor() throws InterruptedException, ExecutionException {
        errorsMap.remove(instanceTwo.toMemberIdentity());
        memberFutures.complete(errorsMap);
        assertFalse(memberFutures.isCompletedExceptionally());
        RemoveMembersFromConsumerGroupResult missingMemberResult =
            new RemoveMembersFromConsumerGroupResult(memberFutures, membersToRemove);

        TestUtils.assertFutureThrows(missingMemberResult.all(), IllegalArgumentException.class);
        assertNull(missingMemberResult.memberResult(instanceOne).get());
        TestUtils.assertFutureThrows(missingMemberResult.memberResult(instanceTwo), IllegalArgumentException.class);
    }

    @Test
    public void testMemberLevelErrorInResponseConstructor() throws InterruptedException, ExecutionException {
        RemoveMembersFromConsumerGroupResult memberLevelErrorResult = createAndVerifyMemberLevelError();
        assertThrows(IllegalArgumentException.class, () -> memberLevelErrorResult.memberResult(
            new MemberToRemove("invalid-instance-id"))
        );
    }

    @Test
    public void testNoErrorConstructor() throws ExecutionException, InterruptedException {
        Map<MemberIdentity, Errors> errorsMap = new HashMap<>();
        errorsMap.put(instanceOne.toMemberIdentity(), Errors.NONE);
        errorsMap.put(instanceTwo.toMemberIdentity(), Errors.NONE);
        RemoveMembersFromConsumerGroupResult noErrorResult =
            new RemoveMembersFromConsumerGroupResult(memberFutures, membersToRemove);
        memberFutures.complete(errorsMap);

        assertNull(noErrorResult.all().get());
        assertNull(noErrorResult.memberResult(instanceOne).get());
        assertNull(noErrorResult.memberResult(instanceTwo).get());
    }

    private RemoveMembersFromConsumerGroupResult createAndVerifyMemberLevelError() throws InterruptedException, ExecutionException {
        memberFutures.complete(errorsMap);
        assertFalse(memberFutures.isCompletedExceptionally());
        RemoveMembersFromConsumerGroupResult memberLevelErrorResult =
            new RemoveMembersFromConsumerGroupResult(memberFutures, membersToRemove);

        TestUtils.assertFutureThrows(memberLevelErrorResult.all(), FencedInstanceIdException.class);
        assertNull(memberLevelErrorResult.memberResult(instanceOne).get());
        TestUtils.assertFutureThrows(memberLevelErrorResult.memberResult(instanceTwo), FencedInstanceIdException.class);
        return memberLevelErrorResult;
    }
}
