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
package org.apache.kafka.clients.admin.internals;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;

public class RemoveMembersFromConsumerGroupHandlerTest {

    private final LogContext logContext = new LogContext();
    private final String groupId = "group-id";
    private final MemberIdentity m1 = new MemberIdentity()
            .setMemberId("m1")
            .setGroupInstanceId("m1-gii");
    private final MemberIdentity m2 = new MemberIdentity()
            .setMemberId("m2")
            .setGroupInstanceId("m2-gii");
    private final List<MemberIdentity> members = Arrays.asList(m1, m2);

    @Test
    public void testBuildRequest() {
        RemoveMembersFromConsumerGroupHandler handler = new RemoveMembersFromConsumerGroupHandler(groupId, members, logContext);
        LeaveGroupRequest request = handler.buildBatchedRequest(1, singleton(CoordinatorKey.byGroupId(groupId))).build();
        assertEquals(groupId, request.data().groupId());
        assertEquals(2, request.data().members().size());
    }

    @Test
    public void testSuccessfulHandleResponse() {
        Map<MemberIdentity, Errors> responseData = Collections.singletonMap(m1, Errors.NONE);
        assertCompleted(handleWithGroupError(Errors.NONE), responseData);
    }

    @Test
    public void testUnmappedHandleResponse() {
        assertUnmapped(handleWithGroupError(Errors.COORDINATOR_NOT_AVAILABLE));
        assertUnmapped(handleWithGroupError(Errors.NOT_COORDINATOR));
    }

    @Test
    public void testRetriableHandleResponse() {
        assertRetriable(handleWithGroupError(Errors.COORDINATOR_LOAD_IN_PROGRESS));
    }

    @Test
    public void testFailedHandleResponse() {
        assertFailed(GroupAuthorizationException.class, handleWithGroupError(Errors.GROUP_AUTHORIZATION_FAILED));
        assertFailed(UnknownServerException.class, handleWithGroupError(Errors.UNKNOWN_SERVER_ERROR));
    }

    @Test
    public void testFailedHandleResponseInMemberLevel() {
        assertMemberFailed(Errors.FENCED_INSTANCE_ID, handleWithMemberError(Errors.FENCED_INSTANCE_ID));
        assertMemberFailed(Errors.UNKNOWN_MEMBER_ID, handleWithMemberError(Errors.UNKNOWN_MEMBER_ID));
    }

    private LeaveGroupResponse buildResponse(Errors error) {
        LeaveGroupResponse response = new LeaveGroupResponse(
            new LeaveGroupResponseData()
                .setErrorCode(error.code())
                .setMembers(singletonList(
                    new MemberResponse()
                        .setErrorCode(Errors.NONE.code())
                        .setMemberId("m1")
                        .setGroupInstanceId("m1-gii"))));
        return response;
    }

    private LeaveGroupResponse buildResponseWithMemberError(Errors error) {
        LeaveGroupResponse response = new LeaveGroupResponse(
            new LeaveGroupResponseData()
                .setErrorCode(Errors.NONE.code())
                .setMembers(singletonList(
                    new MemberResponse()
                        .setErrorCode(error.code())
                        .setMemberId("m1")
                        .setGroupInstanceId("m1-gii"))));
        return response;
    }

    private AdminApiHandler.ApiResult<CoordinatorKey, Map<MemberIdentity, Errors>> handleWithGroupError(
        Errors error
    ) {
        RemoveMembersFromConsumerGroupHandler handler = new RemoveMembersFromConsumerGroupHandler(groupId, members, logContext);
        LeaveGroupResponse response = buildResponse(error);
        return handler.handleResponse(new Node(1, "host", 1234), singleton(CoordinatorKey.byGroupId(groupId)), response);
    }

    private AdminApiHandler.ApiResult<CoordinatorKey, Map<MemberIdentity, Errors>> handleWithMemberError(
        Errors error
    ) {
        RemoveMembersFromConsumerGroupHandler handler = new RemoveMembersFromConsumerGroupHandler(groupId, members, logContext);
        LeaveGroupResponse response = buildResponseWithMemberError(error);
        return handler.handleResponse(new Node(1, "host", 1234), singleton(CoordinatorKey.byGroupId(groupId)), response);
    }

    private void assertUnmapped(
        AdminApiHandler.ApiResult<CoordinatorKey, Map<MemberIdentity, Errors>> result
    ) {
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(singletonList(CoordinatorKey.byGroupId(groupId)), result.unmappedKeys);
    }

    private void assertRetriable(
        AdminApiHandler.ApiResult<CoordinatorKey, Map<MemberIdentity, Errors>> result
    ) {
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
    }

    private void assertCompleted(
        AdminApiHandler.ApiResult<CoordinatorKey, Map<MemberIdentity, Errors>> result,
        Map<MemberIdentity, Errors> expected
    ) {
        CoordinatorKey key = CoordinatorKey.byGroupId(groupId);
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(singleton(key), result.completedKeys.keySet());
        assertEquals(expected, result.completedKeys.get(key));
    }

    private void assertFailed(
        Class<? extends Throwable> expectedExceptionType,
        AdminApiHandler.ApiResult<CoordinatorKey, Map<MemberIdentity, Errors>> result
    ) {
        CoordinatorKey key = CoordinatorKey.byGroupId(groupId);
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(singleton(key), result.failedKeys.keySet());
        assertTrue(expectedExceptionType.isInstance(result.failedKeys.get(key)));
    }

    private void assertMemberFailed(
        Errors expectedError,
        AdminApiHandler.ApiResult<CoordinatorKey, Map<MemberIdentity, Errors>> result
    ) {
        Map<MemberIdentity, Errors> expectedResponseData = Collections.singletonMap(m1, expectedError);
        CoordinatorKey key = CoordinatorKey.byGroupId(groupId);
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(singleton(key), result.completedKeys.keySet());
        assertEquals(expectedResponseData, result.completedKeys.get(key));
    }
}
