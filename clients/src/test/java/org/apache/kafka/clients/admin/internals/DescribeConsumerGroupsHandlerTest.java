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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroup;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroupMember;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.DescribeGroupsResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

public class DescribeConsumerGroupsHandlerTest {

    private final LogContext logContext = new LogContext();
    private final String groupId1 = "group-id1";
    private final String groupId2 = "group-id2";
    private final Set<String> groupIds = new HashSet<>(Arrays.asList(groupId1, groupId2));
    private final Set<CoordinatorKey> keys = groupIds.stream()
            .map(CoordinatorKey::byGroupId)
            .collect(Collectors.toSet());
    private final Node coordinator = new Node(1, "host", 1234);
    private final Set<TopicPartition> tps = new HashSet<>(Arrays.asList(
            new TopicPartition("foo", 0), new TopicPartition("bar",  1)));

    @Test
    public void testBuildRequest() {
        DescribeConsumerGroupsHandler handler = new DescribeConsumerGroupsHandler(false, logContext);
        DescribeGroupsRequest request = handler.buildBatchedRequest(1, keys).build();
        assertEquals(2, request.data().groups().size());
        assertFalse(request.data().includeAuthorizedOperations());

        handler = new DescribeConsumerGroupsHandler(true, logContext);
        request = handler.buildBatchedRequest(1, keys).build();
        assertEquals(2, request.data().groups().size());
        assertTrue(request.data().includeAuthorizedOperations());
    }

    @Test
    public void testInvalidBuildRequest() {
        DescribeConsumerGroupsHandler handler = new DescribeConsumerGroupsHandler(false, logContext);
        assertThrows(IllegalArgumentException.class, () -> handler.buildRequest(1, singleton(CoordinatorKey.byTransactionalId("tId"))));
    }

    @Test
    public void testSuccessfulHandleResponse() {
        Collection<MemberDescription> members = singletonList(new MemberDescription(
                "memberId",
                "clientId",
                "host",
                new MemberAssignment(tps)));
        ConsumerGroupDescription expected = new ConsumerGroupDescription(
                groupId1,
                true,
                members,
                "assignor",
                ConsumerGroupState.STABLE,
                coordinator);
        assertCompleted(handleWithError(Errors.NONE, ""), expected);
    }

    @Test
    public void testUnmappedHandleResponse() {
        assertUnmapped(handleWithError(Errors.COORDINATOR_NOT_AVAILABLE, ""));
        assertUnmapped(handleWithError(Errors.NOT_COORDINATOR, ""));
    }

    @Test
    public void testRetriableHandleResponse() {
        assertRetriable(handleWithError(Errors.COORDINATOR_LOAD_IN_PROGRESS, ""));
    }

    @Test
    public void testFailedHandleResponse() {
        assertFailed(GroupAuthorizationException.class, handleWithError(Errors.GROUP_AUTHORIZATION_FAILED, ""));
        assertFailed(GroupIdNotFoundException.class, handleWithError(Errors.GROUP_ID_NOT_FOUND, ""));
        assertFailed(InvalidGroupIdException.class, handleWithError(Errors.INVALID_GROUP_ID, ""));
        assertFailed(IllegalArgumentException.class, handleWithError(Errors.NONE, "custom-protocol"));
    }

    private DescribeGroupsResponse buildResponse(Errors error, String protocolType) {
        DescribeGroupsResponse response = new DescribeGroupsResponse(
                new DescribeGroupsResponseData()
                    .setGroups(singletonList(
                            new DescribedGroup()
                                .setErrorCode(error.code())
                                .setGroupId(groupId1)
                                .setGroupState(ConsumerGroupState.STABLE.toString())
                                .setProtocolType(protocolType)
                                .setProtocolData("assignor")
                                .setAuthorizedOperations(Utils.to32BitField(emptySet()))
                                .setMembers(singletonList(
                                        new DescribedGroupMember()
                                            .setClientHost("host")
                                            .setClientId("clientId")
                                            .setMemberId("memberId")
                                            .setMemberAssignment(ConsumerProtocol.serializeAssignment(
                                                    new Assignment(new ArrayList<>(tps))).array())
                                            )))));
        return response;
    }

    private AdminApiHandler.ApiResult<CoordinatorKey, ConsumerGroupDescription> handleWithError(
        Errors error,
        String protocolType
    ) {
        DescribeConsumerGroupsHandler handler = new DescribeConsumerGroupsHandler(true, logContext);
        DescribeGroupsResponse response = buildResponse(error, protocolType);
        return handler.handleResponse(coordinator, singleton(CoordinatorKey.byGroupId(groupId1)), response);
    }

    private void assertUnmapped(
        AdminApiHandler.ApiResult<CoordinatorKey, ConsumerGroupDescription> result
    ) {
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(singletonList(CoordinatorKey.byGroupId(groupId1)), result.unmappedKeys);
    }

    private void assertRetriable(
        AdminApiHandler.ApiResult<CoordinatorKey, ConsumerGroupDescription> result
    ) {
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
    }

    private void assertCompleted(
        AdminApiHandler.ApiResult<CoordinatorKey, ConsumerGroupDescription> result,
        ConsumerGroupDescription expected
    ) {
        CoordinatorKey key = CoordinatorKey.byGroupId(groupId1);
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(singleton(key), result.completedKeys.keySet());
        assertEquals(expected, result.completedKeys.get(CoordinatorKey.byGroupId(groupId1)));
    }

    private void assertFailed(
        Class<? extends Throwable> expectedExceptionType,
        AdminApiHandler.ApiResult<CoordinatorKey, ConsumerGroupDescription> result
    ) {
        CoordinatorKey key = CoordinatorKey.byGroupId(groupId1);
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(singleton(key), result.failedKeys.keySet());
        assertTrue(expectedExceptionType.isInstance(result.failedKeys.get(key)));
    }
}
