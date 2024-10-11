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

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ConsumerGroupDescribeRequestData;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroup;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroupMember;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupDescribeResponse;
import org.apache.kafka.common.requests.DescribeGroupsResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DescribeConsumerGroupsHandlerTest {

    private final LogContext logContext = new LogContext();
    private final String groupId1 = "group-id1";
    private final String groupId2 = "group-id2";
    private final Set<String> groupIds = new LinkedHashSet<>(Arrays.asList(
        groupId1,
        groupId2
    ));
    private final Set<CoordinatorKey> keys = new LinkedHashSet<>(Arrays.asList(
        CoordinatorKey.byGroupId(groupId1),
        CoordinatorKey.byGroupId(groupId2)
    ));
    private final Node coordinator = new Node(1, "host", 1234);
    private final Set<TopicPartition> tps = new HashSet<>(Arrays.asList(
        new TopicPartition("foo", 0),
        new TopicPartition("bar",  1)
    ));

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testBuildRequestWithMultipleGroupTypes(boolean includeAuthorizedOperations) {
        DescribeConsumerGroupsHandler handler = new DescribeConsumerGroupsHandler(includeAuthorizedOperations, logContext);

        // Build request for the two groups. It should return one request for
        // the new describe group API.
        Collection<AdminApiHandler.RequestAndKeys<CoordinatorKey>> requestAndKeys = handler.buildRequest(1, keys);
        assertEquals(1, requestAndKeys.size());
        assertRequestAndKeys(
            requestAndKeys.iterator().next(),
            keys,
            new ConsumerGroupDescribeRequestData()
                .setGroupIds(new ArrayList<>(groupIds))
                .setIncludeAuthorizedOperations(includeAuthorizedOperations)
        );

        // Handle the response. We return a retriable error for the first group
        // and a GROUP_ID_NOT_FOUND for the second one. The GROUP_ID_NOT_FOUND
        // means that the group must be described with the classic API.
        handler.handleResponse(
            coordinator,
            keys,
            new ConsumerGroupDescribeResponse(new ConsumerGroupDescribeResponseData()
                .setGroups(Arrays.asList(
                    new ConsumerGroupDescribeResponseData.DescribedGroup()
                        .setGroupId(groupId1)
                        .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code()),
                    new ConsumerGroupDescribeResponseData.DescribedGroup()
                        .setGroupId(groupId2)
                        .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
                ))
            )
        );

        // Build request. It should return one request using the new describe API
        // for the first group and one request using the classic describe API for
        // the second group.
        requestAndKeys = handler.buildRequest(1, keys);
        assertEquals(2, requestAndKeys.size(), requestAndKeys.toString());
        Iterator<AdminApiHandler.RequestAndKeys<CoordinatorKey>> iterator = requestAndKeys.iterator();

        assertRequestAndKeys(
            iterator.next(),
            Collections.singleton(CoordinatorKey.byGroupId(groupId1)),
            new ConsumerGroupDescribeRequestData()
                .setGroupIds(Collections.singletonList(groupId1))
                .setIncludeAuthorizedOperations(includeAuthorizedOperations)
        );

        assertRequestAndKeys(
            iterator.next(),
            Collections.singleton(CoordinatorKey.byGroupId(groupId2)),
            new DescribeGroupsRequestData()
                .setGroups(Collections.singletonList(groupId2))
                .setIncludeAuthorizedOperations(includeAuthorizedOperations)
        );
    }

    @Test
    public void testInvalidBuildRequest() {
        DescribeConsumerGroupsHandler handler = new DescribeConsumerGroupsHandler(false, logContext);
        assertThrows(IllegalArgumentException.class, () -> handler.buildRequest(1, singleton(CoordinatorKey.byTransactionalId("tId"))));
    }

    @Test
    public void testSuccessfulHandleConsumerGroupResponse() {
        DescribeConsumerGroupsHandler handler = new DescribeConsumerGroupsHandler(false, logContext);
        Collection<MemberDescription> members = singletonList(new MemberDescription(
            "memberId",
            Optional.of("instanceId"),
            "clientId",
            "host",
            new MemberAssignment(Set.of(
                new TopicPartition("foo", 0),
                new TopicPartition("bar",  1))
            ),
            Optional.of(new MemberAssignment(Set.of(
                new TopicPartition("foo", 1),
                new TopicPartition("bar",  2)
            )))
        ));
        ConsumerGroupDescription expected = new ConsumerGroupDescription(
            groupId1,
            false,
            members,
            "range",
            GroupType.CONSUMER,
            ConsumerGroupState.STABLE,
            coordinator,
            Collections.emptySet()
        );
        AdminApiHandler.ApiResult<CoordinatorKey, ConsumerGroupDescription> result = handler.handleResponse(
            coordinator,
            Collections.singleton(CoordinatorKey.byGroupId(groupId1)),
            new ConsumerGroupDescribeResponse(
                new ConsumerGroupDescribeResponseData()
                    .setGroups(Collections.singletonList(
                        new ConsumerGroupDescribeResponseData.DescribedGroup()
                            .setGroupId(groupId1)
                            .setGroupState("Stable")
                            .setGroupEpoch(10)
                            .setAssignmentEpoch(10)
                            .setAssignorName("range")
                            .setAuthorizedOperations(Utils.to32BitField(emptySet()))
                            .setMembers(singletonList(
                                new ConsumerGroupDescribeResponseData.Member()
                                    .setMemberId("memberId")
                                    .setInstanceId("instanceId")
                                    .setClientHost("host")
                                    .setClientId("clientId")
                                    .setMemberEpoch(10)
                                    .setRackId("rackid")
                                    .setSubscribedTopicNames(singletonList("foo"))
                                    .setSubscribedTopicRegex("regex")
                                    .setAssignment(new ConsumerGroupDescribeResponseData.Assignment()
                                        .setTopicPartitions(Arrays.asList(
                                            new ConsumerGroupDescribeResponseData.TopicPartitions()
                                                .setTopicId(Uuid.randomUuid())
                                                .setTopicName("foo")
                                                .setPartitions(Collections.singletonList(0)),
                                            new ConsumerGroupDescribeResponseData.TopicPartitions()
                                                .setTopicId(Uuid.randomUuid())
                                                .setTopicName("bar")
                                                .setPartitions(Collections.singletonList(1))
                                        )))
                                    .setTargetAssignment(new ConsumerGroupDescribeResponseData.Assignment()
                                        .setTopicPartitions(Arrays.asList(
                                            new ConsumerGroupDescribeResponseData.TopicPartitions()
                                                .setTopicId(Uuid.randomUuid())
                                                .setTopicName("foo")
                                                .setPartitions(Collections.singletonList(1)),
                                            new ConsumerGroupDescribeResponseData.TopicPartitions()
                                                .setTopicId(Uuid.randomUuid())
                                                .setTopicName("bar")
                                                .setPartitions(Collections.singletonList(2))
                                        )))
                            ))
                    ))
            )
        );
        assertCompleted(result, expected);
    }

    @Test
    public void testSuccessfulHandleClassicGroupResponse() {
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
        assertCompleted(handleClassicGroupWithError(Errors.NONE, ""), expected);
    }

    @Test
    public void testUnmappedHandleClassicGroupResponse() {
        assertUnmapped(handleClassicGroupWithError(Errors.COORDINATOR_NOT_AVAILABLE, ""));
        assertUnmapped(handleClassicGroupWithError(Errors.NOT_COORDINATOR, ""));
    }

    @Test
    public void testRetriableHandleClassicGroupResponse() {
        assertRetriable(handleClassicGroupWithError(Errors.COORDINATOR_LOAD_IN_PROGRESS, ""));
    }

    @Test
    public void testFailedHandleClassicGroupResponse() {
        assertFailed(UnsupportedVersionException.class, handleClassicGroupWithError(Errors.UNSUPPORTED_VERSION, ""));
        assertFailed(GroupAuthorizationException.class, handleClassicGroupWithError(Errors.GROUP_AUTHORIZATION_FAILED, ""));
        assertFailed(GroupIdNotFoundException.class, handleClassicGroupWithError(Errors.GROUP_ID_NOT_FOUND, ""));
        assertFailed(InvalidGroupIdException.class, handleClassicGroupWithError(Errors.INVALID_GROUP_ID, ""));
        assertFailed(IllegalArgumentException.class, handleClassicGroupWithError(Errors.NONE, "custom-protocol"));
    }

    @Test
    public void testUnmappedHandleConsumerGroupResponse() {
        assertUnmapped(handleConsumerGroupWithError(Errors.COORDINATOR_NOT_AVAILABLE));
        assertUnmapped(handleConsumerGroupWithError(Errors.NOT_COORDINATOR));
    }

    @Test
    public void testRetriableHandleConsumerGroupResponse() {
        assertRetriable(handleConsumerGroupWithError(Errors.COORDINATOR_LOAD_IN_PROGRESS));
        assertRetriable(handleConsumerGroupWithError(Errors.GROUP_ID_NOT_FOUND));
        assertRetriable(handleConsumerGroupWithError(Errors.UNSUPPORTED_VERSION));
    }

    @Test
    public void testFailedHandleConsumerGroupResponse() {
        assertFailed(GroupAuthorizationException.class, handleConsumerGroupWithError(Errors.GROUP_AUTHORIZATION_FAILED));
        assertFailed(InvalidGroupIdException.class, handleConsumerGroupWithError(Errors.INVALID_GROUP_ID));
    }

    private ConsumerGroupDescribeResponse buildConsumerGroupDescribeResponse(Errors error) {
        return new ConsumerGroupDescribeResponse(
            new ConsumerGroupDescribeResponseData()
                .setGroups(Collections.singletonList(
                    new ConsumerGroupDescribeResponseData.DescribedGroup()
                        .setGroupId(groupId1)
                        .setErrorCode(error.code())
                ))
        );
    }

    private DescribeGroupsResponse buildResponse(Errors error, String protocolType) {
        return new DescribeGroupsResponse(
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
    }

    private AdminApiHandler.ApiResult<CoordinatorKey, ConsumerGroupDescription> handleClassicGroupWithError(
        Errors error,
        String protocolType
    ) {
        DescribeConsumerGroupsHandler handler = new DescribeConsumerGroupsHandler(true, logContext);
        DescribeGroupsResponse response = buildResponse(error, protocolType);
        return handler.handleResponse(coordinator, singleton(CoordinatorKey.byGroupId(groupId1)), response);
    }

    private AdminApiHandler.ApiResult<CoordinatorKey, ConsumerGroupDescription> handleConsumerGroupWithError(
        Errors error
    ) {
        DescribeConsumerGroupsHandler handler = new DescribeConsumerGroupsHandler(true, logContext);
        ConsumerGroupDescribeResponse response = buildConsumerGroupDescribeResponse(error);
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
        assertInstanceOf(expectedExceptionType, result.failedKeys.get(key));
    }

    private void assertRequestAndKeys(
        AdminApiHandler.RequestAndKeys<CoordinatorKey> requestAndKeys,
        Set<CoordinatorKey> expectedKeys,
        ApiMessage expectedRequest
    ) {
        assertEquals(expectedKeys, requestAndKeys.keys);
        assertEquals(expectedRequest, requestAndKeys.request.build().data());
    }
}
