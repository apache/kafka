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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResult;
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResultCollection;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DeleteGroupsRequest;
import org.apache.kafka.common.requests.DeleteGroupsResponse;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;

public class DeleteConsumerGroupsHandlerTest {

    private final LogContext logContext = new LogContext();
    private final String groupId1 = "group-id1";

    @Test
    public void testBuildRequest() {
        DeleteConsumerGroupsHandler handler = new DeleteConsumerGroupsHandler(logContext);
        DeleteGroupsRequest request = handler.buildBatchedRequest(1, singleton(CoordinatorKey.byGroupId(groupId1))).build();
        assertEquals(1, request.data().groupsNames().size());
        assertEquals(groupId1, request.data().groupsNames().get(0));
    }

    @Test
    public void testSuccessfulHandleResponse() {
        assertCompleted(handleWithError(Errors.NONE));
    }

    @Test
    public void testUnmappedHandleResponse() {
        assertUnmapped(handleWithError(Errors.NOT_COORDINATOR));
        assertUnmapped(handleWithError(Errors.COORDINATOR_NOT_AVAILABLE));
    }

    @Test
    public void testRetriableHandleResponse() {
        assertRetriable(handleWithError(Errors.COORDINATOR_LOAD_IN_PROGRESS));
    }

    @Test
    public void testFailedHandleResponse() {
        assertFailed(GroupAuthorizationException.class, handleWithError(Errors.GROUP_AUTHORIZATION_FAILED));
        assertFailed(GroupIdNotFoundException.class, handleWithError(Errors.GROUP_ID_NOT_FOUND));
        assertFailed(InvalidGroupIdException.class, handleWithError(Errors.INVALID_GROUP_ID));
        assertFailed(GroupNotEmptyException.class, handleWithError(Errors.NON_EMPTY_GROUP));
    }

    private DeleteGroupsResponse buildResponse(Errors error) {
        return new DeleteGroupsResponse(
                new DeleteGroupsResponseData()
                    .setResults(new DeletableGroupResultCollection(singletonList(
                            new DeletableGroupResult()
                                .setErrorCode(error.code())
                                .setGroupId(groupId1)).iterator())));
    }

    private AdminApiHandler.ApiResult<CoordinatorKey, Void> handleWithError(
        Errors error
    ) {
        DeleteConsumerGroupsHandler handler = new DeleteConsumerGroupsHandler(logContext);
        DeleteGroupsResponse response = buildResponse(error);
        return handler.handleResponse(new Node(1, "host", 1234), singleton(CoordinatorKey.byGroupId(groupId1)), response);
    }

    private void assertUnmapped(
        AdminApiHandler.ApiResult<CoordinatorKey, Void> result
    ) {
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(singletonList(CoordinatorKey.byGroupId(groupId1)), result.unmappedKeys);
    }

    private void assertRetriable(
        AdminApiHandler.ApiResult<CoordinatorKey, Void> result
    ) {
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
    }

    private void assertCompleted(
        AdminApiHandler.ApiResult<CoordinatorKey, Void> result
    ) {
        CoordinatorKey key = CoordinatorKey.byGroupId(groupId1);
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(singleton(key), result.completedKeys.keySet());
    }

    private void assertFailed(
        Class<? extends Throwable> expectedExceptionType,
        AdminApiHandler.ApiResult<CoordinatorKey, Void> result
    ) {
        CoordinatorKey key = CoordinatorKey.byGroupId(groupId1);
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(singleton(key), result.failedKeys.keySet());
        assertInstanceOf(expectedExceptionType, result.failedKeys.get(key));
    }
}
