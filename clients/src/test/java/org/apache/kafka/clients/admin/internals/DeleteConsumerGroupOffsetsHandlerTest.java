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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartition;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopic;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.OffsetDeleteRequest;
import org.apache.kafka.common.requests.OffsetDeleteResponse;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;

public class DeleteConsumerGroupOffsetsHandlerTest {

    private final LogContext logContext = new LogContext();
    private final String groupId = "group-id";
    private final TopicPartition t0p0 = new TopicPartition("t0", 0);
    private final TopicPartition t0p1 = new TopicPartition("t0", 1);
    private final TopicPartition t1p0 = new TopicPartition("t1", 0);
    private final Set<TopicPartition> tps = new HashSet<>(Arrays.asList(t0p0, t0p1, t1p0));

    @Test
    public void testBuildRequest() {
        DeleteConsumerGroupOffsetsHandler handler = new DeleteConsumerGroupOffsetsHandler(groupId, tps, logContext);
        OffsetDeleteRequest request = handler.buildBatchedRequest(1, singleton(CoordinatorKey.byGroupId(groupId))).build();
        assertEquals(groupId, request.data().groupId());
        assertEquals(2, request.data().topics().size());
        assertEquals(2, request.data().topics().find("t0").partitions().size());
        assertEquals(1, request.data().topics().find("t1").partitions().size());
    }

    @Test
    public void testSuccessfulHandleResponse() {
        Map<TopicPartition, Errors> responseData = Collections.singletonMap(t0p0, Errors.NONE);
        assertCompleted(handleWithGroupError(Errors.NONE), responseData);
    }

    @Test
    public void testUnmappedHandleResponse() {
        assertUnmapped(handleWithGroupError(Errors.NOT_COORDINATOR));
        assertUnmapped(handleWithGroupError(Errors.COORDINATOR_NOT_AVAILABLE));
    }

    @Test
    public void testRetriableHandleResponse() {
        assertRetriable(handleWithGroupError(Errors.COORDINATOR_LOAD_IN_PROGRESS));
    }

    @Test
    public void testFailedHandleResponseWithGroupError() {
        assertGroupFailed(GroupAuthorizationException.class, handleWithGroupError(Errors.GROUP_AUTHORIZATION_FAILED));
        assertGroupFailed(GroupIdNotFoundException.class, handleWithGroupError(Errors.GROUP_ID_NOT_FOUND));
        assertGroupFailed(InvalidGroupIdException.class, handleWithGroupError(Errors.INVALID_GROUP_ID));
        assertGroupFailed(GroupNotEmptyException.class, handleWithGroupError(Errors.NON_EMPTY_GROUP));
    }

    @Test
    public void testFailedHandleResponseWithPartitionError() {
        assertPartitionFailed(Collections.singletonMap(t0p0, Errors.GROUP_SUBSCRIBED_TO_TOPIC),
            handleWithPartitionError(Errors.GROUP_SUBSCRIBED_TO_TOPIC));
        assertPartitionFailed(Collections.singletonMap(t0p0, Errors.TOPIC_AUTHORIZATION_FAILED),
            handleWithPartitionError(Errors.TOPIC_AUTHORIZATION_FAILED));
        assertPartitionFailed(Collections.singletonMap(t0p0, Errors.UNKNOWN_TOPIC_OR_PARTITION),
            handleWithPartitionError(Errors.UNKNOWN_TOPIC_OR_PARTITION));
    }

    private OffsetDeleteResponse buildGroupErrorResponse(Errors error) {
        OffsetDeleteResponse response = new OffsetDeleteResponse(
            new OffsetDeleteResponseData()
                .setErrorCode(error.code()));
        if (error == Errors.NONE) {
            response.data()
                .setThrottleTimeMs(0)
                .setTopics(new OffsetDeleteResponseTopicCollection(singletonList(
                    new OffsetDeleteResponseTopic()
                        .setName(t0p0.topic())
                        .setPartitions(new OffsetDeleteResponsePartitionCollection(singletonList(
                            new OffsetDeleteResponsePartition()
                                .setPartitionIndex(t0p0.partition())
                                .setErrorCode(error.code())
                        ).iterator()))
                ).iterator()));
        }
        return response;
    }

    private OffsetDeleteResponse buildPartitionErrorResponse(Errors error) {
        OffsetDeleteResponse response = new OffsetDeleteResponse(
            new OffsetDeleteResponseData()
                .setThrottleTimeMs(0)
                .setTopics(new OffsetDeleteResponseTopicCollection(singletonList(
                    new OffsetDeleteResponseTopic()
                        .setName(t0p0.topic())
                        .setPartitions(new OffsetDeleteResponsePartitionCollection(singletonList(
                            new OffsetDeleteResponsePartition()
                                .setPartitionIndex(t0p0.partition())
                                .setErrorCode(error.code())
                        ).iterator()))
                ).iterator()))
        );
        return response;
    }

    private AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> handleWithGroupError(
        Errors error
    ) {
        DeleteConsumerGroupOffsetsHandler handler = new DeleteConsumerGroupOffsetsHandler(groupId, tps, logContext);
        OffsetDeleteResponse response = buildGroupErrorResponse(error);
        return handler.handleResponse(new Node(1, "host", 1234), singleton(CoordinatorKey.byGroupId(groupId)), response);
    }

    private AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> handleWithPartitionError(
        Errors error
    ) {
        DeleteConsumerGroupOffsetsHandler handler = new DeleteConsumerGroupOffsetsHandler(groupId, tps, logContext);
        OffsetDeleteResponse response = buildPartitionErrorResponse(error);
        return handler.handleResponse(new Node(1, "host", 1234), singleton(CoordinatorKey.byGroupId(groupId)), response);
    }

    private void assertUnmapped(
        AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> result
    ) {
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(singletonList(CoordinatorKey.byGroupId(groupId)), result.unmappedKeys);
    }

    private void assertRetriable(
        AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> result
    ) {
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
    }

    private void assertCompleted(
        AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> result,
        Map<TopicPartition, Errors> expected
    ) {
        CoordinatorKey key = CoordinatorKey.byGroupId(groupId);
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(singleton(key), result.completedKeys.keySet());
        assertEquals(expected, result.completedKeys.get(key));
    }

    private void assertGroupFailed(
        Class<? extends Throwable> expectedExceptionType,
        AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> result
    ) {
        CoordinatorKey key = CoordinatorKey.byGroupId(groupId);
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(singleton(key), result.failedKeys.keySet());
        assertTrue(expectedExceptionType.isInstance(result.failedKeys.get(key)));
    }

    private void assertPartitionFailed(
        Map<TopicPartition, Errors> expectedResult,
        AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> result
    ) {
        CoordinatorKey key = CoordinatorKey.byGroupId(groupId);
        assertEquals(singleton(key), result.completedKeys.keySet());

        // verify the completed value is expected result
        Collection<Map<TopicPartition, Errors>> completeCollection = result.completedKeys.values();
        assertEquals(1, completeCollection.size());
        assertEquals(expectedResult, result.completedKeys.get(key));

        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(emptySet(), result.failedKeys.keySet());
    }
}
