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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse.PartitionData;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;

public class ListConsumerGroupOffsetsHandlerTest {

    private final LogContext logContext = new LogContext();
    private final String groupId = "group-id";
    private final TopicPartition t0p0 = new TopicPartition("t0", 0);
    private final TopicPartition t0p1 = new TopicPartition("t0", 1);
    private final TopicPartition t1p0 = new TopicPartition("t1", 0);
    private final TopicPartition t1p1 = new TopicPartition("t1", 1);
    private final List<TopicPartition> tps = Arrays.asList(t0p0, t0p1, t1p0, t1p1);

    @Test
    public void testBuildRequest() {
        ListConsumerGroupOffsetsHandler handler = new ListConsumerGroupOffsetsHandler(groupId, tps, logContext);
        OffsetFetchRequest request = handler.buildBatchedRequest(1, singleton(CoordinatorKey.byGroupId(groupId))).build();
        assertEquals(groupId, request.data().groups().get(0).groupId());
        assertEquals(2, request.data().groups().get(0).topics().size());
        assertEquals(2, request.data().groups().get(0).topics().get(0).partitionIndexes().size());
        assertEquals(2, request.data().groups().get(0).topics().get(1).partitionIndexes().size());
    }

    @Test
    public void testSuccessfulHandleResponse() {
        Map<TopicPartition, OffsetAndMetadata> expected = new HashMap<>();
        assertCompleted(handleWithError(Errors.NONE), expected);
    }


    @Test
    public void testSuccessfulHandleResponseWithOnePartitionError() {
        Map<TopicPartition, OffsetAndMetadata> expectedResult = Collections.singletonMap(t0p0, new OffsetAndMetadata(10L));

        // expected that there's only 1 partition result returned because the other partition is skipped with error
        assertCompleted(handleWithPartitionError(Errors.UNKNOWN_TOPIC_OR_PARTITION), expectedResult);
        assertCompleted(handleWithPartitionError(Errors.TOPIC_AUTHORIZATION_FAILED), expectedResult);
        assertCompleted(handleWithPartitionError(Errors.UNSTABLE_OFFSET_COMMIT), expectedResult);
    }

    @Test
    public void testUnmappedHandleResponse() {
        assertUnmapped(handleWithError(Errors.COORDINATOR_NOT_AVAILABLE));
        assertUnmapped(handleWithError(Errors.NOT_COORDINATOR));
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
    }

    private OffsetFetchResponse buildResponse(Errors error) {
        Map<TopicPartition, PartitionData> responseData = new HashMap<>();
        OffsetFetchResponse response = new OffsetFetchResponse(error, responseData);
        return response;
    }

    private OffsetFetchResponse buildResponseWithPartitionError(Errors error) {

        Map<TopicPartition, PartitionData> responseData = new HashMap<>();
        responseData.put(t0p0, new OffsetFetchResponse.PartitionData(10, Optional.empty(), "", Errors.NONE));
        responseData.put(t0p1, new OffsetFetchResponse.PartitionData(10, Optional.empty(), "", error));

        OffsetFetchResponse response = new OffsetFetchResponse(Errors.NONE, responseData);
        return response;
    }

    private AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> handleWithPartitionError(
        Errors error
    ) {
        ListConsumerGroupOffsetsHandler handler = new ListConsumerGroupOffsetsHandler(groupId, tps, logContext);
        OffsetFetchResponse response = buildResponseWithPartitionError(error);
        return handler.handleResponse(new Node(1, "host", 1234), singleton(CoordinatorKey.byGroupId(groupId)), response);
    }

    private AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> handleWithError(
        Errors error
    ) {
        ListConsumerGroupOffsetsHandler handler = new ListConsumerGroupOffsetsHandler(groupId, tps, logContext);
        OffsetFetchResponse response = buildResponse(error);
        return handler.handleResponse(new Node(1, "host", 1234), singleton(CoordinatorKey.byGroupId(groupId)), response);
    }

    private void assertUnmapped(
        AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> result
    ) {
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(singletonList(CoordinatorKey.byGroupId(groupId)), result.unmappedKeys);
    }

    private void assertRetriable(
        AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> result
    ) {
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
    }

    private void assertCompleted(
        AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> result,
        Map<TopicPartition, OffsetAndMetadata> expected
    ) {
        CoordinatorKey key = CoordinatorKey.byGroupId(groupId);
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(singleton(key), result.completedKeys.keySet());
        assertEquals(expected, result.completedKeys.get(CoordinatorKey.byGroupId(groupId)));
    }

    private void assertFailed(
        Class<? extends Throwable> expectedExceptionType,
        AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> result
    ) {
        CoordinatorKey key = CoordinatorKey.byGroupId(groupId);
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(singleton(key), result.failedKeys.keySet());
        assertTrue(expectedExceptionType.isInstance(result.failedKeys.get(key)));
    }
}
