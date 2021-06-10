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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.utils.LogContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AlterConsumerGroupOffsetsHandlerTest {

    private final LogContext logContext = new LogContext();
    private final String groupId = "group-id";
    private final TopicPartition t0p0 = new TopicPartition("t0", 0);
    private final TopicPartition t0p1 = new TopicPartition("t0", 1);
    private final TopicPartition t1p0 = new TopicPartition("t1", 0);
    private final TopicPartition t1p1 = new TopicPartition("t1", 1);
    private final Map<TopicPartition, OffsetAndMetadata> partitions = new HashMap<>();
    private final long offset = 1L;
    private final Node node = new Node(1, "host", 1234);

    @BeforeEach
    public void setUp() {
        partitions.put(t0p0, new OffsetAndMetadata(offset));
        partitions.put(t0p1, new OffsetAndMetadata(offset));
        partitions.put(t1p0, new OffsetAndMetadata(offset));
        partitions.put(t1p1, new OffsetAndMetadata(offset));
    }

    @Test
    public void testBuildRequest() {
        AlterConsumerGroupOffsetsHandler handler = new AlterConsumerGroupOffsetsHandler(groupId, partitions, logContext);
        OffsetCommitRequest request = handler.buildRequest(-1, singleton(CoordinatorKey.byGroupId(groupId))).build();
        assertEquals(groupId, request.data().groupId());
        assertEquals(2, request.data().topics().size());
        assertEquals(2, request.data().topics().get(0).partitions().size());
        assertEquals(offset, request.data().topics().get(0).partitions().get(0).committedOffset());
    }

    @Test
    public void testSuccessfulHandleResponse() {
        AlterConsumerGroupOffsetsHandler handler = new AlterConsumerGroupOffsetsHandler(groupId, partitions, logContext);
        Map<TopicPartition, Errors> responseData = Collections.singletonMap(t0p0, Errors.NONE);
        OffsetCommitResponse response = new OffsetCommitResponse(0, responseData);
        ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> result = handler.handleResponse(node, singleton(CoordinatorKey.byGroupId(groupId)), response);
        assertCompleted(result, responseData);
    }

    @Test
    public void testRetriableHandleResponse() {
        assertUnmapped(handleWithError(Errors.NOT_COORDINATOR));
        assertUnmapped(handleWithError(Errors.COORDINATOR_LOAD_IN_PROGRESS));
        assertUnmapped(handleWithError(Errors.COORDINATOR_NOT_AVAILABLE));
    }

    @Test
    public void testFailedHandleResponse() {
        assertFailed(GroupAuthorizationException.class, handleWithError(Errors.GROUP_AUTHORIZATION_FAILED));
        assertFailed(UnknownServerException.class, handleWithError(Errors.UNKNOWN_SERVER_ERROR));
    }

    private AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> handleWithError(
        Errors error
    ) {
        AlterConsumerGroupOffsetsHandler handler = new AlterConsumerGroupOffsetsHandler(groupId, partitions, logContext);
        Map<TopicPartition, Errors> responseData = Collections.singletonMap(t0p0, error);
        OffsetCommitResponse response = new OffsetCommitResponse(0, responseData);
        return handler.handleResponse(node, singleton(CoordinatorKey.byGroupId(groupId)), response);
    }

    private void assertUnmapped(
        AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> result
    ) {
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(singletonList(CoordinatorKey.byGroupId(groupId)), result.unmappedKeys);
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

    private void assertFailed(
        Class<? extends Throwable> expectedExceptionType,
        AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> result
    ) {
        CoordinatorKey key = CoordinatorKey.byGroupId(groupId);
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(singleton(key), result.failedKeys.keySet());
        assertTrue(expectedExceptionType.isInstance(result.failedKeys.get(key)));
    }
}
