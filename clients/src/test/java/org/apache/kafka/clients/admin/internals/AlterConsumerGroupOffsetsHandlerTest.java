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

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
        OffsetCommitRequest request = handler.buildBatchedRequest(-1, singleton(CoordinatorKey.byGroupId(groupId))).build();
        assertEquals(groupId, request.data().groupId());
        assertEquals(2, request.data().topics().size());
        assertEquals(2, request.data().topics().get(0).partitions().size());
        assertEquals(offset, request.data().topics().get(0).partitions().get(0).committedOffset());
    }

    @Test
    public void testHandleSuccessfulResponse() {
        AlterConsumerGroupOffsetsHandler handler = new AlterConsumerGroupOffsetsHandler(groupId, partitions, logContext);
        Map<TopicPartition, Errors> responseData = Collections.singletonMap(t0p0, Errors.NONE);
        OffsetCommitResponse response = new OffsetCommitResponse(0, responseData);
        ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> result = handler.handleResponse(node, singleton(CoordinatorKey.byGroupId(groupId)), response);
        assertCompleted(result, responseData);
    }

    @Test
    public void testHandleRetriableResponse() {
        assertUnmappedKey(partitionErrors(Errors.NOT_COORDINATOR));
        assertUnmappedKey(partitionErrors(Errors.COORDINATOR_NOT_AVAILABLE));
        assertRetriableError(partitionErrors(Errors.COORDINATOR_LOAD_IN_PROGRESS));
        assertRetriableError(partitionErrors(Errors.REBALANCE_IN_PROGRESS));
    }

    @Test
    public void testHandleErrorResponse() {
        assertFatalError(partitionErrors(Errors.TOPIC_AUTHORIZATION_FAILED));
        assertFatalError(partitionErrors(Errors.GROUP_AUTHORIZATION_FAILED));
        assertFatalError(partitionErrors(Errors.INVALID_GROUP_ID));
        assertFatalError(partitionErrors(Errors.UNKNOWN_TOPIC_OR_PARTITION));
        assertFatalError(partitionErrors(Errors.OFFSET_METADATA_TOO_LARGE));
        assertFatalError(partitionErrors(Errors.ILLEGAL_GENERATION));
        assertFatalError(partitionErrors(Errors.UNKNOWN_MEMBER_ID));
        assertFatalError(partitionErrors(Errors.INVALID_COMMIT_OFFSET_SIZE));
        assertFatalError(partitionErrors(Errors.UNKNOWN_SERVER_ERROR));
    }

    @Test
    public void testHandleMultipleErrorsResponse() {
        Map<TopicPartition, Errors> partitionErrors = new HashMap<>();
        partitionErrors.put(t0p0, Errors.UNKNOWN_TOPIC_OR_PARTITION);
        partitionErrors.put(t0p1, Errors.INVALID_COMMIT_OFFSET_SIZE);
        partitionErrors.put(t1p0, Errors.TOPIC_AUTHORIZATION_FAILED);
        partitionErrors.put(t1p1, Errors.OFFSET_METADATA_TOO_LARGE);
        assertFatalError(partitionErrors);
    }

    private AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> handleResponse(
        CoordinatorKey groupKey,
        Map<TopicPartition, OffsetAndMetadata> partitions,
        Map<TopicPartition, Errors> partitionResults
    ) {
        AlterConsumerGroupOffsetsHandler handler =
            new AlterConsumerGroupOffsetsHandler(groupKey.idValue, partitions, logContext);
        OffsetCommitResponse response = new OffsetCommitResponse(0, partitionResults);
        return handler.handleResponse(node, singleton(groupKey), response);
    }

    private Map<TopicPartition, Errors> partitionErrors(
        Errors error
    ) {
        Map<TopicPartition, Errors> partitionErrors = new HashMap<>();
        partitions.keySet().forEach(partition ->
            partitionErrors.put(partition, error)
        );
        return partitionErrors;
    }

    private void assertFatalError(
        Map<TopicPartition, Errors> partitionResults
    ) {
        CoordinatorKey groupKey = CoordinatorKey.byGroupId(groupId);
        AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> result = handleResponse(
            groupKey,
            partitions,
            partitionResults
        );

        assertEquals(singleton(groupKey), result.completedKeys.keySet());
        assertEquals(partitionResults, result.completedKeys.get(groupKey));
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(emptyMap(), result.failedKeys);
    }

    private void assertRetriableError(
        Map<TopicPartition, Errors> partitionResults
    ) {
        CoordinatorKey groupKey = CoordinatorKey.byGroupId(groupId);
        AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> result = handleResponse(
            groupKey,
            partitions,
            partitionResults
        );

        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(emptyMap(), result.failedKeys);
    }

    private void assertUnmappedKey(
        Map<TopicPartition, Errors> partitionResults
    ) {
        CoordinatorKey groupKey = CoordinatorKey.byGroupId(groupId);
        AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> result = handleResponse(
            groupKey,
            partitions,
            partitionResults
        );

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
}
