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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestGroup;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse.PartitionData;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;

public class ListConsumerGroupOffsetsHandlerTest {

    private final LogContext logContext = new LogContext();
    private final int throttleMs = 10;
    private final String groupZero = "group0";
    private final String groupOne = "group1";
    private final String groupTwo = "group2";
    private final TopicPartition t0p0 = new TopicPartition("t0", 0);
    private final TopicPartition t0p1 = new TopicPartition("t0", 1);
    private final TopicPartition t1p0 = new TopicPartition("t1", 0);
    private final TopicPartition t1p1 = new TopicPartition("t1", 1);
    private final TopicPartition t2p0 = new TopicPartition("t2", 0);
    private final TopicPartition t2p1 = new TopicPartition("t2", 1);
    private final TopicPartition t2p2 = new TopicPartition("t2", 2);
    private final List<TopicPartition> tps = Arrays.asList(t0p0, t0p1, t1p0, t1p1);
    private final List<TopicPartition> tp0 = singletonList(t0p0);
    private final List<TopicPartition> tp1 = Arrays.asList(t0p0, t1p0, t1p1);
    private final List<TopicPartition> tp2 = Arrays.asList(t0p0, t1p0, t1p1, t2p0, t2p1, t2p2);
    private final Map<String, List<TopicPartition>> requestMap =
        new HashMap<String, List<TopicPartition>>() {{
            put(groupZero, tp0);
            put(groupOne, tp1);
            put(groupTwo, tp2);
        }};

    @Test
    public void testBuildRequest() {
        ListConsumerGroupOffsetsHandler handler =
            new ListConsumerGroupOffsetsHandler(Collections.singletonMap(groupZero, tps), false, logContext);
        OffsetFetchRequest request = handler.buildBatchedRequest(1, singleton(CoordinatorKey.byGroupId(
            groupZero))).build();
        assertEquals(groupZero, request.data().groups().get(0).groupId());
        assertEquals(2, request.data().groups().get(0).topics().size());
        assertEquals(2, request.data().groups().get(0).topics().get(0).partitionIndexes().size());
        assertEquals(2, request.data().groups().get(0).topics().get(1).partitionIndexes().size());
    }

    @Test
    public void testBuildRequestWithMultipleGroups() {
        ListConsumerGroupOffsetsHandler handler = new ListConsumerGroupOffsetsHandler(requestMap, false, logContext);
        OffsetFetchRequest request = handler.buildBatchedRequest(
            1,
            new HashSet<>(Arrays.asList(
                CoordinatorKey.byGroupId(groupZero),
                CoordinatorKey.byGroupId(groupOne),
                CoordinatorKey.byGroupId(groupTwo)))).build();

        assertEquals(new HashSet<>(Arrays.asList(groupZero, groupOne, groupTwo)),
            request.data().groups()
                .stream()
                .map(OffsetFetchRequestGroup::groupId)
                .collect(Collectors.toSet()));

        assertEquals(requestMap, request.groupIdsToPartitions());
        Map<String, List<OffsetFetchRequestTopics>> groupIdsToTopics = request.groupIdsToTopics();

        assertEquals(1, groupIdsToTopics.get(groupZero).size());
        assertEquals(2, groupIdsToTopics.get(groupOne).size());
        assertEquals(3, groupIdsToTopics.get(groupTwo).size());

        assertEquals(1, groupIdsToTopics.get(groupZero).get(0).partitionIndexes().size());
        assertEquals(1, groupIdsToTopics.get(groupOne).get(0).partitionIndexes().size());
        assertEquals(2, groupIdsToTopics.get(groupOne).get(1).partitionIndexes().size());
        assertEquals(1, groupIdsToTopics.get(groupTwo).get(0).partitionIndexes().size());
        assertEquals(2, groupIdsToTopics.get(groupTwo).get(1).partitionIndexes().size());
        assertEquals(3, groupIdsToTopics.get(groupTwo).get(2).partitionIndexes().size());
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
    public void testSuccessfulHandleResponseWithOnePartitionErrorWithMultipleGroups() {
        Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMapZero =
            Collections.singletonMap(t0p0, new OffsetAndMetadata(10L));
        Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMapOne =
            Collections.singletonMap(t1p1, new OffsetAndMetadata(10L));
        Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMapTwo =
            Collections.singletonMap(t2p2, new OffsetAndMetadata(10L));
        Map<String, Map<TopicPartition, OffsetAndMetadata>> expectedResult =
            new HashMap<String, Map<TopicPartition, OffsetAndMetadata>>() {{
                put(groupZero, offsetAndMetadataMapZero);
                put(groupOne, offsetAndMetadataMapOne);
                put(groupTwo, offsetAndMetadataMapTwo);
            }};

        assertCompletedForMultipleGroups(
            handleWithPartitionErrorMultipleGroups(Errors.UNKNOWN_TOPIC_OR_PARTITION), expectedResult);
        assertCompletedForMultipleGroups(
            handleWithPartitionErrorMultipleGroups(Errors.TOPIC_AUTHORIZATION_FAILED), expectedResult);
        assertCompletedForMultipleGroups(
            handleWithPartitionErrorMultipleGroups(Errors.UNSTABLE_OFFSET_COMMIT), expectedResult);
    }

    @Test
    public void testSuccessfulHandleResponseWithMultipleGroups() {
        Map<String, Map<TopicPartition, OffsetAndMetadata>> expected = new HashMap<>();
        Map<String, Errors> errorMap = new HashMap<>();
        errorMap.put(groupZero, Errors.NONE);
        errorMap.put(groupOne, Errors.NONE);
        errorMap.put(groupTwo, Errors.NONE);
        Map<String, List<TopicPartition>> partitionMap = new HashMap<>();
        partitionMap.put(groupZero, tp0);
        partitionMap.put(groupOne, tp1);
        partitionMap.put(groupTwo, tp2);
        assertCompletedForMultipleGroups(handleWithErrorWithMultipleGroups(errorMap, partitionMap), expected);
    }

    @Test
    public void testUnmappedHandleResponse() {
        assertUnmapped(handleWithError(Errors.COORDINATOR_NOT_AVAILABLE));
        assertUnmapped(handleWithError(Errors.NOT_COORDINATOR));
    }

    @Test
    public void testUnmappedHandleResponseWithMultipleGroups() {
        Map<String, Errors> errorMap = new HashMap<>();
        errorMap.put(groupZero, Errors.NOT_COORDINATOR);
        errorMap.put(groupOne, Errors.COORDINATOR_NOT_AVAILABLE);
        errorMap.put(groupTwo, Errors.NOT_COORDINATOR);
        Map<String, List<TopicPartition>> partitionMap = new HashMap<>();
        partitionMap.put(groupZero, tp0);
        partitionMap.put(groupOne, tp1);
        partitionMap.put(groupTwo, tp2);
        assertUnmappedWithMultipleGroups(handleWithErrorWithMultipleGroups(errorMap, partitionMap));
    }

    @Test
    public void testRetriableHandleResponse() {
        assertRetriable(handleWithError(Errors.COORDINATOR_LOAD_IN_PROGRESS));
    }

    @Test
    public void testRetriableHandleResponseWithMultipleGroups() {
        Map<String, Errors> errorMap = new HashMap<>();
        errorMap.put(groupZero, Errors.COORDINATOR_LOAD_IN_PROGRESS);
        errorMap.put(groupOne, Errors.COORDINATOR_LOAD_IN_PROGRESS);
        errorMap.put(groupTwo, Errors.COORDINATOR_LOAD_IN_PROGRESS);
        Map<String, List<TopicPartition>> partitionMap = new HashMap<>();
        partitionMap.put(groupZero, tp0);
        partitionMap.put(groupOne, tp1);
        partitionMap.put(groupTwo, tp2);
        assertRetriable(handleWithErrorWithMultipleGroups(errorMap, partitionMap));
    }

    @Test
    public void testFailedHandleResponse() {
        assertFailed(GroupAuthorizationException.class, handleWithError(Errors.GROUP_AUTHORIZATION_FAILED));
        assertFailed(GroupIdNotFoundException.class, handleWithError(Errors.GROUP_ID_NOT_FOUND));
        assertFailed(InvalidGroupIdException.class, handleWithError(Errors.INVALID_GROUP_ID));
    }

    @Test
    public void testFailedHandleResponseWithMultipleGroups() {
        Map<String, Errors> errorMap = new HashMap<>();
        errorMap.put(groupZero, Errors.GROUP_AUTHORIZATION_FAILED);
        errorMap.put(groupOne, Errors.GROUP_ID_NOT_FOUND);
        errorMap.put(groupTwo, Errors.INVALID_GROUP_ID);
        Map<String, List<TopicPartition>> partitionMap = new HashMap<>();
        partitionMap.put(groupZero, tp0);
        partitionMap.put(groupOne, tp1);
        partitionMap.put(groupTwo, tp2);
        Map<String, Class<? extends Throwable>> groupToExceptionMap = new HashMap<>();
        groupToExceptionMap.put(groupZero, GroupAuthorizationException.class);
        groupToExceptionMap.put(groupOne, GroupIdNotFoundException.class);
        groupToExceptionMap.put(groupTwo, InvalidGroupIdException.class);
        assertFailedForMultipleGroups(groupToExceptionMap,
            handleWithErrorWithMultipleGroups(errorMap, partitionMap));
    }

    private OffsetFetchResponse buildResponse(Errors error) {
        return new OffsetFetchResponse(
            throttleMs,
            Collections.singletonMap(groupZero, error),
            Collections.singletonMap(groupZero, new HashMap<>()));
    }

    private OffsetFetchResponse buildResponseWithMultipleGroups(
        Map<String, Errors> errorMap,
        Map<String, Map<TopicPartition, PartitionData>> responseData) {
        return new OffsetFetchResponse(throttleMs, errorMap, responseData);
    }

    private AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> handleWithErrorWithMultipleGroups(
        Map<String, Errors> errorMap,
        Map<String, List<TopicPartition>> groupToPartitionMap) {
        ListConsumerGroupOffsetsHandler handler = new ListConsumerGroupOffsetsHandler(groupToPartitionMap, false, logContext);
        Map<String, Map<TopicPartition, PartitionData>> responseData = new HashMap<>();
        for (String group : errorMap.keySet()) {
            responseData.put(group, new HashMap<>());
        }
        OffsetFetchResponse response = buildResponseWithMultipleGroups(errorMap, responseData);
        return handler.handleResponse(new Node(1, "host", 1234),
            errorMap.keySet()
                .stream()
                .map(CoordinatorKey::byGroupId)
                .collect(Collectors.toSet()),
            response);
    }

    private OffsetFetchResponse buildResponseWithPartitionError(Errors error) {

        Map<TopicPartition, PartitionData> responseData = new HashMap<>();
        responseData.put(t0p0, new OffsetFetchResponse.PartitionData(10, Optional.empty(), "", Errors.NONE));
        responseData.put(t0p1, new OffsetFetchResponse.PartitionData(10, Optional.empty(), "", error));

        return new OffsetFetchResponse(Errors.NONE, responseData);
    }

    private OffsetFetchResponse buildResponseWithPartitionErrorWithMultipleGroups(Errors error) {
        Map<TopicPartition, PartitionData> responseDataZero = new HashMap<>();
        responseDataZero.put(t0p0, new OffsetFetchResponse.PartitionData(10, Optional.empty(), "", Errors.NONE));

        Map<TopicPartition, PartitionData> responseDataOne = new HashMap<>();
        responseDataOne.put(t0p0, new OffsetFetchResponse.PartitionData(10, Optional.empty(), "", error));
        responseDataOne.put(t1p0, new OffsetFetchResponse.PartitionData(10, Optional.empty(), "", error));
        responseDataOne.put(t1p1, new OffsetFetchResponse.PartitionData(10, Optional.empty(), "", Errors.NONE));

        Map<TopicPartition, PartitionData> responseDataTwo = new HashMap<>();
        responseDataTwo.put(t0p0, new OffsetFetchResponse.PartitionData(10, Optional.empty(), "", error));
        responseDataTwo.put(t1p0, new OffsetFetchResponse.PartitionData(10, Optional.empty(), "", error));
        responseDataTwo.put(t1p1, new OffsetFetchResponse.PartitionData(10, Optional.empty(), "", error));
        responseDataTwo.put(t2p0, new OffsetFetchResponse.PartitionData(10, Optional.empty(), "", error));
        responseDataTwo.put(t2p1, new OffsetFetchResponse.PartitionData(10, Optional.empty(), "", error));
        responseDataTwo.put(t2p2, new OffsetFetchResponse.PartitionData(10, Optional.empty(), "", Errors.NONE));

        Map<String, Map<TopicPartition, PartitionData>> responseData =
            new HashMap<String, Map<TopicPartition, PartitionData>>() {{
                put(groupZero, responseDataZero);
                put(groupOne, responseDataOne);
                put(groupTwo, responseDataTwo);
            }};

        Map<String, Errors> errorMap =
            new HashMap<String, Errors>() {{
                put(groupZero, Errors.NONE);
                put(groupOne, Errors.NONE);
                put(groupTwo, Errors.NONE);
            }};

        return new OffsetFetchResponse(0, errorMap, responseData);
    }

    private AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> handleWithPartitionError(
        Errors error
    ) {
        ListConsumerGroupOffsetsHandler handler = new ListConsumerGroupOffsetsHandler(groupZero, tps,
            logContext);
        OffsetFetchResponse response = buildResponseWithPartitionError(error);
        return handler.handleResponse(new Node(1, "host", 1234),
            singleton(CoordinatorKey.byGroupId(groupZero)), response);
    }

    private AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> handleWithPartitionErrorMultipleGroups(
        Errors error
    ) {
        ListConsumerGroupOffsetsHandler handler = new ListConsumerGroupOffsetsHandler(requestMap, false, logContext);
        OffsetFetchResponse response = buildResponseWithPartitionErrorWithMultipleGroups(error);
        return handler.handleResponse(
            new Node(1, "host", 1234),
            requestMap.keySet()
                .stream()
                .map(CoordinatorKey::byGroupId)
                .collect(Collectors.toSet()),
            response);
    }

    private AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> handleWithError(
        Errors error
    ) {
        ListConsumerGroupOffsetsHandler handler = new ListConsumerGroupOffsetsHandler(
            Collections.singletonMap(groupZero, tps), false, logContext);
        OffsetFetchResponse response = buildResponse(error);
        return handler.handleResponse(new Node(1, "host", 1234),
            singleton(CoordinatorKey.byGroupId(groupZero)),
            response);
    }

    private void assertUnmapped(
        AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> result) {
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(singletonList(CoordinatorKey.byGroupId(groupZero)), result.unmappedKeys);
    }

    private void assertUnmappedWithMultipleGroups(
        AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> result) {
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(Stream.of(groupZero, groupOne, groupTwo)
                .map(CoordinatorKey::byGroupId)
                .collect(Collectors.toSet()),
            new HashSet<>(result.unmappedKeys));
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
        CoordinatorKey key = CoordinatorKey.byGroupId(groupZero);
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(singleton(key), result.completedKeys.keySet());
        assertEquals(expected, result.completedKeys.get(key));
    }

    private void assertCompletedForMultipleGroups(
        AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> result,
        Map<String, Map<TopicPartition, OffsetAndMetadata>> expected) {
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
        for (String g : expected.keySet()) {
            CoordinatorKey key = CoordinatorKey.byGroupId(g);
            assertTrue(result.completedKeys.containsKey(key));
            assertEquals(expected.get(g), result.completedKeys.get(key));
        }
    }

    private void assertFailed(
        Class<? extends Throwable> expectedExceptionType,
        AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> result
    ) {
        CoordinatorKey key = CoordinatorKey.byGroupId(groupZero);
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(singleton(key), result.failedKeys.keySet());
        assertTrue(expectedExceptionType.isInstance(result.failedKeys.get(key)));
    }

    private void assertFailedForMultipleGroups(
        Map<String, Class<? extends Throwable>> groupToExceptionMap,
        AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> result) {
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
        for (String g : groupToExceptionMap.keySet()) {
            CoordinatorKey key = CoordinatorKey.byGroupId(g);
            assertTrue(result.failedKeys.containsKey(key));
            assertTrue(groupToExceptionMap.get(g).isInstance(result.failedKeys.get(key)));
        }
    }
}
