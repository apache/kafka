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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.internals.AdminApiHandler.RequestAndKeys;
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
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

public class ListConsumerGroupOffsetsHandlerTest {

    private final LogContext logContext = new LogContext();
    private final int throttleMs = 10;
    private final String groupZero = "group0";
    private final String groupOne = "group1";
    private final String groupTwo = "group2";
    private final List<String> groups = Arrays.asList(groupZero, groupOne, groupTwo);
    private final TopicPartition t0p0 = new TopicPartition("t0", 0);
    private final TopicPartition t0p1 = new TopicPartition("t0", 1);
    private final TopicPartition t1p0 = new TopicPartition("t1", 0);
    private final TopicPartition t1p1 = new TopicPartition("t1", 1);
    private final TopicPartition t2p0 = new TopicPartition("t2", 0);
    private final TopicPartition t2p1 = new TopicPartition("t2", 1);
    private final TopicPartition t2p2 = new TopicPartition("t2", 2);
    private final Map<String, ListConsumerGroupOffsetsSpec> singleRequestMap = Collections.singletonMap(groupZero,
            new ListConsumerGroupOffsetsSpec().topicPartitions(Arrays.asList(t0p0, t0p1, t1p0, t1p1)));
    private final Map<String, ListConsumerGroupOffsetsSpec> batchedRequestMap =
            new HashMap<String, ListConsumerGroupOffsetsSpec>() {{
                put(groupZero, new ListConsumerGroupOffsetsSpec().topicPartitions(singletonList(t0p0)));
                put(groupOne, new ListConsumerGroupOffsetsSpec().topicPartitions(Arrays.asList(t0p0, t1p0, t1p1)));
                put(groupTwo, new ListConsumerGroupOffsetsSpec().topicPartitions(Arrays.asList(t0p0, t1p0, t1p1, t2p0, t2p1, t2p2)));
            }};

    @Test
    public void testBuildRequest() {
        ListConsumerGroupOffsetsHandler handler =
            new ListConsumerGroupOffsetsHandler(singleRequestMap, false, logContext);
        OffsetFetchRequest request = handler.buildBatchedRequest(coordinatorKeys(groupZero)).build();
        assertEquals(groupZero, request.data().groups().get(0).groupId());
        assertEquals(2, request.data().groups().get(0).topics().size());
        assertEquals(2, request.data().groups().get(0).topics().get(0).partitionIndexes().size());
        assertEquals(2, request.data().groups().get(0).topics().get(1).partitionIndexes().size());
    }

    @Test
    public void testBuildRequestWithMultipleGroups() {
        Map<String, ListConsumerGroupOffsetsSpec> requestMap = new HashMap<>(this.batchedRequestMap);
        String groupThree = "group3";
        requestMap.put(groupThree, new ListConsumerGroupOffsetsSpec()
                .topicPartitions(Arrays.asList(new TopicPartition("t3", 0), new TopicPartition("t3", 1))));

        ListConsumerGroupOffsetsHandler handler = new ListConsumerGroupOffsetsHandler(requestMap, false, logContext);
        OffsetFetchRequest request1 = handler.buildBatchedRequest(coordinatorKeys(groupZero, groupOne, groupTwo)).build();
        assertEquals(Utils.mkSet(groupZero, groupOne, groupTwo), requestGroups(request1));

        OffsetFetchRequest request2 = handler.buildBatchedRequest(coordinatorKeys(groupThree)).build();
        assertEquals(Utils.mkSet(groupThree), requestGroups(request2));

        Map<String, ListConsumerGroupOffsetsSpec> builtRequests = new HashMap<>();
        request1.groupIdsToPartitions().forEach((group, partitions) ->
                builtRequests.put(group, new ListConsumerGroupOffsetsSpec().topicPartitions(partitions)));
        request2.groupIdsToPartitions().forEach((group, partitions) ->
                builtRequests.put(group, new ListConsumerGroupOffsetsSpec().topicPartitions(partitions)));

        assertEquals(requestMap, builtRequests);
        Map<String, List<OffsetFetchRequestTopics>> groupIdsToTopics = request1.groupIdsToTopics();

        assertEquals(3, groupIdsToTopics.size());
        assertEquals(1, groupIdsToTopics.get(groupZero).size());
        assertEquals(2, groupIdsToTopics.get(groupOne).size());
        assertEquals(3, groupIdsToTopics.get(groupTwo).size());

        assertEquals(1, groupIdsToTopics.get(groupZero).get(0).partitionIndexes().size());
        assertEquals(1, groupIdsToTopics.get(groupOne).get(0).partitionIndexes().size());
        assertEquals(2, groupIdsToTopics.get(groupOne).get(1).partitionIndexes().size());
        assertEquals(1, groupIdsToTopics.get(groupTwo).get(0).partitionIndexes().size());
        assertEquals(2, groupIdsToTopics.get(groupTwo).get(1).partitionIndexes().size());
        assertEquals(3, groupIdsToTopics.get(groupTwo).get(2).partitionIndexes().size());

        groupIdsToTopics = request2.groupIdsToTopics();
        assertEquals(1, groupIdsToTopics.size());
        assertEquals(1, groupIdsToTopics.get(groupThree).size());
        assertEquals(2, groupIdsToTopics.get(groupThree).get(0).partitionIndexes().size());
    }

    @Test
    public void testBuildRequestBatchGroups() {
        ListConsumerGroupOffsetsHandler handler = new ListConsumerGroupOffsetsHandler(batchedRequestMap, false, logContext);
        Collection<RequestAndKeys<CoordinatorKey>> requests = handler.buildRequest(1, coordinatorKeys(groupZero, groupOne, groupTwo));
        assertEquals(1, requests.size());
        assertEquals(Utils.mkSet(groupZero, groupOne, groupTwo), requestGroups((OffsetFetchRequest) requests.iterator().next().request.build()));
    }

    @Test
    public void testBuildRequestDoesNotBatchGroup() {
        ListConsumerGroupOffsetsHandler handler = new ListConsumerGroupOffsetsHandler(batchedRequestMap, false, logContext);
        // Disable batching.
        ((CoordinatorStrategy) handler.lookupStrategy()).disableBatch();
        Collection<RequestAndKeys<CoordinatorKey>> requests = handler.buildRequest(1, coordinatorKeys(groupZero, groupOne, groupTwo));
        assertEquals(3, requests.size());
        assertEquals(
            Utils.mkSet(Utils.mkSet(groupZero), Utils.mkSet(groupOne), Utils.mkSet(groupTwo)),
            requests.stream().map(requestAndKey -> requestGroups((OffsetFetchRequest) requestAndKey.request.build())).collect(Collectors.toSet())
        );
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
        Map<String, Errors> errorMap = errorMap(groups, Errors.NONE);
        assertCompletedForMultipleGroups(handleWithErrorWithMultipleGroups(errorMap, batchedRequestMap), expected);
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
        assertUnmappedWithMultipleGroups(handleWithErrorWithMultipleGroups(errorMap, batchedRequestMap));
    }

    @Test
    public void testRetriableHandleResponse() {
        assertRetriable(handleWithError(Errors.COORDINATOR_LOAD_IN_PROGRESS));
    }

    @Test
    public void testRetriableHandleResponseWithMultipleGroups() {
        Map<String, Errors> errorMap = errorMap(groups, Errors.COORDINATOR_LOAD_IN_PROGRESS);
        assertRetriable(handleWithErrorWithMultipleGroups(errorMap, batchedRequestMap));
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
        Map<String, Class<? extends Throwable>> groupToExceptionMap = new HashMap<>();
        groupToExceptionMap.put(groupZero, GroupAuthorizationException.class);
        groupToExceptionMap.put(groupOne, GroupIdNotFoundException.class);
        groupToExceptionMap.put(groupTwo, InvalidGroupIdException.class);
        assertFailedForMultipleGroups(groupToExceptionMap,
            handleWithErrorWithMultipleGroups(errorMap, batchedRequestMap));
    }

    private OffsetFetchResponse buildResponse(Errors error) {
        return new OffsetFetchResponse(
            throttleMs,
            Collections.singletonMap(groupZero, error),
            Collections.singletonMap(groupZero, new HashMap<>()));
    }

    private OffsetFetchResponse buildResponseWithMultipleGroups(
        Map<String, Errors> errorMap,
        Map<String, Map<TopicPartition, PartitionData>> responseData
    ) {
        return new OffsetFetchResponse(throttleMs, errorMap, responseData);
    }

    private AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> handleWithErrorWithMultipleGroups(
        Map<String, Errors> errorMap,
        Map<String, ListConsumerGroupOffsetsSpec> groupSpecs
    ) {
        ListConsumerGroupOffsetsHandler handler = new ListConsumerGroupOffsetsHandler(groupSpecs, false, logContext);
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

        Map<String, Errors> errorMap = errorMap(groups, Errors.NONE);
        return new OffsetFetchResponse(0, errorMap, responseData);
    }

    private AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> handleWithPartitionError(
        Errors error
    ) {
        ListConsumerGroupOffsetsHandler handler = new ListConsumerGroupOffsetsHandler(singleRequestMap,
            false, logContext);
        OffsetFetchResponse response = buildResponseWithPartitionError(error);
        return handler.handleResponse(new Node(1, "host", 1234),
            singleton(CoordinatorKey.byGroupId(groupZero)), response);
    }

    private AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> handleWithPartitionErrorMultipleGroups(
        Errors error
    ) {
        ListConsumerGroupOffsetsHandler handler = new ListConsumerGroupOffsetsHandler(
                batchedRequestMap, false, logContext);
        OffsetFetchResponse response = buildResponseWithPartitionErrorWithMultipleGroups(error);
        return handler.handleResponse(
            new Node(1, "host", 1234),
            coordinatorKeys(groupZero, groupOne, groupTwo),
            response);
    }

    private AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> handleWithError(
        Errors error
    ) {
        ListConsumerGroupOffsetsHandler handler = new ListConsumerGroupOffsetsHandler(
            singleRequestMap, false, logContext);
        OffsetFetchResponse response = buildResponse(error);
        return handler.handleResponse(new Node(1, "host", 1234),
            singleton(CoordinatorKey.byGroupId(groupZero)),
            response);
    }

    private void assertUnmapped(
        AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> result
    ) {
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(singletonList(CoordinatorKey.byGroupId(groupZero)), result.unmappedKeys);
    }

    private void assertUnmappedWithMultipleGroups(
            AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> result
    ) {
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(coordinatorKeys(groupZero, groupOne, groupTwo), new HashSet<>(result.unmappedKeys));
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
        Map<String, Map<TopicPartition, OffsetAndMetadata>> expected
    ) {
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
        AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> result
    ) {
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
        for (String g : groupToExceptionMap.keySet()) {
            CoordinatorKey key = CoordinatorKey.byGroupId(g);
            assertTrue(result.failedKeys.containsKey(key));
            assertTrue(groupToExceptionMap.get(g).isInstance(result.failedKeys.get(key)));
        }
    }

    private Set<CoordinatorKey> coordinatorKeys(String... groups) {
        return Stream.of(groups)
                .map(CoordinatorKey::byGroupId)
                .collect(Collectors.toSet());
    }

    private Set<String> requestGroups(OffsetFetchRequest request) {
        return request.data().groups()
                .stream()
                .map(OffsetFetchRequestGroup::groupId)
                .collect(Collectors.toSet());
    }

    private Map<String, Errors> errorMap(Collection<String> groups, Errors error) {
        return groups.stream().collect(Collectors.toMap(Function.identity(), unused -> error));
    }
}
