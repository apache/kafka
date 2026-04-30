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

import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.internals.AdminApiHandler.RequestAndKeys;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestGroup;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.LogContext;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ListConsumerGroupOffsetsHandlerTest {

    private final LogContext logContext = new LogContext();
    private final String group0 = "group0";
    private final String group1 = "group1";
    private final String group2 = "group2";
    private final String group3 = "group3";
    private final List<String> groups = List.of(group0, group1, group2);
    private final TopicPartition t0p0 = new TopicPartition("t0", 0);
    private final TopicPartition t0p1 = new TopicPartition("t0", 1);
    private final TopicPartition t1p0 = new TopicPartition("t1", 0);
    private final TopicPartition t1p1 = new TopicPartition("t1", 1);
    private final TopicPartition t2p0 = new TopicPartition("t2", 0);
    private final TopicPartition t2p1 = new TopicPartition("t2", 1);
    private final TopicPartition t2p2 = new TopicPartition("t2", 2);
    private final TopicPartition t3p0 = new TopicPartition("t3", 0);
    private final TopicPartition t3p1 = new TopicPartition("t3", 1);

    private final Map<String, ListConsumerGroupOffsetsSpec> singleGroupSpec = Map.of(
        group0, new ListConsumerGroupOffsetsSpec().topicPartitions(Arrays.asList(t0p0, t0p1, t1p0, t1p1))
    );
    private final Map<String, ListConsumerGroupOffsetsSpec> multiGroupSpecs = Map.of(
        group0, new ListConsumerGroupOffsetsSpec().topicPartitions(singletonList(t0p0)),
        group1, new ListConsumerGroupOffsetsSpec().topicPartitions(Arrays.asList(t0p0, t1p0, t1p1)),
        group2, new ListConsumerGroupOffsetsSpec().topicPartitions(Arrays.asList(t0p0, t1p0, t1p1, t2p0, t2p1, t2p2))
    );

    @Test
    public void testBuildRequest() {
        var handler = new ListConsumerGroupOffsetsHandler(
            singleGroupSpec,
            false,
            logContext
        );

        assertEquals(
            new OffsetFetchRequestData()
                .setGroups(List.of(
                    new OffsetFetchRequestData.OffsetFetchRequestGroup()
                        .setGroupId(group0)
                        .setTopics(List.of(
                            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                                .setName(t0p0.topic())
                                .setPartitionIndexes(List.of(t0p0.partition(), t0p1.partition())),
                            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                                .setName(t1p0.topic())
                                .setPartitionIndexes(List.of(t1p0.partition(), t1p1.partition()))
                        ))
                )),
            handler.buildBatchedRequest(coordinatorKeys(group0)).build().data()
        );
    }

    @Test
    public void testBuildRequestWithMultipleGroups() {
        var groupSpecs = new HashMap<>(multiGroupSpecs);
        groupSpecs.put(
            group3,
            new ListConsumerGroupOffsetsSpec().topicPartitions(List.of(t3p0, t3p1))
        );

        var handler = new ListConsumerGroupOffsetsHandler(
            groupSpecs,
            false,
            logContext
        );

        var request1 = handler.buildBatchedRequest(coordinatorKeys(group0, group1, group2)).build();

        assertEquals(
            Set.of(
                new OffsetFetchRequestData.OffsetFetchRequestGroup()
                    .setGroupId(group0)
                    .setTopics(List.of(
                        new OffsetFetchRequestData.OffsetFetchRequestTopics()
                            .setName(t0p0.topic())
                            .setPartitionIndexes(List.of(t0p0.partition()))
                    )),
                new OffsetFetchRequestData.OffsetFetchRequestGroup()
                    .setGroupId(group1)
                    .setTopics(List.of(
                        new OffsetFetchRequestData.OffsetFetchRequestTopics()
                            .setName(t0p0.topic())
                            .setPartitionIndexes(List.of(t0p0.partition())),
                        new OffsetFetchRequestData.OffsetFetchRequestTopics()
                            .setName(t1p0.topic())
                            .setPartitionIndexes(List.of(t1p0.partition(), t1p1.partition()))
                    )),
                new OffsetFetchRequestData.OffsetFetchRequestGroup()
                    .setGroupId(group2)
                    .setTopics(List.of(
                        new OffsetFetchRequestData.OffsetFetchRequestTopics()
                            .setName(t0p0.topic())
                            .setPartitionIndexes(List.of(t0p0.partition())),
                        new OffsetFetchRequestData.OffsetFetchRequestTopics()
                            .setName(t1p0.topic())
                            .setPartitionIndexes(List.of(t1p0.partition(), t1p1.partition())),
                        new OffsetFetchRequestData.OffsetFetchRequestTopics()
                            .setName(t2p0.topic())
                            .setPartitionIndexes(List.of(t2p0.partition(), t2p1.partition(), t2p2.partition()))
                    ))
            ),
            Set.copyOf(request1.data().groups())
        );

        var request2 = handler.buildBatchedRequest(coordinatorKeys(group3)).build();

        assertEquals(
            Set.of(
                new OffsetFetchRequestData.OffsetFetchRequestGroup()
                    .setGroupId(group3)
                    .setTopics(List.of(
                        new OffsetFetchRequestData.OffsetFetchRequestTopics()
                            .setName(t3p0.topic())
                            .setPartitionIndexes(List.of(t3p0.partition(), t3p1.partition()))
                    ))
            ),
            Set.copyOf(request2.data().groups())
        );
    }

    @Test
    public void testBuildRequestBatchGroups() {
        ListConsumerGroupOffsetsHandler handler = new ListConsumerGroupOffsetsHandler(multiGroupSpecs, false, logContext);
        Collection<RequestAndKeys<CoordinatorKey>> requests = handler.buildRequest(1, coordinatorKeys(group0, group1, group2));
        assertEquals(1, requests.size());
        assertEquals(Set.of(group0, group1, group2), requestGroups((OffsetFetchRequest) requests.iterator().next().request.build()));
    }

    @Test
    public void testBuildRequestDoesNotBatchGroup() {
        ListConsumerGroupOffsetsHandler handler = new ListConsumerGroupOffsetsHandler(multiGroupSpecs, false, logContext);
        // Disable batching.
        ((CoordinatorStrategy) handler.lookupStrategy()).disableBatch();
        Collection<RequestAndKeys<CoordinatorKey>> requests = handler.buildRequest(1, coordinatorKeys(group0, group1, group2));
        assertEquals(3, requests.size());
        assertEquals(
            Set.of(Set.of(group0), Set.of(group1), Set.of(group2)),
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
        var expectedResult = Map.of(
            group0, Map.of(t0p0, new OffsetAndMetadata(10L)),
            group1, Map.of(t1p1, new OffsetAndMetadata(10L)),
            group2, Map.of(t2p2, new OffsetAndMetadata(10L))
        );

        assertCompletedForMultipleGroups(
            handleWithPartitionErrorMultipleGroups(Errors.UNKNOWN_TOPIC_OR_PARTITION),
            expectedResult
        );
        assertCompletedForMultipleGroups(
            handleWithPartitionErrorMultipleGroups(Errors.TOPIC_AUTHORIZATION_FAILED),
            expectedResult
        );
        assertCompletedForMultipleGroups(
            handleWithPartitionErrorMultipleGroups(Errors.UNSTABLE_OFFSET_COMMIT),
            expectedResult
        );
    }

    @Test
    public void testSuccessfulHandleResponseWithMultipleGroups() {
        Map<String, Map<TopicPartition, OffsetAndMetadata>> expected = new HashMap<>();
        Map<String, Errors> errorMap = errorMap(groups, Errors.NONE);
        assertCompletedForMultipleGroups(handleWithErrorWithMultipleGroups(errorMap, multiGroupSpecs), expected);
    }

    @Test
    public void testUnmappedHandleResponse() {
        assertUnmapped(handleWithError(Errors.COORDINATOR_NOT_AVAILABLE));
        assertUnmapped(handleWithError(Errors.NOT_COORDINATOR));
    }

    @Test
    public void testUnmappedHandleResponseWithMultipleGroups() {
        var errorMap = Map.of(
            group0, Errors.NOT_COORDINATOR,
            group1, Errors.COORDINATOR_NOT_AVAILABLE,
            group2, Errors.NOT_COORDINATOR
        );
        assertUnmappedWithMultipleGroups(handleWithErrorWithMultipleGroups(errorMap, multiGroupSpecs));
    }

    @Test
    public void testRetriableHandleResponse() {
        assertRetriable(handleWithError(Errors.COORDINATOR_LOAD_IN_PROGRESS));
    }

    @Test
    public void testRetriableHandleResponseWithMultipleGroups() {
        Map<String, Errors> errorMap = errorMap(groups, Errors.COORDINATOR_LOAD_IN_PROGRESS);
        assertRetriable(handleWithErrorWithMultipleGroups(errorMap, multiGroupSpecs));
    }

    @Test
    public void testFailedHandleResponse() {
        assertFailed(GroupAuthorizationException.class, handleWithError(Errors.GROUP_AUTHORIZATION_FAILED));
        assertFailed(GroupIdNotFoundException.class, handleWithError(Errors.GROUP_ID_NOT_FOUND));
        assertFailed(InvalidGroupIdException.class, handleWithError(Errors.INVALID_GROUP_ID));
    }

    @Test
    public void testFailedHandleResponseWithMultipleGroups() {
        var errorMap = Map.of(
            group0, Errors.GROUP_AUTHORIZATION_FAILED,
            group1, Errors.GROUP_ID_NOT_FOUND,
            group2, Errors.INVALID_GROUP_ID
        );
        var groupToExceptionMap = Map.of(
            group0, (Class<? extends Throwable>) GroupAuthorizationException.class,
            group1, (Class<? extends Throwable>) GroupIdNotFoundException.class,
            group2, (Class<? extends Throwable>) InvalidGroupIdException.class
        );
        assertFailedForMultipleGroups(
            groupToExceptionMap,
            handleWithErrorWithMultipleGroups(errorMap, multiGroupSpecs)
        );
    }

    private OffsetFetchResponse buildResponse(Errors error) {
        return new OffsetFetchResponse(
            new OffsetFetchResponseData()
                .setGroups(List.of(
                    new OffsetFetchResponseData.OffsetFetchResponseGroup()
                        .setGroupId(group0)
                        .setErrorCode(error.code())
                )),
            ApiKeys.OFFSET_FETCH.latestVersion()
        );
    }

    private AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> handleWithErrorWithMultipleGroups(
        Map<String, Errors> errorMap,
        Map<String, ListConsumerGroupOffsetsSpec> groupSpecs
    ) {
        var handler = new ListConsumerGroupOffsetsHandler(
            groupSpecs,
            false,
            logContext
        );
        var response = new OffsetFetchResponse(
            new OffsetFetchResponseData()
                .setGroups(errorMap.entrySet().stream().map(entry ->
                    new OffsetFetchResponseData.OffsetFetchResponseGroup()
                        .setGroupId(entry.getKey())
                        .setErrorCode(entry.getValue().code())
                ).collect(Collectors.toList())),
            ApiKeys.OFFSET_FETCH.latestVersion()
        );
        return handler.handleResponse(new Node(1, "host", 1234),
            errorMap.keySet()
                    .stream()
                    .map(CoordinatorKey::byGroupId)
                    .collect(Collectors.toSet()),
            response
        );
    }

    private OffsetFetchResponse buildResponseWithPartitionError(Errors error) {
        return new OffsetFetchResponse(
            new OffsetFetchResponseData()
                .setGroups(List.of(
                    new OffsetFetchResponseData.OffsetFetchResponseGroup()
                        .setGroupId(group0)
                        .setTopics(List.of(
                            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                                .setName(t0p0.topic())
                                .setPartitions(List.of(
                                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                        .setPartitionIndex(t0p0.partition())
                                        .setCommittedOffset(10),
                                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                        .setPartitionIndex(t0p1.partition())
                                        .setCommittedOffset(10)
                                        .setErrorCode(error.code())
                                ))
                        ))
                )),
            ApiKeys.OFFSET_FETCH.latestVersion()
        );
    }

    private OffsetFetchResponse buildResponseWithPartitionErrorWithMultipleGroups(Errors error) {
        var data = new OffsetFetchResponseData()
            .setGroups(List.of(
                new OffsetFetchResponseData.OffsetFetchResponseGroup()
                    .setGroupId(group0)
                    .setTopics(List.of(
                        new OffsetFetchResponseData.OffsetFetchResponseTopics()
                            .setName(t0p0.topic())
                            .setPartitions(List.of(
                                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                    .setPartitionIndex(t0p0.partition())
                                    .setCommittedOffset(10)
                                    .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                                    .setMetadata(OffsetFetchResponse.NO_METADATA)
                                    .setErrorCode(Errors.NONE.code())
                            ))
                    )),
                new OffsetFetchResponseData.OffsetFetchResponseGroup()
                    .setGroupId(group1)
                    .setTopics(List.of(
                        new OffsetFetchResponseData.OffsetFetchResponseTopics()
                            .setName(t0p0.topic())
                            .setPartitions(List.of(
                                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                    .setPartitionIndex(t0p0.partition())
                                    .setCommittedOffset(10)
                                    .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                                    .setMetadata(OffsetFetchResponse.NO_METADATA)
                                    .setErrorCode(error.code())
                            )),
                        new OffsetFetchResponseData.OffsetFetchResponseTopics()
                            .setName(t1p0.topic())
                            .setPartitions(List.of(
                                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                    .setPartitionIndex(t1p0.partition())
                                    .setCommittedOffset(10)
                                    .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                                    .setMetadata(OffsetFetchResponse.NO_METADATA)
                                    .setErrorCode(error.code()),
                                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                    .setPartitionIndex(t1p1.partition())
                                    .setCommittedOffset(10)
                                    .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                                    .setMetadata(OffsetFetchResponse.NO_METADATA)
                                    .setErrorCode(Errors.NONE.code())
                            ))
                        )),
                new OffsetFetchResponseData.OffsetFetchResponseGroup()
                    .setGroupId(group2)
                    .setTopics(List.of(
                        new OffsetFetchResponseData.OffsetFetchResponseTopics()
                            .setName(t0p0.topic())
                            .setPartitions(List.of(
                                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                    .setPartitionIndex(t0p0.partition())
                                    .setCommittedOffset(10)
                                    .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                                    .setMetadata(OffsetFetchResponse.NO_METADATA)
                                    .setErrorCode(error.code())
                            )),
                        new OffsetFetchResponseData.OffsetFetchResponseTopics()
                            .setName(t1p0.topic())
                            .setPartitions(List.of(
                                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                    .setPartitionIndex(t1p0.partition())
                                    .setCommittedOffset(10)
                                    .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                                    .setMetadata(OffsetFetchResponse.NO_METADATA)
                                    .setErrorCode(error.code()),
                                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                    .setPartitionIndex(t1p1.partition())
                                    .setCommittedOffset(10)
                                    .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                                    .setMetadata(OffsetFetchResponse.NO_METADATA)
                                    .setErrorCode(error.code())
                            )),
                        new OffsetFetchResponseData.OffsetFetchResponseTopics()
                            .setName(t2p0.topic())
                            .setPartitions(List.of(
                                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                    .setPartitionIndex(t2p0.partition())
                                    .setCommittedOffset(10)
                                    .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                                    .setMetadata(OffsetFetchResponse.NO_METADATA)
                                    .setErrorCode(error.code()),
                                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                    .setPartitionIndex(t2p1.partition())
                                    .setCommittedOffset(10)
                                    .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                                    .setMetadata(OffsetFetchResponse.NO_METADATA)
                                    .setErrorCode(error.code()),
                                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                    .setPartitionIndex(t2p2.partition())
                                    .setCommittedOffset(10)
                                    .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                                    .setMetadata(OffsetFetchResponse.NO_METADATA)
                                    .setErrorCode(Errors.NONE.code())
                            ))
                    ))
            ));

        return new OffsetFetchResponse(data, ApiKeys.OFFSET_FETCH.latestVersion());
    }

    private AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> handleWithPartitionError(
        Errors error
    ) {
        ListConsumerGroupOffsetsHandler handler = new ListConsumerGroupOffsetsHandler(
            singleGroupSpec,
            false,
            logContext
        );
        OffsetFetchResponse response = buildResponseWithPartitionError(error);
        return handler.handleResponse(new Node(1, "host", 1234),
            singleton(CoordinatorKey.byGroupId(group0)), response);
    }

    private AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> handleWithPartitionErrorMultipleGroups(
        Errors error
    ) {
        ListConsumerGroupOffsetsHandler handler = new ListConsumerGroupOffsetsHandler(
            multiGroupSpecs,
            false,
            logContext
        );
        OffsetFetchResponse response = buildResponseWithPartitionErrorWithMultipleGroups(error);
        return handler.handleResponse(
            new Node(1, "host", 1234),
            coordinatorKeys(group0, group1, group2),
            response
        );
    }

    private AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> handleWithError(
        Errors error
    ) {
        ListConsumerGroupOffsetsHandler handler = new ListConsumerGroupOffsetsHandler(
            singleGroupSpec, false, logContext);
        OffsetFetchResponse response = buildResponse(error);
        return handler.handleResponse(new Node(1, "host", 1234),
            singleton(CoordinatorKey.byGroupId(group0)),
            response);
    }

    private void assertUnmapped(
        AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> result
    ) {
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(singletonList(CoordinatorKey.byGroupId(group0)), result.unmappedKeys);
    }

    private void assertUnmappedWithMultipleGroups(
            AdminApiHandler.ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> result
    ) {
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptySet(), result.failedKeys.keySet());
        assertEquals(coordinatorKeys(group0, group1, group2), new HashSet<>(result.unmappedKeys));
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
        CoordinatorKey key = CoordinatorKey.byGroupId(group0);
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
        CoordinatorKey key = CoordinatorKey.byGroupId(group0);
        assertEquals(emptySet(), result.completedKeys.keySet());
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(singleton(key), result.failedKeys.keySet());
        assertInstanceOf(expectedExceptionType, result.failedKeys.get(key));
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
            assertInstanceOf(groupToExceptionMap.get(g), result.failedKeys.get(key));
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
