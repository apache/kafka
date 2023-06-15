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
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;

public final class ListOffsetsHandlerTest {

    private final LogContext logContext = new LogContext();

    private final TopicPartition t0p0 = new TopicPartition("t0", 0);
    private final TopicPartition t0p1 = new TopicPartition("t0", 1);
    private final TopicPartition t1p0 = new TopicPartition("t1", 0);
    private final TopicPartition t1p1 = new TopicPartition("t1", 1);

    private final Node node = new Node(1, "host", 1234);

    private final Map<TopicPartition, Long> offsetTimestampsByPartition = new HashMap<TopicPartition, Long>() {
        {
            put(t0p0, ListOffsetsRequest.LATEST_TIMESTAMP);
            put(t0p1, ListOffsetsRequest.EARLIEST_TIMESTAMP);
            put(t1p0, 123L);
            put(t1p1, ListOffsetsRequest.MAX_TIMESTAMP);
        }
    };

    @Test
    public void testBuildRequestSimple() {
        ListOffsetsHandler handler =
            new ListOffsetsHandler(offsetTimestampsByPartition, new ListOffsetsOptions(), logContext);
        ListOffsetsRequest request = handler.buildBatchedRequest(node.id(), mkSet(t0p0, t0p1)).build();
        List<ListOffsetsTopic> topics = request.topics();
        assertEquals(1, topics.size());
        ListOffsetsTopic topic = topics.get(0);
        assertEquals(2, topic.partitions().size());
        for (ListOffsetsPartition partition : topic.partitions()) {
            TopicPartition topicPartition = new TopicPartition(topic.name(), partition.partitionIndex());
            assertExpectedTimestamp(topicPartition, partition.timestamp());
        }
        assertEquals(IsolationLevel.READ_UNCOMMITTED, request.isolationLevel());
    }

    @Test
    public void testBuildRequestMultipleTopicsWithReadCommitted() {
        ListOffsetsHandler handler =
            new ListOffsetsHandler(
                offsetTimestampsByPartition, new ListOffsetsOptions(IsolationLevel.READ_COMMITTED), logContext);
        ListOffsetsRequest request =
            handler.buildBatchedRequest(node.id(), offsetTimestampsByPartition.keySet()).build();
        List<ListOffsetsTopic> topics = request.topics();
        assertEquals(2, topics.size());
        Map<TopicPartition, ListOffsetsPartition> partitions = new HashMap<>();
        for (ListOffsetsTopic topic : topics) {
            for (ListOffsetsPartition partition : topic.partitions()) {
                partitions.put(new TopicPartition(topic.name(), partition.partitionIndex()), partition);
            }
        }
        assertEquals(4, partitions.size());
        for (Map.Entry<TopicPartition, ListOffsetsPartition> entry : partitions.entrySet()) {
            assertExpectedTimestamp(entry.getKey(), entry.getValue().timestamp());
        }
        assertEquals(IsolationLevel.READ_COMMITTED, request.isolationLevel());
    }

    @Test
    public void testBuildRequestAllowedVersions() {
        ListOffsetsHandler defaultOptionsHandler =
            new ListOffsetsHandler(offsetTimestampsByPartition, new ListOffsetsOptions(), logContext);
        ListOffsetsRequest.Builder builder =
            defaultOptionsHandler.buildBatchedRequest(node.id(), mkSet(t0p0, t0p1, t1p0));
        assertEquals(1, builder.oldestAllowedVersion());

        ListOffsetsHandler readCommittedHandler =
            new ListOffsetsHandler(
                offsetTimestampsByPartition, new ListOffsetsOptions(IsolationLevel.READ_COMMITTED), logContext);
        builder = readCommittedHandler.buildBatchedRequest(node.id(), mkSet(t0p0, t0p1, t1p0));
        assertEquals(2, builder.oldestAllowedVersion());

        builder = readCommittedHandler.buildBatchedRequest(node.id(), mkSet(t0p0, t0p1, t1p0, t1p1));
        assertEquals(7, builder.oldestAllowedVersion());
    }

    @Test
    public void testHandleSuccessfulResponse() {
        ApiResult<TopicPartition, ListOffsetsResultInfo> result =
            handleResponse(createResponse(emptyMap()));

        assertResult(result, offsetTimestampsByPartition.keySet(), emptyMap(), emptyList(), emptySet());
    }

    @Test
    public void testHandleRetriablePartitionTimeoutResponse() {
        TopicPartition errorPartition = t0p0;
        Map<TopicPartition, Short> errorsByPartition = new HashMap<>();
        errorsByPartition.put(errorPartition, Errors.REQUEST_TIMED_OUT.code());

        ApiResult<TopicPartition, ListOffsetsResultInfo> result =
            handleResponse(createResponse(errorsByPartition));

        // Timeouts should be retried within the fulfillment stage as they are a common type of
        // retriable error.
        Set<TopicPartition> retriable = singleton(errorPartition);
        Set<TopicPartition> completed = new HashSet<>(offsetTimestampsByPartition.keySet());
        completed.removeAll(retriable);
        assertResult(result, completed, emptyMap(), emptyList(), retriable);
    }

    @Test
    public void testHandleLookupRetriablePartitionInvalidMetadataResponse() {
        TopicPartition errorPartition = t0p0;
        Errors error = Errors.NOT_LEADER_OR_FOLLOWER;
        Map<TopicPartition, Short> errorsByPartition = new HashMap<>();
        errorsByPartition.put(errorPartition, error.code());

        ApiResult<TopicPartition, ListOffsetsResultInfo> result =
            handleResponse(createResponse(errorsByPartition));

        // Some invalid metadata errors should be retried from the lookup stage as the partition-to-leader
        // mappings should be recalculated.
        List<TopicPartition> unmapped = new ArrayList<>();
        unmapped.add(errorPartition);
        Set<TopicPartition> completed = new HashSet<>(offsetTimestampsByPartition.keySet());
        completed.removeAll(unmapped);
        assertResult(result, completed, emptyMap(), unmapped, emptySet());
    }

    @Test
    public void testHandleUnexpectedPartitionErrorResponse() {
        TopicPartition errorPartition = t0p0;
        Errors error = Errors.UNKNOWN_SERVER_ERROR;
        Map<TopicPartition, Short> errorsByPartition = new HashMap<>();
        errorsByPartition.put(errorPartition, error.code());

        ApiResult<TopicPartition, ListOffsetsResultInfo> result =
            handleResponse(createResponse(errorsByPartition));

        Map<TopicPartition, Throwable> failed = new HashMap<>();
        failed.put(errorPartition, error.exception());
        Set<TopicPartition> completed = new HashSet<>(offsetTimestampsByPartition.keySet());
        completed.removeAll(failed.keySet());
        assertResult(result, completed, failed, emptyList(), emptySet());
    }

    @Test
    public void testHandleResponseSanityCheck() {
        TopicPartition errorPartition = t0p0;
        Map<TopicPartition, Long> specsByPartition = new HashMap<>(offsetTimestampsByPartition);
        specsByPartition.remove(errorPartition);

        ApiResult<TopicPartition, ListOffsetsResultInfo> result =
            handleResponse(createResponse(emptyMap(), specsByPartition));

        assertEquals(offsetTimestampsByPartition.size() - 1, result.completedKeys.size());
        assertEquals(1, result.failedKeys.size());
        assertEquals(errorPartition, result.failedKeys.keySet().iterator().next());
        String sanityCheckMessage = result.failedKeys.get(errorPartition).getMessage();
        assertTrue(sanityCheckMessage.contains("did not contain a result for topic partition"));
        assertTrue(result.unmappedKeys.isEmpty());
    }

    @Test
    public void testHandleResponseUnsupportedVersion() {
        int brokerId = 1;
        UnsupportedVersionException uve = new UnsupportedVersionException("");
        Map<TopicPartition, OffsetSpec> maxTimestampPartitions = new HashMap<>();
        maxTimestampPartitions.put(t1p1, OffsetSpec.maxTimestamp());

        ListOffsetsHandler handler =
            new ListOffsetsHandler(offsetTimestampsByPartition, new ListOffsetsOptions(), logContext);

        final Map<TopicPartition, Long> nonMaxTimestampPartitions = new HashMap<>(offsetTimestampsByPartition);
        maxTimestampPartitions.forEach((k, v) -> nonMaxTimestampPartitions.remove(k));
        // Unsupported version exceptions currently cannot be handled if there's no partition with a
        // MAX_TIMESTAMP spec...
        Set<TopicPartition> keysToTest = nonMaxTimestampPartitions.keySet();
        Set<TopicPartition> expectedFailures = keysToTest;
        assertEquals(
            mapToError(expectedFailures, uve),
            handler.handleUnsupportedVersionException(brokerId, uve, keysToTest));

        // ...or if there are only partitions with MAX_TIMESTAMP specs.
        keysToTest = maxTimestampPartitions.keySet();
        expectedFailures = keysToTest;
        assertEquals(
            mapToError(expectedFailures, uve),
            handler.handleUnsupportedVersionException(brokerId, uve, keysToTest));

        // What can be handled is a request with a mix of partitions with MAX_TIMESTAMP specs
        // and partitions with non-MAX_TIMESTAMP specs.
        keysToTest = offsetTimestampsByPartition.keySet();
        expectedFailures = maxTimestampPartitions.keySet();
        assertEquals(
            mapToError(expectedFailures, uve),
            handler.handleUnsupportedVersionException(brokerId, uve, keysToTest));
    }

    private static Map<TopicPartition, Throwable> mapToError(Set<TopicPartition> keys, Throwable t) {
        return keys.stream().collect(Collectors.toMap(k -> k, k -> t));
    }

    private void assertExpectedTimestamp(TopicPartition topicPartition, long actualTimestamp) {
        Long expectedTimestamp = offsetTimestampsByPartition.get(topicPartition);
        assertEquals(expectedTimestamp, actualTimestamp);
    }

    private ListOffsetsResponse createResponse(Map<TopicPartition, Short> errorsByPartition) {
        return createResponse(errorsByPartition, offsetTimestampsByPartition);
    }

    private static ListOffsetsResponse createResponse(
        Map<TopicPartition, Short> errorsByPartition,
        Map<TopicPartition, Long> specsByPartition
    ) {
        Map<String, ListOffsetsTopicResponse> responsesByTopic = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> offsetSpecEntry : specsByPartition.entrySet()) {
            TopicPartition topicPartition = offsetSpecEntry.getKey();
            ListOffsetsTopicResponse topicResponse = responsesByTopic.computeIfAbsent(
                topicPartition.topic(), t -> new ListOffsetsTopicResponse());
            topicResponse.setName(topicPartition.topic());
            ListOffsetsPartitionResponse partitionResponse = new ListOffsetsPartitionResponse();
            partitionResponse.setPartitionIndex(topicPartition.partition());
            partitionResponse.setOffset(getOffset(topicPartition, offsetSpecEntry.getValue()));
            partitionResponse.setErrorCode(errorsByPartition.getOrDefault(topicPartition, (short) 0));
            topicResponse.partitions().add(partitionResponse);
        }
        ListOffsetsResponseData responseData = new ListOffsetsResponseData();
        responseData.setTopics(new ArrayList<>(responsesByTopic.values()));
        return new ListOffsetsResponse(responseData);
    }

    private ApiResult<TopicPartition, ListOffsetsResultInfo> handleResponse(ListOffsetsResponse response) {
        ListOffsetsHandler handler =
            new ListOffsetsHandler(offsetTimestampsByPartition, new ListOffsetsOptions(), logContext);
        return handler.handleResponse(node, offsetTimestampsByPartition.keySet(), response);
    }

    private void assertResult(
        ApiResult<TopicPartition, ListOffsetsResultInfo> result,
        Set<TopicPartition> expectedCompleted,
        Map<TopicPartition, Throwable> expectedFailed,
        List<TopicPartition> expectedUnmapped,
        Set<TopicPartition> expectedRetriable
    ) {
        assertEquals(expectedCompleted, result.completedKeys.keySet());
        assertEquals(expectedFailed, result.failedKeys);
        assertEquals(expectedUnmapped, result.unmappedKeys);
        Set<TopicPartition> actualRetriable = new HashSet<>(offsetTimestampsByPartition.keySet());
        actualRetriable.removeAll(result.completedKeys.keySet());
        actualRetriable.removeAll(result.failedKeys.keySet());
        actualRetriable.removeAll(new HashSet<>(result.unmappedKeys));
        assertEquals(expectedRetriable, actualRetriable);
    }

    private static long getOffset(TopicPartition topicPartition, Long offsetQuery) {
        long base = 1 << 10;
        if (offsetQuery == ListOffsetsRequest.EARLIEST_TIMESTAMP) {
            return topicPartition.hashCode() & (base - 1);
        } else if (offsetQuery >= 0L) {
            return base;
        } else if (offsetQuery == ListOffsetsRequest.LATEST_TIMESTAMP) {
            return base + 1 + (topicPartition.hashCode() & (base - 1));
        }
        return 2 * base + 1;
    }
}
