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
import org.apache.kafka.clients.admin.OffsetSpec.TimestampSpec;
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

    private final Map<TopicPartition, OffsetSpec> offsetSpecsByPartition = new HashMap<TopicPartition, OffsetSpec>() {
        {
            put(t0p0, OffsetSpec.latest());
            put(t0p1, OffsetSpec.earliest());
            put(t1p0, OffsetSpec.forTimestamp(123L));
            put(t1p1, OffsetSpec.maxTimestamp());
        }
    };

    @Test
    public void testBuildRequestSimple() {
        ListOffsetsHandler handler =
            new ListOffsetsHandler(offsetSpecsByPartition, new ListOffsetsOptions(), logContext);
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
                offsetSpecsByPartition, new ListOffsetsOptions(IsolationLevel.READ_COMMITTED), logContext);
        ListOffsetsRequest request =
            handler.buildBatchedRequest(node.id(), offsetSpecsByPartition.keySet()).build();
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
            new ListOffsetsHandler(offsetSpecsByPartition, new ListOffsetsOptions(), logContext);
        ListOffsetsRequest.Builder builder =
            defaultOptionsHandler.buildBatchedRequest(node.id(), mkSet(t0p0, t0p1, t1p0));
        assertEquals(1, builder.oldestAllowedVersion());

        ListOffsetsHandler readCommittedHandler =
            new ListOffsetsHandler(
                offsetSpecsByPartition, new ListOffsetsOptions(IsolationLevel.READ_COMMITTED), logContext);
        builder = readCommittedHandler.buildBatchedRequest(node.id(), mkSet(t0p0, t0p1, t1p0));
        assertEquals(2, builder.oldestAllowedVersion());

        builder = readCommittedHandler.buildBatchedRequest(node.id(), mkSet(t0p0, t0p1, t1p0, t1p1));
        assertEquals(7, builder.oldestAllowedVersion());
    }

    @Test
    public void testHandleSuccessfulResponse() {
        ApiResult<TopicPartition, ListOffsetsResultInfo> result =
            handleResponse(createResponse(emptyMap()));

        assertResult(result, offsetSpecsByPartition.keySet(), emptyMap(), emptyList(), emptySet());
    }

    @Test
    public void testHandlePartitionTimeoutResponse() {
        TopicPartition errorPartition = t0p0;
        Map<TopicPartition, Short> errorsByPartition = new HashMap<>();
        errorsByPartition.put(errorPartition, Errors.REQUEST_TIMED_OUT.code());

        ApiResult<TopicPartition, ListOffsetsResultInfo> result =
            handleResponse(createResponse(errorsByPartition));

        // Timeouts should be retried within the fulfillment stage as they are a common type of
        // retriable error.
        Set<TopicPartition> retriable = singleton(errorPartition);
        Set<TopicPartition> completed = new HashSet<>(offsetSpecsByPartition.keySet());
        completed.removeAll(retriable);
        assertResult(result, completed, emptyMap(), emptyList(), retriable);
    }

    @Test
    public void testHandlePartitionInvalidMetadataResponse() {
        TopicPartition errorPartition = t0p0;
        Errors error = Errors.NOT_LEADER_OR_FOLLOWER;
        Map<TopicPartition, Short> errorsByPartition = new HashMap<>();
        errorsByPartition.put(errorPartition, error.code());

        ApiResult<TopicPartition, ListOffsetsResultInfo> result =
            handleResponse(createResponse(errorsByPartition));

        // Invalid metadata errors should be retried from the lookup stage as the partition-to-leader
        // mappings should be recalculated.
        List<TopicPartition> unmapped = new ArrayList<>();
        unmapped.add(errorPartition);
        Set<TopicPartition> completed = new HashSet<>(offsetSpecsByPartition.keySet());
        completed.removeAll(unmapped);
        assertResult(result, completed, emptyMap(), unmapped, emptySet());
    }

    @Test
    public void testHandlePartitionErrorResponse() {
        TopicPartition errorPartition = t0p0;
        Errors error = Errors.UNKNOWN_SERVER_ERROR;
        Map<TopicPartition, Short> errorsByPartition = new HashMap<>();
        errorsByPartition.put(errorPartition, error.code());

        ApiResult<TopicPartition, ListOffsetsResultInfo> result =
            handleResponse(createResponse(errorsByPartition));

        Map<TopicPartition, Throwable> failed = new HashMap<>();
        failed.put(errorPartition, error.exception());
        Set<TopicPartition> completed = new HashSet<>(offsetSpecsByPartition.keySet());
        completed.removeAll(failed.keySet());
        assertResult(result, completed, failed, emptyList(), emptySet());
    }

    @Test
    public void testHandleResponseSanityCheck() {
        TopicPartition errorPartition = t0p0;
        Map<TopicPartition, OffsetSpec> specsByPartition = new HashMap<>(offsetSpecsByPartition);
        specsByPartition.remove(errorPartition);

        ApiResult<TopicPartition, ListOffsetsResultInfo> result =
            handleResponse(createResponse(emptyMap(), specsByPartition));

        assertEquals(offsetSpecsByPartition.size() - 1, result.completedKeys.size());
        assertEquals(1, result.failedKeys.size());
        assertEquals(errorPartition, result.failedKeys.keySet().iterator().next());
        String sanityCheckMessage = result.failedKeys.get(errorPartition).getMessage();
        assertTrue(sanityCheckMessage.contains("did not contain a result for topic partition"));
        assertTrue(result.unmappedKeys.isEmpty());
    }

    @Test
    public void testHandleResponseUnsupportedVersion() {
        UnsupportedVersionException uve = new UnsupportedVersionException("");
        Map<TopicPartition, OffsetSpec> maxTimestampPartitions = new HashMap<>();
        maxTimestampPartitions.put(t1p1, OffsetSpec.maxTimestamp());

        ListOffsetsHandler handler =
            new ListOffsetsHandler(offsetSpecsByPartition, new ListOffsetsOptions(), logContext);
        // Unsupported version exception currently cannot be handled if not in the fulfillment stage...
        Set<TopicPartition> keysToTest = offsetSpecsByPartition.keySet();
        Set<TopicPartition> expectedFailures = keysToTest;
        assertEquals(
            mapToError(expectedFailures, uve),
            handler.handleUnsupportedVersionException(uve, keysToTest, false));

        final Map<TopicPartition, OffsetSpec> nonMaxTimestampPartitions =
            new HashMap<>(offsetSpecsByPartition);
        maxTimestampPartitions.forEach((k, v) -> nonMaxTimestampPartitions.remove(k));
        // ...it also cannot be handled if there's no partition with a MAX_TIMESTAMP spec...
        keysToTest = nonMaxTimestampPartitions.keySet();
        expectedFailures = keysToTest;
        assertEquals(
            mapToError(expectedFailures, uve),
            handler.handleUnsupportedVersionException(uve, keysToTest, true));

        // ...or if there are only partitions with MAX_TIMESTAMP specs...
        keysToTest = maxTimestampPartitions.keySet();
        expectedFailures = keysToTest;
        assertEquals(
            mapToError(expectedFailures, uve),
            handler.handleUnsupportedVersionException(uve, keysToTest, true));

        keysToTest = offsetSpecsByPartition.keySet();
        expectedFailures = maxTimestampPartitions.keySet();
        assertEquals(
            mapToError(expectedFailures, uve),
            handler.handleUnsupportedVersionException(uve, keysToTest, true));
    }

    private static Map<TopicPartition, Throwable> mapToError(Set<TopicPartition> keys, Throwable t) {
        return keys.stream().collect(Collectors.toMap(k -> k, k -> t));
    }

    private void assertExpectedTimestamp(TopicPartition topicPartition, long actualTimestamp) {
        OffsetSpec expectedTimestampSpec = offsetSpecsByPartition.get(topicPartition);
        long expectedTimestamp = ListOffsetsHandler.getOffsetFromSpec(expectedTimestampSpec);
        assertEquals(expectedTimestamp, actualTimestamp);
    }

    private ListOffsetsResponse createResponse(Map<TopicPartition, Short> errorsByPartition) {
        return createResponse(errorsByPartition, offsetSpecsByPartition);
    }

    private static ListOffsetsResponse createResponse(
        Map<TopicPartition, Short> errorsByPartition,
        Map<TopicPartition, OffsetSpec> specsByPartition
    ) {
        Map<String, ListOffsetsTopicResponse> responsesByTopic = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetSpec> offsetSpecEntry : specsByPartition.entrySet()) {
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
            new ListOffsetsHandler(offsetSpecsByPartition, new ListOffsetsOptions(), logContext);
        return handler.handleResponse(node, offsetSpecsByPartition.keySet(), response);
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
        Set<TopicPartition> actualRetriable = new HashSet<>(offsetSpecsByPartition.keySet());
        actualRetriable.removeAll(result.completedKeys.keySet());
        actualRetriable.removeAll(result.failedKeys.keySet());
        actualRetriable.removeAll(new HashSet<>(result.unmappedKeys));
        assertEquals(expectedRetriable, actualRetriable);
    }

    private static long getOffset(TopicPartition topicPartition, OffsetSpec offsetSpec) {
        long base = 1 << 10;
        if (offsetSpec instanceof OffsetSpec.EarliestSpec) {
            return topicPartition.hashCode() & (base - 1);
        } else if (offsetSpec instanceof TimestampSpec) {
            return base;
        } else if (offsetSpec instanceof OffsetSpec.LatestSpec) {
            return base + 1 + (topicPartition.hashCode() & (base - 1));
        }
        return 2 * base + 1;
    }
}
