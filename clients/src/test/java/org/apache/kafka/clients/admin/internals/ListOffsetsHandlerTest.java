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

import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ListOffsetsHandlerTest {
    private ListOffsetsHandler newHandler(
            Map<TopicPartition, Long> topicPartitionOffsets
    ) {
        return new ListOffsetsHandler(
            topicPartitionOffsets,
            new LogContext(),
            IsolationLevel.READ_COMMITTED
        );
    }

    @Test
    public void testBuildRequest() {
        Set<TopicPartition> topicPartitions = mkSet(
            new TopicPartition("foo", 5),
            new TopicPartition("bar", 3),
            new TopicPartition("foo", 4),
            new TopicPartition("bar", 2)
        );

        Map<TopicPartition, Long> topicPartitionOffsets = mkMap(
            mkEntry(new TopicPartition("foo", 5), ListOffsetsRequest.EARLIEST_TIMESTAMP),
            mkEntry(new TopicPartition("bar", 3), ListOffsetsRequest.LATEST_TIMESTAMP),
            mkEntry(new TopicPartition("foo", 4), 1L),
            mkEntry(new TopicPartition("bar", 2), ListOffsetsRequest.MAX_TIMESTAMP)
        );

        ListOffsetsHandler handler = newHandler(
                topicPartitionOffsets
        );

        int brokerId = 3;
        ListOffsetsRequest request = handler.buildRequest(brokerId, topicPartitions).build();

        List<ListOffsetsRequestData.ListOffsetsTopic> topics = request.data().topics();

        assertEquals(mkSet("foo", "bar"), topics.stream()
            .map(ListOffsetsRequestData.ListOffsetsTopic::name)
            .collect(Collectors.toSet()));

        topics.forEach(topic -> {
            Set<ListOffsetsRequestData.ListOffsetsPartition> expectedTopicPartitions = "foo".equals(topic.name()) ?
                mkSet(
                    new ListOffsetsRequestData.ListOffsetsPartition()
                        .setPartitionIndex(4)
                        .setTimestamp(1L),
                    new ListOffsetsRequestData.ListOffsetsPartition()
                        .setPartitionIndex(5)
                        .setTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP)
                ) : mkSet(
                    new ListOffsetsRequestData.ListOffsetsPartition()
                        .setPartitionIndex(3)
                        .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP),
                new ListOffsetsRequestData.ListOffsetsPartition()
                    .setPartitionIndex(2)
                    .setTimestamp(ListOffsetsRequest.MAX_TIMESTAMP)
                );
            assertEquals(expectedTopicPartitions, new HashSet<>(topic.partitions()));
        });
    }

    @Test
    public void testUnexpectedError() {
        TopicPartition topicPartition = new TopicPartition("foo", 5);
        Throwable exception = assertFatalError(topicPartition, Errors.UNKNOWN_SERVER_ERROR);
        assertTrue(exception instanceof UnknownServerException);
    }

    @Test
    public void testRetriableErrors() {
        TopicPartition topicPartition = new TopicPartition("foo", 5);
        assertRetriableError(topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION);
    }

    @Test
    public void testUnmappedAfterNotLeaderError() {
        TopicPartition topicPartition = new TopicPartition("foo", 5);
        ApiResult<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> result =
            handleResponseWithError(topicPartition, Errors.NOT_LEADER_OR_FOLLOWER);
        assertEquals(emptyMap(), result.failedKeys);
        assertEquals(emptyMap(), result.completedKeys);
        assertEquals(singletonList(topicPartition), result.unmappedKeys);
    }

    @Test
    public void testUnmappedAfterLeaderNotAvailableError() {
        TopicPartition topicPartition = new TopicPartition("foo", 5);
        ApiResult<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> result =
                handleResponseWithError(topicPartition, Errors.LEADER_NOT_AVAILABLE);
        assertEquals(emptyMap(), result.failedKeys);
        assertEquals(emptyMap(), result.completedKeys);
        assertEquals(singletonList(topicPartition), result.unmappedKeys);
    }

    @Test
    public void testCompletedResult() {
        TopicPartition topicPartition = new TopicPartition("foo", 5);
        ListOffsetsHandler handler = newHandler(mkMap(mkEntry(topicPartition, ListOffsetsRequest.EARLIEST_TIMESTAMP))
        );

        int brokerId = 3;
        ListOffsetsPartitionResponse partitionResponse =
            samplePartitionOffsets(topicPartition);
        ListOffsetsResponse response = listOffsetsResponse(
            singletonMap(topicPartition, partitionResponse)
        );

        ApiResult<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> result =
            handler.handleResponse(new Node(brokerId, "host", 1234), mkSet(topicPartition), response);

        assertEquals(mkSet(topicPartition), result.completedKeys.keySet());
        assertEquals(emptyMap(), result.failedKeys);
        assertEquals(emptyList(), result.unmappedKeys);

        ListOffsetsResult.ListOffsetsResultInfo listOffsetsResultInfo = result.completedKeys.get(topicPartition);
        assertMatchingOffsets(partitionResponse, listOffsetsResultInfo);
    }

    @Test
    public void testHandleUnsupportedVersion() {
        TopicPartition supported = new TopicPartition("foo", 5);
        TopicPartition unsupported = new TopicPartition("bar", 5);

        Set<TopicPartition> topicPartitions = mkSet(supported, unsupported);

        Map<TopicPartition, Long> topicPartitionOffsets = mkMap(
            mkEntry(supported, ListOffsetsRequest.EARLIEST_TIMESTAMP),
            mkEntry(unsupported, ListOffsetsRequest.MAX_TIMESTAMP)  // unsupported version
        );

        ListOffsetsHandler handler = newHandler(
            topicPartitionOffsets
        );

        int brokerId = 3;
        ApiRequestScope mockScope = new ApiRequestScope() {
            public OptionalInt destinationBrokerId() {
                return OptionalInt.of(brokerId);
            }
        };

        final Map<TopicPartition, Throwable> unsupportedVersion = handler.handleUnsupportedVersion(
            new AdminApiDriver.RequestSpec<>(null, mockScope, topicPartitions, null, 0L, 0L, 1),
            new UnsupportedVersionException("unsupported version")
        );

        assertEquals(mkSet(unsupported), unsupportedVersion.keySet());
    }

    private void assertRetriableError(
        TopicPartition topicPartition,
        Errors error
    ) {
        ApiResult<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> result =
            handleResponseWithError(topicPartition, error);
        assertEquals(emptyMap(), result.failedKeys);
        assertEquals(emptyMap(), result.completedKeys);
        assertEquals(emptyList(), result.unmappedKeys);
    }

    private Throwable assertFatalError(
        TopicPartition topicPartition,
        Errors error
    ) {
        ApiResult<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> result = handleResponseWithError(
            topicPartition, error);
        assertEquals(emptyMap(), result.completedKeys);
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(mkSet(topicPartition), result.failedKeys.keySet());
        return result.failedKeys.get(topicPartition);
    }

    private ApiResult<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> handleResponseWithError(
        TopicPartition topicPartition,
        Errors error
    ) {
        ListOffsetsHandler handler = newHandler(mkMap(mkEntry(
            topicPartition, ListOffsetsRequest.EARLIEST_TIMESTAMP)));
        ListOffsetsResponse response = buildResponseWithError(topicPartition, error);
        return handler.handleResponse(new Node(3, "host", 1234), mkSet(topicPartition), response);
    }

    private ListOffsetsResponse buildResponseWithError(
        TopicPartition topicPartition,
        Errors error
    ) {
        ListOffsetsPartitionResponse partitionResponse =
            new ListOffsetsPartitionResponse()
                .setPartitionIndex(topicPartition.partition())
                .setErrorCode(error.code());
        return listOffsetsResponse(singletonMap(topicPartition, partitionResponse));
    }

    private ListOffsetsPartitionResponse samplePartitionOffsets(
        TopicPartition topicPartition
    ) {

        return new ListOffsetsPartitionResponse()
            .setPartitionIndex(topicPartition.partition())
             .setLeaderEpoch(1)
             .setOffset(1L)
             .setTimestamp(0L)
            .setErrorCode(Errors.NONE.code());
    }

    private void assertMatchingOffsets(
        ListOffsetsPartitionResponse expected,
        ListOffsetsResult.ListOffsetsResultInfo actual
    ) {
        Optional<Integer> leaderEpoch = (expected.leaderEpoch() == ListOffsetsResponse.UNKNOWN_EPOCH)
                ? Optional.empty()
                : Optional.of(expected.leaderEpoch());

        assertEquals(leaderEpoch, actual.leaderEpoch());
        assertEquals(expected.offset(), actual.offset());
        assertEquals(expected.timestamp(), actual.timestamp());
    }

    private ListOffsetsResponse listOffsetsResponse(
        Map<TopicPartition, ListOffsetsPartitionResponse> partitionResponses
    ) {
        ListOffsetsResponseData response = new ListOffsetsResponseData();
        Map<String, Map<Integer, ListOffsetsPartitionResponse>> partitionResponsesByTopic =
            CollectionUtils.groupPartitionDataByTopic(partitionResponses);

        for (Map.Entry<String, Map<Integer, ListOffsetsPartitionResponse>> topicEntry:
                partitionResponsesByTopic.entrySet()) {
            String topic = topicEntry.getKey();
            Map<Integer, ListOffsetsPartitionResponse> topicPartitionResponses = topicEntry.getValue();

            ListOffsetsResponseData.ListOffsetsTopicResponse topicResponse =
                new ListOffsetsResponseData.ListOffsetsTopicResponse().setName(topic);
            response.topics().add(topicResponse);

            for (Map.Entry<Integer, ListOffsetsPartitionResponse> partitionEntry:
                topicPartitionResponses.entrySet()) {

                Integer partitionId = partitionEntry.getKey();
                ListOffsetsPartitionResponse partitionResponse = partitionEntry.getValue();
                topicResponse.partitions().add(partitionResponse.setPartitionIndex(partitionId));
            }
        }

        return new ListOffsetsResponse(response);
    }

}
