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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ListOffsetsRequestManagerTest {

    private ListOffsetsRequestManager requestManager;
    private ConsumerMetadata metadata;
    private SubscriptionState subscriptionState;
    private MockTime time;
    private static final String TEST_TOPIC = "t1";
    private static final TopicPartition TEST_PARTITION_1 = new TopicPartition(TEST_TOPIC, 1);
    private static final TopicPartition TEST_PARTITION_2 = new TopicPartition(TEST_TOPIC, 2);
    private static final Node LEADER_1 = new Node(0, "host1", 9092);
    private static final Node LEADER_2 = new Node(0, "host2", 9092);
    private static final IsolationLevel DEFAULT_ISOLATION_LEVEL = IsolationLevel.READ_COMMITTED;

    @BeforeEach
    public void setup() {
        metadata = mock(ConsumerMetadata.class);
        subscriptionState = mock(SubscriptionState.class);
        this.time = new MockTime(0);
        requestManager = new ListOffsetsRequestManager(subscriptionState,
                metadata, DEFAULT_ISOLATION_LEVEL, time,
                mock(ApiVersions.class), new LogContext());
    }

    @Test
    public void testListOffsetsRequest_Success() throws ExecutionException, InterruptedException {
        Map<TopicPartition, Long> partitionsOffsets = new HashMap<>();
        long offset = ListOffsetsRequest.EARLIEST_TIMESTAMP;
        partitionsOffsets.put(TEST_PARTITION_1, offset);

        expectSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        CompletableFuture<Map<TopicPartition, Long>> result = requestManager.fetchOffsets(
                partitionsOffsets.keySet(),
                ListOffsetsRequest.EARLIEST_TIMESTAMP,
                false);
        assertEquals(1, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        verifySuccessfulPollAndResponseReceived(result, partitionsOffsets);
    }

    @Test
    public void testBuildingRequestFails_RetrySucceeds() throws ExecutionException,
            InterruptedException {
        Map<TopicPartition, Long> partitionsOffsets = new HashMap<>();
        long offset = 1L;
        partitionsOffsets.put(TEST_PARTITION_1, offset);

        // Building list offsets request fails with unknown leader
        expectFailedRequest_MissingLeader();
        CompletableFuture<Map<TopicPartition, Long>> fetchOffsetsFuture =
                requestManager.fetchOffsets(partitionsOffsets.keySet(),
                        ListOffsetsRequest.EARLIEST_TIMESTAMP,
                        false);
        assertEquals(0, requestManager.requestsToSend());
        assertEquals(1, requestManager.requestsToRetry());
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        assertFalse(fetchOffsetsFuture.isDone());

        // Cluster metadata update. Previously failed attempt to build the request should be retried
        // and succeed
        expectSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        requestManager.onUpdate(new ClusterResource(""));
        assertEquals(1, requestManager.requestsToSend());

        verifySuccessfulPollAndResponseReceived(fetchOffsetsFuture, partitionsOffsets);
    }

    @ParameterizedTest
    @MethodSource("retriableErrors")
    public void testRequestFailsWithRetriableError_RetrySucceeds(Errors error) throws ExecutionException, InterruptedException {
        Map<TopicPartition, Long> partitionsOffsets = new HashMap<>();
        long offset = 1L;
        partitionsOffsets.put(TEST_PARTITION_1, offset);

        // List offsets request that is successfully built
        expectSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        CompletableFuture<Map<TopicPartition, Long>> fetchOffsetsFuture = requestManager.fetchOffsets(
                partitionsOffsets.keySet(),
                ListOffsetsRequest.EARLIEST_TIMESTAMP,
                false);
        assertEquals(1, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        // Request successfully sent to single broker
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        verifySuccessfulPoll(res);
        assertFalse(fetchOffsetsFuture.isDone());

        // Failed response received
        NetworkClientDelegate.UnsentRequest unsentRequest = res.unsentRequests.get(0);
        unsentRequest.future().complete(
                buildClientResponseWithErrors(
                        unsentRequest,
                        Collections.singletonMap(TEST_PARTITION_1, error)));
        assertFalse(fetchOffsetsFuture.isDone());
        assertEquals(1, requestManager.requestsToRetry());
        assertEquals(0, requestManager.requestsToSend());

        // Cluster metadata update. Failed requests should be retried
        expectSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        requestManager.onUpdate(new ClusterResource(""));
        assertEquals(1, requestManager.requestsToSend());

        verifySuccessfulPollAndResponseReceived(fetchOffsetsFuture, partitionsOffsets);
    }

    private static Stream<Arguments> retriableErrors() {
        return Stream.of(
                Arguments.of(Errors.NOT_LEADER_OR_FOLLOWER),
                Arguments.of(Errors.REPLICA_NOT_AVAILABLE),
                Arguments.of(Errors.KAFKA_STORAGE_ERROR),
                Arguments.of(Errors.OFFSET_NOT_AVAILABLE),
                Arguments.of(Errors.LEADER_NOT_AVAILABLE),
                Arguments.of(Errors.FENCED_LEADER_EPOCH),
                Arguments.of(Errors.UNKNOWN_LEADER_EPOCH));
    }

    @Test
    public void testRequestPartiallyFailsWithRetriableError_RetrySucceeds() throws ExecutionException, InterruptedException {
        Map<TopicPartition, Long> partitionsOffsets = new HashMap<>();
        long offset = ListOffsetsRequest.EARLIEST_TIMESTAMP;
        partitionsOffsets.put(TEST_PARTITION_1, offset);
        partitionsOffsets.put(TEST_PARTITION_2, offset);

        // List offsets request to 2 brokers successfully built
        Map<TopicPartition, Node> partitionLeaders = new HashMap<>();
        partitionLeaders.put(TEST_PARTITION_1, LEADER_1);
        partitionLeaders.put(TEST_PARTITION_2, LEADER_2);
        expectSuccessfulRequest(partitionLeaders);
        CompletableFuture<Map<TopicPartition, Long>> fetchOffsetsFuture = requestManager.fetchOffsets(
                partitionsOffsets.keySet(),
                ListOffsetsRequest.EARLIEST_TIMESTAMP,
                false);
        assertEquals(2, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        // Request successfully sent to both brokers
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        verifySuccessfulPoll(res, 2);
        assertFalse(fetchOffsetsFuture.isDone());

        // Mixed response with failures and successes. Offsets successfully fetched from one
        // broker but retriable UNKNOWN_LEADER_EPOCH received from second broker.
        NetworkClientDelegate.UnsentRequest unsentRequest1 = res.unsentRequests.get(0);
        unsentRequest1.future().complete(
                buildClientResponseWithOffsets(
                        unsentRequest1,
                        Collections.singletonMap(TEST_PARTITION_1, offset)));
        NetworkClientDelegate.UnsentRequest unsentRequest2 = res.unsentRequests.get(1);
        unsentRequest2.future().complete(
                buildClientResponseWithErrors(
                        unsentRequest2,
                        Collections.singletonMap(TEST_PARTITION_2, Errors.UNKNOWN_LEADER_EPOCH)));

        assertFalse(fetchOffsetsFuture.isDone());
        assertEquals(1, requestManager.requestsToRetry());
        assertEquals(0, requestManager.requestsToSend());

        // Cluster metadata update. Failed requests should be retried
        expectSuccessfulRequest(partitionLeaders);
        requestManager.onUpdate(new ClusterResource(""));
        assertEquals(1, requestManager.requestsToSend());

        // Following poll should send the request and get a successful response
        NetworkClientDelegate.PollResult retriedPoll = requestManager.poll(time.milliseconds());
        verifySuccessfulPoll(retriedPoll);
        NetworkClientDelegate.UnsentRequest unsentRequest = retriedPoll.unsentRequests.get(0);
        unsentRequest.future().complete(buildClientResponseWithOffsets(unsentRequest, Collections.singletonMap(TEST_PARTITION_2, offset)));

        // Verify global result with the offset initially retrieved, and the offset that
        // initially failed but succeeded after a metadata update
        verifyRequestSuccessfullyCompleted(fetchOffsetsFuture, partitionsOffsets);
    }

    @Test
    public void testRequestFailedResponse_NonRetriableAuthError() {
        Map<TopicPartition, Long> partitionsOffsets = new HashMap<>();
        long offset = 1L;
        partitionsOffsets.put(TEST_PARTITION_1, offset);

        // List offsets request that is successfully built
        expectSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        CompletableFuture<Map<TopicPartition, Long>> fetchOffsetsFuture = requestManager.fetchOffsets(
                partitionsOffsets.keySet(),
                ListOffsetsRequest.EARLIEST_TIMESTAMP,
                false);
        assertEquals(1, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        // Request successfully sent
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        verifySuccessfulPoll(res);

        // Failed response received
        NetworkClientDelegate.UnsentRequest unsentRequest = res.unsentRequests.get(0);
        unsentRequest.future().complete(
                buildClientResponseWithErrors(
                        unsentRequest,
                        Collections.singletonMap(TEST_PARTITION_1, Errors.TOPIC_AUTHORIZATION_FAILED)));

        // Request completed with error. Nothing pending to be sent or retried
        verifyRequestCompletedWithErrorResponse(fetchOffsetsFuture, TopicAuthorizationException.class);
        assertEquals(0, requestManager.requestsToRetry());
        assertEquals(0, requestManager.requestsToSend());
    }

    private void verifySuccessfulPollAndResponseReceived(CompletableFuture<Map<TopicPartition, Long>> actualResult,
                                                         Map<TopicPartition, Long> expectedResult) throws ExecutionException, InterruptedException {
        // Following poll should send the request and get a response
        NetworkClientDelegate.PollResult retriedPoll = requestManager.poll(time.milliseconds());
        verifySuccessfulPoll(retriedPoll);
        NetworkClientDelegate.UnsentRequest unsentRequest = retriedPoll.unsentRequests.get(0);
        unsentRequest.future().complete(buildClientResponseWithOffsets(unsentRequest, expectedResult));
        verifyRequestSuccessfullyCompleted(actualResult, expectedResult);
    }

    private void expectSuccessfulRequest(Map<TopicPartition, Node> partitionLeaders) {
        partitionLeaders.forEach((tp, broker) -> {
            when(metadata.currentLeader(tp)).thenReturn(testLeaderEpoch(broker));
            when(subscriptionState.isAssigned(tp)).thenReturn(true);
        });
        when(metadata.fetch()).thenReturn(testClusterMetadata(partitionLeaders));
    }

    private void expectFailedRequest_MissingLeader() {
        when(metadata.currentLeader(any(TopicPartition.class))).thenReturn(
                new Metadata.LeaderAndEpoch(Optional.empty(), Optional.of(1)));
        when(subscriptionState.isAssigned(any(TopicPartition.class))).thenReturn(true);
    }

    private void verifySuccessfulPoll(NetworkClientDelegate.PollResult pollResult) {
        verifySuccessfulPoll(pollResult, 1);
    }

    private void verifySuccessfulPoll(NetworkClientDelegate.PollResult pollResult,
                                      int requestCount) {
        assertEquals(0, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());
        assertEquals(requestCount, pollResult.unsentRequests.size());
    }

    private void verifyRequestSuccessfullyCompleted(CompletableFuture<Map<TopicPartition, Long>> actualResult,
                                                    Map<TopicPartition, Long> expectedResult)
            throws ExecutionException, InterruptedException {
        assertEquals(0, requestManager.requestsToRetry());
        assertEquals(0, requestManager.requestsToSend());

        assertTrue(actualResult.isDone());
        assertFalse(actualResult.isCompletedExceptionally());
        Map<TopicPartition, Long> partitionOffsets = actualResult.get();
        assertEquals(expectedResult, partitionOffsets);
        verifySubscriptionStateUpdated(expectedResult);
    }

    private void verifySubscriptionStateUpdated(Map<TopicPartition, Long> expectedResult) {
        ArgumentCaptor<TopicPartition> tpCaptor = ArgumentCaptor.forClass(TopicPartition.class);
        ArgumentCaptor<Long> offsetCaptor = ArgumentCaptor.forClass(Long.class);

        verify(subscriptionState, times(expectedResult.size())).updateLastStableOffset(tpCaptor.capture(),
                offsetCaptor.capture());

        List<TopicPartition> updatedTp = tpCaptor.getAllValues();
        List<Long> updatedOffsets = offsetCaptor.getAllValues();
        assertEquals(expectedResult.keySet(), new HashSet<>(updatedTp));
        assertEquals(new ArrayList<>(expectedResult.values()), updatedOffsets);
    }

    private void verifyRequestCompletedWithErrorResponse(CompletableFuture<Map<TopicPartition, Long>> actualResult,
                                                         Class<? extends Throwable> expectedFailure) {
        assertTrue(actualResult.isDone());
        assertTrue(actualResult.isCompletedExceptionally());
        Throwable failure = assertThrows(ExecutionException.class, actualResult::get);
        assertEquals(expectedFailure, failure.getCause().getClass());
    }

    private Metadata.LeaderAndEpoch testLeaderEpoch(Node leader) {
        return new Metadata.LeaderAndEpoch(Optional.of(leader),
                Optional.of(1));
    }

    private Cluster testClusterMetadata(Map<TopicPartition, Node> partitionLeaders) {
        List<PartitionInfo> partitions =
                partitionLeaders.keySet().stream()
                        .map(tp -> new PartitionInfo(tp.topic(), tp.partition(),
                                partitionLeaders.get(tp), null, null))
                        .collect(Collectors.toList());

        return new Cluster("clusterId", partitionLeaders.values(), partitions,
                Collections.emptySet(),
                Collections.emptySet());
    }

    private ClientResponse buildClientResponseWithOffsets(
            final NetworkClientDelegate.UnsentRequest request,
            final Map<TopicPartition, Long> partitionsOffsets) {
        return buildClientResponse(request, partitionsOffsets, Collections.emptyMap());
    }

    private ClientResponse buildClientResponseWithErrors(
            final NetworkClientDelegate.UnsentRequest request,
            final Map<TopicPartition, Errors> partitionsErrors) {
        return buildClientResponse(request, Collections.emptyMap(), partitionsErrors);
    }

    private ClientResponse buildClientResponse(
            final NetworkClientDelegate.UnsentRequest request,
            final Map<TopicPartition, Long> partitionsOffsets,
            final Map<TopicPartition, Errors> partitionsErrors) {
        AbstractRequest abstractRequest = request.requestBuilder().build();
        assertTrue(abstractRequest instanceof ListOffsetsRequest);
        ListOffsetsRequest offsetFetchRequest = (ListOffsetsRequest) abstractRequest;
        ListOffsetsResponse response = buildListOffsetsResponse(partitionsOffsets,
                partitionsErrors);
        return new ClientResponse(
                new RequestHeader(ApiKeys.OFFSET_FETCH, offsetFetchRequest.version(), "", 1),
                request.callback(),
                "-1",
                time.milliseconds(),
                time.milliseconds(),
                false,
                null,
                null,
                response
        );
    }

    private ListOffsetsResponse buildListOffsetsResponse(Map<TopicPartition, Long> partitionsOffsets,
                                                         Map<TopicPartition, Errors> partitionsErrors) {
        List<ListOffsetsResponseData.ListOffsetsTopicResponse> offsetsTopicResponses = new ArrayList<>();
        // Generate successful responses
        partitionsOffsets.forEach((tp, offset) -> offsetsTopicResponses.add(
                ListOffsetsResponse.singletonListOffsetsTopicResponse(tp, Errors.NONE, -1L,
                        offset, 123)));

        // Generate responses with error
        partitionsErrors.forEach((tp, error) -> offsetsTopicResponses.add(
                ListOffsetsResponse.singletonListOffsetsTopicResponse(
                        tp, error,
                        ListOffsetsResponse.UNKNOWN_TIMESTAMP,
                        ListOffsetsResponse.UNKNOWN_OFFSET,
                        ListOffsetsResponse.UNKNOWN_EPOCH)));

        ListOffsetsResponseData responseData = new ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(offsetsTopicResponses);

        return new ListOffsetsResponse(responseData);
    }
}
