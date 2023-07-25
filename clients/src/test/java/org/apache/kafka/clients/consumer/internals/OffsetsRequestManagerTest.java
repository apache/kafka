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
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.Assertions;
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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OffsetsRequestManagerTest {

    private OffsetsRequestManager requestManager;
    private ConsumerMetadata metadata;
    private SubscriptionState subscriptionState;
    private MockTime time;
    private ApiVersions apiVersions;
    private static final String TEST_TOPIC = "t1";
    private static final TopicPartition TEST_PARTITION_1 = new TopicPartition(TEST_TOPIC, 1);
    private static final TopicPartition TEST_PARTITION_2 = new TopicPartition(TEST_TOPIC, 2);
    private static final Node LEADER_1 = new Node(0, "host1", 9092);
    private static final Node LEADER_2 = new Node(0, "host2", 9092);
    private static final IsolationLevel DEFAULT_ISOLATION_LEVEL = IsolationLevel.READ_COMMITTED;
    private static final int RETRY_BACKOFF_MS = 500;
    private static final int REQUEST_TIMEOUT_MS = 500;

    @BeforeEach
    public void setup() {
        metadata = mock(ConsumerMetadata.class);
        subscriptionState = mock(SubscriptionState.class);
        this.time = new MockTime(0);
        this.apiVersions = mock(ApiVersions.class);
        requestManager = new OffsetsRequestManager(subscriptionState, metadata,
                DEFAULT_ISOLATION_LEVEL, time, RETRY_BACKOFF_MS, REQUEST_TIMEOUT_MS,
                apiVersions, new LogContext());
    }

    @Test
    public void testListOffsetsRequest_Success() throws ExecutionException, InterruptedException {
        Map<TopicPartition, Long> timestampsToSearch = Collections.singletonMap(TEST_PARTITION_1,
                ListOffsetsRequest.EARLIEST_TIMESTAMP);

        expectSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> result = requestManager.fetchOffsets(
                timestampsToSearch,
                false);
        assertEquals(1, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        Map<TopicPartition, OffsetAndTimestamp> expectedOffsets = Collections.singletonMap(TEST_PARTITION_1, new OffsetAndTimestamp(5L, 1L));
        verifySuccessfulPollAndResponseReceived(result, expectedOffsets);
    }

    @Test
    public void testListOffsetsRequest_UnknownOffset() throws ExecutionException,
            InterruptedException {
        Map<TopicPartition, Long> timestampsToSearch = Collections.singletonMap(TEST_PARTITION_1,
                ListOffsetsRequest.EARLIEST_TIMESTAMP);

        expectSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> result = requestManager.fetchOffsets(
                timestampsToSearch,
                false);
        assertEquals(1, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        Map<TopicPartition, OffsetAndTimestamp> expectedOffsets = Collections.singletonMap(TEST_PARTITION_1, new OffsetAndTimestamp(5L, 1L));
        verifySuccessfulPollAndResponseReceived(result, expectedOffsets);
    }

    @Test
    public void testListOffsetsWaitingForMetadataUpdate_Timeout() {
        Map<TopicPartition, Long> timestampsToSearch = Collections.singletonMap(TEST_PARTITION_1,
                ListOffsetsRequest.EARLIEST_TIMESTAMP);

        // Building list offsets request fails with unknown leader
        expectFailedRequest_MissingLeader();
        CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> fetchOffsetsFuture =
                requestManager.fetchOffsets(timestampsToSearch, false);
        assertEquals(0, requestManager.requestsToSend());
        assertEquals(1, requestManager.requestsToRetry());
        verify(metadata).requestUpdate();
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        // Metadata update not happening within the time boundaries of the request future, so
        // future should time out.
        assertThrows(TimeoutException.class, () -> fetchOffsetsFuture.get(100L, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testListOffsetsRequestMultiplePartitions() throws ExecutionException,
            InterruptedException {
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        timestampsToSearch.put(TEST_PARTITION_1, ListOffsetsRequest.EARLIEST_TIMESTAMP);
        timestampsToSearch.put(TEST_PARTITION_2, ListOffsetsRequest.EARLIEST_TIMESTAMP);


        Map<TopicPartition, Node> partitionLeaders = new HashMap<>();
        partitionLeaders.put(TEST_PARTITION_1, LEADER_1);
        partitionLeaders.put(TEST_PARTITION_2, LEADER_1);
        expectSuccessfulRequest(partitionLeaders);
        CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> result = requestManager.fetchOffsets(
                timestampsToSearch,
                false);
        assertEquals(1, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        Map<TopicPartition, OffsetAndTimestamp> expectedOffsets = timestampsToSearch.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> new OffsetAndTimestamp(5L, 1L)));
        verifySuccessfulPollAndResponseReceived(result, expectedOffsets);
    }

    @Test
    public void testListOffsetsRequestEmpty() throws ExecutionException, InterruptedException {
        CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> result = requestManager.fetchOffsets(
                Collections.emptyMap(),
                false);
        assertEquals(0, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        NetworkClientDelegate.PollResult pollResult = requestManager.poll(time.milliseconds());
        assertTrue(pollResult.unsentRequests.isEmpty());

        assertEquals(0, requestManager.requestsToRetry());
        assertEquals(0, requestManager.requestsToSend());

        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());
        assertTrue(result.get().isEmpty());
    }

    @Test
    public void testListOffsetsRequestUnknownOffset() throws ExecutionException,
            InterruptedException {
        Map<TopicPartition, Long> timestampsToSearch = Collections.singletonMap(TEST_PARTITION_1,
                ListOffsetsRequest.EARLIEST_TIMESTAMP);

        expectSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> result = requestManager.fetchOffsets(
                timestampsToSearch,
                false);
        assertEquals(1, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        List<ListOffsetsResponseData.ListOffsetsTopicResponse> topicResponses = Collections.singletonList(
                mockUnknownOffsetResponse(TEST_PARTITION_1));

        NetworkClientDelegate.PollResult retriedPoll = requestManager.poll(time.milliseconds());
        verifySuccessfulPollAwaitingResponse(retriedPoll);
        NetworkClientDelegate.UnsentRequest unsentRequest = retriedPoll.unsentRequests.get(0);
        ClientResponse clientResponse = buildClientResponse(unsentRequest, topicResponses);
        clientResponse.onComplete();

        Map<TopicPartition, OffsetAndTimestamp> expectedOffsets =
                Collections.singletonMap(TEST_PARTITION_1, null);
        verifyRequestSuccessfullyCompleted(result, expectedOffsets);

    }

    @Test
    public void testListOffsetsWaitingForMetadataUpdate_RetrySucceeds() throws ExecutionException,
            InterruptedException {
        Map<TopicPartition, Long> timestampsToSearch = Collections.singletonMap(TEST_PARTITION_1,
                ListOffsetsRequest.EARLIEST_TIMESTAMP);

        // Building list offsets request fails with unknown leader
        expectFailedRequest_MissingLeader();
        CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> fetchOffsetsFuture =
                requestManager.fetchOffsets(timestampsToSearch,
                        false);
        assertEquals(0, requestManager.requestsToSend());
        assertEquals(1, requestManager.requestsToRetry());
        verify(metadata).requestUpdate();

        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        assertFalse(fetchOffsetsFuture.isDone());

        // Cluster metadata update. Previously failed attempt to build the request should be retried
        // and succeed
        expectSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        requestManager.onUpdate(new ClusterResource(""));
        assertEquals(1, requestManager.requestsToSend());

        Map<TopicPartition, OffsetAndTimestamp> expectedOffsets = Collections.singletonMap(
                TEST_PARTITION_1, new OffsetAndTimestamp(5L, 1L));
        verifySuccessfulPollAndResponseReceived(fetchOffsetsFuture, expectedOffsets);
    }

    @ParameterizedTest
    @MethodSource("retriableErrors")
    public void testFetchOffsetsFailsWithRetriableError_RetrySucceeds(Errors error) throws ExecutionException, InterruptedException {
        Map<TopicPartition, Long> timestampsToSearch = Collections.singletonMap(TEST_PARTITION_1,
                ListOffsetsRequest.EARLIEST_TIMESTAMP);

        // List offsets request successfully built
        expectSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> fetchOffsetsFuture = requestManager.fetchOffsets(
                timestampsToSearch,
                false);
        assertEquals(1, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        // Request successfully sent to single broker
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        verifySuccessfulPollAwaitingResponse(res);
        assertFalse(fetchOffsetsFuture.isDone());

        // Response received with error
        NetworkClientDelegate.UnsentRequest unsentRequest = res.unsentRequests.get(0);
        ClientResponse clientResponse = buildClientResponseWithErrors(
                unsentRequest,
                Collections.singletonMap(TEST_PARTITION_1, error));
        clientResponse.onComplete();
        assertFalse(fetchOffsetsFuture.isDone());
        assertEquals(1, requestManager.requestsToRetry());
        assertEquals(0, requestManager.requestsToSend());

        // Cluster metadata update. Failed requests should be retried and succeed
        expectSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        requestManager.onUpdate(new ClusterResource(""));
        assertEquals(1, requestManager.requestsToSend());

        Map<TopicPartition, OffsetAndTimestamp> expectedOffsets = Collections.singletonMap(TEST_PARTITION_1, new OffsetAndTimestamp(5L, 1L));
        verifySuccessfulPollAndResponseReceived(fetchOffsetsFuture, expectedOffsets);
    }

    @Test
    public void testFetchOffsetsNotSupportedErrorReturnsNullOffset() throws ExecutionException,
            InterruptedException {
        testFetchOffsetsResponseWithErrorAndUnknownOffsets(Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT);
    }

    @Test
    public void testFetchOffsetsWithUnknownOffsetInResponseReturnsNullOffset() throws ExecutionException,
            InterruptedException {
        testFetchOffsetsResponseWithErrorAndUnknownOffsets(Errors.NONE);
    }

    private void testFetchOffsetsResponseWithErrorAndUnknownOffsets(Errors error) throws ExecutionException, InterruptedException {
        Map<TopicPartition, Long> timestampsToSearch = Collections.singletonMap(TEST_PARTITION_1,
                ListOffsetsRequest.EARLIEST_TIMESTAMP);

        // List offsets request successfully built
        expectSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> fetchOffsetsFuture = requestManager.fetchOffsets(
                timestampsToSearch,
                false);
        assertEquals(1, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        // Request successfully sent to single broker
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        verifySuccessfulPollAwaitingResponse(res);
        assertFalse(fetchOffsetsFuture.isDone());

        // Response received with error
        NetworkClientDelegate.UnsentRequest unsentRequest = res.unsentRequests.get(0);
        ClientResponse clientResponse = buildClientResponseWithErrors(
                unsentRequest,
                Collections.singletonMap(TEST_PARTITION_1, error));
        clientResponse.onComplete();

        // Null offsets should be returned for each partition
        Map<TopicPartition, OffsetAndTimestamp> expectedOffsets = Collections.singletonMap(TEST_PARTITION_1, null);
        verifyRequestSuccessfullyCompleted(fetchOffsetsFuture, expectedOffsets);
    }

    @Test
    public void testFetchOffsetsPartiallyFailsWithRetriableError_RetrySucceeds() throws ExecutionException, InterruptedException {
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        timestampsToSearch.put(TEST_PARTITION_1, ListOffsetsRequest.EARLIEST_TIMESTAMP);
        timestampsToSearch.put(TEST_PARTITION_2, ListOffsetsRequest.EARLIEST_TIMESTAMP);

        Map<TopicPartition, OffsetAndTimestamp> expectedOffsets = timestampsToSearch.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> new OffsetAndTimestamp(5L, 1L)));

        // List offsets request to 2 brokers successfully built
        Map<TopicPartition, Node> partitionLeaders = new HashMap<>();
        partitionLeaders.put(TEST_PARTITION_1, LEADER_1);
        partitionLeaders.put(TEST_PARTITION_2, LEADER_2);
        expectSuccessfulRequest(partitionLeaders);
        CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> fetchOffsetsFuture = requestManager.fetchOffsets(
                timestampsToSearch,
                false);
        assertEquals(2, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        // Requests successfully sent to both brokers
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        verifySuccessfulPollAwaitingResponse(res, 2);
        assertFalse(fetchOffsetsFuture.isDone());

        // Mixed response with failures and successes. Offsets successfully fetched from one
        // broker but retriable UNKNOWN_LEADER_EPOCH received from second broker.
        NetworkClientDelegate.UnsentRequest unsentRequest1 = res.unsentRequests.get(0);
        ClientResponse clientResponse1 = buildClientResponse(
                unsentRequest1,
                Collections.singletonMap(TEST_PARTITION_1, expectedOffsets.get(TEST_PARTITION_1)));
        clientResponse1.onComplete();
        NetworkClientDelegate.UnsentRequest unsentRequest2 = res.unsentRequests.get(1);
        ClientResponse clientResponse2 = buildClientResponseWithErrors(
                unsentRequest2,
                Collections.singletonMap(TEST_PARTITION_2, Errors.UNKNOWN_LEADER_EPOCH));
        clientResponse2.onComplete();

        assertFalse(fetchOffsetsFuture.isDone());
        assertEquals(1, requestManager.requestsToRetry());
        assertEquals(0, requestManager.requestsToSend());

        // Cluster metadata update. Failed requests should be retried
        expectSuccessfulRequest(partitionLeaders);
        requestManager.onUpdate(new ClusterResource(""));
        assertEquals(1, requestManager.requestsToSend());

        // Following poll should send the request and get a successful response
        NetworkClientDelegate.PollResult retriedPoll = requestManager.poll(time.milliseconds());
        verifySuccessfulPollAwaitingResponse(retriedPoll);
        NetworkClientDelegate.UnsentRequest unsentRequest = retriedPoll.unsentRequests.get(0);
        ClientResponse clientResponse = buildClientResponse(unsentRequest,
                Collections.singletonMap(TEST_PARTITION_2, expectedOffsets.get(TEST_PARTITION_2)));
        clientResponse.onComplete();

        // Verify global result with the offset initially retrieved, and the offset that
        // initially failed but succeeded after a metadata update
        verifyRequestSuccessfullyCompleted(fetchOffsetsFuture, expectedOffsets);
    }

    @Test
    public void testFetchOffsetsFailedResponse_NonRetriableAuthError() {
        Map<TopicPartition, Long> timestampsToSearch = Collections.singletonMap(TEST_PARTITION_1,
                ListOffsetsRequest.EARLIEST_TIMESTAMP);

        // List offsets request successfully built
        expectSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> fetchOffsetsFuture =
                requestManager.fetchOffsets(
                        timestampsToSearch,
                        false);
        assertEquals(1, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        // Request successfully sent
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        verifySuccessfulPollAwaitingResponse(res);

        // Response received with non-retriable auth error
        NetworkClientDelegate.UnsentRequest unsentRequest = res.unsentRequests.get(0);
        ClientResponse clientResponse = buildClientResponseWithErrors(
                unsentRequest, Collections.singletonMap(TEST_PARTITION_2, Errors.TOPIC_AUTHORIZATION_FAILED));
        clientResponse.onComplete();

        verifyRequestCompletedWithErrorResponse(fetchOffsetsFuture, TopicAuthorizationException.class);
        assertEquals(0, requestManager.requestsToRetry());
        assertEquals(0, requestManager.requestsToSend());
    }

    @Test
    public void testFetchOffsetsFailedResponse_NonRetriableErrorTimeout() {
        Map<TopicPartition, Long> timestampsToSearch = Collections.singletonMap(TEST_PARTITION_1,
                ListOffsetsRequest.EARLIEST_TIMESTAMP);

        // List offsets request successfully built
        expectSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> fetchOffsetsFuture =
                requestManager.fetchOffsets(
                        timestampsToSearch,
                        false);
        assertEquals(1, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        // Request successfully sent
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        verifySuccessfulPollAwaitingResponse(res);

        // Response received
        NetworkClientDelegate.UnsentRequest unsentRequest = res.unsentRequests.get(0);
        ClientResponse clientResponse = buildClientResponseWithErrors(
                unsentRequest, Collections.singletonMap(TEST_PARTITION_2, Errors.BROKER_NOT_AVAILABLE));
        clientResponse.onComplete();

        assertFalse(fetchOffsetsFuture.isDone());
        assertThrows(TimeoutException.class, () -> fetchOffsetsFuture.get(100L, TimeUnit.MILLISECONDS));

        // Request completed with error. Nothing pending to be sent or retried
        assertEquals(0, requestManager.requestsToRetry());
        assertEquals(0, requestManager.requestsToSend());
    }

    @Test
    public void testFetchOffsetsFails_AuthenticationException() {
        Map<TopicPartition, Long> timestampsToSearch = Collections.singletonMap(TEST_PARTITION_1,
                ListOffsetsRequest.EARLIEST_TIMESTAMP);

        // List offsets request successfully built
        expectSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> fetchOffsetsFuture =
                requestManager.fetchOffsets(
                        timestampsToSearch,
                        false);
        assertEquals(1, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        // Request successfully sent
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        verifySuccessfulPollAwaitingResponse(res);

        // Response received with auth error
        NetworkClientDelegate.UnsentRequest unsentRequest = res.unsentRequests.get(0);
        ClientResponse clientResponse =
                buildClientResponseWithAuthenticationException(unsentRequest);
        clientResponse.onComplete();

        // Request completed with error. Nothing pending to be sent or retried
        verifyRequestCompletedWithErrorResponse(fetchOffsetsFuture, AuthenticationException.class);
        assertEquals(0, requestManager.requestsToRetry());
        assertEquals(0, requestManager.requestsToSend());
    }

    @Test
    public void testResetPositionsSendsNoRequestIfNoPartitionsNeedingReset() {
        when(subscriptionState.partitionsNeedingReset(time.milliseconds())).thenReturn(Collections.emptySet());
        requestManager.resetPositionsIfNeeded();
        assertEquals(0, requestManager.requestsToSend());
    }

    @Test
    public void testResetPositionsMissingLeader() {
        expectFailedRequest_MissingLeader();
        when(subscriptionState.partitionsNeedingReset(time.milliseconds())).thenReturn(Collections.singleton(TEST_PARTITION_1));
        when(subscriptionState.resetStrategy(any())).thenReturn(OffsetResetStrategy.EARLIEST);
        requestManager.resetPositionsIfNeeded();
        verify(metadata).requestUpdate();
        assertEquals(0, requestManager.requestsToSend());
    }

    @Test
    public void testResetPositionsSuccess_NoLeaderEpochInResponse() {
        testResetPositionsSuccessWithLeaderEpoch(Metadata.LeaderAndEpoch.noLeaderOrEpoch());
        verify(metadata, never()).updateLastSeenEpochIfNewer(any(), anyInt());
    }

    @Test
    public void testResetPositionsSuccess_LeaderEpochInResponse() {
        Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(Optional.of(LEADER_1),
                Optional.of(5));
        testResetPositionsSuccessWithLeaderEpoch(leaderAndEpoch);
        verify(metadata).updateLastSeenEpochIfNewer(TEST_PARTITION_1, leaderAndEpoch.epoch.get());
    }

    private void testResetPositionsSuccessWithLeaderEpoch(Metadata.LeaderAndEpoch leaderAndEpoch) {
        TopicPartition tp = TEST_PARTITION_1;
        Node leader = LEADER_1;
        OffsetResetStrategy strategy = OffsetResetStrategy.EARLIEST;
        long offset = 5L;
        Map<TopicPartition, OffsetAndTimestamp> expectedOffsets = Collections.singletonMap(tp,
                new OffsetAndTimestamp(offset, 1L, leaderAndEpoch.epoch));
        when(subscriptionState.partitionsNeedingReset(time.milliseconds())).thenReturn(Collections.singleton(tp));
        when(subscriptionState.resetStrategy(any())).thenReturn(strategy);
        expectSuccessfulRequest(Collections.singletonMap(tp, leader));

        requestManager.resetPositionsIfNeeded();
        assertEquals(1, requestManager.requestsToSend());

        // Reset positions response with offsets
        when(metadata.currentLeader(tp)).thenReturn(testLeaderEpoch(leader, leaderAndEpoch.epoch));
        NetworkClientDelegate.PollResult pollResult = requestManager.poll(time.milliseconds());
        NetworkClientDelegate.UnsentRequest unsentRequest = pollResult.unsentRequests.get(0);
        ClientResponse clientResponse = buildClientResponse(unsentRequest, expectedOffsets);
        clientResponse.onComplete();
        assertTrue(unsentRequest.future().isDone());
        assertFalse(unsentRequest.future().isCompletedExceptionally());
    }

    @Test
    public void testResetPositionsThrowsPreviousException() {
        when(subscriptionState.partitionsNeedingReset(time.milliseconds())).thenReturn(Collections.singleton(TEST_PARTITION_1));
        when(subscriptionState.resetStrategy(any())).thenReturn(OffsetResetStrategy.EARLIEST);
        expectSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));

        requestManager.resetPositionsIfNeeded();

        // Reset positions response with TopicAuthorizationException
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        NetworkClientDelegate.UnsentRequest unsentRequest = res.unsentRequests.get(0);
        ClientResponse clientResponse = buildClientResponseWithErrors(
                unsentRequest, Collections.singletonMap(TEST_PARTITION_1, Errors.TOPIC_AUTHORIZATION_FAILED));
        clientResponse.onComplete();

        assertTrue(unsentRequest.future().isDone());
        assertFalse(unsentRequest.future().isCompletedExceptionally());

        verify(subscriptionState).requestFailed(any(), anyLong());
        verify(metadata).requestUpdate();

        // Following resetPositions should raise the previous exception without performing any
        // request
        assertThrows(TopicAuthorizationException.class,
                () -> requestManager.resetPositionsIfNeeded());
        assertEquals(0, requestManager.requestsToSend());
    }

    @Test
    public void testValidatePositionsSendsNoRequestIfNoPartitionsNeedingValidation() {
        when(metadata.updateVersion()).thenReturn(-1);
        when(subscriptionState.partitionsNeedingValidation(time.milliseconds())).thenReturn(Collections.emptySet());
        requestManager.validatePositionsIfNeeded();
        assertEquals(0, requestManager.requestsToSend());
    }

    @Test
    public void testValidatePositionsSendsNoRequestIfPartitionsNeedValidationButNotKnownPosition() {
        when(metadata.updateVersion()).thenReturn(-1);
        when(subscriptionState.partitionsNeedingValidation(time.milliseconds())).thenReturn(Collections.singleton(TEST_PARTITION_1));
        when(subscriptionState.position(TEST_PARTITION_1)).thenReturn(null);
        requestManager.validatePositionsIfNeeded();
        assertEquals(0, requestManager.requestsToSend());
    }

    @Test
    public void testValidatePositionsSendsNoRequestIfLeaderUnknownApiVersion() {
        when(metadata.updateVersion()).thenReturn(1); // Metadata updated, all assigned partitions
        // should be validated;
        Set<TopicPartition> assignedPartitions = Collections.singleton(TEST_PARTITION_1);
        when(subscriptionState.assignedPartitions()).thenReturn(assignedPartitions);
        Metadata.LeaderAndEpoch currentLeader = new Metadata.LeaderAndEpoch(Optional.of(LEADER_1), Optional.of(1));
        when(metadata.currentLeader(TEST_PARTITION_1)).thenReturn(currentLeader);
        when(subscriptionState.partitionsNeedingValidation(time.milliseconds())).thenReturn(assignedPartitions);
        SubscriptionState.FetchPosition currentPosition = new SubscriptionState.FetchPosition(1L,
                Optional.of(1), currentLeader);
        when(subscriptionState.position(TEST_PARTITION_1)).thenReturn(currentPosition);
        when(apiVersions.get(currentLeader.leader.get().idString())).thenReturn(null);
        requestManager.validatePositionsIfNeeded();
        assertEquals(0, requestManager.requestsToSend());
    }

    @Test
    public void testValidatePositionsSuccess() {
        final int leaderEpoch = 1;
        Metadata.LeaderAndEpoch currentLeader = new Metadata.LeaderAndEpoch(Optional.of(LEADER_1), Optional.of(leaderEpoch));
        SubscriptionState.FetchPosition currentPosition = new SubscriptionState.FetchPosition(1L,
                Optional.of(1), currentLeader);
        expectSuccessfulValidatePositionsRequest(TEST_PARTITION_1, currentLeader, currentPosition);

        requestManager.validatePositionsIfNeeded();

        assertEquals(1, requestManager.requestsToSend());
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        NetworkClientDelegate.UnsentRequest unsentRequest = res.unsentRequests.get(0);

        final long endOffset = 5L;
        ClientResponse response =
                buildClientResponseForOffsetsForLeaderEpoch(unsentRequest, TEST_PARTITION_1,
                        leaderEpoch, endOffset, Errors.NONE);
        response.onComplete();

        assertEquals(0, requestManager.requestsToSend());

        OffsetForLeaderEpochResponseData.EpochEndOffset expectedEpochEndOffset =
                epochEndOffsetFromResponse(response);
        verify(subscriptionState).maybeCompleteValidation(TEST_PARTITION_1, currentPosition,
                expectedEpochEndOffset);
    }

    @Test
    public void testValidatePositionsSkippedForOldBroker() {
        final int leaderEpoch = 1;
        Metadata.LeaderAndEpoch currentLeader = new Metadata.LeaderAndEpoch(Optional.of(LEADER_1), Optional.of(leaderEpoch));
        SubscriptionState.FetchPosition currentPosition = new SubscriptionState.FetchPosition(1L,
                Optional.of(1), currentLeader);

        Set<TopicPartition> assignedPartitions = new HashSet<>(asList(TEST_PARTITION_1));
        when(subscriptionState.partitionsNeedingValidation(time.milliseconds())).thenReturn(assignedPartitions);
        when(subscriptionState.position(TEST_PARTITION_1)).thenReturn(currentPosition);
        NodeApiVersions versionUnderV3 = NodeApiVersions.create(ApiKeys.OFFSET_FOR_LEADER_EPOCH.id,
                (short) 0, (short) 2);
        when(apiVersions.get(currentLeader.leader.get().idString())).thenReturn(versionUnderV3);

        requestManager.validatePositionsIfNeeded();

        assertEquals(0, requestManager.requestsToSend());
        verify(subscriptionState).completeValidation(TEST_PARTITION_1);
    }

    @Test
    public void testValidatePositionsUnknownNode() {
        when(metadata.updateVersion()).thenReturn(1);
        final int leaderEpoch = 1;
        Metadata.LeaderAndEpoch currentLeader = new Metadata.LeaderAndEpoch(Optional.of(LEADER_1), Optional.of(leaderEpoch));
        SubscriptionState.FetchPosition currentPosition = new SubscriptionState.FetchPosition(1L,
                Optional.of(1), currentLeader);
        when(subscriptionState.partitionsNeedingValidation(time.milliseconds())).thenReturn(new HashSet<>(asList(TEST_PARTITION_1)));
        when(subscriptionState.position(TEST_PARTITION_1)).thenReturn(currentPosition);
        when(apiVersions.get(currentLeader.leader.get().idString())).thenReturn(null);

        requestManager.validatePositionsIfNeeded();

        assertEquals(0, requestManager.requestsToSend());
        verify(subscriptionState, never()).maybeCompleteValidation(any(), any(), any());
    }

    @Test
    public void testValidatePositionsThrowsPreviousException() {
        final int leaderEpoch = 1;
        Metadata.LeaderAndEpoch currentLeader = new Metadata.LeaderAndEpoch(Optional.of(LEADER_1), Optional.of(leaderEpoch));
        SubscriptionState.FetchPosition currentPosition = new SubscriptionState.FetchPosition(1L,
                Optional.of(1), currentLeader);
        expectSuccessfulValidatePositionsRequest(TEST_PARTITION_1, currentLeader, currentPosition);

        requestManager.validatePositionsIfNeeded();

        assertEquals(1, requestManager.requestsToSend());
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        NetworkClientDelegate.UnsentRequest unsentRequest = res.unsentRequests.get(0);

        final long endOffset = 5L;
        ClientResponse response =
                buildClientResponseForOffsetsForLeaderEpoch(unsentRequest, TEST_PARTITION_1,
                        leaderEpoch, endOffset, Errors.TOPIC_AUTHORIZATION_FAILED);
        response.onComplete();

        assertEquals(0, requestManager.requestsToSend());
        verify(subscriptionState).requestFailed(any(), anyLong());
        verify(metadata).requestUpdate();

        // Following validatePositions should raise the previous exception without performing any
        // request
        assertThrows(TopicAuthorizationException.class,
                () -> requestManager.validatePositionsIfNeeded());
        assertEquals(0, requestManager.requestsToSend());
    }

    private OffsetForLeaderEpochResponseData.EpochEndOffset epochEndOffsetFromResponse(ClientResponse response) {
        OffsetsForLeaderEpochResponse expectedResponse =
                (OffsetsForLeaderEpochResponse) response.responseBody();
        return expectedResponse.data().topics().find(TEST_PARTITION_1.topic()).partitions().get(0);
    }

    private void expectSuccessfulValidatePositionsRequest(TopicPartition partition,
                                                          Metadata.LeaderAndEpoch currentLeader,
                                                          SubscriptionState.FetchPosition currentPosition) {
        Set<TopicPartition> assignedPartitions = new HashSet<>(asList(partition));
        when(subscriptionState.partitionsNeedingValidation(time.milliseconds())).thenReturn(assignedPartitions);
        when(subscriptionState.position(partition)).thenReturn(currentPosition);
        when(apiVersions.get(currentLeader.leader.get().idString())).thenReturn(NodeApiVersions.create());
    }

    private ListOffsetsResponseData.ListOffsetsTopicResponse mockUnknownOffsetResponse(
            TopicPartition tp) {
        return new ListOffsetsResponseData.ListOffsetsTopicResponse()
                .setName(tp.topic())
                .setPartitions(Collections.singletonList(new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                        .setPartitionIndex(tp.partition())
                        .setErrorCode(Errors.NONE.code())
                        .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
                        .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)));
    }

    private static Stream<Arguments> retriableErrors() {
        return Stream.of(
                Arguments.of(Errors.NOT_LEADER_OR_FOLLOWER),
                Arguments.of(Errors.REPLICA_NOT_AVAILABLE),
                Arguments.of(Errors.KAFKA_STORAGE_ERROR),
                Arguments.of(Errors.OFFSET_NOT_AVAILABLE),
                Arguments.of(Errors.LEADER_NOT_AVAILABLE),
                Arguments.of(Errors.FENCED_LEADER_EPOCH),
                Arguments.of(Errors.BROKER_NOT_AVAILABLE),
                Arguments.of(Errors.INVALID_REQUEST),
                Arguments.of(Errors.UNKNOWN_LEADER_EPOCH),
                Arguments.of(Errors.UNKNOWN_TOPIC_OR_PARTITION));
    }

    private void verifySuccessfulPollAndResponseReceived(
            CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> actualResult,
            Map<TopicPartition, OffsetAndTimestamp> expectedResult) throws ExecutionException,
            InterruptedException {
        // Following poll should send the request and get a response
        NetworkClientDelegate.PollResult retriedPoll = requestManager.poll(time.milliseconds());
        verifySuccessfulPollAwaitingResponse(retriedPoll);
        NetworkClientDelegate.UnsentRequest unsentRequest = retriedPoll.unsentRequests.get(0);
        ClientResponse clientResponse = buildClientResponse(unsentRequest,
                expectedResult);
        clientResponse.onComplete();
        verifyRequestSuccessfullyCompleted(actualResult, expectedResult);
    }

    private void expectSuccessfulRequest(Map<TopicPartition, Node> partitionLeaders) {
        partitionLeaders.forEach((tp, broker) -> {
            when(metadata.currentLeader(tp)).thenReturn(testLeaderEpoch(broker,
                    Metadata.LeaderAndEpoch.noLeaderOrEpoch().epoch));
            when(subscriptionState.isAssigned(tp)).thenReturn(true);
        });
        when(metadata.fetch()).thenReturn(testClusterMetadata(partitionLeaders));
    }

    private void expectFailedRequest_MissingLeader() {
        when(metadata.currentLeader(any(TopicPartition.class))).thenReturn(
                new Metadata.LeaderAndEpoch(Optional.empty(), Optional.of(1)));
        when(subscriptionState.isAssigned(any(TopicPartition.class))).thenReturn(true);
    }

    private void verifySuccessfulPollAwaitingResponse(NetworkClientDelegate.PollResult pollResult) {
        verifySuccessfulPollAwaitingResponse(pollResult, 1);
    }

    private void verifySuccessfulPollAwaitingResponse(NetworkClientDelegate.PollResult pollResult,
                                                      int requestCount) {
        assertEquals(0, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());
        assertEquals(requestCount, pollResult.unsentRequests.size());
    }

    private void verifyRequestSuccessfullyCompleted(
            CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> actualResult,
            Map<TopicPartition, OffsetAndTimestamp> expectedResult) throws ExecutionException, InterruptedException {
        assertEquals(0, requestManager.requestsToRetry());
        assertEquals(0, requestManager.requestsToSend());

        assertTrue(actualResult.isDone());
        assertFalse(actualResult.isCompletedExceptionally());
        Map<TopicPartition, OffsetAndTimestamp> partitionOffsets = actualResult.get();
        assertEquals(expectedResult, partitionOffsets);

        // Validate that the subscription state has been updated for all non-null offsets retrieved
        Map<TopicPartition, OffsetAndTimestamp> validExpectedOffsets = expectedResult.entrySet().stream()
                .filter(entry -> entry.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        verifySubscriptionStateUpdated(validExpectedOffsets);
    }

    private void verifySubscriptionStateUpdated(Map<TopicPartition, OffsetAndTimestamp> expectedResult) {
        ArgumentCaptor<TopicPartition> tpCaptor = ArgumentCaptor.forClass(TopicPartition.class);
        ArgumentCaptor<Long> offsetCaptor = ArgumentCaptor.forClass(Long.class);

        verify(subscriptionState, times(expectedResult.size())).updateLastStableOffset(tpCaptor.capture(),
                offsetCaptor.capture());

        List<TopicPartition> updatedTp = tpCaptor.getAllValues();
        List<Long> updatedOffsets = offsetCaptor.getAllValues();
        assertEquals(expectedResult.keySet().size(), updatedOffsets.size());
        assertEquals(expectedResult.keySet(), new HashSet<>(updatedTp));

        assertEquals(expectedResult.values().size(), updatedOffsets.size());
        expectedResult.values().stream()
                .map(offsetAndTimestamp -> updatedOffsets.contains(offsetAndTimestamp.offset()))
                .forEach(Assertions::assertTrue);
    }

    private void verifyRequestCompletedWithErrorResponse(CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> actualResult,
                                                         Class<? extends Throwable> expectedFailure) {
        assertTrue(actualResult.isDone());
        assertTrue(actualResult.isCompletedExceptionally());
        Throwable failure = assertThrows(ExecutionException.class, actualResult::get);
        assertEquals(expectedFailure, failure.getCause().getClass());
    }

    private Metadata.LeaderAndEpoch testLeaderEpoch(Node leader, Optional<Integer> epoch) {
        return new Metadata.LeaderAndEpoch(Optional.of(leader), epoch);
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

    private ClientResponse buildClientResponse(
            final NetworkClientDelegate.UnsentRequest request,
            final Map<TopicPartition, OffsetAndTimestamp> partitionsOffsets) {
        List<ListOffsetsResponseData.ListOffsetsTopicResponse> topicResponses = new
                ArrayList<>();
        partitionsOffsets.forEach((tp, offsetAndTimestamp) -> {
            ListOffsetsResponseData.ListOffsetsTopicResponse topicResponse = ListOffsetsResponse.singletonListOffsetsTopicResponse(
                    tp, Errors.NONE,
                    offsetAndTimestamp.timestamp(),
                    offsetAndTimestamp.offset(),
                    offsetAndTimestamp.leaderEpoch().orElse(ListOffsetsResponse.UNKNOWN_EPOCH));
            topicResponses.add(topicResponse);
        });

        return buildClientResponse(request, topicResponses, false, null);
    }

    private ClientResponse buildClientResponse(
            final NetworkClientDelegate.UnsentRequest request,
            final List<ListOffsetsResponseData.ListOffsetsTopicResponse> topicResponses) {

        return buildClientResponse(request, topicResponses, false, null);
    }

    private ClientResponse buildClientResponseWithErrors(
            final NetworkClientDelegate.UnsentRequest request,
            final Map<TopicPartition, Errors> partitionErrors) {
        List<ListOffsetsResponseData.ListOffsetsTopicResponse> topicResponses = new ArrayList<>();
        partitionErrors.forEach((tp, error) -> topicResponses.add(ListOffsetsResponse.singletonListOffsetsTopicResponse(
                tp,
                error,
                ListOffsetsResponse.UNKNOWN_TIMESTAMP,
                ListOffsetsResponse.UNKNOWN_OFFSET,
                ListOffsetsResponse.UNKNOWN_EPOCH)));

        return buildClientResponse(request, topicResponses, false, null);
    }

    private ClientResponse buildClientResponseWithAuthenticationException(
            final NetworkClientDelegate.UnsentRequest request) {
        return buildClientResponse(request, Collections.emptyList(), true,
                new AuthenticationException("Authentication failed"));
    }

    private ClientResponse buildClientResponse(
            final NetworkClientDelegate.UnsentRequest request,
            final List<ListOffsetsResponseData.ListOffsetsTopicResponse> topicResponses,
            final boolean disconnected,
            final AuthenticationException authenticationException) {
        AbstractRequest abstractRequest = request.requestBuilder().build();
        assertTrue(abstractRequest instanceof ListOffsetsRequest);
        ListOffsetsResponse response = buildListOffsetsResponse(topicResponses);
        return new ClientResponse(
                new RequestHeader(ApiKeys.OFFSET_FETCH, ApiKeys.OFFSET_FETCH.latestVersion(), "", 1),
                request.callback(),
                "-1",
                time.milliseconds(),
                time.milliseconds(),
                disconnected,
                null,
                authenticationException,
                response
        );
    }

    private ListOffsetsResponse buildListOffsetsResponse(
            List<ListOffsetsResponseData.ListOffsetsTopicResponse> offsetsTopicResponses) {
        ListOffsetsResponseData responseData = new ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(offsetsTopicResponses);

        return new ListOffsetsResponse(responseData);
    }

    private ClientResponse buildClientResponseForOffsetsForLeaderEpoch(
            final NetworkClientDelegate.UnsentRequest request,
            TopicPartition topicPartition,
            int leaderEpoch,
            long endOffset,
            Errors error) {
        AbstractRequest abstractRequest = request.requestBuilder().build();
        assertTrue(abstractRequest instanceof OffsetsForLeaderEpochRequest);
        OffsetsForLeaderEpochResponse response =
                buildOffsetsForLeaderEpochResponse(topicPartition, leaderEpoch, endOffset, error);
        return new ClientResponse(
                new RequestHeader(ApiKeys.OFFSET_FOR_LEADER_EPOCH, ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion(),
                        "",
                        1),
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

    private OffsetsForLeaderEpochResponse buildOffsetsForLeaderEpochResponse(TopicPartition partition, int leaderEpoch, long endOffset, Errors error) {
        OffsetForLeaderEpochResponseData data = new OffsetForLeaderEpochResponseData();
        data.topics().add(new OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult()
                .setTopic(partition.topic())
                .setPartitions(asList(
                        new OffsetForLeaderEpochResponseData.EpochEndOffset()
                                .setPartition(partition.partition())
                                .setErrorCode(error.code())
                                .setLeaderEpoch(leaderEpoch)
                                .setEndOffset(endOffset)
                )));

        return new OffsetsForLeaderEpochResponse(data);
    }
}