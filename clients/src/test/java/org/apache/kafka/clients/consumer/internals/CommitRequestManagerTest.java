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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.internals.ConsumerTestBuilder.DEFAULT_GROUP_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CommitRequestManagerTest {

    private long retryBackoffMs = 100;
    private long retryBackoffMaxMs = 1000;
    private Node mockedNode = new Node(1, "host1", 9092);
    private SubscriptionState subscriptionState;
    private GroupState groupState;
    private LogContext logContext;
    private MockTime time;
    private CoordinatorRequestManager coordinatorRequestManager;
    private Properties props;

    @BeforeEach
    public void setup() {
        this.logContext = new LogContext();
        this.time = new MockTime(0);
        this.subscriptionState = mock(SubscriptionState.class);
        this.coordinatorRequestManager = mock(CoordinatorRequestManager.class);
        this.groupState = new GroupState(DEFAULT_GROUP_ID, Optional.empty());

        this.props = new Properties();
        this.props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
        this.props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        this.props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    @Test
    public void testPoll_SkipIfCoordinatorUnknown() {
        CommitRequestManager commitRequestManger = create(false, 0);
        assertPoll(false, 0, commitRequestManger);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("t1", 0), new OffsetAndMetadata(0));
        commitRequestManger.addOffsetCommitRequest(offsets);
        assertPoll(false, 0, commitRequestManger);
    }

    @Test
    public void testPoll_EnsureManualCommitSent() {
        CommitRequestManager commitRequestManger = create(false, 0);
        assertPoll(0, commitRequestManger);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("t1", 0), new OffsetAndMetadata(0));
        commitRequestManger.addOffsetCommitRequest(offsets);
        assertPoll(1, commitRequestManger);
    }

    @Test
    public void testPoll_EnsureAutocommitSent() {
        CommitRequestManager commitRequestManger = create(true, 100);
        assertPoll(0, commitRequestManger);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("t1", 0), new OffsetAndMetadata(0));
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        when(subscriptionState.allConsumed()).thenReturn(offsets);
        time.sleep(100);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        assertPoll(1, commitRequestManger);
    }

    @Test
    public void testPoll_EnsureCorrectInflightRequestBufferSize() {
        CommitRequestManager commitManager = create(false, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        // Create some offset commit requests
        Map<TopicPartition, OffsetAndMetadata> offsets1 = new HashMap<>();
        offsets1.put(new TopicPartition("test", 0), new OffsetAndMetadata(10L));
        offsets1.put(new TopicPartition("test", 1), new OffsetAndMetadata(20L));
        Map<TopicPartition, OffsetAndMetadata> offsets2 = new HashMap<>();
        offsets2.put(new TopicPartition("test", 3), new OffsetAndMetadata(20L));
        offsets2.put(new TopicPartition("test", 4), new OffsetAndMetadata(20L));

        // Add the requests to the CommitRequestManager and store their futures
        ArrayList<CompletableFuture<Void>> commitFutures = new ArrayList<>();
        ArrayList<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> fetchFutures = new ArrayList<>();
        commitFutures.add(commitManager.addOffsetCommitRequest(offsets1));
        fetchFutures.add(commitManager.addOffsetFetchRequest(Collections.singleton(new TopicPartition("test", 0))));
        commitFutures.add(commitManager.addOffsetCommitRequest(offsets2));
        fetchFutures.add(commitManager.addOffsetFetchRequest(Collections.singleton(new TopicPartition("test", 1))));

        // Poll the CommitRequestManager and verify that the inflightOffsetFetches size is correct
        NetworkClientDelegate.PollResult result = commitManager.poll(time.milliseconds());
        assertEquals(4, result.unsentRequests.size());
        assertTrue(result.unsentRequests
                .stream().anyMatch(r -> r.requestBuilder() instanceof OffsetCommitRequest.Builder));
        assertTrue(result.unsentRequests
                .stream().anyMatch(r -> r.requestBuilder() instanceof OffsetFetchRequest.Builder));
        assertFalse(commitManager.pendingRequests.hasUnsentRequests());
        assertEquals(2, commitManager.pendingRequests.inflightOffsetFetches.size());

        // Verify that the inflight offset fetch requests have been removed from the pending request buffer
        commitFutures.forEach(f -> f.complete(null));
        fetchFutures.forEach(f -> f.complete(null));
        assertEquals(0, commitManager.pendingRequests.inflightOffsetFetches.size());
    }

    @Test
    public void testPoll_EnsureEmptyPendingRequestAfterPoll() {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        commitRequestManger.addOffsetCommitRequest(new HashMap<>());
        assertEquals(1, commitRequestManger.unsentOffsetCommitRequests().size());
        assertEquals(1, commitRequestManger.poll(time.milliseconds()).unsentRequests.size());
        assertTrue(commitRequestManger.unsentOffsetCommitRequests().isEmpty());
        assertEmptyPendingRequests(commitRequestManger);
    }

    @Test
    public void testAutocommit_ResendAutocommitAfterException() {
        CommitRequestManager commitRequestManger = create(true, 100);
        time.sleep(100);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        List<CompletableFuture<ClientResponse>> futures = assertPoll(1, commitRequestManger);
        time.sleep(99);
        // complete the autocommit request (exceptionally)
        futures.get(0).complete(mockOffsetCommitResponse(
            "topic",
            1,
            (short) 1,
            Errors.COORDINATOR_LOAD_IN_PROGRESS));

        // we can then autocommit again
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        assertPoll(0, commitRequestManger);
        time.sleep(1);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        assertPoll(1, commitRequestManger);
        assertEmptyPendingRequests(commitRequestManger);
    }

    @Test
    public void testAutocommit_EnsureOnlyOneInflightRequest() {
        CommitRequestManager commitRequestManger = create(true, 100);
        time.sleep(100);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        List<CompletableFuture<ClientResponse>> futures = assertPoll(1, commitRequestManger);
        time.sleep(100);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        // We want to make sure we don't resend autocommit if the previous request has not been completed
        assertPoll(0, commitRequestManger);
        assertEmptyPendingRequests(commitRequestManger);

        // complete the unsent request and re-poll
        futures.get(0).complete(buildOffsetCommitClientResponse(new OffsetCommitResponse(0, new HashMap<>()), Errors.NONE));
        assertPoll(1, commitRequestManger);
    }

    @Test
    public void testOffsetFetchRequest_EnsureDuplicatedRequestSucceed() {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(new TopicPartition("t1", 0));
        List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> futures = sendAndVerifyDuplicatedOffsetFetchRequests(
                commitRequestManger,
                partitions,
                2,
                Errors.NONE);
        futures.forEach(f -> {
            assertTrue(f.isDone());
            assertFalse(f.isCompletedExceptionally());
        });
        // expecting the buffers to be emptied after being completed successfully
        commitRequestManger.poll(0);
        assertEmptyPendingRequests(commitRequestManger);
    }

    @ParameterizedTest
    @MethodSource("offsetFetchExceptionSupplier")
    public void testOffsetFetchRequest_ErroredRequests(final Errors error, final boolean isRetriable) {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(new TopicPartition("t1", 0));
        List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> futures = sendAndVerifyDuplicatedOffsetFetchRequests(
            commitRequestManger,
            partitions,
            5,
            error);
        // we only want to make sure to purge the outbound buffer for non-retriables, so retriable will be re-queued.
        if (isRetriable)
            testRetriable(commitRequestManger, futures);
        else {
            testNonRetriable(futures);
            assertEmptyPendingRequests(commitRequestManger);
        }

        assertCoordinatorDisconnect(error);
    }

    @ParameterizedTest
    @MethodSource("offsetCommitExceptionSupplier")
    public void testOffsetCommitRequest_ErroredRequests(final Errors error, final boolean isRetriable) {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        sendAndVerifyOffsetCommitRequests(commitRequestManger, offsets, error);

        assertExceptionHandling(commitRequestManger, error);
        assertCoordinatorDisconnect(error);
    }

    @Test
    public void testEnsureBackoffRetryOnOffsetCommitRequestTimeout() {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        commitRequestManger.addOffsetCommitRequest(offsets);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        res.unsentRequests.get(0).handler().onFailure(time.milliseconds(), new TimeoutException());

        assertTrue(commitRequestManger.pendingRequests.hasUnsentRequests());
        assertEquals(1, commitRequestManger.unsentOffsetCommitRequests().size());
        assertRetryBackOff(commitRequestManger, retryBackoffMs);
    }

    private void assertCoordinatorDisconnect(final Errors error) {
        if (error.exception() instanceof DisconnectException) {
            verify(coordinatorRequestManager).markCoordinatorUnknown(any(), any());
        }
    }

    private void assertExceptionHandling(CommitRequestManager commitRequestManger, Errors errors) {
        long remainBackoffMs = retryBackoffMs;
        switch (errors) {
            case NOT_COORDINATOR:
            case COORDINATOR_NOT_AVAILABLE:
            case REQUEST_TIMED_OUT:
                verify(coordinatorRequestManager).markCoordinatorUnknown(any(), anyLong());
                assertPollDoesNotReturn(commitRequestManger, remainBackoffMs);
                break;
            case UNKNOWN_TOPIC_OR_PARTITION:
            case COORDINATOR_LOAD_IN_PROGRESS:
                assertRetryBackOff(commitRequestManger, remainBackoffMs);
                break;
            case GROUP_AUTHORIZATION_FAILED:
                // failed
                break;
            case TOPIC_AUTHORIZATION_FAILED:
            case OFFSET_METADATA_TOO_LARGE:
            case INVALID_COMMIT_OFFSET_SIZE:
                assertPollDoesNotReturn(commitRequestManger, Long.MAX_VALUE);
                break;
            case FENCED_INSTANCE_ID:
                // This is a fatal failure, so we should not retry
                assertPollDoesNotReturn(commitRequestManger, Long.MAX_VALUE);
                break;
            default:
                assertPollDoesNotReturn(commitRequestManger, Long.MAX_VALUE);
        }
    }

    private void assertPollDoesNotReturn(CommitRequestManager commitRequestManager, long assertNextPollMs) {
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        assertEquals(assertNextPollMs, res.timeUntilNextPollMs);
    }

    private void assertRetryBackOff(CommitRequestManager commitRequestManager, long retryBackoffMs) {
        assertPollDoesNotReturn(commitRequestManager, retryBackoffMs);
        time.sleep(retryBackoffMs - 1);
        assertPollDoesNotReturn(commitRequestManager, 1);
        time.sleep(1);
        assertPoll(1, commitRequestManager);
    }

    private void testRetriable(final CommitRequestManager commitRequestManger,
                               final List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> futures) {
        futures.forEach(f -> assertFalse(f.isDone()));

        time.sleep(500);
        commitRequestManger.poll(time.milliseconds());
        futures.forEach(f -> assertFalse(f.isDone()));
    }

    private void testNonRetriable(final List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> futures) {
        futures.forEach(f -> assertTrue(f.isCompletedExceptionally()));
    }

    // Supplies (error, isRetriable)
    private static Stream<Arguments> offsetCommitExceptionSupplier() {
        return Stream.of(
            Arguments.of(Errors.NOT_COORDINATOR, true),
            Arguments.of(Errors.COORDINATOR_LOAD_IN_PROGRESS, true),
            Arguments.of(Errors.UNKNOWN_SERVER_ERROR, false),
            Arguments.of(Errors.GROUP_AUTHORIZATION_FAILED, false),
            Arguments.of(Errors.OFFSET_METADATA_TOO_LARGE, false),
            Arguments.of(Errors.INVALID_COMMIT_OFFSET_SIZE, false),
            Arguments.of(Errors.UNKNOWN_TOPIC_OR_PARTITION, true),
            Arguments.of(Errors.COORDINATOR_NOT_AVAILABLE, true),
            Arguments.of(Errors.NOT_COORDINATOR, true),
            Arguments.of(Errors.REQUEST_TIMED_OUT, true),
            Arguments.of(Errors.FENCED_INSTANCE_ID, false),
            Arguments.of(Errors.TOPIC_AUTHORIZATION_FAILED, false));
    }

    // Supplies (error, isRetriable)
    private static Stream<Arguments> offsetFetchExceptionSupplier() {
        // fetchCommit is only retrying on a subset of RetriableErrors
        return Stream.of(
            Arguments.of(Errors.NOT_COORDINATOR, true),
            Arguments.of(Errors.COORDINATOR_LOAD_IN_PROGRESS, true),
            Arguments.of(Errors.UNKNOWN_SERVER_ERROR, false),
            Arguments.of(Errors.GROUP_AUTHORIZATION_FAILED, false),
            Arguments.of(Errors.OFFSET_METADATA_TOO_LARGE, false),
            Arguments.of(Errors.INVALID_COMMIT_OFFSET_SIZE, false),
            Arguments.of(Errors.UNKNOWN_TOPIC_OR_PARTITION, false),
            Arguments.of(Errors.COORDINATOR_NOT_AVAILABLE, false),
            Arguments.of(Errors.NOT_COORDINATOR, true),
            Arguments.of(Errors.REQUEST_TIMED_OUT, false),
            Arguments.of(Errors.FENCED_INSTANCE_ID, false),
            Arguments.of(Errors.TOPIC_AUTHORIZATION_FAILED, false));
    }

    @ParameterizedTest
    @MethodSource("partitionDataErrorSupplier")
    public void testOffsetFetchRequest_PartitionDataError(final Errors error, final boolean isRetriable) {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        Set<TopicPartition> partitions = new HashSet<>();
        TopicPartition tp1 = new TopicPartition("t1", 2);
        TopicPartition tp2 = new TopicPartition("t2", 3);
        partitions.add(tp1);
        partitions.add(tp2);
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future =
                commitRequestManger.addOffsetFetchRequest(partitions);

        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());

        // Setting 1 partition with error
        HashMap<TopicPartition, OffsetFetchResponse.PartitionData> topicPartitionData = new HashMap<>();
        topicPartitionData.put(tp1, new OffsetFetchResponse.PartitionData(100L, Optional.of(1), "metadata", error));
        topicPartitionData.put(tp2, new OffsetFetchResponse.PartitionData(100L, Optional.of(1), "metadata", Errors.NONE));

        res.unsentRequests.get(0).handler().onComplete(buildOffsetFetchClientResponse(
                res.unsentRequests.get(0),
                topicPartitionData,
                Errors.NONE));
        if (isRetriable)
            testRetriable(commitRequestManger, Collections.singletonList(future));
        else
            testNonRetriable(Collections.singletonList(future));
    }

    private static void assertEmptyPendingRequests(CommitRequestManager commitRequestManger) {
        assertTrue(commitRequestManger.pendingRequests.inflightOffsetFetches.isEmpty());
        assertTrue(commitRequestManger.pendingRequests.unsentOffsetFetches.isEmpty());
        assertTrue(commitRequestManger.pendingRequests.unsentOffsetCommits.isEmpty());
    }

    // Supplies (error, isRetriable)
    private static Stream<Arguments> partitionDataErrorSupplier() {
        return Stream.of(
            Arguments.of(Errors.UNSTABLE_OFFSET_COMMIT, true),
            Arguments.of(Errors.UNKNOWN_TOPIC_OR_PARTITION, false),
            Arguments.of(Errors.TOPIC_AUTHORIZATION_FAILED, false),
            Arguments.of(Errors.UNKNOWN_SERVER_ERROR, false));
    }

    private List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> sendAndVerifyDuplicatedOffsetFetchRequests(
            final CommitRequestManager commitRequestManger,
            final Set<TopicPartition> partitions,
            int numRequest,
            final Errors error) {
        List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> futures = new ArrayList<>();

        for (int i = 0; i < numRequest; i++) {
            futures.add(commitRequestManger.addOffsetFetchRequest(partitions));
        }

        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        res.unsentRequests.get(0).handler().onComplete(buildOffsetFetchClientResponse(res.unsentRequests.get(0),
            partitions, error));
        res = commitRequestManger.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        return futures;
    }

    private void sendAndVerifyOffsetCommitRequests(
        final CommitRequestManager commitRequestManger,
        final Map<TopicPartition, OffsetAndMetadata> offsets,
        final Errors error) {
        commitRequestManger.addOffsetCommitRequest(offsets);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        res.unsentRequests.get(0).future().complete(mockOffsetCommitResponse("topic", 1, (short) 1, error));
        res = commitRequestManger.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
    }

    private List<CompletableFuture<ClientResponse>> assertPoll(
        final int numRes,
        final CommitRequestManager manager) {
        return assertPoll(true, numRes, manager);
    }

    private List<CompletableFuture<ClientResponse>> assertPoll(
        final boolean coordinatorDiscovered,
        final int numRes,
        final CommitRequestManager manager) {
        if (coordinatorDiscovered) {
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        } else {
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.empty());
        }
        NetworkClientDelegate.PollResult res = manager.poll(time.milliseconds());
        assertEquals(numRes, res.unsentRequests.size());

        return res.unsentRequests.stream().map(NetworkClientDelegate.UnsentRequest::future).collect(Collectors.toList());
    }

    private CommitRequestManager create(final boolean autoCommitEnabled, final long autoCommitInterval) {
        props.setProperty(AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitInterval));
        props.setProperty(ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommitEnabled));
        return spy(new CommitRequestManager(
            this.time,
            this.logContext,
            this.subscriptionState,
            new ConsumerConfig(props),
            this.coordinatorRequestManager,
            this.groupState,
            retryBackoffMs,
            retryBackoffMaxMs,
            0));
    }

    private ClientResponse buildOffsetFetchClientResponse(
            final NetworkClientDelegate.UnsentRequest request,
            final Set<TopicPartition> topicPartitions,
            final Errors error) {
        HashMap<TopicPartition, OffsetFetchResponse.PartitionData> topicPartitionData = new HashMap<>();
        topicPartitions.forEach(tp -> topicPartitionData.put(tp, new OffsetFetchResponse.PartitionData(
                100L,
                Optional.of(1),
                "metadata",
                Errors.NONE)));
        return buildOffsetFetchClientResponse(request, topicPartitionData, error);
    }

    private ClientResponse buildOffsetCommitClientResponse(final OffsetCommitResponse commitResponse,
                                                           final Errors error) {
        OffsetCommitResponseData data = new OffsetCommitResponseData();
        OffsetCommitResponse response = new OffsetCommitResponse(data);
        short apiVersion = 1;
        return new ClientResponse(
            new RequestHeader(ApiKeys.OFFSET_COMMIT, apiVersion, "", 1),
            null,
            "-1",
            time.milliseconds(),
            time.milliseconds(),
            false,
            null,
            null,
            commitResponse
        );
    }

    public ClientResponse mockOffsetCommitResponse(String topic, int partition, short apiKeyVersion, Errors error) {
        OffsetCommitResponseData responseData = new OffsetCommitResponseData()
            .setTopics(Arrays.asList(
                new OffsetCommitResponseData.OffsetCommitResponseTopic()
                    .setName(topic)
                    .setPartitions(Collections.singletonList(
                        new OffsetCommitResponseData.OffsetCommitResponsePartition()
                            .setErrorCode(error.code())
                            .setPartitionIndex(partition)))));
        OffsetCommitResponse response = mock(OffsetCommitResponse.class);
        when(response.data()).thenReturn(responseData);
        return new ClientResponse(
            new RequestHeader(ApiKeys.OFFSET_COMMIT, apiKeyVersion, "", 1),
            null,
            "-1",
            time.milliseconds(),
            time.milliseconds(),
            false,
            null,
            null,
            new OffsetCommitResponse(responseData)
        );
    }

    private ClientResponse buildOffsetFetchClientResponse(
            final NetworkClientDelegate.UnsentRequest request,
            final Map<TopicPartition, OffsetFetchResponse.PartitionData> topicPartitionData,
            final Errors error) {
        AbstractRequest abstractRequest = request.requestBuilder().build();
        assertTrue(abstractRequest instanceof OffsetFetchRequest);
        OffsetFetchRequest offsetFetchRequest = (OffsetFetchRequest) abstractRequest;
        OffsetFetchResponse response =
                new OffsetFetchResponse(error, topicPartitionData);
        return new ClientResponse(
                new RequestHeader(ApiKeys.OFFSET_FETCH, offsetFetchRequest.version(), "", 1),
                request.handler(),
                "-1",
                time.milliseconds(),
                time.milliseconds(),
                false,
                null,
                null,
                response
        );
    }
}
