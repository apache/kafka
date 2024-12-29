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
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
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
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singleton;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CommitRequestManagerTest {

    private final long retryBackoffMs = 100;
    private final long retryBackoffMaxMs = 1000;
    private static final String CONSUMER_COORDINATOR_METRICS = "consumer-coordinator-metrics";
    private static final String DEFAULT_GROUP_ID = "group-id";
    private static final String DEFAULT_GROUP_INSTANCE_ID = "group-instance-id";
    private final Node mockedNode = new Node(1, "host1", 9092);
    private SubscriptionState subscriptionState;
    private ConsumerMetadata metadata;
    private LogContext logContext;
    private MockTime time;
    private CoordinatorRequestManager coordinatorRequestManager;
    private OffsetCommitCallbackInvoker offsetCommitCallbackInvoker;
    private final Metrics metrics = new Metrics();
    private Properties props;

    private final int defaultApiTimeoutMs = 60000;


    @BeforeEach
    public void setup() {
        this.logContext = new LogContext();
        this.time = new MockTime(0);
        this.subscriptionState = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.EARLIEST);
        this.metadata = mock(ConsumerMetadata.class);
        this.coordinatorRequestManager = mock(CoordinatorRequestManager.class);
        this.offsetCommitCallbackInvoker = mock(OffsetCommitCallbackInvoker.class);
        this.props = new Properties();
        this.props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
        this.props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        this.props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    @Test
    public void testOffsetFetchRequestStateToStringBase() {
        ConsumerConfig config = mock(ConsumerConfig.class);

        CommitRequestManager commitRequestManager = new CommitRequestManager(
                time,
                logContext,
                subscriptionState,
                config,
                coordinatorRequestManager,
                offsetCommitCallbackInvoker,
                DEFAULT_GROUP_ID,
                Optional.of(DEFAULT_GROUP_INSTANCE_ID),
                retryBackoffMs,
                retryBackoffMaxMs,
                OptionalDouble.of(0),
                metrics,
                metadata);

        commitRequestManager.onMemberEpochUpdated(Optional.of(1), Uuid.randomUuid().toString());
        Set<TopicPartition> requestedPartitions = Collections.singleton(new TopicPartition("topic-1", 1));

        CommitRequestManager.OffsetFetchRequestState offsetFetchRequestState = commitRequestManager.createOffsetFetchRequest(requestedPartitions, 0);

        TimedRequestState timedRequestState = new TimedRequestState(
                logContext,
                CommitRequestManager.class.getSimpleName(),
                retryBackoffMs,
                2,
                retryBackoffMaxMs,
                0,
                TimedRequestState.deadlineTimer(time, 0)
        );

        String target = timedRequestState.toStringBase() +
                ", " + offsetFetchRequestState.memberInfo +
                ", requestedPartitions=" + offsetFetchRequestState.requestedPartitions;

        assertDoesNotThrow(timedRequestState::toString);
        assertFalse(target.contains("Optional"));
        assertEquals(target, offsetFetchRequestState.toStringBase());
    }

    @Test
    public void testPollSkipIfCoordinatorUnknown() {
        CommitRequestManager commitRequestManager = create(false, 0);
        assertPoll(false, 0, commitRequestManager);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("t1", 0), new OffsetAndMetadata(0));
        commitRequestManager.commitAsync(Optional.of(offsets));
        assertPoll(false, 0, commitRequestManager);
    }

    @Test
    public void testAsyncCommitWhileCoordinatorUnknownIsSentOutWhenCoordinatorDiscovered() {
        CommitRequestManager commitRequestManager = create(false, 0);
        assertPoll(false, 0, commitRequestManager);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("t1", 0), new OffsetAndMetadata(0));
        commitRequestManager.commitAsync(Optional.of(offsets));
        assertPoll(false, 0, commitRequestManager);
        assertPoll(true, 1, commitRequestManager);
    }

    @Test
    public void testPollEnsureManualCommitSent() {
        CommitRequestManager commitRequestManager = create(false, 0);
        assertPoll(0, commitRequestManager);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("t1", 0), new OffsetAndMetadata(0));
        commitRequestManager.commitAsync(Optional.of(offsets));
        assertPoll(1, commitRequestManager);
    }

    @Test
    public void testPollEnsureAutocommitSent() {
        TopicPartition tp = new TopicPartition("t1", 1);
        subscriptionState.assignFromUser(Collections.singleton(tp));
        subscriptionState.seek(tp, 100);
        CommitRequestManager commitRequestManager = create(true, 100);
        assertPoll(0, commitRequestManager);

        commitRequestManager.updateAutoCommitTimer(time.milliseconds());
        time.sleep(100);
        commitRequestManager.updateAutoCommitTimer(time.milliseconds());
        List<NetworkClientDelegate.FutureCompletionHandler> pollResults = assertPoll(1, commitRequestManager);
        pollResults.forEach(v -> v.onComplete(mockOffsetCommitResponse(
                "t1",
                1,
                (short) 1,
                Errors.NONE)));

        assertEquals(0.03, (double) getMetric("commit-rate").metricValue(), 0.01);
        assertEquals(1.0, getMetric("commit-total").metricValue());
    }

    @Test
    public void testPollEnsureCorrectInflightRequestBufferSize() {
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
        long deadlineMs = time.milliseconds() + defaultApiTimeoutMs;
        commitManager.commitSync(Optional.of(offsets1), deadlineMs);
        commitManager.fetchOffsets(Collections.singleton(new TopicPartition("test", 0)), deadlineMs);
        commitManager.commitSync(Optional.of(offsets2), deadlineMs);
        commitManager.fetchOffsets(Collections.singleton(new TopicPartition("test", 1)), deadlineMs);

        // Poll the CommitRequestManager and verify that the inflightOffsetFetches size is correct
        NetworkClientDelegate.PollResult result = commitManager.poll(time.milliseconds());
        assertEquals(4, result.unsentRequests.size());
        assertTrue(result.unsentRequests
                .stream().anyMatch(r -> r.requestBuilder() instanceof OffsetCommitRequest.Builder));
        assertTrue(result.unsentRequests
                .stream().anyMatch(r -> r.requestBuilder() instanceof OffsetFetchRequest.Builder));
        assertFalse(commitManager.pendingRequests.hasUnsentRequests());
        assertEquals(2, commitManager.pendingRequests.inflightOffsetFetches.size());

        // Complete requests with a response
        result.unsentRequests.forEach(req -> {
            if (req.requestBuilder() instanceof OffsetFetchRequest.Builder) {
                req.handler().onComplete(buildOffsetFetchClientResponse(req, Collections.emptySet(), Errors.NONE));
            } else {
                req.handler().onComplete(buildOffsetCommitClientResponse(new OffsetCommitResponse(0, new HashMap<>())));
            }
        });

        // Verify that the inflight offset fetch requests have been removed from the pending request buffer
        assertEquals(0, commitManager.pendingRequests.inflightOffsetFetches.size());
    }

    @Test
    public void testPollEnsureEmptyPendingRequestAfterPoll() {
        CommitRequestManager commitRequestManager = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
            new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));
        commitRequestManager.commitAsync(Optional.of(offsets));
        assertEquals(1, commitRequestManager.unsentOffsetCommitRequests().size());
        assertEquals(1, commitRequestManager.poll(time.milliseconds()).unsentRequests.size());
        assertTrue(commitRequestManager.unsentOffsetCommitRequests().isEmpty());
        assertEmptyPendingRequests(commitRequestManager);
    }

    @Test
    public void testCommitSync() {
        subscriptionState = mock(SubscriptionState.class);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        TopicPartition tp = new TopicPartition("topic", 1);
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(0, Optional.of(1), "");
        Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(tp, offsetAndMetadata);

        CommitRequestManager commitRequestManager = create(false, 100);
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future = commitRequestManager.commitSync(
            Optional.of(offsets), time.milliseconds() + defaultApiTimeoutMs);
        assertEquals(1, commitRequestManager.unsentOffsetCommitRequests().size());
        List<NetworkClientDelegate.FutureCompletionHandler> pollResults = assertPoll(1, commitRequestManager);
        pollResults.forEach(v -> v.onComplete(mockOffsetCommitResponse(
            "topic",
            1,
            (short) 1,
            Errors.NONE)));

        verify(subscriptionState, never()).allConsumed();
        verify(metadata).updateLastSeenEpochIfNewer(tp, 1);
        Map<TopicPartition, OffsetAndMetadata> commitOffsets = assertDoesNotThrow(() -> future.get());
        assertTrue(future.isDone());
        assertEquals(offsets, commitOffsets);
    }

    @Test
    public void testCommitSyncWithEmptyOffsets() {
        subscriptionState = mock(SubscriptionState.class);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        TopicPartition tp = new TopicPartition("topic", 1);
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(0, Optional.of(1), "");
        Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(tp, offsetAndMetadata);
        doReturn(offsets).when(subscriptionState).allConsumed();

        CommitRequestManager commitRequestManager = create(false, 100);
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future = commitRequestManager.commitSync(
            Optional.empty(), time.milliseconds() + defaultApiTimeoutMs);
        assertEquals(1, commitRequestManager.unsentOffsetCommitRequests().size());
        List<NetworkClientDelegate.FutureCompletionHandler> pollResults = assertPoll(1, commitRequestManager);
        pollResults.forEach(v -> v.onComplete(mockOffsetCommitResponse(
            "topic",
            1,
            (short) 1,
            Errors.NONE)));

        verify(subscriptionState).allConsumed();
        verify(metadata).updateLastSeenEpochIfNewer(tp, 1);
        Map<TopicPartition, OffsetAndMetadata> commitOffsets = assertDoesNotThrow(() -> future.get());
        assertTrue(future.isDone());
        assertEquals(offsets, commitOffsets);
    }

    @Test
    public void testCommitSyncWithEmptyAllConsumedOffsets() {
        subscriptionState = mock(SubscriptionState.class);
        doReturn(Map.of()).when(subscriptionState).allConsumed();

        CommitRequestManager commitRequestManager = create(true, 100);
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future = commitRequestManager.commitSync(
            Optional.empty(), time.milliseconds() + defaultApiTimeoutMs);

        verify(subscriptionState).allConsumed();
        Map<TopicPartition, OffsetAndMetadata> commitOffsets = assertDoesNotThrow(() -> future.get());
        assertTrue(future.isDone());
        assertTrue(commitOffsets.isEmpty());
    }

    @Test
    public void testCommitAsync() {
        subscriptionState = mock(SubscriptionState.class);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        TopicPartition tp = new TopicPartition("topic", 1);
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(0, Optional.of(1), "");
        Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(tp, offsetAndMetadata);

        CommitRequestManager commitRequestManager = create(true, 100);
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future = commitRequestManager.commitAsync(Optional.of(offsets));
        assertEquals(1, commitRequestManager.unsentOffsetCommitRequests().size());
        List<NetworkClientDelegate.FutureCompletionHandler> pollResults = assertPoll(1, commitRequestManager);
        pollResults.forEach(v -> v.onComplete(mockOffsetCommitResponse(
            "topic",
            1,
            (short) 1,
            Errors.NONE)));

        verify(subscriptionState, never()).allConsumed();
        verify(metadata).updateLastSeenEpochIfNewer(tp, 1);
        assertTrue(future.isDone());
        Map<TopicPartition, OffsetAndMetadata> commitOffsets = assertDoesNotThrow(() -> future.get());
        assertEquals(offsets, commitOffsets);
    }

    @Test
    public void testCommitAsyncWithEmptyOffsets() {
        subscriptionState = mock(SubscriptionState.class);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        TopicPartition tp = new TopicPartition("topic", 1);
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(0, Optional.of(1), "");
        Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(tp, offsetAndMetadata);
        doReturn(offsets).when(subscriptionState).allConsumed();

        CommitRequestManager commitRequestManager = create(true, 100);
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future = commitRequestManager.commitAsync(Optional.empty());
        assertEquals(1, commitRequestManager.unsentOffsetCommitRequests().size());
        List<NetworkClientDelegate.FutureCompletionHandler> pollResults = assertPoll(1, commitRequestManager);
        pollResults.forEach(v -> v.onComplete(mockOffsetCommitResponse(
            "topic",
            1,
            (short) 1,
            Errors.NONE)));

        verify(subscriptionState).allConsumed();
        verify(metadata).updateLastSeenEpochIfNewer(tp, 1);
        assertTrue(future.isDone());
        Map<TopicPartition, OffsetAndMetadata> commitOffsets = assertDoesNotThrow(() -> future.get());
        assertEquals(offsets, commitOffsets);
    }

    @Test
    public void testCommitAsyncWithEmptyAllConsumedOffsets() {
        subscriptionState = mock(SubscriptionState.class);
        doReturn(Map.of()).when(subscriptionState).allConsumed();

        CommitRequestManager commitRequestManager = create(true, 100);
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future = commitRequestManager.commitAsync(Optional.empty());

        verify(subscriptionState).allConsumed();
        assertTrue(future.isDone());
        Map<TopicPartition, OffsetAndMetadata> commitOffsets = assertDoesNotThrow(() -> future.get());
        assertTrue(commitOffsets.isEmpty());
    }

    // This is the case of the async auto commit sent on calls to assign (async commit that
    // should not be retried).
    @Test
    public void testAsyncAutocommitNotRetriedAfterException() {
        long commitInterval = retryBackoffMs * 2;
        CommitRequestManager commitRequestManager = create(true, commitInterval);
        TopicPartition tp = new TopicPartition("topic", 1);
        subscriptionState.assignFromUser(Collections.singleton(tp));
        subscriptionState.seek(tp, 100);
        time.sleep(commitInterval);
        commitRequestManager.updateAutoCommitTimer(time.milliseconds());
        List<NetworkClientDelegate.FutureCompletionHandler> futures = assertPoll(1, commitRequestManager);
        // Complete the autocommit request exceptionally. It should fail right away, without retry.
        futures.get(0).onComplete(mockOffsetCommitResponse(
            "topic",
            1,
            (short) 1,
            Errors.COORDINATOR_LOAD_IN_PROGRESS));

        // When polling again before the auto-commit interval no request should be generated
        // (making sure we wait for the backoff, to check that the failed request is not being
        // retried).
        time.sleep(retryBackoffMs);
        commitRequestManager.updateAutoCommitTimer(time.milliseconds());
        assertPoll(0, commitRequestManager);
        commitRequestManager.updateAutoCommitTimer(time.milliseconds());

        // Only when polling after the auto-commit interval, a new auto-commit request should be
        // generated.
        time.sleep(commitInterval);
        commitRequestManager.updateAutoCommitTimer(time.milliseconds());
        futures = assertPoll(1, commitRequestManager);
        assertEmptyPendingRequests(commitRequestManager);
        futures.get(0).onComplete(mockOffsetCommitResponse(
                "topic",
                1,
                (short) 1,
                Errors.NONE));
    }

    // This is the case of the sync commit triggered from an API call to commitSync or when the
    // consumer is being closed. It should be retried until it succeeds, fails, or timer expires.
    @ParameterizedTest
    @MethodSource("offsetCommitExceptionSupplier")
    public void testCommitSyncRetriedAfterExpectedRetriableException(Errors error) {
        CommitRequestManager commitRequestManager = create(false, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
            new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));
        long deadlineMs = time.milliseconds() + defaultApiTimeoutMs;
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commitResult = commitRequestManager.commitSync(Optional.of(offsets), deadlineMs);
        sendAndVerifyOffsetCommitRequestFailedAndMaybeRetried(commitRequestManager, error, commitResult);

        // We expect that request should have been retried on this sync commit.
        assertExceptionHandling(commitRequestManager, error, true);
    }

    private static Stream<Arguments> commitSyncExpectedExceptions() {
        return Stream.of(
            Arguments.of(Errors.UNKNOWN_MEMBER_ID, CommitFailedException.class),
            Arguments.of(Errors.OFFSET_METADATA_TOO_LARGE, Errors.OFFSET_METADATA_TOO_LARGE.exception().getClass()),
            Arguments.of(Errors.INVALID_COMMIT_OFFSET_SIZE, Errors.INVALID_COMMIT_OFFSET_SIZE.exception().getClass()),
            Arguments.of(Errors.GROUP_AUTHORIZATION_FAILED, Errors.GROUP_AUTHORIZATION_FAILED.exception().getClass()),
            Arguments.of(Errors.CORRUPT_MESSAGE, KafkaException.class),
            Arguments.of(Errors.UNKNOWN_SERVER_ERROR, KafkaException.class));
    }

    @Test
    public void testCommitSyncFailsWithCommitFailedExceptionIfUnknownMemberId() {
        CommitRequestManager commitRequestManager = create(false, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
            new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));
        long deadlineMs = time.milliseconds() + defaultApiTimeoutMs;
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commitResult = commitRequestManager.commitSync(Optional.of(offsets), deadlineMs);

        completeOffsetCommitRequestWithError(commitRequestManager, Errors.UNKNOWN_MEMBER_ID);
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        assertTrue(commitResult.isDone());
        assertFutureThrows(commitResult, CommitFailedException.class);
    }

    @Test
    public void testCommitSyncFailsWithCommitFailedExceptionOnStaleMemberEpoch() {
        CommitRequestManager commitRequestManager = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
            new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        // Send commit request expected to be retried on retriable errors
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commitResult = commitRequestManager.commitSync(
            Optional.of(offsets), time.milliseconds() + defaultApiTimeoutMs);
        completeOffsetCommitRequestWithError(commitRequestManager, Errors.STALE_MEMBER_EPOCH);
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());

        // Commit should fail with CommitFailedException
        assertTrue(commitResult.isDone());
        assertFutureThrows(commitResult, CommitFailedException.class);
    }

    /**
     * This is the case of the async auto commit request triggered on the interval. The request
     * internally fails with the fatal stale epoch error, and the expectation is that it just
     * resets the commit timer to the interval, to attempt again when the interval expires.
     */
    @Test
    public void testAutoCommitAsyncFailsWithStaleMemberEpochContinuesToCommitOnTheInterval() {
        CommitRequestManager commitRequestManager = create(true, 100);
        time.sleep(100);
        commitRequestManager.updateAutoCommitTimer(time.milliseconds());
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        TopicPartition t1p = new TopicPartition("topic1", 0);
        subscriptionState.assignFromUser(singleton(t1p));
        subscriptionState.seek(t1p, 10);

        // Async commit on the interval fails with fatal stale epoch and just resets the timer to
        // the interval
        commitRequestManager.maybeAutoCommitAsync();
        completeOffsetCommitRequestWithError(commitRequestManager, Errors.STALE_MEMBER_EPOCH);
        verify(commitRequestManager).resetAutoCommitTimer();

        // Async commit retried, only when the interval expires
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size(), "No request should be generated until the " +
            "interval expires");
        time.sleep(100);
        commitRequestManager.updateAutoCommitTimer(time.milliseconds());
        res = commitRequestManager.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());

    }

    @Test
    public void testCommitAsyncFailsWithRetriableOnCoordinatorDisconnected() {
        CommitRequestManager commitRequestManager = create(false, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
            new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        // Async commit that won't be retried.
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commitResult = commitRequestManager.commitAsync(Optional.of(offsets));

        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        NetworkClientDelegate.UnsentRequest req = res.unsentRequests.get(0);
        ClientResponse response = mockOffsetCommitResponseDisconnected("topic", 1, (short) 1, req);
        response.onComplete();

        // Commit should mark the coordinator unknown and fail with RetriableCommitFailedException.
        assertTrue(commitResult.isDone());
        assertFutureThrows(commitResult, RetriableCommitFailedException.class);
        assertCoordinatorDisconnectHandling();
    }

    @Test
    public void testAutocommitEnsureOnlyOneInflightRequest() {
        TopicPartition t1p = new TopicPartition("topic1", 0);
        subscriptionState.assignFromUser(singleton(t1p));
        subscriptionState.seek(t1p, 100);

        CommitRequestManager commitRequestManager = create(true, 100);
        time.sleep(100);
        commitRequestManager.updateAutoCommitTimer(time.milliseconds());
        List<NetworkClientDelegate.FutureCompletionHandler> futures = assertPoll(1, commitRequestManager);

        time.sleep(100);
        commitRequestManager.updateAutoCommitTimer(time.milliseconds());
        // We want to make sure we don't resend autocommit if the previous request has not been
        // completed, even if the interval expired
        assertPoll(0, commitRequestManager);
        assertEmptyPendingRequests(commitRequestManager);

        // complete the unsent request and re-poll
        futures.get(0).onComplete(buildOffsetCommitClientResponse(new OffsetCommitResponse(0, new HashMap<>())));
        assertPoll(1, commitRequestManager);
    }

    @Test
    public void testAutoCommitBeforeRevocationNotBlockedByAutoCommitOnIntervalInflightRequest() {
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        TopicPartition t1p = new TopicPartition("topic1", 0);
        subscriptionState.assignFromUser(singleton(t1p));
        subscriptionState.seek(t1p, 100);

        // Send auto-commit request on the interval.
        CommitRequestManager commitRequestManager = create(true, 100);
        time.sleep(100);
        commitRequestManager.updateAutoCommitTimer(time.milliseconds());
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        NetworkClientDelegate.FutureCompletionHandler autoCommitOnInterval =
            res.unsentRequests.get(0).handler();

        // Another auto-commit request should be sent if a revocation happens, even if an
        // auto-commit on the interval is in-flight.
        CompletableFuture<Void> autoCommitBeforeRevocation =
            commitRequestManager.maybeAutoCommitSyncBeforeRevocation(200);
        assertEquals(1, commitRequestManager.pendingRequests.unsentOffsetCommits.size());

        // Receive response for initial auto-commit on interval
        autoCommitOnInterval.onComplete(buildOffsetCommitClientResponse(new OffsetCommitResponse(0, new HashMap<>())));
        assertFalse(autoCommitBeforeRevocation.isDone(), "Auto-commit before revocation should " +
            "not complete until it receives a response");
    }

    @Test
    public void testAutocommitInterceptorsInvoked() {
        TopicPartition t1p = new TopicPartition("topic1", 0);
        subscriptionState.assignFromUser(singleton(t1p));
        subscriptionState.seek(t1p, 100);

        CommitRequestManager commitRequestManager = create(true, 100);
        time.sleep(100);
        commitRequestManager.updateAutoCommitTimer(time.milliseconds());
        List<NetworkClientDelegate.FutureCompletionHandler> futures = assertPoll(1, commitRequestManager);

        // complete the unsent request to trigger interceptor
        futures.get(0).onComplete(buildOffsetCommitClientResponse(new OffsetCommitResponse(0, new HashMap<>())));
        verify(offsetCommitCallbackInvoker).enqueueInterceptorInvocation(
            eq(Collections.singletonMap(t1p, new OffsetAndMetadata(100L)))
        );
    }

    @Test
    public void testAutocommitInterceptorsNotInvokedOnError() {
        TopicPartition t1p = new TopicPartition("topic1", 0);
        subscriptionState.assignFromUser(singleton(t1p));
        subscriptionState.seek(t1p, 100);

        CommitRequestManager commitRequestManager = create(true, 100);
        time.sleep(100);
        commitRequestManager.updateAutoCommitTimer(time.milliseconds());
        List<NetworkClientDelegate.FutureCompletionHandler> futures = assertPoll(1, commitRequestManager);

        // complete the unsent request to trigger interceptor
        futures.get(0).onComplete(buildOffsetCommitClientResponse(
            new OffsetCommitResponse(0, Collections.singletonMap(t1p, Errors.NETWORK_EXCEPTION)))
        );
        Mockito.verify(offsetCommitCallbackInvoker, never()).enqueueInterceptorInvocation(any());
    }

    @Test
    public void testAutoCommitEmptyOffsetsDoesNotGenerateRequest() {
        CommitRequestManager commitRequestManager = create(true, 100);
        time.sleep(100);
        commitRequestManager.updateAutoCommitTimer(time.milliseconds());
        commitRequestManager.maybeAutoCommitAsync();
        assertTrue(commitRequestManager.pendingRequests.unsentOffsetCommits.isEmpty());
        verify(commitRequestManager).resetAutoCommitTimer();
    }

    @Test
    public void testAutoCommitEmptyDoesNotLeaveInflightRequestFlagOn() {
        TopicPartition t1p = new TopicPartition("topic1", 0);
        subscriptionState.assignFromUser(singleton(t1p));
        CommitRequestManager commitRequestManager = create(true, 100);

        // Auto-commit of empty offsets
        time.sleep(100);
        commitRequestManager.updateAutoCommitTimer(time.milliseconds());
        commitRequestManager.maybeAutoCommitAsync();

        // Next auto-commit consumed offsets (not empty). Should generate a request, ensuring
        // that the previous auto-commit of empty did not leave the inflight request flag on
        subscriptionState.seek(t1p, 100);
        time.sleep(100);
        commitRequestManager.updateAutoCommitTimer(time.milliseconds());
        commitRequestManager.maybeAutoCommitAsync();
        assertEquals(1, commitRequestManager.pendingRequests.unsentOffsetCommits.size());

        verify(commitRequestManager, times(2)).resetAutoCommitTimer();
    }

    @Test
    public void testAutoCommitOnIntervalSkippedIfPreviousOneInFlight() {
        TopicPartition t1p = new TopicPartition("topic1", 0);
        subscriptionState.assignFromUser(singleton(t1p));
        subscriptionState.seek(t1p, 100);

        CommitRequestManager commitRequestManager = create(true, 100);

        // Send auto-commit request that will remain in-flight without a response
        time.sleep(100);
        commitRequestManager.updateAutoCommitTimer(time.milliseconds());
        commitRequestManager.maybeAutoCommitAsync();
        List<NetworkClientDelegate.FutureCompletionHandler> futures = assertPoll(1, commitRequestManager);
        assertEquals(1, futures.size());
        NetworkClientDelegate.FutureCompletionHandler inflightCommitResult = futures.get(0);
        verify(commitRequestManager, times(1)).resetAutoCommitTimer();
        clearInvocations(commitRequestManager);

        // After next interval expires, no new auto-commit request should be sent. The interval
        // should not be reset either, to ensure that the next auto-commit is sent out as soon as
        // the inflight receives a response.
        time.sleep(100);
        commitRequestManager.updateAutoCommitTimer(time.milliseconds());
        commitRequestManager.maybeAutoCommitAsync();
        assertPoll(0, commitRequestManager);
        verify(commitRequestManager, never()).resetAutoCommitTimer();

        // When a response for the inflight is received, a next auto-commit should be sent when
        // polling the manager.
        inflightCommitResult.onComplete(
            mockOffsetCommitResponse(t1p.topic(), t1p.partition(), (short) 1, Errors.NONE));
        assertPoll(1, commitRequestManager);
    }

    @Test
    public void testOffsetFetchRequestEnsureDuplicatedRequestSucceed() {
        CommitRequestManager commitRequestManager = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(new TopicPartition("t1", 0));
        List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> futures = sendAndVerifyDuplicatedOffsetFetchRequests(
                commitRequestManager,
                partitions,
                2,
                Errors.NONE);
        futures.forEach(f -> {
            assertTrue(f.isDone());
            assertFalse(f.isCompletedExceptionally());
        });
        // expecting the buffers to be emptied after being completed successfully
        commitRequestManager.poll(0);
        assertEmptyPendingRequests(commitRequestManager);
    }

    @ParameterizedTest
    @MethodSource("offsetFetchExceptionSupplier")
    public void testOffsetFetchRequestErroredRequests(final Errors error) {
        CommitRequestManager commitRequestManager = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(new TopicPartition("t1", 0));
        List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> futures = sendAndVerifyDuplicatedOffsetFetchRequests(
            commitRequestManager,
            partitions,
            1,
            error);
        // we only want to make sure to purge the outbound buffer for non-retriables, so retriable will be re-queued.
        if (error.exception() instanceof RetriableException)
            testRetriable(commitRequestManager, futures, error);
        else {
            testNonRetriable(futures);
            assertEmptyPendingRequests(commitRequestManager);
        }
    }

    @ParameterizedTest
    @MethodSource("offsetFetchExceptionSupplier")
    public void testOffsetFetchRequestTimeoutRequests(final Errors error) {
        CommitRequestManager commitRequestManager = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(new TopicPartition("t1", 0));
        List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> futures = sendAndVerifyDuplicatedOffsetFetchRequests(
                commitRequestManager,
                partitions,
                1,
                error);

        if (error.exception() instanceof RetriableException) {
            futures.forEach(f -> assertFalse(f.isDone()));

            // Insert a long enough sleep to force a timeout of the operation. Invoke poll() again so that each
            // OffsetFetchRequestState is evaluated via isExpired().
            time.sleep(defaultApiTimeoutMs);
            assertFalse(commitRequestManager.pendingRequests.unsentOffsetFetches.isEmpty());
            NetworkClientDelegate.PollResult poll = commitRequestManager.poll(time.milliseconds());
            mimicResponse(error, poll);
            futures.forEach(f -> assertFutureThrows(f, TimeoutException.class));
            assertTrue(commitRequestManager.pendingRequests.unsentOffsetFetches.isEmpty());
        } else {
            futures.forEach(f -> assertFutureThrows(f, KafkaException.class));
            assertEmptyPendingRequests(commitRequestManager);
        }
    }

    @Test
    public void testSuccessfulOffsetFetch() {
        CommitRequestManager commitManager = create(false, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        long deadlineMs = time.milliseconds() + defaultApiTimeoutMs;
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> fetchResult =
            commitManager.fetchOffsets(Collections.singleton(new TopicPartition("test", 0)),
                deadlineMs);

        // Send fetch request
        NetworkClientDelegate.PollResult result = commitManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());
        assertEquals(1, commitManager.pendingRequests.inflightOffsetFetches.size());
        assertFalse(fetchResult.isDone());

        // Complete request with a response
        TopicPartition tp = new TopicPartition("topic1", 0);
        long expectedOffset = 100;
        NetworkClientDelegate.UnsentRequest req = result.unsentRequests.get(0);
        Map<TopicPartition, OffsetFetchResponse.PartitionData> topicPartitionData =
            Collections.singletonMap(
                tp,
                new OffsetFetchResponse.PartitionData(expectedOffset, Optional.of(1), "", Errors.NONE));
        req.handler().onComplete(buildOffsetFetchClientResponse(req, topicPartitionData, Errors.NONE, false));

        // Validate request future completes with the response received
        assertTrue(fetchResult.isDone());
        assertFalse(fetchResult.isCompletedExceptionally());
        Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata = null;
        try {
            offsetsAndMetadata = fetchResult.get();
        } catch (InterruptedException | ExecutionException e) {
            fail(e);
        }
        assertNotNull(offsetsAndMetadata);
        assertEquals(1, offsetsAndMetadata.size());
        assertTrue(offsetsAndMetadata.containsKey(tp));
        assertEquals(expectedOffset, offsetsAndMetadata.get(tp).offset());
        assertEquals(0, commitManager.pendingRequests.inflightOffsetFetches.size(), "Inflight " +
            "request should be removed from the queue when a response is received.");
    }

    @ParameterizedTest
    @MethodSource("offsetFetchRetriableCoordinatorErrors")
    public void testOffsetFetchMarksCoordinatorUnknownOnRetriableCoordinatorErrors(Errors error,
                                                                                   boolean shouldRediscoverCoordinator) {
        CommitRequestManager commitRequestManager = create(false, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(new TopicPartition("t1", 0));

        long deadlineMs = time.milliseconds() + defaultApiTimeoutMs;
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> result = commitRequestManager.fetchOffsets(partitions, deadlineMs);

        completeOffsetFetchRequestWithError(commitRequestManager, partitions, error);

        // Request not completed just yet
        assertFalse(result.isDone());
        if (shouldRediscoverCoordinator) {
            assertCoordinatorDisconnectOnCoordinatorError();
        }

        // Request should be retried with backoff.
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        time.sleep(retryBackoffMs);
        res = commitRequestManager.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
    }

    @Test
    public void testOffsetFetchMarksCoordinatorUnknownOnCoordinatorDisconnectedAndRetries() {
        CommitRequestManager commitRequestManager = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(new TopicPartition("t1", 0));

        long deadlineMs = time.milliseconds() + defaultApiTimeoutMs;
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> result = commitRequestManager.fetchOffsets(partitions, deadlineMs);

        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        NetworkClientDelegate.UnsentRequest request = res.unsentRequests.get(0);
        ClientResponse response = buildOffsetFetchClientResponseDisconnected(request);
        response.onComplete();

        // Request not completed just yet, but should have marked the coordinator unknown
        assertFalse(result.isDone());
        assertCoordinatorDisconnectHandling();

        time.sleep(retryBackoffMs);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        res = commitRequestManager.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        assertEquals(1, commitRequestManager.pendingRequests.inflightOffsetFetches.size());
    }

    @ParameterizedTest
    @MethodSource("offsetCommitExceptionSupplier")
    public void testOffsetCommitRequestErroredRequestsNotRetriedForAsyncCommit(final Errors error) {
        CommitRequestManager commitRequestManager = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        // Send async commit (not expected to be retried).
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commitResult = commitRequestManager.commitAsync(Optional.of(offsets));
        completeOffsetCommitRequestWithError(commitRequestManager, error);
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        assertTrue(commitResult.isDone());
        assertTrue(commitResult.isCompletedExceptionally());
        if (error.exception() instanceof RetriableException) {
            assertFutureThrows(commitResult, RetriableCommitFailedException.class);
        }

        // We expect that the request should not have been retried on this async commit.
        assertExceptionHandling(commitRequestManager, error, false);
    }

    @Test
    public void testOffsetCommitSyncTimeoutNotReturnedOnPollAndFails() {
        CommitRequestManager commitRequestManager = create(false, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
            new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        // Send sync offset commit request that fails with retriable error.
        long deadlineMs = time.milliseconds() + retryBackoffMs * 2;
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commitResult = commitRequestManager.commitSync(Optional.of(offsets), deadlineMs);
        completeOffsetCommitRequestWithError(commitRequestManager, Errors.REQUEST_TIMED_OUT);

        // Request retried after backoff, and fails with retriable again. Should not complete yet
        // given that the request timeout hasn't expired.
        time.sleep(retryBackoffMs);
        completeOffsetCommitRequestWithError(commitRequestManager, Errors.REQUEST_TIMED_OUT);
        assertFalse(commitResult.isDone());

        // Sleep to expire the request timeout. Request should fail on the next poll.
        time.sleep(retryBackoffMs);
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        assertTrue(commitResult.isDone());
        assertTrue(commitResult.isCompletedExceptionally());
    }

    /**
     * Sync commit requests that fail with an expected retriable error should be retried
     * while there is time. When time expires, they should fail with a TimeoutException.
     */
    @ParameterizedTest
    @MethodSource("offsetCommitExceptionSupplier")
    public void testOffsetCommitSyncFailedWithRetriableThrowsTimeoutWhenRetryTimeExpires(final Errors error) {
        CommitRequestManager commitRequestManager = create(false, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
            new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        // Send offset commit request that fails with retriable error.
        long deadlineMs = time.milliseconds() + retryBackoffMs * 2;
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commitResult = commitRequestManager.commitSync(Optional.of(offsets), deadlineMs);
        completeOffsetCommitRequestWithError(commitRequestManager, error);

        // Sleep to expire the request timeout. Request should fail on the next poll with a
        // TimeoutException.
        time.sleep(deadlineMs);
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        assertTrue(commitResult.isDone());

        if (error.exception() instanceof RetriableException)
            assertFutureThrows(commitResult, TimeoutException.class);
        else
            assertFutureThrows(commitResult, KafkaException.class);
    }

    /**
     * Async commit requests that fail with a retriable error are not retried, and they should fail
     * with a RetriableCommitException.
     */
    @Test
    public void testOffsetCommitAsyncFailedWithRetriableThrowsRetriableCommitException() {
        CommitRequestManager commitRequestManager = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        // Send async commit request that fails with retriable error (not expected to be retried).
        Errors retriableError = Errors.COORDINATOR_NOT_AVAILABLE;
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commitResult = commitRequestManager.commitAsync(Optional.of(offsets));
        completeOffsetCommitRequestWithError(commitRequestManager, retriableError);
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        assertTrue(commitResult.isDone());
        assertTrue(commitResult.isCompletedExceptionally());

        // We expect that the request should not have been retried on this async commit.
        assertExceptionHandling(commitRequestManager, retriableError, false);

        // Request should complete with a RetriableCommitException
        assertFutureThrows(commitResult, RetriableCommitFailedException.class);
    }

    @ParameterizedTest
    @MethodSource("offsetCommitExceptionSupplier")
    public void testOffsetCommitSingleFailedAttemptPerRequestWhenPartitionErrors(final Errors error) {
        CommitRequestManager commitRequestManager = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("t1", 0), new OffsetAndMetadata(1));
        offsets.put(new TopicPartition("t1", 1), new OffsetAndMetadata(2));
        offsets.put(new TopicPartition("t1", 2), new OffsetAndMetadata(3));

        commitRequestManager.commitSync(Optional.of(offsets), time.milliseconds() + defaultApiTimeoutMs);
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());

        res.unsentRequests.get(0).handler().onComplete(mockOffsetCommitResponse("topic", (short) 1, error, offsets.size()));
        CommitRequestManager.OffsetCommitRequestState commitRequest = commitRequestManager.pendingRequests.unsentOffsetCommits.peek();
        if (error.exception() instanceof RetriableException) {
            assertNotNull(commitRequest);
            assertEquals(1, commitRequest.numAttempts, "Only one failed attempt should be registered, even if the response contains multiple partition errors");
            time.sleep(retryBackoffMs);
            res = commitRequestManager.poll(time.milliseconds());
            res.unsentRequests.get(0).handler().onComplete(mockOffsetCommitResponse("topic", (short) 1, error, offsets.size()));
            assertEquals(2, commitRequest.numAttempts, "Only one failed attempt should be registered, even if the response contains multiple partition errors");
        } else assertNull(commitRequest);
    }

    @Test
    public void testEnsureBackoffRetryOnOffsetCommitRequestTimeout() {
        CommitRequestManager commitRequestManager = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        long deadlineMs = time.milliseconds() + defaultApiTimeoutMs;
        commitRequestManager.commitSync(Optional.of(offsets), deadlineMs);
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        res.unsentRequests.get(0).handler().onFailure(time.milliseconds(), new TimeoutException());

        assertTrue(commitRequestManager.pendingRequests.hasUnsentRequests());
        assertEquals(1, commitRequestManager.unsentOffsetCommitRequests().size());
        assertRetryBackOff(commitRequestManager, retryBackoffMs);
    }

    private void assertCoordinatorDisconnectHandling() {
        verify(coordinatorRequestManager).handleCoordinatorDisconnect(any(), anyLong());
    }

    private void assertCoordinatorDisconnectOnCoordinatorError() {
        verify(coordinatorRequestManager).markCoordinatorUnknown(any(), anyLong());
    }

    private void assertExceptionHandling(CommitRequestManager commitRequestManager, Errors errors,
                                         boolean requestShouldBeRetried) {
        long remainBackoffMs;
        if (requestShouldBeRetried) {
            remainBackoffMs = retryBackoffMs;
        } else {
            remainBackoffMs = Long.MAX_VALUE;
        }
        switch (errors) {
            case NOT_COORDINATOR:
            case COORDINATOR_NOT_AVAILABLE:
            case REQUEST_TIMED_OUT:
                verify(coordinatorRequestManager).markCoordinatorUnknown(any(), anyLong());
                assertPollDoesNotReturn(commitRequestManager, remainBackoffMs);
                break;
            case UNKNOWN_TOPIC_OR_PARTITION:
            case COORDINATOR_LOAD_IN_PROGRESS:
                if (requestShouldBeRetried) {
                    assertRetryBackOff(commitRequestManager, remainBackoffMs);
                }
                break;
            case GROUP_AUTHORIZATION_FAILED:
                // failed
                break;
            case TOPIC_AUTHORIZATION_FAILED:
            case OFFSET_METADATA_TOO_LARGE:
            case INVALID_COMMIT_OFFSET_SIZE:
                assertPollDoesNotReturn(commitRequestManager, Long.MAX_VALUE);
                break;
            default:
                if (errors.exception() instanceof RetriableException && requestShouldBeRetried) {
                    assertRetryBackOff(commitRequestManager, remainBackoffMs);
                } else {
                    assertPollDoesNotReturn(commitRequestManager, Long.MAX_VALUE);
                }
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

    // This should be the case where the OffsetFetch fails with invalid member epoch and the
    // member already has a new epoch (ex. when member just joined the group or got a new
    // epoch after a reconciliation). The request should just be retried with the new epoch
    // and succeed.
    @Test
    public void testSyncOffsetFetchFailsWithStaleEpochAndRetriesWithNewEpoch() {
        CommitRequestManager commitRequestManager = create(false, 100);
        Set<TopicPartition> partitions = Collections.singleton(new TopicPartition("t1", 0));
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        // Send request that is expected to fail with invalid epoch.
        long deadlineMs = time.milliseconds() + defaultApiTimeoutMs;
        commitRequestManager.fetchOffsets(partitions, deadlineMs);

        // Mock member has new a valid epoch.
        int newEpoch = 8;
        String memberId = "member1";
        commitRequestManager.onMemberEpochUpdated(Optional.of(newEpoch), memberId);

        // Receive error when member already has a newer member epoch. Request should be retried.
        completeOffsetFetchRequestWithError(commitRequestManager, partitions, Errors.STALE_MEMBER_EPOCH);

        // Check that the request that failed was removed from the inflight requests buffer.
        assertEquals(0, commitRequestManager.pendingRequests.inflightOffsetFetches.size());
        assertEquals(1, commitRequestManager.pendingRequests.unsentOffsetFetches.size());

        // Request should be retried with backoff.
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        time.sleep(retryBackoffMs);
        res = commitRequestManager.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());

        // The retried request should include the latest member ID and epoch.
        OffsetFetchRequestData reqData = (OffsetFetchRequestData) res.unsentRequests.get(0).requestBuilder().build().data();
        assertEquals(1, reqData.groups().size());
        assertEquals(newEpoch, reqData.groups().get(0).memberEpoch());
        assertEquals(memberId, reqData.groups().get(0).memberId());
    }

    // This should be the case of an OffsetFetch that fails because the member is not in the
    // group anymore (left the group, failed with fatal error, or got fenced). In that case the
    // request should fail without retry.
    @Test
    public void testSyncOffsetFetchFailsWithStaleEpochAndNotRetriedIfMemberNotInGroupAnymore() {
        CommitRequestManager commitRequestManager = create(false, 100);
        Set<TopicPartition> partitions = Collections.singleton(new TopicPartition("t1", 0));
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        // Send request that is expected to fail with invalid epoch.
        long deadlineMs = time.milliseconds() + defaultApiTimeoutMs;
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> requestResult =
            commitRequestManager.fetchOffsets(partitions, deadlineMs);

        // Mock member not having a valid epoch anymore (left/failed/fenced).
        commitRequestManager.onMemberEpochUpdated(Optional.empty(), Uuid.randomUuid().toString());

        // Receive error when member is not in the group anymore. Request should fail.
        completeOffsetFetchRequestWithError(commitRequestManager, partitions, Errors.STALE_MEMBER_EPOCH);

        assertTrue(requestResult.isDone());
        assertTrue(requestResult.isCompletedExceptionally());

        // No new request should be generated on the next poll.
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
    }

    // This should be the case where the OffsetCommit expected to be retried on
    // STALE_MEMBER_EPOCH (ex. triggered from the reconciliation process), fails with invalid
    // member epoch. The expectation is that if the member already has a new epoch, the request
    // should be retried with the new epoch.
    @ParameterizedTest
    @MethodSource("offsetCommitExceptionSupplier")
    public void testAutoCommitSyncBeforeRevocationRetriesOnRetriableAndStaleEpoch(Errors error) {
        // Enable auto-commit but with very long interval to avoid triggering auto-commits on the
        // interval and just test the auto-commits triggered before revocation
        CommitRequestManager commitRequestManager = create(true, Integer.MAX_VALUE);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        TopicPartition tp = new TopicPartition("topic", 1);
        subscriptionState.assignFromUser(singleton(tp));
        subscriptionState.seek(tp, 5);
        long deadlineMs = time.milliseconds() + retryBackoffMs * 2;

        // Send commit request expected to be retried on STALE_MEMBER_EPOCH error while it does not expire
        commitRequestManager.maybeAutoCommitSyncBeforeRevocation(deadlineMs);

        int newEpoch = 8;
        String memberId = "member1";
        if (error == Errors.STALE_MEMBER_EPOCH) {
            // Mock member has new a valid epoch
            commitRequestManager.onMemberEpochUpdated(Optional.of(newEpoch), memberId);
        }

        completeOffsetCommitRequestWithError(commitRequestManager, error);

        if ((error.exception() instanceof RetriableException || error == Errors.STALE_MEMBER_EPOCH) && error != Errors.UNKNOWN_TOPIC_OR_PARTITION) {
            assertEquals(1, commitRequestManager.pendingRequests.unsentOffsetCommits.size(),
                "Request to be retried should be added to the outbound queue");

            // Request should be retried with backoff
            NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
            assertEquals(0, res.unsentRequests.size());
            time.sleep(retryBackoffMs);
            res = commitRequestManager.poll(time.milliseconds());
            assertEquals(1, res.unsentRequests.size());
            if (error == Errors.STALE_MEMBER_EPOCH) {
                // The retried request should include the latest member ID and epoch
                OffsetCommitRequestData reqData = (OffsetCommitRequestData) res.unsentRequests.get(0).requestBuilder().build().data();
                assertEquals(newEpoch, reqData.generationIdOrMemberEpoch());
                assertEquals(memberId, reqData.memberId());
            }
        } else {
            assertEquals(0, commitRequestManager.pendingRequests.unsentOffsetCommits.size(),
                "Non-retriable failed request should be removed from the outbound queue");

            // Request should not be retried, even after the backoff expires
            NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
            assertEquals(0, res.unsentRequests.size());
            time.sleep(retryBackoffMs);
            res = commitRequestManager.poll(time.milliseconds());
            assertEquals(0, res.unsentRequests.size());
        }
    }

    @Test
    public void testLastEpochSentOnCommit() {
        // Enable auto-commit but with very long interval to avoid triggering auto-commits on the
        // interval and just test the auto-commits triggered before revocation
        CommitRequestManager commitRequestManager = create(true, Integer.MAX_VALUE);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        TopicPartition tp = new TopicPartition("topic", 1);
        subscriptionState.assignFromUser(singleton(tp));
        subscriptionState.seek(tp, 100);

        // Send auto commit to revoke partitions, expected to be retried on STALE_MEMBER_EPOCH
        // with the latest epochs received (using long deadline to avoid expiring the request
        // while retrying with the new epochs)
        commitRequestManager.maybeAutoCommitSyncBeforeRevocation(Long.MAX_VALUE);

        int initialEpoch = 1;
        String memberId = "member1";
        commitRequestManager.onMemberEpochUpdated(Optional.of(initialEpoch), memberId);

        // Send request with epoch 1
        completeOffsetCommitRequestWithError(commitRequestManager, Errors.STALE_MEMBER_EPOCH);
        assertEquals(initialEpoch, commitRequestManager.lastEpochSentOnCommit().orElse(null));

        // Receive new epoch. Last epoch sent should change only when sending out the next request
        commitRequestManager.onMemberEpochUpdated(Optional.of(initialEpoch + 1), memberId);
        assertEquals(initialEpoch, commitRequestManager.lastEpochSentOnCommit().get());
        time.sleep(retryBackoffMs);
        completeOffsetCommitRequestWithError(commitRequestManager, Errors.STALE_MEMBER_EPOCH);
        assertEquals(initialEpoch + 1, commitRequestManager.lastEpochSentOnCommit().orElse(null));

        // Receive empty epochs
        commitRequestManager.onMemberEpochUpdated(Optional.empty(), memberId);
        time.sleep(retryBackoffMs * 2);
        completeOffsetCommitRequestWithError(commitRequestManager, Errors.STALE_MEMBER_EPOCH);
        assertFalse(commitRequestManager.lastEpochSentOnCommit().isPresent());
    }

    @Test
    public void testEnsureCommitSensorRecordsMetric() {
        CommitRequestManager commitRequestManager = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        commitOffsetWithAssertedLatency(commitRequestManager, 100);
        commitOffsetWithAssertedLatency(commitRequestManager, 101);

        assertEquals(100.5, getMetric("commit-latency-avg").metricValue()); // 201 / 2
        assertEquals(101.0, getMetric("commit-latency-max").metricValue()); // Math.max(101, 100)
        assertEquals(0.066, (double) getMetric("commit-rate").metricValue(), 0.001);
        assertEquals(2.0, getMetric("commit-total").metricValue());
    }

    private void commitOffsetWithAssertedLatency(CommitRequestManager commitRequestManager, long latencyMs) {
        final String topic = "topic";
        final int partition = 1;
        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
                new TopicPartition(topic, partition),
                new OffsetAndMetadata(0));

        long commitCreationTimeMs = time.milliseconds();
        commitRequestManager.commitAsync(Optional.of(offsets));

        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());

        time.sleep(latencyMs);
        long commitReceivedTimeMs = time.milliseconds();
        res.unsentRequests.get(0).future().complete(mockOffsetCommitResponse(
                topic,
                partition,
                (short) 1,
                commitCreationTimeMs,
                commitReceivedTimeMs,
                Errors.NONE));
    }

    private void completeOffsetFetchRequestWithError(CommitRequestManager commitRequestManager,
                                                     Set<TopicPartition> partitions,
                                                     Errors error) {
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        res.unsentRequests.get(0).future().complete(buildOffsetFetchClientResponse(res.unsentRequests.get(0), partitions, error));
    }

    private void completeOffsetCommitRequestWithError(CommitRequestManager commitRequestManager,
                                                      Errors error) {
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        res.unsentRequests.get(0).future().complete(mockOffsetCommitResponse("topic", 1, (short) 1, error));
    }

    private void testRetriable(final CommitRequestManager commitRequestManager,
                               final List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> futures,
                               final Errors error
    ) {
        futures.forEach(f -> assertFalse(f.isDone()));
        assertEquals(1, commitRequestManager.pendingRequests.unsentOffsetFetches.get(0).numAttempts,
                "Only one failed attempt should be registered, even if the response contains multiple partition errors");

        // The manager should backoff before retry
        time.sleep(retryBackoffMs);
        NetworkClientDelegate.PollResult poll = commitRequestManager.poll(time.milliseconds());
        assertEquals(1, poll.unsentRequests.size());
        futures.forEach(f -> assertFalse(f.isDone()));
        mimicResponse(error, poll);
        assertEquals(2, commitRequestManager.pendingRequests.unsentOffsetFetches.get(0).numAttempts,
                "Only one failed attempt should be registered, even if the response contains multiple partition errors");

        // Sleep util timeout
        time.sleep(defaultApiTimeoutMs);
        poll = commitRequestManager.poll(time.milliseconds());
        assertEquals(1, poll.unsentRequests.size());
        mimicResponse(error, poll);
        futures.forEach(f -> {
            assertTrue(f.isCompletedExceptionally());
            assertFutureThrows(f, TimeoutException.class);
        });
    }

    private void mimicResponse(Errors error, NetworkClientDelegate.PollResult poll) {
        poll.unsentRequests.get(0).handler().onComplete(buildOffsetFetchClientResponse(poll.unsentRequests.get(0), new HashSet<>(), error));
    }

    private void testNonRetriable(final List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> futures) {
        futures.forEach(f -> assertTrue(f.isCompletedExceptionally()));
    }

    /**
     * @return {@link Errors} that could be received in {@link ApiKeys#OFFSET_COMMIT} responses.
     */
    private static Stream<Arguments> offsetCommitExceptionSupplier() {
        return Stream.of(
            Arguments.of(Errors.NOT_COORDINATOR),
            Arguments.of(Errors.COORDINATOR_LOAD_IN_PROGRESS),
            Arguments.of(Errors.UNKNOWN_SERVER_ERROR),
            Arguments.of(Errors.GROUP_AUTHORIZATION_FAILED),
            Arguments.of(Errors.OFFSET_METADATA_TOO_LARGE),
            Arguments.of(Errors.INVALID_COMMIT_OFFSET_SIZE),
            Arguments.of(Errors.UNKNOWN_TOPIC_OR_PARTITION),
            Arguments.of(Errors.COORDINATOR_NOT_AVAILABLE),
            Arguments.of(Errors.REQUEST_TIMED_OUT),
            Arguments.of(Errors.TOPIC_AUTHORIZATION_FAILED),
            Arguments.of(Errors.STALE_MEMBER_EPOCH),
            Arguments.of(Errors.UNKNOWN_MEMBER_ID));
    }

    /**
     * @return {@link Errors} that could be received in {@link ApiKeys#OFFSET_FETCH} responses.
     */
    private static Stream<Arguments> offsetFetchExceptionSupplier() {
        return Stream.of(
            Arguments.of(Errors.NOT_COORDINATOR),
            Arguments.of(Errors.COORDINATOR_LOAD_IN_PROGRESS),
            Arguments.of(Errors.UNKNOWN_SERVER_ERROR),
            Arguments.of(Errors.GROUP_AUTHORIZATION_FAILED),
            Arguments.of(Errors.OFFSET_METADATA_TOO_LARGE),
            Arguments.of(Errors.INVALID_COMMIT_OFFSET_SIZE),
            Arguments.of(Errors.UNKNOWN_TOPIC_OR_PARTITION),
            Arguments.of(Errors.COORDINATOR_NOT_AVAILABLE),
            Arguments.of(Errors.REQUEST_TIMED_OUT),
            Arguments.of(Errors.TOPIC_AUTHORIZATION_FAILED),
            Arguments.of(Errors.UNKNOWN_MEMBER_ID),
            // Adding STALE_MEMBER_EPOCH as non-retriable here because it is only retried if a new
            // member epoch is received. Tested separately.
            Arguments.of(Errors.STALE_MEMBER_EPOCH),
            Arguments.of(Errors.UNSTABLE_OFFSET_COMMIT));
    }

    /**
     * @return Retriable coordinator errors and a boolean indicating if it is expected to mark
     * the coordinator unknown when the error occurs.
     */
    private static Stream<Arguments> offsetFetchRetriableCoordinatorErrors() {
        return Stream.of(
            Arguments.of(Errors.NOT_COORDINATOR, true),
            Arguments.of(Errors.COORDINATOR_NOT_AVAILABLE, true),
            Arguments.of(Errors.COORDINATOR_LOAD_IN_PROGRESS, false));
    }

    @ParameterizedTest
    @MethodSource("partitionDataErrorSupplier")
    public void testOffsetFetchRequestPartitionDataError(final Errors error, final boolean isRetriable) {
        CommitRequestManager commitRequestManager = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        Set<TopicPartition> partitions = new HashSet<>();
        TopicPartition tp1 = new TopicPartition("t1", 2);
        TopicPartition tp2 = new TopicPartition("t2", 3);
        TopicPartition tp3 = new TopicPartition("t3", 4);
        partitions.add(tp1);
        partitions.add(tp2);
        partitions.add(tp3);
        long deadlineMs = time.milliseconds() + defaultApiTimeoutMs;
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future =
                commitRequestManager.fetchOffsets(partitions, deadlineMs);

        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());

        // Setting 1 partition with error
        HashMap<TopicPartition, OffsetFetchResponse.PartitionData> topicPartitionData = new HashMap<>();
        topicPartitionData.put(tp1, new OffsetFetchResponse.PartitionData(100L, Optional.of(1), "metadata", error));
        topicPartitionData.put(tp2, new OffsetFetchResponse.PartitionData(100L, Optional.of(1), "metadata", Errors.NONE));
        topicPartitionData.put(tp3, new OffsetFetchResponse.PartitionData(100L, Optional.of(1), "metadata", error));

        res.unsentRequests.get(0).handler().onComplete(buildOffsetFetchClientResponse(
                res.unsentRequests.get(0),
                topicPartitionData,
                Errors.NONE,
                false));
        if (isRetriable)
            testRetriable(commitRequestManager, Collections.singletonList(future), error);
        else
            testNonRetriable(Collections.singletonList(future));
    }

    @Test
    public void testSignalClose() {
        CommitRequestManager commitRequestManager = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        commitRequestManager.commitAsync(Optional.of(offsets));
        commitRequestManager.signalClose();
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        OffsetCommitRequestData data = (OffsetCommitRequestData) res.unsentRequests.get(0).requestBuilder().build().data();
        assertEquals("topic", data.topics().get(0).name());
    }

    private static void assertEmptyPendingRequests(CommitRequestManager commitRequestManager) {
        assertTrue(commitRequestManager.pendingRequests.inflightOffsetFetches.isEmpty());
        assertTrue(commitRequestManager.pendingRequests.unsentOffsetFetches.isEmpty());
        assertTrue(commitRequestManager.pendingRequests.unsentOffsetCommits.isEmpty());
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
            final CommitRequestManager commitRequestManager,
            final Set<TopicPartition> partitions,
            int numRequest,
            final Errors error) {
        List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> futures = new ArrayList<>();
        long deadlineMs = time.milliseconds() + defaultApiTimeoutMs;
        for (int i = 0; i < numRequest; i++) {
            futures.add(commitRequestManager.fetchOffsets(partitions, deadlineMs));
        }

        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        res.unsentRequests.get(0).handler().onComplete(buildOffsetFetchClientResponse(res.unsentRequests.get(0),
            partitions, error));
        res = commitRequestManager.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        return futures;
    }

    private void sendAndVerifyOffsetCommitRequestFailedAndMaybeRetried(
        final CommitRequestManager commitRequestManager,
        final Errors error,
        final CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commitResult) {
        completeOffsetCommitRequestWithError(commitRequestManager, error);
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        if (error.exception() instanceof RetriableException) {
            // Commit should not complete if the timer is still valid and the error is retriable.
            assertFalse(commitResult.isDone());
        } else {
            // Commit should fail if the timer expired or the error is not retriable.
            assertTrue(commitResult.isDone());
            assertTrue(commitResult.isCompletedExceptionally());
        }
    }

    private List<NetworkClientDelegate.FutureCompletionHandler> assertPoll(
        final int numRes,
        final CommitRequestManager manager) {
        return assertPoll(true, numRes, manager);
    }

    private List<NetworkClientDelegate.FutureCompletionHandler> assertPoll(
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

        return res.unsentRequests.stream().map(NetworkClientDelegate.UnsentRequest::handler).collect(Collectors.toList());
    }

    private CommitRequestManager create(final boolean autoCommitEnabled, final long autoCommitInterval) {
        props.setProperty(AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitInterval));
        props.setProperty(ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommitEnabled));

        if (autoCommitEnabled)
            props.setProperty(GROUP_ID_CONFIG, TestUtils.randomString(10));

        return spy(new CommitRequestManager(
                this.time,
                this.logContext,
                this.subscriptionState,
                new ConsumerConfig(props),
                this.coordinatorRequestManager,
                this.offsetCommitCallbackInvoker,
                DEFAULT_GROUP_ID,
                Optional.of(DEFAULT_GROUP_INSTANCE_ID),
                retryBackoffMs,
                retryBackoffMaxMs,
                OptionalDouble.of(0),
                metrics,
                metadata));
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
        return buildOffsetFetchClientResponse(request, topicPartitionData, error, false);
    }

    private ClientResponse buildOffsetFetchClientResponseDisconnected(
        final NetworkClientDelegate.UnsentRequest request) {
        return buildOffsetFetchClientResponse(request, Collections.emptyMap(), Errors.NONE, true);
    }

    private ClientResponse buildOffsetCommitClientResponse(final OffsetCommitResponse commitResponse) {
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


    private ClientResponse mockOffsetCommitResponse(String topic,
                                                   int partition,
                                                   short apiKeyVersion,
                                                   Errors error) {
        return mockOffsetCommitResponse(topic, partition, apiKeyVersion, time.milliseconds(), time.milliseconds(), error);
    }

    private ClientResponse mockOffsetCommitResponse(String topic,
                                                   int partition,
                                                   short apiKeyVersion,
                                                   long createdTimeMs,
                                                   long receivedTimeMs,
                                                   Errors error) {
        OffsetCommitResponseData responseData = new OffsetCommitResponseData()
            .setTopics(Collections.singletonList(
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
                createdTimeMs,
                receivedTimeMs,
                false,
                null,
                null,
                new OffsetCommitResponse(responseData)
        );
    }

    private ClientResponse mockOffsetCommitResponse(String topic,
                                                    short apiKeyVersion,
                                                    Errors error,
                                                    int partitionSize) {
        return mockOffsetCommitResponse(topic, apiKeyVersion, time.milliseconds(), time.milliseconds(), error, partitionSize);
    }

    private ClientResponse mockOffsetCommitResponse(String topic,
                                                    short apiKeyVersion,
                                                    long createdTimeMs,
                                                    long receivedTimeMs,
                                                    Errors error,
                                                    int partitionSize) {
        OffsetCommitResponseData responseData = new OffsetCommitResponseData()
                .setTopics(Collections.singletonList(
                        new OffsetCommitResponseData.OffsetCommitResponseTopic()
                                .setName(topic)
                                .setPartitions(mockOffsetCommitResponseWithPartitionErrors(error, partitionSize))));
        OffsetCommitResponse response = mock(OffsetCommitResponse.class);
        when(response.data()).thenReturn(responseData);
        return new ClientResponse(
                new RequestHeader(ApiKeys.OFFSET_COMMIT, apiKeyVersion, "", 1),
                null,
                "-1",
                createdTimeMs,
                receivedTimeMs,
                false,
                null,
                null,
                new OffsetCommitResponse(responseData)
        );
    }

    private ClientResponse mockOffsetCommitResponseDisconnected(String topic, int partition,
                                                               short apiKeyVersion,
                                                               NetworkClientDelegate.UnsentRequest unsentRequest) {
        OffsetCommitResponseData responseData = new OffsetCommitResponseData()
            .setTopics(Collections.singletonList(
                new OffsetCommitResponseData.OffsetCommitResponseTopic()
                    .setName(topic)
                    .setPartitions(Collections.singletonList(
                        new OffsetCommitResponseData.OffsetCommitResponsePartition()
                            .setErrorCode(Errors.NONE.code())
                            .setPartitionIndex(partition)))));
        OffsetCommitResponse response = mock(OffsetCommitResponse.class);
        when(response.data()).thenReturn(responseData);
        return new ClientResponse(
            new RequestHeader(ApiKeys.OFFSET_COMMIT, apiKeyVersion, "", 1),
            unsentRequest.handler(),
            "-1",
            time.milliseconds(),
            time.milliseconds(),
            true,
            null,
            null,
            new OffsetCommitResponse(responseData)
        );
    }

    private ClientResponse buildOffsetFetchClientResponse(
            final NetworkClientDelegate.UnsentRequest request,
            final Map<TopicPartition, OffsetFetchResponse.PartitionData> topicPartitionData,
            final Errors error,
            final boolean disconnected) {
        AbstractRequest abstractRequest = request.requestBuilder().build();
        assertInstanceOf(OffsetFetchRequest.class, abstractRequest);
        OffsetFetchRequest offsetFetchRequest = (OffsetFetchRequest) abstractRequest;
        OffsetFetchResponse response =
                new OffsetFetchResponse(error, topicPartitionData);
        return new ClientResponse(
                new RequestHeader(ApiKeys.OFFSET_FETCH, offsetFetchRequest.version(), "", 1),
                request.handler(),
                "-1",
                time.milliseconds(),
                time.milliseconds(),
                disconnected,
                null,
                null,
                response
        );
    }

    private KafkaMetric getMetric(String name) {
        return metrics.metrics().get(metrics.metricName(
            name,
            CONSUMER_COORDINATOR_METRICS));
    }

    private List<OffsetCommitResponseData.OffsetCommitResponsePartition> mockOffsetCommitResponseWithPartitionErrors(Errors error, int partitionSize) {
        List<OffsetCommitResponseData.OffsetCommitResponsePartition> partitions = new ArrayList<>(partitionSize);
        for (int i = 0; i < partitionSize; i++) {
            partitions.add(new OffsetCommitResponseData.OffsetCommitResponsePartition().setErrorCode(error.code()).setPartitionIndex(i));
        }
        return partitions;
    }
}
