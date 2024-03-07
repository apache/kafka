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
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
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
import java.util.Arrays;
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
import static org.apache.kafka.clients.consumer.internals.ConsumerTestBuilder.DEFAULT_GROUP_ID;
import static org.apache.kafka.clients.consumer.internals.ConsumerTestBuilder.DEFAULT_GROUP_INSTANCE_ID;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CommitRequestManagerTest {

    private long retryBackoffMs = 100;
    private long retryBackoffMaxMs = 1000;
    private static final String CONSUMER_COORDINATOR_METRICS = "consumer-coordinator-metrics";
    private Node mockedNode = new Node(1, "host1", 9092);
    private SubscriptionState subscriptionState;
    private LogContext logContext;
    private MockTime time;
    private CoordinatorRequestManager coordinatorRequestManager;
    private OffsetCommitCallbackInvoker offsetCommitCallbackInvoker;
    private Metrics metrics = new Metrics();
    private Properties props;

    private final int defaultApiTimeoutMs = 60000;


    @BeforeEach
    public void setup() {
        this.logContext = new LogContext();
        this.time = new MockTime(0);
        this.subscriptionState = new SubscriptionState(new LogContext(), OffsetResetStrategy.EARLIEST);
        this.coordinatorRequestManager = mock(CoordinatorRequestManager.class);
        this.offsetCommitCallbackInvoker = mock(OffsetCommitCallbackInvoker.class);
        this.props = new Properties();
        this.props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
        this.props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        this.props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    @Test
    public void testPollSkipIfCoordinatorUnknown() {
        CommitRequestManager commitRequestManger = create(false, 0);
        assertPoll(false, 0, commitRequestManger);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("t1", 0), new OffsetAndMetadata(0));
        commitRequestManger.commitAsync(offsets);
        assertPoll(false, 0, commitRequestManger);
    }

    @Test
    public void testPollEnsureManualCommitSent() {
        CommitRequestManager commitRequestManger = create(false, 0);
        assertPoll(0, commitRequestManger);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("t1", 0), new OffsetAndMetadata(0));
        commitRequestManger.commitAsync(offsets);
        assertPoll(1, commitRequestManger);
    }

    @Test
    public void testPollEnsureAutocommitSent() {
        TopicPartition tp = new TopicPartition("t1", 1);
        subscriptionState.assignFromUser(Collections.singleton(tp));
        subscriptionState.seek(tp, 100);
        CommitRequestManager commitRequestManger = create(true, 100);
        assertPoll(0, commitRequestManger);

        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        time.sleep(100);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        List<NetworkClientDelegate.FutureCompletionHandler> pollResults = assertPoll(1, commitRequestManger);
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
        long expirationTimeMs  = time.milliseconds() + defaultApiTimeoutMs;
        commitManager.commitSync(offsets1, expirationTimeMs);
        commitManager.fetchOffsets(Collections.singleton(new TopicPartition("test", 0)), expirationTimeMs);
        commitManager.commitSync(offsets2, expirationTimeMs);
        commitManager.fetchOffsets(Collections.singleton(new TopicPartition("test", 1)), expirationTimeMs);

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
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
            new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));
        commitRequestManger.commitAsync(offsets);
        assertEquals(1, commitRequestManger.unsentOffsetCommitRequests().size());
        assertEquals(1, commitRequestManger.poll(time.milliseconds()).unsentRequests.size());
        assertTrue(commitRequestManger.unsentOffsetCommitRequests().isEmpty());
        assertEmptyPendingRequests(commitRequestManger);
    }

    // This is the case of the async auto commit sent on calls to assign (async commit that
    // should not be retried).
    @Test
    public void testAsyncAutocommitNotRetriedAfterException() {
        long commitInterval = retryBackoffMs * 2;
        CommitRequestManager commitRequestManger = create(true, commitInterval);
        TopicPartition tp = new TopicPartition("topic", 1);
        subscriptionState.assignFromUser(Collections.singleton(tp));
        subscriptionState.seek(tp, 100);
        time.sleep(commitInterval);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        List<NetworkClientDelegate.FutureCompletionHandler> futures = assertPoll(1, commitRequestManger);
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
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        assertPoll(0, commitRequestManger);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());

        // Only when polling after the auto-commit interval, a new auto-commit request should be
        // generated.
        time.sleep(commitInterval);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        futures = assertPoll(1, commitRequestManger);
        assertEmptyPendingRequests(commitRequestManger);
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
        CommitRequestManager commitRequestManger = create(false, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
            new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));
        long expirationTimeMs = time.milliseconds() + defaultApiTimeoutMs;
        CompletableFuture<Void> commitResult = commitRequestManger.commitSync(offsets, expirationTimeMs);
        sendAndVerifyOffsetCommitRequestFailedAndMaybeRetried(commitRequestManger, error, commitResult);

        // We expect that request should have been retried on this sync commit.
        assertExceptionHandling(commitRequestManger, error, true);
    }

    @ParameterizedTest
    @MethodSource("commitSyncExpectedExceptions")
    public void testCommitSyncFailsWithExpectedException(Errors commitError,
                                                         Class<? extends Exception> expectedException) {
        CommitRequestManager commitRequestManger = create(false, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
            new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        // Send sync offset commit that fails and verify it propagates the expected exception.
        Long expirationTimeMs = time.milliseconds() + retryBackoffMs;
        CompletableFuture<Void> commitResult = commitRequestManger.commitSync(offsets, expirationTimeMs);
        completeOffsetCommitRequestWithError(commitRequestManger, commitError);
        assertFutureThrows(commitResult, expectedException);
    }

    private static Stream<Arguments> commitSyncExpectedExceptions() {
        return Stream.of(
            Arguments.of(Errors.FENCED_INSTANCE_ID, CommitFailedException.class),
            Arguments.of(Errors.UNKNOWN_MEMBER_ID, CommitFailedException.class),
            Arguments.of(Errors.OFFSET_METADATA_TOO_LARGE, Errors.OFFSET_METADATA_TOO_LARGE.exception().getClass()),
            Arguments.of(Errors.INVALID_COMMIT_OFFSET_SIZE, Errors.INVALID_COMMIT_OFFSET_SIZE.exception().getClass()),
            Arguments.of(Errors.GROUP_AUTHORIZATION_FAILED, Errors.GROUP_AUTHORIZATION_FAILED.exception().getClass()),
            Arguments.of(Errors.CORRUPT_MESSAGE, KafkaException.class),
            Arguments.of(Errors.UNKNOWN_SERVER_ERROR, KafkaException.class));
    }

    @Test
    public void testCommitSyncFailsWithCommitFailedExceptionIfUnknownMemberId() {
        CommitRequestManager commitRequestManger = create(false, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
            new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));
        long expirationTimeMs = time.milliseconds() + defaultApiTimeoutMs;
        CompletableFuture<Void> commitResult = commitRequestManger.commitSync(offsets, expirationTimeMs);

        completeOffsetCommitRequestWithError(commitRequestManger, Errors.UNKNOWN_MEMBER_ID);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        assertTrue(commitResult.isDone());
        assertFutureThrows(commitResult, CommitFailedException.class);
    }

    @Test
    public void testCommitSyncFailsWithCommitFailedExceptionOnStaleMemberEpoch() {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
            new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        // Send commit request expected to be retried on retriable errors
        CompletableFuture<Void> commitResult = commitRequestManger.commitSync(
            offsets, time.milliseconds() + defaultApiTimeoutMs);
        completeOffsetCommitRequestWithError(commitRequestManger, Errors.STALE_MEMBER_EPOCH);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
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
        CommitRequestManager commitRequestManger = create(true, 100);
        time.sleep(100);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        TopicPartition t1p = new TopicPartition("topic1", 0);
        subscriptionState.assignFromUser(singleton(t1p));
        subscriptionState.seek(t1p, 10);

        // Async commit on the interval fails with fatal stale epoch and just resets the timer to
        // the interval
        commitRequestManger.maybeAutoCommitAsync();
        completeOffsetCommitRequestWithError(commitRequestManger, Errors.STALE_MEMBER_EPOCH);
        verify(commitRequestManger).resetAutoCommitTimer();

        // Async commit retried, only when the interval expires
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size(), "No request should be generated until the " +
            "interval expires");
        time.sleep(100);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        res = commitRequestManger.poll(time.milliseconds());
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
        CompletableFuture<Void> commitResult = commitRequestManager.commitAsync(offsets);

        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        NetworkClientDelegate.UnsentRequest req = res.unsentRequests.get(0);
        ClientResponse response = mockOffsetCommitResponseDisconnected("topic", 1, (short) 1, req);
        response.onComplete();

        // Commit should mark the coordinator unknown and fail with RetriableCommitFailedException.
        assertTrue(commitResult.isDone());
        assertFutureThrows(commitResult, RetriableCommitFailedException.class);
        assertCoordinatorDisconnect();
    }

    @Test
    public void testAutocommitEnsureOnlyOneInflightRequest() {
        TopicPartition t1p = new TopicPartition("topic1", 0);
        subscriptionState.assignFromUser(singleton(t1p));
        subscriptionState.seek(t1p, 100);

        CommitRequestManager commitRequestManger = create(true, 100);
        time.sleep(100);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        List<NetworkClientDelegate.FutureCompletionHandler> futures = assertPoll(1, commitRequestManger);

        time.sleep(100);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        // We want to make sure we don't resend autocommit if the previous request has not been
        // completed, even if the interval expired
        assertPoll(0, commitRequestManger);
        assertEmptyPendingRequests(commitRequestManger);

        // complete the unsent request and re-poll
        futures.get(0).onComplete(buildOffsetCommitClientResponse(new OffsetCommitResponse(0, new HashMap<>())));
        assertPoll(1, commitRequestManger);
    }

    @Test
    public void testAutocommitInterceptorsInvoked() {
        TopicPartition t1p = new TopicPartition("topic1", 0);
        subscriptionState.assignFromUser(singleton(t1p));
        subscriptionState.seek(t1p, 100);

        CommitRequestManager commitRequestManger = create(true, 100);
        time.sleep(100);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        List<NetworkClientDelegate.FutureCompletionHandler> futures = assertPoll(1, commitRequestManger);

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

        CommitRequestManager commitRequestManger = create(true, 100);
        time.sleep(100);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        List<NetworkClientDelegate.FutureCompletionHandler> futures = assertPoll(1, commitRequestManger);

        // complete the unsent request to trigger interceptor
        futures.get(0).onComplete(buildOffsetCommitClientResponse(
            new OffsetCommitResponse(0, Collections.singletonMap(t1p, Errors.NETWORK_EXCEPTION)))
        );
        Mockito.verify(offsetCommitCallbackInvoker, never()).enqueueInterceptorInvocation(any());
    }

    @Test
    public void testAutoCommitEmptyOffsetsDoesNotGenerateRequest() {
        CommitRequestManager commitRequestManger = create(true, 100);
        time.sleep(100);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        // CompletableFuture<Void> result = commitRequestManger.maybeAutoCommitAllConsumedAsync();
        commitRequestManger.maybeAutoCommitAsync();
        assertTrue(commitRequestManger.pendingRequests.unsentOffsetCommits.isEmpty());
        verify(commitRequestManger).resetAutoCommitTimer();
    }

    @Test
    public void testAutoCommitEmptyDoesNotLeaveInflightRequestFlagOn() {
        TopicPartition t1p = new TopicPartition("topic1", 0);
        subscriptionState.assignFromUser(singleton(t1p));
        CommitRequestManager commitRequestManger = create(true, 100);

        // Auto-commit of empty offsets
        time.sleep(100);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        commitRequestManger.maybeAutoCommitAsync();

        // Next auto-commit consumed offsets (not empty). Should generate a request, ensuring
        // that the previous auto-commit of empty did not leave the inflight request flag on
        subscriptionState.seek(t1p, 100);
        time.sleep(100);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        commitRequestManger.maybeAutoCommitAsync();
        assertEquals(1, commitRequestManger.pendingRequests.unsentOffsetCommits.size());

        verify(commitRequestManger, times(2)).resetAutoCommitTimer();
    }

    @Test
    public void testOffsetFetchRequestEnsureDuplicatedRequestSucceed() {
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
    public void testOffsetFetchRequestErroredRequests(final Errors error, final boolean isRetriable) {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(new TopicPartition("t1", 0));
        List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> futures = sendAndVerifyDuplicatedOffsetFetchRequests(
            commitRequestManger,
            partitions,
            1,
            error);
        // we only want to make sure to purge the outbound buffer for non-retriables, so retriable will be re-queued.
        if (isRetriable)
            testRetriable(commitRequestManger, futures);
        else {
            testNonRetriable(futures);
            assertEmptyPendingRequests(commitRequestManger);
        }
    }

    @Test
    public void testSuccessfulOffsetFetch() {
        CommitRequestManager commitManager = create(false, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        long expirationTimeMs = time.milliseconds() + defaultApiTimeoutMs;
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> fetchResult =
            commitManager.fetchOffsets(Collections.singleton(new TopicPartition("test", 0)),
                expirationTimeMs);

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

        long expirationTimeMs = time.milliseconds() + defaultApiTimeoutMs;
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> result = commitRequestManager.fetchOffsets(partitions, expirationTimeMs);

        completeOffsetFetchRequestWithError(commitRequestManager, partitions, error);

        // Request not completed just yet
        assertFalse(result.isDone());
        if (shouldRediscoverCoordinator) {
            assertCoordinatorDisconnect();
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
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(new TopicPartition("t1", 0));

        long expirationTimeMs = time.milliseconds() + defaultApiTimeoutMs;
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> result = commitRequestManger.fetchOffsets(partitions, expirationTimeMs);

        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        NetworkClientDelegate.UnsentRequest request = res.unsentRequests.get(0);
        ClientResponse response = buildOffsetFetchClientResponseDisconnected(request);
        response.onComplete();

        // Request not completed just yet, but should have marked the coordinator unknown
        assertFalse(result.isDone());
        assertCoordinatorDisconnect();

        time.sleep(retryBackoffMs);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        res = commitRequestManger.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        assertEquals(1, commitRequestManger.pendingRequests.inflightOffsetFetches.size());
    }

    @ParameterizedTest
    @MethodSource("offsetCommitExceptionSupplier")
    public void testOffsetCommitRequestErroredRequestsNotRetriedForAsyncCommit(final Errors error) {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        // Send async commit (not expected to be retried).
        CompletableFuture<Void> commitResult = commitRequestManger.commitAsync(offsets);
        completeOffsetCommitRequestWithError(commitRequestManger, error);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        assertTrue(commitResult.isDone());
        assertTrue(commitResult.isCompletedExceptionally());
        if (error.exception() instanceof RetriableException) {
            assertFutureThrows(commitResult, RetriableCommitFailedException.class);
        }

        // We expect that the request should not have been retried on this async commit.
        assertExceptionHandling(commitRequestManger, error, false);
    }

    @Test
    public void testOffsetCommitSyncTimeoutNotReturnedOnPollAndFails() {
        CommitRequestManager commitRequestManger = create(false, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
            new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        // Send sync offset commit request that fails with retriable error.
        Long expirationTimeMs = time.milliseconds() + retryBackoffMs * 2;
        CompletableFuture<Void> commitResult = commitRequestManger.commitSync(offsets, expirationTimeMs);
        completeOffsetCommitRequestWithError(commitRequestManger, Errors.REQUEST_TIMED_OUT);

        // Request retried after backoff, and fails with retriable again. Should not complete yet
        // given that the request timeout hasn't expired.
        time.sleep(retryBackoffMs);
        completeOffsetCommitRequestWithError(commitRequestManger, Errors.REQUEST_TIMED_OUT);
        assertFalse(commitResult.isDone());

        // Sleep to expire the request timeout. Request should fail on the next poll.
        time.sleep(retryBackoffMs);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        assertTrue(commitResult.isDone());
        assertTrue(commitResult.isCompletedExceptionally());
    }

    /**
     * Sync commit requests that fail with an expected retriable error should be retried
     * while there is time. When time expires, they should fail with a TimeoutException.
     */
    @Test
    public void testOffsetCommitSyncFailedWithRetriableThrowsTimeoutWhenRetryTimeExpires() {
        CommitRequestManager commitRequestManger = create(false, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
            new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        // Send offset commit request that fails with retriable error.
        long expirationTimeMs = time.milliseconds() + retryBackoffMs * 2;
        CompletableFuture<Void> commitResult = commitRequestManger.commitSync(offsets, expirationTimeMs);
        completeOffsetCommitRequestWithError(commitRequestManger, Errors.COORDINATOR_NOT_AVAILABLE);

        // Sleep to expire the request timeout. Request should fail on the next poll with a
        // TimeoutException.
        time.sleep(expirationTimeMs);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        assertTrue(commitResult.isDone());
        assertFutureThrows(commitResult, TimeoutException.class);
    }

    /**
     * Async commit requests that fail with a retriable error are not retried, and they should fail
     * with a RetriableCommitException.
     */
    @Test
    public void testOffsetCommitAsyncFailedWithRetriableThrowsRetriableCommitException() {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        // Send async commit request that fails with retriable error (not expected to be retried).
        Errors retriableError = Errors.COORDINATOR_NOT_AVAILABLE;
        CompletableFuture<Void> commitResult = commitRequestManger.commitAsync(offsets);
        completeOffsetCommitRequestWithError(commitRequestManger, retriableError);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        assertTrue(commitResult.isDone());
        assertTrue(commitResult.isCompletedExceptionally());

        // We expect that the request should not have been retried on this async commit.
        assertExceptionHandling(commitRequestManger, retriableError, false);

        // Request should complete with a RetriableCommitException
        assertFutureThrows(commitResult, RetriableCommitFailedException.class);
    }

    @Test
    public void testEnsureBackoffRetryOnOffsetCommitRequestTimeout() {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        long expirationTimeMs = time.milliseconds() + defaultApiTimeoutMs;
        commitRequestManger.commitSync(offsets, expirationTimeMs);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        res.unsentRequests.get(0).handler().onFailure(time.milliseconds(), new TimeoutException());

        assertTrue(commitRequestManger.pendingRequests.hasUnsentRequests());
        assertEquals(1, commitRequestManger.unsentOffsetCommitRequests().size());
        assertRetryBackOff(commitRequestManger, retryBackoffMs);
    }

    private void assertCoordinatorDisconnect() {
        verify(coordinatorRequestManager).markCoordinatorUnknown(any(), anyLong());
    }

    private void assertExceptionHandling(CommitRequestManager commitRequestManger, Errors errors,
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
                assertPollDoesNotReturn(commitRequestManger, remainBackoffMs);
                break;
            case UNKNOWN_TOPIC_OR_PARTITION:
            case COORDINATOR_LOAD_IN_PROGRESS:
                if (requestShouldBeRetried) {
                    assertRetryBackOff(commitRequestManger, remainBackoffMs);
                }
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
                if (errors.exception() instanceof RetriableException && requestShouldBeRetried) {
                    assertRetryBackOff(commitRequestManger, remainBackoffMs);
                } else {
                    assertPollDoesNotReturn(commitRequestManger, Long.MAX_VALUE);
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
        long expirationTimeMs = time.milliseconds() + defaultApiTimeoutMs;
        commitRequestManager.fetchOffsets(partitions, expirationTimeMs);

        // Mock member has new a valid epoch.
        int newEpoch = 8;
        String memberId = "member1";
        commitRequestManager.onMemberEpochUpdated(Optional.of(newEpoch), Optional.of(memberId));

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
        long expirationTimeMs = time.milliseconds() + defaultApiTimeoutMs;
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> requestResult =
            commitRequestManager.fetchOffsets(partitions, expirationTimeMs);

        // Mock member not having a valid epoch anymore (left/failed/fenced).
        commitRequestManager.onMemberEpochUpdated(Optional.empty(), Optional.empty());

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
        long expirationTimeMs = time.milliseconds() + retryBackoffMs * 2;

        // Send commit request expected to be retried on STALE_MEMBER_EPOCH error while it does not expire
        commitRequestManager.maybeAutoCommitSyncNow(expirationTimeMs);

        int newEpoch = 8;
        String memberId = "member1";
        if (error == Errors.STALE_MEMBER_EPOCH) {
            // Mock member has new a valid epoch
            commitRequestManager.onMemberEpochUpdated(Optional.of(newEpoch), Optional.of(memberId));
        }

        completeOffsetCommitRequestWithError(commitRequestManager, error);

        if (error.exception() instanceof RetriableException || error == Errors.STALE_MEMBER_EPOCH) {
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
        commitRequestManager.commitAsync(offsets);

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

    private void testRetriable(final CommitRequestManager commitRequestManger,
                               final List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> futures) {
        futures.forEach(f -> assertFalse(f.isDone()));

        // The manager should backoff for 100ms
        time.sleep(100);
        commitRequestManger.poll(time.milliseconds());
        futures.forEach(f -> assertFalse(f.isDone()));
    }

    private void testNonRetriable(final List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> futures) {
        futures.forEach(f -> assertTrue(f.isCompletedExceptionally()));
    }

    /**
     * @return {@link Errors} that could be received in OffsetCommit responses.
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
            Arguments.of(Errors.FENCED_INSTANCE_ID),
            Arguments.of(Errors.TOPIC_AUTHORIZATION_FAILED),
            Arguments.of(Errors.STALE_MEMBER_EPOCH),
            Arguments.of(Errors.UNKNOWN_MEMBER_ID));
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
            Arguments.of(Errors.COORDINATOR_NOT_AVAILABLE, true),
            Arguments.of(Errors.REQUEST_TIMED_OUT, false),
            Arguments.of(Errors.FENCED_INSTANCE_ID, false),
            Arguments.of(Errors.TOPIC_AUTHORIZATION_FAILED, false),
            Arguments.of(Errors.UNKNOWN_MEMBER_ID, false),
            // Adding STALE_MEMBER_EPOCH as non-retriable here because it is only retried if a new
            // member epoch is received. Tested separately.
            Arguments.of(Errors.STALE_MEMBER_EPOCH, false));
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
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        Set<TopicPartition> partitions = new HashSet<>();
        TopicPartition tp1 = new TopicPartition("t1", 2);
        TopicPartition tp2 = new TopicPartition("t2", 3);
        partitions.add(tp1);
        partitions.add(tp2);
        long expirationTimeMs = time.milliseconds() + defaultApiTimeoutMs;
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future =
                commitRequestManger.fetchOffsets(partitions, expirationTimeMs);

        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());

        // Setting 1 partition with error
        HashMap<TopicPartition, OffsetFetchResponse.PartitionData> topicPartitionData = new HashMap<>();
        topicPartitionData.put(tp1, new OffsetFetchResponse.PartitionData(100L, Optional.of(1), "metadata", error));
        topicPartitionData.put(tp2, new OffsetFetchResponse.PartitionData(100L, Optional.of(1), "metadata", Errors.NONE));

        res.unsentRequests.get(0).handler().onComplete(buildOffsetFetchClientResponse(
                res.unsentRequests.get(0),
                topicPartitionData,
                Errors.NONE,
                false));
        if (isRetriable)
            testRetriable(commitRequestManger, Collections.singletonList(future));
        else
            testNonRetriable(Collections.singletonList(future));
    }

    @Test
    public void testSignalClose() {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        commitRequestManger.commitAsync(offsets);
        commitRequestManger.signalClose();
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        OffsetCommitRequestData data = (OffsetCommitRequestData) res.unsentRequests.get(0).requestBuilder().build().data();
        assertEquals("topic", data.topics().get(0).name());
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
        long expirationTimeMs = time.milliseconds() + defaultApiTimeoutMs;
        for (int i = 0; i < numRequest; i++) {
            futures.add(commitRequestManger.fetchOffsets(partitions, expirationTimeMs));
        }

        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        res.unsentRequests.get(0).handler().onComplete(buildOffsetFetchClientResponse(res.unsentRequests.get(0),
            partitions, error));
        res = commitRequestManger.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        return futures;
    }

    private void sendAndVerifyOffsetCommitRequestFailedAndMaybeRetried(
        final CommitRequestManager commitRequestManger,
        final Errors error,
        final CompletableFuture<Void> commitResult) {
        completeOffsetCommitRequestWithError(commitRequestManger, error);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
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
                metrics));
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


    public ClientResponse mockOffsetCommitResponse(String topic,
                                                   int partition,
                                                   short apiKeyVersion,
                                                   Errors error) {
        return mockOffsetCommitResponse(topic, partition, apiKeyVersion, time.milliseconds(), time.milliseconds(), error);
    }
    public ClientResponse mockOffsetCommitResponse(String topic,
                                                   int partition,
                                                   short apiKeyVersion,
                                                   long createdTimeMs,
                                                   long receivedTimeMs,
                                                   Errors error) {
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
                createdTimeMs,
                receivedTimeMs,
                false,
                null,
                null,
                new OffsetCommitResponse(responseData)
        );
    }

    public ClientResponse mockOffsetCommitResponseDisconnected(String topic, int partition,
                                                               short apiKeyVersion,
                                                               NetworkClientDelegate.UnsentRequest unsentRequest) {
        OffsetCommitResponseData responseData = new OffsetCommitResponseData()
            .setTopics(Arrays.asList(
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
}
