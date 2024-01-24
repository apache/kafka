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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.DisconnectException;
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
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.test.TestUtils;
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
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.COORDINATOR_METRICS_SUFFIX;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    private String consumerMetricGroupPrefix = CONSUMER_METRIC_GROUP_PREFIX;
    private String consumerMetricGroupName = consumerMetricGroupPrefix + COORDINATOR_METRICS_SUFFIX;
    private Node mockedNode = new Node(1, "host1", 9092);
    private SubscriptionState subscriptionState;
    private LogContext logContext;
    private MockTime time;
    private CoordinatorRequestManager coordinatorRequestManager;
    private Metrics metrics = new Metrics();
    private Properties props;
    private final int defaultApiTimeoutMs = 60000;


    @BeforeEach
    public void setup() {
        this.logContext = new LogContext();
        this.time = new MockTime(0);
        this.subscriptionState = new SubscriptionState(new LogContext(), OffsetResetStrategy.EARLIEST);
        this.coordinatorRequestManager = mock(CoordinatorRequestManager.class);
        this.props = new Properties();
        this.props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
        this.props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        this.props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    @Test
    public void testPollSkipIfCoordinatorUnknown() {
        CommitRequestManager commitRequestManger = create(false, 0);
        Timer timer = time.timer(defaultApiTimeoutMs);
        assertPoll(false, 0, commitRequestManger, timer);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("t1", 0), new OffsetAndMetadata(0));
        commitRequestManger.addOffsetCommitRequest(offsets, Optional.empty(), false);
        assertPoll(false, 0, commitRequestManger, timer);
    }

    @Test
    public void testPollEnsureManualCommitSent() {
        CommitRequestManager commitRequestManger = create(false, 0);
        Timer timer = time.timer(defaultApiTimeoutMs);
        assertPoll(0, commitRequestManger, timer);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("t1", 0), new OffsetAndMetadata(0));
        commitRequestManger.addOffsetCommitRequest(offsets, Optional.empty(), false);
        assertPoll(1, commitRequestManger, timer);
    }

    @Test
    public void testPollEnsureAutocommitSent() {
        TopicPartition tp = new TopicPartition("t1", 1);
        subscriptionState.assignFromUser(Collections.singleton(tp));
        subscriptionState.seek(tp, 100);
        CommitRequestManager commitRequestManger = create(true, 100);
        Timer timer = time.timer(defaultApiTimeoutMs);
        assertPoll(0, commitRequestManger, timer);

        commitRequestManger.updateAutoCommitTimer(timer.currentTimeMs());
        timer.sleep(100);
        commitRequestManger.updateAutoCommitTimer(timer.currentTimeMs());
        List<NetworkClientDelegate.FutureCompletionHandler> pollResults = assertPoll(1, commitRequestManger, timer);
        pollResults.forEach(v -> v.onComplete(mockOffsetCommitResponse(
                "t1",
                1,
                (short) 1,
                Errors.NONE,
                timer)));
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
        ArrayList<CompletableFuture<Void>> commitFutures = new ArrayList<>();
        ArrayList<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> fetchFutures = new ArrayList<>();
        Timer timer = time.timer(defaultApiTimeoutMs);
        commitFutures.add(commitManager.addOffsetCommitRequest(offsets1, Optional.of(timer), false));
        fetchFutures.add(commitManager.addOffsetFetchRequest(Collections.singleton(new TopicPartition("test", 0)), timer));
        commitFutures.add(commitManager.addOffsetCommitRequest(offsets2, Optional.of(timer), false));
        fetchFutures.add(commitManager.addOffsetFetchRequest(Collections.singleton(new TopicPartition("test", 1)), timer));

        // Poll the CommitRequestManager and verify that the inflightOffsetFetches size is correct
        NetworkClientDelegate.PollResult result = commitManager.poll(timer.currentTimeMs());
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
    public void testPollEnsureEmptyPendingRequestAfterPoll() {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
            new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));
        commitRequestManger.addOffsetCommitRequest(offsets, Optional.empty(), false);
        assertEquals(1, commitRequestManger.unsentOffsetCommitRequests().size());
        Timer timer = time.timer(defaultApiTimeoutMs);
        assertEquals(1, commitRequestManger.poll(timer.currentTimeMs()).unsentRequests.size());
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
        Timer timer = time.timer(defaultApiTimeoutMs);
        timer.sleep(commitInterval);
        commitRequestManger.updateAutoCommitTimer(timer.currentTimeMs());
        List<NetworkClientDelegate.FutureCompletionHandler> futures = assertPoll(1, commitRequestManger, timer);
        // Complete the autocommit request exceptionally. It should fail right away, without retry.
        futures.get(0).onComplete(mockOffsetCommitResponse(
            "topic",
            1,
            (short) 1,
            Errors.COORDINATOR_LOAD_IN_PROGRESS,
            timer));

        // When polling again before the auto-commit interval no request should be generated
        // (making sure we wait for the backoff, to check that the failed request is not being
        // retried).
        timer.sleep(retryBackoffMs);
        commitRequestManger.updateAutoCommitTimer(timer.currentTimeMs());
        assertPoll(0, commitRequestManger, timer);
        commitRequestManger.updateAutoCommitTimer(timer.currentTimeMs());

        // Only when polling after the auto-commit interval, a new auto-commit request should be
        // generated.
        timer.sleep(commitInterval);
        commitRequestManger.updateAutoCommitTimer(timer.currentTimeMs());
        futures = assertPoll(1, commitRequestManger, timer);
        assertEmptyPendingRequests(commitRequestManger);
        futures.get(0).onComplete(mockOffsetCommitResponse(
                "topic",
                1,
                (short) 1,
                Errors.NONE,
                timer));
    }

    // This is the case of the sync auto commit sent when the consumer is being closed (sync commit
    // that should be retried until it succeeds, fails, or timer expires).
    @ParameterizedTest
    @MethodSource("offsetCommitExceptionSupplier")
    public void testSyncAutocommitRetriedAfterRetriableException(Errors error) {
        long commitInterval = retryBackoffMs * 2;
        CommitRequestManager commitRequestManger = create(true, commitInterval);
        TopicPartition tp = new TopicPartition("topic", 1);
        subscriptionState.assignFromUser(Collections.singleton(tp));
        subscriptionState.seek(tp, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        Timer timer = time.timer(defaultApiTimeoutMs);
        timer.sleep(commitInterval);
        commitRequestManger.updateAutoCommitTimer(timer.currentTimeMs());

        // Auto-commit all consume sync (ex. triggered when the consumer is closed).
        CompletableFuture<Void> commitResult =
            commitRequestManger.maybeAutoCommitAllConsumedNow(Optional.of(timer), false);
        sendAndVerifyOffsetCommitRequestFailedAndMaybeRetried(commitRequestManger, error, commitResult, timer);

        // We expect that request should have been retried on this sync commit.
        assertExceptionHandling(commitRequestManger, error, true, timer);
        assertCoordinatorDisconnect(error);
    }

    @Test
    public void testOffsetCommitFailsWithCommitFailedExceptionIfUnknownMemberId() {
        long commitInterval = retryBackoffMs * 2;
        CommitRequestManager commitRequestManger = create(true, commitInterval);
        TopicPartition tp = new TopicPartition("topic", 1);
        subscriptionState.assignFromUser(Collections.singleton(tp));
        subscriptionState.seek(tp, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        Timer timer = time.timer(defaultApiTimeoutMs);
        timer.sleep(commitInterval);
        commitRequestManger.updateAutoCommitTimer(timer.currentTimeMs());

        // Auto-commit all consume sync (ex. triggered when the consumer is closed).
        CompletableFuture<Void> commitResult =
            commitRequestManger.maybeAutoCommitAllConsumedNow(Optional.of(timer), false);

        completeOffsetCommitRequestWithError(commitRequestManger, Errors.UNKNOWN_MEMBER_ID, timer);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(timer.currentTimeMs());
        assertEquals(0, res.unsentRequests.size());
        // Commit should fail with CommitFailedException
        assertTrue(commitResult.isDone());
        assertFutureThrows(commitResult, CommitFailedException.class);
    }

    /**
     * This is the case where a request to commit offsets is performed without retrying on
     * STALE_MEMBER_EPOCH (ex. commits triggered from the consumer API, auto-commits triggered on
     * the interval). The expectation is that the request should fail with CommitFailedException.
     */
    @Test
    public void testOffsetCommitFailsWithCommitFailedExceptionIfStaleMemberEpochNotRetried() {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
            new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        // Send commit request expected to be retried on retriable errors
        Timer timer = time.timer(defaultApiTimeoutMs);
        CompletableFuture<Void> commitResult = commitRequestManger.addOffsetCommitRequest(
            offsets, Optional.of(timer), false);
        completeOffsetCommitRequestWithError(commitRequestManger, Errors.STALE_MEMBER_EPOCH, timer);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(timer.currentTimeMs());
        assertEquals(0, res.unsentRequests.size());

        // Commit should fail with CommitFailedException
        assertTrue(commitResult.isDone());
        assertFutureThrows(commitResult, CommitFailedException.class);
    }

    /**
     * This is the case of the async auto commit request triggered on the interval. The
     * expectation is that the request should fail with RetriableCommitFailedException, so that
     * the timer is reset and the request is retried on the next interval.
     */
    @Test
    public void testOffsetAutoCommitOnIntervalFailsWithRetriableOnStaleMemberEpoch() {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
            new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        // Async auto-commit that won't be retried.
        CompletableFuture<Void> commitResult = commitRequestManger.addOffsetCommitRequest(
            offsets, Optional.empty(), true);
        Timer timer = time.timer(defaultApiTimeoutMs);
        completeOffsetCommitRequestWithError(commitRequestManger, Errors.STALE_MEMBER_EPOCH, timer);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(timer.currentTimeMs());
        assertEquals(0, res.unsentRequests.size());

        // Commit should fail with RetriableCommitFailedException.
        assertTrue(commitResult.isDone());
        assertFutureThrows(commitResult, RetriableCommitFailedException.class);
    }

    @Test
    public void testAutocommitEnsureOnlyOneInflightRequest() {
        TopicPartition t1p = new TopicPartition("topic1", 0);
        subscriptionState.assignFromUser(singleton(t1p));
        subscriptionState.seek(t1p, 100);

        CommitRequestManager commitRequestManger = create(true, 100);
        Timer timer = time.timer(defaultApiTimeoutMs);
        timer.sleep(100);
        commitRequestManger.updateAutoCommitTimer(timer.currentTimeMs());
        List<NetworkClientDelegate.FutureCompletionHandler> futures = assertPoll(1, commitRequestManger, timer);

        timer.sleep(100);
        commitRequestManger.updateAutoCommitTimer(timer.currentTimeMs());
        // We want to make sure we don't resend autocommit if the previous request has not been
        // completed, even if the interval expired
        assertPoll(0, commitRequestManger, timer);
        assertEmptyPendingRequests(commitRequestManger);

        // complete the unsent request and re-poll
        futures.get(0).onComplete(buildOffsetCommitClientResponse(new OffsetCommitResponse(0, new HashMap<>()), timer));
        assertPoll(1, commitRequestManger, timer);
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
            5,
            error);
        // we only want to make sure to purge the outbound buffer for non-retriables, so retriable will be re-queued.
        if (isRetriable) {
            Timer timer = time.timer(defaultApiTimeoutMs);
            testRetriable(commitRequestManger, futures, timer);
        } else {
            testNonRetriable(futures);
            assertEmptyPendingRequests(commitRequestManger);
        }

        assertCoordinatorDisconnect(error);
    }

    @ParameterizedTest
    @MethodSource("offsetCommitExceptionSupplier")
    public void testOffsetCommitRequestErroredRequestsNotRetriedForAsyncCommit(final Errors error) {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        // Send commit request without expiration (async commit not expected to be retried).
        CompletableFuture<Void> commitResult = commitRequestManger.addOffsetCommitRequest(offsets, Optional.empty(), false);
        Timer timer = time.timer(defaultApiTimeoutMs);
        completeOffsetCommitRequestWithError(commitRequestManger, error, timer);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(timer.currentTimeMs());
        assertEquals(0, res.unsentRequests.size());
        assertTrue(commitResult.isDone());
        assertTrue(commitResult.isCompletedExceptionally());
        if (error.exception() instanceof RetriableException) {
            assertFutureThrows(commitResult, RetriableCommitFailedException.class);
        }

        // We expect that the request should not have been retried on this async commit.
        assertExceptionHandling(commitRequestManger, error, false, timer);
        assertCoordinatorDisconnect(error);
    }


    @Test
    public void testAsyncOffsetCommitThrowsRetriableCommitExceptionForUnhandledRetriable() {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        // Send commit request without expiration (async commit) that fails with retriable
        // network exception that has no specific handling. Should fail with
        // RetriableCommitException.
        CompletableFuture<Void> commitResult = commitRequestManger.addOffsetCommitRequest(offsets, Optional.empty(), false);
        Timer timer = time.timer(defaultApiTimeoutMs);
        completeOffsetCommitRequestWithError(commitRequestManger, Errors.NETWORK_EXCEPTION, timer);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(timer.currentTimeMs());
        assertEquals(0, res.unsentRequests.size());
        assertTrue(commitResult.isDone());
        assertTrue(commitResult.isCompletedExceptionally());
        assertFutureThrows(commitResult, RetriableCommitFailedException.class);
    }

    @Test
    public void testOffsetCommitSyncTimeoutNotReturnedOnPollAndFails() {
        CommitRequestManager commitRequestManger = create(false, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
            new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        // Send sync offset commit request that fails with retriable error.
        Timer timer = time.timer(retryBackoffMs * 2);
        CompletableFuture<Void> commitResult = commitRequestManger.addOffsetCommitRequest(offsets, Optional.of(timer), false);
        completeOffsetCommitRequestWithError(commitRequestManger, Errors.REQUEST_TIMED_OUT, timer);

        // Request retried after backoff, and fails with retriable again. Should not complete yet
        // given that the request timeout hasn't expired.
        timer.sleep(retryBackoffMs);
        completeOffsetCommitRequestWithError(commitRequestManger, Errors.REQUEST_TIMED_OUT, timer);
        assertFalse(commitResult.isDone());

        // Sleep to expire the request timeout. Request should fail on the next poll.
        timer.sleep(retryBackoffMs);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(timer.currentTimeMs());
        assertEquals(0, res.unsentRequests.size());
        assertTrue(commitResult.isDone());
        assertTrue(commitResult.isCompletedExceptionally());
    }

    /**
     * Sync commit requests that fail with a retriable error continue to be retry while there is
     * time. When time expires, they should fail with a TimeoutException.
     */
    @Test
    public void testOffsetCommitSyncFailedWithRetriableThrowsTimeoutWhenRetryTimeExpires() {
        CommitRequestManager commitRequestManger = create(false, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
            new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        // Send offset commit request that fails with retriable error.
        Timer timer = time.timer(retryBackoffMs * 2);
        CompletableFuture<Void> commitResult = commitRequestManger.addOffsetCommitRequest(offsets, Optional.of(timer), false);
        completeOffsetCommitRequestWithError(commitRequestManger, Errors.COORDINATOR_NOT_AVAILABLE, timer);

        // Sleep to expire the request timeout. Request should fail on the next poll with a
        // TimeoutException.
        timer.sleep(timer.remainingMs());
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(timer.currentTimeMs());
        assertEquals(0, res.unsentRequests.size());
        assertTrue(commitResult.isDone());
        assertTrue(commitResult.isCompletedExceptionally());
        Throwable t = assertThrows(ExecutionException.class, () -> commitResult.get());
        assertEquals(TimeoutException.class, t.getCause().getClass());
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
        CompletableFuture<Void> commitResult = commitRequestManger.addOffsetCommitRequest(offsets, Optional.empty(), false);
        Timer timer = time.timer(defaultApiTimeoutMs);
        completeOffsetCommitRequestWithError(commitRequestManger, retriableError, timer);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(timer.currentTimeMs());
        assertEquals(0, res.unsentRequests.size());
        assertTrue(commitResult.isDone());
        assertTrue(commitResult.isCompletedExceptionally());

        // We expect that the request should not have been retried on this async commit.
        assertExceptionHandling(commitRequestManger, retriableError, false, timer);

        // Request should have been completed with a RetriableCommitException
        Throwable t = assertThrows(ExecutionException.class, () -> commitResult.get());
        assertEquals(RetriableCommitFailedException.class, t.getCause().getClass());
        assertEquals(retriableError.exception().getClass(), t.getCause().getCause().getClass());
    }


    @Test
    public void testEnsureBackoffRetryOnOffsetCommitRequestTimeout() {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        Timer timer = time.timer(defaultApiTimeoutMs);
        commitRequestManger.addOffsetCommitRequest(offsets, Optional.of(timer), false);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(timer.currentTimeMs());
        assertEquals(1, res.unsentRequests.size());
        res.unsentRequests.get(0).handler().onFailure(timer.currentTimeMs(), new TimeoutException());

        assertTrue(commitRequestManger.pendingRequests.hasUnsentRequests());
        assertEquals(1, commitRequestManger.unsentOffsetCommitRequests().size());
        assertRetryBackOff(commitRequestManger, retryBackoffMs, timer);
    }

    private void assertCoordinatorDisconnect(final Errors error) {
        if (error.exception() instanceof DisconnectException) {
            verify(coordinatorRequestManager).markCoordinatorUnknown(any(), any());
        }
    }

    private void assertExceptionHandling(CommitRequestManager commitRequestManger, Errors errors,
                                         boolean requestShouldBeRetried, Timer timer) {
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
                assertPollDoesNotReturn(commitRequestManger, remainBackoffMs, timer);
                break;
            case UNKNOWN_TOPIC_OR_PARTITION:
            case COORDINATOR_LOAD_IN_PROGRESS:
                if (requestShouldBeRetried) {
                    assertRetryBackOff(commitRequestManger, remainBackoffMs, timer);
                }
                break;
            case GROUP_AUTHORIZATION_FAILED:
                // failed
                break;
            case TOPIC_AUTHORIZATION_FAILED:
            case OFFSET_METADATA_TOO_LARGE:
            case INVALID_COMMIT_OFFSET_SIZE:
                assertPollDoesNotReturn(commitRequestManger, Long.MAX_VALUE, timer);
                break;
            case FENCED_INSTANCE_ID:
                // This is a fatal failure, so we should not retry
                assertPollDoesNotReturn(commitRequestManger, Long.MAX_VALUE, timer);
                break;
            default:
                if (errors.exception() instanceof RetriableException && requestShouldBeRetried) {
                    assertRetryBackOff(commitRequestManger, remainBackoffMs, timer);
                } else {
                    assertPollDoesNotReturn(commitRequestManger, Long.MAX_VALUE, timer);
                }
        }
    }

    private void assertPollDoesNotReturn(CommitRequestManager commitRequestManager, long assertNextPollMs, Timer timer) {
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(timer.currentTimeMs());
        assertEquals(0, res.unsentRequests.size());
        assertEquals(assertNextPollMs, res.timeUntilNextPollMs);
    }

    private void assertRetryBackOff(CommitRequestManager commitRequestManager, long retryBackoffMs, Timer timer) {
        assertPollDoesNotReturn(commitRequestManager, retryBackoffMs, timer);
        timer.sleep(retryBackoffMs - 1);
        assertPollDoesNotReturn(commitRequestManager, 1, timer);
        timer.sleep(1);
        assertPoll(1, commitRequestManager, timer);
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
        Timer timer = time.timer(defaultApiTimeoutMs);
        commitRequestManager.addOffsetFetchRequest(partitions, timer);

        // Mock member has new a valid epoch.
        int newEpoch = 8;
        String memberId = "member1";
        commitRequestManager.onMemberEpochUpdated(Optional.of(newEpoch), Optional.of(memberId));

        // Receive error when member already has a newer member epoch. Request should be retried.
        completeOffsetFetchRequestWithError(commitRequestManager, partitions, Errors.STALE_MEMBER_EPOCH, timer);

        // Check that the request that failed was removed from the inflight requests buffer.
        assertEquals(0, commitRequestManager.pendingRequests.inflightOffsetFetches.size());
        assertEquals(1, commitRequestManager.pendingRequests.unsentOffsetFetches.size());

        // Request should be retried with backoff.
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(timer.currentTimeMs());
        assertEquals(0, res.unsentRequests.size());
        timer.sleep(retryBackoffMs);
        res = commitRequestManager.poll(timer.currentTimeMs());
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
        Timer timer = time.timer(defaultApiTimeoutMs);
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> requestResult =
            commitRequestManager.addOffsetFetchRequest(partitions, timer);

        // Mock member not having a valid epoch anymore (left/failed/fenced).
        commitRequestManager.onMemberEpochUpdated(Optional.empty(), Optional.empty());

        // Receive error when member is not in the group anymore. Request should fail.
        completeOffsetFetchRequestWithError(commitRequestManager, partitions, Errors.STALE_MEMBER_EPOCH, timer);

        assertTrue(requestResult.isDone());
        assertTrue(requestResult.isCompletedExceptionally());

        // No new request should be generated on the next poll.
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(timer.currentTimeMs());
        assertEquals(0, res.unsentRequests.size());
    }

    // This should be the case where the OffsetCommit expected to be retried on
    // STALE_MEMBER_EPOCH (ex. triggered from the reconciliation process), fails with invalid
    // member epoch. The expectation is that if the member already has a new epoch, the request
    // should be retried with the new epoch.
    @Test
    public void testOffsetCommitFailsWithStaleEpochAndRetriesWithNewEpoch() {
        CommitRequestManager commitRequestManager = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        // Send commit request expected to be retried on STALE_MEMBER_EPOCH.
        // errors while it does not expire).
        Timer timer = time.timer(defaultApiTimeoutMs);
        commitRequestManager.addOffsetCommitRequest(offsets, Optional.of(timer), true);

        // Mock member has new a valid epoch.
        int newEpoch = 8;
        String memberId = "member1";
        commitRequestManager.onMemberEpochUpdated(Optional.of(newEpoch), Optional.of(memberId));

        completeOffsetCommitRequestWithError(commitRequestManager, Errors.STALE_MEMBER_EPOCH, timer);

        // Check that the request that failed was removed from the inflight requests buffer.
        assertEquals(0, commitRequestManager.pendingRequests.inflightOffsetFetches.size());
        assertEquals(1, commitRequestManager.pendingRequests.unsentOffsetCommits.size());

        // Request should be retried with backoff.
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(timer.currentTimeMs());
        assertEquals(0, res.unsentRequests.size());
        timer.sleep(retryBackoffMs);
        res = commitRequestManager.poll(timer.currentTimeMs());
        assertEquals(1, res.unsentRequests.size());

        // The retried request should include the latest member ID and epoch.
        OffsetCommitRequestData reqData = (OffsetCommitRequestData) res.unsentRequests.get(0).requestBuilder().build().data();
        assertEquals(newEpoch, reqData.generationIdOrMemberEpoch());
        assertEquals(memberId, reqData.memberId());
    }

    @Test
    public void testEnsureCommitSensorRecordsMetric() {
        CommitRequestManager commitRequestManager = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Timer timer = time.timer(defaultApiTimeoutMs);
        commitOffsetWithAssertedLatency(commitRequestManager, 100, timer);
        commitOffsetWithAssertedLatency(commitRequestManager, 101, timer);

        assertEquals(100.5, getMetric("commit-latency-avg").metricValue()); // 201 / 2
        assertEquals(101.0, getMetric("commit-latency-max").metricValue()); // Math.max(101, 100)
        assertEquals(0.066, (double) getMetric("commit-rate").metricValue(), 0.001);
        assertEquals(2.0, getMetric("commit-total").metricValue());
    }

    private void commitOffsetWithAssertedLatency(CommitRequestManager commitRequestManager, long latencyMs, Timer timer) {
        final String topic = "topic";
        final int partition = 1;
        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
                new TopicPartition(topic, partition),
                new OffsetAndMetadata(0));

        long commitCreationTimeMs = timer.currentTimeMs();
        commitRequestManager.addOffsetCommitRequest(offsets, Optional.empty(), true);

        NetworkClientDelegate.PollResult res = commitRequestManager.poll(timer.currentTimeMs());
        assertEquals(1, res.unsentRequests.size());

        timer.sleep(latencyMs);
        long commitReceivedTimeMs = timer.currentTimeMs();
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
                                                     Errors error,
                                                     Timer timer) {
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(timer.currentTimeMs());
        assertEquals(1, res.unsentRequests.size());
        res.unsentRequests.get(0).future().complete(buildOffsetFetchClientResponse(res.unsentRequests.get(0), partitions, error));
    }

    private void completeOffsetCommitRequestWithError(CommitRequestManager commitRequestManager,
                                                      Errors error,
                                                      Timer timer) {
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(timer.currentTimeMs());
        assertEquals(1, res.unsentRequests.size());
        res.unsentRequests.get(0).future().complete(mockOffsetCommitResponse("topic", 1, (short) 1, error, timer));
    }

    private void testRetriable(final CommitRequestManager commitRequestManger,
                               final List<CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>> futures,
                               final Timer timer) {
        futures.forEach(f -> assertFalse(f.isDone()));

        // The manager should backoff for 100ms
        timer.sleep(100);
        commitRequestManger.poll(timer.currentTimeMs());
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
            Arguments.of(Errors.NETWORK_EXCEPTION),
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
            Arguments.of(Errors.COORDINATOR_NOT_AVAILABLE, false),
            Arguments.of(Errors.REQUEST_TIMED_OUT, false),
            Arguments.of(Errors.FENCED_INSTANCE_ID, false),
            Arguments.of(Errors.TOPIC_AUTHORIZATION_FAILED, false),
            Arguments.of(Errors.UNKNOWN_MEMBER_ID, false),
            // Adding STALE_MEMBER_EPOCH as non-retriable here because it is only retried if a new
            // member epoch is received. Tested separately.
            Arguments.of(Errors.STALE_MEMBER_EPOCH, false));
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
        Timer timer = time.timer(defaultApiTimeoutMs);
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future =
                commitRequestManger.addOffsetFetchRequest(partitions, timer);

        NetworkClientDelegate.PollResult res = commitRequestManger.poll(timer.currentTimeMs());
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
            testRetriable(commitRequestManger, Collections.singletonList(future), timer);
        else
            testNonRetriable(Collections.singletonList(future));
    }

    @Test
    public void testSignalClose() {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        commitRequestManger.addOffsetCommitRequest(offsets, Optional.empty(), false);
        commitRequestManger.signalClose();
        Timer timer = time.timer(defaultApiTimeoutMs);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(timer.currentTimeMs());
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
        Timer timer = time.timer(defaultApiTimeoutMs);
        for (int i = 0; i < numRequest; i++) {
            futures.add(commitRequestManger.addOffsetFetchRequest(partitions, timer));
        }

        NetworkClientDelegate.PollResult res = commitRequestManger.poll(timer.currentTimeMs());
        assertEquals(1, res.unsentRequests.size());
        res.unsentRequests.get(0).handler().onComplete(buildOffsetFetchClientResponse(res.unsentRequests.get(0),
            partitions, error));
        res = commitRequestManger.poll(timer.currentTimeMs());
        assertEquals(0, res.unsentRequests.size());
        return futures;
    }

    private void sendAndVerifyOffsetCommitRequestFailedAndMaybeRetried(
        final CommitRequestManager commitRequestManger,
        final Errors error,
        final CompletableFuture<Void> commitResult,
        final Timer timer) {
        completeOffsetCommitRequestWithError(commitRequestManger, error, timer);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(timer.currentTimeMs());
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
        final CommitRequestManager manager,
        final Timer timer) {
        return assertPoll(true, numRes, manager, timer);
    }

    private List<NetworkClientDelegate.FutureCompletionHandler> assertPoll(
        final boolean coordinatorDiscovered,
        final int numRes,
        final CommitRequestManager manager,
        final Timer timer) {
        if (coordinatorDiscovered) {
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        } else {
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.empty());
        }
        NetworkClientDelegate.PollResult res = manager.poll(timer.currentTimeMs());
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
                DEFAULT_GROUP_ID,
                Optional.of(DEFAULT_GROUP_INSTANCE_ID),
                retryBackoffMs,
                retryBackoffMaxMs,
                OptionalDouble.of(0),
                defaultApiTimeoutMs,
                consumerMetricGroupPrefix,
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
        return buildOffsetFetchClientResponse(request, topicPartitionData, error);
    }

    private ClientResponse buildOffsetCommitClientResponse(final OffsetCommitResponse commitResponse,
                                                           final Timer timer) {
        short apiVersion = 1;
        return new ClientResponse(
            new RequestHeader(ApiKeys.OFFSET_COMMIT, apiVersion, "", 1),
            null,
            "-1",
            timer.currentTimeMs(),
            timer.currentTimeMs(),
            false,
            null,
            null,
            commitResponse
        );
    }


    public ClientResponse mockOffsetCommitResponse(String topic,
                                                   int partition,
                                                   short apiKeyVersion,
                                                   Errors error,
                                                   Timer timer) {
        return mockOffsetCommitResponse(topic, partition, apiKeyVersion, timer.currentTimeMs(), timer.currentTimeMs(), error);
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
                request.timer().currentTimeMs(),
                request.timer().currentTimeMs(),
                false,
                null,
                null,
                response
        );
    }

    private KafkaMetric getMetric(String name) {
        return metrics.metrics().get(metrics.metricName(
            name,
            consumerMetricGroupName));
    }
}
