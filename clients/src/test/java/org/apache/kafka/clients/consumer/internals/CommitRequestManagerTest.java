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
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
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
    private LogContext logContext;
    private MockTime time;
    private CoordinatorRequestManager coordinatorRequestManager;
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
    public void testPoll_SkipIfCoordinatorUnknown() {
        CommitRequestManager commitRequestManger = create(false, 0);
        assertPoll(false, 0, commitRequestManger);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("t1", 0), new OffsetAndMetadata(0));
        commitRequestManger.addOffsetCommitRequest(offsets, Optional.empty());
        assertPoll(false, 0, commitRequestManger);
    }

    @Test
    public void testPoll_EnsureManualCommitSent() {
        CommitRequestManager commitRequestManger = create(false, 0);
        assertPoll(0, commitRequestManger);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("t1", 0), new OffsetAndMetadata(0));
        commitRequestManger.addOffsetCommitRequest(offsets, Optional.empty());
        assertPoll(1, commitRequestManger);
    }

    @Test
    public void testPoll_EnsureAutocommitSent() {
        TopicPartition tp = new TopicPartition("t1", 1);
        subscriptionState.assignFromUser(Collections.singleton(tp));
        subscriptionState.seek(tp, 100);
        CommitRequestManager commitRequestManger = create(true, 100);
        assertPoll(0, commitRequestManger);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(0));
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        time.sleep(100);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        List<NetworkClientDelegate.FutureCompletionHandler> pollResults = assertPoll(1, commitRequestManger);
        pollResults.forEach(v -> v.onComplete(mockOffsetCommitResponse(
                "t1",
                1,
                (short) 1,
                Errors.NONE)));
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
        Timer timer = time.timer(defaultApiTimeoutMs);
        commitFutures.add(commitManager.addOffsetCommitRequest(offsets1, Optional.of(timer)));
        fetchFutures.add(commitManager.addOffsetFetchRequest(Collections.singleton(new TopicPartition("test", 0)), timer));
        commitFutures.add(commitManager.addOffsetCommitRequest(offsets2, Optional.of(timer)));
        fetchFutures.add(commitManager.addOffsetFetchRequest(Collections.singleton(new TopicPartition("test", 1)), timer));

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
        commitRequestManger.addOffsetCommitRequest(new HashMap<>(), Optional.empty());
        assertEquals(1, commitRequestManger.unsentOffsetCommitRequests().size());
        assertEquals(1, commitRequestManger.poll(time.milliseconds()).unsentRequests.size());
        assertTrue(commitRequestManger.unsentOffsetCommitRequests().isEmpty());
        assertEmptyPendingRequests(commitRequestManger);
    }

    // This is the case of the async auto commit sent on calls to assign (async commit that
    // should not be retried).
    @Test
    public void testAsyncAutocommit_NotRetriedAfterException() {
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

    // This is the case of the sync auto commit sent when the consumer is being closed (sync commit
    // that should be retried until it succeeds, fails, or timer expires).
    @ParameterizedTest
    @MethodSource("offsetCommitExceptionSupplier")
    public void testSyncAutocommit_RetriedAfterRetriableException(Errors error) {
        long commitInterval = retryBackoffMs * 2;
        CommitRequestManager commitRequestManger = create(true, commitInterval);
        TopicPartition tp = new TopicPartition("topic", 1);
        subscriptionState.assignFromUser(Collections.singleton(tp));
        subscriptionState.seek(tp, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));
        time.sleep(commitInterval);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());

        // Auto-commit all consume sync (ex. triggered when the consumer is closed).
        Timer timer = time.timer(defaultApiTimeoutMs);
        CompletableFuture<Void> commitResult = commitRequestManger.maybeAutoCommitAllConsumed(Optional.of(timer));
        sendAndVerifyOffsetCommitRequestFailedAndMaybeRetried(commitRequestManger, timer, error, commitResult);

        // We expect that request should have been retried on this sync commit.
        assertExceptionHandling(commitRequestManger, error, true);
        assertCoordinatorDisconnect(error);
    }

    @Test
    public void testAutocommit_EnsureOnlyOneInflightRequest() {
        TopicPartition t1p = new TopicPartition("topic1", 0);
        subscriptionState.assignFromUser(singleton(t1p));

        CommitRequestManager commitRequestManger = create(true, 100);
        time.sleep(100);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        // Nothing consumed therefore no commit request is sent
        assertPoll(0, commitRequestManger);
        time.sleep(10);
        subscriptionState.seekUnvalidated(t1p, new SubscriptionState.FetchPosition(100L));
        List<NetworkClientDelegate.FutureCompletionHandler> futures = assertPoll(1, commitRequestManger);

        time.sleep(90);
        commitRequestManger.updateAutoCommitTimer(time.milliseconds());
        // We want to make sure we don't resend autocommit if the previous request has not been completed
        assertPoll(0, commitRequestManger);
        assertEmptyPendingRequests(commitRequestManger);

        // complete the unsent request and re-poll
        futures.get(0).onComplete(buildOffsetCommitClientResponse(new OffsetCommitResponse(0, new HashMap<>())));
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
    public void testOffsetCommitRequest_ErroredRequestsNotRetriedForAsyncCommit(final Errors error) {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        // Send commit request without expiration (async commit not expected to be retried).
        CompletableFuture<Void> commitResult = commitRequestManger.addOffsetCommitRequest(offsets, Optional.empty());
        completeOffsetCommitRequestWithError(commitRequestManger, error);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        assertTrue(commitResult.isDone());
        assertTrue(commitResult.isCompletedExceptionally());

        // We expect that the request should not have been retried on this async commit.
        assertExceptionHandling(commitRequestManger, error, false);
        assertCoordinatorDisconnect(error);
    }

    @Test
    public void testOffsetCommitSyncTimeoutNotReturnedOnPollAndFails() {
        CommitRequestManager commitRequestManger = create(false, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        // Send sync offset commit request that fails with retriable error.
        Timer requestTimer = time.timer(retryBackoffMs * 2);
        CompletableFuture<Void> commitResult = commitRequestManger.addOffsetCommitRequest(offsets, Optional.of(requestTimer));
        completeOffsetCommitRequestWithError(commitRequestManger, Errors.REQUEST_TIMED_OUT);

        // Request retried after backoff, and fails with retriable again. Should not complete yet
        // given that the request timeout hasn't expired.
        requestTimer.sleep(retryBackoffMs);
        completeOffsetCommitRequestWithError(commitRequestManger, Errors.REQUEST_TIMED_OUT);
        assertFalse(commitResult.isDone());

        // Sleep to expire the request timeout. Request should fail on the next poll.
        requestTimer.sleep(retryBackoffMs);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        assertTrue(commitResult.isDone());
        assertTrue(commitResult.isCompletedExceptionally());
    }

    @Test
    public void testEnsureBackoffRetryOnOffsetCommitRequestTimeout() {
        CommitRequestManager commitRequestManger = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        commitRequestManger.addOffsetCommitRequest(offsets, Optional.of(time.timer(defaultApiTimeoutMs)));
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

    // This should be the case where the OffsetFetch fails with invalid member ID/epoch and the
    // member already has a new ID/epoch (ex. when member just joined the group or got a new
    // epoch after a reconciliation). The request should just be retried with the new ID/epoch
    // and succeed.
    @ParameterizedTest
    @MethodSource("retriableGroupErrors")
    public void testSyncOffsetFetchFailsWithGroupErrorsAndRetriesWithNewMemberIdAndEpoch(final Errors error) {
        CommitRequestManager commitRequestManager = create(false, 100);
        Set<TopicPartition> partitions = Collections.singleton(new TopicPartition("t1", 0));
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        // Send request that is expected to fail with invalid ID/epoch.
        commitRequestManager.addOffsetFetchRequest(partitions, time.timer(defaultApiTimeoutMs));

        // Mock member has new a valid epoch.
        int newEpoch = 8;
        String memberId = "member1";
        commitRequestManager.onMemberEpochUpdated(Optional.of(newEpoch), Optional.of(memberId));

        // Receive error when member already has a newer member ID/epoch. Request should be retried.
        completeOffsetFetchRequestWithError(commitRequestManager, partitions, error);

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
    @ParameterizedTest
    @MethodSource("retriableGroupErrors")
    public void testSyncOffsetFetchFailsWithGroupErrorsAndNotRetriedIfMemberNotInGroupAnymore(final Errors error) {
        CommitRequestManager commitRequestManager = create(false, 100);
        Set<TopicPartition> partitions = Collections.singleton(new TopicPartition("t1", 0));
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        // Send request that is expected to fail with invalid ID/epoch.
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> requestResult =
            commitRequestManager.addOffsetFetchRequest(partitions, time.timer(defaultApiTimeoutMs));

        // Mock member not having a valid epoch anymore (left/failed/fenced).
        commitRequestManager.onMemberEpochUpdated(Optional.empty(), Optional.empty());

        // Receive error when member is not in the group anymore. Request should fail.
        completeOffsetFetchRequestWithError(commitRequestManager, partitions, error);

        assertTrue(requestResult.isDone());
        assertTrue(requestResult.isCompletedExceptionally());

        // No new request should be generated on the next poll.
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
    }

    // This should be the case where the OffsetCommit fails with invalid member ID/epoch and the
    // member already has a new ID/epoch (ex. when member just joined the group or got a new
    // epoch after a reconciliation). The request should just be retried with the new ID/epoch
    // and succeed.
    @ParameterizedTest
    @MethodSource("retriableGroupErrors")
    public void testSyncOffsetCommitFailsWithGroupErrorsAndRetriesWithNewMemberIdAndEpoch(final Errors error) {
        CommitRequestManager commitRequestManager = create(true, 100);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mockedNode));

        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(new TopicPartition("topic", 1),
            new OffsetAndMetadata(0));

        // Send commit with a timer not expired (sync commit expected to be retried on retriable
        // errors while timer is not expired).
        Timer timer = time.timer(defaultApiTimeoutMs);
        commitRequestManager.addOffsetCommitRequest(offsets, Optional.of(timer));

        // Mock member has new a valid epoch.
        int newEpoch = 8;
        String memberId = "member1";
        commitRequestManager.onMemberEpochUpdated(Optional.of(newEpoch), Optional.of(memberId));

        completeOffsetCommitRequestWithError(commitRequestManager, error);

        // Check that the request that failed was removed from the inflight requests buffer.
        assertEquals(0, commitRequestManager.pendingRequests.inflightOffsetFetches.size());
        assertEquals(1, commitRequestManager.pendingRequests.unsentOffsetCommits.size());

        // Request should be retried with backoff.
        NetworkClientDelegate.PollResult res = commitRequestManager.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        time.sleep(retryBackoffMs);
        res = commitRequestManager.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());

        // The retried request should include the latest member ID and epoch.
        OffsetCommitRequestData reqData = (OffsetCommitRequestData) res.unsentRequests.get(0).requestBuilder().build().data();
        assertEquals(newEpoch, reqData.generationIdOrMemberEpoch());
        assertEquals(memberId, reqData.memberId());
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

    // Supplies (error, isRetriable)
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

    // Errors that should be retried only if a new member ID/epoch has been received.
    private static Stream<Arguments> retriableGroupErrors() {
        return Stream.of(
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
            // Adding STALE_MEMBER_EPOCH and UNKNOWN_MEMBER_ID as non-retriable here
            // because they are only retried if a new member ID/epoch is
            // received. Tested separately.
            Arguments.of(Errors.STALE_MEMBER_EPOCH, false),
            Arguments.of(Errors.UNKNOWN_MEMBER_ID, false));
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
                commitRequestManger.addOffsetFetchRequest(partitions, time.timer(defaultApiTimeoutMs));

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
        Timer timer = time.timer(defaultApiTimeoutMs);
        for (int i = 0; i < numRequest; i++) {
            futures.add(commitRequestManger.addOffsetFetchRequest(partitions, timer));
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
        final Timer timer,
        final Errors error,
        final CompletableFuture<Void> commitResult) {
        completeOffsetCommitRequestWithError(commitRequestManger, error);
        NetworkClientDelegate.PollResult res = commitRequestManger.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        if (timer.isExpired() || !(error.exception() instanceof RetriableException)) {
            // Commit should fail if the timer expired or the error is not retriable.
            assertTrue(commitResult.isDone());
            assertTrue(commitResult.isCompletedExceptionally());
        } else {
            // Commit should not complete if the timer is still valid and the error is retriable.
            assertFalse(commitResult.isDone());
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
                DEFAULT_GROUP_ID,
                Optional.of(DEFAULT_GROUP_INSTANCE_ID),
                retryBackoffMs,
                retryBackoffMaxMs,
                OptionalDouble.of(0)));
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
