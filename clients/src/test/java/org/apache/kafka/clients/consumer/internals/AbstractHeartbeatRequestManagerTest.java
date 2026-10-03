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
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.internals.LogContext;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Base test class for {@link AbstractHeartbeatRequestManager}. Tests defined here exercise
 * behavior implemented in the abstract manager and must produce the same outcome for every
 * concrete subclass.
 */
abstract class AbstractHeartbeatRequestManagerTest<R extends AbstractResponse> {

    protected static final String DEFAULT_GROUP_ID = "groupId";
    protected static final String DEFAULT_MEMBER_ID = "member-id";
    protected static final int DEFAULT_MEMBER_EPOCH = 1;
    protected static final int DEFAULT_HEARTBEAT_INTERVAL_MS = 1000;
    protected static final int DEFAULT_MAX_POLL_INTERVAL_MS = 10000;
    protected static final long DEFAULT_RETRY_BACKOFF_MS = 80;
    protected static final long DEFAULT_RETRY_BACKOFF_MAX_MS = 1000;
    protected static final double DEFAULT_HEARTBEAT_JITTER_MS = 0.0;

    protected Time time;
    protected LogContext logContext;
    protected Timer pollTimer;
    protected CoordinatorRequestManager coordinatorRequestManager;
    protected SubscriptionState subscriptions;
    protected BackgroundEventHandler backgroundEventHandler;
    protected HeartbeatRequestState heartbeatRequestState;
    protected AbstractMembershipManager<R> membershipManager;
    protected AbstractHeartbeatRequestManager<R> heartbeatRequestManager;
    protected Metrics metrics;

    protected final Class<R> responseClass;

    protected AbstractHeartbeatRequestManagerTest(Class<R> responseClass) {
        this.responseClass = responseClass;
    }

    protected abstract ClientResponse createHeartbeatResponse(
        NetworkClientDelegate.UnsentRequest request, Errors error);

    protected abstract ClientResponse createHeartbeatResponse(
        NetworkClientDelegate.UnsentRequest request, Errors error, int heartbeatIntervalMs);

    protected abstract void recreateHeartbeatRequestManager();

    protected abstract void verifyHeartbeatStateReset();

    protected abstract String metricGroupName();

    protected void createHeartbeatRequestStateWithZeroHeartbeatInterval() {
        createHeartbeatRequestStateWithHeartbeatInterval(0);
    }

    protected void createHeartbeatRequestStateWithHeartbeatInterval(final long heartbeatIntervalMs) {
        heartbeatRequestState = spy(new HeartbeatRequestState(
            logContext,
            time,
            heartbeatIntervalMs,
            DEFAULT_RETRY_BACKOFF_MS,
            DEFAULT_RETRY_BACKOFF_MAX_MS,
            DEFAULT_HEARTBEAT_JITTER_MS)
        );

        recreateHeartbeatRequestManager();
    }

    @Test
    public void testTimerNotDue() {
        time.sleep(100); // before heartbeatInterval, no heartbeat should be sent
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

        assertEquals(0, result.unsentRequests.size());
        assertEquals(DEFAULT_HEARTBEAT_INTERVAL_MS - 100, result.timeUntilNextPollMs);
        assertEquals(DEFAULT_HEARTBEAT_INTERVAL_MS - 100, heartbeatRequestManager.maximumTimeToWait(time.milliseconds()));

        // Member in state where it should not send Heartbeat anymore
        when(subscriptions.hasAutoAssignedPartitions()).thenReturn(true);
        when(membershipManager.shouldSkipHeartbeat()).thenReturn(true);
        result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(Long.MAX_VALUE, result.timeUntilNextPollMs);
    }

    @Test
    public void testHeartbeatOutsideInterval() {
        when(membershipManager.shouldSkipHeartbeat()).thenReturn(false);
        when(membershipManager.shouldHeartbeatNow()).thenReturn(true);
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

        assertEquals(1, result.unsentRequests.size());
        assertEquals(DEFAULT_HEARTBEAT_INTERVAL_MS, result.timeUntilNextPollMs);
        assertEquals(DEFAULT_HEARTBEAT_INTERVAL_MS, heartbeatRequestManager.maximumTimeToWait(time.milliseconds()));
        verify(membershipManager).onHeartbeatRequestGenerated();
    }

    @Test
    public void testNoCoordinator() {
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.empty());
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

        assertEquals(Long.MAX_VALUE, result.timeUntilNextPollMs);
        assertEquals(DEFAULT_RETRY_BACKOFF_MS, heartbeatRequestManager.maximumTimeToWait(time.milliseconds()));
        assertEquals(0, result.unsentRequests.size());
    }

    /**
     * This is expected to be the case where a member is already leaving the group and the
     * poll timer expires. The poll timer expiration should not transition the member to
     * STALE, and the member should continue to send heartbeats while the ongoing leaving
     * operation completes (send heartbeats while waiting for callbacks before leaving, or
     * send last heartbeat to leave).
     */
    @Test
    public void testPollTimerExpirationShouldNotMarkMemberStaleIfMemberAlreadyLeaving() {
        when(membershipManager.shouldSkipHeartbeat()).thenReturn(false);
        when(membershipManager.isLeavingGroup()).thenReturn(true);

        time.sleep(DEFAULT_MAX_POLL_INTERVAL_MS);
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

        // No transition to leave due to stale member should be triggered, because the member
        // is already leaving the group.
        verify(membershipManager, never()).transitionToSendingLeaveGroup(anyBoolean());

        assertEquals(1, result.unsentRequests.size(), "A heartbeat request should be generated to" +
            " complete the ongoing leaving operation that was triggered before the poll timer expired.");
    }

    @Test
    public void testSuccessfulHeartbeatTiming() {
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(0, result.unsentRequests.size(),
            "No heartbeat should be sent while interval has not expired");
        assertEquals(heartbeatRequestState.timeToNextHeartbeatMs(time.milliseconds()), result.timeUntilNextPollMs);
        assertNextHeartbeatTiming(DEFAULT_HEARTBEAT_INTERVAL_MS);

        result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size(), "A heartbeat should be sent when interval expires");
        NetworkClientDelegate.UnsentRequest inflightReq = result.unsentRequests.get(0);
        assertEquals(DEFAULT_HEARTBEAT_INTERVAL_MS,
            heartbeatRequestState.timeToNextHeartbeatMs(time.milliseconds()),
            "Heartbeat timer was not reset to the interval when the heartbeat request was sent.");

        long partOfInterval = DEFAULT_HEARTBEAT_INTERVAL_MS / 3;
        time.sleep(partOfInterval);
        result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(0, result.unsentRequests.size(),
            "No heartbeat should be sent while only part of the interval has passed");
        assertEquals(DEFAULT_HEARTBEAT_INTERVAL_MS - partOfInterval,
            heartbeatRequestState.timeToNextHeartbeatMs(time.milliseconds()),
            "Time to next interval was not properly updated.");

        inflightReq.handler().onComplete(createHeartbeatResponse(inflightReq, Errors.NONE));
        assertNextHeartbeatTiming(DEFAULT_HEARTBEAT_INTERVAL_MS - partOfInterval);
    }

    @Test
    public void testLogsHeartbeatIntervalReceivedFromCoordinatorOnlyWhenChanged() {
        try (LogCaptureAppender logAppender =
                 LogCaptureAppender.createAndRegister(heartbeatRequestManager.getClass())) {
            logAppender.setClassLogger(heartbeatRequestManager.getClass(), Level.INFO);
            when(membershipManager.memberId()).thenReturn(DEFAULT_MEMBER_ID);

            int changedIntervalMs = DEFAULT_HEARTBEAT_INTERVAL_MS + 500;

            // A successful heartbeat whose interval differs from the current one is applied and logged.
            time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
            NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
            assertEquals(1, result.unsentRequests.size());
            result.unsentRequests.get(0).handler().onComplete(
                createHeartbeatResponse(result.unsentRequests.get(0), Errors.NONE, changedIntervalMs));

            assertEquals(changedIntervalMs, heartbeatRequestState.heartbeatIntervalMs());
            assertEquals(1, countHeartbeatIntervalLogs(logAppender),
                "The heartbeat interval received from the coordinator should be logged when it changes.");
            assertTrue(logAppender.getMessages().stream().anyMatch(message -> message.contains(
                    "Member " + DEFAULT_MEMBER_ID + " received heartbeat interval " + changedIntervalMs + "ms")),
                "The logged message should contain the member id and the received interval.");

            // A subsequent heartbeat carrying the same interval must not be logged again.
            time.sleep(changedIntervalMs);
            result = heartbeatRequestManager.poll(time.milliseconds());
            assertEquals(1, result.unsentRequests.size());
            result.unsentRequests.get(0).handler().onComplete(
                createHeartbeatResponse(result.unsentRequests.get(0), Errors.NONE, changedIntervalMs));

            assertEquals(1, countHeartbeatIntervalLogs(logAppender),
                "An unchanged heartbeat interval must not be logged again.");
        }
    }

    private static long countHeartbeatIntervalLogs(final LogCaptureAppender logAppender) {
        return logAppender.getMessages().stream()
            .filter(message -> message.contains("received heartbeat interval"))
            .count();
    }

    /**
     * Test that GROUP_ID_NOT_FOUND error while unsubscribed is not treated as fatal. This can
     * happen when the consumer never successfully joined the group (e.g., due to an
     * InvalidTopicException during poll() and close() sends a leave heartbeat for a group
     * that was never created).
     */
    @Test
    public void testGroupIdNotFoundExceptionWhileUnsubscribed() {
        when(membershipManager.state()).thenReturn(MemberState.UNSUBSCRIBED);
        when(membershipManager.memberEpoch()).thenReturn(-1);

        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());

        ClientResponse response = createHeartbeatResponse(result.unsentRequests.get(0), Errors.GROUP_ID_NOT_FOUND);
        result.unsentRequests.get(0).handler().onComplete(response);

        verify(membershipManager, never()).transitionToFatal();
        verify(membershipManager).onHeartbeatFailure(false);
        verify(backgroundEventHandler, never()).add(any());
    }

    /**
     * Test that GROUP_ID_NOT_FOUND error while stable is treated as fatal. This would indicate
     * the group was unexpectedly deleted while the member was actively participating.
     */
    @Test
    public void testGroupIdNotFoundWhileStableIsFatal() {
        when(membershipManager.state()).thenReturn(MemberState.STABLE);
        when(membershipManager.memberEpoch()).thenReturn(DEFAULT_MEMBER_EPOCH);

        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());

        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.empty());
        ClientResponse response = createHeartbeatResponse(result.unsentRequests.get(0), Errors.GROUP_ID_NOT_FOUND);
        result.unsentRequests.get(0).handler().onComplete(response);

        verify(membershipManager).transitionToFatal();
        verify(backgroundEventHandler).add(any());
    }

    @ParameterizedTest
    @MethodSource("errorProvider")
    public void testHeartbeatResponseOnErrorHandling(final Errors error, final boolean isFatal) {
        // Handling errors on the second heartbeat
        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());

        // Manually completing the response to test error handling
        when(subscriptions.hasAutoAssignedPartitions()).thenReturn(true);
        ClientResponse response = createHeartbeatResponse(
            result.unsentRequests.get(0),
            error);
        result.unsentRequests.get(0).handler().onComplete(response);
        R mockResponse = responseClass.cast(response.responseBody());

        assertHeartbeatErrorHandling(error, isFatal, mockResponse);
    }

    @Test
    public void testHeartbeatOnStartup() {
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(0, result.unsentRequests.size());

        createHeartbeatRequestStateWithZeroHeartbeatInterval();
        assertEquals(0, heartbeatRequestManager.maximumTimeToWait(time.milliseconds()));
        result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());

        // Ensure we do not resend the request without the first request being completed
        NetworkClientDelegate.PollResult result2 = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(0, result2.unsentRequests.size());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSkippingHeartbeat(final boolean shouldSkipHeartbeat) {
        // The initial heartbeatInterval is set to 0
        createHeartbeatRequestStateWithZeroHeartbeatInterval();

        // Mocking notInGroup
        when(membershipManager.shouldSkipHeartbeat()).thenReturn(shouldSkipHeartbeat);

        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

        if (!shouldSkipHeartbeat) {
            assertEquals(1, result.unsentRequests.size());
            assertEquals(0, result.timeUntilNextPollMs);
        } else {
            assertEquals(0, result.unsentRequests.size());
            assertEquals(Long.MAX_VALUE, result.timeUntilNextPollMs);
        }
    }

    /**
     * When the consumer uses manual partition assignment (assign()) instead of subscribe(), the
     * member stays in UNSUBSCRIBED state indefinitely. Because heartbeats are skipped in that
     * state and heartbeatIntervalMs initialises to 0, maximumTimeToWait used to return 0, causing
     * a busy-loop in pollForFetches. Verify that maximumTimeToWait returns Long.MAX_VALUE whenever
     * the member is in UNSUBSCRIBED state so the application thread can block for the full poll
     * timeout.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMaximumTimeToWaitWhenHeartbeatShouldBeSkipped(final boolean isUnsubscribed) {
        // Start with zero heartbeat interval (simulates the initial state before any HB response)
        createHeartbeatRequestStateWithZeroHeartbeatInterval();
        when(membershipManager.state()).thenReturn(isUnsubscribed ? MemberState.UNSUBSCRIBED : MemberState.JOINING);

        long result = heartbeatRequestManager.maximumTimeToWait(time.milliseconds());

        if (isUnsubscribed) {
            assertEquals(Long.MAX_VALUE, result,
                "maximumTimeToWait should return Long.MAX_VALUE when in UNSUBSCRIBED state " +
                    "(e.g., manual assignment) to prevent a busy loop");
        } else {
            assertEquals(0, result,
                "maximumTimeToWait should return 0 when heartbeat interval timer has already expired");
        }
    }

    /**
     * KAFKA-20253: when the coordinator is unavailable (e.g. after a re-authentication failure),
     * poll() returns EMPTY, so no heartbeat can be sent. maximumTimeToWait() must return a positive
     * value in that case; returning 0 busy-spins the application thread (and, via wakeups, the
     * consumer network thread), which is the AsyncKafkaConsumer high-CPU loop in this ticket.
     */
    @Test
    public void testMaximumTimeToWaitWhenCoordinatorUnavailableDoesNotSpin() {
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.empty());
        when(membershipManager.state()).thenReturn(MemberState.STABLE);
        when(membershipManager.shouldHeartbeatNow()).thenReturn(true);

        long result = heartbeatRequestManager.maximumTimeToWait(time.milliseconds());

        assertTrue(result > 0,
            "maximumTimeToWait must be > 0 when the coordinator is unavailable to avoid a busy-spin; got " + result);
        assertEquals(DEFAULT_RETRY_BACKOFF_MS, result);
    }

    /**
     * While bootstrap DNS resolution is still in progress the coordinator is unknown,
     * and a member that wants to join has a zero heartbeat interval, since the interval is only
     * learned from the first heartbeat response. maximumTimeToWait() must wait a retry backoff
     * rather than the (zero) heartbeat interval; returning 0 busy-spins the application and
     * network threads.
     */
    @Test
    public void testMaximumTimeToWaitWhenJoiningAndCoordinatorUnknownDoesNotSpin() {
        createHeartbeatRequestStateWithZeroHeartbeatInterval();
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.empty());
        when(membershipManager.state()).thenReturn(MemberState.JOINING);
        when(membershipManager.shouldHeartbeatNow()).thenReturn(true);

        long result = heartbeatRequestManager.maximumTimeToWait(time.milliseconds());

        assertTrue(result > 0, "maximumTimeToWait must be > 0 while the member is joining and the coordinator is unknown to avoid a busy-spin; got " + result);
        assertEquals(DEFAULT_RETRY_BACKOFF_MS, result);
    }

    @Test
    public void testMaximumTimeToWaitWhenFatalReturnsMaxValue() {
        when(membershipManager.state()).thenReturn(MemberState.FATAL);

        assertEquals(Long.MAX_VALUE, heartbeatRequestManager.maximumTimeToWait(time.milliseconds()),
            "maximumTimeToWait should return Long.MAX_VALUE in the terminal FATAL state");
    }

    @Test
    public void testMaximumTimeToWaitWhenFencedWaitsRetryBackoff() {
        createHeartbeatRequestStateWithZeroHeartbeatInterval();
        when(membershipManager.state()).thenReturn(MemberState.FENCED);
        when(membershipManager.shouldSkipHeartbeat()).thenReturn(true);

        long result = heartbeatRequestManager.maximumTimeToWait(time.milliseconds());

        assertTrue(result > 0, "maximumTimeToWait must be > 0 while the member is fenced to avoid a busy-spin; got " + result);
        assertEquals(DEFAULT_RETRY_BACKOFF_MS, result);
    }

    @Test
    public void testHeartbeatNotSentIfAnotherOneInFlight() {
        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);

        // Heartbeat sent (no response received)
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());
        NetworkClientDelegate.UnsentRequest inflightReq = result.unsentRequests.get(0);

        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(0, result.unsentRequests.size(), "No heartbeat should be sent while a " +
                "previous one is in-flight");

        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(0, result.unsentRequests.size(), "No heartbeat should be sent when the " +
                "interval expires if there is a previous HB request in-flight");

        // Receive response for the inflight after the interval expired. The next HB should be sent
        // on the next poll waiting only for the minimal backoff.
        inflightReq.handler().onComplete(createHeartbeatResponse(inflightReq, Errors.NONE));
        time.sleep(DEFAULT_RETRY_BACKOFF_MS);
        result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size(), "A next heartbeat should be sent on " +
            "the first poll after receiving a response that took longer than the interval, " +
            "waiting only for the minimal backoff.");
    }

    @Test
    public void testNetworkTimeout() {
        // The initial heartbeatInterval is set to 0
        createHeartbeatRequestStateWithZeroHeartbeatInterval();
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());
        // Mimic network timeout
        result.unsentRequests.get(0).handler().onFailure(time.milliseconds(), new TimeoutException("timeout"));
        verify(membershipManager).onHeartbeatFailure(true);
        verify(backgroundEventHandler, never()).add(any());

        // Assure the manager will backoff on timeout
        time.sleep(DEFAULT_RETRY_BACKOFF_MS - 1);
        result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(0, result.unsentRequests.size());

        time.sleep(1);
        result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());
    }

    @Test
    public void testDisconnect() {
        createHeartbeatRequestStateWithZeroHeartbeatInterval();
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());
        // Mimic disconnect
        result.unsentRequests.get(0).handler().onFailure(time.milliseconds(), DisconnectException.INSTANCE);
        verify(membershipManager).onHeartbeatFailure(true);
        // Ensure that the coordinatorManager rediscovers the coordinator
        verify(coordinatorRequestManager).handleCoordinatorDisconnect(any(), anyLong());
        verify(backgroundEventHandler, never()).add(any());

        time.sleep(DEFAULT_RETRY_BACKOFF_MS - 1);
        result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(0, result.unsentRequests.size(), "No request should be generated before the backoff expires");

        time.sleep(1);
        result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size(), "A new request should be generated after the backoff expires");
    }

    @Test
    public void testFailureOnFatalException() {
        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);

        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());
        result.unsentRequests.get(0).handler().onFailure(time.milliseconds(), new KafkaException("fatal"));
        verify(membershipManager).onHeartbeatFailure(false);
        verify(membershipManager).transitionToFatal();
        verify(backgroundEventHandler).add(any());
    }

    @Test
    public void testHeartbeatResponseErrorNotifiedToGroupManagerAfterErrorPropagated() {
        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());
        ClientResponse response = createHeartbeatResponse(result.unsentRequests.get(0), Errors.GROUP_AUTHORIZATION_FAILED);
        result.unsentRequests.get(0).handler().onComplete(response);

        // The error should be propagated before notifying the group manager. This ensures that the app thread is aware
        // of the HB error before the manager completes any ongoing unsubscribe.
        InOrder inOrder = inOrder(backgroundEventHandler, membershipManager);
        inOrder.verify(backgroundEventHandler).add(any(ErrorEvent.class));
        inOrder.verify(membershipManager).onHeartbeatFailure(false);
    }

    @Test
    public void testHeartbeatRequestFailureNotifiedToGroupManagerAfterErrorPropagated() {
        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());
        result.unsentRequests.get(0).handler().onFailure(time.milliseconds(), new AuthenticationException("Fatal error in HB"));

        // The error should be propagated before notifying the group manager. This ensures that the app thread is aware
        // of the HB error before the manager completes any ongoing unsubscribe.
        InOrder inOrder = inOrder(backgroundEventHandler, membershipManager);
        inOrder.verify(backgroundEventHandler).add(any(ErrorEvent.class));
        inOrder.verify(membershipManager).onHeartbeatFailure(false);
    }

    @Test
    public void testPollTimerExpiration() {
        recreateHeartbeatRequestManager();
        when(membershipManager.shouldSkipHeartbeat()).thenReturn(false);

        // On poll timer expiration, the member should send a last heartbeat to leave the group
        // and notify the membership manager
        time.sleep(DEFAULT_MAX_POLL_INTERVAL_MS);
        assertHeartbeat(heartbeatRequestManager, DEFAULT_HEARTBEAT_INTERVAL_MS);
        verify(membershipManager).transitionToSendingLeaveGroup(true);
        verifyHeartbeatStateReset();
        verify(heartbeatRequestState).reset();
        verify(membershipManager).onHeartbeatRequestGenerated();

        when(membershipManager.shouldSkipHeartbeat()).thenReturn(true);
        assertNoHeartbeat(heartbeatRequestManager);
        heartbeatRequestManager.resetPollTimer(time.milliseconds());
        assertTrue(pollTimer.notExpired());
        verify(membershipManager).maybeRejoinStaleMember();
        when(membershipManager.shouldSkipHeartbeat()).thenReturn(false);
        assertHeartbeat(heartbeatRequestManager, DEFAULT_HEARTBEAT_INTERVAL_MS);
    }

    @Test
    public void testisExpiredByUsedForLogging() {
        when(membershipManager.shouldSkipHeartbeat()).thenReturn(false);

        int exceededTimeMs = 5;
        time.sleep(DEFAULT_MAX_POLL_INTERVAL_MS + exceededTimeMs);

        when(membershipManager.isLeavingGroup()).thenReturn(false);
        NetworkClientDelegate.PollResult pollResult = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, pollResult.unsentRequests.size());
        verify(membershipManager).transitionToSendingLeaveGroup(true);
        verify(pollTimer, never()).isExpiredBy();

        clearInvocations(pollTimer);
        heartbeatRequestManager.resetPollTimer(time.milliseconds());
        verify(pollTimer).isExpiredBy();
    }

    @Test
    public void testFencedMemberStopHeartbeatUntilItReleasesAssignmentToRejoin() {
        recreateHeartbeatRequestManager();

        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());

        // Receive HB response fencing member
        when(subscriptions.hasAutoAssignedPartitions()).thenReturn(true);
        ClientResponse response = createHeartbeatResponse(result.unsentRequests.get(0), Errors.FENCED_MEMBER_EPOCH);
        result.unsentRequests.get(0).handler().onComplete(response);

        verify(membershipManager).transitionToFenced();
        verify(heartbeatRequestState).onFailedAttempt(anyLong());
        verify(heartbeatRequestState).reset();

        when(membershipManager.shouldSkipHeartbeat()).thenReturn(true);
        result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(0, result.unsentRequests.size(), "Member should not send heartbeats while FENCED");

        when(membershipManager.shouldSkipHeartbeat()).thenReturn(false);
        result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size(), "Fenced member should resume heartbeat after transitioning to JOINING");
    }

    @Test
    public void testHeartbeatMetrics() {
        assertNotNull(getMetric("heartbeat-response-time-max"));
        assertNotNull(getMetric("heartbeat-rate"));
        assertNotNull(getMetric("heartbeat-total"));
        assertNotNull(getMetric("last-heartbeat-seconds-ago"));

        // test poll
        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        assertHeartbeat(heartbeatRequestManager, DEFAULT_HEARTBEAT_INTERVAL_MS);
        time.sleep(1000);
        assertEquals(1.0, getMetric("heartbeat-total").metricValue());
        assertEquals((double) TimeUnit.MILLISECONDS.toSeconds(DEFAULT_HEARTBEAT_INTERVAL_MS), getMetric("last-heartbeat-seconds-ago").metricValue());

        assertHeartbeat(heartbeatRequestManager, DEFAULT_HEARTBEAT_INTERVAL_MS);
        assertEquals(0.06d, (double) getMetric("heartbeat-rate").metricValue(), 0.005d);
        assertEquals(2.0, getMetric("heartbeat-total").metricValue());

        // Randomly sleep for some time
        Random rand = new Random();
        int randomSleepS = rand.nextInt(11);
        time.sleep(randomSleepS * 1000);
        assertEquals((double) randomSleepS, getMetric("last-heartbeat-seconds-ago").metricValue());
    }

    protected void assertHeartbeat(AbstractHeartbeatRequestManager<R> hrm, int nextPollMs) {
        NetworkClientDelegate.PollResult pollResult = hrm.poll(time.milliseconds());
        assertEquals(1, pollResult.unsentRequests.size());
        assertEquals(nextPollMs, pollResult.timeUntilNextPollMs);
        pollResult.unsentRequests.get(0)
            .handler()
            .onComplete(createHeartbeatResponse(pollResult.unsentRequests.get(0), Errors.NONE));
    }

    protected void assertNoHeartbeat(AbstractHeartbeatRequestManager<R> hrm) {
        NetworkClientDelegate.PollResult pollResult = hrm.poll(time.milliseconds());
        assertEquals(0, pollResult.unsentRequests.size());
    }

    private KafkaMetric getMetric(final String name) {
        return metrics.metrics().get(metrics.metricName(name, metricGroupName()));
    }

    protected void assertHeartbeatErrorHandling(final Errors error,
                                                final boolean isFatal,
                                                final R response) {
        switch (error) {
            case NONE:
                verify(membershipManager).onHeartbeatSuccess(response);
                assertNextHeartbeatTiming(DEFAULT_HEARTBEAT_INTERVAL_MS);
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                verify(backgroundEventHandler, never()).add(any());
                assertNextHeartbeatTiming(DEFAULT_RETRY_BACKOFF_MS);
                break;

            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                verify(backgroundEventHandler, never()).add(any());
                verify(coordinatorRequestManager).markCoordinatorUnknown(any(), anyLong());
                assertNextHeartbeatTiming(0);
                break;
            case UNKNOWN_MEMBER_ID:
            case FENCED_MEMBER_EPOCH:
                verify(backgroundEventHandler, never()).add(any());
                assertNextHeartbeatTiming(0);
                break;

            case TOPIC_AUTHORIZATION_FAILED:
                verify(backgroundEventHandler).add(any(ErrorEvent.class));
                assertNextHeartbeatTiming(DEFAULT_RETRY_BACKOFF_MS);
                verify(membershipManager, never()).transitionToFatal();
                break;

            default:
                if (isFatal) {
                    // Drop the coordinator so the follow-up poll inside ensureFatalError() does
                    // not produce another heartbeat request.
                    when(coordinatorRequestManager.coordinator()).thenReturn(Optional.empty());
                    ensureFatalError(error);
                } else {
                    verify(backgroundEventHandler, never()).add(any());
                    assertNextHeartbeatTiming(0);
                }
                break;
        }

        if (error != Errors.NONE) {
            verify(membershipManager).onHeartbeatFailure(false);
        }

        if (!isFatal) {
            // Make sure a next heartbeat is sent for all non-fatal errors (to retry or rejoin)
            time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
            NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
            assertEquals(1, result.unsentRequests.size(),
                "A follow-up heartbeat should be sent after a non-fatal error " + error);
        }
    }

    protected void assertNextHeartbeatTiming(long expectedTimeToNextHeartbeatMs) {
        long currentTimeMs = time.milliseconds();
        assertEquals(expectedTimeToNextHeartbeatMs, heartbeatRequestState.timeToNextHeartbeatMs(currentTimeMs));
        if (expectedTimeToNextHeartbeatMs != 0) {
            assertFalse(heartbeatRequestState.canSendRequest(currentTimeMs));
            time.sleep(expectedTimeToNextHeartbeatMs);
        }
        assertTrue(heartbeatRequestState.canSendRequest(time.milliseconds()));
    }

    private void ensureFatalError(Errors expectedError) {
        verify(membershipManager).transitionToFatal();

        ArgumentCaptor<ErrorEvent> errorEventArgumentCaptor = ArgumentCaptor.forClass(ErrorEvent.class);
        verify(backgroundEventHandler).add(errorEventArgumentCaptor.capture());
        ErrorEvent errorEvent = errorEventArgumentCaptor.getValue();
        assertInstanceOf(expectedError.exception().getClass(), errorEvent.error(),
            "The fatal error propagated to the app thread does not match the error received in the heartbeat response.");

        // Ensure no further heartbeat is generated after the fatal error.
        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(0, result.unsentRequests.size(),
            "No further heartbeat should be sent after a fatal " + expectedError + " error.");
    }

    // error, isFatal
    private static Collection<Arguments> errorProvider() {
        return Arrays.asList(
            Arguments.of(Errors.NONE, false),
            Arguments.of(Errors.COORDINATOR_NOT_AVAILABLE, false),
            Arguments.of(Errors.COORDINATOR_LOAD_IN_PROGRESS, false),
            Arguments.of(Errors.NOT_COORDINATOR, false),
            Arguments.of(Errors.GROUP_AUTHORIZATION_FAILED, true),
            Arguments.of(Errors.INVALID_REQUEST, true),
            Arguments.of(Errors.UNKNOWN_MEMBER_ID, false),
            Arguments.of(Errors.FENCED_MEMBER_EPOCH, false),
            Arguments.of(Errors.UNSUPPORTED_ASSIGNOR, true),
            Arguments.of(Errors.UNSUPPORTED_VERSION, true),
            Arguments.of(Errors.UNRELEASED_INSTANCE_ID, true),
            Arguments.of(Errors.FENCED_INSTANCE_ID, true),
            Arguments.of(Errors.GROUP_MAX_SIZE_REACHED, true),
            Arguments.of(Errors.TOPIC_AUTHORIZATION_FAILED, false));
    }
}
