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
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ShareGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ShareGroupHeartbeatResponse;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShareHeartbeatRequestManagerTest {
    private static final String DEFAULT_GROUP_ID = "groupId";
    private static final String DEFAULT_MEMBER_ID = "member-id";
    private static final int DEFAULT_MEMBER_EPOCH = 1;
    private static final int DEFAULT_HEARTBEAT_INTERVAL_MS = 1000;
    private static final int DEFAULT_MAX_POLL_INTERVAL_MS = 10000;
    private static final long DEFAULT_RETRY_BACKOFF_MS = 80;
    private static final long DEFAULT_RETRY_BACKOFF_MAX_MS = 1000;
    private static final double DEFAULT_HEARTBEAT_JITTER_MS = 0.0;
    private static final String SHARE_CONSUMER_COORDINATOR_METRICS = "consumer-share-coordinator-metrics";

    private Time time;
    private Timer pollTimer;
    private CoordinatorRequestManager coordinatorRequestManager;
    private SubscriptionState subscriptions;
    private Metadata metadata;
    private ShareHeartbeatRequestManager heartbeatRequestManager;
    private ShareMembershipManager membershipManager;
    private ShareHeartbeatRequestManager.HeartbeatRequestState heartbeatRequestState;
    private ShareHeartbeatRequestManager.HeartbeatState heartbeatState;
    private BackgroundEventHandler backgroundEventHandler;
    private Metrics metrics;
    private LogContext logContext;

    @BeforeEach
    public void setUp() {
        time = new MockTime();
        pollTimer = spy(time.timer(DEFAULT_MAX_POLL_INTERVAL_MS));
        coordinatorRequestManager = mock(CoordinatorRequestManager.class);
        subscriptions = mock(SubscriptionState.class);
        backgroundEventHandler = mock(BackgroundEventHandler.class);
        membershipManager = mock(ShareMembershipManager.class);
        heartbeatState = mock(ShareHeartbeatRequestManager.HeartbeatState.class);
        metadata = mock(ConsumerMetadata.class);
        metrics = new Metrics(time);
        logContext = new LogContext();
        ConsumerConfig config = mock(ConsumerConfig.class);

        heartbeatRequestState = spy(new ShareHeartbeatRequestManager.HeartbeatRequestState(
                logContext,
                time,
                DEFAULT_HEARTBEAT_INTERVAL_MS,
                DEFAULT_RETRY_BACKOFF_MS,
                DEFAULT_RETRY_BACKOFF_MAX_MS,
                DEFAULT_HEARTBEAT_JITTER_MS));

        heartbeatRequestManager = new ShareHeartbeatRequestManager(
                logContext,
                pollTimer,
                config,
                coordinatorRequestManager,
                membershipManager,
                heartbeatState,
                heartbeatRequestState,
                backgroundEventHandler,
                metrics);

        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(new Node(1, "localhost", 9999)));
    }

    private void createHeartbeatRequestStateWithZeroHeartbeatInterval() {
        heartbeatRequestState = spy(new ShareHeartbeatRequestManager.HeartbeatRequestState(logContext,
                time,
                0,
                DEFAULT_RETRY_BACKOFF_MS,
                DEFAULT_RETRY_BACKOFF_MAX_MS,
                DEFAULT_HEARTBEAT_JITTER_MS));

        heartbeatRequestManager = createHeartbeatRequestManager(
                coordinatorRequestManager,
                membershipManager,
                heartbeatState,
                heartbeatRequestState,
                backgroundEventHandler);
    }

    private void createHeartbeatStateAndRequestManager() {
        this.heartbeatState = new ShareHeartbeatRequestManager.HeartbeatState(
                subscriptions,
                membershipManager);

        this.heartbeatRequestManager = createHeartbeatRequestManager(
                coordinatorRequestManager,
                membershipManager,
                heartbeatState,
                heartbeatRequestState,
                backgroundEventHandler);
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

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.SHARE_GROUP_HEARTBEAT)
    public void testFirstHeartbeatIncludesRequiredInfoToJoinGroupAndGetAssignments(short version) {
        createHeartbeatStateAndRequestManager();
        createHeartbeatRequestStateWithZeroHeartbeatInterval();
        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        String topic = "topic1";
        Set<String> set = Collections.singleton(topic);
        when(subscriptions.subscription()).thenReturn(set);
        subscriptions.subscribeToShareGroup(set);

        // Create a ShareGroupHeartbeatRequest and verify the payload
        mockJoiningMemberData();
        assertEquals(0, heartbeatRequestManager.maximumTimeToWait(time.milliseconds()));
        NetworkClientDelegate.PollResult pollResult = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, pollResult.unsentRequests.size());
        NetworkClientDelegate.UnsentRequest request = pollResult.unsentRequests.get(0);
        assertInstanceOf(ShareGroupHeartbeatRequest.Builder.class, request.requestBuilder());

        ShareGroupHeartbeatRequest heartbeatRequest =
                (ShareGroupHeartbeatRequest) request.requestBuilder().build(version);

        // Should include epoch 0 to join and no member ID.
        assertTrue(heartbeatRequest.data().memberId().isEmpty());
        assertEquals(0, heartbeatRequest.data().memberEpoch());

        // Should include subscription and group basic info to start getting assignments.
        assertEquals(Collections.singletonList(topic), heartbeatRequest.data().subscribedTopicNames());
        assertEquals(DEFAULT_GROUP_ID, heartbeatRequest.data().groupId());
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

    @Test
    public void testTimerNotDue() {
        time.sleep(100); // time elapsed < heartbeatInterval, no heartbeat should be sent
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
    public void testHeartbeatNotSentIfAnotherOneInFlight() {
        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);

        // Heartbeat sent (no response received)
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());
        NetworkClientDelegate.UnsentRequest inflightReq = result.unsentRequests.get(0);

        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(0, result.unsentRequests.size(), "No heartbeat should be sent while a " +
                "previous one in-flight");

        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(0, result.unsentRequests.size(), "No heartbeat should be sent when the " +
                "interval expires if there is a previous heartbeat request in-flight");

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
    public void testHeartbeatOutsideInterval() {
        when(membershipManager.shouldSkipHeartbeat()).thenReturn(false);
        when(membershipManager.shouldHeartbeatNow()).thenReturn(true);
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

        // Heartbeat should be sent
        assertEquals(1, result.unsentRequests.size());
        // Interval timer reset
        assertEquals(DEFAULT_HEARTBEAT_INTERVAL_MS, result.timeUntilNextPollMs);
        assertEquals(DEFAULT_HEARTBEAT_INTERVAL_MS, heartbeatRequestManager.maximumTimeToWait(time.milliseconds()));
        // Membership manager updated (to transition out of the heartbeating state)
        verify(membershipManager).onHeartbeatRequestGenerated();
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
    public void testFailureOnFatalException() {
        // The initial heartbeatInterval is set to 0
        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);

        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());
        result.unsentRequests.get(0).handler().onFailure(time.milliseconds(), new KafkaException("fatal"));
        verify(membershipManager).transitionToFatal();
        verify(backgroundEventHandler).add(any());
    }

    @Test
    public void testNoCoordinator() {
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.empty());
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

        assertEquals(Long.MAX_VALUE, result.timeUntilNextPollMs);
        assertEquals(DEFAULT_HEARTBEAT_INTERVAL_MS, heartbeatRequestManager.maximumTimeToWait(time.milliseconds()));
        assertEquals(0, result.unsentRequests.size());
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
        ShareGroupHeartbeatResponse mockResponse = (ShareGroupHeartbeatResponse) response.responseBody();

        switch (error) {
            case NONE:
                verify(membershipManager).onHeartbeatSuccess(mockResponse);
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
                verify(backgroundEventHandler, never()).add(any());
                assertNextHeartbeatTiming(0);
                break;

            default:
                if (isFatal) {
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
            result = heartbeatRequestManager.poll(time.milliseconds());
            assertEquals(1, result.unsentRequests.size());
        }
    }

    @Test
    public void testHeartbeatState() {
        mockJoiningMemberData();

        heartbeatState = new ShareHeartbeatRequestManager.HeartbeatState(
                subscriptions,
                membershipManager);

        createHeartbeatRequestStateWithZeroHeartbeatInterval();

        // The initial ShareGroupHeartbeatRequest sets most fields to their initial empty values
        ShareGroupHeartbeatRequestData data = heartbeatState.buildRequestData();
        assertEquals(DEFAULT_GROUP_ID, data.groupId());
        assertEquals("", data.memberId());
        assertEquals(0, data.memberEpoch());
        assertEquals(Collections.emptyList(), data.subscribedTopicNames());
        membershipManager.onHeartbeatRequestGenerated();

        // Mock a response from the group coordinator, that supplies the member ID and a new epoch
        when(membershipManager.state()).thenReturn(MemberState.STABLE);
        when(subscriptions.hasAutoAssignedPartitions()).thenReturn(true);
        when(subscriptions.rebalanceListener()).thenReturn(Optional.empty());
        mockStableMemberData();
        data = heartbeatState.buildRequestData();
        assertEquals(DEFAULT_GROUP_ID, data.groupId());
        assertEquals(DEFAULT_MEMBER_ID, data.memberId());
        assertEquals(DEFAULT_MEMBER_EPOCH, data.memberEpoch());
        assertNull(data.subscribedTopicNames());
        membershipManager.onHeartbeatRequestGenerated();

        // Join the group and subscribe to a topic, but the response has not yet been received
        String topic = "topic1";
        subscriptions.subscribe(Collections.singleton(topic), Optional.empty());
        when(subscriptions.subscription()).thenReturn(Collections.singleton(topic));
        mockRejoiningMemberData();
        data = heartbeatState.buildRequestData();
        assertEquals(DEFAULT_GROUP_ID, data.groupId());
        assertEquals(DEFAULT_MEMBER_ID, data.memberId());
        assertEquals(0, data.memberEpoch());
        assertEquals(Collections.singletonList(topic), data.subscribedTopicNames());
        membershipManager.onHeartbeatRequestGenerated();

        data = heartbeatState.buildRequestData();
        assertEquals(DEFAULT_GROUP_ID, data.groupId());
        assertEquals(DEFAULT_MEMBER_ID, data.memberId());
        assertEquals(0, data.memberEpoch());
        assertEquals(Collections.singletonList(topic), data.subscribedTopicNames());

        // Mock the response from the group coordinator which returns an assignment
        ShareGroupHeartbeatResponseData.TopicPartitions tpTopic1 =
                new ShareGroupHeartbeatResponseData.TopicPartitions();
        Uuid topicId = Uuid.randomUuid();
        tpTopic1.setTopicId(topicId);
        tpTopic1.setPartitions(Collections.singletonList(0));
        ShareGroupHeartbeatResponseData.Assignment assignmentTopic1 =
                new ShareGroupHeartbeatResponseData.Assignment();
        assignmentTopic1.setTopicPartitions(Collections.singletonList(tpTopic1));
        ShareGroupHeartbeatResponse rs1 = new ShareGroupHeartbeatResponse(new ShareGroupHeartbeatResponseData()
                .setHeartbeatIntervalMs(DEFAULT_HEARTBEAT_INTERVAL_MS)
                .setMemberId(DEFAULT_MEMBER_ID)
                .setMemberEpoch(DEFAULT_MEMBER_EPOCH)
                .setAssignment(assignmentTopic1));
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, "topic1"));
        membershipManager.onHeartbeatSuccess(rs1);
    }

    @Test
    public void testPollTimerExpiration() {
        heartbeatRequestManager = createHeartbeatRequestManager(
                coordinatorRequestManager,
                membershipManager,
                heartbeatState,
                heartbeatRequestState,
                backgroundEventHandler);
        when(membershipManager.shouldSkipHeartbeat()).thenReturn(false);

        // On poll timer expiration, the member should send a last heartbeat to leave the group
        // and notify the membership manager
        time.sleep(DEFAULT_MAX_POLL_INTERVAL_MS);
        assertHeartbeat(heartbeatRequestManager, DEFAULT_HEARTBEAT_INTERVAL_MS);
        verify(membershipManager).transitionToSendingLeaveGroup(true);
        verify(heartbeatState).reset();
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

    /**
     * This is expected to be the case where a member is already leaving the group and the poll
     * timer expires. The poll timer expiration should not transition the member to STALE, and
     * the member should continue to send heartbeats while the ongoing leaving operation
     * completes (send heartbeats while waiting for callbacks before leaving, or send last
     * heartbeat to leave).
     */
    @Test
    public void testPollTimerExpirationShouldNotMarkMemberStaleIfMemberAlreadyLeaving() {
        when(membershipManager.shouldSkipHeartbeat()).thenReturn(false);
        when(membershipManager.isLeavingGroup()).thenReturn(true);

        time.sleep(DEFAULT_MAX_POLL_INTERVAL_MS);
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

        // No transition to leave due to stale member should be triggered, because the member is
        // already leaving the group
        verify(membershipManager, never()).transitionToSendingLeaveGroup(anyBoolean());

        assertEquals(1, result.unsentRequests.size(), "A heartbeat request should be generated to" +
                " complete the ongoing leaving operation that was triggered before the poll timer expired.");
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

    private void assertHeartbeat(ShareHeartbeatRequestManager hrm, int nextPollMs) {
        NetworkClientDelegate.PollResult pollResult = hrm.poll(time.milliseconds());
        assertEquals(1, pollResult.unsentRequests.size());
        assertEquals(nextPollMs, pollResult.timeUntilNextPollMs);
        pollResult.unsentRequests.get(0).handler().onComplete(createHeartbeatResponse(pollResult.unsentRequests.get(0),
                Errors.NONE));
    }

    private void assertNoHeartbeat(ShareHeartbeatRequestManager hrm) {
        NetworkClientDelegate.PollResult pollResult = hrm.poll(time.milliseconds());
        assertEquals(0, pollResult.unsentRequests.size());
    }

    private void assertNextHeartbeatTiming(long expectedTimeToNextHeartbeatMs) {
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

        final ArgumentCaptor<ErrorEvent> errorEventArgumentCaptor = ArgumentCaptor.forClass(ErrorEvent.class);
        verify(backgroundEventHandler).add(errorEventArgumentCaptor.capture());
        ErrorEvent errorEvent = errorEventArgumentCaptor.getValue();
        assertInstanceOf(expectedError.exception().getClass(), errorEvent.error(),
                "The fatal error propagated to the app thread does not match the error received in the heartbeat response.");

        ensureHeartbeatStopped();
    }

    private void ensureHeartbeatStopped() {
        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(0, result.unsentRequests.size());
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
                Arguments.of(Errors.GROUP_MAX_SIZE_REACHED, true));
    }

    private ClientResponse createHeartbeatResponse(
            final NetworkClientDelegate.UnsentRequest request,
            final Errors error) {
        ShareGroupHeartbeatResponseData data = new ShareGroupHeartbeatResponseData()
                .setErrorCode(error.code())
                .setHeartbeatIntervalMs(DEFAULT_HEARTBEAT_INTERVAL_MS)
                .setMemberId(DEFAULT_MEMBER_ID)
                .setMemberEpoch(DEFAULT_MEMBER_EPOCH);
        if (error != Errors.NONE) {
            data.setErrorMessage("stubbed error message");
        }
        ShareGroupHeartbeatResponse response = new ShareGroupHeartbeatResponse(data);
        return new ClientResponse(
                new RequestHeader(ApiKeys.SHARE_GROUP_HEARTBEAT, ApiKeys.SHARE_GROUP_HEARTBEAT.latestVersion(), "client-id", 1),
                request.handler(),
                "0",
                time.milliseconds(),
                time.milliseconds(),
                false,
                null,
                null,
                response);
    }

    private ConsumerConfig config() {
        Properties prop = new Properties();
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");

        prop.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(DEFAULT_MAX_POLL_INTERVAL_MS));
        prop.setProperty(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(DEFAULT_RETRY_BACKOFF_MS));
        prop.setProperty(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG, String.valueOf(DEFAULT_RETRY_BACKOFF_MAX_MS));
        return new ConsumerConfig(prop);
    }

    private KafkaMetric getMetric(final String name) {
        return metrics.metrics().get(metrics.metricName(name, SHARE_CONSUMER_COORDINATOR_METRICS));
    }

    private ShareHeartbeatRequestManager createHeartbeatRequestManager(
            final CoordinatorRequestManager coordinatorRequestManager,
            final ShareMembershipManager membershipManager,
            final ShareHeartbeatRequestManager.HeartbeatState heartbeatState,
            final ShareHeartbeatRequestManager.HeartbeatRequestState heartbeatRequestState,
            final BackgroundEventHandler backgroundEventHandler) {
        LogContext logContext = new LogContext();
        pollTimer = time.timer(DEFAULT_MAX_POLL_INTERVAL_MS);
        return new ShareHeartbeatRequestManager(
                logContext,
                pollTimer,
                config(),
                coordinatorRequestManager,
                membershipManager,
                heartbeatState,
                heartbeatRequestState,
                backgroundEventHandler,
                new Metrics());
    }

    private void mockJoiningMemberData() {
        when(membershipManager.state()).thenReturn(MemberState.JOINING);
        when(membershipManager.groupId()).thenReturn(DEFAULT_GROUP_ID);
        when(membershipManager.memberId()).thenReturn("");
        when(membershipManager.memberEpoch()).thenReturn(0);
    }

    private void mockRejoiningMemberData() {
        when(membershipManager.state()).thenReturn(MemberState.JOINING);
        when(membershipManager.memberEpoch()).thenReturn(0);
    }

    private void mockStableMemberData() {
        when(membershipManager.currentAssignment()).thenReturn(new AbstractMembershipManager.LocalAssignment(0, Collections.emptyMap()));
        when(membershipManager.groupId()).thenReturn(DEFAULT_GROUP_ID);
        when(membershipManager.memberId()).thenReturn(DEFAULT_MEMBER_ID);
        when(membershipManager.memberEpoch()).thenReturn(DEFAULT_MEMBER_EPOCH);
    }
}
