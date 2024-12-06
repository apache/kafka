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
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager.HeartbeatRequestState;
import org.apache.kafka.clients.consumer.internals.AbstractMembershipManager.LocalAssignment;
import org.apache.kafka.clients.consumer.internals.ConsumerHeartbeatRequestManager.HeartbeatState;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.Builder;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.requests.RequestHeader;
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
import org.mockito.InOrder;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;

import static org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager.CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG;
import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.REGEX_RESOLUTION_NOT_SUPPORTED_MSG;
import static org.apache.kafka.common.utils.Utils.mkSortedSet;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class ConsumerHeartbeatRequestManagerTest {
    private static final String DEFAULT_GROUP_ID = "groupId";
    private static final String DEFAULT_REMOTE_ASSIGNOR = "uniform";
    private static final String DEFAULT_GROUP_INSTANCE_ID = "group-instance-id";
    private static final int DEFAULT_HEARTBEAT_INTERVAL_MS = 1000;
    private static final int DEFAULT_MAX_POLL_INTERVAL_MS = 10000;
    private static final long DEFAULT_RETRY_BACKOFF_MS = 80;
    private static final long DEFAULT_RETRY_BACKOFF_MAX_MS = 1000;
    private static final double DEFAULT_HEARTBEAT_JITTER_MS = 0.0;
    private static final String DEFAULT_MEMBER_ID = "member-id";
    private static final int DEFAULT_MEMBER_EPOCH = 1;

    private Time time;
    private Timer pollTimer;
    private CoordinatorRequestManager coordinatorRequestManager;
    private SubscriptionState subscriptions;
    private Metadata metadata;
    private ConsumerHeartbeatRequestManager heartbeatRequestManager;
    private ConsumerMembershipManager membershipManager;
    private HeartbeatRequestState heartbeatRequestState;
    private HeartbeatState heartbeatState;
    private BackgroundEventHandler backgroundEventHandler;
    private LogContext logContext;

    @BeforeEach
    public void setUp() {
        this.time = new MockTime();
        this.logContext = new LogContext();
        this.pollTimer = spy(time.timer(DEFAULT_MAX_POLL_INTERVAL_MS));
        this.coordinatorRequestManager = mock(CoordinatorRequestManager.class);
        this.heartbeatState = mock(HeartbeatState.class);
        this.backgroundEventHandler = mock(BackgroundEventHandler.class);
        this.subscriptions = mock(SubscriptionState.class);
        this.membershipManager = mock(ConsumerMembershipManager.class);
        this.metadata = mock(ConsumerMetadata.class);
        Metrics metrics = new Metrics(time);
        ConsumerConfig config = mock(ConsumerConfig.class);

        this.heartbeatRequestState = spy(new HeartbeatRequestState(
                logContext,
                time,
                DEFAULT_HEARTBEAT_INTERVAL_MS,
                DEFAULT_RETRY_BACKOFF_MS,
                DEFAULT_RETRY_BACKOFF_MAX_MS,
                DEFAULT_HEARTBEAT_JITTER_MS));

        this.heartbeatRequestManager = new ConsumerHeartbeatRequestManager(
                logContext,
                pollTimer,
                config,
                coordinatorRequestManager,
                membershipManager,
                heartbeatState,
                heartbeatRequestState,
                backgroundEventHandler,
                metrics);

        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(mock(Node.class)));
    }

    private void createHeartbeatRequestStateWithZeroHeartbeatInterval() {
        this.heartbeatRequestState = spy(new HeartbeatRequestState(
                logContext,
                time,
                0,
                DEFAULT_RETRY_BACKOFF_MS,
                DEFAULT_RETRY_BACKOFF_MAX_MS,
                DEFAULT_HEARTBEAT_JITTER_MS));

        this.heartbeatRequestManager = createHeartbeatRequestManager(
                coordinatorRequestManager,
                membershipManager,
                heartbeatState,
                heartbeatRequestState,
                backgroundEventHandler);
    }

    private void createHeartbeatStateAndRequestManager() {
        this.heartbeatState = new HeartbeatState(
                subscriptions,
                membershipManager,
                DEFAULT_MAX_POLL_INTERVAL_MS
        );

        this.heartbeatRequestManager = createHeartbeatRequestManager(
                coordinatorRequestManager,
                membershipManager,
                heartbeatState,
                heartbeatRequestState,
                backgroundEventHandler
        );
    }


    @Test
    public void testHeartBeatRequestStateToStringBase() {
        long retryBackoffMs = 100;
        long retryBackoffMaxMs = 1000;
        LogContext logContext = new LogContext();
        HeartbeatRequestState heartbeatRequestState = new HeartbeatRequestState(
                logContext,
                time,
                DEFAULT_HEARTBEAT_INTERVAL_MS,
                retryBackoffMs,
                retryBackoffMaxMs,
                .2
        );

        RequestState requestState = new RequestState(
                logContext,
                HeartbeatRequestState.class.getName(),
                retryBackoffMs,
                retryBackoffMaxMs
        );

        String target = requestState.toStringBase() +
                ", remainingMs=" + DEFAULT_HEARTBEAT_INTERVAL_MS +
                ", heartbeatIntervalMs=" + DEFAULT_HEARTBEAT_INTERVAL_MS;

        assertDoesNotThrow(heartbeatRequestState::toString);
        assertEquals(target, heartbeatRequestState.toStringBase());
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
    @ApiKeyVersionsSource(apiKey = ApiKeys.CONSUMER_GROUP_HEARTBEAT)
    public void testFirstHeartbeatIncludesRequiredInfoToJoinGroupAndGetAssignments(short version) {
        createHeartbeatStateAndRequestManager();
        createHeartbeatRequestStateWithZeroHeartbeatInterval();
        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        String topic = "topic1";
        Set<String> set = Collections.singleton(topic);
        when(subscriptions.subscription()).thenReturn(set);
        subscriptions.subscribe(set, Optional.empty());

        // Create a ConsumerHeartbeatRequest and verify the payload
        mockJoiningMemberData(DEFAULT_GROUP_INSTANCE_ID);
        assertEquals(0, heartbeatRequestManager.maximumTimeToWait(time.milliseconds()));
        NetworkClientDelegate.PollResult pollResult = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, pollResult.unsentRequests.size());
        NetworkClientDelegate.UnsentRequest request = pollResult.unsentRequests.get(0);
        assertInstanceOf(Builder.class, request.requestBuilder());

        ConsumerGroupHeartbeatRequest heartbeatRequest =
                (ConsumerGroupHeartbeatRequest) request.requestBuilder().build(version);

        // Should include epoch 0 and member id to join
        String memberId = heartbeatRequest.data().memberId();
        assertNotNull(memberId);
        assertFalse(memberId.isEmpty());
        assertEquals(0, heartbeatRequest.data().memberEpoch());

        // Should include subscription and group basic info to start getting assignments, as well as rebalanceTimeoutMs
        assertEquals(Collections.singletonList(topic), heartbeatRequest.data().subscribedTopicNames());
        assertEquals(DEFAULT_MAX_POLL_INTERVAL_MS, heartbeatRequest.data().rebalanceTimeoutMs());
        assertEquals(DEFAULT_GROUP_ID, heartbeatRequest.data().groupId());
        assertEquals(DEFAULT_GROUP_INSTANCE_ID, heartbeatRequest.data().instanceId());
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
        // The initial heartbeatInterval is set to 0
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
        ClientResponse response = createHeartbeatResponse(result.unsentRequests.get(0), Errors.GROUP_AUTHORIZATION_FAILED);
        result.unsentRequests.get(0).handler().onFailure(time.milliseconds(), new AuthenticationException("Fatal error in HB"));

        // The error should be propagated before notifying the group manager. This ensures that the app thread is aware
        // of the HB error before the manager completes any ongoing unsubscribe.
        InOrder inOrder = inOrder(backgroundEventHandler, membershipManager);
        inOrder.verify(backgroundEventHandler).add(any(ErrorEvent.class));
        inOrder.verify(membershipManager).onHeartbeatFailure(false);
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
    @ApiKeyVersionsSource(apiKey = ApiKeys.CONSUMER_GROUP_HEARTBEAT)
    public void testValidateConsumerGroupHeartbeatRequest(final short version) {
        createHeartbeatStateAndRequestManager();

        // The initial heartbeatInterval is set to 0, but we're testing
        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);

        String subscribedTopic = "topic";
        when(subscriptions.subscription()).thenReturn(Collections.singleton(subscribedTopic));

        // Update membershipManager's memberId and memberEpoch
        ConsumerGroupHeartbeatResponse result =
            new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
            .setMemberId(DEFAULT_MEMBER_ID)
            .setMemberEpoch(DEFAULT_MEMBER_EPOCH));
        membershipManager.onHeartbeatSuccess(result);

        // Create a ConsumerHeartbeatRequest and verify the payload
        mockStableMemberData(DEFAULT_GROUP_INSTANCE_ID);
        NetworkClientDelegate.PollResult pollResult = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, pollResult.unsentRequests.size());
        NetworkClientDelegate.UnsentRequest request = pollResult.unsentRequests.get(0);
        assertInstanceOf(Builder.class, request.requestBuilder());

        ConsumerGroupHeartbeatRequest heartbeatRequest =
                (ConsumerGroupHeartbeatRequest) request.requestBuilder().build(version);

        assertEquals(DEFAULT_GROUP_ID, heartbeatRequest.data().groupId());
        assertEquals(DEFAULT_MEMBER_ID, heartbeatRequest.data().memberId());
        assertEquals(DEFAULT_MEMBER_EPOCH, heartbeatRequest.data().memberEpoch());
        assertEquals(10000, heartbeatRequest.data().rebalanceTimeoutMs());
        assertEquals(subscribedTopic, heartbeatRequest.data().subscribedTopicNames().get(0));
        assertEquals(DEFAULT_GROUP_INSTANCE_ID, heartbeatRequest.data().instanceId());
        assertEquals(DEFAULT_REMOTE_ASSIGNOR, heartbeatRequest.data().serverAssignor());
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.CONSUMER_GROUP_HEARTBEAT)
    public void testValidateConsumerGroupHeartbeatRequestAssignmentSentWhenLocalEpochChanges(final short version) {
        createHeartbeatStateAndRequestManager();

        when(membershipManager.shouldHeartbeatNow()).thenReturn(true);

        Uuid topicId = Uuid.randomUuid();
        ConsumerGroupHeartbeatRequestData.TopicPartitions expectedTopicPartitions =
            new ConsumerGroupHeartbeatRequestData.TopicPartitions();
        Map<Uuid, SortedSet<Integer>> testAssignment = Collections.singletonMap(
            topicId, mkSortedSet(0)
        );
        expectedTopicPartitions.setTopicId(topicId);
        expectedTopicPartitions.setPartitions(Collections.singletonList(0));

        // First heartbeat, include assignment
        when(membershipManager.currentAssignment()).thenReturn(new LocalAssignment(0, testAssignment));

        ConsumerGroupHeartbeatRequest heartbeatRequest1 = getHeartbeatRequest(heartbeatRequestManager, version);
        assertEquals(Collections.singletonList(expectedTopicPartitions), heartbeatRequest1.data().topicPartitions());

        // Assignment did not change, so no assignment should be sent
        when(heartbeatRequestState.canSendRequest(anyLong())).thenReturn(true);
        ConsumerGroupHeartbeatRequest heartbeatRequest2 = getHeartbeatRequest(heartbeatRequestManager, version);
        assertNull(heartbeatRequest2.data().topicPartitions());

        // Local epoch bumped, so assignment should be sent
        when(membershipManager.currentAssignment()).thenReturn(new LocalAssignment(1, testAssignment));

        ConsumerGroupHeartbeatRequest heartbeatRequest3 = getHeartbeatRequest(heartbeatRequestManager, version);
        assertEquals(Collections.singletonList(expectedTopicPartitions), heartbeatRequest3.data().topicPartitions());
    }

    private ConsumerGroupHeartbeatRequest getHeartbeatRequest(ConsumerHeartbeatRequestManager heartbeatRequestManager, final short version) {
        NetworkClientDelegate.PollResult pollResult = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, pollResult.unsentRequests.size());
        NetworkClientDelegate.UnsentRequest request = pollResult.unsentRequests.get(0);
        assertInstanceOf(Builder.class, request.requestBuilder());
        return (ConsumerGroupHeartbeatRequest) request.requestBuilder().build(version);
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
        ConsumerGroupHeartbeatResponse mockResponse = (ConsumerGroupHeartbeatResponse) response.responseBody();

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
            case FENCED_MEMBER_EPOCH:
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

    /**
     * This validates the UnsupportedApiVersion the client generates while building a HB if:
     * 1. HB API is not supported.
     * 2. Required HB API version is not available.
     */
    @ParameterizedTest
    @ValueSource(strings = {CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG, REGEX_RESOLUTION_NOT_SUPPORTED_MSG})
    public void testUnsupportedVersion(String errorMsg) {
        mockResponseWithException(new UnsupportedVersionException(errorMsg));
        ArgumentCaptor<ErrorEvent> errorEventArgumentCaptor = ArgumentCaptor.forClass(ErrorEvent.class);
        verify(backgroundEventHandler).add(errorEventArgumentCaptor.capture());
        ErrorEvent errorEvent = errorEventArgumentCaptor.getValue();
        assertInstanceOf(Errors.UNSUPPORTED_VERSION.exception().getClass(), errorEvent.error());
        assertEquals(errorMsg, errorEvent.error().getMessage());
        clearInvocations(backgroundEventHandler);
    }

    private void mockErrorResponse(Errors error, String exceptionCustomMsg) {
        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());

        when(subscriptions.hasAutoAssignedPartitions()).thenReturn(true);
        ClientResponse response = createHeartbeatResponse(
            result.unsentRequests.get(0), error, exceptionCustomMsg);
        result.unsentRequests.get(0).handler().onComplete(response);
    }

    private void mockResponseWithException(UnsupportedVersionException exception) {
        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());

        when(subscriptions.hasAutoAssignedPartitions()).thenReturn(true);
        ClientResponse response = createHeartbeatResponseWithException(
            result.unsentRequests.get(0), exception);
        result.unsentRequests.get(0).handler().onComplete(response);
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

    @Test
    public void testHeartbeatState() {
        mockJoiningMemberData(null);

        heartbeatState = new HeartbeatState(
                subscriptions,
                membershipManager,
                DEFAULT_MAX_POLL_INTERVAL_MS
        );

        createHeartbeatRequestStateWithZeroHeartbeatInterval();

        // The initial ConsumerGroupHeartbeatRequest sets most fields to their initial empty values
        ConsumerGroupHeartbeatRequestData data = heartbeatState.buildRequestData();
        assertEquals(DEFAULT_GROUP_ID, data.groupId());
        assertEquals(DEFAULT_MEMBER_ID, data.memberId());
        assertEquals(0, data.memberEpoch());
        assertNull(data.instanceId());
        assertEquals(DEFAULT_MAX_POLL_INTERVAL_MS, data.rebalanceTimeoutMs());
        assertEquals(Collections.emptyList(), data.subscribedTopicNames());
        assertEquals(DEFAULT_REMOTE_ASSIGNOR, data.serverAssignor());
        assertEquals(Collections.emptyList(), data.topicPartitions());

        // Mock a response from the group coordinator, that supplies the member ID and a new epoch
        when(membershipManager.state()).thenReturn(MemberState.STABLE);
        when(subscriptions.hasAutoAssignedPartitions()).thenReturn(true);
        when(subscriptions.rebalanceListener()).thenReturn(Optional.empty());
        mockStableMemberData(null);
        data = heartbeatState.buildRequestData();
        assertEquals(DEFAULT_GROUP_ID, data.groupId());
        assertEquals(DEFAULT_MEMBER_ID, data.memberId());
        assertEquals(1, data.memberEpoch());
        assertNull(data.instanceId());
        assertEquals(-1, data.rebalanceTimeoutMs());
        assertNull(data.subscribedTopicNames());
        assertNull(data.serverAssignor());
        assertEquals(Collections.emptyList(), data.topicPartitions());

        // Join the group and subscribe to a topic, but the response has not yet been received
        String topic = "topic1";
        subscriptions.subscribe(Collections.singleton(topic), Optional.empty());
        when(subscriptions.subscription()).thenReturn(Collections.singleton(topic));
        mockRejoiningMemberData();
        data = heartbeatState.buildRequestData();
        assertEquals(DEFAULT_GROUP_ID, data.groupId());
        assertEquals(DEFAULT_MEMBER_ID, data.memberId());
        assertEquals(0, data.memberEpoch());
        assertNull(data.instanceId());
        assertEquals(DEFAULT_MAX_POLL_INTERVAL_MS, data.rebalanceTimeoutMs());
        assertEquals(Collections.singletonList(topic), data.subscribedTopicNames());
        assertEquals(DEFAULT_REMOTE_ASSIGNOR, data.serverAssignor());
        assertEquals(Collections.emptyList(), data.topicPartitions());

        data = heartbeatState.buildRequestData();
        assertEquals(DEFAULT_GROUP_ID, data.groupId());
        assertEquals(DEFAULT_MEMBER_ID, data.memberId());
        assertEquals(0, data.memberEpoch());
        assertNull(data.instanceId());
        assertEquals(DEFAULT_MAX_POLL_INTERVAL_MS, data.rebalanceTimeoutMs());
        assertEquals(Collections.singletonList(topic), data.subscribedTopicNames());
        assertEquals(DEFAULT_REMOTE_ASSIGNOR, data.serverAssignor());
        assertEquals(Collections.emptyList(), data.topicPartitions());

        // Mock the response from the group coordinator which returns an assignment
        ConsumerGroupHeartbeatResponseData.TopicPartitions tpTopic1 =
            new ConsumerGroupHeartbeatResponseData.TopicPartitions();
        Uuid topicId = Uuid.randomUuid();
        tpTopic1.setTopicId(topicId);
        tpTopic1.setPartitions(Collections.singletonList(0));
        ConsumerGroupHeartbeatResponseData.Assignment assignmentTopic1 =
            new ConsumerGroupHeartbeatResponseData.Assignment();
        assignmentTopic1.setTopicPartitions(Collections.singletonList(tpTopic1));
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, "topic1"));
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
        heartbeatRequestManager = createHeartbeatRequestManager(
                coordinatorRequestManager,
                membershipManager,
                heartbeatState,
                heartbeatRequestState,
                backgroundEventHandler);

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

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.CONSUMER_GROUP_HEARTBEAT)
    public void testSendingLeaveGroupHeartbeatWhenPreviousOneInFlight(final short version) {
        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());
        result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(0, result.unsentRequests.size(), "No heartbeat should be sent while a previous one is in-flight");

        when(membershipManager.state()).thenReturn(MemberState.LEAVING);
        when(heartbeatState.buildRequestData()).thenReturn(new ConsumerGroupHeartbeatRequestData().setMemberEpoch(-1));
        ConsumerGroupHeartbeatRequest heartbeatToLeave = getHeartbeatRequest(heartbeatRequestManager, version);
        assertEquals(ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH, heartbeatToLeave.data().memberEpoch());

        when(membershipManager.shouldSkipHeartbeat()).thenReturn(true);
        NetworkClientDelegate.PollResult pollAgain = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(0, pollAgain.unsentRequests.size());
    }
    
    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.CONSUMER_GROUP_HEARTBEAT)
    public void testConsumerAcksReconciledAssignmentAfterAckLost(final short version) {
        String topic = "topic1";
        Set<String> topics = Collections.singleton(topic);
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;
        Map<Uuid, SortedSet<Integer>> testAssignment = Collections.singletonMap(
                topicId, mkSortedSet(partition)
        );
        
        // complete reconciliation
        createHeartbeatStateAndRequestManager();
        when(subscriptions.subscription()).thenReturn(topics);
        subscriptions.subscribe(topics, Optional.empty());
        mockReconcilingMemberData(testAssignment);
        
        // send heartbeat1 to ack assignment tp0
        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        
        // HB1 times out
        assertFalse(result.unsentRequests.isEmpty());
        result.unsentRequests.get(0)
                .handler()
                .onFailure(time.milliseconds(), new TimeoutException("timeout"));
        
        // heartbeat request manager resets the sentFields to null HeartbeatState.reset()
        time.sleep(DEFAULT_MAX_POLL_INTERVAL_MS);
        assertHeartbeat(heartbeatRequestManager, DEFAULT_HEARTBEAT_INTERVAL_MS);
        verify(heartbeatRequestState).reset();
        
        // following HB will include tp0 (and act as ack), tp0 != null
        result = heartbeatRequestManager.poll(time.milliseconds());
        NetworkClientDelegate.UnsentRequest request = result.unsentRequests.get(0);
        ConsumerGroupHeartbeatRequest heartbeatRequest =
                (ConsumerGroupHeartbeatRequest) request.requestBuilder().build(version);

        assertEquals(Collections.singletonList(topic), heartbeatRequest.data().subscribedTopicNames());
        assertEquals(testAssignment.size(), heartbeatRequest.data().topicPartitions().size());
        ConsumerGroupHeartbeatRequestData.TopicPartitions topicPartitions = 
                heartbeatRequest.data().topicPartitions().get(0);
        assertEquals(topicId, topicPartitions.topicId());
        assertEquals(Collections.singletonList(partition), topicPartitions.partitions());
    }

    @Test
    public void testPollOnCloseGeneratesRequestIfNeeded() {
        when(membershipManager.isLeavingGroup()).thenReturn(true);
        NetworkClientDelegate.PollResult pollResult = heartbeatRequestManager.pollOnClose(time.milliseconds());
        assertEquals(1, pollResult.unsentRequests.size(),
            "A request to leave the group should be generated if the member is still leaving when closing the manager");

        when(membershipManager.isLeavingGroup()).thenReturn(false);
        pollResult = heartbeatRequestManager.pollOnClose(time.milliseconds());
        assertTrue(pollResult.unsentRequests.isEmpty(),
            "No requests should be generated on close if the member is not leaving when closing the manager");
    }

    @Test
    public void testRegexInHeartbeatLifecycle() {
        heartbeatState = new HeartbeatState(subscriptions, membershipManager, DEFAULT_MAX_POLL_INTERVAL_MS);
        createHeartbeatRequestStateWithZeroHeartbeatInterval();

        // Initial heartbeat with regex
        mockJoiningMemberData(null);
        when(subscriptions.subscriptionPattern()).thenReturn(new SubscriptionPattern("t1.*"));
        ConsumerGroupHeartbeatRequestData data = heartbeatState.buildRequestData();
        assertEquals("t1.*", data.subscribedTopicRegex());

        // Regex not included in HB if not updated
        when(membershipManager.state()).thenReturn(MemberState.STABLE);
        data = heartbeatState.buildRequestData();
        assertNull(data.subscribedTopicRegex());

        // Regex included in HB if updated
        when(subscriptions.subscriptionPattern()).thenReturn(new SubscriptionPattern("t2.*"));
        data = heartbeatState.buildRequestData();
        assertEquals("t2.*", data.subscribedTopicRegex());

        // Empty regex included in HB to remove pattern subscription
        when(subscriptions.subscriptionPattern()).thenReturn(null);
        data = heartbeatState.buildRequestData();
        assertEquals("", data.subscribedTopicRegex());

        // Regex not included in HB after pattern subscription removed
        when(subscriptions.subscriptionPattern()).thenReturn(null);
        data = heartbeatState.buildRequestData();
        assertNull(data.subscribedTopicRegex());
    }

    @Test
    public void testRegexInJoiningHeartbeat() {
        heartbeatState = new HeartbeatState(subscriptions, membershipManager, DEFAULT_MAX_POLL_INTERVAL_MS);
        createHeartbeatRequestStateWithZeroHeartbeatInterval();

        // Initial heartbeat with regex
        mockJoiningMemberData(null);
        when(subscriptions.subscriptionPattern()).thenReturn(new SubscriptionPattern("t1.*"));
        ConsumerGroupHeartbeatRequestData data = heartbeatState.buildRequestData();
        assertEquals("t1.*", data.subscribedTopicRegex());

        // Members unsubscribes from regex (empty regex included in HB)
        when(subscriptions.subscriptionPattern()).thenReturn(null);
        data = heartbeatState.buildRequestData();
        assertEquals("", data.subscribedTopicRegex());

        // Member rejoins (ie. fenced) should not include regex field in HB
        when(membershipManager.state()).thenReturn(MemberState.JOINING);
        when(subscriptions.subscriptionPattern()).thenReturn(null);
        data = heartbeatState.buildRequestData();
        assertNull(data.subscribedTopicRegex());
    }

    private void assertHeartbeat(ConsumerHeartbeatRequestManager hrm, int nextPollMs) {
        NetworkClientDelegate.PollResult pollResult = hrm.poll(time.milliseconds());
        assertEquals(1, pollResult.unsentRequests.size());
        assertEquals(nextPollMs, pollResult.timeUntilNextPollMs);
        pollResult.unsentRequests.get(0).handler().onComplete(createHeartbeatResponse(pollResult.unsentRequests.get(0),
            Errors.NONE));
    }

    private void assertNoHeartbeat(ConsumerHeartbeatRequestManager hrm) {
        NetworkClientDelegate.PollResult pollResult = hrm.poll(time.milliseconds());
        assertEquals(0, pollResult.unsentRequests.size());
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
            Arguments.of(Errors.FENCED_INSTANCE_ID, true),
            Arguments.of(Errors.GROUP_MAX_SIZE_REACHED, true));
    }

    private ClientResponse createHeartbeatResponse(NetworkClientDelegate.UnsentRequest request,
                                                   Errors error) {
        return createHeartbeatResponse(request, error, "stubbed error message");
    }

    private ClientResponse createHeartbeatResponse(
        final NetworkClientDelegate.UnsentRequest request,
        final Errors error,
        final String msg
    ) {
        ConsumerGroupHeartbeatResponseData data = new ConsumerGroupHeartbeatResponseData()
            .setErrorCode(error.code())
            .setHeartbeatIntervalMs(DEFAULT_HEARTBEAT_INTERVAL_MS)
            .setMemberId(DEFAULT_MEMBER_ID)
            .setMemberEpoch(DEFAULT_MEMBER_EPOCH);
        if (error != Errors.NONE) {
            data.setErrorMessage(msg);
        }
        ConsumerGroupHeartbeatResponse response = new ConsumerGroupHeartbeatResponse(data);
        return new ClientResponse(
            new RequestHeader(ApiKeys.CONSUMER_GROUP_HEARTBEAT, ApiKeys.CONSUMER_GROUP_HEARTBEAT.latestVersion(), "client-id", 1),
            request.handler(),
            "0",
            time.milliseconds(),
            time.milliseconds(),
            false,
            null,
            null,
            response);
    }

    private ClientResponse createHeartbeatResponseWithException(
        final NetworkClientDelegate.UnsentRequest request,
        final UnsupportedVersionException exception
    ) {
        ConsumerGroupHeartbeatResponse response = new ConsumerGroupHeartbeatResponse(null);
        return new ClientResponse(
            new RequestHeader(ApiKeys.CONSUMER_GROUP_HEARTBEAT, ApiKeys.CONSUMER_GROUP_HEARTBEAT.latestVersion(), "client-id", 1),
            request.handler(),
            "0",
            time.milliseconds(),
            time.milliseconds(),
            false,
            exception,
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
        prop.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(DEFAULT_HEARTBEAT_INTERVAL_MS));
        return new ConsumerConfig(prop);
    }

    private ConsumerHeartbeatRequestManager createHeartbeatRequestManager(
            final CoordinatorRequestManager coordinatorRequestManager,
            final ConsumerMembershipManager membershipManager,
            final HeartbeatState heartbeatState,
            final HeartbeatRequestState heartbeatRequestState,
            final BackgroundEventHandler backgroundEventHandler) {
        LogContext logContext = new LogContext();
        pollTimer = time.timer(DEFAULT_MAX_POLL_INTERVAL_MS);
        return new ConsumerHeartbeatRequestManager(
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

    private void mockJoiningMemberData(String instanceId) {
        when(membershipManager.state()).thenReturn(MemberState.JOINING);
        when(membershipManager.groupInstanceId()).thenReturn(Optional.ofNullable(instanceId));
        when(membershipManager.memberId()).thenReturn(DEFAULT_MEMBER_ID);
        when(membershipManager.memberEpoch()).thenReturn(0);
        when(membershipManager.groupId()).thenReturn(DEFAULT_GROUP_ID);
        when(membershipManager.currentAssignment()).thenReturn(LocalAssignment.NONE);
        when(membershipManager.serverAssignor()).thenReturn(Optional.of(DEFAULT_REMOTE_ASSIGNOR));
    }

    private void mockRejoiningMemberData() {
        when(membershipManager.state()).thenReturn(MemberState.JOINING);
        when(membershipManager.memberEpoch()).thenReturn(0);
        when(membershipManager.groupInstanceId()).thenReturn(Optional.empty());
    }

    private void mockStableMemberData(String instanceId) {
        when(membershipManager.groupInstanceId()).thenReturn(Optional.ofNullable(instanceId));
        when(membershipManager.currentAssignment()).thenReturn(new LocalAssignment(0, Collections.emptyMap()));
        when(membershipManager.groupId()).thenReturn(DEFAULT_GROUP_ID);
        when(membershipManager.memberId()).thenReturn(DEFAULT_MEMBER_ID);
        when(membershipManager.memberEpoch()).thenReturn(DEFAULT_MEMBER_EPOCH);
        when(membershipManager.serverAssignor()).thenReturn(Optional.of(DEFAULT_REMOTE_ASSIGNOR));
    }
    
    private void mockReconcilingMemberData(Map<Uuid, SortedSet<Integer>> assignment) {
        when(membershipManager.state()).thenReturn(MemberState.RECONCILING);
        when(membershipManager.currentAssignment()).thenReturn(new LocalAssignment(0, assignment));
        when(membershipManager.memberId()).thenReturn(DEFAULT_MEMBER_ID);
        when(membershipManager.memberEpoch()).thenReturn(DEFAULT_MEMBER_EPOCH);
        when(membershipManager.groupId()).thenReturn(DEFAULT_GROUP_ID);
        when(membershipManager.serverAssignor()).thenReturn(Optional.of(DEFAULT_REMOTE_ASSIGNOR));
    }
}
