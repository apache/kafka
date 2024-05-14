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
import org.apache.kafka.clients.consumer.internals.HeartbeatRequestManager.HeartbeatRequestState;
import org.apache.kafka.clients.consumer.internals.HeartbeatRequestManager.HeartbeatState;
import org.apache.kafka.clients.consumer.internals.MembershipManager.LocalAssignment;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData.Assignment;
import org.apache.kafka.common.metrics.KafkaMetric;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.internals.ConsumerTestBuilder.DEFAULT_GROUP_INSTANCE_ID;
import static org.apache.kafka.clients.consumer.internals.ConsumerTestBuilder.DEFAULT_HEARTBEAT_INTERVAL_MS;
import static org.apache.kafka.clients.consumer.internals.ConsumerTestBuilder.DEFAULT_MAX_POLL_INTERVAL_MS;
import static org.apache.kafka.clients.consumer.internals.ConsumerTestBuilder.DEFAULT_REMOTE_ASSIGNOR;
import static org.apache.kafka.clients.consumer.internals.ConsumerTestBuilder.DEFAULT_RETRY_BACKOFF_MAX_MS;
import static org.apache.kafka.clients.consumer.internals.ConsumerTestBuilder.DEFAULT_RETRY_BACKOFF_MS;
import static org.apache.kafka.common.utils.Utils.mkSortedSet;
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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class HeartbeatRequestManagerTest {
    private static final String DEFAULT_GROUP_ID = "groupId";
    private static final String CONSUMER_COORDINATOR_METRICS = "consumer-coordinator-metrics";

    private ConsumerTestBuilder testBuilder;
    private Time time;
    private Timer pollTimer;
    private CoordinatorRequestManager coordinatorRequestManager;
    private SubscriptionState subscriptions;
    private Metadata metadata;
    private HeartbeatRequestManager heartbeatRequestManager;
    private MembershipManager membershipManager;
    private HeartbeatRequestManager.HeartbeatRequestState heartbeatRequestState;
    private HeartbeatRequestManager.HeartbeatState heartbeatState;
    private final String memberId = "member-id";
    private final int memberEpoch = 1;
    private BackgroundEventHandler backgroundEventHandler;
    private Metrics metrics;

    @BeforeEach
    public void setUp() {
        setUp(ConsumerTestBuilder.createDefaultGroupInformation());
    }

    private void setUp(Optional<ConsumerTestBuilder.GroupInformation> groupInfo) {
        testBuilder = new ConsumerTestBuilder(groupInfo, true, false);
        time = testBuilder.time;
        coordinatorRequestManager = testBuilder.coordinatorRequestManager.orElseThrow(IllegalStateException::new);
        heartbeatRequestManager = testBuilder.heartbeatRequestManager.orElseThrow(IllegalStateException::new);
        heartbeatRequestState = testBuilder.heartbeatRequestState.orElseThrow(IllegalStateException::new);
        heartbeatState = testBuilder.heartbeatState.orElseThrow(IllegalStateException::new);
        backgroundEventHandler = testBuilder.backgroundEventHandler;
        subscriptions = testBuilder.subscriptions;
        membershipManager = testBuilder.membershipManager.orElseThrow(IllegalStateException::new);
        metadata = testBuilder.metadata;
        metrics = new Metrics(time);

        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(new Node(1, "localhost", 9999)));
    }

    private void resetWithZeroHeartbeatInterval(Optional<String> groupInstanceId) {
        cleanup();

        ConsumerTestBuilder.GroupInformation gi = new ConsumerTestBuilder.GroupInformation(
                DEFAULT_GROUP_ID,
                groupInstanceId,
                0,
                0.0,
                Optional.of(DEFAULT_REMOTE_ASSIGNOR)
        );

        setUp(Optional.of(gi));
    }

    @AfterEach
    public void cleanup() {
        if (testBuilder != null) {
            testBuilder.close();
        }
    }

    @Test
    public void testHeartbeatOnStartup() {
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(0, result.unsentRequests.size());

        resetWithZeroHeartbeatInterval(Optional.empty());
        mockStableMember();
        assertEquals(0, heartbeatRequestManager.maximumTimeToWait(time.milliseconds()));
        result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());

        // Ensure we do not resend the request without the first request being completed
        NetworkClientDelegate.PollResult result2 = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(0, result2.unsentRequests.size());
    }

    @Test
    public void testSuccessfulHeartbeatTiming() {
        mockStableMember();
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
        resetWithZeroHeartbeatInterval(Optional.of(DEFAULT_GROUP_INSTANCE_ID));
        String topic = "topic1";
        subscriptions.subscribe(Collections.singleton(topic), Optional.empty());
        membershipManager.onSubscriptionUpdated();

        // Create a ConsumerHeartbeatRequest and verify the payload
        assertEquals(0, heartbeatRequestManager.maximumTimeToWait(time.milliseconds()));
        NetworkClientDelegate.PollResult pollResult = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, pollResult.unsentRequests.size());
        NetworkClientDelegate.UnsentRequest request = pollResult.unsentRequests.get(0);
        assertInstanceOf(Builder.class, request.requestBuilder());

        ConsumerGroupHeartbeatRequest heartbeatRequest =
                (ConsumerGroupHeartbeatRequest) request.requestBuilder().build(version);

        // Should include epoch 0 to join and no member ID.
        assertTrue(heartbeatRequest.data().memberId().isEmpty());
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
        resetWithZeroHeartbeatInterval(Optional.empty());

        // Mocking notInGroup
        when(membershipManager.shouldSkipHeartbeat()).thenReturn(shouldSkipHeartbeat);
        when(heartbeatRequestState.canSendRequest(anyLong())).thenReturn(true);

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
        mockStableMember();
        time.sleep(100); // time elapsed < heartbeatInterval, no heartbeat should be sent
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(0, result.unsentRequests.size());
        assertEquals(DEFAULT_HEARTBEAT_INTERVAL_MS - 100, result.timeUntilNextPollMs);
        assertEquals(DEFAULT_HEARTBEAT_INTERVAL_MS - 100, heartbeatRequestManager.maximumTimeToWait(time.milliseconds()));

        // Member in state where it should not send Heartbeat anymore
        when(subscriptions.hasAutoAssignedPartitions()).thenReturn(true);
        membershipManager.transitionToFatal();
        result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(Long.MAX_VALUE, result.timeUntilNextPollMs);
    }

    @Test
    public void testHeartbeatNotSentIfAnotherOneInFlight() {
        mockStableMember();
        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);

        // Heartbeat sent (no response received)
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());
        NetworkClientDelegate.UnsentRequest inflightReq = result.unsentRequests.get(0);

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
        verify(membershipManager).onHeartbeatRequestSent();
    }

    @Test
    public void testNetworkTimeout() {
        // The initial heartbeatInterval is set to 0
        resetWithZeroHeartbeatInterval(Optional.empty());
        mockStableMember();
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(new Node(1, "localhost", 9999)));
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());
        // Mimic network timeout
        result.unsentRequests.get(0).handler().onFailure(time.milliseconds(), new TimeoutException("timeout"));

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
        resetWithZeroHeartbeatInterval(Optional.empty());
        mockStableMember();

        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(new Node(1, "localhost", 9999)));
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
    @ApiKeyVersionsSource(apiKey = ApiKeys.CONSUMER_GROUP_HEARTBEAT)
    public void testValidateConsumerGroupHeartbeatRequest(final short version) {
        // The initial heartbeatInterval is set to 0, but we're testing
        resetWithZeroHeartbeatInterval(Optional.of(DEFAULT_GROUP_INSTANCE_ID));
        mockStableMember();

        List<String> subscribedTopics = Collections.singletonList("topic");
        subscriptions.subscribe(new HashSet<>(subscribedTopics), Optional.empty());

        // Update membershipManager's memberId and memberEpoch
        ConsumerGroupHeartbeatResponse result =
            new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
            .setMemberId(memberId)
            .setMemberEpoch(memberEpoch));
        membershipManager.onHeartbeatSuccess(result.data());

        // Create a ConsumerHeartbeatRequest and verify the payload
        NetworkClientDelegate.PollResult pollResult = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, pollResult.unsentRequests.size());
        NetworkClientDelegate.UnsentRequest request = pollResult.unsentRequests.get(0);
        assertInstanceOf(Builder.class, request.requestBuilder());

        ConsumerGroupHeartbeatRequest heartbeatRequest =
            (ConsumerGroupHeartbeatRequest) request.requestBuilder().build(version);
        assertEquals(DEFAULT_GROUP_ID, heartbeatRequest.data().groupId());
        assertEquals(memberId, heartbeatRequest.data().memberId());
        assertEquals(memberEpoch, heartbeatRequest.data().memberEpoch());
        assertEquals(10000, heartbeatRequest.data().rebalanceTimeoutMs());
        assertEquals(subscribedTopics, heartbeatRequest.data().subscribedTopicNames());
        assertEquals(DEFAULT_GROUP_INSTANCE_ID, heartbeatRequest.data().instanceId());
        assertEquals(DEFAULT_REMOTE_ASSIGNOR, heartbeatRequest.data().serverAssignor());
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.CONSUMER_GROUP_HEARTBEAT)
    public void testValidateConsumerGroupHeartbeatRequestAssignmentSentWhenLocalEpochChanges(final short version) {
        CoordinatorRequestManager coordinatorRequestManager = mock(CoordinatorRequestManager.class);
        MembershipManager membershipManager = mock(MembershipManager.class);
        BackgroundEventHandler backgroundEventHandler = mock(BackgroundEventHandler.class);
        SubscriptionState subscriptionState = mock(SubscriptionState.class);
        HeartbeatRequestState requestState = mock(HeartbeatRequestState.class);
        HeartbeatState heartbeatState = new HeartbeatState(subscriptionState, membershipManager, DEFAULT_MAX_POLL_INTERVAL_MS);

        HeartbeatRequestManager heartbeatRequestManager = createHeartbeatRequestManager(
            coordinatorRequestManager,
            membershipManager,
            heartbeatState,
            requestState,
            backgroundEventHandler
        );

        when(membershipManager.shouldHeartbeatNow()).thenReturn(true);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(new Node(1, "localhost", 9999)));

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
        ConsumerGroupHeartbeatRequest heartbeatRequest2 = getHeartbeatRequest(heartbeatRequestManager, version);
        assertNull(heartbeatRequest2.data().topicPartitions());

        // Local epoch bumped, so assignment should be sent
        when(membershipManager.currentAssignment()).thenReturn(new LocalAssignment(1, testAssignment));

        ConsumerGroupHeartbeatRequest heartbeatRequest3 = getHeartbeatRequest(heartbeatRequestManager, version);
        assertEquals(Collections.singletonList(expectedTopicPartitions), heartbeatRequest3.data().topicPartitions());
    }

    private ConsumerGroupHeartbeatRequest getHeartbeatRequest(HeartbeatRequestManager heartbeatRequestManager, final short version) {
        // Create a ConsumerHeartbeatRequest and verify the payload -- no assignment should be sent
        NetworkClientDelegate.PollResult pollResult = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, pollResult.unsentRequests.size());
        NetworkClientDelegate.UnsentRequest request = pollResult.unsentRequests.get(0);
        assertInstanceOf(Builder.class, request.requestBuilder());
        return (ConsumerGroupHeartbeatRequest) request.requestBuilder().build(version);
    }

    @ParameterizedTest
    @MethodSource("errorProvider")
    public void testHeartbeatResponseOnErrorHandling(final Errors error, final boolean isFatal) {
        mockStableMember();

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
                verify(membershipManager).onHeartbeatSuccess(mockResponse.data());
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
                    // The memberStateManager should have stopped heartbeat at this point
                    ensureFatalError();
                } else {
                    verify(backgroundEventHandler, never()).add(any());
                    assertNextHeartbeatTiming(0);
                }
                break;
        }

        if (!isFatal) {
            // Make sure a next heartbeat is sent for all non-fatal errors (to retry or rejoin)
            time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
            result = heartbeatRequestManager.poll(time.milliseconds());
            assertEquals(1, result.unsentRequests.size());
        }
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
        // The initial ConsumerGroupHeartbeatRequest sets most fields to their initial empty values
        ConsumerGroupHeartbeatRequestData data = heartbeatState.buildRequestData();
        assertEquals(ConsumerTestBuilder.DEFAULT_GROUP_ID, data.groupId());
        assertEquals("", data.memberId());
        assertEquals(0, data.memberEpoch());
        assertNull(data.instanceId());
        assertEquals(ConsumerTestBuilder.DEFAULT_MAX_POLL_INTERVAL_MS, data.rebalanceTimeoutMs());
        assertEquals(Collections.emptyList(), data.subscribedTopicNames());
        assertEquals(ConsumerTestBuilder.DEFAULT_REMOTE_ASSIGNOR, data.serverAssignor());
        assertEquals(Collections.emptyList(), data.topicPartitions());
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());

        // Mock a response from the group coordinator, that supplies the member ID and a new epoch
        mockStableMember();
        data = heartbeatState.buildRequestData();
        assertEquals(ConsumerTestBuilder.DEFAULT_GROUP_ID, data.groupId());
        assertEquals(memberId, data.memberId());
        assertEquals(1, data.memberEpoch());
        assertNull(data.instanceId());
        assertEquals(-1, data.rebalanceTimeoutMs());
        assertNull(data.subscribedTopicNames());
        assertNull(data.serverAssignor());
        assertEquals(data.topicPartitions(), Collections.emptyList());
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.STABLE, membershipManager.state());

        // Join the group and subscribe to a topic, but the response has not yet been received
        String topic = "topic1";
        subscriptions.subscribe(Collections.singleton(topic), Optional.empty());
        membershipManager.onSubscriptionUpdated();
        membershipManager.transitionToFenced(); // And indirect way of moving to JOINING state
        data = heartbeatState.buildRequestData();
        assertEquals(ConsumerTestBuilder.DEFAULT_GROUP_ID, data.groupId());
        assertEquals(memberId, data.memberId());
        assertEquals(0, data.memberEpoch());
        assertNull(data.instanceId());
        assertEquals(DEFAULT_MAX_POLL_INTERVAL_MS, data.rebalanceTimeoutMs());
        assertEquals(Collections.singletonList(topic), data.subscribedTopicNames());
        assertEquals(ConsumerTestBuilder.DEFAULT_REMOTE_ASSIGNOR, data.serverAssignor());
        assertEquals(Collections.emptyList(), data.topicPartitions());
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.JOINING, membershipManager.state());

        membershipManager.transitionToFenced();
        data = heartbeatState.buildRequestData();
        assertEquals(ConsumerTestBuilder.DEFAULT_GROUP_ID, data.groupId());
        assertEquals(memberId, data.memberId());
        assertEquals(0, data.memberEpoch());
        assertNull(data.instanceId());
        assertEquals(DEFAULT_MAX_POLL_INTERVAL_MS, data.rebalanceTimeoutMs());
        assertEquals(Collections.singletonList(topic), data.subscribedTopicNames());
        assertEquals(ConsumerTestBuilder.DEFAULT_REMOTE_ASSIGNOR, data.serverAssignor());
        assertEquals(Collections.emptyList(), data.topicPartitions());
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.JOINING, membershipManager.state());

        // Mock the response from the group coordinator which returns an assignment
        ConsumerGroupHeartbeatResponseData.TopicPartitions tpTopic1 =
            new ConsumerGroupHeartbeatResponseData.TopicPartitions();
        Uuid topicId = Uuid.randomUuid();
        tpTopic1.setTopicId(topicId);
        tpTopic1.setPartitions(Collections.singletonList(0));
        ConsumerGroupHeartbeatResponseData.Assignment assignmentTopic1 =
            new ConsumerGroupHeartbeatResponseData.Assignment();
        assignmentTopic1.setTopicPartitions(Collections.singletonList(tpTopic1));
        ConsumerGroupHeartbeatResponse rs1 = new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
                .setHeartbeatIntervalMs(DEFAULT_HEARTBEAT_INTERVAL_MS)
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setAssignment(assignmentTopic1));
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, "topic1"));
        membershipManager.onHeartbeatSuccess(rs1.data());

        // We remain in RECONCILING state, as the assignment will be reconciled on the next poll
        assertEquals(MemberState.RECONCILING, membershipManager.state());
    }

    @Test
    public void testPollTimerExpiration() {
        coordinatorRequestManager = mock(CoordinatorRequestManager.class);
        membershipManager = mock(MembershipManager.class);
        heartbeatState = mock(HeartbeatRequestManager.HeartbeatState.class);
        heartbeatRequestState = spy(new HeartbeatRequestManager.HeartbeatRequestState(
                new LogContext(),
                time,
                DEFAULT_HEARTBEAT_INTERVAL_MS,
                DEFAULT_RETRY_BACKOFF_MS,
                DEFAULT_RETRY_BACKOFF_MAX_MS,
                0));
        backgroundEventHandler = mock(BackgroundEventHandler.class);

        heartbeatRequestManager = createHeartbeatRequestManager(
                coordinatorRequestManager,
                membershipManager,
                heartbeatState,
                heartbeatRequestState,
                backgroundEventHandler);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(new Node(1, "localhost", 9999)));
        when(membershipManager.shouldSkipHeartbeat()).thenReturn(false);

        // On poll timer expiration, the member should send a last heartbeat to leave the group
        // and notify the membership manager
        time.sleep(DEFAULT_MAX_POLL_INTERVAL_MS);
        assertHeartbeat(heartbeatRequestManager, DEFAULT_HEARTBEAT_INTERVAL_MS);
        verify(membershipManager).transitionToSendingLeaveGroup(true);
        verify(heartbeatState).reset();
        verify(heartbeatRequestState).reset();
        verify(membershipManager).onHeartbeatRequestSent();

        when(membershipManager.state()).thenReturn(MemberState.STALE);
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
        Timer pollTimer = spy(time.timer(DEFAULT_MAX_POLL_INTERVAL_MS));
        heartbeatRequestManager = new HeartbeatRequestManager(new LogContext(), pollTimer, config(),
            coordinatorRequestManager, membershipManager, heartbeatState, heartbeatRequestState,
            backgroundEventHandler, metrics);
        when(membershipManager.shouldSkipHeartbeat()).thenReturn(false);

        int exceededTimeMs = 5;
        time.sleep(DEFAULT_MAX_POLL_INTERVAL_MS + exceededTimeMs);

        NetworkClientDelegate.PollResult pollResult = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, pollResult.unsentRequests.size());
        verify(membershipManager).transitionToSendingLeaveGroup(true);
        verify(pollTimer, never()).isExpiredBy();
        assertEquals(exceededTimeMs, pollTimer.isExpiredBy());

        clearInvocations(pollTimer);
        heartbeatRequestManager.resetPollTimer(time.milliseconds());
        verify(pollTimer).isExpiredBy();
    }

    @Test
    public void testHeartbeatMetrics() {
        // setup
        coordinatorRequestManager = mock(CoordinatorRequestManager.class);
        membershipManager = mock(MembershipManager.class);
        heartbeatState = mock(HeartbeatRequestManager.HeartbeatState.class);
        time = new MockTime();
        metrics = new Metrics(time);
        heartbeatRequestState = new HeartbeatRequestManager.HeartbeatRequestState(
            new LogContext(),
            time,
            0, // This initial interval should be 0 to ensure heartbeat on the clock
            DEFAULT_RETRY_BACKOFF_MS,
            DEFAULT_RETRY_BACKOFF_MAX_MS,
            0);
        backgroundEventHandler = mock(BackgroundEventHandler.class);
        heartbeatRequestManager = createHeartbeatRequestManager(
            coordinatorRequestManager,
            membershipManager,
            heartbeatState,
            heartbeatRequestState,
            backgroundEventHandler);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(new Node(1, "localhost", 9999)));
        when(membershipManager.state()).thenReturn(MemberState.STABLE);

        assertNotNull(getMetric("heartbeat-response-time-max"));
        assertNotNull(getMetric("heartbeat-rate"));
        assertNotNull(getMetric("heartbeat-total"));
        assertNotNull(getMetric("last-heartbeat-seconds-ago"));

        // test poll
        assertHeartbeat(heartbeatRequestManager, 0);
        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
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

    @Test
    public void testFencedMemberStopHeartbeatUntilItReleasesAssignmentToRejoin() {
        mockStableMember();

        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());

        // Receive HB response fencing member
        when(subscriptions.hasAutoAssignedPartitions()).thenReturn(true);
        doNothing().when(membershipManager).transitionToFenced();
        ClientResponse response = createHeartbeatResponse(result.unsentRequests.get(0), Errors.FENCED_MEMBER_EPOCH);
        result.unsentRequests.get(0).handler().onComplete(response);

        verify(membershipManager).transitionToFenced();
        verify(heartbeatRequestState).onFailedAttempt(anyLong());
        verify(heartbeatRequestState).reset();

        when(membershipManager.state()).thenReturn(MemberState.FENCED);
        result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(0, result.unsentRequests.size(), "Member should not send heartbeats while FENCED");

        when(membershipManager.state()).thenReturn(MemberState.JOINING);
        result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size(), "Fenced member should resume heartbeat after transitioning to JOINING");
    }

    private void assertHeartbeat(HeartbeatRequestManager hrm, int nextPollMs) {
        NetworkClientDelegate.PollResult pollResult = hrm.poll(time.milliseconds());
        assertEquals(1, pollResult.unsentRequests.size());
        assertEquals(nextPollMs, pollResult.timeUntilNextPollMs);
        pollResult.unsentRequests.get(0).handler().onComplete(createHeartbeatResponse(pollResult.unsentRequests.get(0),
            Errors.NONE));
    }

    private void assertNoHeartbeat(HeartbeatRequestManager hrm) {
        NetworkClientDelegate.PollResult pollResult = hrm.poll(time.milliseconds());
        assertEquals(0, pollResult.unsentRequests.size());
    }

    private void mockStableMember() {
        membershipManager.onSubscriptionUpdated();
        // Heartbeat response without assignment to set the state to STABLE.
        when(subscriptions.hasAutoAssignedPartitions()).thenReturn(true);
        when(subscriptions.rebalanceListener()).thenReturn(Optional.empty());
        ConsumerGroupHeartbeatResponse rs1 = new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
                .setHeartbeatIntervalMs(DEFAULT_HEARTBEAT_INTERVAL_MS)
                .setMemberId(memberId)
                .setMemberEpoch(memberEpoch)
                .setAssignment(new Assignment())
        );
        membershipManager.onHeartbeatSuccess(rs1.data());
        membershipManager.poll(time.milliseconds());
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.STABLE, membershipManager.state());
    }

    private void ensureFatalError() {
        verify(membershipManager).transitionToFatal();
        verify(backgroundEventHandler).add(any());
        ensureHeartbeatStopped();
    }

    private void ensureHeartbeatStopped() {
        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        assertEquals(MemberState.FATAL, membershipManager.state());
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
        final Errors error
    ) {
        ConsumerGroupHeartbeatResponseData data = new ConsumerGroupHeartbeatResponseData()
            .setErrorCode(error.code())
            .setHeartbeatIntervalMs(DEFAULT_HEARTBEAT_INTERVAL_MS)
            .setMemberId(memberId)
            .setMemberEpoch(memberEpoch);
        if (error != Errors.NONE) {
            data.setErrorMessage("stubbed error message");
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

    private KafkaMetric getMetric(final String name) {
        return metrics.metrics().get(metrics.metricName(name, CONSUMER_COORDINATOR_METRICS));
    }

    private HeartbeatRequestManager createHeartbeatRequestManager(
            final CoordinatorRequestManager coordinatorRequestManager,
            final MembershipManager membershipManager,
            final HeartbeatRequestManager.HeartbeatState heartbeatState,
            final HeartbeatRequestManager.HeartbeatRequestState heartbeatRequestState,
            final BackgroundEventHandler backgroundEventHandler) {
        LogContext logContext = new LogContext();
        pollTimer = time.timer(DEFAULT_MAX_POLL_INTERVAL_MS);
        return new HeartbeatRequestManager(
                logContext,
                pollTimer,
                config(),
                coordinatorRequestManager,
                membershipManager,
                heartbeatState,
                heartbeatRequestState,
                backgroundEventHandler,
                metrics);
    }
}
