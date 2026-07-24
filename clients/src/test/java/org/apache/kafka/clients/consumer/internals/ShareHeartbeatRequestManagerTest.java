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
import org.apache.kafka.common.errors.UnsupportedVersionException;
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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager.SHARE_PROTOCOL_NOT_SUPPORTED_MSG;
import static org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager.SHARE_PROTOCOL_VERSION_NOT_SUPPORTED_MSG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShareHeartbeatRequestManagerTest
        extends AbstractHeartbeatRequestManagerTest<ShareGroupHeartbeatResponse> {

    private static final String SHARE_CONSUMER_COORDINATOR_METRICS = "consumer-share-coordinator-metrics";

    // Shadows the base field so subclass-only tests can access ShareMembershipManager-typed
    // methods. The subclass setUp() assigns the same mock to super.membershipManager so
    // inherited tests see the same instance.
    private ShareMembershipManager membershipManager;
    private ShareHeartbeatRequestManager heartbeatRequestManager;
    private Metadata metadata;
    private ShareHeartbeatRequestManager.HeartbeatState heartbeatState;
    private Metrics metrics;
    private LogContext logContext;

    public ShareHeartbeatRequestManagerTest() {
        super(ShareGroupHeartbeatResponse.class);
    }

    @BeforeEach
    public void setUp() {
        time = new MockTime();
        pollTimer = spy(time.timer(DEFAULT_MAX_POLL_INTERVAL_MS));
        coordinatorRequestManager = mock(CoordinatorRequestManager.class);
        subscriptions = mock(SubscriptionState.class);
        backgroundEventHandler = mock(BackgroundEventHandler.class);
        membershipManager = mock(ShareMembershipManager.class);
        super.membershipManager = membershipManager;
        heartbeatState = mock(ShareHeartbeatRequestManager.HeartbeatState.class);
        metadata = mock(ConsumerMetadata.class);
        metrics = new Metrics(time);
        logContext = new LogContext();
        ConsumerConfig config = mock(ConsumerConfig.class);

        heartbeatRequestState = spy(new HeartbeatRequestState(
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

        super.heartbeatRequestManager = this.heartbeatRequestManager;

        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(new Node(1, "localhost", 9999)));
    }

    private void createHeartbeatRequestStateWithZeroHeartbeatInterval() {
        heartbeatRequestState = spy(new HeartbeatRequestState(logContext,
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

    /**
     * ShareConsumerImpl runs on the same ConsumerNetworkThread as the AsyncKafkaConsumer, thus we also test to ensure
     * we don't enter a CPU spin state. Refer to KAFKA-20253
     */
    @Test
    public void testMaximumTimeToWaitWhenHeartbeatShouldBeSkippedDoesNotSpin() {
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(new Node(1, "localhost", 9999)));
        when(membershipManager.state()).thenReturn(MemberState.FATAL);
        when(membershipManager.shouldSkipHeartbeat()).thenReturn(true);
        when(heartbeatRequestState.timeToNextHeartbeatMs(anyLong())).thenReturn(0L);

        long result = heartbeatRequestManager.maximumTimeToWait(time.milliseconds());

        assertTrue(result > 0,
            "maximumTimeToWait must be > 0 while heartbeats are skipped to avoid a busy-spin; got " + result);
        assertEquals(DEFAULT_HEARTBEAT_INTERVAL_MS, result);
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
    @ApiKeyVersionsSource(apiKey = ApiKeys.SHARE_GROUP_HEARTBEAT)
    public void testFirstHeartbeatIncludesRequiredInfoToJoinGroupAndGetAssignments(short version) {
        createHeartbeatStateAndRequestManager();
        createHeartbeatRequestStateWithZeroHeartbeatInterval();
        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        String topic = "topic1";
        Set<String> set = Set.of(topic);
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
        assertEquals(List.of(topic), heartbeatRequest.data().subscribedTopicNames());
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

    @ParameterizedTest
    @ValueSource(strings = {SHARE_PROTOCOL_NOT_SUPPORTED_MSG})
    public void testUnsupportedVersionGeneratedOnTheBroker(String errorMsg) {
        mockResponseWithException(new UnsupportedVersionException(errorMsg), true);

        ArgumentCaptor<ErrorEvent> errorEventArgumentCaptor = ArgumentCaptor.forClass(ErrorEvent.class);
        verify(backgroundEventHandler).add(errorEventArgumentCaptor.capture());
        ErrorEvent errorEvent = errorEventArgumentCaptor.getValue();
        assertInstanceOf(Errors.UNSUPPORTED_VERSION.exception().getClass(), errorEvent.error());
        assertEquals(errorMsg, errorEvent.error().getMessage());
        clearInvocations(backgroundEventHandler);
    }

    @ParameterizedTest
    @ValueSource(strings = {SHARE_PROTOCOL_VERSION_NOT_SUPPORTED_MSG})
    public void testUnsupportedVersionGeneratedOnTheClient(String errorMsg) {
        mockResponseWithException(new UnsupportedVersionException(errorMsg), false);

        ArgumentCaptor<ErrorEvent> errorEventArgumentCaptor = ArgumentCaptor.forClass(ErrorEvent.class);
        verify(backgroundEventHandler).add(errorEventArgumentCaptor.capture());
        ErrorEvent errorEvent = errorEventArgumentCaptor.getValue();
        assertInstanceOf(Errors.UNSUPPORTED_VERSION.exception().getClass(), errorEvent.error());
        assertEquals(errorMsg, errorEvent.error().getMessage());
        clearInvocations(backgroundEventHandler);
    }

    private void mockResponseWithException(UnsupportedVersionException exception, boolean isFromBroker) {
        time.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());

        // Manually completing the response to test error handling
        when(subscriptions.hasAutoAssignedPartitions()).thenReturn(true);
        ClientResponse response = createHeartbeatResponseWithException(
                result.unsentRequests.get(0),
                exception,
                isFromBroker);
        result.unsentRequests.get(0).handler().onComplete(response);
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
        assertEquals(List.of(), data.subscribedTopicNames());
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
        subscriptions.subscribe(Set.of(topic), Optional.empty());
        when(subscriptions.subscription()).thenReturn(Set.of(topic));
        mockRejoiningMemberData();
        data = heartbeatState.buildRequestData();
        assertEquals(DEFAULT_GROUP_ID, data.groupId());
        assertEquals(DEFAULT_MEMBER_ID, data.memberId());
        assertEquals(0, data.memberEpoch());
        assertEquals(List.of(topic), data.subscribedTopicNames());
        membershipManager.onHeartbeatRequestGenerated();

        data = heartbeatState.buildRequestData();
        assertEquals(DEFAULT_GROUP_ID, data.groupId());
        assertEquals(DEFAULT_MEMBER_ID, data.memberId());
        assertEquals(0, data.memberEpoch());
        assertEquals(List.of(topic), data.subscribedTopicNames());

        // Mock the response from the group coordinator which returns an assignment
        ShareGroupHeartbeatResponseData.TopicPartitions tpTopic1 =
                new ShareGroupHeartbeatResponseData.TopicPartitions();
        Uuid topicId = Uuid.randomUuid();
        tpTopic1.setTopicId(topicId);
        tpTopic1.setPartitions(List.of(0));
        ShareGroupHeartbeatResponseData.Assignment assignmentTopic1 =
                new ShareGroupHeartbeatResponseData.Assignment();
        assignmentTopic1.setTopicPartitions(List.of(tpTopic1));
        ShareGroupHeartbeatResponse rs1 = new ShareGroupHeartbeatResponse(new ShareGroupHeartbeatResponseData()
                .setHeartbeatIntervalMs(DEFAULT_HEARTBEAT_INTERVAL_MS)
                .setMemberId(DEFAULT_MEMBER_ID)
                .setMemberEpoch(DEFAULT_MEMBER_EPOCH)
                .setAssignment(assignmentTopic1));
        when(metadata.topicNames()).thenReturn(Map.of(topicId, "topic1"));
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

    private void assertHeartbeat(AbstractHeartbeatRequestManager<ShareGroupHeartbeatResponse> hrm, int nextPollMs) {
        NetworkClientDelegate.PollResult pollResult = hrm.poll(time.milliseconds());
        assertEquals(1, pollResult.unsentRequests.size());
        assertEquals(nextPollMs, pollResult.timeUntilNextPollMs);
        pollResult.unsentRequests.get(0).handler().onComplete(createHeartbeatResponse(pollResult.unsentRequests.get(0),
                Errors.NONE));
    }

    private void assertNoHeartbeat(AbstractHeartbeatRequestManager<ShareGroupHeartbeatResponse> hrm) {
        NetworkClientDelegate.PollResult pollResult = hrm.poll(time.milliseconds());
        assertEquals(0, pollResult.unsentRequests.size());
    }

    @Override
    protected ClientResponse createHeartbeatResponse(
            final NetworkClientDelegate.UnsentRequest request,
            final Errors error) {
        return createHeartbeatResponse(request, error, DEFAULT_HEARTBEAT_INTERVAL_MS);
    }

    @Override
    protected ClientResponse createHeartbeatResponse(
            final NetworkClientDelegate.UnsentRequest request,
            final Errors error,
            final int heartbeatIntervalMs) {
        ShareGroupHeartbeatResponseData data = new ShareGroupHeartbeatResponseData()
                .setErrorCode(error.code())
                .setHeartbeatIntervalMs(heartbeatIntervalMs)
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

    private ClientResponse createHeartbeatResponseWithException(
            final NetworkClientDelegate.UnsentRequest request,
            final UnsupportedVersionException exception,
            final boolean isFromBroker
    ) {
        ShareGroupHeartbeatResponse response = null;
        if (isFromBroker) {
            response = new ShareGroupHeartbeatResponse(new ShareGroupHeartbeatResponseData().setErrorCode(Errors.UNSUPPORTED_VERSION.code()));
        }
        return new ClientResponse(
                new RequestHeader(ApiKeys.SHARE_GROUP_HEARTBEAT, ApiKeys.SHARE_GROUP_HEARTBEAT.latestVersion(), "client-id", 1),
                request.handler(),
                "0",
                time.milliseconds(),
                time.milliseconds(),
                false,
                isFromBroker ? null : exception,
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
            final HeartbeatRequestState heartbeatRequestState,
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
        when(membershipManager.currentAssignment()).thenReturn(new AbstractMembershipManager.LocalAssignment(0, Map.of()));
        when(membershipManager.groupId()).thenReturn(DEFAULT_GROUP_ID);
        when(membershipManager.memberId()).thenReturn(DEFAULT_MEMBER_ID);
        when(membershipManager.memberEpoch()).thenReturn(DEFAULT_MEMBER_EPOCH);
    }
}
