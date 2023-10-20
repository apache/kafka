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
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;
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
import java.util.Optional;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HeartbeatRequestManagerTest {

    private static final int HEARTBEAT_INTERVAL_MS = 1000;
    private static final long RETRY_BACKOFF_MAX_MS = 3000;
    private static final long RETRY_BACKOFF_MS = 100;
    private static final String GROUP_INSTANCE_ID = "group-instance-id";
    private static final String GROUP_ID = "group-id";

    private Time time;
    private LogContext logContext;
    private CoordinatorRequestManager coordinatorRequestManager;
    private SubscriptionState subscriptionState;
    private HeartbeatRequestManager heartbeatRequestManager;
    private MembershipManager membershipManager;
    private HeartbeatRequestManager.HeartbeatRequestState heartbeatRequestState;
    private ConsumerConfig config;

    private String memberId = "member-id";
    private int memberEpoch = 1;
    private ErrorEventHandler errorEventHandler;

    @BeforeEach
    public void setUp() {
        time = new MockTime();
        logContext = new LogContext();
        config = new ConsumerConfig(createConsumerConfig());
        coordinatorRequestManager = mock(CoordinatorRequestManager.class);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(new Node(1, "localhost", 9999)));
        subscriptionState = mock(SubscriptionState.class);
        membershipManager = spy(new MembershipManagerImpl(GROUP_ID, logContext));
        heartbeatRequestState = mock(HeartbeatRequestManager.HeartbeatRequestState.class);
        errorEventHandler = mock(ErrorEventHandler.class);
        heartbeatRequestManager = createManager();
    }

    private Properties createConsumerConfig() {
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(RETRY_BACKOFF_MS_CONFIG, "100");
        return properties;
    }

    @Test
    public void testHeartbeatOnStartup() {
        // The initial heartbeatInterval is set to 0
        heartbeatRequestState = new HeartbeatRequestManager.HeartbeatRequestState(
            logContext,
            time,
            0,
            RETRY_BACKOFF_MS,
            RETRY_BACKOFF_MAX_MS,
            0);
        heartbeatRequestManager = createManager();
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());

        // Ensure we do not resend the request without the first request being completed
        NetworkClientDelegate.PollResult result2 = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(0, result2.unsentRequests.size());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSendHeartbeatOnMemberState(final boolean shouldSendHeartbeat) {
        // Mocking notInGroup
        when(membershipManager.shouldSendHeartbeat()).thenReturn(shouldSendHeartbeat);
        when(heartbeatRequestState.canSendRequest(anyLong())).thenReturn(true);

        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

        if (shouldSendHeartbeat) {
            assertEquals(1, result.unsentRequests.size());
            assertEquals(0, result.timeUntilNextPollMs);
        } else {
            assertEquals(0, result.unsentRequests.size());
            assertEquals(Long.MAX_VALUE, result.timeUntilNextPollMs);

        }
    }

    @ParameterizedTest
    @MethodSource("stateProvider")
    public void testTimerNotDue(final MemberState state) {
        heartbeatRequestState = new HeartbeatRequestManager.HeartbeatRequestState(
            logContext,
            time,
            HEARTBEAT_INTERVAL_MS,
            RETRY_BACKOFF_MS,
            RETRY_BACKOFF_MAX_MS);
        heartbeatRequestManager = createManager();

        when(membershipManager.state()).thenReturn(state);
        time.sleep(100); // time elapsed < heartbeatInterval, no heartbeat should be sent
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(0, result.unsentRequests.size());

        if (membershipManager.shouldSendHeartbeat()) {
            assertEquals(HEARTBEAT_INTERVAL_MS - 100, result.timeUntilNextPollMs);
        } else {
            assertEquals(Long.MAX_VALUE, result.timeUntilNextPollMs);
        }
    }

    @Test
    public void testNetworkTimeout() {
        heartbeatRequestState = new HeartbeatRequestManager.HeartbeatRequestState(
            logContext,
            time,
            0,
            RETRY_BACKOFF_MS,
            RETRY_BACKOFF_MAX_MS,
            0);
        heartbeatRequestManager = createManager();
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(new Node(1, "localhost", 9999)));
        when(membershipManager.shouldSendHeartbeat()).thenReturn(true);
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());
        // Mimic network timeout
        result.unsentRequests.get(0).handler().onFailure(time.milliseconds(), new TimeoutException("timeout"));

        // Assure the manager will backoff on timeout
        time.sleep(RETRY_BACKOFF_MS - 1);
        result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(0, result.unsentRequests.size());

        time.sleep(1);
        result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());
    }

    @Test
    public void testFailureOnFatalException() {
        heartbeatRequestState = spy(new HeartbeatRequestManager.HeartbeatRequestState(
            logContext,
            time,
            0,
            RETRY_BACKOFF_MS,
            RETRY_BACKOFF_MAX_MS,
            0));
        heartbeatRequestManager = createManager();
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(new Node(1, "localhost", 9999)));
        when(membershipManager.shouldSendHeartbeat()).thenReturn(true);
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());
        result.unsentRequests.get(0).future().completeExceptionally(new KafkaException("fatal"));
        verify(membershipManager).transitionToFailed();
        verify(errorEventHandler).handle(any());
    }

    @Test
    public void testNoCoordinator() {
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.empty());
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

        assertEquals(Long.MAX_VALUE, result.timeUntilNextPollMs);
        assertEquals(0, result.unsentRequests.size());
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.CONSUMER_GROUP_HEARTBEAT)
    public void testValidateConsumerGroupHeartbeatRequest(final short version) {
        List<String> subscribedTopics = Collections.singletonList("topic");
        subscriptionState = new SubscriptionState(logContext, OffsetResetStrategy.NONE);
        subscriptionState.subscribe(new HashSet<>(subscribedTopics), new NoOpConsumerRebalanceListener());

        Properties prop = createConsumerConfig();
        prop.setProperty(MAX_POLL_INTERVAL_MS_CONFIG, "10000");
        config = new ConsumerConfig(prop);
        membershipManager = new MembershipManagerImpl(GROUP_ID, GROUP_INSTANCE_ID, null, logContext);
        heartbeatRequestState = new HeartbeatRequestManager.HeartbeatRequestState(
            logContext,
            time,
            0,
            RETRY_BACKOFF_MS,
            RETRY_BACKOFF_MAX_MS,
            0);
        heartbeatRequestManager = createManager();
        // Update membershipManager's memberId and memberEpoch
        ConsumerGroupHeartbeatResponse result =
            new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
            .setMemberId(memberId)
            .setMemberEpoch(memberEpoch));
        membershipManager.updateState(result.data());

        // Create a ConsumerHeartbeatRequest and verify the payload
        NetworkClientDelegate.PollResult pollResult = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, pollResult.unsentRequests.size());
        NetworkClientDelegate.UnsentRequest request = pollResult.unsentRequests.get(0);
        assertTrue(request.requestBuilder() instanceof ConsumerGroupHeartbeatRequest.Builder);

        ConsumerGroupHeartbeatRequest heartbeatRequest =
            (ConsumerGroupHeartbeatRequest) request.requestBuilder().build(version);
        assertEquals(GROUP_ID, heartbeatRequest.data().groupId());
        assertEquals(memberId, heartbeatRequest.data().memberId());
        assertEquals(memberEpoch, heartbeatRequest.data().memberEpoch());
        assertEquals(10000, heartbeatRequest.data().rebalanceTimeoutMs());
        assertEquals(subscribedTopics, heartbeatRequest.data().subscribedTopicNames());
        assertEquals(GROUP_INSTANCE_ID, heartbeatRequest.data().instanceId());
        // TODO: Test pattern subscription and user provided assignor selection.
        assertNull(heartbeatRequest.data().serverAssignor());
        assertNull(heartbeatRequest.data().subscribedTopicRegex());
    }

    @ParameterizedTest
    @MethodSource("errorProvider")
    public void testHeartbeatResponseOnErrorHandling(final Errors error, final boolean isFatal) {
        heartbeatRequestState = new HeartbeatRequestManager.HeartbeatRequestState(
            logContext,
            time,
            HEARTBEAT_INTERVAL_MS,
            RETRY_BACKOFF_MS,
            RETRY_BACKOFF_MAX_MS,
            0);
        heartbeatRequestManager = createManager();

        // Sending first heartbeat w/o assignment to set the state to STABLE
        ConsumerGroupHeartbeatResponse rs1 = new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
            .setHeartbeatIntervalMs(HEARTBEAT_INTERVAL_MS)
            .setMemberId(memberId)
            .setMemberEpoch(memberEpoch));
        membershipManager.updateState(rs1.data());
        assertEquals(MemberState.STABLE, membershipManager.state());

        // Handling errors on the second heartbeat
        time.sleep(HEARTBEAT_INTERVAL_MS);
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());

        // Manually completing the response to test error handling
        ClientResponse response = createHeartbeatResponse(
            result.unsentRequests.get(0),
            error);
        result.unsentRequests.get(0).handler().onComplete(response);
        ConsumerGroupHeartbeatResponse mockResponse = (ConsumerGroupHeartbeatResponse) response.responseBody();

        switch (error) {
            case NONE:
                verify(errorEventHandler, never()).handle(any());
                verify(membershipManager, times(2)).updateState(mockResponse.data());
                assertEquals(HEARTBEAT_INTERVAL_MS, heartbeatRequestState.nextHeartbeatMs(time.milliseconds()));
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                verify(errorEventHandler, never()).handle(any());
                assertEquals(RETRY_BACKOFF_MS, heartbeatRequestState.nextHeartbeatMs(time.milliseconds()));
                break;

            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                verify(errorEventHandler, never()).handle(any());
                verify(coordinatorRequestManager).markCoordinatorUnknown(any(), anyLong());
                assertEquals(0, heartbeatRequestState.nextHeartbeatMs(time.milliseconds()));
                break;

            default:
                if (isFatal) {
                    // The memberStateManager should have stopped heartbeat at this point
                    ensureFatalError();
                } else {
                    verify(errorEventHandler, never()).handle(any());
                    assertEquals(0, heartbeatRequestState.nextHeartbeatMs(time.milliseconds()));
                }
                break;
        }
    }

    private void ensureFatalError() {
        verify(membershipManager).transitionToFailed();
        verify(errorEventHandler).handle(any());
        ensureHeartbeatStopped();
    }

    private void ensureHeartbeatStopped() {
        time.sleep(HEARTBEAT_INTERVAL_MS);
        assertEquals(MemberState.FAILED, membershipManager.state());
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

    private static Collection<Arguments> stateProvider() {
        return Arrays.asList(
            Arguments.of(MemberState.UNJOINED),
            Arguments.of(MemberState.RECONCILING),
            Arguments.of(MemberState.FAILED),
            Arguments.of(MemberState.STABLE),
            Arguments.of(MemberState.FENCED));
    }

    private ClientResponse createHeartbeatResponse(
        final NetworkClientDelegate.UnsentRequest request,
        final Errors error) {
        ConsumerGroupHeartbeatResponseData data = new ConsumerGroupHeartbeatResponseData()
            .setErrorCode(error.code())
            .setHeartbeatIntervalMs(HEARTBEAT_INTERVAL_MS)
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

    private HeartbeatRequestManager createManager() {
        return new HeartbeatRequestManager(
            logContext,
            time,
            config,
            coordinatorRequestManager,
            subscriptionState,
            membershipManager,
            heartbeatRequestState,
            errorEventHandler);
    }
}
