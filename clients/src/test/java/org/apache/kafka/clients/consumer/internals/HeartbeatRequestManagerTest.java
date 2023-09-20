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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HeartbeatRequestManagerTest {

    private final int heartbeatInterval = 1000;
    private final long retryBackoffMaxMs = 3000;
    private final long retryBackoffMs = 100;
    private final String groupId = "group-id";

    private Time mockTime;
    private LogContext mockLogContext;
    private CoordinatorRequestManager mockCoordinatorRequestManager;
    private SubscriptionState mockSubscriptionState;
    private HeartbeatRequestManager heartbeatRequestManager;
    private MembershipManager mockMembershipManager;
    private HeartbeatRequestManager.HeartbeatRequestState heartbeatRequestState;
    private ConsumerConfig config;

    private String memberId = "member-id";
    private int memberEpoch = 1;
    private ConsumerGroupHeartbeatResponseData.Assignment memberAssignment = mockAssignment();
    private ErrorEventHandler errorEventHandler;

    private ConsumerGroupHeartbeatResponseData.Assignment mockAssignment() {
        return new ConsumerGroupHeartbeatResponseData.Assignment()
            .setAssignedTopicPartitions(Arrays.asList(
                new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                    .setTopicId(Uuid.randomUuid())
                    .setPartitions(Arrays.asList(0, 1, 2)),
                new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                    .setTopicId(Uuid.randomUuid())
                    .setPartitions(Arrays.asList(3, 4, 5))
            ));
    }

    @BeforeEach
    public void setUp() {
        mockTime = new MockTime();
        mockLogContext = new LogContext();
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(RETRY_BACKOFF_MS_CONFIG, "100");
        config = new ConsumerConfig(properties);
        mockCoordinatorRequestManager = mock(CoordinatorRequestManager.class);
        when(mockCoordinatorRequestManager.coordinator()).thenReturn(Optional.of(new Node(1, "localhost", 9999)));
        mockSubscriptionState = mock(SubscriptionState.class);
        mockMembershipManager = spy(new MembershipManagerImpl(groupId));
        heartbeatRequestState = mock(HeartbeatRequestManager.HeartbeatRequestState.class);
        errorEventHandler = mock(ErrorEventHandler.class);
        heartbeatRequestManager = new HeartbeatRequestManager(
            mockTime,
            mockLogContext,
            config,
            mockCoordinatorRequestManager,
            mockSubscriptionState,
            mockMembershipManager,
            heartbeatRequestState,
            errorEventHandler);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSendHeartbeatOnMemberState(boolean shouldSendHeartbeat) {
        // Mocking notInGroup
        when(mockMembershipManager.shouldSendHeartbeat()).thenReturn(shouldSendHeartbeat);
        when(heartbeatRequestState.canSendRequest(anyLong())).thenReturn(true);

        NetworkClientDelegate.PollResult result;
        result = heartbeatRequestManager.poll(mockTime.milliseconds());

        assertEquals(Long.MAX_VALUE, result.timeUntilNextPollMs);

        if (shouldSendHeartbeat) {
            assertEquals(1, result.unsentRequests.size());
        } else {
            assertEquals(0, result.unsentRequests.size());
        }
    }

    @ParameterizedTest
    @MethodSource("stateProvider")
    public void testTimerNotDue(final MemberState state) {
        this.heartbeatRequestState = new HeartbeatRequestManager.HeartbeatRequestState(
            mockLogContext,
            mockTime,
            heartbeatInterval,
            retryBackoffMs,
            retryBackoffMaxMs);
        heartbeatRequestManager = createManager();

        when(mockMembershipManager.state()).thenReturn(state);
        mockTime.sleep(100); // time elapsed < heartbeatInterval, no heartbeat should be sent
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(mockTime.milliseconds());
        assertEquals(0, result.unsentRequests.size());

        if (mockMembershipManager.shouldSendHeartbeat()) {
            assertEquals(heartbeatInterval - 100, result.timeUntilNextPollMs);
        } else {
            assertEquals(Long.MAX_VALUE, result.timeUntilNextPollMs);
        }
    }

    @Test
    public void testNoCoordinator() {
        when(mockCoordinatorRequestManager.coordinator()).thenReturn(Optional.empty());
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(mockTime.milliseconds());

        assertEquals(Long.MAX_VALUE, result.timeUntilNextPollMs);
        assertEquals(0, result.unsentRequests.size());
    }

    @ParameterizedTest
    @MethodSource("errorProvider")
    public void testHeartbeatResponseOnErrorHandling(final Errors error, final boolean isFatal) {
        heartbeatRequestState = new HeartbeatRequestManager.HeartbeatRequestState(
            mockLogContext,
            mockTime,
            heartbeatInterval,
            retryBackoffMs,
            retryBackoffMaxMs,
            0);
        errorEventHandler = mock(ErrorEventHandler.class);
        when(mockMembershipManager.state()).thenReturn(MemberState.RECONCILING);
        heartbeatRequestManager = createManager();

        mockTime.sleep(1001);
        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(mockTime.milliseconds());
        assertEquals(1, result.unsentRequests.size());

        // manually completing the response to test error handling
        ClientResponse response = createHeartbeatResponse(
            result.unsentRequests.get(0),
            error);
        result.unsentRequests.get(0).future().complete(response);
        ConsumerGroupHeartbeatResponse mockResponse = (ConsumerGroupHeartbeatResponse) response.responseBody();

        short errorCode = error.code();
        if (errorCode == Errors.NONE.code()) {
            verify(errorEventHandler, never()).handle(any());
            verify(mockMembershipManager).updateState(mockResponse.data());
            assertEquals(heartbeatInterval, heartbeatRequestState.nextHeartbeatMs(mockTime.milliseconds()));
        } else if (isFatal) {
            verify(errorEventHandler).handle(any());
            assertEquals(retryBackoffMs, heartbeatRequestState.nextHeartbeatMs(mockTime.milliseconds()));
            if (errorCode == Errors.UNRELEASED_INSTANCE_ID.code()) {
                verify(mockMembershipManager).failMember();
            } else if (errorCode == Errors.FENCED_MEMBER_EPOCH.code() ||
                errorCode == Errors.UNKNOWN_MEMBER_ID.code()) {
                verify(mockMembershipManager).fenceMember();
            }
            // The memberStateManager should have stopped heartbeat at this point
        } else {
            // NOT_COORDINATOR/COORDINATOR_LOAD_IN_PROGRESS/COORDINATOR_NOT_AVAILABLE
            // should be automatically retried
            verify(errorEventHandler, never()).handle(any());
            assertEquals(retryBackoffMs, heartbeatRequestState.nextHeartbeatMs(mockTime.milliseconds()));
        }
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
        ConsumerGroupHeartbeatResponse response = new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
            .setErrorCode(error.code())
            .setHeartbeatIntervalMs(heartbeatInterval)
            .setMemberId(memberId)
            .setMemberEpoch(memberEpoch)
            .setAssignment(memberAssignment));

        return new ClientResponse(
            new RequestHeader(ApiKeys.CONSUMER_GROUP_HEARTBEAT, ApiKeys.CONSUMER_GROUP_HEARTBEAT.latestVersion(), "client-id", 1),
            request.callback(),
            "0",
            mockTime.milliseconds(),
            mockTime.milliseconds(),
            false,
            null,
            null,
            response);
    }

    private HeartbeatRequestManager createManager() {
        return new HeartbeatRequestManager(
            mockTime,
            mockLogContext,
            config,
            mockCoordinatorRequestManager,
            mockSubscriptionState,
            mockMembershipManager,
            heartbeatRequestState,
            errorEventHandler);
    }
}
