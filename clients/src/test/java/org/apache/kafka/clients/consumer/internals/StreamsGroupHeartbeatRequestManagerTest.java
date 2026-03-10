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
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StreamsGroupHeartbeatRequestManagerTest {

    private static final LogContext LOG_CONTEXT = new LogContext("test");
    private static final long RECEIVED_HEARTBEAT_INTERVAL_MS = 1200;
    private static final int DEFAULT_MAX_POLL_INTERVAL_MS = 10000;
    private static final String GROUP_ID = "group-id";
    private static final String MEMBER_ID = "member-id";
    private static final int MEMBER_EPOCH = 1;
    private static final String INSTANCE_ID = "instance-id";
    private static final UUID PROCESS_ID = UUID.randomUUID();
    private static final StreamsRebalanceData.HostInfo ENDPOINT = new StreamsRebalanceData.HostInfo("localhost", 8080);
    private static final String SOURCE_TOPIC_1 = "sourceTopic1";
    private static final String SOURCE_TOPIC_2 = "sourceTopic2";
    private static final Set<String> SOURCE_TOPICS = Set.of(SOURCE_TOPIC_1, SOURCE_TOPIC_2);
    private static final String REPARTITION_SINK_TOPIC_1 = "repartitionSinkTopic1";
    private static final String REPARTITION_SINK_TOPIC_2 = "repartitionSinkTopic2";
    private static final String REPARTITION_SINK_TOPIC_3 = "repartitionSinkTopic3";
    private static final Set<String> REPARTITION_SINK_TOPICS = Set.of(
        REPARTITION_SINK_TOPIC_1,
        REPARTITION_SINK_TOPIC_2,
        REPARTITION_SINK_TOPIC_3
    );
    private static final String REPARTITION_SOURCE_TOPIC_1 = "repartitionSourceTopic1";
    private static final String REPARTITION_SOURCE_TOPIC_2 = "repartitionSourceTopic2";
    private static final Map<String, StreamsRebalanceData.TopicInfo> REPARTITION_SOURCE_TOPICS = Map.of(
        REPARTITION_SOURCE_TOPIC_1, new StreamsRebalanceData.TopicInfo(Optional.of(2), Optional.of((short) 1), Map.of("config3", "value3", "config1", "value1")),
        REPARTITION_SOURCE_TOPIC_2, new StreamsRebalanceData.TopicInfo(Optional.of(3), Optional.of((short) 3), Collections.emptyMap())
    );
    private static final String CHANGELOG_TOPIC_1 = "changelogTopic1";
    private static final String CHANGELOG_TOPIC_2 = "changelogTopic2";
    private static final String CHANGELOG_TOPIC_3 = "changelogTopic3";
    private static final Map<String, StreamsRebalanceData.TopicInfo> CHANGELOG_TOPICS = Map.of(
        CHANGELOG_TOPIC_1, new StreamsRebalanceData.TopicInfo(Optional.empty(), Optional.of((short) 1), Map.of()),
        CHANGELOG_TOPIC_2, new StreamsRebalanceData.TopicInfo(Optional.empty(), Optional.of((short) 2), Map.of()),
        CHANGELOG_TOPIC_3, new StreamsRebalanceData.TopicInfo(Optional.empty(), Optional.of((short) 3), Map.of("config4", "value4", "config2", "value2"))
    );
    private static final Collection<Set<String>> COPARTITION_GROUP = Set.of(
        Set.of(SOURCE_TOPIC_1, REPARTITION_SOURCE_TOPIC_2),
        Set.of(SOURCE_TOPIC_2, REPARTITION_SOURCE_TOPIC_1)
    );
    private static final String SUBTOPOLOGY_NAME_1 = "subtopology1";
    private static final StreamsRebalanceData.Subtopology SUBTOPOLOGY_1 = new StreamsRebalanceData.Subtopology(
        SOURCE_TOPICS,
        REPARTITION_SINK_TOPICS,
        REPARTITION_SOURCE_TOPICS,
        CHANGELOG_TOPICS,
        COPARTITION_GROUP
    );
    private static final String SUBTOPOLOGY_NAME_2 = "subtopology2";
    private static final String SOURCE_TOPIC_3 = "sourceTopic3";
    private static final String CHANGELOG_TOPIC_4 = "changelogTopic4";
    private static final StreamsRebalanceData.Subtopology SUBTOPOLOGY_2 = new StreamsRebalanceData.Subtopology(
        Set.of(SOURCE_TOPIC_3),
        Set.of(),
        Map.of(),
        Map.of(CHANGELOG_TOPIC_4, new StreamsRebalanceData.TopicInfo(Optional.empty(), Optional.of((short) 1), Map.of())),
        Collections.emptyList()
    );
    private static final Map<String, StreamsRebalanceData.Subtopology> SUBTOPOLOGIES =
        Map.of(
            SUBTOPOLOGY_NAME_1, SUBTOPOLOGY_1,
            SUBTOPOLOGY_NAME_2, SUBTOPOLOGY_2
        );
    private static final String CLIENT_TAG_1 = "client-tag1";
    private static final String VALUE_1 = "value1";
    private static final Map<String, String> CLIENT_TAGS = Map.of(CLIENT_TAG_1, VALUE_1);
    private static final List<StreamsGroupHeartbeatResponseData.EndpointToPartitions> ENDPOINT_TO_PARTITIONS =
        List.of(
            new StreamsGroupHeartbeatResponseData.EndpointToPartitions()
                .setUserEndpoint(new StreamsGroupHeartbeatResponseData.Endpoint().setHost("localhost").setPort(8080))
                .setActivePartitions(List.of(
                    new StreamsGroupHeartbeatResponseData.TopicPartition().setTopic("topic").setPartitions(List.of(0)))
                )
        );

    private final StreamsRebalanceData streamsRebalanceData = new StreamsRebalanceData(
        PROCESS_ID,
        Optional.of(ENDPOINT),
        SUBTOPOLOGIES,
        CLIENT_TAGS
    );

    private final Time time = new MockTime();

    private final ConsumerConfig config = config();

    @Mock
    private CoordinatorRequestManager coordinatorRequestManager;

    @Mock
    private StreamsMembershipManager membershipManager;

    @Mock
    private BackgroundEventHandler backgroundEventHandler;

    private final Metrics metrics = new Metrics(time);

    private final Node coordinatorNode = new Node(1, "localhost", 9092);

    @Test
    public void testConstructWithNullCoordinatorRequestManager() {
        final Exception exception = assertThrows(NullPointerException.class, () -> new StreamsGroupHeartbeatRequestManager(
            new LogContext("test"),
            time,
            config,
            null,
            membershipManager,
            backgroundEventHandler,
            metrics,
            streamsRebalanceData
        ));
        assertEquals("Coordinator request manager cannot be null", exception.getMessage());
    }

    @Test
    public void testConstructWithNullMembershipManager() {
        final Exception exception = assertThrows(NullPointerException.class, () -> new StreamsGroupHeartbeatRequestManager(
            new LogContext("test"),
            time,
            config,
            coordinatorRequestManager,
            null,
            backgroundEventHandler,
            metrics,
            streamsRebalanceData
        ));
        assertEquals("Streams membership manager cannot be null", exception.getMessage());
    }

    @Test
    public void testConstructWithNullBackgroundEventHandler() {
        final Exception exception = assertThrows(NullPointerException.class, () -> new StreamsGroupHeartbeatRequestManager(
            new LogContext("test"),
            time,
            config,
            coordinatorRequestManager,
            membershipManager,
            null,
            metrics,
            streamsRebalanceData
        ));
        assertEquals("Background event handler cannot be null", exception.getMessage());
    }

    @Test
    public void testConstructWithNullMetrics() {
        final Exception exception = assertThrows(NullPointerException.class, () -> new StreamsGroupHeartbeatRequestManager(
            new LogContext("test"),
            time,
            config,
            coordinatorRequestManager,
            membershipManager,
            backgroundEventHandler,
            null,
            streamsRebalanceData
        ));
        assertEquals("Metrics cannot be null", exception.getMessage());
    }

    @Test
    public void testConstructWithNullStreamsRebalanceData() {
        final Exception exception = assertThrows(NullPointerException.class, () -> new StreamsGroupHeartbeatRequestManager(
            new LogContext("test"),
            time,
            config,
            coordinatorRequestManager,
            membershipManager,
            backgroundEventHandler,
            metrics,
            null
        ));
        assertEquals("Streams rebalance data cannot be null", exception.getMessage());
    }

    @Test
    public void testNoHeartbeatIfCoordinatorUnknown() {
        try (final MockedConstruction<Timer> pollTimerMockedConstruction = mockConstruction(Timer.class)) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final Timer pollTimer = pollTimerMockedConstruction.constructed().get(0);
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.empty());

            final NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

            assertEquals(0, result.unsentRequests.size());
            verify(membershipManager).onHeartbeatRequestSkipped();
            verify(pollTimer, never()).update();
        }
    }

    @Test
    public void testNoHeartbeatIfHeartbeatSkipped() {
        try (final MockedConstruction<Timer> pollTimerMockedConstruction = mockConstruction(Timer.class)) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final Timer pollTimer = pollTimerMockedConstruction.constructed().get(0);
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
            when(membershipManager.shouldSkipHeartbeat()).thenReturn(true);

            final NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

            assertEquals(0, result.unsentRequests.size());
            verify(membershipManager).onHeartbeatRequestSkipped();
            verify(pollTimer, never()).update();
        }
    }

    @Test
    public void testPropagateCoordinatorFatalErrorToApplicationThread() {
        final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.empty());
        final Throwable fatalError = new RuntimeException("KABOOM");
        when(coordinatorRequestManager.getAndClearFatalError()).thenReturn(Optional.of(fatalError));

        final NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

        assertEquals(0, result.unsentRequests.size());
        verify(membershipManager).onHeartbeatRequestSkipped();
        verify(backgroundEventHandler).add(argThat(
            errorEvent -> errorEvent instanceof ErrorEvent && ((ErrorEvent) errorEvent).error() == fatalError));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testSendingHeartbeatIfMemberIsLeaving(final boolean requestInFlight) {
        final long heartbeatIntervalMs = 1234;
        try (
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> {
                    when(mock.canSendRequest(time.milliseconds())).thenReturn(false);
                    when(mock.heartbeatIntervalMs()).thenReturn(heartbeatIntervalMs);
                    when(mock.requestInFlight()).thenReturn(requestInFlight);
                });
             final MockedConstruction<Timer> pollTimerMockedConstruction = mockConstruction(Timer.class)
        ) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final Timer pollTimer = pollTimerMockedConstruction.constructed().get(0);
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
            when(membershipManager.state()).thenReturn(MemberState.LEAVING);

            final NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

            assertEquals(1, result.unsentRequests.size());
            assertEquals(heartbeatIntervalMs, result.timeUntilNextPollMs);
            verify(pollTimer).update(time.milliseconds());
        }
    }

    @ParameterizedTest
    @EnumSource(value = MemberState.class, names = {"JOINING", "ACKNOWLEDGING"})
    public void testSendingHeartbeatIfMemberIsJoiningOrAcknowledging(final MemberState memberState) {
        final long heartbeatIntervalMs = 1234;
        try (
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> {
                    when(mock.canSendRequest(time.milliseconds())).thenReturn(false);
                    when(mock.heartbeatIntervalMs()).thenReturn(heartbeatIntervalMs);
                });
             final MockedConstruction<Timer> pollTimerMockedConstruction = mockConstruction(Timer.class)
        ) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final Timer pollTimer = pollTimerMockedConstruction.constructed().get(0);
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
            when(membershipManager.state()).thenReturn(memberState);

            final NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

            assertEquals(1, result.unsentRequests.size());
            assertEquals(heartbeatIntervalMs, result.timeUntilNextPollMs);
            verify(pollTimer).update(time.milliseconds());
        }
    }

    @ParameterizedTest
    @EnumSource(value = MemberState.class, names = {"JOINING", "ACKNOWLEDGING"})
    public void testNotSendingHeartbeatIfMemberIsJoiningOrAcknowledgingWhenHeartbeatInFlight(final MemberState memberState) {
        final long timeToNextHeartbeatMs = 1234;
        try (
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> {
                    when(mock.canSendRequest(time.milliseconds())).thenReturn(false);
                    when(mock.timeToNextHeartbeatMs(time.milliseconds())).thenReturn(timeToNextHeartbeatMs);
                    when(mock.requestInFlight()).thenReturn(true);
                });
            final MockedConstruction<Timer> pollTimerMockedConstruction = mockConstruction(Timer.class)
        ) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final Timer pollTimer = pollTimerMockedConstruction.constructed().get(0);
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
            when(membershipManager.state()).thenReturn(memberState);

            final NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

            assertEquals(0, result.unsentRequests.size());
            assertEquals(timeToNextHeartbeatMs, result.timeUntilNextPollMs);
            verify(pollTimer).update(time.milliseconds());
        }
    }

    @Test
    public void testSendingHeartbeatIfHeartbeatCanBeSent() {
        final long heartbeatIntervalMs = 1234;
        try (
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> {
                    when(mock.canSendRequest(time.milliseconds())).thenReturn(true);
                    when(mock.heartbeatIntervalMs()).thenReturn(heartbeatIntervalMs);

                });
            final MockedConstruction<Timer> pollTimerMockedConstruction = mockConstruction(Timer.class)
        ) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final Timer pollTimer = pollTimerMockedConstruction.constructed().get(0);
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
            when(membershipManager.state()).thenReturn(MemberState.STABLE);

            final NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

            assertEquals(1, result.unsentRequests.size());
            assertEquals(heartbeatIntervalMs, result.timeUntilNextPollMs);
            verify(pollTimer).update(time.milliseconds());
        }
    }

    @Test
    public void testNotSendingHeartbeatIfHeartbeatCannotBeSent() {
        final long timeToNextHeartbeatMs = 1234;
        try (
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> {
                    when(mock.canSendRequest(time.milliseconds())).thenReturn(false);
                    when(mock.timeToNextHeartbeatMs(time.milliseconds())).thenReturn(timeToNextHeartbeatMs);
                });
            final MockedConstruction<Timer> pollTimerMockedConstruction = mockConstruction(Timer.class)
        ) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final Timer pollTimer = pollTimerMockedConstruction.constructed().get(0);
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));

            final NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

            assertEquals(0, result.unsentRequests.size());
            assertEquals(timeToNextHeartbeatMs, result.timeUntilNextPollMs);
            verify(pollTimer).update(time.milliseconds());
        }
    }

    @Test
    public void testSendingLeaveHeartbeatIfPollTimerExpired() {
        final long heartbeatIntervalMs = 1234;
        try (
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> when(mock.heartbeatIntervalMs()).thenReturn(heartbeatIntervalMs));
            final MockedConstruction<Timer> pollTimerMockedConstruction = mockConstruction(
                Timer.class,
                (mock, context) -> when(mock.isExpired()).thenReturn(true));
            final MockedConstruction<StreamsGroupHeartbeatRequestManager.HeartbeatState> heartbeatStateMockedConstruction = mockConstruction(
                StreamsGroupHeartbeatRequestManager.HeartbeatState.class)
        ) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final HeartbeatRequestState heartbeatRequestState = heartbeatRequestStateMockedConstruction.constructed().get(0);
            final StreamsGroupHeartbeatRequestManager.HeartbeatState heartbeatState = heartbeatStateMockedConstruction.constructed().get(0);
            final Timer pollTimer = pollTimerMockedConstruction.constructed().get(0);
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));

            final NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

            assertEquals(1, result.unsentRequests.size());
            assertEquals(heartbeatIntervalMs, result.timeUntilNextPollMs);
            verify(pollTimer).update(time.milliseconds());
            verify(membershipManager).onPollTimerExpired();
            verify(heartbeatRequestState).reset();
            verify(heartbeatState).reset();
        }
    }

    @Test
    public void testNotSendingLeaveHeartbeatIfPollTimerExpiredAndMemberIsLeaving() {
        final long timeToNextHeartbeatMs = 1234;
        try (
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> when(mock.timeToNextHeartbeatMs(time.milliseconds())).thenReturn(timeToNextHeartbeatMs));
            final MockedConstruction<Timer> pollTimerMockedConstruction = mockConstruction(
                Timer.class,
                (mock, context) -> when(mock.isExpired()).thenReturn(true));
            final MockedConstruction<StreamsGroupHeartbeatRequestManager.HeartbeatState> heartbeatStateMockedConstruction = mockConstruction(
                StreamsGroupHeartbeatRequestManager.HeartbeatState.class)
        ) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final HeartbeatRequestState heartbeatRequestState = heartbeatRequestStateMockedConstruction.constructed().get(0);
            final StreamsGroupHeartbeatRequestManager.HeartbeatState heartbeatState = heartbeatStateMockedConstruction.constructed().get(0);
            final Timer pollTimer = pollTimerMockedConstruction.constructed().get(0);
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
            when(membershipManager.isLeavingGroup()).thenReturn(true);
            when(membershipManager.state()).thenReturn(MemberState.PREPARE_LEAVING);

            final NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

            assertEquals(0, result.unsentRequests.size());
            assertEquals(timeToNextHeartbeatMs, result.timeUntilNextPollMs);
            verify(pollTimer).update(time.milliseconds());
            verify(membershipManager, never()).onPollTimerExpired();
            verify(heartbeatRequestState, never()).reset();
            verify(heartbeatState, never()).reset();
        }
    }

    @Test
    public void testSendingLeaveHeartbeatRequestWhenPollTimerExpired() {
        try (
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> when(mock.canSendRequest(time.milliseconds())).thenReturn(true));
            final MockedConstruction<Timer> pollTimerMockedConstruction = mockConstruction(
                Timer.class,
                (mock, context) -> when(mock.isExpired()).thenReturn(true))
        ) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final HeartbeatRequestState heartbeatRequestState = heartbeatRequestStateMockedConstruction.constructed().get(0);
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
            when(membershipManager.groupId()).thenReturn(GROUP_ID);
            when(membershipManager.memberId()).thenReturn(MEMBER_ID);
            when(membershipManager.memberEpoch()).thenReturn(LEAVE_GROUP_MEMBER_EPOCH);
            when(membershipManager.groupInstanceId()).thenReturn(Optional.of(INSTANCE_ID));

            final NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

            assertEquals(0, result.timeUntilNextPollMs);
            assertEquals(1, result.unsentRequests.size());
            assertEquals(Optional.of(coordinatorNode), result.unsentRequests.get(0).node());
            NetworkClientDelegate.UnsentRequest networkRequest = result.unsentRequests.get(0);
            StreamsGroupHeartbeatRequest streamsRequest = (StreamsGroupHeartbeatRequest) networkRequest.requestBuilder().build();
            assertEquals(GROUP_ID, streamsRequest.data().groupId());
            assertEquals(MEMBER_ID, streamsRequest.data().memberId());
            assertEquals(LEAVE_GROUP_MEMBER_EPOCH, streamsRequest.data().memberEpoch());
            assertEquals(INSTANCE_ID, streamsRequest.data().instanceId());
            verify(heartbeatRequestState).onSendAttempt(time.milliseconds());
            verify(membershipManager).onHeartbeatRequestGenerated();
            final ClientResponse response = buildClientResponse();
            networkRequest.handler().onComplete(response);
            verify(heartbeatRequestState, never()).updateHeartbeatIntervalMs(anyLong());
            verify(heartbeatRequestState, never()).onSuccessfulAttempt(anyLong());
            verify(membershipManager, never()).onHeartbeatSuccess(any());
        }
    }

    @Test
    public void testSendingHeartbeatRequest() {
        try (
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> when(mock.canSendRequest(time.milliseconds())).thenReturn(true))
        ) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final HeartbeatRequestState heartbeatRequestState = heartbeatRequestStateMockedConstruction.constructed().get(0);
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
            when(membershipManager.groupId()).thenReturn(GROUP_ID);
            when(membershipManager.memberId()).thenReturn(MEMBER_ID);
            when(membershipManager.memberEpoch()).thenReturn(MEMBER_EPOCH);
            when(membershipManager.groupInstanceId()).thenReturn(Optional.of(INSTANCE_ID));

            final NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

            assertEquals(0, result.timeUntilNextPollMs);
            assertEquals(1, result.unsentRequests.size());
            assertEquals(Optional.of(coordinatorNode), result.unsentRequests.get(0).node());
            NetworkClientDelegate.UnsentRequest networkRequest = result.unsentRequests.get(0);
            StreamsGroupHeartbeatRequest streamsRequest = (StreamsGroupHeartbeatRequest) networkRequest.requestBuilder().build();
            assertEquals(GROUP_ID, streamsRequest.data().groupId());
            assertEquals(MEMBER_ID, streamsRequest.data().memberId());
            assertEquals(MEMBER_EPOCH, streamsRequest.data().memberEpoch());
            assertEquals(INSTANCE_ID, streamsRequest.data().instanceId());
            verify(heartbeatRequestState).onSendAttempt(time.milliseconds());
            verify(membershipManager).onHeartbeatRequestGenerated();
            time.sleep(2000);
            assertEquals(
                2.0,
                metrics.metric(metrics.metricName("last-heartbeat-seconds-ago", "consumer-coordinator-metrics")).metricValue()
            );
            final ClientResponse response = buildClientResponse();
            networkRequest.handler().onComplete(response);
            verify(membershipManager).onHeartbeatSuccess((StreamsGroupHeartbeatResponse) response.responseBody());
            verify(heartbeatRequestState).updateHeartbeatIntervalMs(RECEIVED_HEARTBEAT_INTERVAL_MS);
            verify(heartbeatRequestState).onSuccessfulAttempt(networkRequest.handler().completionTimeMs());
            verify(heartbeatRequestState).resetTimer();
            final List<TopicPartition> topicPartitions = streamsRebalanceData.partitionsByHost()
                .get(new StreamsRebalanceData.HostInfo(
                    ENDPOINT_TO_PARTITIONS.get(0).userEndpoint().host(),
                    ENDPOINT_TO_PARTITIONS.get(0).userEndpoint().port())
                ).activePartitions();
            assertEquals(ENDPOINT_TO_PARTITIONS.get(0).activePartitions().get(0).topic(), topicPartitions.get(0).topic());
            assertEquals(ENDPOINT_TO_PARTITIONS.get(0).activePartitions().get(0).partitions().get(0), topicPartitions.get(0).partition());
            assertEquals(
                1.0,
                metrics.metric(metrics.metricName("heartbeat-total", "consumer-coordinator-metrics")).metricValue()
            );
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBuildingHeartbeatRequestFieldsThatAreAlwaysSent(final boolean instanceIdPresent) {
        when(membershipManager.groupId()).thenReturn(GROUP_ID);
        when(membershipManager.memberId()).thenReturn(MEMBER_ID);
        when(membershipManager.memberEpoch()).thenReturn(MEMBER_EPOCH);
        when(membershipManager.groupInstanceId()).thenReturn(instanceIdPresent ? Optional.of(INSTANCE_ID) : Optional.empty());
        final StreamsGroupHeartbeatRequestManager.HeartbeatState heartbeatState =
            new StreamsGroupHeartbeatRequestManager.HeartbeatState(
                streamsRebalanceData,
                membershipManager,
                1000
            );

        StreamsGroupHeartbeatRequestData requestData1 = heartbeatState.buildRequestData();

        assertEquals(GROUP_ID, requestData1.groupId());
        assertEquals(MEMBER_ID, requestData1.memberId());
        assertEquals(MEMBER_EPOCH, requestData1.memberEpoch());
        if (instanceIdPresent) {
            assertEquals(INSTANCE_ID, requestData1.instanceId());
        } else {
            assertNull(requestData1.instanceId());
        }

        StreamsGroupHeartbeatRequestData requestData2 = heartbeatState.buildRequestData();

        assertEquals(GROUP_ID, requestData2.groupId());
        assertEquals(MEMBER_ID, requestData2.memberId());
        assertEquals(MEMBER_EPOCH, requestData2.memberEpoch());
        if (instanceIdPresent) {
            assertEquals(INSTANCE_ID, requestData2.instanceId());
        } else {
            assertNull(requestData2.instanceId());
        }
    }

    @ParameterizedTest
    @MethodSource("provideNonJoiningStates")
    public void testBuildingHeartbeatRequestTopologySentWhenJoining(final MemberState memberState) {
        final StreamsGroupHeartbeatRequestManager.HeartbeatState heartbeatState =
            new StreamsGroupHeartbeatRequestManager.HeartbeatState(
                streamsRebalanceData,
                membershipManager,
                1000
            );
        when(membershipManager.state()).thenReturn(MemberState.JOINING);

        StreamsGroupHeartbeatRequestData requestData1 = heartbeatState.buildRequestData();

        assertEquals(streamsRebalanceData.topologyEpoch(), requestData1.topology().epoch());
        final List<StreamsGroupHeartbeatRequestData.Subtopology> subtopologies = requestData1.topology().subtopologies();
        assertEquals(2, subtopologies.size());
        final StreamsGroupHeartbeatRequestData.Subtopology subtopology1 = subtopologies.get(0);
        assertEquals(SUBTOPOLOGY_NAME_1, subtopology1.subtopologyId());
        assertEquals(List.of(SOURCE_TOPIC_1, SOURCE_TOPIC_2), subtopology1.sourceTopics());
        assertEquals(List.of(REPARTITION_SINK_TOPIC_1, REPARTITION_SINK_TOPIC_2, REPARTITION_SINK_TOPIC_3), subtopology1.repartitionSinkTopics());
        assertEquals(REPARTITION_SOURCE_TOPICS.size(), subtopology1.repartitionSourceTopics().size());
        subtopology1.repartitionSourceTopics().forEach(topicInfo -> {
            final StreamsRebalanceData.TopicInfo repartitionTopic = REPARTITION_SOURCE_TOPICS.get(topicInfo.name());
            assertEquals(repartitionTopic.numPartitions().get(), topicInfo.partitions());
            assertEquals(repartitionTopic.replicationFactor().get(), topicInfo.replicationFactor());
            assertEquals(repartitionTopic.topicConfigs().size(), topicInfo.topicConfigs().size());
            assertTrue(isSorted(topicInfo.topicConfigs(), Comparator.comparing(StreamsGroupHeartbeatRequestData.KeyValue::key)));
        });
        assertEquals(CHANGELOG_TOPICS.size(), subtopology1.stateChangelogTopics().size());
        subtopology1.stateChangelogTopics().forEach(topicInfo -> {
            assertTrue(CHANGELOG_TOPICS.containsKey(topicInfo.name()));
            assertEquals(0, topicInfo.partitions());
            final StreamsRebalanceData.TopicInfo changelogTopic = CHANGELOG_TOPICS.get(topicInfo.name());
            assertEquals(changelogTopic.replicationFactor().get(), topicInfo.replicationFactor());
            assertEquals(changelogTopic.topicConfigs().size(), topicInfo.topicConfigs().size());
            assertTrue(isSorted(topicInfo.topicConfigs(), Comparator.comparing(StreamsGroupHeartbeatRequestData.KeyValue::key)));
        });
        assertEquals(2, subtopology1.copartitionGroups().size());
        final StreamsGroupHeartbeatRequestData.CopartitionGroup expectedCopartitionGroupData1 =
            new StreamsGroupHeartbeatRequestData.CopartitionGroup()
                .setRepartitionSourceTopics(Collections.singletonList((short) 0))
                .setSourceTopics(Collections.singletonList((short) 1));
        final StreamsGroupHeartbeatRequestData.CopartitionGroup expectedCopartitionGroupData2 =
            new StreamsGroupHeartbeatRequestData.CopartitionGroup()
                .setRepartitionSourceTopics(Collections.singletonList((short) 1))
                .setSourceTopics(Collections.singletonList((short) 0));
        assertTrue(subtopology1.copartitionGroups().contains(expectedCopartitionGroupData1));
        assertTrue(subtopology1.copartitionGroups().contains(expectedCopartitionGroupData2));
        final StreamsGroupHeartbeatRequestData.Subtopology subtopology2 = subtopologies.get(1);
        assertEquals(SUBTOPOLOGY_NAME_2, subtopology2.subtopologyId());
        assertEquals(List.of(SOURCE_TOPIC_3), subtopology2.sourceTopics());
        assertEquals(Collections.emptyList(), subtopology2.repartitionSinkTopics());
        assertEquals(Collections.emptyList(), subtopology2.repartitionSourceTopics());
        assertEquals(1, subtopology2.stateChangelogTopics().size());
        assertEquals(CHANGELOG_TOPIC_4, subtopology2.stateChangelogTopics().get(0).name());
        assertEquals(0, subtopology2.stateChangelogTopics().get(0).partitions());
        assertEquals(1, subtopology2.stateChangelogTopics().get(0).replicationFactor());
        assertEquals(0, subtopology2.stateChangelogTopics().get(0).topicConfigs().size());

        when(membershipManager.state()).thenReturn(memberState);

        StreamsGroupHeartbeatRequestData nonJoiningRequestData = heartbeatState.buildRequestData();
        assertNull(nonJoiningRequestData.topology());
    }

    private <V> boolean isSorted(List<V> collection, Comparator<V> comparator) {
        for (int i = 1; i < collection.size(); i++) {
            if (comparator.compare(collection.get(i - 1), collection.get(i)) > 0) {
                return false;
            }
        }
        return true;
    }

    @ParameterizedTest
    @MethodSource("provideNonJoiningStates")
    public void testBuildingHeartbeatRequestRebalanceTimeoutSentWhenJoining(final MemberState memberState) {
        final int rebalanceTimeoutMs = 1234;
        final StreamsGroupHeartbeatRequestManager.HeartbeatState heartbeatState =
            new StreamsGroupHeartbeatRequestManager.HeartbeatState(
                streamsRebalanceData,
                membershipManager,
                rebalanceTimeoutMs
            );
        when(membershipManager.state()).thenReturn(MemberState.JOINING);

        StreamsGroupHeartbeatRequestData requestData1 = heartbeatState.buildRequestData();

        assertEquals(rebalanceTimeoutMs, requestData1.rebalanceTimeoutMs());

        when(membershipManager.state()).thenReturn(memberState);

        StreamsGroupHeartbeatRequestData nonJoiningRequestData = heartbeatState.buildRequestData();

        assertEquals(-1, nonJoiningRequestData.rebalanceTimeoutMs());
    }

    @ParameterizedTest
    @MethodSource("provideNonJoiningStates")
    public void testBuildingHeartbeatProcessIdSentWhenJoining(final MemberState memberState) {
        final StreamsGroupHeartbeatRequestManager.HeartbeatState heartbeatState =
            new StreamsGroupHeartbeatRequestManager.HeartbeatState(
                streamsRebalanceData,
                membershipManager,
                1234
            );
        when(membershipManager.state()).thenReturn(MemberState.JOINING);

        StreamsGroupHeartbeatRequestData requestData1 = heartbeatState.buildRequestData();

        assertEquals(PROCESS_ID.toString(), requestData1.processId());

        when(membershipManager.state()).thenReturn(memberState);

        StreamsGroupHeartbeatRequestData nonJoiningRequestData = heartbeatState.buildRequestData();

        assertNull(nonJoiningRequestData.processId());
    }

    @ParameterizedTest
    @MethodSource("provideNonJoiningStates")
    public void testBuildingHeartbeatEndpointSentWhenJoining(final MemberState memberState) {
        final StreamsGroupHeartbeatRequestManager.HeartbeatState heartbeatState =
            new StreamsGroupHeartbeatRequestManager.HeartbeatState(
                streamsRebalanceData,
                membershipManager,
                1234
            );
        when(membershipManager.state()).thenReturn(MemberState.JOINING);

        StreamsGroupHeartbeatRequestData joiningRequestData = heartbeatState.buildRequestData();

        assertEquals(ENDPOINT.host(), joiningRequestData.userEndpoint().host());
        assertEquals(ENDPOINT.port(), joiningRequestData.userEndpoint().port());

        when(membershipManager.state()).thenReturn(memberState);

        StreamsGroupHeartbeatRequestData nonJoiningRequestData = heartbeatState.buildRequestData();

        assertNull(nonJoiningRequestData.userEndpoint());
    }

    @ParameterizedTest
    @MethodSource("provideNonJoiningStates")
    public void testBuildingHeartbeatClientTagsSentWhenJoining(final MemberState memberState) {
        final StreamsGroupHeartbeatRequestManager.HeartbeatState heartbeatState =
            new StreamsGroupHeartbeatRequestManager.HeartbeatState(
                streamsRebalanceData,
                membershipManager,
                1234
            );
        when(membershipManager.state()).thenReturn(MemberState.JOINING);

        StreamsGroupHeartbeatRequestData joiningRequestData = heartbeatState.buildRequestData();

        assertEquals(CLIENT_TAG_1, joiningRequestData.clientTags().get(0).key());
        assertEquals(VALUE_1, joiningRequestData.clientTags().get(0).value());

        when(membershipManager.state()).thenReturn(memberState);

        StreamsGroupHeartbeatRequestData nonJoiningRequestData = heartbeatState.buildRequestData();

        assertNull(nonJoiningRequestData.clientTags());
    }

    @ParameterizedTest
    @MethodSource("provideNonJoiningStates")
    public void testBuildingHeartbeatAssignmentSentWhenChanged(final MemberState memberState) {
        final StreamsGroupHeartbeatRequestManager.HeartbeatState heartbeatState =
            new StreamsGroupHeartbeatRequestManager.HeartbeatState(
                streamsRebalanceData,
                membershipManager,
                1234
            );
        when(membershipManager.state()).thenReturn(MemberState.JOINING);

        StreamsGroupHeartbeatRequestData joiningRequestData = heartbeatState.buildRequestData();

        assertEquals(List.of(), joiningRequestData.activeTasks());
        assertEquals(List.of(), joiningRequestData.standbyTasks());
        assertEquals(List.of(), joiningRequestData.warmupTasks());

        when(membershipManager.state()).thenReturn(memberState);
        streamsRebalanceData.setReconciledAssignment(
            new StreamsRebalanceData.Assignment(
                Set.of(
                    new StreamsRebalanceData.TaskId(SUBTOPOLOGY_NAME_1, 0),
                    new StreamsRebalanceData.TaskId(SUBTOPOLOGY_NAME_1, 1),
                    new StreamsRebalanceData.TaskId(SUBTOPOLOGY_NAME_2, 2)
                ),
                Set.of(
                    new StreamsRebalanceData.TaskId(SUBTOPOLOGY_NAME_1, 2)
                ),
                Set.of(
                    new StreamsRebalanceData.TaskId(SUBTOPOLOGY_NAME_1, 3),
                    new StreamsRebalanceData.TaskId(SUBTOPOLOGY_NAME_1, 4),
                    new StreamsRebalanceData.TaskId(SUBTOPOLOGY_NAME_1, 5)
                ),
                true
            )
        );

        StreamsGroupHeartbeatRequestData firstNonJoiningRequestData = heartbeatState.buildRequestData();

        assertTaskIdsEquals(
            List.of(
                new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopologyId(SUBTOPOLOGY_NAME_1)
                    .setPartitions(List.of(0, 1)),
                new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopologyId(SUBTOPOLOGY_NAME_2)
                    .setPartitions(List.of(2))
            ),
            firstNonJoiningRequestData.activeTasks()
        );
        assertTaskIdsEquals(
            List.of(
                new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopologyId(SUBTOPOLOGY_NAME_1)
                    .setPartitions(List.of(2))
            ),
            firstNonJoiningRequestData.standbyTasks()
        );
        assertTaskIdsEquals(
            List.of(
                new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopologyId(SUBTOPOLOGY_NAME_1)
                    .setPartitions(List.of(3, 4, 5))
            ),
            firstNonJoiningRequestData.warmupTasks()
        );

        StreamsGroupHeartbeatRequestData nonJoiningRequestDataWithoutChanges = heartbeatState.buildRequestData();

        assertNull(nonJoiningRequestDataWithoutChanges.activeTasks());
        assertNull(nonJoiningRequestDataWithoutChanges.standbyTasks());
        assertNull(nonJoiningRequestDataWithoutChanges.warmupTasks());

        streamsRebalanceData.setReconciledAssignment(
            new StreamsRebalanceData.Assignment(
                Set.of(
                    new StreamsRebalanceData.TaskId(SUBTOPOLOGY_NAME_1, 0)
                ),
                Set.of(
                    new StreamsRebalanceData.TaskId(SUBTOPOLOGY_NAME_1, 2)
                ),
                Set.of(
                ),
                true
            )
        );

        StreamsGroupHeartbeatRequestData nonJoiningRequestDataWithChanges = heartbeatState.buildRequestData();

        assertTaskIdsEquals(
            List.of(
                new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopologyId(SUBTOPOLOGY_NAME_1)
                    .setPartitions(List.of(0))
            ),
            nonJoiningRequestDataWithChanges.activeTasks()
        );
        assertTaskIdsEquals(
            List.of(
                new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopologyId(SUBTOPOLOGY_NAME_1)
                    .setPartitions(List.of(2))
            ),
            nonJoiningRequestDataWithChanges.standbyTasks()
        );
        assertEquals(List.of(), nonJoiningRequestDataWithChanges.warmupTasks());
    }

    @ParameterizedTest
    @MethodSource("provideNonJoiningStates")
    public void testResettingHeartbeatState(final MemberState memberState) {
        when(membershipManager.groupId()).thenReturn(GROUP_ID);
        when(membershipManager.memberId()).thenReturn(MEMBER_ID);
        when(membershipManager.memberEpoch()).thenReturn(MEMBER_EPOCH);
        when(membershipManager.groupInstanceId()).thenReturn(Optional.of(INSTANCE_ID));
        final StreamsGroupHeartbeatRequestManager.HeartbeatState heartbeatState =
            new StreamsGroupHeartbeatRequestManager.HeartbeatState(
                streamsRebalanceData,
                membershipManager,
                1234
            );
        when(membershipManager.state()).thenReturn(memberState);
        streamsRebalanceData.setReconciledAssignment(
            new StreamsRebalanceData.Assignment(
                Set.of(
                    new StreamsRebalanceData.TaskId(SUBTOPOLOGY_NAME_1, 0),
                    new StreamsRebalanceData.TaskId(SUBTOPOLOGY_NAME_1, 1),
                    new StreamsRebalanceData.TaskId(SUBTOPOLOGY_NAME_2, 2)
                ),
                Set.of(
                    new StreamsRebalanceData.TaskId(SUBTOPOLOGY_NAME_1, 2)
                ),
                Set.of(
                    new StreamsRebalanceData.TaskId(SUBTOPOLOGY_NAME_1, 3),
                    new StreamsRebalanceData.TaskId(SUBTOPOLOGY_NAME_1, 4),
                    new StreamsRebalanceData.TaskId(SUBTOPOLOGY_NAME_1, 5)
                ),
                true
            )
        );
        StreamsGroupHeartbeatRequestData requestDataBeforeReset = heartbeatState.buildRequestData();
        assertEquals(GROUP_ID, requestDataBeforeReset.groupId());
        assertEquals(MEMBER_ID, requestDataBeforeReset.memberId());
        assertEquals(MEMBER_EPOCH, requestDataBeforeReset.memberEpoch());
        assertEquals(INSTANCE_ID, requestDataBeforeReset.instanceId());
        assertFalse(requestDataBeforeReset.activeTasks().isEmpty());
        assertFalse(requestDataBeforeReset.standbyTasks().isEmpty());
        assertFalse(requestDataBeforeReset.warmupTasks().isEmpty());

        heartbeatState.reset();

        StreamsGroupHeartbeatRequestData requestDataAfterReset = heartbeatState.buildRequestData();
        assertEquals(GROUP_ID, requestDataAfterReset.groupId());
        assertEquals(MEMBER_ID, requestDataAfterReset.memberId());
        assertEquals(MEMBER_EPOCH, requestDataAfterReset.memberEpoch());
        assertEquals(INSTANCE_ID, requestDataAfterReset.instanceId());
        assertEquals(requestDataBeforeReset.activeTasks(), requestDataAfterReset.activeTasks());
        assertEquals(requestDataBeforeReset.standbyTasks(), requestDataAfterReset.standbyTasks());
        assertEquals(requestDataBeforeReset.warmupTasks(), requestDataAfterReset.warmupTasks());
    }

    private static Stream<Arguments> provideNonJoiningStates() {
        return Stream.of(
            Arguments.of(MemberState.ACKNOWLEDGING),
            Arguments.of(MemberState.RECONCILING),
            Arguments.of(MemberState.STABLE),
            Arguments.of(MemberState.PREPARE_LEAVING),
            Arguments.of(MemberState.LEAVING)
        );
    }

    @ParameterizedTest
    @EnumSource(
        value = MemberState.class,
        names = {"JOINING", "ACKNOWLEDGING", "RECONCILING", "STABLE", "PREPARE_LEAVING", "LEAVING"}
    )
    public void testBuildingHeartbeatShutdownRequested(final MemberState memberState) {
        final StreamsGroupHeartbeatRequestManager.HeartbeatState heartbeatState =
            new StreamsGroupHeartbeatRequestManager.HeartbeatState(
                streamsRebalanceData,
                membershipManager,
                1234
            );
        when(membershipManager.state()).thenReturn(memberState);

        StreamsGroupHeartbeatRequestData requestDataWithoutShutdownRequest = heartbeatState.buildRequestData();

        assertFalse(requestDataWithoutShutdownRequest.shutdownApplication());

        streamsRebalanceData.requestShutdown();

        StreamsGroupHeartbeatRequestData requestDataWithShutdownRequest = heartbeatState.buildRequestData();

        assertTrue(requestDataWithShutdownRequest.shutdownApplication());
    }

    @Test
    public void testCoordinatorDisconnectFailureWhileSending() {
        try (
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> when(mock.canSendRequest(time.milliseconds())).thenReturn(true));
            final MockedConstruction<StreamsGroupHeartbeatRequestManager.HeartbeatState> heartbeatStateMockedConstruction = mockConstruction(
                StreamsGroupHeartbeatRequestManager.HeartbeatState.class)
        ) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final StreamsGroupHeartbeatRequestManager.HeartbeatState heartbeatState = heartbeatStateMockedConstruction.constructed().get(0);
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));

            final NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

            assertEquals(1, result.unsentRequests.size());
            final NetworkClientDelegate.UnsentRequest networkRequest = result.unsentRequests.get(0);
            time.sleep(1234);
            final long completionTimeMs = time.milliseconds();
            final DisconnectException disconnectException = DisconnectException.INSTANCE;
            networkRequest.handler().onFailure(completionTimeMs, disconnectException);
            final HeartbeatRequestState heartbeatRequestState = heartbeatRequestStateMockedConstruction.constructed().get(0);
            verify(heartbeatRequestState).onFailedAttempt(completionTimeMs);
            verify(heartbeatState).reset();
            verify(coordinatorRequestManager).handleCoordinatorDisconnect(disconnectException, completionTimeMs);
            verify(membershipManager).onRetriableHeartbeatFailure();
        }
    }

    @Test
    public void testUnsupportedVersionFailureWhileSending() {
        try (
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> when(mock.canSendRequest(time.milliseconds())).thenReturn(true));
            final MockedConstruction<StreamsGroupHeartbeatRequestManager.HeartbeatState> heartbeatStateMockedConstruction = mockConstruction(
                StreamsGroupHeartbeatRequestManager.HeartbeatState.class)
        ) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final StreamsGroupHeartbeatRequestManager.HeartbeatState heartbeatState = heartbeatStateMockedConstruction.constructed().get(0);
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));

            final NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

            assertEquals(1, result.unsentRequests.size());
            final NetworkClientDelegate.UnsentRequest networkRequest = result.unsentRequests.get(0);
            time.sleep(1234);
            final long completionTimeMs = time.milliseconds();
            final UnsupportedVersionException unsupportedVersionException = new UnsupportedVersionException("message");
            networkRequest.handler().onFailure(completionTimeMs, unsupportedVersionException);
            final HeartbeatRequestState heartbeatRequestState = heartbeatRequestStateMockedConstruction.constructed().get(0);
            verify(heartbeatRequestState).onFailedAttempt(completionTimeMs);
            verify(heartbeatState).reset();
            verify(membershipManager).onFatalHeartbeatFailure();
            ArgumentCaptor<ErrorEvent> errorEvent = ArgumentCaptor.forClass(ErrorEvent.class);
            verify(backgroundEventHandler).add(errorEvent.capture());
            assertEquals(
                "The cluster does not support the STREAMS group " +
                    "protocol or does not support the versions of the STREAMS group protocol used by this client " +
                    "(used versions: " + StreamsGroupHeartbeatRequestData.LOWEST_SUPPORTED_VERSION + " to " +
                    StreamsGroupHeartbeatRequestData.HIGHEST_SUPPORTED_VERSION + ").",
                errorEvent.getValue().error().getMessage()
            );
            assertInstanceOf(UnsupportedVersionException.class, errorEvent.getValue().error());
            verify(membershipManager).transitionToFatal();
        }
    }

    @Test
    public void testFatalFailureWhileSending() {
        try (
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> when(mock.canSendRequest(time.milliseconds())).thenReturn(true));
            final MockedConstruction<StreamsGroupHeartbeatRequestManager.HeartbeatState> heartbeatStateMockedConstruction = mockConstruction(
                StreamsGroupHeartbeatRequestManager.HeartbeatState.class)
        ) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final StreamsGroupHeartbeatRequestManager.HeartbeatState heartbeatState = heartbeatStateMockedConstruction.constructed().get(0);
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));

            final NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

            assertEquals(1, result.unsentRequests.size());
            final NetworkClientDelegate.UnsentRequest networkRequest = result.unsentRequests.get(0);
            time.sleep(1234);
            final long completionTimeMs = time.milliseconds();
            final RuntimeException fatalException = new RuntimeException();
            networkRequest.handler().onFailure(completionTimeMs, fatalException);
            final HeartbeatRequestState heartbeatRequestState = heartbeatRequestStateMockedConstruction.constructed().get(0);
            verify(heartbeatRequestState).onFailedAttempt(completionTimeMs);
            verify(heartbeatState).reset();
            verify(membershipManager).onFatalHeartbeatFailure();
            ArgumentCaptor<ErrorEvent> errorEvent = ArgumentCaptor.forClass(ErrorEvent.class);
            verify(backgroundEventHandler).add(errorEvent.capture());
            assertEquals(fatalException, errorEvent.getValue().error());
            verify(membershipManager).transitionToFatal();
        }
    }

    @ParameterizedTest
    @EnumSource(
        value = Errors.class,
        names = {"NOT_COORDINATOR", "COORDINATOR_NOT_AVAILABLE"}
    )
    public void testNotCoordinatorAndCoordinatorNotAvailableErrorResponse(final Errors error) {
        try (
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> when(mock.canSendRequest(time.milliseconds())).thenReturn(true));
            final MockedConstruction<StreamsGroupHeartbeatRequestManager.HeartbeatState> heartbeatStateMockedConstruction = mockConstruction(
                StreamsGroupHeartbeatRequestManager.HeartbeatState.class)
        ) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final StreamsGroupHeartbeatRequestManager.HeartbeatState heartbeatState = heartbeatStateMockedConstruction.constructed().get(0);
            final HeartbeatRequestState heartbeatRequestState = heartbeatRequestStateMockedConstruction.constructed().get(0);
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));

            final NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

            assertEquals(1, result.unsentRequests.size());
            final NetworkClientDelegate.UnsentRequest networkRequest = result.unsentRequests.get(0);
            time.sleep(1234);
            final long completionTimeMs = time.milliseconds();
            final ClientResponse response = buildClientErrorResponse(error, "error message");
            networkRequest.handler().onComplete(response);
            verify(coordinatorRequestManager).markCoordinatorUnknown(
                ((StreamsGroupHeartbeatResponse) response.responseBody()).data().errorMessage(),
                completionTimeMs
            );
            verify(heartbeatState).reset();
            verify(heartbeatRequestState).reset();
            verify(membershipManager).onFatalHeartbeatFailure();
        }
    }

    @Test
    public void testCoordinatorLoadInProgressErrorResponse() {
        try (
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> when(mock.canSendRequest(time.milliseconds())).thenReturn(true));
            final MockedConstruction<StreamsGroupHeartbeatRequestManager.HeartbeatState> heartbeatStateMockedConstruction = mockConstruction(
                StreamsGroupHeartbeatRequestManager.HeartbeatState.class)
        ) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final StreamsGroupHeartbeatRequestManager.HeartbeatState heartbeatState = heartbeatStateMockedConstruction.constructed().get(0);
            final HeartbeatRequestState heartbeatRequestState = heartbeatRequestStateMockedConstruction.constructed().get(0);
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));

            final NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

            assertEquals(1, result.unsentRequests.size());
            final NetworkClientDelegate.UnsentRequest networkRequest = result.unsentRequests.get(0);
            final ClientResponse response = buildClientErrorResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS, "message");
            networkRequest.handler().onComplete(response);
            verify(heartbeatState).reset();
            verify(membershipManager).onFatalHeartbeatFailure();
            verify(heartbeatRequestState, never()).reset();
        }
    }

    @Test
    public void testGroupAuthorizationFailedErrorResponse() {
        try (
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> when(mock.canSendRequest(time.milliseconds())).thenReturn(true));
            final MockedConstruction<StreamsGroupHeartbeatRequestManager.HeartbeatState> heartbeatStateMockedConstruction = mockConstruction(
                StreamsGroupHeartbeatRequestManager.HeartbeatState.class);
            final LogCaptureAppender logAppender = LogCaptureAppender.createAndRegister(StreamsGroupHeartbeatRequestManager.class)
        ) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final StreamsGroupHeartbeatRequestManager.HeartbeatState heartbeatState = heartbeatStateMockedConstruction.constructed().get(0);
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
            when(membershipManager.groupId()).thenReturn(GROUP_ID);

            final NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

            assertEquals(1, result.unsentRequests.size());
            final NetworkClientDelegate.UnsentRequest networkRequest = result.unsentRequests.get(0);
            final ClientResponse response = buildClientErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED, "message");
            networkRequest.handler().onComplete(response);
            assertTrue(logAppender.getMessages("ERROR").stream()
                .anyMatch(m -> m.contains("StreamsGroupHeartbeatRequest failed due to group authorization failure: " +
                    "Not authorized to access group: " + GROUP_ID)));
            verify(heartbeatState).reset();
            ArgumentCaptor<ErrorEvent> errorEvent = ArgumentCaptor.forClass(ErrorEvent.class);
            verify(backgroundEventHandler).add(errorEvent.capture());
            assertEquals(
                GroupAuthorizationException.forGroupId(GROUP_ID).getMessage(),
                errorEvent.getValue().error().getMessage()
            );
            assertInstanceOf(GroupAuthorizationException.class, errorEvent.getValue().error());
            verify(membershipManager).transitionToFatal();
            verify(membershipManager).onFatalHeartbeatFailure();
        }
    }

    @Test
    public void testTopicAuthorizationFailedErrorResponse() {
        try (
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> when(mock.canSendRequest(time.milliseconds())).thenReturn(true));
            final MockedConstruction<StreamsGroupHeartbeatRequestManager.HeartbeatState> heartbeatStateMockedConstruction = mockConstruction(
                StreamsGroupHeartbeatRequestManager.HeartbeatState.class);
            final LogCaptureAppender logAppender = LogCaptureAppender.createAndRegister(StreamsGroupHeartbeatRequestManager.class)
        ) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final StreamsGroupHeartbeatRequestManager.HeartbeatState heartbeatState = heartbeatStateMockedConstruction.constructed().get(0);
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
            when(membershipManager.state()).thenReturn(MemberState.STABLE);
            when(membershipManager.memberId()).thenReturn(MEMBER_ID);

            final NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

            assertEquals(1, result.unsentRequests.size());
            final NetworkClientDelegate.UnsentRequest networkRequest = result.unsentRequests.get(0);
            final String errorMessage = "message";
            final ClientResponse response = buildClientErrorResponse(Errors.TOPIC_AUTHORIZATION_FAILED, errorMessage);
            networkRequest.handler().onComplete(response);
            assertTrue(logAppender.getMessages("ERROR").stream()
                .anyMatch(m -> m.contains("StreamsGroupHeartbeatRequest failed for member " + MEMBER_ID +
                    " with state " + MemberState.STABLE + " due to " + Errors.TOPIC_AUTHORIZATION_FAILED + ": " +
                    errorMessage)));
            verify(heartbeatState).reset();
            ArgumentCaptor<ErrorEvent> errorEvent = ArgumentCaptor.forClass(ErrorEvent.class);
            verify(backgroundEventHandler).add(errorEvent.capture());
            assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.message(), errorEvent.getValue().error().getMessage());
            assertInstanceOf(TopicAuthorizationException.class, errorEvent.getValue().error());
            verify(membershipManager).onFatalHeartbeatFailure();
        }
    }

    @ParameterizedTest
    @EnumSource(
        value = Errors.class,
        names = {
            "INVALID_REQUEST",
            "GROUP_MAX_SIZE_REACHED",
            "UNSUPPORTED_VERSION",
            "STREAMS_INVALID_TOPOLOGY",
            "STREAMS_INVALID_TOPOLOGY_EPOCH",
            "STREAMS_TOPOLOGY_FENCED"
        }
    )
    public void testKnownFatalErrorResponse(final Errors error) {
        try (
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> when(mock.canSendRequest(time.milliseconds())).thenReturn(true));
            final MockedConstruction<StreamsGroupHeartbeatRequestManager.HeartbeatState> heartbeatStateMockedConstruction = mockConstruction(
                StreamsGroupHeartbeatRequestManager.HeartbeatState.class);
            final LogCaptureAppender logAppender = LogCaptureAppender.createAndRegister(StreamsGroupHeartbeatRequestManager.class)
        ) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final StreamsGroupHeartbeatRequestManager.HeartbeatState heartbeatState = heartbeatStateMockedConstruction.constructed().get(0);
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));

            final NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

            assertEquals(1, result.unsentRequests.size());
            final NetworkClientDelegate.UnsentRequest networkRequest = result.unsentRequests.get(0);
            final String errorMessageInResponse = "message";
            final ClientResponse response = buildClientErrorResponse(error, errorMessageInResponse);
            networkRequest.handler().onComplete(response);
            verify(heartbeatState).reset();
            ArgumentCaptor<ErrorEvent> errorEvent = ArgumentCaptor.forClass(ErrorEvent.class);
            verify(backgroundEventHandler).add(errorEvent.capture());
            if (error == Errors.UNSUPPORTED_VERSION) {
                final String errorMessage = "The cluster does not support the STREAMS group " +
                    "protocol or does not support the versions of the STREAMS group protocol used by this client " +
                    "(used versions: " + StreamsGroupHeartbeatRequestData.LOWEST_SUPPORTED_VERSION + " to " +
                    StreamsGroupHeartbeatRequestData.HIGHEST_SUPPORTED_VERSION + ").";
                assertTrue(logAppender.getMessages("ERROR").stream()
                    .anyMatch(m -> m.contains("StreamsGroupHeartbeatRequest failed due to " +
                        error + ": " + errorMessage)));
                assertEquals(errorMessage, errorEvent.getValue().error().getMessage());
            } else {
                assertTrue(logAppender.getMessages("ERROR").stream()
                    .anyMatch(m -> m.contains("StreamsGroupHeartbeatRequest failed due to " +
                        error + ": " + errorMessageInResponse)));
                assertEquals(errorMessageInResponse, errorEvent.getValue().error().getMessage());
            }
            assertInstanceOf(error.exception().getClass(), errorEvent.getValue().error());
            verify(membershipManager).transitionToFatal();
            verify(membershipManager).onFatalHeartbeatFailure();
        }
    }

    @ParameterizedTest
    @EnumSource(
        value = Errors.class,
        names = {"FENCED_MEMBER_EPOCH", "UNKNOWN_MEMBER_ID"}
    )
    public void testFencedMemberOrUnknownMemberIdErrorResponse(final Errors error) {
        try (
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> when(mock.canSendRequest(time.milliseconds())).thenReturn(true));
            final MockedConstruction<StreamsGroupHeartbeatRequestManager.HeartbeatState> heartbeatStateMockedConstruction = mockConstruction(
                StreamsGroupHeartbeatRequestManager.HeartbeatState.class)
        ) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final StreamsGroupHeartbeatRequestManager.HeartbeatState heartbeatState = heartbeatStateMockedConstruction.constructed().get(0);
            final HeartbeatRequestState heartbeatRequestState = heartbeatRequestStateMockedConstruction.constructed().get(0);
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));

            final NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

            assertEquals(1, result.unsentRequests.size());
            final NetworkClientDelegate.UnsentRequest networkRequest = result.unsentRequests.get(0);
            final String errorMessage = "message";
            final ClientResponse response = buildClientErrorResponse(error, errorMessage);
            networkRequest.handler().onComplete(response);
            verify(heartbeatState).reset();
            verify(heartbeatRequestState).reset();
            verify(membershipManager).onFenced();
            verify(membershipManager).onFatalHeartbeatFailure();
        }
    }

    @ParameterizedTest
    @MethodSource("provideOtherErrors")
    public void testOtherErrorResponse(final Errors error) {
        try (
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> when(mock.canSendRequest(time.milliseconds())).thenReturn(true));
            final MockedConstruction<StreamsGroupHeartbeatRequestManager.HeartbeatState> heartbeatStateMockedConstruction = mockConstruction(
                StreamsGroupHeartbeatRequestManager.HeartbeatState.class);
            final LogCaptureAppender logAppender = LogCaptureAppender.createAndRegister(StreamsGroupHeartbeatRequestManager.class)
        ) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final StreamsGroupHeartbeatRequestManager.HeartbeatState heartbeatState = heartbeatStateMockedConstruction.constructed().get(0);
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));

            final NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

            assertEquals(1, result.unsentRequests.size());
            final NetworkClientDelegate.UnsentRequest networkRequest = result.unsentRequests.get(0);
            final String errorMessage = "message";
            final ClientResponse response = buildClientErrorResponse(error, errorMessage);
            networkRequest.handler().onComplete(response);
            assertTrue(logAppender.getMessages("ERROR").stream()
                .anyMatch(m -> m.contains("StreamsGroupHeartbeatRequest failed due to unexpected error")));
            verify(heartbeatState).reset();
            ArgumentCaptor<ErrorEvent> errorEvent = ArgumentCaptor.forClass(ErrorEvent.class);
            verify(backgroundEventHandler).add(errorEvent.capture());
            assertEquals(errorMessage, errorEvent.getValue().error().getMessage());
            assertInstanceOf(error.exception().getClass(), errorEvent.getValue().error());
            verify(membershipManager).transitionToFatal();
            verify(membershipManager).onFatalHeartbeatFailure();
        }
    }

    private static Stream<Arguments> provideOtherErrors() {
        final Set<Errors> consideredErrors = Set.of(
            Errors.NONE,
            Errors.NOT_COORDINATOR,
            Errors.COORDINATOR_NOT_AVAILABLE,
            Errors.COORDINATOR_LOAD_IN_PROGRESS,
            Errors.GROUP_AUTHORIZATION_FAILED,
            Errors.TOPIC_AUTHORIZATION_FAILED,
            Errors.INVALID_REQUEST,
            Errors.GROUP_MAX_SIZE_REACHED,
            Errors.FENCED_MEMBER_EPOCH,
            Errors.UNKNOWN_MEMBER_ID,
            Errors.UNSUPPORTED_VERSION,
            Errors.STREAMS_INVALID_TOPOLOGY,
            Errors.STREAMS_INVALID_TOPOLOGY_EPOCH,
            Errors.STREAMS_TOPOLOGY_FENCED);
        return Arrays.stream(Errors.values())
            .filter(error -> !consideredErrors.contains(error))
            .map(Arguments::of);
    }

    @Test
    public void testPollOnCloseWhenIsNotLeaving() {
        final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();

        NetworkClientDelegate.PollResult result = heartbeatRequestManager.pollOnClose(time.milliseconds());

        assertEquals(NetworkClientDelegate.PollResult.EMPTY, result);
    }

    @Test
    public void testPollOnCloseWhenIsLeaving() {
        final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
        when(membershipManager.isLeavingGroup()).thenReturn(true);
        when(membershipManager.groupId()).thenReturn(GROUP_ID);
        when(membershipManager.memberId()).thenReturn(MEMBER_ID);
        when(membershipManager.memberEpoch()).thenReturn(LEAVE_GROUP_MEMBER_EPOCH);

        NetworkClientDelegate.PollResult result = heartbeatRequestManager.pollOnClose(time.milliseconds());

        assertEquals(1, result.unsentRequests.size());
        final NetworkClientDelegate.UnsentRequest networkRequest = result.unsentRequests.get(0);
        StreamsGroupHeartbeatRequest streamsRequest = (StreamsGroupHeartbeatRequest) networkRequest.requestBuilder().build();
        assertEquals(GROUP_ID, streamsRequest.data().groupId());
        assertEquals(MEMBER_ID, streamsRequest.data().memberId());
        assertEquals(LEAVE_GROUP_MEMBER_EPOCH, streamsRequest.data().memberEpoch());
    }

    @Test
    public void testMaximumTimeToWaitPollTimerExpired() {
        try (
            final MockedConstruction<Timer> timerMockedConstruction =
                mockConstruction(Timer.class, (mock, context) -> when(mock.isExpired()).thenReturn(true));
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> when(mock.requestInFlight()).thenReturn(false))
        ) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final Timer pollTimer = timerMockedConstruction.constructed().get(0);
            time.sleep(1234);

            final long maximumTimeToWait = heartbeatRequestManager.maximumTimeToWait(time.milliseconds());

            assertEquals(0, maximumTimeToWait);
            verify(pollTimer).update(time.milliseconds());
        }
    }

    @Test
    public void testMaximumTimeToWaitWhenHeartbeatShouldBeSentImmediately() {
        try (
            final MockedConstruction<Timer> timerMockedConstruction = mockConstruction(Timer.class);
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> when(mock.requestInFlight()).thenReturn(false))
        ) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final Timer pollTimer = timerMockedConstruction.constructed().get(0);
            when(membershipManager.shouldNotWaitForHeartbeatInterval()).thenReturn(true);
            time.sleep(1234);

            final long maximumTimeToWait = heartbeatRequestManager.maximumTimeToWait(time.milliseconds());

            assertEquals(0, maximumTimeToWait);
            verify(pollTimer).update(time.milliseconds());
        }
    }

    @ParameterizedTest
    @CsvSource({"true, false", "false, false", "true, true"})
    public void testMaximumTimeToWaitWhenHeartbeatShouldBeNotSentImmediately(final boolean isRequestInFlight,
                                                                             final boolean shouldNotWaitForHeartbeatInterval) {
        final long remainingMs = 12L;
        final long timeToNextHeartbeatMs = 6L;
        try (
            final MockedConstruction<Timer> timerMockedConstruction =
                mockConstruction(Timer.class, (mock, context) -> when(mock.remainingMs()).thenReturn(remainingMs));
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> {
                    when(mock.requestInFlight()).thenReturn(isRequestInFlight);
                    when(mock.timeToNextHeartbeatMs(anyLong())).thenReturn(timeToNextHeartbeatMs);
                })
        ) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final Timer pollTimer = timerMockedConstruction.constructed().get(0);
            when(membershipManager.shouldNotWaitForHeartbeatInterval()).thenReturn(shouldNotWaitForHeartbeatInterval);
            time.sleep(1234);

            final long maximumTimeToWait = heartbeatRequestManager.maximumTimeToWait(time.milliseconds());

            assertEquals(timeToNextHeartbeatMs, maximumTimeToWait);
            verify(pollTimer).update(time.milliseconds());
        }
    }

    @ParameterizedTest
    @CsvSource({"12, 5", "10, 6"})
    public void testMaximumTimeToWaitSelectingMinimumWaitTime(final long remainingMs,
                                                              final long timeToNextHeartbeatMs) {
        try (
            final MockedConstruction<Timer> timerMockedConstruction =
                mockConstruction(Timer.class, (mock, context) -> when(mock.remainingMs()).thenReturn(remainingMs));
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> when(mock.timeToNextHeartbeatMs(anyLong())).thenReturn(timeToNextHeartbeatMs))
        ) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final Timer pollTimer = timerMockedConstruction.constructed().get(0);
            time.sleep(1234);

            final long maximumTimeToWait = heartbeatRequestManager.maximumTimeToWait(time.milliseconds());

            assertEquals(5, maximumTimeToWait);
            verify(pollTimer).update(time.milliseconds());
        }
    }

    @Test
    public void testResetPollTimer() {
        try (final MockedConstruction<Timer> pollTimerMockedConstruction = mockConstruction(Timer.class)) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final Timer pollTimer = pollTimerMockedConstruction.constructed().get(1);

            heartbeatRequestManager.resetPollTimer(time.milliseconds());
            verify(pollTimer).update(time.milliseconds());
            verify(pollTimer).isExpired();
            verify(pollTimer).reset(DEFAULT_MAX_POLL_INTERVAL_MS);
        }
    }

    @Test
    public void testResetPollTimerWhenExpired() {
        try (final MockedConstruction<Timer> pollTimerMockedConstruction = mockConstruction(Timer.class)) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            final Timer pollTimer = pollTimerMockedConstruction.constructed().get(1);

            when(pollTimer.isExpired()).thenReturn(true);
            heartbeatRequestManager.resetPollTimer(time.milliseconds());
            verify(pollTimer).update(time.milliseconds());
            verify(pollTimer).isExpired();
            verify(pollTimer).isExpiredBy();
            verify(membershipManager).memberId();
            verify(membershipManager).maybeRejoinStaleMember();
            verify(pollTimer).reset(DEFAULT_MAX_POLL_INTERVAL_MS);
        }
    }

    @Test
    public void testStreamsRebalanceDataHeartbeatIntervalMsUpdatedOnSuccess() {
        try (
                final MockedConstruction<HeartbeatRequestState> ignored = mockConstruction(
                        HeartbeatRequestState.class,
                        (mock, context) -> when(mock.canSendRequest(time.milliseconds())).thenReturn(true))
        ) {
            final StreamsGroupHeartbeatRequestManager heartbeatRequestManager = createStreamsGroupHeartbeatRequestManager();
            when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
            when(membershipManager.groupId()).thenReturn(GROUP_ID);
            when(membershipManager.memberId()).thenReturn(MEMBER_ID);
            when(membershipManager.memberEpoch()).thenReturn(MEMBER_EPOCH);
            when(membershipManager.groupInstanceId()).thenReturn(Optional.of(INSTANCE_ID));

            // Initially, heartbeatIntervalMs should be -1
            assertEquals(-1, streamsRebalanceData.heartbeatIntervalMs());

            final NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());
            assertEquals(1, result.unsentRequests.size());

            final NetworkClientDelegate.UnsentRequest networkRequest = result.unsentRequests.get(0);
            final ClientResponse response = buildClientResponse();
            networkRequest.handler().onComplete(response);

            // After successful response, heartbeatIntervalMs should be updated
            assertEquals(RECEIVED_HEARTBEAT_INTERVAL_MS, streamsRebalanceData.heartbeatIntervalMs());
        }
    }

    private static ConsumerConfig config() {
        Properties prop = new Properties();
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(DEFAULT_MAX_POLL_INTERVAL_MS));
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return new ConsumerConfig(prop);
    }

    private StreamsGroupHeartbeatRequestManager createStreamsGroupHeartbeatRequestManager() {
        return new StreamsGroupHeartbeatRequestManager(
            LOG_CONTEXT,
            time,
            config,
            coordinatorRequestManager,
            membershipManager,
            backgroundEventHandler,
            metrics,
            streamsRebalanceData
        );
    }

    private ClientResponse buildClientResponse() {
        return new ClientResponse(
            new RequestHeader(ApiKeys.STREAMS_GROUP_HEARTBEAT, (short) 1, "", 1),
            null,
            "-1",
            time.milliseconds(),
            time.milliseconds(),
            false,
            null,
            null,
            new StreamsGroupHeartbeatResponse(
                new StreamsGroupHeartbeatResponseData()
                    .setPartitionsByUserEndpoint(ENDPOINT_TO_PARTITIONS)
                    .setHeartbeatIntervalMs((int) RECEIVED_HEARTBEAT_INTERVAL_MS)
            )
        );
    }

    private ClientResponse buildClientErrorResponse(final Errors error, final String errorMessage) {
        return new ClientResponse(
            new RequestHeader(ApiKeys.STREAMS_GROUP_HEARTBEAT, (short) 1, "", 1),
            null,
            "-1",
            time.milliseconds(),
            time.milliseconds(),
            false,
            null,
            null,
            new StreamsGroupHeartbeatResponse(
                new StreamsGroupHeartbeatResponseData()
                    .setErrorCode(error.code())
                    .setErrorMessage(errorMessage)
            )
        );
    }

    private static void assertTaskIdsEquals(final List<StreamsGroupHeartbeatRequestData.TaskIds> expected,
                                            final List<StreamsGroupHeartbeatRequestData.TaskIds> actual) {
        List<StreamsGroupHeartbeatRequestData.TaskIds> sortedExpected = expected.stream()
            .map(taskIds -> new StreamsGroupHeartbeatRequestData.TaskIds()
                .setSubtopologyId(taskIds.subtopologyId())
                .setPartitions(taskIds.partitions().stream().sorted().collect(Collectors.toList())))
            .sorted(Comparator.comparing(StreamsGroupHeartbeatRequestData.TaskIds::subtopologyId))
            .collect(Collectors.toList());
        List<StreamsGroupHeartbeatRequestData.TaskIds> sortedActual = actual.stream()
            .map(taskIds -> new StreamsGroupHeartbeatRequestData.TaskIds()
                .setSubtopologyId(taskIds.subtopologyId())
                .setPartitions(taskIds.partitions().stream().sorted().collect(Collectors.toList())))
            .sorted(Comparator.comparing(StreamsGroupHeartbeatRequestData.TaskIds::subtopologyId))
            .collect(Collectors.toList());
        assertEquals(sortedExpected, sortedActual);
    }
}
