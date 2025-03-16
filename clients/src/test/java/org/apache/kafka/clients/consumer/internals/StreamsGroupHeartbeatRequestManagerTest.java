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
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
        REPARTITION_SOURCE_TOPIC_1, new StreamsRebalanceData.TopicInfo(Optional.of(2), Optional.of((short) 1), Map.of("config1", "value1")),
        REPARTITION_SOURCE_TOPIC_2, new StreamsRebalanceData.TopicInfo(Optional.of(3), Optional.of((short) 3), Collections.emptyMap())
    );
    private static final String CHANGELOG_TOPIC_1 = "changelogTopic1";
    private static final String CHANGELOG_TOPIC_2 = "changelogTopic2";
    private static final String CHANGELOG_TOPIC_3 = "changelogTopic3";
    private static final Map<String, StreamsRebalanceData.TopicInfo> CHANGELOG_TOPICS = Map.of(
        CHANGELOG_TOPIC_1, new StreamsRebalanceData.TopicInfo(Optional.empty(), Optional.of((short) 1), Map.of()),
        CHANGELOG_TOPIC_2, new StreamsRebalanceData.TopicInfo(Optional.empty(), Optional.of((short) 2), Map.of()),
        CHANGELOG_TOPIC_3, new StreamsRebalanceData.TopicInfo(Optional.empty(), Optional.of((short) 3), Map.of("config2", "value2"))
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
    private static final Map<String, StreamsRebalanceData.Subtopology> SUBTOPOLOGIES =
        Map.of(SUBTOPOLOGY_NAME_1, SUBTOPOLOGY_1);
    private static final String CLIENT_TAG_1 = "client-tag1";
    private static final String VALUE_1 = "value1";
    private static final Map<String, String> CLIENT_TAGS = Map.of(CLIENT_TAG_1, VALUE_1);
    private static final List<StreamsGroupHeartbeatResponseData.EndpointToPartitions> ENDPOINT_TO_PARTITIONS =
        List.of(
            new StreamsGroupHeartbeatResponseData.EndpointToPartitions()
                .setUserEndpoint(new StreamsGroupHeartbeatResponseData.Endpoint().setHost("localhost").setPort(8080))
                .setPartitions(List.of(
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
                (mock, context) -> {
                    when(mock.heartbeatIntervalMs()).thenReturn(heartbeatIntervalMs);
                });
            final MockedConstruction<Timer> pollTimerMockedConstruction = mockConstruction(
                Timer.class,
                (mock, context) -> {
                    when(mock.isExpired()).thenReturn(true);
                });
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
                (mock, context) -> {
                    when(mock.timeToNextHeartbeatMs(time.milliseconds())).thenReturn(timeToNextHeartbeatMs);
                });
            final MockedConstruction<Timer> pollTimerMockedConstruction = mockConstruction(
                Timer.class,
                (mock, context) -> {
                    when(mock.isExpired()).thenReturn(true);
                });
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
    public void testSendingFullHeartbeatRequest() {
        try (
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> {
                    when(mock.canSendRequest(time.milliseconds())).thenReturn(true);
                })
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
            assertEquals(PROCESS_ID.toString(), streamsRequest.data().processId());
            assertEquals(ENDPOINT.host(), streamsRequest.data().userEndpoint().host());
            assertEquals(ENDPOINT.port(), streamsRequest.data().userEndpoint().port());
            assertEquals(1, streamsRequest.data().clientTags().size());
            assertEquals(CLIENT_TAG_1, streamsRequest.data().clientTags().get(0).key());
            assertEquals(VALUE_1, streamsRequest.data().clientTags().get(0).value());
            assertEquals(streamsRebalanceData.topologyEpoch(), streamsRequest.data().topology().epoch());
            assertNotNull(streamsRequest.data().topology());
            final List<StreamsGroupHeartbeatRequestData.Subtopology> subtopologies = streamsRequest.data().topology().subtopologies();
            assertEquals(1, subtopologies.size());
            final StreamsGroupHeartbeatRequestData.Subtopology subtopology = subtopologies.get(0);
            assertEquals(SUBTOPOLOGY_NAME_1, subtopology.subtopologyId());
            assertEquals(Arrays.asList("sourceTopic1", "sourceTopic2"), subtopology.sourceTopics());
            assertEquals(Arrays.asList("repartitionSinkTopic1", "repartitionSinkTopic2", "repartitionSinkTopic3"), subtopology.repartitionSinkTopics());
            assertEquals(REPARTITION_SOURCE_TOPICS.size(), subtopology.repartitionSourceTopics().size());
            subtopology.repartitionSourceTopics().forEach(topicInfo -> {
                final StreamsRebalanceData.TopicInfo repartitionTopic = REPARTITION_SOURCE_TOPICS.get(topicInfo.name());
                assertEquals(repartitionTopic.numPartitions().get(), topicInfo.partitions());
                assertEquals(repartitionTopic.replicationFactor().get(), topicInfo.replicationFactor());
                assertEquals(repartitionTopic.topicConfigs().size(), topicInfo.topicConfigs().size());
            });
            assertEquals(CHANGELOG_TOPICS.size(), subtopology.stateChangelogTopics().size());
            subtopology.stateChangelogTopics().forEach(topicInfo -> {
                assertTrue(CHANGELOG_TOPICS.containsKey(topicInfo.name()));
                assertEquals(0, topicInfo.partitions());
                final StreamsRebalanceData.TopicInfo changelogTopic = CHANGELOG_TOPICS.get(topicInfo.name());
                assertEquals(changelogTopic.replicationFactor().get(), topicInfo.replicationFactor());
                assertEquals(changelogTopic.topicConfigs().size(), topicInfo.topicConfigs().size());
            });
            assertEquals(2, subtopology.copartitionGroups().size());
            final StreamsGroupHeartbeatRequestData.CopartitionGroup expectedCopartitionGroupData1 =
                new StreamsGroupHeartbeatRequestData.CopartitionGroup()
                    .setRepartitionSourceTopics(Collections.singletonList((short) 0))
                    .setSourceTopics(Collections.singletonList((short) 1));
            final StreamsGroupHeartbeatRequestData.CopartitionGroup expectedCopartitionGroupData2 =
                new StreamsGroupHeartbeatRequestData.CopartitionGroup()
                    .setRepartitionSourceTopics(Collections.singletonList((short) 1))
                    .setSourceTopics(Collections.singletonList((short) 0));
            assertTrue(subtopology.copartitionGroups().contains(expectedCopartitionGroupData1));
            assertTrue(subtopology.copartitionGroups().contains(expectedCopartitionGroupData2));
            verify(heartbeatRequestState).onSendAttempt(time.milliseconds());
            verify(membershipManager).onHeartbeatRequestGenerated();
            time.sleep(2000);
            assertEquals(
                2.0,
                metrics.metric(metrics.metricName("last-heartbeat-seconds-ago", "consumer-coordinator-metrics")).metricValue()
            );
            final ClientResponse response = buildClientResponse();
            networkRequest.future().complete(response);
            verify(membershipManager).onHeartbeatSuccess((StreamsGroupHeartbeatResponse) response.responseBody());
            verify(heartbeatRequestState).updateHeartbeatIntervalMs(RECEIVED_HEARTBEAT_INTERVAL_MS);
            verify(heartbeatRequestState).onSuccessfulAttempt(networkRequest.handler().completionTimeMs());
            verify(heartbeatRequestState).resetTimer();
            final List<TopicPartition> topicPartitions = streamsRebalanceData.partitionsByHost()
                .get(new StreamsRebalanceData.HostInfo(
                    ENDPOINT_TO_PARTITIONS.get(0).userEndpoint().host(),
                    ENDPOINT_TO_PARTITIONS.get(0).userEndpoint().port())
                );
            assertEquals(ENDPOINT_TO_PARTITIONS.get(0).partitions().get(0).topic(), topicPartitions.get(0).topic());
            assertEquals(ENDPOINT_TO_PARTITIONS.get(0).partitions().get(0).partitions().get(0), topicPartitions.get(0).partition());
            assertEquals(
                1.0,
                metrics.metric(metrics.metricName("heartbeat-total", "consumer-coordinator-metrics")).metricValue()
            );
        }
    }

    @Test
    public void testSendingLeaveHeartbeatRequestWhenPollTimerExpired() {
        try (
            final MockedConstruction<HeartbeatRequestState> heartbeatRequestStateMockedConstruction = mockConstruction(
                HeartbeatRequestState.class,
                (mock, context) -> {
                    when(mock.canSendRequest(time.milliseconds())).thenReturn(true);
                });
            final MockedConstruction<Timer> pollTimerMockedConstruction = mockConstruction(
                Timer.class,
                (mock, context) -> {
                    when(mock.isExpired()).thenReturn(true);
                });
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
            networkRequest.future().complete(response);
            verify(heartbeatRequestState, never()).updateHeartbeatIntervalMs(anyLong());
            verify(heartbeatRequestState, never()).onSuccessfulAttempt(anyLong());
            verify(membershipManager, never()).onHeartbeatSuccess(any());
        }
    }

    private static ConsumerConfig config() {
        Properties prop = new Properties();
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(DEFAULT_MAX_POLL_INTERVAL_MS));
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
}