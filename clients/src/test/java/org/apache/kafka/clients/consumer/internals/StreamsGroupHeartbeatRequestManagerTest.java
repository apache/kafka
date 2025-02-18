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
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest;
import org.apache.kafka.clients.consumer.internals.StreamsAssignmentInterface.Subtopology;
import org.apache.kafka.clients.consumer.internals.StreamsAssignmentInterface.TopicInfo;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.apache.kafka.clients.consumer.internals.StreamsGroupHeartbeatRequestManager.getTopicPartitionList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StreamsGroupHeartbeatRequestManagerTest {

    public static final String TEST_GROUP_ID = "testGroupId";
    public static final String TEST_MEMBER_ID = "testMemberId";
    public static final int TEST_MEMBER_EPOCH = 5;
    public static final String TEST_INSTANCE_ID = "instanceId";
    public static final int TEST_THROTTLE_TIME_MS = 5;
    private static final int DEFAULT_MAX_POLL_INTERVAL_MS = 10000;
    private StreamsGroupHeartbeatRequestManager heartbeatRequestManager;

    private Time time;

    private StreamsAssignmentInterface streamsAssignmentInterface;

    private ConsumerConfig config;

    @Mock
    private CoordinatorRequestManager coordinatorRequestManager;

    @Mock
    private StreamsMembershipManager membershipManager;

    @Mock
    private BackgroundEventHandler backgroundEventHandler;

    @Mock
    private Metrics metrics;

    @Mock
    private ConsumerMetadata metadata;

    // Static data for testing
    private final UUID processID = new UUID(1, 1);

    private final StreamsAssignmentInterface.HostInfo endPoint = new StreamsAssignmentInterface.HostInfo("localhost", 8080);

    private final Map<String, Subtopology> subtopologyMap = new HashMap<>();

    private final Map<String, String> clientTags = new HashMap<>();

    private final Node coordinatorNode = new Node(1, "localhost", 9092);

    @BeforeEach
    void setUp() {
        config = config();

        subtopologyMap.clear();
        clientTags.clear();
        streamsAssignmentInterface =
            new StreamsAssignmentInterface(
                processID,
                Optional.of(endPoint),
                subtopologyMap,
                clientTags
            );
        LogContext logContext = new LogContext("test");
        time = new MockTime();

        MockitoAnnotations.openMocks(this);
        when(metrics.sensor(anyString())).thenReturn(mock(Sensor.class));
        heartbeatRequestManager = new StreamsGroupHeartbeatRequestManager(
            logContext,
            time,
            config,
            coordinatorRequestManager,
            membershipManager,
            backgroundEventHandler,
            metrics,
            streamsAssignmentInterface
        );

        when(membershipManager.groupId()).thenReturn(TEST_GROUP_ID);
        when(membershipManager.memberId()).thenReturn(TEST_MEMBER_ID);
        when(membershipManager.memberEpoch()).thenReturn(TEST_MEMBER_EPOCH);
        when(membershipManager.groupInstanceId()).thenReturn(Optional.of(TEST_INSTANCE_ID));
    }


    @Test
    void testNoHeartbeatIfCoordinatorUnknown() {
        when(membershipManager.shouldHeartbeatNow()).thenReturn(true);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.empty());

        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

        assertEquals(0, result.unsentRequests.size());
        verify(membershipManager).transitionToUnsubscribeIfLeaving();
    }

    @Test
    void testNoHeartbeatIfHeartbeatSkipped() {
        when(membershipManager.shouldSkipHeartbeat()).thenReturn(true);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));

        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

        assertEquals(0, result.unsentRequests.size());
        verify(membershipManager).transitionToUnsubscribeIfLeaving();
    }

    @Test
    void testHeartbeatWhenCoordinatorKnown() {
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));

        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

        assertEquals(1, result.unsentRequests.size());
        assertEquals(Optional.of(coordinatorNode), result.unsentRequests.get(0).node());

        StreamsGroupHeartbeatRequest request = (StreamsGroupHeartbeatRequest) result.unsentRequests.get(0).requestBuilder().build();

        assertEquals(TEST_GROUP_ID, request.data().groupId());
        assertEquals(TEST_MEMBER_ID, request.data().memberId());
        assertEquals(TEST_MEMBER_EPOCH, request.data().memberEpoch());
        assertEquals(TEST_INSTANCE_ID, request.data().instanceId());

        // Static information is null
        assertNull(request.data().processId());
        assertNull(request.data().userEndpoint());
        assertNull(request.data().clientTags());
        assertNull(request.data().topology());
    }

    @Test
    void testFullStaticInformationWhenJoining() {
        mockJoiningState();

        final Set<String> sourceTopics = Set.of("sourceTopic1", "sourceTopic2");
        final Set<String> repartitionSinkTopics = Set.of("repartitionSinkTopic1", "repartitionSinkTopic2", "repartitionSinkTopic3");
        final Map<String, StreamsAssignmentInterface.TopicInfo> repartitionSourceTopics = mkMap(
            mkEntry("repartitionTopic1", new StreamsAssignmentInterface.TopicInfo(Optional.of(2), Optional.of((short) 1), Map.of("config1", "value1"))),
            mkEntry("repartitionTopic2", new StreamsAssignmentInterface.TopicInfo(Optional.of(3), Optional.of((short) 3), Collections.emptyMap()))
        );
        final Map<String, StreamsAssignmentInterface.TopicInfo> changelogTopics = mkMap(
            mkEntry("changelogTopic1", new StreamsAssignmentInterface.TopicInfo(Optional.empty(), Optional.of((short) 1), Collections.emptyMap())),
            mkEntry("changelogTopic2", new StreamsAssignmentInterface.TopicInfo(Optional.empty(), Optional.of((short) 2), Collections.emptyMap())),
            mkEntry("changelogTopic3", new StreamsAssignmentInterface.TopicInfo(Optional.empty(), Optional.of((short) 3), Map.of("config2", "value2")))
        );
        final Collection<Set<String>> copartitionGroup = Set.of(
            Set.of("sourceTopic1", "repartitionTopic2"),
            Set.of("sourceTopic2", "repartitionTopic1")
        );
        final StreamsAssignmentInterface.Subtopology subtopology1 = new StreamsAssignmentInterface.Subtopology(
            sourceTopics,
            repartitionSinkTopics,
            repartitionSourceTopics,
            changelogTopics,
            copartitionGroup
        );
        final String subtopologyName1 = "subtopology1";
        subtopologyMap.put(subtopologyName1, subtopology1);

        clientTags.put("clientTag1", "value2");

        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

        assertEquals(1, result.unsentRequests.size());
        assertEquals(Optional.of(coordinatorNode), result.unsentRequests.get(0).node());

        StreamsGroupHeartbeatRequest request = (StreamsGroupHeartbeatRequest) result.unsentRequests.get(0).requestBuilder().build();

        assertEquals(processID.toString(), request.data().processId());
        assertEquals(endPoint.host, request.data().userEndpoint().host());
        assertEquals(endPoint.port, request.data().userEndpoint().port());
        assertEquals(1, request.data().clientTags().size());
        assertEquals("clientTag1", request.data().clientTags().get(0).key());
        assertEquals("value2", request.data().clientTags().get(0).value());
        assertEquals(streamsAssignmentInterface.topologyEpoch(), request.data().topology().epoch());
        assertNotNull(request.data().topology());
        final List<StreamsGroupHeartbeatRequestData.Subtopology> subtopologies = request.data().topology().subtopologies();
        assertEquals(1, subtopologies.size());
        final StreamsGroupHeartbeatRequestData.Subtopology subtopology = subtopologies.get(0);
        assertEquals(subtopologyName1, subtopology.subtopologyId());
        assertEquals(Arrays.asList("sourceTopic1", "sourceTopic2"), subtopology.sourceTopics());
        assertEquals(Arrays.asList("repartitionSinkTopic1", "repartitionSinkTopic2", "repartitionSinkTopic3"), subtopology.repartitionSinkTopics());
        assertEquals(repartitionSourceTopics.size(), subtopology.repartitionSourceTopics().size());
        subtopology.repartitionSourceTopics().forEach(topicInfo -> {
            final StreamsAssignmentInterface.TopicInfo repartitionTopic = repartitionSourceTopics.get(topicInfo.name());
            assertEquals(repartitionTopic.numPartitions.get(), topicInfo.partitions());
            assertEquals(repartitionTopic.replicationFactor.get(), topicInfo.replicationFactor());
            assertEquals(repartitionTopic.topicConfigs.size(), topicInfo.topicConfigs().size());
        });
        assertEquals(changelogTopics.size(), subtopology.stateChangelogTopics().size());
        subtopology.stateChangelogTopics().forEach(topicInfo -> {
            assertTrue(changelogTopics.containsKey(topicInfo.name()));
            assertEquals(0, topicInfo.partitions());
            final StreamsAssignmentInterface.TopicInfo changelogTopic = changelogTopics.get(topicInfo.name());
            assertEquals(changelogTopic.replicationFactor.get(), topicInfo.replicationFactor());
            assertEquals(changelogTopic.topicConfigs.size(), topicInfo.topicConfigs().size());
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
    }

    @Test
    void testShutdownRequested() {
        mockJoiningState();
        streamsAssignmentInterface.requestShutdown();

        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

        assertEquals(1, result.unsentRequests.size());
        assertEquals(Optional.of(coordinatorNode), result.unsentRequests.get(0).node());

        StreamsGroupHeartbeatRequest request = (StreamsGroupHeartbeatRequest) result.unsentRequests.get(0).requestBuilder().build();

        assertEquals(true, request.data().shutdownApplication());
    }

    @Test
    void testSuccessfulResponse() {
        mockJoiningState();

        final Uuid uuid0 = Uuid.randomUuid();
        final Uuid uuid1 = Uuid.randomUuid();

        final StreamsGroupHeartbeatResponseData.Endpoint endpoint = new StreamsGroupHeartbeatResponseData.Endpoint();
        endpoint.setHost("localhost");
        endpoint.setPort(8080);
        StreamsGroupHeartbeatResponseData.TopicPartition active = new StreamsGroupHeartbeatResponseData.TopicPartition();
        active.setTopic("activeTopic");
        active.setPartitions(Arrays.asList(0, 1, 2));
        StreamsGroupHeartbeatResponseData.TopicPartition standby = new StreamsGroupHeartbeatResponseData.TopicPartition();
        standby.setTopic("standbyTopic");
        standby.setPartitions(Arrays.asList(3, 4, 5));
        StreamsGroupHeartbeatResponseData.EndpointToPartitions endpointToPartitions = new StreamsGroupHeartbeatResponseData.EndpointToPartitions();
        endpointToPartitions.setActivePartitions(List.of(active));
        endpointToPartitions.setStandbyPartitions(List.of(standby));
        endpointToPartitions.setUserEndpoint(endpoint);

        final TopicInfo emptyTopicInfo = new TopicInfo(Optional.empty(), Optional.empty(), Collections.emptyMap());

        when(metadata.topicIds()).thenReturn(
            mkMap(
                mkEntry("source0", uuid0),
                mkEntry("repartition0", uuid1)
            ));

        streamsAssignmentInterface.subtopologyMap().put("0",
            new Subtopology(
                Collections.singleton("source0"),
                Collections.singleton("sink0"),
                Collections.singletonMap("repartition0", emptyTopicInfo),
                Collections.singletonMap("changelog0", emptyTopicInfo),
                Collections.singletonList(Set.of("source0", "repartition0"))
            ));
        streamsAssignmentInterface.subtopologyMap().put("1",
            new Subtopology(
                Collections.singleton("source1"),
                Collections.singleton("sink1"),
                Collections.singletonMap("repartition1", emptyTopicInfo),
                Collections.singletonMap("changelog1", emptyTopicInfo),
                Collections.singletonList(Set.of("source1", "repartition1"))
            ));
        streamsAssignmentInterface.subtopologyMap().put("2",
            new Subtopology(
                Collections.singleton("source2"),
                Collections.singleton("sink2"),
                Collections.singletonMap("repartition2", emptyTopicInfo),
                Collections.singletonMap("changelog2", emptyTopicInfo),
                Collections.singletonList(Set.of("source2", "repartition2"))
            ));

        StreamsGroupHeartbeatResponseData data = new StreamsGroupHeartbeatResponseData()
            .setErrorCode(Errors.NONE.code())
            .setThrottleTimeMs(0)
            .setMemberId(TEST_MEMBER_ID)
            .setMemberEpoch(TEST_MEMBER_EPOCH)
            .setThrottleTimeMs(TEST_THROTTLE_TIME_MS)
            .setHeartbeatIntervalMs(1000)
            .setPartitionsByUserEndpoint(List.of(endpointToPartitions))
            .setActiveTasks(Collections.singletonList(
                new StreamsGroupHeartbeatResponseData.TaskIds().setSubtopologyId("0").setPartitions(Collections.singletonList(0))))
            .setStandbyTasks(Collections.singletonList(
                new StreamsGroupHeartbeatResponseData.TaskIds().setSubtopologyId("1").setPartitions(Collections.singletonList(1))))
            .setWarmupTasks(Collections.singletonList(
                new StreamsGroupHeartbeatResponseData.TaskIds().setSubtopologyId("2").setPartitions(Collections.singletonList(2))));

        mockResponse(data);

        ArgumentCaptor<StreamsGroupHeartbeatResponse> captor = ArgumentCaptor.forClass(StreamsGroupHeartbeatResponse.class);
        verify(membershipManager, times(1)).onHeartbeatSuccess(captor.capture());
        StreamsGroupHeartbeatResponseData response = captor.getValue().data();
        assertEquals(Errors.NONE.code(), response.errorCode());
        assertEquals(TEST_MEMBER_ID, response.memberId());
        assertEquals(TEST_MEMBER_EPOCH, response.memberEpoch());
        assertEquals(TEST_THROTTLE_TIME_MS, response.throttleTimeMs());
        assertEquals(1000, response.heartbeatIntervalMs());
        assertEquals(data.activeTasks(), response.activeTasks());
        assertEquals(data.standbyTasks(), response.standbyTasks());
        assertEquals(data.warmupTasks(), response.warmupTasks());

        assertEquals(data.partitionsByUserEndpoint(), response.partitionsByUserEndpoint());
        Map<StreamsAssignmentInterface.HostInfo, StreamsAssignmentInterface.EndpointPartitions> endpointPartitionsMap = streamsAssignmentInterface.partitionsByHost.get();
        assertEquals(endpointPartitionsMap.size(), response.partitionsByUserEndpoint().size());
        StreamsAssignmentInterface.HostInfo hostInfo = endpointPartitionsMap.keySet().iterator().next();
        assertEquals(endpoint.host(), hostInfo.host);
        assertEquals(endpoint.port(), hostInfo.port);
        StreamsAssignmentInterface.EndpointPartitions endpointPartitions = endpointPartitionsMap.get(hostInfo);
        List<TopicPartition> activeTopicPartitions = getTopicPartitionList(endpointToPartitions.activePartitions());
        List<TopicPartition> standbyTopicPartitions = getTopicPartitionList(endpointToPartitions.standbyPartitions());
        assertIterableEquals(endpointPartitions.activePartitions(), activeTopicPartitions);
        assertIterableEquals(endpointPartitions.standbyPartitions(), standbyTopicPartitions);
    }

    private void mockResponse(final StreamsGroupHeartbeatResponseData data) {

        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

        assertEquals(1, result.unsentRequests.size());
        final UnsentRequest unsentRequest = result.unsentRequests.get(0);
        assertEquals(Optional.of(coordinatorNode), unsentRequest.node());

        ClientResponse response = createHeartbeatResponse(unsentRequest, data);

        unsentRequest.handler().onComplete(response);
    }

    private void mockJoiningState() {
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
        when(membershipManager.state()).thenReturn(MemberState.JOINING);
    }

    private ClientResponse createHeartbeatResponse(
        final NetworkClientDelegate.UnsentRequest request,
        final StreamsGroupHeartbeatResponseData data
    ) {
        StreamsGroupHeartbeatResponse response = new StreamsGroupHeartbeatResponse(data);
        return new ClientResponse(
            new RequestHeader(ApiKeys.STREAMS_GROUP_HEARTBEAT, ApiKeys.STREAMS_GROUP_HEARTBEAT.latestVersion(), "client-id", 1),
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
        prop.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(DEFAULT_MAX_POLL_INTERVAL_MS));
        return new ConsumerConfig(prop);
    }
}