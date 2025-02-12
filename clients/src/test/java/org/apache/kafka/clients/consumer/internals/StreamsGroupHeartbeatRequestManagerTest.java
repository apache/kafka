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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
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

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StreamsGroupHeartbeatRequestManagerTest {

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
    private static final Map<String, StreamsRebalanceData.TopicInfo> REPARTITION_SOURCE_TOPICS = mkMap(
        mkEntry(REPARTITION_SOURCE_TOPIC_1, new StreamsRebalanceData.TopicInfo(Optional.of(2), Optional.of((short) 1), Map.of("config1", "value1"))),
        mkEntry(REPARTITION_SOURCE_TOPIC_2, new StreamsRebalanceData.TopicInfo(Optional.of(3), Optional.of((short) 3), Collections.emptyMap()))
    );
    private static final String CHANGELOG_TOPIC_1 = "changelogTopic1";
    private static final String CHANGELOG_TOPIC_2 = "changelogTopic2";
    private static final String CHANGELOG_TOPIC_3 = "changelogTopic3";
    private static final Map<String, StreamsRebalanceData.TopicInfo> CHANGELOG_TOPICS = mkMap(
        mkEntry(CHANGELOG_TOPIC_1, new StreamsRebalanceData.TopicInfo(Optional.empty(), Optional.of((short) 1), Collections.emptyMap())),
        mkEntry(CHANGELOG_TOPIC_2, new StreamsRebalanceData.TopicInfo(Optional.empty(), Optional.of((short) 2), Collections.emptyMap())),
        mkEntry(CHANGELOG_TOPIC_3, new StreamsRebalanceData.TopicInfo(Optional.empty(), Optional.of((short) 3), Map.of("config2", "value2")))
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
    private static final Map<String, StreamsRebalanceData.Subtopology> SUBTOPOLOGIES = mkMap(
        mkEntry(SUBTOPOLOGY_NAME_1, SUBTOPOLOGY_1)
    );
    private static final Map<String, String> CLIENT_TAGS = Map.of("clientTag1", "value2");

    private final StreamsRebalanceData streamsRebalanceData = new StreamsRebalanceData(
        PROCESS_ID,
        Optional.of(ENDPOINT),
        SUBTOPOLOGIES,
        CLIENT_TAGS
    );

    private StreamsGroupHeartbeatRequestManager heartbeatRequestManager;

    private Time time = new MockTime();

    private ConsumerConfig config;

    @Mock
    private CoordinatorRequestManager coordinatorRequestManager;

    @Mock
    private StreamsMembershipManager membershipManager;

    @Mock
    private BackgroundEventHandler backgroundEventHandler;

    @Mock
    private Metrics metrics;

    private final Node coordinatorNode = new Node(1, "localhost", 9092);

    @BeforeEach
    void setUp() {
        config = config();

        LogContext logContext = new LogContext("test");

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
            streamsRebalanceData
        );
    }

    private ConsumerConfig config() {
        Properties prop = new Properties();
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(DEFAULT_MAX_POLL_INTERVAL_MS));
        return new ConsumerConfig(prop);
    }

    @Test
    void testSendingFullHeartbeatRequest() {
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
        when(membershipManager.groupId()).thenReturn(GROUP_ID);
        when(membershipManager.memberId()).thenReturn(MEMBER_ID);
        when(membershipManager.memberEpoch()).thenReturn(MEMBER_EPOCH);
        when(membershipManager.groupInstanceId()).thenReturn(Optional.of(INSTANCE_ID));

        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

        assertEquals(1, result.unsentRequests.size());
        assertEquals(Optional.of(coordinatorNode), result.unsentRequests.get(0).node());
        StreamsGroupHeartbeatRequest request = (StreamsGroupHeartbeatRequest) result.unsentRequests.get(0).requestBuilder().build();
        assertEquals(GROUP_ID, request.data().groupId());
        assertEquals(MEMBER_ID, request.data().memberId());
        assertEquals(MEMBER_EPOCH, request.data().memberEpoch());
        assertEquals(INSTANCE_ID, request.data().instanceId());
        assertEquals(PROCESS_ID.toString(), request.data().processId());
        assertEquals(ENDPOINT.host(), request.data().userEndpoint().host());
        assertEquals(ENDPOINT.port(), request.data().userEndpoint().port());
        assertEquals(1, request.data().clientTags().size());
        assertEquals("clientTag1", request.data().clientTags().get(0).key());
        assertEquals("value2", request.data().clientTags().get(0).value());
        assertEquals(streamsRebalanceData.topologyEpoch(), request.data().topology().epoch());
        assertNotNull(request.data().topology());
        final List<StreamsGroupHeartbeatRequestData.Subtopology> subtopologies = request.data().topology().subtopologies();
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
    }
}