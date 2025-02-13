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
package org.apache.kafka.coordinator.group.streams.topics;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfigCollection;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse.Status;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.Subtopology;
import org.apache.kafka.coordinator.group.streams.StreamsTopology;
import org.apache.kafka.coordinator.group.streams.TopicMetadata;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InternalTopicManagerTest {

    public static final String SOURCE_TOPIC_1 = "source_topic1";
    public static final String SOURCE_TOPIC_2 = "source_topic2";
    public static final String REPARTITION_TOPIC = "repartition_topic";
    public static final String STATE_CHANGELOG_TOPIC_1 = "state_changelog_topic1";
    public static final String STATE_CHANGELOG_TOPIC_2 = "state_changelog_topic2";
    public static final String SUBTOPOLOGY_1 = "subtopology1";
    public static final String SUBTOPOLOGY_2 = "subtopology2";
    public static final String CONFIG_KEY = "cleanup.policy";
    public static final String CONFIG_VALUE = "compact";

    @Test
    void testConfigureTopicsSetsConfigurationExceptionWhenSourceTopicIsMissing() {
        Map<String, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(SOURCE_TOPIC_1, new TopicMetadata(Uuid.randomUuid(), SOURCE_TOPIC_1, 2));
        // SOURCE_TOPIC_2 is missing from topicMetadata
        StreamsTopology topology = makeTestTopology();

        final ConfiguredTopology configuredTopology = InternalTopicManager.configureTopics(new LogContext(), topology, topicMetadata);

        assertEquals(Optional.empty(), configuredTopology.subtopologies());
        assertTrue(configuredTopology.topicConfigurationException().isPresent());
        assertEquals(Status.MISSING_SOURCE_TOPICS, configuredTopology.topicConfigurationException().get().status());
        assertEquals(String.format("Source topics %s are missing.", SOURCE_TOPIC_2), configuredTopology.topicConfigurationException().get().getMessage());
    }

    @Test
    void testConfigureTopics() {
        Map<String, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(SOURCE_TOPIC_1, new TopicMetadata(Uuid.randomUuid(), SOURCE_TOPIC_1, 2));
        topicMetadata.put(SOURCE_TOPIC_2, new TopicMetadata(Uuid.randomUuid(), SOURCE_TOPIC_2, 2));
        topicMetadata.put(STATE_CHANGELOG_TOPIC_2,
            new TopicMetadata(Uuid.randomUuid(), STATE_CHANGELOG_TOPIC_2, 2));
        StreamsTopology topology = makeTestTopology();

        ConfiguredTopology configuredTopology = InternalTopicManager.configureTopics(new LogContext(), topology, topicMetadata);
        final Map<String, CreatableTopic> internalTopicsToBeCreated = configuredTopology.internalTopicsToBeCreated();

        assertEquals(2, internalTopicsToBeCreated.size());
        assertEquals(
            new CreatableTopic()
                .setName(REPARTITION_TOPIC)
                .setNumPartitions(2)
                .setReplicationFactor((short) 3),
            internalTopicsToBeCreated.get(REPARTITION_TOPIC)
        );
        assertEquals(
            new CreatableTopic()
                .setName(STATE_CHANGELOG_TOPIC_1)
                .setNumPartitions(2)
                .setReplicationFactor((short) -1)
                .setConfigs(
                    new CreatableTopicConfigCollection(
                        Collections.singletonList(new CreatableTopicConfig().setName(CONFIG_KEY).setValue(CONFIG_VALUE)).iterator())
                ),
            internalTopicsToBeCreated.get(STATE_CHANGELOG_TOPIC_1));

        Optional<Map<String, ConfiguredSubtopology>> expectedConfiguredTopology = Optional.of(makeExpectedConfiguredSubtopologies());
        assertEquals(expectedConfiguredTopology, configuredTopology.subtopologies());
    }

    private static Map<String, ConfiguredSubtopology> makeExpectedConfiguredSubtopologies() {
        return mkMap(
            mkEntry(SUBTOPOLOGY_1,
                new ConfiguredSubtopology(
                    Set.of(SOURCE_TOPIC_1),
                    Map.of(),
                    Set.of(REPARTITION_TOPIC),
                    Map.of(STATE_CHANGELOG_TOPIC_1,
                        new ConfiguredInternalTopic(
                            STATE_CHANGELOG_TOPIC_1,
                            2,
                            Optional.empty(),
                            Map.of(CONFIG_KEY, CONFIG_VALUE)
                        ))
                )
            ),
            mkEntry(SUBTOPOLOGY_2,
                new ConfiguredSubtopology(
                    Set.of(SOURCE_TOPIC_2),
                    Map.of(REPARTITION_TOPIC,
                        new ConfiguredInternalTopic(REPARTITION_TOPIC,
                            2,
                            Optional.of((short) 3),
                            Collections.emptyMap()
                        )
                    ),
                    Set.of(),
                    Map.of(STATE_CHANGELOG_TOPIC_2,
                        new ConfiguredInternalTopic(STATE_CHANGELOG_TOPIC_2,
                            2,
                            Optional.empty(),
                            Collections.emptyMap()
                        )))
            )
        );
    }

    private static StreamsTopology makeTestTopology() {
        // Create a subtopology source -> repartition
        Subtopology subtopology1 = new Subtopology()
            .setSubtopologyId(SUBTOPOLOGY_1)
            .setSourceTopics(Collections.singletonList(SOURCE_TOPIC_1))
            .setRepartitionSinkTopics(Collections.singletonList(REPARTITION_TOPIC))
            .setStateChangelogTopics(Collections.singletonList(
                new StreamsGroupTopologyValue.TopicInfo()
                    .setName(STATE_CHANGELOG_TOPIC_1)
                    .setTopicConfigs(Collections.singletonList(
                        new StreamsGroupTopologyValue.TopicConfig()
                            .setKey(CONFIG_KEY)
                            .setValue(CONFIG_VALUE)
                    ))
            ));
        // Create a subtopology repartition/source2 -> sink (copartitioned)
        Subtopology subtopology2 = new Subtopology()
            .setSubtopologyId(SUBTOPOLOGY_2)
            .setSourceTopics(Collections.singletonList(SOURCE_TOPIC_2))
            .setRepartitionSourceTopics(Collections.singletonList(
                new StreamsGroupTopologyValue.TopicInfo()
                    .setName(REPARTITION_TOPIC)
                    .setReplicationFactor((short) 3)
            ))
            .setStateChangelogTopics(Collections.singletonList(
                new StreamsGroupTopologyValue.TopicInfo()
                    .setName(STATE_CHANGELOG_TOPIC_2)
            ))
            .setCopartitionGroups(Collections.singletonList(
                new StreamsGroupTopologyValue.CopartitionGroup()
                    .setSourceTopics(Collections.singletonList((short) 0))
                    .setRepartitionSourceTopics(Collections.singletonList((short) 0))
            ));

        return new StreamsTopology(3, Map.of(SUBTOPOLOGY_1, subtopology1, SUBTOPOLOGY_2, subtopology2));
    }

}