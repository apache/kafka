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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.Subtopology;
import org.apache.kafka.coordinator.group.streams.StreamsTopology;
import org.apache.kafka.image.MetadataImage;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InternalTopicManagerTest {

    public static final MockTime TIME = new MockTime();
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
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), SOURCE_TOPIC_1, 2)
            .build();
        // SOURCE_TOPIC_2 is missing from topicMetadata
        StreamsTopology topology = makeTestTopology();

        final TopologyValidationResult result = InternalTopicManager.configureTopics(new LogContext(), 0L, topology, new KRaftCoordinatorMetadataImage(metadataImage), TIME);

        assertFalse(result.isReady());
        assertTrue(result.numTasksBySubtopology().isEmpty());
        assertTrue(result.topicConfigurationException().isPresent());
        assertEquals(Status.MISSING_SOURCE_TOPICS, result.topicConfigurationException().get().status());
        assertEquals(String.format("Source topics %s are missing.", SOURCE_TOPIC_2), result.topicConfigurationException().get().getMessage());
    }

    @Test
    void testConfigureTopicsWithMissingInternalTopics() {
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), SOURCE_TOPIC_1, 2)
            .addTopic(Uuid.randomUuid(), SOURCE_TOPIC_2, 2)
            .addTopic(Uuid.randomUuid(), STATE_CHANGELOG_TOPIC_2, 2)
            .build();
        StreamsTopology topology = makeTestTopology();

        TopologyValidationResult result = InternalTopicManager.configureTopics(new LogContext(), 0L, topology, new KRaftCoordinatorMetadataImage(metadataImage), TIME);
        final Map<String, CreatableTopic> internalTopicsToBeCreated = result.internalTopicsToBeCreated();

        // Not ready because internal topics are missing
        assertFalse(result.isReady());
        assertTrue(result.numTasksBySubtopology().isEmpty());
        assertTrue(result.topicConfigurationException().isPresent());
        assertEquals(Status.MISSING_INTERNAL_TOPICS, result.topicConfigurationException().get().status());

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
                        List.of(new CreatableTopicConfig().setName(CONFIG_KEY).setValue(CONFIG_VALUE)).iterator())
                ),
            internalTopicsToBeCreated.get(STATE_CHANGELOG_TOPIC_1));
    }

    @Test
    void testConfigureTopicsAllTopicsExist() {
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), SOURCE_TOPIC_1, 2)
            .addTopic(Uuid.randomUuid(), SOURCE_TOPIC_2, 2)
            .addTopic(Uuid.randomUuid(), STATE_CHANGELOG_TOPIC_1, 2)
            .addTopic(Uuid.randomUuid(), STATE_CHANGELOG_TOPIC_2, 2)
            .addTopic(Uuid.randomUuid(), REPARTITION_TOPIC, 2)
            .build();
        StreamsTopology topology = makeTestTopology();

        TopologyValidationResult result = InternalTopicManager.configureTopics(new LogContext(), 0L, topology, new KRaftCoordinatorMetadataImage(metadataImage), TIME);

        assertTrue(result.isReady());
        assertTrue(result.numTasksBySubtopology().isPresent());
        assertTrue(result.topicConfigurationException().isEmpty());
        assertTrue(result.internalTopicsToBeCreated().isEmpty());

        // Verify numTasksBySubtopology is correctly populated
        assertEquals(2, result.numTasksBySubtopology().get().get(SUBTOPOLOGY_1));
        assertEquals(2, result.numTasksBySubtopology().get().get(SUBTOPOLOGY_2));
    }

    private static StreamsTopology makeTestTopology() {
        // Create a subtopology source -> repartition
        Subtopology subtopology1 = new Subtopology()
            .setSubtopologyId(SUBTOPOLOGY_1)
            .setSourceTopics(List.of(SOURCE_TOPIC_1))
            .setRepartitionSinkTopics(List.of(REPARTITION_TOPIC))
            .setStateChangelogTopics(List.of(
                new StreamsGroupTopologyValue.TopicInfo()
                    .setName(STATE_CHANGELOG_TOPIC_1)
                    .setTopicConfigs(List.of(
                        new StreamsGroupTopologyValue.TopicConfig()
                            .setKey(CONFIG_KEY)
                            .setValue(CONFIG_VALUE)
                    ))
            ));
        // Create a subtopology repartition/source2 -> sink (copartitioned)
        Subtopology subtopology2 = new Subtopology()
            .setSubtopologyId(SUBTOPOLOGY_2)
            .setSourceTopics(List.of(SOURCE_TOPIC_2))
            .setRepartitionSourceTopics(List.of(
                new StreamsGroupTopologyValue.TopicInfo()
                    .setName(REPARTITION_TOPIC)
                    .setReplicationFactor((short) 3)
            ))
            .setStateChangelogTopics(List.of(
                new StreamsGroupTopologyValue.TopicInfo()
                    .setName(STATE_CHANGELOG_TOPIC_2)
            ))
            .setCopartitionGroups(List.of(
                new StreamsGroupTopologyValue.CopartitionGroup()
                    .setSourceTopics(List.of((short) 0))
                    .setRepartitionSourceTopics(List.of((short) 0))
            ));

        return new StreamsTopology(3, Map.of(SUBTOPOLOGY_1, subtopology1, SUBTOPOLOGY_2, subtopology2));
    }

}
