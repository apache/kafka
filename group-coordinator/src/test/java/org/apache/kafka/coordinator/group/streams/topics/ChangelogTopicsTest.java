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

import org.apache.kafka.common.errors.StreamsInvalidTopologyException;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.Subtopology;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.TopicConfig;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.TopicInfo;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ChangelogTopicsTest {

    private static final Logger LOG = LoggerFactory.getLogger(ChangelogTopicsTest.class);
    private static final String SOURCE_TOPIC_NAME = "source";
    private static final String SINK_TOPIC_NAME = "sink";
    private static final String REPARTITION_TOPIC_NAME = "repartition";
    private static final String CHANGELOG_TOPIC_NAME1 = "changelog1";
    private static final TopicConfig TOPIC_CONFIG = new TopicConfig().setKey("config1").setValue("val1");
    private static final TopicInfo REPARTITION_TOPIC_INFO = new TopicInfo()
        .setName(REPARTITION_TOPIC_NAME)
        .setTopicConfigs(List.of(TOPIC_CONFIG));
    private static final Subtopology SUBTOPOLOGY_NO_SOURCE = new Subtopology()
        .setSubtopologyId("SUBTOPOLOGY_NO_SOURCE")
        .setSourceTopics(List.of())
        .setRepartitionSinkTopics(List.of(SINK_TOPIC_NAME))
        .setRepartitionSourceTopics(List.of(REPARTITION_TOPIC_INFO))
        .setStateChangelogTopics(List.of());
    private static final Subtopology SUBTOPOLOGY_NO_REPARTITION_SOURCE = new Subtopology()
        .setSubtopologyId("SUBTOPOLOGY_NO_SOURCE")
        .setSourceTopics(List.of(SOURCE_TOPIC_NAME))
        .setRepartitionSinkTopics(List.of(SINK_TOPIC_NAME))
        .setRepartitionSourceTopics(List.of())
        .setStateChangelogTopics(List.of());
    private static final Subtopology SUBTOPOLOGY_NO_SOURCE_NO_REPARTITION_SOURCE = new Subtopology()
        .setSubtopologyId("SUBTOPOLOGY_NO_SOURCE")
        .setSourceTopics(List.of())
        .setRepartitionSinkTopics(List.of(SINK_TOPIC_NAME))
        .setRepartitionSourceTopics(List.of())
        .setStateChangelogTopics(List.of());
    private static final Subtopology SUBTOPOLOGY_STATELESS = new Subtopology()
        .setSubtopologyId("SUBTOPOLOGY_STATELESS")
        .setSourceTopics(List.of(SOURCE_TOPIC_NAME))
        .setRepartitionSinkTopics(List.of(SINK_TOPIC_NAME))
        .setRepartitionSourceTopics(List.of(REPARTITION_TOPIC_INFO))
        .setStateChangelogTopics(List.of());
    private static final TopicInfo SOURCE_CHANGELOG_TOPIC_CONFIG = new TopicInfo()
        .setName(SOURCE_TOPIC_NAME)
        .setTopicConfigs(List.of(TOPIC_CONFIG));
    private static final Subtopology SUBTOPOLOGY_SOURCE_CHANGELOG = new Subtopology()
        .setSubtopologyId("SUBTOPOLOGY_SOURCE_CHANGELOG")
        .setSourceTopics(List.of(SOURCE_TOPIC_NAME))
        .setRepartitionSinkTopics(List.of(SINK_TOPIC_NAME))
        .setRepartitionSourceTopics(List.of(REPARTITION_TOPIC_INFO))
        .setStateChangelogTopics(List.of(SOURCE_CHANGELOG_TOPIC_CONFIG));
    private static final TopicInfo CHANGELOG_TOPIC_CONFIG = new TopicInfo()
        .setName(CHANGELOG_TOPIC_NAME1)
        .setTopicConfigs(List.of(TOPIC_CONFIG));
    private static final Subtopology SUBTOPOLOGY_STATEFUL = new Subtopology()
        .setSubtopologyId("SUBTOPOLOGY_STATEFUL")
        .setSourceTopics(List.of(SOURCE_TOPIC_NAME))
        .setRepartitionSinkTopics(List.of(SINK_TOPIC_NAME))
        .setRepartitionSourceTopics(List.of(REPARTITION_TOPIC_INFO))
        .setStateChangelogTopics(List.of(CHANGELOG_TOPIC_CONFIG));
    private static final Subtopology SUBTOPOLOGY_BOTH = new Subtopology()
        .setSubtopologyId("SUBTOPOLOGY_BOTH")
        .setSourceTopics(List.of(SOURCE_TOPIC_NAME))
        .setRepartitionSinkTopics(List.of(SINK_TOPIC_NAME))
        .setRepartitionSourceTopics(List.of(REPARTITION_TOPIC_INFO))
        .setStateChangelogTopics(List.of(SOURCE_CHANGELOG_TOPIC_CONFIG, CHANGELOG_TOPIC_CONFIG));

    private static OptionalInt topicPartitionProvider(String s) {
        return OptionalInt.of(3);
    }

    @Test
    public void shouldFailIfNoSourceTopicsAndNoRepartitionSourceTopics() {
        final List<Subtopology> subtopologies = List.of(SUBTOPOLOGY_NO_SOURCE_NO_REPARTITION_SOURCE);

        final ChangelogTopics changelogTopics =
            new ChangelogTopics(LOG, subtopologies, ChangelogTopicsTest::topicPartitionProvider);
        StreamsInvalidTopologyException e = assertThrows(StreamsInvalidTopologyException.class, changelogTopics::setup);

        assertTrue(e.getMessage().contains("No source topics found for subtopology"));
    }

    @Test
    public void shouldNotFailIfOnlySourceTopicsEmpty() {
        final List<Subtopology> subtopologies = List.of(SUBTOPOLOGY_NO_SOURCE);

        final ChangelogTopics changelogTopics =
            new ChangelogTopics(LOG, subtopologies, ChangelogTopicsTest::topicPartitionProvider);
        assertDoesNotThrow(changelogTopics::setup);
    }

    @Test
    public void shouldNotFailIfOnlyRepartitionSourceTopicsEmpty() {
        final List<Subtopology> subtopologies = List.of(SUBTOPOLOGY_NO_REPARTITION_SOURCE);

        final ChangelogTopics changelogTopics =
            new ChangelogTopics(LOG, subtopologies, ChangelogTopicsTest::topicPartitionProvider);
        assertDoesNotThrow(changelogTopics::setup);
    }

    @Test
    public void shouldNotContainChangelogsForStatelessTasks() {
        final List<Subtopology> subtopologies = List.of(SUBTOPOLOGY_STATELESS);

        final ChangelogTopics changelogTopics =
            new ChangelogTopics(LOG, subtopologies, ChangelogTopicsTest::topicPartitionProvider);
        Map<String, Integer> setup = changelogTopics.setup();

        assertEquals(Map.of(), setup);
    }

    @Test
    public void shouldContainNonSourceBasedChangelogs() {
        final List<Subtopology> subtopologies = List.of(SUBTOPOLOGY_STATEFUL);

        final ChangelogTopics changelogTopics =
            new ChangelogTopics(LOG, subtopologies, ChangelogTopicsTest::topicPartitionProvider);
        Map<String, Integer> setup = changelogTopics.setup();

        assertEquals(Map.of(CHANGELOG_TOPIC_CONFIG.name(), 3), setup);
    }

    @Test
    public void shouldContainSourceBasedChangelogs() {
        final List<Subtopology> subtopologies = List.of(SUBTOPOLOGY_SOURCE_CHANGELOG);

        final ChangelogTopics changelogTopics =
            new ChangelogTopics(LOG, subtopologies, ChangelogTopicsTest::topicPartitionProvider);
        Map<String, Integer> setup = changelogTopics.setup();

        assertEquals(Map.of(SOURCE_TOPIC_NAME, 3), setup);
    }

    @Test
    public void shouldContainBothTypesOfPreExistingChangelogs() {
        final List<Subtopology> subtopologies = List.of(SUBTOPOLOGY_BOTH);

        final ChangelogTopics changelogTopics =
            new ChangelogTopics(LOG, subtopologies, ChangelogTopicsTest::topicPartitionProvider);
        Map<String, Integer> setup = changelogTopics.setup();

        assertEquals(Map.of(CHANGELOG_TOPIC_CONFIG.name(), 3, SOURCE_TOPIC_NAME, 3), setup);
    }
}
