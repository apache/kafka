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
import org.apache.kafka.common.utils.LogContext;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ChangelogTopicsTest {

    private static final LogContext LOG_CONTEXT = new LogContext();
    private static final String SOURCE_TOPIC_NAME = "source";
    private static final String SINK_TOPIC_NAME = "sink";
    private static final String REPARTITION_TOPIC_NAME = "repartition";
    private static final String CHANGELOG_TOPIC_NAME1 = "changelog1";
    private static final Map<String, String> TOPIC_CONFIG = Collections.singletonMap("config1", "val1");
    private static final ConfiguredInternalTopic REPARTITION_TOPIC_CONFIG =
        new ConfiguredInternalTopic(REPARTITION_TOPIC_NAME, TOPIC_CONFIG);
    private static final ConfiguredSubtopology TOPICS_INFO_NOSOURCE = new ConfiguredSubtopology(
        Collections.singleton(SINK_TOPIC_NAME),
        Collections.emptySet(),
        mkMap(mkEntry(REPARTITION_TOPIC_NAME, REPARTITION_TOPIC_CONFIG)),
        mkMap()
    );
    private static final ConfiguredSubtopology TOPICS_INFO_STATELESS = new ConfiguredSubtopology(
        Collections.singleton(SINK_TOPIC_NAME),
        Collections.singleton(SOURCE_TOPIC_NAME),
        mkMap(mkEntry(REPARTITION_TOPIC_NAME, REPARTITION_TOPIC_CONFIG)),
        mkMap()
    );
    private static final ConfiguredInternalTopic CHANGELOG_TOPIC_CONFIG =
        new ConfiguredInternalTopic(CHANGELOG_TOPIC_NAME1, TOPIC_CONFIG);
    private static final ConfiguredSubtopology TOPICS_INFO_STATEFUL = new ConfiguredSubtopology(
        Collections.singleton(SINK_TOPIC_NAME),
        Collections.singleton(SOURCE_TOPIC_NAME),
        mkMap(mkEntry(REPARTITION_TOPIC_NAME, REPARTITION_TOPIC_CONFIG)),
        mkMap(mkEntry(CHANGELOG_TOPIC_NAME1, CHANGELOG_TOPIC_CONFIG))
    );
    private static final ConfiguredSubtopology TOPICS_INFO_SOURCE_CHANGELOG = new ConfiguredSubtopology(
        Collections.singleton(SINK_TOPIC_NAME),
        Collections.singleton(SOURCE_TOPIC_NAME),
        mkMap(mkEntry(REPARTITION_TOPIC_NAME, REPARTITION_TOPIC_CONFIG)),
        mkMap(mkEntry(SOURCE_TOPIC_NAME, CHANGELOG_TOPIC_CONFIG))
    );
    private static final ConfiguredSubtopology TOPICS_INFO_BOTH = new ConfiguredSubtopology(
        Collections.singleton(SINK_TOPIC_NAME),
        Collections.singleton(SOURCE_TOPIC_NAME),
        mkMap(mkEntry(REPARTITION_TOPIC_NAME, REPARTITION_TOPIC_CONFIG)),
        mkMap(mkEntry(SOURCE_TOPIC_NAME, null), mkEntry(CHANGELOG_TOPIC_NAME1, CHANGELOG_TOPIC_CONFIG))
    );

    private static Integer topicPartitionProvider(String s) {
        return 3;
    }

    @Test
    public void shouldFailIfNoSourceTopics() {
        final Map<String, ConfiguredSubtopology> subtopologies = mkMap(mkEntry("subtopology_0", TOPICS_INFO_NOSOURCE));

        final ChangelogTopics changelogTopics =
            new ChangelogTopics(LOG_CONTEXT, subtopologies, ChangelogTopicsTest::topicPartitionProvider);
        StreamsInvalidTopologyException e = assertThrows(StreamsInvalidTopologyException.class, () -> changelogTopics.setup());

        assertTrue(e.getMessage().contains("No source topics found for subtopology"));
    }

    @Test
    public void shouldNotContainChangelogsForStatelessTasks() {
        final Map<String, ConfiguredSubtopology> subtopologies = mkMap(mkEntry("subtopology_0", TOPICS_INFO_STATELESS));

        final ChangelogTopics changelogTopics =
            new ChangelogTopics(LOG_CONTEXT, subtopologies, ChangelogTopicsTest::topicPartitionProvider);
        Map<String, ConfiguredInternalTopic> setup = changelogTopics.setup();

        assertEquals(Collections.emptyMap(), setup);
    }

    @Test
    public void shouldContainNonSourceBasedChangelogs() {
        final Map<String, ConfiguredSubtopology> subtopologies = mkMap(mkEntry("subtopology_0", TOPICS_INFO_STATEFUL));

        final ChangelogTopics changelogTopics =
            new ChangelogTopics(LOG_CONTEXT, subtopologies, ChangelogTopicsTest::topicPartitionProvider);
        Map<String, ConfiguredInternalTopic> setup = changelogTopics.setup();

        assertEquals(mkMap(mkEntry(CHANGELOG_TOPIC_NAME1, CHANGELOG_TOPIC_CONFIG)), setup);
        assertEquals(3, CHANGELOG_TOPIC_CONFIG.numberOfPartitions().orElse(Integer.MIN_VALUE));
    }

    @Test
    public void shouldNotContainSourceBasedChangelogs() {
        final Map<String, ConfiguredSubtopology> subtopologies = mkMap(mkEntry("subtopology_0", TOPICS_INFO_SOURCE_CHANGELOG));

        final ChangelogTopics changelogTopics =
            new ChangelogTopics(LOG_CONTEXT, subtopologies, ChangelogTopicsTest::topicPartitionProvider);
        Map<String, ConfiguredInternalTopic> setup = changelogTopics.setup();

        assertEquals(Collections.emptyMap(), setup);
    }

    @Test
    public void shouldContainBothTypesOfPreExistingChangelogs() {
        final Map<String, ConfiguredSubtopology> subtopologies = mkMap(mkEntry("subtopology_0", TOPICS_INFO_BOTH));

        final ChangelogTopics changelogTopics =
            new ChangelogTopics(LOG_CONTEXT, subtopologies, ChangelogTopicsTest::topicPartitionProvider);
        Map<String, ConfiguredInternalTopic> setup = changelogTopics.setup();

        assertEquals(mkMap(mkEntry(CHANGELOG_TOPIC_NAME1, CHANGELOG_TOPIC_CONFIG)), setup);
        assertEquals(3, CHANGELOG_TOPIC_CONFIG.numberOfPartitions().orElse(Integer.MIN_VALUE));
    }
}
