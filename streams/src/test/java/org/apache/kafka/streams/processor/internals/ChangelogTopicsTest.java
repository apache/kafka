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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.TopicsInfo;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.SUBTOPOLOGY_0;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class ChangelogTopicsTest {

    private static final String SOURCE_TOPIC_NAME = "source";
    private static final String SINK_TOPIC_NAME = "sink";
    private static final String REPARTITION_TOPIC_NAME = "repartition";
    private static final String CHANGELOG_TOPIC_NAME1 = "changelog1";
    private static final Map<String, String> TOPIC_CONFIG = Collections.singletonMap("config1", "val1");
    private static final RepartitionTopicConfig REPARTITION_TOPIC_CONFIG =
        new RepartitionTopicConfig(REPARTITION_TOPIC_NAME, TOPIC_CONFIG);
    private static final UnwindowedUnversionedChangelogTopicConfig CHANGELOG_TOPIC_CONFIG =
        new UnwindowedUnversionedChangelogTopicConfig(CHANGELOG_TOPIC_NAME1, TOPIC_CONFIG);

    private static final TopicsInfo TOPICS_INFO1 = new TopicsInfo(
        Set.of(SINK_TOPIC_NAME),
        Set.of(SOURCE_TOPIC_NAME),
        mkMap(mkEntry(REPARTITION_TOPIC_NAME, REPARTITION_TOPIC_CONFIG)),
        mkMap(mkEntry(CHANGELOG_TOPIC_NAME1, CHANGELOG_TOPIC_CONFIG))
    );
    private static final TopicsInfo TOPICS_INFO2 = new TopicsInfo(
        Set.of(SINK_TOPIC_NAME),
        Set.of(SOURCE_TOPIC_NAME),
        mkMap(mkEntry(REPARTITION_TOPIC_NAME, REPARTITION_TOPIC_CONFIG)),
        mkMap()
    );
    private static final TopicsInfo TOPICS_INFO3 = new TopicsInfo(
        Set.of(SINK_TOPIC_NAME),
        Set.of(SOURCE_TOPIC_NAME),
        mkMap(mkEntry(REPARTITION_TOPIC_NAME, REPARTITION_TOPIC_CONFIG)),
        mkMap(mkEntry(SOURCE_TOPIC_NAME, CHANGELOG_TOPIC_CONFIG))
    );
    private static final TopicsInfo TOPICS_INFO4 = new TopicsInfo(
        Set.of(SINK_TOPIC_NAME),
        Set.of(SOURCE_TOPIC_NAME),
        mkMap(mkEntry(REPARTITION_TOPIC_NAME, REPARTITION_TOPIC_CONFIG)),
        mkMap(mkEntry(SOURCE_TOPIC_NAME, null), mkEntry(CHANGELOG_TOPIC_NAME1, CHANGELOG_TOPIC_CONFIG))
    );
    private static final TaskId TASK_0_0 = new TaskId(0, 0);
    private static final TaskId TASK_0_1 = new TaskId(0, 1);
    private static final TaskId TASK_0_2 = new TaskId(0, 2);

    final InternalTopicManager internalTopicManager = mock(InternalTopicManager.class);

    @Test
    public void shouldNotContainChangelogsForStatelessTasks() {
        when(internalTopicManager.makeReady(Collections.emptyMap())).thenReturn(Collections.emptySet());
        final Map<Subtopology, TopicsInfo> topicGroups = mkMap(mkEntry(SUBTOPOLOGY_0, TOPICS_INFO2));
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(mkEntry(SUBTOPOLOGY_0, Set.of(TASK_0_0, TASK_0_1, TASK_0_2)));

        final ChangelogTopics changelogTopics =
                new ChangelogTopics(internalTopicManager, topicGroups, tasksForTopicGroup, "[test] ");
        changelogTopics.setup();

        assertTrue(changelogTopics.preExistingPartitionsFor(TASK_0_0).isEmpty());
        assertTrue(changelogTopics.preExistingPartitionsFor(TASK_0_1).isEmpty());
        assertTrue(changelogTopics.preExistingPartitionsFor(TASK_0_2).isEmpty());
        assertTrue(changelogTopics.preExistingSourceTopicBasedPartitions().isEmpty());
        assertTrue(changelogTopics.preExistingNonSourceTopicBasedPartitions().isEmpty());
    }

    @Test
    public void shouldNotContainAnyPreExistingChangelogsIfChangelogIsNewlyCreated() {
        when(internalTopicManager.makeReady(mkMap(mkEntry(CHANGELOG_TOPIC_NAME1, CHANGELOG_TOPIC_CONFIG))))
            .thenReturn(Set.of(CHANGELOG_TOPIC_NAME1));
        final Map<Subtopology, TopicsInfo> topicGroups = mkMap(mkEntry(SUBTOPOLOGY_0, TOPICS_INFO1));
        final Set<TaskId> tasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2);
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(mkEntry(SUBTOPOLOGY_0, tasks));

        final ChangelogTopics changelogTopics =
                new ChangelogTopics(internalTopicManager, topicGroups, tasksForTopicGroup, "[test] ");
        changelogTopics.setup();

        assertEquals(3, CHANGELOG_TOPIC_CONFIG.numberOfPartitions().orElse(Integer.MIN_VALUE));
        assertTrue(changelogTopics.preExistingPartitionsFor(TASK_0_0).isEmpty());
        assertTrue(changelogTopics.preExistingPartitionsFor(TASK_0_1).isEmpty());
        assertTrue(changelogTopics.preExistingPartitionsFor(TASK_0_2).isEmpty());
        assertTrue(changelogTopics.preExistingSourceTopicBasedPartitions().isEmpty());
        assertTrue(changelogTopics.preExistingNonSourceTopicBasedPartitions().isEmpty());
    }

    @Test
    public void shouldOnlyContainPreExistingNonSourceBasedChangelogs() {
        when(internalTopicManager.makeReady(mkMap(mkEntry(CHANGELOG_TOPIC_NAME1, CHANGELOG_TOPIC_CONFIG))))
            .thenReturn(Collections.emptySet());
        final Map<Subtopology, TopicsInfo> topicGroups = mkMap(mkEntry(SUBTOPOLOGY_0, TOPICS_INFO1));
        final Set<TaskId> tasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2);
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(mkEntry(SUBTOPOLOGY_0, tasks));

        final ChangelogTopics changelogTopics =
                new ChangelogTopics(internalTopicManager, topicGroups, tasksForTopicGroup, "[test] ");
        changelogTopics.setup();

        assertEquals(3, CHANGELOG_TOPIC_CONFIG.numberOfPartitions().orElse(Integer.MIN_VALUE));
        final TopicPartition changelogPartition0 = new TopicPartition(CHANGELOG_TOPIC_NAME1, 0);
        final TopicPartition changelogPartition1 = new TopicPartition(CHANGELOG_TOPIC_NAME1, 1);
        final TopicPartition changelogPartition2 = new TopicPartition(CHANGELOG_TOPIC_NAME1, 2);
        assertEquals(Set.of(changelogPartition0), changelogTopics.preExistingPartitionsFor(TASK_0_0));
        assertEquals(Set.of(changelogPartition1), changelogTopics.preExistingPartitionsFor(TASK_0_1));
        assertEquals(Set.of(changelogPartition2), changelogTopics.preExistingPartitionsFor(TASK_0_2));
        assertTrue(changelogTopics.preExistingSourceTopicBasedPartitions().isEmpty());
        assertEquals(Set.of(changelogPartition0, changelogPartition1, changelogPartition2), changelogTopics.preExistingNonSourceTopicBasedPartitions());
    }

    @Test
    public void shouldOnlyContainPreExistingSourceBasedChangelogs() {
        when(internalTopicManager.makeReady(Collections.emptyMap())).thenReturn(Collections.emptySet());
        final Map<Subtopology, TopicsInfo> topicGroups = mkMap(mkEntry(SUBTOPOLOGY_0, TOPICS_INFO3));
        final Set<TaskId> tasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2);
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(mkEntry(SUBTOPOLOGY_0, tasks));

        final ChangelogTopics changelogTopics =
                new ChangelogTopics(internalTopicManager, topicGroups, tasksForTopicGroup, "[test] ");
        changelogTopics.setup();

        final TopicPartition changelogPartition0 = new TopicPartition(SOURCE_TOPIC_NAME, 0);
        final TopicPartition changelogPartition1 = new TopicPartition(SOURCE_TOPIC_NAME, 1);
        final TopicPartition changelogPartition2 = new TopicPartition(SOURCE_TOPIC_NAME, 2);
        assertEquals(Set.of(changelogPartition0), changelogTopics.preExistingPartitionsFor(TASK_0_0));
        assertEquals(Set.of(changelogPartition1), changelogTopics.preExistingPartitionsFor(TASK_0_1));
        assertEquals(Set.of(changelogPartition2), changelogTopics.preExistingPartitionsFor(TASK_0_2));
        assertEquals(Set.of(changelogPartition0, changelogPartition1, changelogPartition2), changelogTopics.preExistingSourceTopicBasedPartitions());
        assertTrue(changelogTopics.preExistingNonSourceTopicBasedPartitions().isEmpty());
    }

    @Test
    public void shouldContainBothTypesOfPreExistingChangelogs() {
        when(internalTopicManager.makeReady(mkMap(mkEntry(CHANGELOG_TOPIC_NAME1, CHANGELOG_TOPIC_CONFIG))))
            .thenReturn(Collections.emptySet());
        final Map<Subtopology, TopicsInfo> topicGroups = mkMap(mkEntry(SUBTOPOLOGY_0, TOPICS_INFO4));
        final Set<TaskId> tasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2);
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(mkEntry(SUBTOPOLOGY_0, tasks));

        final ChangelogTopics changelogTopics =
                new ChangelogTopics(internalTopicManager, topicGroups, tasksForTopicGroup, "[test] ");
        changelogTopics.setup();

        assertEquals(3, CHANGELOG_TOPIC_CONFIG.numberOfPartitions().orElse(Integer.MIN_VALUE));
        final TopicPartition changelogPartition0 = new TopicPartition(CHANGELOG_TOPIC_NAME1, 0);
        final TopicPartition changelogPartition1 = new TopicPartition(CHANGELOG_TOPIC_NAME1, 1);
        final TopicPartition changelogPartition2 = new TopicPartition(CHANGELOG_TOPIC_NAME1, 2);
        final TopicPartition sourcePartition0 = new TopicPartition(SOURCE_TOPIC_NAME, 0);
        final TopicPartition sourcePartition1 = new TopicPartition(SOURCE_TOPIC_NAME, 1);
        final TopicPartition sourcePartition2 = new TopicPartition(SOURCE_TOPIC_NAME, 2);
        assertEquals(Set.of(sourcePartition0, changelogPartition0), changelogTopics.preExistingPartitionsFor(TASK_0_0));
        assertEquals(Set.of(sourcePartition1, changelogPartition1), changelogTopics.preExistingPartitionsFor(TASK_0_1));
        assertEquals(Set.of(sourcePartition2, changelogPartition2), changelogTopics.preExistingPartitionsFor(TASK_0_2));
        assertEquals(Set.of(sourcePartition0, sourcePartition1, sourcePartition2), changelogTopics.preExistingSourceTopicBasedPartitions());
        assertEquals(Set.of(changelogPartition0, changelogPartition1, changelogPartition2), changelogTopics.preExistingNonSourceTopicBasedPartitions());
    }
}
