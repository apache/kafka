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
import org.apache.kafka.streams.errors.MisconfiguredInternalTopicException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalTopicManager.ValidationResult;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.TopicsInfo;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

public class ChangelogTopicsTest {

    private static final String SOURCE_TOPIC_NAME = "source";
    private static final String SINK_TOPIC_NAME = "sink";
    private static final String REPARTITION_TOPIC_NAME = "repartition";
    private static final String CHANGELOG_TOPIC_NAME1 = "changelog1";
    private static final String CHANGELOG_TOPIC_NAME2 = "changelog2";
    private static final Map<String, String> TOPIC_CONFIG1 = Collections.singletonMap("config1", "val1");
    private static final Map<String, String> TOPIC_CONFIG2 = Collections.singletonMap("config2", "val2");
    private static final RepartitionTopicConfig REPARTITION_TOPIC_CONFIG =
        new RepartitionTopicConfig(REPARTITION_TOPIC_NAME, TOPIC_CONFIG1);
    private static final UnwindowedChangelogTopicConfig CHANGELOG_TOPIC_CONFIG1 =
        new UnwindowedChangelogTopicConfig(CHANGELOG_TOPIC_NAME1, TOPIC_CONFIG1);
    private static final UnwindowedChangelogTopicConfig CHANGELOG_TOPIC_CONFIG2 =
        new UnwindowedChangelogTopicConfig(CHANGELOG_TOPIC_NAME2, TOPIC_CONFIG2);

    private static final TopicsInfo TOPICS_INFO1 = new TopicsInfo(
        mkSet(SINK_TOPIC_NAME),
        mkSet(SOURCE_TOPIC_NAME),
        mkMap(mkEntry(REPARTITION_TOPIC_NAME, REPARTITION_TOPIC_CONFIG)),
        mkMap(mkEntry(CHANGELOG_TOPIC_NAME1, CHANGELOG_TOPIC_CONFIG1))
    );
    private static final TopicsInfo TOPICS_INFO2 = new TopicsInfo(
        mkSet(SINK_TOPIC_NAME),
        mkSet(SOURCE_TOPIC_NAME),
        mkMap(mkEntry(REPARTITION_TOPIC_NAME, REPARTITION_TOPIC_CONFIG)),
        mkMap()
    );
    private static final TopicsInfo TOPICS_INFO3 = new TopicsInfo(
        mkSet(SINK_TOPIC_NAME),
        mkSet(SOURCE_TOPIC_NAME),
        mkMap(mkEntry(REPARTITION_TOPIC_NAME, REPARTITION_TOPIC_CONFIG)),
        mkMap(mkEntry(SOURCE_TOPIC_NAME, CHANGELOG_TOPIC_CONFIG1))
    );
    private static final TopicsInfo TOPICS_INFO4 = new TopicsInfo(
        mkSet(SINK_TOPIC_NAME),
        mkSet(SOURCE_TOPIC_NAME),
        mkMap(mkEntry(REPARTITION_TOPIC_NAME, REPARTITION_TOPIC_CONFIG)),
        mkMap(mkEntry(SOURCE_TOPIC_NAME, null), mkEntry(CHANGELOG_TOPIC_NAME1, CHANGELOG_TOPIC_CONFIG1))
    );
    private static final TopicsInfo TOPICS_INFO5 = new TopicsInfo(
        mkSet(SINK_TOPIC_NAME),
        mkSet(SOURCE_TOPIC_NAME),
        mkMap(mkEntry(REPARTITION_TOPIC_NAME, REPARTITION_TOPIC_CONFIG)),
        mkMap(
            mkEntry(CHANGELOG_TOPIC_NAME1, CHANGELOG_TOPIC_CONFIG1),
            mkEntry(CHANGELOG_TOPIC_NAME2, CHANGELOG_TOPIC_CONFIG2)
        )
    );
    private static final TaskId TASK_0_0 = new TaskId(0, 0);
    private static final TaskId TASK_0_1 = new TaskId(0, 1);
    private static final TaskId TASK_0_2 = new TaskId(0, 2);

    final InternalTopicManager internalTopicManager = mock(InternalTopicManager.class);

    @Test
    public void shouldNotContainChangelogsForStatelessTasks() {
        final ValidationResult validationResult = new ValidationResult();
        final Map<String, InternalTopicConfig> changelogsToValidateAndSetup = Collections.emptyMap();
        expect(internalTopicManager.validate(changelogsToValidateAndSetup)).andReturn(validationResult);
        internalTopicManager.setup(changelogsToValidateAndSetup);
        final Map<Integer, TopicsInfo> topicGroups = mkMap(mkEntry(0, TOPICS_INFO2));
        final Map<Integer, Set<TaskId>> tasksForTopicGroup = mkMap(mkEntry(0, mkSet(TASK_0_0, TASK_0_1, TASK_0_2)));
        replay(internalTopicManager);

        final ChangelogTopics changelogTopics =
                new ChangelogTopics(internalTopicManager, topicGroups, tasksForTopicGroup, "[test] ");
        changelogTopics.setup();

        verify(internalTopicManager);
        assertThat(changelogTopics.preExistingPartitionsFor(TASK_0_0), is(Collections.emptySet()));
        assertThat(changelogTopics.preExistingPartitionsFor(TASK_0_1), is(Collections.emptySet()));
        assertThat(changelogTopics.preExistingPartitionsFor(TASK_0_2), is(Collections.emptySet()));
        assertThat(changelogTopics.preExistingSourceTopicBasedPartitions(), is(Collections.emptySet()));
        assertThat(changelogTopics.preExistingNonSourceTopicBasedPartitions(), is(Collections.emptySet()));
    }

    @Test
    public void shouldNotContainAnyPreExistingChangelogsIfChangelogIsNewlyCreated() {
        final ValidationResult validationResult = new ValidationResult();
        validationResult.addMissingTopic(CHANGELOG_TOPIC_NAME1);
        final Map<String, InternalTopicConfig> changelogsToValidateAndSetup = mkMap(
            mkEntry(CHANGELOG_TOPIC_NAME1, CHANGELOG_TOPIC_CONFIG1)
        );
        expect(internalTopicManager.validate(changelogsToValidateAndSetup)).andReturn(validationResult);
        internalTopicManager.setup(changelogsToValidateAndSetup);
        final Map<Integer, TopicsInfo> topicGroups = mkMap(mkEntry(0, TOPICS_INFO1));
        final Set<TaskId> tasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2);
        final Map<Integer, Set<TaskId>> tasksForTopicGroup = mkMap(mkEntry(0, tasks));
        replay(internalTopicManager);

        final ChangelogTopics changelogTopics =
                new ChangelogTopics(internalTopicManager, topicGroups, tasksForTopicGroup, "[test] ");
        changelogTopics.setup();

        verify(internalTopicManager);
        assertThat(CHANGELOG_TOPIC_CONFIG1.numberOfPartitions().orElse(Integer.MIN_VALUE), is(3));
        assertThat(changelogTopics.preExistingPartitionsFor(TASK_0_0), is(Collections.emptySet()));
        assertThat(changelogTopics.preExistingPartitionsFor(TASK_0_1), is(Collections.emptySet()));
        assertThat(changelogTopics.preExistingPartitionsFor(TASK_0_2), is(Collections.emptySet()));
        assertThat(changelogTopics.preExistingSourceTopicBasedPartitions(), is(Collections.emptySet()));
        assertThat(changelogTopics.preExistingNonSourceTopicBasedPartitions(), is(Collections.emptySet()));
    }

    @Test
    public void shouldSetupOnlyMissingChangelogs() {
        final ValidationResult validationResult = new ValidationResult();
        validationResult.addMissingTopic(CHANGELOG_TOPIC_NAME1);
        final Map<String, InternalTopicConfig> changelogsToValidate = mkMap(
            mkEntry(CHANGELOG_TOPIC_NAME1, CHANGELOG_TOPIC_CONFIG1),
            mkEntry(CHANGELOG_TOPIC_NAME2, CHANGELOG_TOPIC_CONFIG2)
        );
        final Map<String, InternalTopicConfig> changelogsToSetup = mkMap(
            mkEntry(CHANGELOG_TOPIC_NAME1, CHANGELOG_TOPIC_CONFIG1)
        );
        expect(internalTopicManager.validate(changelogsToValidate)).andReturn(validationResult);
        internalTopicManager.setup(changelogsToSetup);
        final Map<Integer, TopicsInfo> topicGroups = mkMap(mkEntry(0, TOPICS_INFO5));
        final Set<TaskId> tasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2);
        final Map<Integer, Set<TaskId>> tasksForTopicGroup = mkMap(mkEntry(0, tasks));
        replay(internalTopicManager);

        final ChangelogTopics changelogTopics =
                new ChangelogTopics(internalTopicManager, topicGroups, tasksForTopicGroup, "[test] ");
        changelogTopics.setup();

        verify(internalTopicManager);
        assertThat(CHANGELOG_TOPIC_CONFIG1.numberOfPartitions().orElse(Integer.MIN_VALUE), is(3));
        assertThat(CHANGELOG_TOPIC_CONFIG2.numberOfPartitions().orElse(Integer.MIN_VALUE), is(3));
        final TopicPartition changelogPartition10 = new TopicPartition(CHANGELOG_TOPIC_NAME2, 0);
        final TopicPartition changelogPartition11 = new TopicPartition(CHANGELOG_TOPIC_NAME2, 1);
        final TopicPartition changelogPartition12 = new TopicPartition(CHANGELOG_TOPIC_NAME2, 2);
        assertThat(changelogTopics.preExistingPartitionsFor(TASK_0_0), is(mkSet(changelogPartition10)));
        assertThat(changelogTopics.preExistingPartitionsFor(TASK_0_1), is(mkSet(changelogPartition11)));
        assertThat(changelogTopics.preExistingPartitionsFor(TASK_0_2), is(mkSet(changelogPartition12)));
        assertThat(changelogTopics.preExistingSourceTopicBasedPartitions(), is(Collections.emptySet()));
        assertThat(
            changelogTopics.preExistingNonSourceTopicBasedPartitions(),
            is(mkSet(changelogPartition10, changelogPartition11, changelogPartition12))
        );
    }

    @Test
    public void shouldOnlyContainPreExistingNonSourceBasedChangelogs() {
        final ValidationResult validationResult = new ValidationResult();
        final Map<String, InternalTopicConfig> changelogsToValidate = mkMap(
            mkEntry(CHANGELOG_TOPIC_NAME1, CHANGELOG_TOPIC_CONFIG1)
        );
        expect(internalTopicManager.validate(changelogsToValidate)).andReturn(validationResult);
        internalTopicManager.setup(Collections.emptyMap());
        final Map<Integer, TopicsInfo> topicGroups = mkMap(mkEntry(0, TOPICS_INFO1));
        final Set<TaskId> tasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2);
        final Map<Integer, Set<TaskId>> tasksForTopicGroup = mkMap(mkEntry(0, tasks));
        replay(internalTopicManager);

        final ChangelogTopics changelogTopics =
                new ChangelogTopics(internalTopicManager, topicGroups, tasksForTopicGroup, "[test] ");
        changelogTopics.setup();

        verify(internalTopicManager);
        assertThat(CHANGELOG_TOPIC_CONFIG1.numberOfPartitions().orElse(Integer.MIN_VALUE), is(3));
        final TopicPartition changelogPartition0 = new TopicPartition(CHANGELOG_TOPIC_NAME1, 0);
        final TopicPartition changelogPartition1 = new TopicPartition(CHANGELOG_TOPIC_NAME1, 1);
        final TopicPartition changelogPartition2 = new TopicPartition(CHANGELOG_TOPIC_NAME1, 2);
        assertThat(changelogTopics.preExistingPartitionsFor(TASK_0_0), is(mkSet(changelogPartition0)));
        assertThat(changelogTopics.preExistingPartitionsFor(TASK_0_1), is(mkSet(changelogPartition1)));
        assertThat(changelogTopics.preExistingPartitionsFor(TASK_0_2), is(mkSet(changelogPartition2)));
        assertThat(changelogTopics.preExistingSourceTopicBasedPartitions(), is(Collections.emptySet()));
        assertThat(
            changelogTopics.preExistingNonSourceTopicBasedPartitions(),
            is(mkSet(changelogPartition0, changelogPartition1, changelogPartition2))
        );
    }

    @Test
    public void shouldOnlyContainPreExistingSourceBasedChangelogs() {
        final ValidationResult validationResult = new ValidationResult();
        final Map<String, InternalTopicConfig> changelogsToValidateAndSetup = Collections.emptyMap();
        expect(internalTopicManager.validate(changelogsToValidateAndSetup)).andReturn(validationResult);
        internalTopicManager.setup(changelogsToValidateAndSetup);
        final Map<Integer, TopicsInfo> topicGroups = mkMap(mkEntry(0, TOPICS_INFO3));
        final Set<TaskId> tasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2);
        final Map<Integer, Set<TaskId>> tasksForTopicGroup = mkMap(mkEntry(0, tasks));
        replay(internalTopicManager);

        final ChangelogTopics changelogTopics =
                new ChangelogTopics(internalTopicManager, topicGroups, tasksForTopicGroup, "[test] ");
        changelogTopics.setup();

        verify(internalTopicManager);
        final TopicPartition changelogPartition0 = new TopicPartition(SOURCE_TOPIC_NAME, 0);
        final TopicPartition changelogPartition1 = new TopicPartition(SOURCE_TOPIC_NAME, 1);
        final TopicPartition changelogPartition2 = new TopicPartition(SOURCE_TOPIC_NAME, 2);
        assertThat(changelogTopics.preExistingPartitionsFor(TASK_0_0), is(mkSet(changelogPartition0)));
        assertThat(changelogTopics.preExistingPartitionsFor(TASK_0_1), is(mkSet(changelogPartition1)));
        assertThat(changelogTopics.preExistingPartitionsFor(TASK_0_2), is(mkSet(changelogPartition2)));
        assertThat(
            changelogTopics.preExistingSourceTopicBasedPartitions(),
            is(mkSet(changelogPartition0, changelogPartition1, changelogPartition2))
        );
        assertThat(changelogTopics.preExistingNonSourceTopicBasedPartitions(), is(Collections.emptySet()));
    }

    @Test
    public void shouldContainBothTypesOfPreExistingChangelogs() {
        final ValidationResult validationResult = new ValidationResult();
        final Map<String, InternalTopicConfig> changelogsToValidate = mkMap(
            mkEntry(CHANGELOG_TOPIC_NAME1, CHANGELOG_TOPIC_CONFIG1)
        );
        expect(internalTopicManager.validate(changelogsToValidate)).andReturn(validationResult);
        internalTopicManager.setup(Collections.emptyMap());
        final Map<Integer, TopicsInfo> topicGroups = mkMap(mkEntry(0, TOPICS_INFO4));
        final Set<TaskId> tasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2);
        final Map<Integer, Set<TaskId>> tasksForTopicGroup = mkMap(mkEntry(0, tasks));
        replay(internalTopicManager);

        final ChangelogTopics changelogTopics =
                new ChangelogTopics(internalTopicManager, topicGroups, tasksForTopicGroup, "[test] ");
        changelogTopics.setup();

        verify(internalTopicManager);
        assertThat(CHANGELOG_TOPIC_CONFIG1.numberOfPartitions().orElse(Integer.MIN_VALUE), is(3));
        final TopicPartition changelogPartition0 = new TopicPartition(CHANGELOG_TOPIC_NAME1, 0);
        final TopicPartition changelogPartition1 = new TopicPartition(CHANGELOG_TOPIC_NAME1, 1);
        final TopicPartition changelogPartition2 = new TopicPartition(CHANGELOG_TOPIC_NAME1, 2);
        final TopicPartition sourcePartition0 = new TopicPartition(SOURCE_TOPIC_NAME, 0);
        final TopicPartition sourcePartition1 = new TopicPartition(SOURCE_TOPIC_NAME, 1);
        final TopicPartition sourcePartition2 = new TopicPartition(SOURCE_TOPIC_NAME, 2);
        assertThat(changelogTopics.preExistingPartitionsFor(TASK_0_0), is(mkSet(sourcePartition0, changelogPartition0)));
        assertThat(changelogTopics.preExistingPartitionsFor(TASK_0_1), is(mkSet(sourcePartition1, changelogPartition1)));
        assertThat(changelogTopics.preExistingPartitionsFor(TASK_0_2), is(mkSet(sourcePartition2, changelogPartition2)));
        assertThat(
            changelogTopics.preExistingSourceTopicBasedPartitions(),
            is(mkSet(sourcePartition0, sourcePartition1, sourcePartition2))
        );
        assertThat(
            changelogTopics.preExistingNonSourceTopicBasedPartitions(),
            is(mkSet(changelogPartition0, changelogPartition1, changelogPartition2))
        );
    }

    @Test
    public void shouldThrowMisconfiguredInternalTopicExceptionIfMisconfigurationsExistsDuringSetup() {
        final ValidationResult validationResult = new ValidationResult();
        final String misconfiguration1 = "misconfiguration1";
        final String misconfiguration2 = "misconfiguration2";
        validationResult.addMisconfiguration(CHANGELOG_TOPIC_NAME1, misconfiguration1);
        validationResult.addMisconfiguration(CHANGELOG_TOPIC_NAME2, misconfiguration2);
        expect(internalTopicManager.validate(
            mkMap(
                mkEntry(CHANGELOG_TOPIC_NAME1, CHANGELOG_TOPIC_CONFIG1),
                mkEntry(CHANGELOG_TOPIC_NAME2, CHANGELOG_TOPIC_CONFIG2)
            ))
        ).andReturn(validationResult);
        final Map<Integer, TopicsInfo> topicGroups = mkMap(mkEntry(0, TOPICS_INFO5));
        final Set<TaskId> tasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2);
        final Map<Integer, Set<TaskId>> tasksForTopicGroup = mkMap(mkEntry(0, tasks));
        replay(internalTopicManager);

        final ChangelogTopics changelogTopics =
            new ChangelogTopics(internalTopicManager, topicGroups, tasksForTopicGroup, "[test] ");
        final MisconfiguredInternalTopicException misconfiguredInternalTopicException = assertThrows(
            MisconfiguredInternalTopicException.class,
            changelogTopics::setup
        );

        final String message = misconfiguredInternalTopicException.getMessage();
        assertThat(message, containsString(misconfiguration1));
        assertThat(message, containsString(misconfiguration2));
    }
}
