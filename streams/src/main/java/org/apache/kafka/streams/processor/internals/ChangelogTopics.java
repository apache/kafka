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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.MisconfiguredInternalTopicException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalTopicManager.ValidationResult;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.TopicsInfo;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.UNKNOWN;

public class ChangelogTopics {

    private final InternalTopicManager internalTopicManager;
    private final ValidationResult validationResult;
    private final Map<String, InternalTopicConfig> changelogTopicConfigs;
    private final Map<TaskId, Set<TopicPartition>> changelogPartitionsForStatefulTask = new HashMap<>();
    private final Map<TaskId, Set<TopicPartition>> preExistingChangelogPartitionsForTask = new HashMap<>();
    private final Set<TopicPartition> preExistingNonSourceTopicBasedChangelogPartitions = new HashSet<>();
    private final Set<String> sourceTopicBasedChangelogTopics = new HashSet<>();
    private final Set<TopicPartition> preExistingSourceTopicBasedChangelogPartitions = new HashSet<>();
    private final Logger log;

    public ChangelogTopics(final InternalTopicManager internalTopicManager,
                           final Map<Integer, TopicsInfo> topicGroups,
                           final Map<Integer, Set<TaskId>> tasksForTopicGroup,
                           final String logPrefix) {
        this.internalTopicManager = internalTopicManager;
        final LogContext logContext = new LogContext(logPrefix);
        log = logContext.logger(getClass());
        changelogTopicConfigs = computeChangelogTopicConfig(topicGroups, tasksForTopicGroup);
        validationResult = internalTopicManager.validate(changelogTopicConfigs);
    }

    public void setup() {
        if (!validationResult.misconfigurationsForTopics().isEmpty()) {
            throw new MisconfiguredInternalTopicException(Utils.join(misconfigured().values().stream()
                .flatMap(Collection::stream).collect(Collectors.toList()), Utils.NL)
            );
        }

        final Set<String> missingTopics = validationResult.missingTopics();
        final Map<String, InternalTopicConfig> changelogsToCreate = changelogTopicConfigs.entrySet().stream()
            .filter(entry -> missingTopics.contains(entry.getKey()))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        internalTopicManager.setup(changelogsToCreate);
        log.info("Created state changelog topics {} from the parsed topology.", changelogsToCreate.values());

        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : changelogPartitionsForStatefulTask.entrySet()) {
            final TaskId taskId = entry.getKey();
            final Set<TopicPartition> topicPartitions = entry.getValue();
            for (final TopicPartition topicPartition : topicPartitions) {
                if (!missingTopics.contains(topicPartition.topic())) {
                    preExistingChangelogPartitionsForTask.computeIfAbsent(taskId, task -> new HashSet<>()).add(topicPartition);
                    if (!sourceTopicBasedChangelogTopics.contains(topicPartition.topic())) {
                        preExistingNonSourceTopicBasedChangelogPartitions.add(topicPartition);
                    } else {
                        preExistingSourceTopicBasedChangelogPartitions.add(topicPartition);
                    }
                }
            }
        }
    }

    public Set<String> missing() {
        return validationResult.missingTopics();
    }

    public Map<String, List<String>> misconfigured() {
        return validationResult.misconfigurationsForTopics();
    }

    public Set<TopicPartition> preExistingNonSourceTopicBasedPartitions() {
        return Collections.unmodifiableSet(preExistingNonSourceTopicBasedChangelogPartitions);
    }

    public Set<TopicPartition> preExistingPartitionsFor(final TaskId taskId) {
        if (preExistingChangelogPartitionsForTask.containsKey(taskId)) {
            return Collections.unmodifiableSet(preExistingChangelogPartitionsForTask.get(taskId));
        }
        return Collections.emptySet();
    }

    public Set<TopicPartition> preExistingSourceTopicBasedPartitions() {
        return Collections.unmodifiableSet(preExistingSourceTopicBasedChangelogPartitions);
    }

    public Set<TaskId> statefulTaskIds() {
        return Collections.unmodifiableSet(changelogPartitionsForStatefulTask.keySet());
    }

    private Map<String, InternalTopicConfig> computeChangelogTopicConfig(final Map<Integer, TopicsInfo> topicGroups,
                                                                         final Map<Integer, Set<TaskId>> tasksForTopicGroup) {
        final Map<String, InternalTopicConfig> changelogTopicMetadata = new HashMap<>();
        for (final Map.Entry<Integer, TopicsInfo> entry : topicGroups.entrySet()) {
            final int topicGroupId = entry.getKey();
            final TopicsInfo topicsInfo = entry.getValue();

            final Set<TaskId> topicGroupTasks = tasksForTopicGroup.get(topicGroupId);
            if (topicGroupTasks == null) {
                log.debug("No tasks found for topic group {}", topicGroupId);
                continue;
            } else if (topicsInfo.stateChangelogTopics.isEmpty()) {
                continue;
            }
            for (final TaskId task : topicGroupTasks) {
                final Set<TopicPartition> changelogTopicPartitions = topicsInfo.stateChangelogTopics
                    .keySet()
                    .stream()
                    .map(topic -> new TopicPartition(topic, task.partition))
                    .collect(Collectors.toSet());
                changelogPartitionsForStatefulTask.put(task, changelogTopicPartitions);
            }
            for (final InternalTopicConfig topicConfig : topicsInfo.nonSourceChangelogTopics()) {
                // the expected number of partitions is the max value of TaskId.partition + 1
                int numPartitions = UNKNOWN;
                for (final TaskId task : topicGroupTasks) {
                    if (numPartitions < task.partition + 1) {
                        numPartitions = task.partition + 1;
                    }
                }
                topicConfig.setNumberOfPartitions(numPartitions);
                changelogTopicMetadata.put(topicConfig.name(), topicConfig);
            }
            sourceTopicBasedChangelogTopics.addAll(topicsInfo.sourceTopicChangelogs());
        }
        return changelogTopicMetadata;
    }
}