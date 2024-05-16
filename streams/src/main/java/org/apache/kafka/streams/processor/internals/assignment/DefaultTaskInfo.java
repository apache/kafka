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
package org.apache.kafka.streams.processor.internals.assignment;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.TaskInfo;

public class DefaultTaskInfo implements TaskInfo {

    private final TaskId id;
    private final boolean isStateful;
    private final Map<TopicPartition, Set<String>> partitionToRackIds;
    private final Set<String> stateStoreNames;
    private final Set<TopicPartition> inputTopicPartitions;
    private final Set<TopicPartition> changelogTopicPartitions;

    public DefaultTaskInfo(final TaskId id,
                           final boolean isStateful,
                           final Map<TopicPartition, Set<String>> partitionToRackIds,
                           final Set<String> stateStoreNames,
                           final Set<TopicPartition> inputTopicPartitions,
                           final Set<TopicPartition> changelogTopicPartitions) {
        this.id = id;
        this.partitionToRackIds = unmodifiableMap(partitionToRackIds);
        this.isStateful = isStateful;
        this.stateStoreNames = unmodifiableSet(stateStoreNames);
        this.inputTopicPartitions = unmodifiableSet(inputTopicPartitions);
        this.changelogTopicPartitions = unmodifiableSet(changelogTopicPartitions);
    }

    public static DefaultTaskInfo of(final TaskId taskId,
                                     final boolean isStateful,
                                     final ValueMapper<TopicPartition, String> previousOwnerForPartition,
                                     final Map<String, Optional<String>> rackForConsumer,
                                     final Map<TaskId, Set<TopicPartition>> inputPartitionsForTask,
                                     final Map<TaskId, Set<TopicPartition>> changelogPartitionsForTask) {

        final Set<TopicPartition> inputPartitions = inputPartitionsForTask.get(taskId);
        final Set<TopicPartition> changelogPartitions = changelogPartitionsForTask.get(taskId);
        final Map<TopicPartition, Set<String>> racksForPartition = new HashMap<>();

        inputPartitions.forEach(partition -> {
            racksForPartition.computeIfAbsent(partition, k -> new HashSet<>());
            final String consumer = previousOwnerForPartition.apply(partition);
            final Optional<String> rack = rackForConsumer.get(consumer);
            rack.ifPresent(s -> racksForPartition.get(partition).add(s));
        });

        changelogPartitions.forEach(partition -> {
            racksForPartition.computeIfAbsent(partition, k -> new HashSet<>());
            final String consumer = previousOwnerForPartition.apply(partition);
            final Optional<String> rack = rackForConsumer.get(consumer);
            rack.ifPresent(s -> racksForPartition.get(partition).add(s));
        });

        final Set<String> stateStoreNames = new HashSet<>();
        return new DefaultTaskInfo(
            taskId,
            isStateful, // All standby tasks are stateful.
            racksForPartition,
            stateStoreNames,
            inputPartitions,
            changelogPartitions
        );
    }

    @Override
    public TaskId id() {
        return id;
    }

    @Override
    public boolean isStateful() {
        return isStateful;
    }

    @Override
    public Set<String> stateStoreNames() {
        return stateStoreNames;
    }

    @Override
    public Set<TopicPartition> inputTopicPartitions() {
        return inputTopicPartitions;
    }

    @Override
    public Set<TopicPartition> changelogTopicPartitions() {
        return changelogTopicPartitions;
    }

    @Override
    public Map<TopicPartition, Set<String>> partitionToRackIds() {
        return partitionToRackIds;
    }
}
