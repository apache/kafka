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

import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.TaskInfo;

public class DefaultTaskInfo implements TaskInfo {

    private final TaskId id;
    private final boolean isStateful;
    private final Map<TopicPartition, Set<String>> partitionToRackIds;
    private final Set<String> stateStoreNames;
    private final Set<TopicPartition> sourceTopicPartitions;
    private final Set<TopicPartition> changelogTopicPartitions;

    public DefaultTaskInfo(final TaskId id,
                           final boolean isStateful,
                           final Map<TopicPartition, Set<String>> partitionToRackIds,
                           final Set<String> stateStoreNames,
                           final Set<TopicPartition> sourceTopicPartitions,
                           final Set<TopicPartition> changelogTopicPartitions) {
        this.id = id;
        this.partitionToRackIds = unmodifiableMap(partitionToRackIds);
        this.isStateful = isStateful;
        this.stateStoreNames = unmodifiableSet(stateStoreNames);
        this.sourceTopicPartitions = unmodifiableSet(sourceTopicPartitions);
        this.changelogTopicPartitions = unmodifiableSet(changelogTopicPartitions);
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
    public Set<TopicPartition> sourceTopicPartitions() {
        return sourceTopicPartitions;
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
