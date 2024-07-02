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

import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.TaskInfo;
import org.apache.kafka.streams.processor.assignment.TaskTopicPartition;

import java.util.Set;

import static java.util.Collections.unmodifiableSet;

public class DefaultTaskInfo implements TaskInfo {

    private final TaskId id;
    private final boolean isStateful;
    private final Set<String> stateStoreNames;
    private final Set<TaskTopicPartition> topicPartitions;

    public DefaultTaskInfo(final TaskId id,
                           final boolean isStateful,
                           final Set<String> stateStoreNames,
                           final Set<TaskTopicPartition> topicPartitions) {
        this.id = id;
        this.isStateful = isStateful;
        this.stateStoreNames = unmodifiableSet(stateStoreNames);
        this.topicPartitions = unmodifiableSet(topicPartitions);
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
    public Set<TaskTopicPartition> topicPartitions() {
        return topicPartitions;
    }
}
