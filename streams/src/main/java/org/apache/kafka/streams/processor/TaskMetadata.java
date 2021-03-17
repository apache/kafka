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
package org.apache.kafka.streams.processor;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Represents the state of a single task running within a {@link KafkaStreams} application.
 */
public class TaskMetadata {

    private final String taskId;

    private final Set<TopicPartition> topicPartitions;

    private final Map<TopicPartition, Long> committedOffsets;

    private final Map<TopicPartition, Long> endOffsets;

    private final Optional<Long> timeCurrentIdlingStarted;

    public TaskMetadata(final String taskId,
                        final Set<TopicPartition> topicPartitions,
                        final Map<TopicPartition, Long> committedOffsets,
                        final Map<TopicPartition, Long> endOffsets,
                        final Optional<Long> timeCurrentIdlingStarted) {
        this.taskId = taskId;
        this.topicPartitions = Collections.unmodifiableSet(topicPartitions);
        this.committedOffsets = Collections.unmodifiableMap(committedOffsets);
        this.endOffsets = Collections.unmodifiableMap(endOffsets);
        this.timeCurrentIdlingStarted = timeCurrentIdlingStarted;
    }

    public String taskId() {
        return taskId;
    }

    public Set<TopicPartition> topicPartitions() {
        return topicPartitions;
    }

    public Map<TopicPartition, Long> committedOffsets() {
        return committedOffsets;
    }

    public Map<TopicPartition, Long> endOffsets() {
        return endOffsets;
    }

    public Optional<Long> timeCurrentIdlingStarted() {
        return timeCurrentIdlingStarted;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TaskMetadata that = (TaskMetadata) o;
        return Objects.equals(taskId, that.taskId) &&
               Objects.equals(topicPartitions, that.topicPartitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, topicPartitions);
    }

    @Override
    public String toString() {
        return "TaskMetadata{" +
                "taskId=" + taskId +
                ", topicPartitions=" + topicPartitions +
                ", committedOffsets=" + committedOffsets +
                ", endOffsets=" + endOffsets +
                ", timeCurrentIdlingStarted=" + timeCurrentIdlingStarted +
                '}';
    }
}
