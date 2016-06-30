/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state;

import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents all the {@link org.apache.kafka.streams.processor.StateStore} instances
 * and {@link TopicPartition}s that can be found on particular instance/process of
 * a {@link org.apache.kafka.streams.KafkaStreams} application
 */
public class TaskMetadata {
    private final Set<String> stateStoreNames = new HashSet<>();
    private final Set<TopicPartition> topicPartitions = new HashSet<>();

    public TaskMetadata(final Set<String> stateStoreNames,
                        final Set<TopicPartition> topicPartitions) {
        this.stateStoreNames.addAll(stateStoreNames);
        this.topicPartitions.addAll(topicPartitions);
    }

    public Set<String> getStateStoreNames() {
        return Collections.unmodifiableSet(stateStoreNames);
    }

    public Set<TopicPartition> getTopicPartitions() {
        return Collections.unmodifiableSet(topicPartitions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TaskMetadata that = (TaskMetadata) o;

        if (!stateStoreNames.equals(that.stateStoreNames)) return false;
        return topicPartitions.equals(that.topicPartitions);

    }

    @Override
    public int hashCode() {
        int result = stateStoreNames.hashCode();
        result = 31 * result + topicPartitions.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TaskMetadata{" +
                "stateStoreNames=" + stateStoreNames +
                ", topicPartitions=" + topicPartitions +
                '}';
    }
}
