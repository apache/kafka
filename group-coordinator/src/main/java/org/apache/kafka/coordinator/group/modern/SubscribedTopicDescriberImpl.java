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
package org.apache.kafka.coordinator.group.modern;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The subscribed topic metadata class is used by the {@link PartitionAssignor} to obtain
 * topic and partition metadata for the topics that the modern group is subscribed to.
 */
public class SubscribedTopicDescriberImpl implements SubscribedTopicDescriber {
    /**
     * The topic Ids mapped to their corresponding {@link TopicMetadata}
     * object, which contains topic and partition metadata.
     */
    private final Map<Uuid, TopicMetadata> topicMetadata;
    private final Map<Uuid, Set<Integer>> topicPartitionAllowedMap;

    public SubscribedTopicDescriberImpl(Map<Uuid, TopicMetadata> topicMetadata) {
        this(topicMetadata, null);
    }

    public SubscribedTopicDescriberImpl(Map<Uuid, TopicMetadata> topicMetadata, Map<Uuid, Set<Integer>> topicPartitionAllowedMap) {
        this.topicMetadata = Objects.requireNonNull(topicMetadata);
        this.topicPartitionAllowedMap = topicPartitionAllowedMap;
    }

    /**
     * Map of topic Ids to topic metadata.
     *
     * @return The map of topic Ids to topic metadata.
     */
    public Map<Uuid, TopicMetadata> topicMetadata() {
        return this.topicMetadata;
    }

    /**
     * The number of partitions for the given topic Id.
     *
     * @param topicId   Uuid corresponding to the topic.
     * @return The number of partitions corresponding to the given topic Id,
     *         or -1 if the topic Id does not exist.
     */
    @Override
    public int numPartitions(Uuid topicId) {
        TopicMetadata topic = this.topicMetadata.get(topicId);
        return topic == null ? -1 : topic.numPartitions();
    }

    /**
     * Returns all the available racks associated with the replicas of the given partition.
     *
     * @param topicId       Uuid corresponding to the partition's topic.
     * @param partition     Partition Id within the topic.
     * @return The set of racks corresponding to the replicas of the topics partition.
     *         If the topic Id does not exist or no partition rack information is available, an empty set is returned.
     */
    @Override
    public Set<String> racksForPartition(Uuid topicId, int partition) {
        return Set.of();
    }

    /**
     * Returns a set of assignable partitions from the topic metadata.
     * If the allowed partition map is null, all the partitions in the corresponding
     * topic metadata are returned for the argument topic id. If allowed map is empty,
     * empty set is returned.
     *
     * @param topicId The uuid of the topic
     * @return Set of integers if assignable partitions available, empty otherwise.
     */
    @Override
    public Set<Integer> assignablePartitions(Uuid topicId) {
        TopicMetadata topic = this.topicMetadata.get(topicId);
        if (topic == null) {
            return Set.of();
        }

        if (topicPartitionAllowedMap == null) {
            return IntStream.range(0, topic.numPartitions()).boxed().collect(Collectors.toUnmodifiableSet());
        }

        return topicPartitionAllowedMap.getOrDefault(topicId, Set.of());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubscribedTopicDescriberImpl that = (SubscribedTopicDescriberImpl) o;
        return topicMetadata.equals(that.topicMetadata);
    }

    @Override
    public int hashCode() {
        return topicMetadata.hashCode();
    }

    @Override
    public String toString() {
        return "SubscribedTopicMetadata(" +
            "topicMetadata=" + topicMetadata +
            ')';
    }
}
