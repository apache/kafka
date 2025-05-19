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
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.PartitionRegistration;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The subscribed topic metadata class is used by the {@link PartitionAssignor} to obtain
 * topic and partition metadata for the topics that the modern group is subscribed to.
 */
public class SubscribedTopicDescriberImpl implements SubscribedTopicDescriber {
    /**
     * The map of topic Ids to the set of allowed partitions for each topic.
     * If this is empty, all partitions are allowed.
     */
    private final Optional<Map<Uuid, Set<Integer>>> topicPartitionAllowedMap;

    /**
     * The metadata image that contains the latest metadata information.
     */
    private final MetadataImage metadataImage;

    public SubscribedTopicDescriberImpl(MetadataImage metadataImage) {
        this(metadataImage, Optional.empty());
    }

    public SubscribedTopicDescriberImpl(
        MetadataImage metadataImage,
        Optional<Map<Uuid, Set<Integer>>> topicPartitionAllowedMap
    ) {
        this.metadataImage = Objects.requireNonNull(metadataImage);
        this.topicPartitionAllowedMap = Objects.requireNonNull(topicPartitionAllowedMap);
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
        TopicImage topicImage = this.metadataImage.topics().getTopic(topicId);
        return topicImage == null ? -1 : topicImage.partitions().size();
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
        TopicImage topic = metadataImage.topics().getTopic(topicId);
        if (topic != null) {
            PartitionRegistration partitionRegistration = topic.partitions().get(partition);
            if (partitionRegistration != null) {
                Set<String> racks = new HashSet<>();
                for (int replica : partitionRegistration.replicas) {
                    // Only add the rack if it is available for the broker/replica.
                    BrokerRegistration brokerRegistration = metadataImage.cluster().broker(replica);
                    if (brokerRegistration != null) {
                        brokerRegistration.rack().ifPresent(racks::add);
                    }
                }
                return Collections.unmodifiableSet(racks);
            }
        }
        return Set.of();
    }

    /**
     * Returns a set of assignable partitions from the metadata image.
     * If the allowed partition map is Optional.empty(), all the partitions in the corresponding
     * topic image are returned for the argument topic id. If allowed map is empty,
     * empty set is returned.
     *
     * @param topicId The uuid of the topic
     * @return Set of integers if assignable partitions available, empty otherwise.
     */
    @Override
    public Set<Integer> assignablePartitions(Uuid topicId) {
        TopicImage topic = metadataImage.topics().getTopic(topicId);
        if (topic == null) {
            return Set.of();
        }

        if (topicPartitionAllowedMap.isEmpty()) {
            return Collections.unmodifiableSet(topic.partitions().keySet());
        }

        return topicPartitionAllowedMap.get().getOrDefault(topicId, Set.of());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubscribedTopicDescriberImpl that = (SubscribedTopicDescriberImpl) o;
        if (!topicPartitionAllowedMap.equals(that.topicPartitionAllowedMap)) return false;
        return metadataImage.equals(that.metadataImage);
    }

    @Override
    public int hashCode() {
        int result = metadataImage.hashCode();
        result = 31 * result + topicPartitionAllowedMap.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SubscribedTopicMetadata(" +
            "metadataImage=" + metadataImage +
            ", topicPartitionAllowedMap=" + topicPartitionAllowedMap +
            ')';
    }
}
