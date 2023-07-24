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
package org.apache.kafka.coordinator.group.consumer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.assignor.PartitionMetadata;
import org.apache.kafka.coordinator.group.assignor.SubscribedTopicDescriber;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * The subscribed topic metadata class is used by the {@link PartitionAssignor}
 * to obtain topic and partition metadata of subscribed topics.
 */
public class SubscribedTopicMetadata implements SubscribedTopicDescriber {

    /**
     * The topic IDs are mapped to their corresponding {@link PartitionMetadata}
     * object, which contains metadata such as the rack information for the partition replicas.
     */
    Map<Uuid, PartitionMetadata> topicPartitionMetadata;

    public SubscribedTopicMetadata(Map<Uuid, PartitionMetadata> TopicPartitionMetadata) {
        this.topicPartitionMetadata = TopicPartitionMetadata;
    }

    /**
     * A set of topic Ids that the consumer group is subscribed to.
     *
     * @return Set of topic Ids corresponding to the subscribed topics.
     */
    @Override
    public Set<Uuid> subscribedTopicIds() {
        return topicPartitionMetadata.keySet();
    }

    /**
     * Number of partitions available for the given topic Id.
     *
     * @param topicId   Uuid corresponding to the topic.
     * @return The number of partitions corresponding to the given topicId.
     *         If the topicId doesn't exist return 0;
     */
    @Override
    public int numPartitions(Uuid topicId) {
        PartitionMetadata partitionMetadata = topicPartitionMetadata.get(topicId);
        if (partitionMetadata == null) {
            return 0;
        }

        return partitionMetadata.numPartitions();
    }

    /**
     * Returns all the racks associated with the replicas for the given partition.
     *
     * @param topicId   Uuid corresponding to the partition's topic.
     * @param partition Partition number within topic.
     * @return The set of racks corresponding to the replicas of the topics partition.
     *         If the topicId doesn't exist return an empty set;
     */
    @Override
    public Set<String> racksForPartition(Uuid topicId, int partition) {
        PartitionMetadata partitionMetadata = topicPartitionMetadata.get(topicId);
        if (partitionMetadata == null) {
            return Collections.emptySet();
        }

        return partitionMetadata.racks(partition);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubscribedTopicMetadata that = (SubscribedTopicMetadata) o;
        return topicPartitionMetadata.equals(that.topicPartitionMetadata);
    }

    @Override
    public int hashCode() {
        return topicPartitionMetadata.hashCode();
    }

    @Override
    public String toString() {
        return "SubscribedTopicMetadata(" +
            "topicPartitionMetadata=" + topicPartitionMetadata +
            ')';
    }
}
