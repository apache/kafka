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
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.MetadataResponse;

import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;

/**
 * Immutable internal view of cluster metadata.
 * For usages internal to the client, prefer to use & extend this class's API Vs public {@link Cluster}.
 */
public class InternalCluster {
    protected final Cluster publicCluster;
    protected final Map<TopicPartition, MetadataResponse.PartitionMetadata> partitionMetadata;

    public InternalCluster(
        Cluster publicCluster,
        Map<TopicPartition, MetadataResponse.PartitionMetadata> partitionMetadata
    ) {
        this.publicCluster = publicCluster;
        this.partitionMetadata = partitionMetadata;
    }

    public Optional<MetadataResponse.PartitionMetadata> partitionMetadata(TopicPartition topicPartition) {
        return Optional.ofNullable(partitionMetadata.get(topicPartition));
    }

    public Cluster toPublicCluster() {
        return publicCluster;
    }

    /**
     * Get the leader-epoch of input partition.
     * @param tp partition
     * @return leader-epoch if known else optional.empty() is returned.
     */
    public Optional<Integer> leaderEpochFor(TopicPartition tp) {
        PartitionMetadata partMetadata = partitionMetadata.get(tp);
        Optional<Integer> leaderEpoch = (partMetadata == null) ? Optional.empty() : partMetadata.leaderEpoch;
        return leaderEpoch;
    }

}
