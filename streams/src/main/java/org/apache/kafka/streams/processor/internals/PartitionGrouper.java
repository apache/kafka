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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Groups partitions by the partition id.
 *
 * Join operations requires that topics of the joining entities are copartitioned, i.e., being partitioned by the same key and having the same
 * number of partitions. Copartitioning is ensured by having the same number of partitions on
 * joined topics, and by using the serialization and Producer's default partitioner.
 */
public class PartitionGrouper {

    private static final Logger log = LoggerFactory.getLogger(PartitionGrouper.class);

    /**
     * Generate tasks with the assigned topic partitions.
     *
     * @param topicGroups   group of topics that need to be joined together
     * @param metadata      metadata of the consuming cluster
     * @return The map from generated task ids to the assigned partitions
     */
    public Map<TaskId, Set<TopicPartition>> partitionGroups(final Map<Subtopology, Set<String>> topicGroups, final Cluster metadata) {
        return partitionGroups(topicGroups, new HashMap<>(), new HashMap<>(), metadata);
    }

    /**
     * Generate tasks with the assigned topic partitions.
     *
     * @param sourceTopicGroups        group of source topics that need to be joined together
     * @param changelogTopicGroups     group of changelog topics that need to be joined together
     * @param changelogPartitionGroups the output grouping of the changelog topic partitions by task
     *                                 id. This input will be mutated.
     * @param metadata                 metadata of the consuming cluster
     * @return The map from generated task ids to the assigned partitions
     */
    public Map<TaskId, Set<TopicPartition>> partitionGroups(final Map<Subtopology, Set<String>> sourceTopicGroups,
                                                            final Map<Subtopology, Set<String>> changelogTopicGroups,
                                                            final Map<TaskId, Set<TopicPartition>> changelogPartitionGroups,
                                                            final Cluster metadata) {
        final Map<TaskId, Set<TopicPartition>> sourcePartitionGroups = new HashMap<>();

        for (final Map.Entry<Subtopology, Set<String>> entry : sourceTopicGroups.entrySet()) {
            final Subtopology subtopology = entry.getKey();
            final Set<String> sourceTopicGroup = entry.getValue();
            final Set<String> changelogTopicGroup = changelogTopicGroups.getOrDefault(subtopology, new HashSet<>());

            final int maxNumPartitions = maxNumPartitions(metadata, sourceTopicGroup);

            for (int partitionId = 0; partitionId < maxNumPartitions; partitionId++) {
                final Set<TopicPartition> sourcePartitionGroup = new HashSet<>(sourceTopicGroup.size());
                final Set<TopicPartition> changelogPartitionGroup = new HashSet<>(changelogTopicGroup.size());

                for (final String topic : sourceTopicGroup) {
                    final List<PartitionInfo> partitions = metadata.partitionsForTopic(topic);
                    if (partitionId < partitions.size()) {
                        sourcePartitionGroup.add(new TopicPartition(topic, partitionId));
                    }
                }

                for (final String topic : changelogTopicGroup) {
                    changelogPartitionGroup.add(new TopicPartition(topic, partitionId));
                }

                final TaskId taskId = new TaskId(subtopology.nodeGroupId, partitionId, subtopology.namedTopology);
                sourcePartitionGroups.put(taskId, Collections.unmodifiableSet(sourcePartitionGroup));
                changelogPartitionGroups.put(taskId, Collections.unmodifiableSet(changelogPartitionGroup));
            }
        }

        return Collections.unmodifiableMap(sourcePartitionGroups);
    }

    /**
     * @throws StreamsException if no metadata can be received for a topic
     */
    protected int maxNumPartitions(final Cluster metadata, final Set<String> topics) {
        int maxNumPartitions = 0;
        for (final String topic : topics) {
            final List<PartitionInfo> partitions = metadata.partitionsForTopic(topic);
            if (partitions.isEmpty()) {
                log.error("Empty partitions for topic {}", topic);
                throw new RuntimeException("Empty partitions for topic " + topic);
            }

            final int numPartitions = partitions.size();
            if (numPartitions > maxNumPartitions) {
                maxNumPartitions = numPartitions;
            }
        }
        return maxNumPartitions;
    }

}
