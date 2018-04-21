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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of the {@link PartitionGrouper} interface that groups partitions by the partition id.
 *
 * Join operations requires that topics of the joining entities are copartitoned, i.e., being partitioned by the same key and having the same
 * number of partitions. Copartitioning is ensured by having the same number of partitions on
 * joined topics, and by using the serialization and Producer's default partitioner.
 */
public class DefaultPartitionGrouper implements PartitionGrouper {

    private static final Logger log = LoggerFactory.getLogger(DefaultPartitionGrouper.class);
    /**
     * Generate tasks with the assigned topic partitions.
     *
     * @param topicGroups   group of topics that need to be joined together
     * @param metadata      metadata of the consuming cluster
     * @return The map from generated task ids to the assigned partitions
     */
    public Map<TaskId, Set<TopicPartition>> partitionGroups(Map<Integer, Set<String>> topicGroups, Cluster metadata) {
        Map<TaskId, Set<TopicPartition>> groups = new HashMap<>();

        for (Map.Entry<Integer, Set<String>> entry : topicGroups.entrySet()) {
            Integer topicGroupId = entry.getKey();
            Set<String> topicGroup = entry.getValue();

            int maxNumPartitions = maxNumPartitions(metadata, topicGroup);

            for (int partitionId = 0; partitionId < maxNumPartitions; partitionId++) {
                Set<TopicPartition> group = new HashSet<>(topicGroup.size());

                for (String topic : topicGroup) {
                    List<PartitionInfo> partitions = metadata.partitionsForTopic(topic);
                    if (partitionId < partitions.size()) {
                        group.add(new TopicPartition(topic, partitionId));
                    }
                }
                groups.put(new TaskId(topicGroupId, partitionId), Collections.unmodifiableSet(group));
            }
        }

        return Collections.unmodifiableMap(groups);
    }

    /**
     * @throws StreamsException if no metadata can be received for a topic
     */
    protected int maxNumPartitions(Cluster metadata, Set<String> topics) {
        int maxNumPartitions = 0;
        for (String topic : topics) {
            List<PartitionInfo> partitions = metadata.partitionsForTopic(topic);

            if (partitions.isEmpty()) {

                log.warn("Skipping creating tasks for the topic group {} since topic {}'s metadata is not available yet;"
                         + " no tasks for this topic group will be assigned to any client.\n"
                         + " Make sure all supplied topics in the topology are created before starting"
                         + " as this could lead to unexpected results", topics, topic);
                return StreamsPartitionAssignor.NOT_AVAILABLE;
            } else {
                int numPartitions = partitions.size();
                if (numPartitions > maxNumPartitions)
                    maxNumPartitions = numPartitions;
            }
        }
        return maxNumPartitions;
    }

}



