/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class PartitionGrouper {

    protected Collection<Set<String>> topicGroups;

    private Map<TopicPartition, Set<Long>> partitionToTaskIds;

    /**
     * Returns a map of task ids to groups of partitions. The task id is the 64 bit integer
     * which uniquely identifies a task. The higher 32 bit integer is an id assigned to a topic group.
     * The lower 32 bit integer is a partition id with which the task's local states are associated.
     *
     * @param metadata
     * @return a map of task ids to groups of partitions
     */
    public abstract Map<Long, List<TopicPartition>> partitionGroups(Cluster metadata);

    public final void topicGroups(Collection<Set<String>> topicGroups) {
        this.topicGroups = topicGroups;
    }

    public final void partitionToTaskIds(Map<TopicPartition, Set<Long>> partitionToTaskIds) {
        this.partitionToTaskIds = partitionToTaskIds;
    }

    public final Set<Long> taskIds(TopicPartition partition) {
        return partitionToTaskIds.get(partition);
    }

}
