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
import org.apache.kafka.streams.processor.internals.KafkaStreamingPartitionAssignor;

import java.util.Map;
import java.util.Set;

public abstract class PartitionGrouper {

    protected Map<Integer, Set<String>> topicGroups;

    private KafkaStreamingPartitionAssignor partitionAssignor = null;

    /**
     * Returns a map of task ids to groups of partitions.
     *
     * @param metadata
     * @return a map of task ids to groups of partitions
     */
    public abstract Map<TaskId, Set<TopicPartition>> partitionGroups(Cluster metadata);

    public void topicGroups(Map<Integer, Set<String>> topicGroups) {
        this.topicGroups = topicGroups;
    }

    public void partitionAssignor(KafkaStreamingPartitionAssignor partitionAssignor) {
        this.partitionAssignor = partitionAssignor;
    }

    public Set<TaskId> taskIds(TopicPartition partition) {
        return partitionAssignor.taskIds(partition);
    }

    public Set<TaskId> standbyTasks() {
        return partitionAssignor.standbyTasks();
    }

}
