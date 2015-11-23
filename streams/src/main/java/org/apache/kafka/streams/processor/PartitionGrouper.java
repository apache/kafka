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

    public static class TopicsInfo {
        public Set<String> sourceTopics;
        public Set<String> stateTopics;

        public TopicsInfo(Set<String> sourceTopics, Set<String> stateTopics) {
            this.sourceTopics = sourceTopics;
            this.stateTopics = stateTopics;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof TopicsInfo) {
                TopicsInfo other = (TopicsInfo) o;
                return other.sourceTopics.equals(this.sourceTopics) && other.stateTopics.equals(this.stateTopics);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            long n = ((long) sourceTopics.hashCode() << 32) | (long) stateTopics.hashCode();
            return (int) (n % 0xFFFFFFFFL);
        }
    }

    public static class TasksInfo {
        public Map<TaskId, Set<TopicPartition>> partitionsForTask;
        public Map<String, Set<TaskId>> tasksForState;

        public TasksInfo(Map<TaskId, Set<TopicPartition>> partitionsForTask, Map<String, Set<TaskId>> tasksForState) {
            this.partitionsForTask = partitionsForTask;
            this.tasksForState = tasksForState;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof TasksInfo) {
                TasksInfo other = (TasksInfo) o;
                return other.partitionsForTask.equals(this.partitionsForTask) && other.tasksForState.equals(this.tasksForState);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            long n = ((long) partitionsForTask.hashCode() << 32) | (long) tasksForState.hashCode();
            return (int) (n % 0xFFFFFFFFL);
        }
    }

    protected Map<Integer, TopicsInfo> topicGroups;

    private KafkaStreamingPartitionAssignor partitionAssignor = null;

    /**
     * Returns generated tasks information.
     *
     * @param metadata of the cluster
     * @return tasks information
     */
    public abstract TasksInfo partitionGroups(Cluster metadata);

    public void topicGroups(Map<Integer, TopicsInfo> topicGroups) {
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
