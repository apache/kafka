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
package org.apache.kafka.clients.consumer.internals;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class holds the data that is needed to participate in the Streams rebalance protocol.
 */
public class StreamsRebalanceData {

    public static class TaskId implements Comparable<TaskId> {

        private final String subtopologyId;
        private final int partitionId;

        public TaskId(final String subtopologyId, final int partitionId) {
            this.subtopologyId = Objects.requireNonNull(subtopologyId, "Subtopology ID cannot be null");
            this.partitionId = partitionId;
        }

        public int partitionId() {
            return partitionId;
        }

        public String subtopologyId() {
            return subtopologyId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TaskId taskId = (TaskId) o;
            return partitionId == taskId.partitionId && Objects.equals(subtopologyId, taskId.subtopologyId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(subtopologyId, partitionId);
        }

        @Override
        public int compareTo(TaskId taskId) {
            Objects.requireNonNull(taskId, "taskId cannot be null");
            return Comparator.comparing(TaskId::subtopologyId)
                .thenComparingInt(TaskId::partitionId).compare(this, taskId);
        }

        @Override
        public String toString() {
            return "TaskId{" +
                "subtopologyId=" + subtopologyId +
                ", partitionId=" + partitionId +
                '}';
        }
    }

    public static class Assignment {

        public static final Assignment EMPTY = new Assignment();

        private final Set<TaskId> activeTasks;

        private final Set<TaskId> standbyTasks;

        private final Set<TaskId> warmupTasks;

        private Assignment() {
            this.activeTasks = Set.of();
            this.standbyTasks = Set.of();
            this.warmupTasks = Set.of();
        }

        public Assignment(final Set<TaskId> activeTasks,
                          final Set<TaskId> standbyTasks,
                          final Set<TaskId> warmupTasks) {
            this.activeTasks = Set.copyOf(Objects.requireNonNull(activeTasks, "Active tasks cannot be null"));
            this.standbyTasks = Set.copyOf(Objects.requireNonNull(standbyTasks, "Standby tasks cannot be null"));
            this.warmupTasks = Set.copyOf(Objects.requireNonNull(warmupTasks, "Warmup tasks cannot be null"));
        }

        public Set<TaskId> activeTasks() {
            return activeTasks;
        }

        public Set<TaskId> standbyTasks() {
            return standbyTasks;
        }

        public Set<TaskId> warmupTasks() {
            return warmupTasks;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Assignment that = (Assignment) o;
            return Objects.equals(activeTasks, that.activeTasks)
                && Objects.equals(standbyTasks, that.standbyTasks)
                && Objects.equals(warmupTasks, that.warmupTasks);
        }

        @Override
        public int hashCode() {
            return Objects.hash(activeTasks, standbyTasks, warmupTasks);
        }

        public Assignment copy() {
            return new Assignment(activeTasks, standbyTasks, warmupTasks);
        }

        @Override
        public String toString() {
            return "Assignment{" +
                "activeTasks=" + activeTasks +
                ", standbyTasks=" + standbyTasks +
                ", warmupTasks=" + warmupTasks +
                '}';
        }
    }

    public static class Subtopology {

        private final Set<String> sourceTopics;
        private final Set<String> repartitionSinkTopics;
        private final Map<String, TopicInfo> stateChangelogTopics;
        private final Map<String, TopicInfo> repartitionSourceTopics;
        private final Collection<Set<String>> copartitionGroups;

        public Subtopology(final Set<String> sourceTopics,
                           final Set<String> repartitionSinkTopics,
                           final Map<String, TopicInfo> repartitionSourceTopics,
                           final Map<String, TopicInfo> stateChangelogTopics,
                           final Collection<Set<String>> copartitionGroups
        ) {
            this.sourceTopics = Set.copyOf(Objects.requireNonNull(sourceTopics, "Subtopology ID cannot be null"));
            this.repartitionSinkTopics =
                Set.copyOf(Objects.requireNonNull(repartitionSinkTopics, "Repartition sink topics cannot be null"));
            this.repartitionSourceTopics =
                Map.copyOf(Objects.requireNonNull(repartitionSourceTopics, "Repartition source topics cannot be null"));
            this.stateChangelogTopics =
                Map.copyOf(Objects.requireNonNull(stateChangelogTopics, "State changelog topics cannot be null"));
            this.copartitionGroups =
                Collections.unmodifiableCollection(Objects.requireNonNull(
                    copartitionGroups,
                    "Co-partition groups cannot be null"
                    )
                );
        }

        public Set<String> sourceTopics() {
            return sourceTopics;
        }

        public Set<String> repartitionSinkTopics() {
            return repartitionSinkTopics;
        }

        public Map<String, TopicInfo> stateChangelogTopics() {
            return stateChangelogTopics;
        }

        public Map<String, TopicInfo> repartitionSourceTopics() {
            return repartitionSourceTopics;
        }

        public Collection<Set<String>> copartitionGroups() {
            return copartitionGroups;
        }

        @Override
        public String toString() {
            return "Subtopology{" +
                "sourceTopics=" + sourceTopics +
                ", repartitionSinkTopics=" + repartitionSinkTopics +
                ", stateChangelogTopics=" + stateChangelogTopics +
                ", repartitionSourceTopics=" + repartitionSourceTopics +
                ", copartitionGroups=" + copartitionGroups +
                '}';
        }
    }

    public static class TopicInfo {

        private final Optional<Integer> numPartitions;
        private final Optional<Short> replicationFactor;
        private final Map<String, String> topicConfigs;

        public TopicInfo(final Optional<Integer> numPartitions,
                         final Optional<Short> replicationFactor,
                         final Map<String, String> topicConfigs) {
            this.numPartitions = Objects.requireNonNull(numPartitions, "Number of partitions cannot be null");
            this.replicationFactor = Objects.requireNonNull(replicationFactor, "Replication factor cannot be null");
            this.topicConfigs =
                Map.copyOf(Objects.requireNonNull(topicConfigs, "Additional topic configs cannot be null"));
        }

        public Optional<Integer> numPartitions() {
            return numPartitions;
        }

        public Optional<Short> replicationFactor() {
            return replicationFactor;
        }

        public Map<String, String> topicConfigs() {
            return topicConfigs;
        }

        @Override
        public String toString() {
            return "TopicInfo{" +
                "numPartitions=" + numPartitions +
                ", replicationFactor=" + replicationFactor +
                ", topicConfigs=" + topicConfigs +
                '}';
        }
    }

    private final Map<String, Subtopology> subtopologies;

    private final AtomicReference<Assignment> reconciledAssignment = new AtomicReference<>(Assignment.EMPTY);

    public StreamsRebalanceData(Map<String, Subtopology> subtopologies) {
        this.subtopologies = Map.copyOf(Objects.requireNonNull(subtopologies, "Subtopologies cannot be null"));
    }

    public Map<String, Subtopology> subtopologies() {
        return subtopologies;
    }

    public void setReconciledAssignment(final Assignment assignment) {
        reconciledAssignment.set(assignment);
    }

    public Assignment reconciledAssignment() {
        return reconciledAssignment.get();
    }
}
