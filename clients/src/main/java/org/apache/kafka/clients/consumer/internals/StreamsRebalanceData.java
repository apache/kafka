package org.apache.kafka.clients.consumer.internals;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class StreamsRebalanceData {

    public static class Assignment {

        public static final Assignment EMPTY = new Assignment();

        public final Set<TaskId> activeTasks = new HashSet<>();

        public final Set<TaskId> standbyTasks = new HashSet<>();

        public final Set<TaskId> warmupTasks = new HashSet<>();

        public Assignment() {
        }

        public Assignment(final Set<TaskId> activeTasks,
                          final Set<TaskId> standbyTasks,
                          final Set<TaskId> warmupTasks) {
            this.activeTasks.addAll(activeTasks);
            this.standbyTasks.addAll(standbyTasks);
            this.warmupTasks.addAll(warmupTasks);
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

    public static class TaskId implements Comparable<TaskId> {

        private final String subtopologyId;
        private final int partitionId;

        public int partitionId() {
            return partitionId;
        }

        public String subtopologyId() {
            return subtopologyId;
        }

        public TaskId(final String subtopologyId, final int partitionId) {
            this.subtopologyId = subtopologyId;
            this.partitionId = partitionId;
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
            if (subtopologyId.equals(taskId.subtopologyId)) {
                return partitionId - taskId.partitionId;
            }
            return subtopologyId.compareTo(taskId.subtopologyId);
        }

        @Override
        public String toString() {
            return "TaskId{" +
                "subtopologyId=" + subtopologyId +
                ", partitionId=" + partitionId +
                '}';
        }
    }

    public static class Subtopology {

        public final Set<String> sourceTopics;
        public final Set<String> repartitionSinkTopics;
        public final Map<String, TopicInfo> stateChangelogTopics;
        public final Map<String, TopicInfo> repartitionSourceTopics;
        public final Collection<Set<String>> copartitionGroups;

        public Subtopology(final Set<String> sourceTopics,
                           final Set<String> repartitionSinkTopics,
                           final Map<String, TopicInfo> repartitionSourceTopics,
                           final Map<String, TopicInfo> stateChangelogTopics,
                           final Collection<Set<String>> copartitionGroups
        ) {
            this.sourceTopics = sourceTopics;
            this.repartitionSinkTopics = repartitionSinkTopics;
            this.stateChangelogTopics = stateChangelogTopics;
            this.repartitionSourceTopics = repartitionSourceTopics;
            this.copartitionGroups = copartitionGroups;
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

        public final Optional<Integer> numPartitions;
        public final Optional<Short> replicationFactor;
        public final Map<String, String> topicConfigs;

        public TopicInfo(final Optional<Integer> numPartitions,
                         final Optional<Short> replicationFactor,
                         final Map<String, String> topicConfigs) {
            this.numPartitions = numPartitions;
            this.replicationFactor = replicationFactor;
            this.topicConfigs = topicConfigs;
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

    private final AtomicReference<Assignment> reconciledAssignment = new AtomicReference<>(
        new Assignment(
            new HashSet<>(),
            new HashSet<>(),
            new HashSet<>()
        )
    );

    public StreamsRebalanceData(Map<String, Subtopology> subtopologies) {
        this.subtopologies = subtopologies;
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
