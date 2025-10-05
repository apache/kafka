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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A description of the assignments of a specific group member.
 */
@InterfaceStability.Evolving
public class StreamsGroupMemberAssignment {

    private final List<TaskIds> activeTasks;
    private final List<TaskIds> standbyTasks;
    private final List<TaskIds> warmupTasks;

    public StreamsGroupMemberAssignment(
        final List<TaskIds> activeTasks,
        final List<TaskIds> standbyTasks,
        final List<TaskIds> warmupTasks
    ) {
        this.activeTasks = activeTasks;
        this.standbyTasks = standbyTasks;
        this.warmupTasks = warmupTasks;
    }

    /**
     * Active tasks for this client.
     */
    public List<TaskIds> activeTasks() {
        return List.copyOf(activeTasks);
    }

    /**
     * Standby tasks for this client.
     */
    public List<TaskIds> standbyTasks() {
        return List.copyOf(standbyTasks);
    }

    /**
     * Warmup tasks for this client.
     */
    public List<TaskIds> warmupTasks() {
        return List.copyOf(warmupTasks);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final StreamsGroupMemberAssignment that = (StreamsGroupMemberAssignment) o;
        return Objects.equals(activeTasks, that.activeTasks)
            && Objects.equals(standbyTasks, that.standbyTasks)
            && Objects.equals(warmupTasks, that.warmupTasks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            activeTasks,
            standbyTasks,
            warmupTasks
        );
    }

    @Override
    public String toString() {
        return "(" +
            "activeTasks=" + activeTasks.stream().map(TaskIds::toString).collect(Collectors.joining(",")) +
            ", standbyTasks=" + standbyTasks.stream().map(TaskIds::toString).collect(Collectors.joining(",")) +
            ", warmupTasks=" + warmupTasks.stream().map(TaskIds::toString).collect(Collectors.joining(",")) +
            ')';
    }

    /**
     * All tasks for one subtopology of a member.
     */
    public static class TaskIds {
        private final String subtopologyId;
        private final List<Integer> partitions;

        public TaskIds(final String subtopologyId, final List<Integer> partitions) {
            this.subtopologyId = Objects.requireNonNull(subtopologyId, "subtopologyId must be non-null");
            this.partitions = Objects.requireNonNull(partitions, "partitions must be non-null");
        }

        /**
         * The subtopology identifier.
         */
        public String subtopologyId() {
            return subtopologyId;
        }

        /**
         * The partitions of the subtopology processed by this member.
         */
        public List<Integer> partitions() {
            return List.copyOf(partitions);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final TaskIds taskIds = (TaskIds) o;
            return Objects.equals(subtopologyId, taskIds.subtopologyId)
                && Objects.equals(partitions, taskIds.partitions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                subtopologyId,
                partitions
            );
        }

        @Override
        public String toString() {
            return partitions.stream().map(x -> subtopologyId + "_" + x).collect(Collectors.joining(","));
        }
    }
}
