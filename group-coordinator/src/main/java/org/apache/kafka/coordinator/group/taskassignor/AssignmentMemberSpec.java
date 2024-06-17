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
package org.apache.kafka.coordinator.group.taskassignor;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The assignment specification for a Streams group member.
 */
public class AssignmentMemberSpec {

    /**
     * The instance ID if provided.
     */
    private final Optional<String> instanceId;

    /**
     * The rack ID if provided.
     */
    private final Optional<String> rackId;

    /**
     * Reconciled active tasks
     */
    private final Map<String, Set<Integer>> activeTasks;

    /**
     * Reconciled standby tasks
     */
    private final Map<String, Set<Integer>> standbyTasks;

    /**
     * Reconciled warm-up tasks
     */
    private final Map<String, Set<Integer>> warmupTasks;

    /**
     * The process ID.
     */
    private final String processId;

    /**
     * The client tags for a rack-aware assignment.
     */
    private final Map<String, String> clientTags;

    /**
     * The assignment configs for customizing the assignment.
     */
    private final Map<String, String> assignmentConfigs;

    /**
     * The last received cumulative task offsets of assigned tasks or dormant tasks.
     */
    private final Map<TaskId, Long> taskOffsets;

    /**
     * @return The instance ID as an Optional.
     */
    public Optional<String> instanceId() {
        return instanceId;
    }

    /**
     * @return The rack ID as an Optional.
     */
    public Optional<String> rackId() {
        return rackId;
    }

    /**
     * @return The assigned active tasks.
     */
    public Map<String, Set<Integer>> activeTasks() {
        return activeTasks;
    }

    /**
     * @return The assigned standby tasks.
     */
    public Map<String, Set<Integer>> standbyTasks() {
        return standbyTasks;
    }

    /**
     * @return The assigned warm-up tasks.
     */
    public Map<String, Set<Integer>> warmupTasks() {
        return warmupTasks;
    }

    public Map<TaskId, Long> taskOffsets() {
        return taskOffsets;
    }

    public Map<String, String> clientTags() {
        return clientTags;
    }

    public Map<String, String> assignmentConfigs() {
        return assignmentConfigs;
    }

    public String processId() {
        return processId;
    }

    public AssignmentMemberSpec(final Optional<String> instanceId,
                                final Optional<String> rackId,
                                final Map<String, Set<Integer>> activeTasks,
                                final Map<String, Set<Integer>> standbyTasks,
                                final Map<String, Set<Integer>> warmupTasks,
                                final String processId,
                                final Map<String, String> clientTags,
                                final Map<String, String> assignmentConfigs,
                                final Map<TaskId, Long> taskOffsets) {
        this.instanceId = Objects.requireNonNull(instanceId);
        this.rackId = Objects.requireNonNull(rackId);
        this.activeTasks = Collections.unmodifiableMap(Objects.requireNonNull(activeTasks));
        this.standbyTasks = Collections.unmodifiableMap(Objects.requireNonNull(standbyTasks));
        this.warmupTasks = Collections.unmodifiableMap(Objects.requireNonNull(warmupTasks));
        this.processId = Objects.requireNonNull(processId);
        this.clientTags = Collections.unmodifiableMap(Objects.requireNonNull(clientTags));
        this.assignmentConfigs = Collections.unmodifiableMap(Objects.requireNonNull(assignmentConfigs));
        this.taskOffsets = Collections.unmodifiableMap(Objects.requireNonNull(taskOffsets));
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AssignmentMemberSpec that = (AssignmentMemberSpec) o;
        return Objects.equals(instanceId, that.instanceId)
            && Objects.equals(rackId, that.rackId)
            && Objects.equals(activeTasks, that.activeTasks)
            && Objects.equals(standbyTasks, that.standbyTasks)
            && Objects.equals(warmupTasks, that.warmupTasks)
            && Objects.equals(processId, that.processId)
            && Objects.equals(clientTags, that.clientTags)
            && Objects.equals(assignmentConfigs, that.assignmentConfigs)
            && Objects.equals(taskOffsets, that.taskOffsets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            instanceId,
            rackId,
            activeTasks,
            standbyTasks,
            warmupTasks,
            processId,
            clientTags,
            assignmentConfigs,
            taskOffsets
        );
    }

    @Override
    public String toString() {
        return "AssignmentMemberSpec{" +
            "instanceId=" + instanceId +
            ", rackId=" + rackId +
            ", activeTasks=" + activeTasks +
            ", standbyTasks=" + standbyTasks +
            ", warmupTasks=" + warmupTasks +
            ", processId='" + processId + '\'' +
            ", clientTags=" + clientTags +
            ", assignmentConfigs=" + assignmentConfigs +
            ", taskOffsets=" + taskOffsets +
            '}';
    }

}
