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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.group.TargetAssignmentMetadata;
import org.apache.kafka.coordinator.group.api.streams.assignor.AssignmentConfigs;
import org.apache.kafka.coordinator.group.api.streams.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.streams.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.streams.assignor.TaskAssignor;
import org.apache.kafka.coordinator.group.api.streams.assignor.TaskAssignorException;
import org.apache.kafka.coordinator.group.streams.assignor.AssignmentConfigsImpl;
import org.apache.kafka.coordinator.group.streams.assignor.GroupSpecImpl;
import org.apache.kafka.coordinator.group.streams.assignor.MemberMetadataAndStateImpl;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredTopology;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Build the new target member assignments based on the provided parameters by calling the task assignor.
 */
public class TargetAssignmentBuilder {

    /**
     * The time.
     */
    private Time time;

    /**
     * The group epoch.
     */
    private final int groupEpoch;

    /**
     * The partition assignor used to compute the assignment.
     */
    private final TaskAssignor assignor;

    /**
     * The assignment configs.
     */
    private final AssignmentConfigs assignmentConfigs;

    /**
     * The members in the group.
     */
    private Map<String, StreamsGroupMember> members = Map.of();

    /**
     * The metadata image.
     */
    private CoordinatorMetadataImage metadataImage = CoordinatorMetadataImage.EMPTY;

    /**
     * The topology.
     */
    private ConfiguredTopology topology;

    /**
     * The latest per-task changelog offsets reported by each member, keyed by member ID. Transient (not persisted);
     * fed to the assignor so it can estimate task lag.
     */
    private Map<String, MemberTaskOffsets> taskOffsets = Map.of();

    /**
     * Constructs the object.
     *
     * @param groupEpoch The group epoch to compute a target assignment for.
     * @param assignor   The assignor to use to compute the target assignment.
     */
    public TargetAssignmentBuilder(
        int groupEpoch,
        TaskAssignor assignor,
        Map<String, String> assignmentConfigs
    ) {
        this.groupEpoch = groupEpoch;
        this.assignor = Objects.requireNonNull(assignor);
        this.assignmentConfigs = AssignmentConfigsImpl.fromMap(Objects.requireNonNull(assignmentConfigs));
    }

    static MemberMetadataAndStateImpl createMemberMetadataAndState(
        StreamsGroupMember member,
        MemberTaskOffsets taskOffsets
    ) {
        // Active, standby and warm-up tasks all reflect the tasks the member currently has, not the
        // target assignment.
        TasksTupleWithEpochs currentAssignment = member.assignedTasks();
        return new MemberMetadataAndStateImpl(
            member.instanceId(),
            member.rackId(),
            member.processId(),
            member.clientTags(),
            currentAssignment.activeTasks(),
            currentAssignment.standbyTasks(),
            currentAssignment.warmupTasks(),
            taskOffsets.taskOffsets(),
            taskOffsets.taskEndOffsets()
        );
    }

    /**
     * Sets the time.
     *
     * @param time The time.
     * @return This object.
     */
    public TargetAssignmentBuilder withTime(Time time) {
        this.time = time;
        return this;
    }

    /**
     * Adds all the existing members.
     *
     * @param members The existing members in the streams group.
     * @return This object.
     */
    public TargetAssignmentBuilder withMembers(
        Map<String, StreamsGroupMember> members
    ) {
        this.members = members;
        return this;
    }

    /**
     * Adds the latest per-task changelog offsets reported by each member.
     *
     * @param taskOffsets The reported task offsets/end-offsets keyed by member ID.
     * @return This object.
     */
    public TargetAssignmentBuilder withTaskOffsets(
        Map<String, MemberTaskOffsets> taskOffsets
    ) {
        this.taskOffsets = taskOffsets;
        return this;
    }

    /**
     * Adds the metadata image to use.
     *
     * @param metadataImage The metadata image.
     * @return This object.
     */
    public TargetAssignmentBuilder withMetadataImage(
        CoordinatorMetadataImage metadataImage
    ) {
        this.metadataImage = metadataImage;
        return this;
    }

    /**
     * Adds the topology image.
     *
     * @param topology The topology.
     * @return This object.
     */
    public TargetAssignmentBuilder withTopology(
        ConfiguredTopology topology
    ) {
        this.topology = topology;
        return this;
    }

    /**
     * Builds the new target assignment.
     *
     * @return A TargetAssignmentResult which contains the records to update the existing target assignment.
     * @throws TaskAssignorException if the target assignment cannot be computed.
     */
    public TargetAssignmentResult build() throws TaskAssignorException {
        Map<String, MemberMetadataAndStateImpl> memberMetadataMap = new HashMap<>();

        // Prepare the member metadata for all members.
        members.forEach((memberId, member) -> memberMetadataMap.put(memberId, createMemberMetadataAndState(
            member,
            taskOffsets.getOrDefault(memberId, MemberTaskOffsets.EMPTY)
        )));

        // Compute the assignment.
        GroupAssignment newGroupAssignment;
        if (topology.isReady()) {
            if (topology.subtopologies().isEmpty()) {
                throw new IllegalStateException("Subtopologies must be present if topology is ready.");
            }
            newGroupAssignment = assignor.assign(
                new GroupSpecImpl(
                    memberMetadataMap,
                    assignmentConfigs
                ),
                new TopologyMetadata(metadataImage, topology.subtopologies().get())
            );
        } else {
            newGroupAssignment = new GroupAssignment(
                memberMetadataMap.keySet().stream().collect(Collectors.toMap(x -> x, x -> new MemberAssignment(Map.of(), Map.of()))));
        }

        Map<String, org.apache.kafka.coordinator.group.streams.TasksTuple> newTargetAssignment = new HashMap<>();
        memberMetadataMap.keySet().forEach(memberId -> {
            newTargetAssignment.put(memberId, newMemberAssignment(newGroupAssignment, memberId));
        });

        return new TargetAssignmentResult(
            newTargetAssignment,
            new TargetAssignmentMetadata(groupEpoch, time.milliseconds())
        );
    }

    private TasksTuple newMemberAssignment(
        GroupAssignment newGroupAssignment,
        String memberId
    ) {
        MemberAssignment newMemberAssignment = newGroupAssignment.members().get(memberId);
        if (newMemberAssignment != null) {
            // Copy the maps returned by the assignor so the server does not keep a reference to
            // maps the assignor is free to mutate afterwards.
            return new TasksTuple(
                copyTasks(newMemberAssignment.activeTasks()),
                copyTasks(newMemberAssignment.standbyTasks()),
                // Warm-up tasks are not assigned by the assignor; they are decided during reconciliation.
                Map.of()
            );
        } else {
            return TasksTuple.EMPTY;
        }
    }

    private static Map<String, Set<Integer>> copyTasks(Map<String, Set<Integer>> tasks) {
        Map<String, Set<Integer>> copy = new HashMap<>();
        tasks.forEach((subtopologyId, partitions) -> copy.put(subtopologyId, new HashSet<>(partitions)));
        return copy;
    }

    /**
     * The assignment result returned by {@link TargetAssignmentBuilder#build()}.
     *
     * @param targetAssignment         The new target assignment for the group.
     * @param targetAssignmentMetadata The new target assignment metadata.
     */
    public record TargetAssignmentResult(
        Map<String, TasksTuple> targetAssignment,
        TargetAssignmentMetadata targetAssignmentMetadata
    ) {
        public TargetAssignmentResult {
            Objects.requireNonNull(targetAssignment);
            Objects.requireNonNull(targetAssignmentMetadata);
        }
    }
}
