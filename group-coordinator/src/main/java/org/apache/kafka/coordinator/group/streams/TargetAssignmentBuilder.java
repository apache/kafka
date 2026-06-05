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
import org.apache.kafka.coordinator.group.streams.assignor.AssignmentMemberSpec;
import org.apache.kafka.coordinator.group.streams.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.streams.assignor.GroupSpecImpl;
import org.apache.kafka.coordinator.group.streams.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignor;
import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignorException;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredTopology;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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
     * The partition assignor used to compute the assignment.
     */
    private final TaskAssignor assignor;

    /**
     * The assignment configs.
     */
    private final Map<String, String> assignmentConfigs;

    /**
     * The members in the group.
     */
    private Map<String, StreamsGroupMember> members = Map.of();

    /**
     * The metadata image.
     */
    private CoordinatorMetadataImage metadataImage = CoordinatorMetadataImage.EMPTY;

    /**
     * The existing target assignment.
     */
    private Map<String, org.apache.kafka.coordinator.group.streams.TasksTuple> targetAssignment = Map.of();

    /**
     * The topology.
     */
    private ConfiguredTopology topology;

    /**
     * Constructs the object.
     *
     * @param assignor   The assignor to use to compute the target assignment.
     */
    public TargetAssignmentBuilder(
        TaskAssignor assignor,
        Map<String, String> assignmentConfigs
    ) {
        this.assignor = Objects.requireNonNull(assignor);
        this.assignmentConfigs = Objects.requireNonNull(assignmentConfigs);
    }

    static AssignmentMemberSpec createAssignmentMemberSpec(
        StreamsGroupMember member,
        TasksTuple targetAssignment
    ) {
        return new AssignmentMemberSpec(
            member.instanceId(),
            member.rackId(),
            targetAssignment.activeTasks(),
            targetAssignment.standbyTasks(),
            targetAssignment.warmupTasks(),
            member.processId(),
            member.clientTags(),
            Map.of(),
            Map.of()
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
     * Adds the existing target assignment.
     *
     * @param targetAssignment The existing target assignment.
     * @return This object.
     */
    public TargetAssignmentBuilder withTargetAssignment(
        Map<String, org.apache.kafka.coordinator.group.streams.TasksTuple> targetAssignment
    ) {
        this.targetAssignment = targetAssignment;
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
        Map<String, AssignmentMemberSpec> memberSpecs = new HashMap<>();

        // Prepare the member spec for all members.
        members.forEach((memberId, member) -> memberSpecs.put(memberId, createAssignmentMemberSpec(
            member,
            targetAssignment.getOrDefault(memberId, org.apache.kafka.coordinator.group.streams.TasksTuple.EMPTY)
        )));

        // Compute the assignment.
        GroupAssignment newGroupAssignment;
        if (topology.isReady()) {
            if (topology.subtopologies().isEmpty()) {
                throw new IllegalStateException("Subtopologies must be present if topology is ready.");
            }
            newGroupAssignment = assignor.assign(
                new GroupSpecImpl(
                    Collections.unmodifiableMap(memberSpecs),
                    assignmentConfigs
                ),
                new TopologyMetadata(metadataImage, topology.subtopologies().get())
            );
        } else {
            newGroupAssignment = new GroupAssignment(
                memberSpecs.keySet().stream().collect(Collectors.toMap(x -> x, x -> MemberAssignment.empty())));
        }

        Map<String, org.apache.kafka.coordinator.group.streams.TasksTuple> newTargetAssignment = new HashMap<>();
        memberSpecs.keySet().forEach(memberId -> {
            newTargetAssignment.put(memberId, newMemberAssignment(newGroupAssignment, memberId));
        });

        return new TargetAssignmentResult(newTargetAssignment, time.milliseconds());
    }

    private TasksTuple newMemberAssignment(
        GroupAssignment newGroupAssignment,
        String memberId
    ) {
        MemberAssignment newMemberAssignment = newGroupAssignment.members().get(memberId);
        if (newMemberAssignment != null) {
            return new TasksTuple(
                newMemberAssignment.activeTasks(),
                newMemberAssignment.standbyTasks(),
                newMemberAssignment.warmupTasks()
            );
        } else {
            return TasksTuple.EMPTY;
        }
    }

    /**
     * The assignment result returned by {@link TargetAssignmentBuilder#build()}.
     *
     * @param targetAssignment      The new target assignment for the group.
     * @param assignmentTimestampMs The time at which the target assignment calculation finished.
     */
    public record TargetAssignmentResult(
        Map<String, TasksTuple> targetAssignment,
        long assignmentTimestampMs
    ) {
        public TargetAssignmentResult {
            Objects.requireNonNull(targetAssignment);
        }
    }
}
