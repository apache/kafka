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

import org.apache.kafka.coordinator.group.api.streams.assignor.AssignmentConfigs;
import org.apache.kafka.coordinator.group.api.streams.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.streams.assignor.AssignmentConfigsImpl;
import org.apache.kafka.coordinator.group.streams.assignor.GroupSpecImpl;
import org.apache.kafka.coordinator.group.streams.assignor.MemberMetadataAndStateImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Builds the {@link GroupSpec} describing the members of a streams group and their existing assignments.
 */
public class GroupSpecBuilder {

    /**
     * The assignment configs.
     */
    private AssignmentConfigs assignmentConfigs;

    /**
     * The members in the group.
     */
    private Map<String, StreamsGroupMember> members = Map.of();

    /*
     * The latest per-task changelog offsets reported by each member, keyed by member ID. Transient (not persisted);
     * fed to the assignor so it can estimate task lag.
     */
    private Map<String, MemberTaskOffsets> taskOffsets = Map.of();

    /**
     * Constructs the object.
     */
    public GroupSpecBuilder(
        Map<String, String> assignmentConfigs
    ) {
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
     * Adds all the existing members.
     *
     * @param members The existing members in the streams group.
     * @return This object.
     */
    public GroupSpecBuilder withMembers(
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
    public GroupSpecBuilder withTaskOffsets(
        Map<String, MemberTaskOffsets> taskOffsets
    ) {
        this.taskOffsets = taskOffsets;
        return this;
    }

    /**
     * Builds the {@link GroupSpec} to be passed to the assignor.
     *
     * @return The {@link GroupSpec} describing the members and their existing assignments.
     */
    public GroupSpec build() {
        Map<String, MemberMetadataAndStateImpl> memberMetadataMap = new HashMap<>();

        // Prepare the member metadata for all members.
        members.forEach((memberId, member) -> memberMetadataMap.put(memberId, createMemberMetadataAndState(
            member,
            taskOffsets.getOrDefault(memberId, MemberTaskOffsets.EMPTY)
        )));

        return new GroupSpecImpl(
            memberMetadataMap,
            assignmentConfigs
        );
    }
}
