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

import org.apache.kafka.coordinator.group.streams.assignor.AssignmentMemberSpec;
import org.apache.kafka.coordinator.group.streams.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.streams.assignor.GroupSpecImpl;

import java.util.Collections;
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
    private Map<String, String> assignmentConfigs;

    /**
     * The members in the group.
     */
    private Map<String, StreamsGroupMember> members = Map.of();

    /**
     * The existing target assignment.
     */
    private Map<String, org.apache.kafka.coordinator.group.streams.TasksTuple> targetAssignment = Map.of();

    /**
     * Constructs the object.
     */
    public GroupSpecBuilder(
        Map<String, String> assignmentConfigs
    ) {
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
     * Adds the existing target assignment.
     *
     * @param targetAssignment The existing target assignment.
     * @return This object.
     */
    public GroupSpecBuilder withTargetAssignment(
        Map<String, org.apache.kafka.coordinator.group.streams.TasksTuple> targetAssignment
    ) {
        this.targetAssignment = targetAssignment;
        return this;
    }

    /**
     * Builds the {@link GroupSpec} to be passed to the assignor.
     *
     * @return The {@link GroupSpec} describing the members and their existing assignments.
     */
    public GroupSpec build() {
        Map<String, AssignmentMemberSpec> memberSpecs = new HashMap<>();

        // Prepare the member spec for all members.
        members.forEach((memberId, member) -> memberSpecs.put(memberId, createAssignmentMemberSpec(
            member,
            targetAssignment.getOrDefault(memberId, org.apache.kafka.coordinator.group.streams.TasksTuple.EMPTY)
        )));

        return new GroupSpecImpl(
            Collections.unmodifiableMap(memberSpecs),
            assignmentConfigs
        );
    }
}
