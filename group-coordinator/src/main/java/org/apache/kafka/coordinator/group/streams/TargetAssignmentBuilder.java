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

import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.streams.assignor.AssignmentMemberSpec;
import org.apache.kafka.coordinator.group.streams.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.streams.assignor.GroupSpecImpl;
import org.apache.kafka.coordinator.group.streams.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignor;
import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignorException;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredTopology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Build the new target member assignments based on the provided parameters by calling the task assignor.
 * As a result,
 * it yields the records that must be persisted to the log and the new member assignments as a map from member ID to tasks tuple.
 * <p>
 * Records are only created for members which have a new target assignment. If their assignment did not change, no new record is needed.
 * <p>
 * When a member is deleted, it is assumed that its target assignment record is deleted as part of the member deletion process. In other
 * words, this class does not yield a tombstone for removed members.
 */
public class TargetAssignmentBuilder {

    /**
     * The group ID.
     */
    private final String groupId;
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
    private final Map<String, String> assignmentConfigs;

    /**
     * The members which have been updated or deleted. A null value signals deleted members.
     */
    private final Map<String, StreamsGroupMember> updatedMembers = new HashMap<>();

    /**
     * The members in the group.
     */
    private Map<String, StreamsGroupMember> members = Map.of();

    /**
     * The partition metadata.
     */
    private Map<String, org.apache.kafka.coordinator.group.streams.TopicMetadata> partitionMetadata = Map.of();

    /**
     * The existing target assignment.
     */
    private Map<String, org.apache.kafka.coordinator.group.streams.TasksTuple> targetAssignment = Map.of();

    /**
     * The topology.
     */
    private ConfiguredTopology topology;

    /**
     * The static members in the group.
     */
    private Map<String, String> staticMembers = Map.of();

    /**
     * Constructs the object.
     *
     * @param groupId    The group ID.
     * @param groupEpoch The group epoch to compute a target assignment for.
     * @param assignor   The assignor to use to compute the target assignment.
     */
    public TargetAssignmentBuilder(
        String groupId,
        int groupEpoch,
        TaskAssignor assignor,
        Map<String, String> assignmentConfigs
    ) {
        this.groupId = Objects.requireNonNull(groupId);
        this.groupEpoch = groupEpoch;
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
     * Adds all the existing static members.
     *
     * @param staticMembers The existing static members in the streams group.
     * @return This object.
     */
    public TargetAssignmentBuilder withStaticMembers(
        Map<String, String> staticMembers
    ) {
        this.staticMembers = staticMembers;
        return this;
    }

    /**
     * Adds the partition metadata to use.
     *
     * @param partitionMetadata The partition metadata.
     * @return This object.
     */
    public TargetAssignmentBuilder withPartitionMetadata(
        Map<String, org.apache.kafka.coordinator.group.streams.TopicMetadata> partitionMetadata
    ) {
        this.partitionMetadata = partitionMetadata;
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
     * Adds or updates a member. This is useful when the updated member is not yet materialized in memory.
     *
     * @param memberId The member ID.
     * @param member   The member to add or update.
     * @return This object.
     */
    public TargetAssignmentBuilder addOrUpdateMember(
        String memberId,
        StreamsGroupMember member
    ) {
        this.updatedMembers.put(memberId, member);
        return this;
    }

    /**
     * Removes a member. This is useful when the removed member is not yet materialized in memory.
     *
     * @param memberId The member ID.
     * @return This object.
     */
    public TargetAssignmentBuilder removeMember(
        String memberId
    ) {
        return addOrUpdateMember(memberId, null);
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

        // Update the member spec if updated or deleted members.
        updatedMembers.forEach((memberId, updatedMemberOrNull) -> {
            if (updatedMemberOrNull == null) {
                memberSpecs.remove(memberId);
            } else {
                org.apache.kafka.coordinator.group.streams.TasksTuple assignment = targetAssignment.getOrDefault(memberId,
                    org.apache.kafka.coordinator.group.streams.TasksTuple.EMPTY);

                // A new static member joins and needs to replace an existing departed one.
                if (updatedMemberOrNull.instanceId().isPresent()) {
                    String previousMemberId = staticMembers.get(updatedMemberOrNull.instanceId().get());
                    if (previousMemberId != null && !previousMemberId.equals(memberId)) {
                        assignment = targetAssignment.getOrDefault(previousMemberId,
                            org.apache.kafka.coordinator.group.streams.TasksTuple.EMPTY);
                    }
                }

                memberSpecs.put(memberId, createAssignmentMemberSpec(
                    updatedMemberOrNull,
                    assignment
                ));
            }
        });

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
                new TopologyMetadata(partitionMetadata, topology.subtopologies().get())
            );
        } else {
            newGroupAssignment = new GroupAssignment(
                memberSpecs.keySet().stream().collect(Collectors.toMap(x -> x, x -> MemberAssignment.empty())));
        }

        // Compute delta from previous to new target assignment and create the
        // relevant records.
        List<CoordinatorRecord> records = new ArrayList<>();
        Map<String, org.apache.kafka.coordinator.group.streams.TasksTuple> newTargetAssignment = new HashMap<>();

        memberSpecs.keySet().forEach(memberId -> {
            org.apache.kafka.coordinator.group.streams.TasksTuple oldMemberAssignment = targetAssignment.get(memberId);
            org.apache.kafka.coordinator.group.streams.TasksTuple newMemberAssignment = newMemberAssignment(newGroupAssignment, memberId);

            newTargetAssignment.put(memberId, newMemberAssignment);

            if (oldMemberAssignment == null) {
                // If the member had no assignment, we always create a record for it.
                records.add(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(
                    groupId,
                    memberId,
                    newMemberAssignment
                ));
            } else {
                // If the member had an assignment, we only create a record if the
                // new assignment is different.
                if (!newMemberAssignment.equals(oldMemberAssignment)) {
                    records.add(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(
                        groupId,
                        memberId,
                        newMemberAssignment
                    ));
                }
            }
        });

        // Bump the target assignment epoch.
        records.add(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentEpochRecord(groupId, groupEpoch));

        return new TargetAssignmentResult(records, newTargetAssignment);
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
     * The assignment result returned by {{@link TargetAssignmentBuilder#build()}}.
     *
     * @param records          The records that must be applied to the __consumer_offsets topics to persist the new target assignment.
     * @param targetAssignment The new target assignment for the group.
     */
    public record TargetAssignmentResult(
        List<CoordinatorRecord> records,
        Map<String, TasksTuple> targetAssignment
    ) {
        public TargetAssignmentResult {
            Objects.requireNonNull(records);
            Objects.requireNonNull(targetAssignment);
        }
    }
}
