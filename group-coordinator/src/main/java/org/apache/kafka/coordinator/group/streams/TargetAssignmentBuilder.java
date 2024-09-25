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
import org.apache.kafka.coordinator.group.taskassignor.AssignmentMemberSpec;
import org.apache.kafka.coordinator.group.taskassignor.GroupAssignment;
import org.apache.kafka.coordinator.group.taskassignor.GroupSpecImpl;
import org.apache.kafka.coordinator.group.taskassignor.MemberAssignment;
import org.apache.kafka.coordinator.group.taskassignor.TaskAssignor;
import org.apache.kafka.coordinator.group.taskassignor.TaskAssignorException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Build a new Target Assignment based on the provided parameters. As a result, it yields the records that must be persisted to the log and
 * the new member assignments as a map.
 * <p>
 * Records are only created for members which have a new target assignment. If their assignment did not change, no new record is needed.
 * <p>
 * When a member is deleted, it is assumed that its target assignment record is deleted as part of the member deletion process. In other
 * words, this class does not yield a tombstone for removed members.
 */
public class TargetAssignmentBuilder {

    /**
     * The assignment result returned by {{@link TargetAssignmentBuilder#build()}}.
     */
    public static class TargetAssignmentResult {

        /**
         * The records that must be applied to the __streams_offsets topics to persist the new target assignment.
         */
        private final List<CoordinatorRecord> records;

        /**
         * The new target assignment for the group.
         */
        private final Map<String, org.apache.kafka.coordinator.group.streams.Assignment> targetAssignment;

        TargetAssignmentResult(
            List<CoordinatorRecord> records,
            Map<String, org.apache.kafka.coordinator.group.streams.Assignment> targetAssignment
        ) {
            Objects.requireNonNull(records);
            Objects.requireNonNull(targetAssignment);
            this.records = records;
            this.targetAssignment = targetAssignment;
        }

        /**
         * @return The records.
         */
        public List<CoordinatorRecord> records() {
            return records;
        }

        /**
         * @return The target assignment.
         */
        public Map<String, org.apache.kafka.coordinator.group.streams.Assignment> targetAssignment() {
            return targetAssignment;
        }
    }

    /**
     * The group id.
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
     * The members in the group.
     */
    private Map<String, StreamsGroupMember> members = Collections.emptyMap();

    /**
     * The subscription metadata.
     */
    private Map<String, org.apache.kafka.coordinator.group.streams.TopicMetadata> subscriptionMetadata = Collections.emptyMap();

    /**
     * The existing target assignment.
     */
    private Map<String, org.apache.kafka.coordinator.group.streams.Assignment> targetAssignment = Collections.emptyMap();

    /**
     * The topology.
     */
    private StreamsTopology topology;

    /**
     * The members which have been updated or deleted. Deleted members are signaled by a null value.
     */
    private final Map<String, StreamsGroupMember> updatedMembers = new HashMap<>();

    /**
     * The static members in the group.
     */
    private Map<String, String> staticMembers = new HashMap<>();

    /**
     * Constructs the object.
     *
     * @param groupId    The group id.
     * @param groupEpoch The group epoch to compute a target assignment for.
     * @param assignor   The assignor to use to compute the target assignment.
     */
    public TargetAssignmentBuilder(
        String groupId,
        int groupEpoch,
        TaskAssignor assignor
    ) {
        this.groupId = Objects.requireNonNull(groupId);
        this.groupEpoch = groupEpoch;
        this.assignor = Objects.requireNonNull(assignor);
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
     * Adds the subscription metadata to use.
     *
     * @param subscriptionMetadata The subscription metadata.
     * @return This object.
     */
    public TargetAssignmentBuilder withSubscriptionMetadata(
        Map<String, org.apache.kafka.coordinator.group.streams.TopicMetadata> subscriptionMetadata
    ) {
        this.subscriptionMetadata = subscriptionMetadata;
        return this;
    }

    /**
     * Adds the existing target assignment.
     *
     * @param targetAssignment The existing target assignment.
     * @return This object.
     */
    public TargetAssignmentBuilder withTargetAssignment(
        Map<String, org.apache.kafka.coordinator.group.streams.Assignment> targetAssignment
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
        StreamsTopology topology
    ) {
        this.topology = topology;
        return this;
    }


    /**
     * Adds or updates a member. This is useful when the updated member is not yet materialized in memory.
     *
     * @param memberId The member id.
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
     * @param memberId The member id.
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
            targetAssignment.getOrDefault(memberId, org.apache.kafka.coordinator.group.streams.Assignment.EMPTY)
        )));

        // Update the member spec if updated or deleted members.
        updatedMembers.forEach((memberId, updatedMemberOrNull) -> {
            if (updatedMemberOrNull == null) {
                memberSpecs.remove(memberId);
            } else {
                org.apache.kafka.coordinator.group.streams.Assignment assignment = targetAssignment.getOrDefault(memberId,
                    org.apache.kafka.coordinator.group.streams.Assignment.EMPTY);

                // A new static member joins and needs to replace an existing departed one.
                if (updatedMemberOrNull.instanceId() != null) {
                    String previousMemberId = staticMembers.get(updatedMemberOrNull.instanceId());
                    if (previousMemberId != null && !previousMemberId.equals(memberId)) {
                        assignment = targetAssignment.getOrDefault(previousMemberId,
                            org.apache.kafka.coordinator.group.streams.Assignment.EMPTY);
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
        if (topology != null) {
            newGroupAssignment = assignor.assign(
                new GroupSpecImpl(
                    Collections.unmodifiableMap(memberSpecs),
                    new ArrayList<>(topology.subtopologies().keySet()),
                    new HashMap<>()
                ),
                new TopologyMetadata(subscriptionMetadata, topology)
            );
        } else {
            newGroupAssignment = new GroupAssignment(memberSpecs.keySet().stream().collect(Collectors.toMap(x -> x, x -> MemberAssignment.empty())));
        }

        // Compute delta from previous to new target assignment and create the
        // relevant records.
        List<CoordinatorRecord> records = new ArrayList<>();
        Map<String, org.apache.kafka.coordinator.group.streams.Assignment> newTargetAssignment = new HashMap<>();

        memberSpecs.keySet().forEach(memberId -> {
            org.apache.kafka.coordinator.group.streams.Assignment oldMemberAssignment = targetAssignment.get(memberId);
            org.apache.kafka.coordinator.group.streams.Assignment newMemberAssignment = newMemberAssignment(newGroupAssignment, memberId);

            newTargetAssignment.put(memberId, newMemberAssignment);

            if (oldMemberAssignment == null) {
                // If the member had no assignment, we always create a record for it.
                records.add(CoordinatorStreamsRecordHelpers.newStreamsGroupTargetAssignmentRecord(
                    groupId,
                    memberId,
                    newMemberAssignment.activeTasks(),
                    newMemberAssignment.standbyTasks(),
                    newMemberAssignment.warmupTasks()
                ));
            } else {
                // If the member had an assignment, we only create a record if the
                // new assignment is different.
                if (!newMemberAssignment.equals(oldMemberAssignment)) {
                    records.add(CoordinatorStreamsRecordHelpers.newStreamsGroupTargetAssignmentRecord(
                        groupId,
                        memberId,
                        newMemberAssignment.activeTasks(),
                        newMemberAssignment.standbyTasks(),
                        newMemberAssignment.warmupTasks()
                    ));
                }
            }
        });

        // Bump the target assignment epoch.
        records.add(CoordinatorStreamsRecordHelpers.newStreamsGroupTargetAssignmentEpochRecord(groupId, groupEpoch));

        return new TargetAssignmentResult(records, newTargetAssignment);
    }

    private org.apache.kafka.coordinator.group.streams.Assignment newMemberAssignment(
        org.apache.kafka.coordinator.group.taskassignor.GroupAssignment newGroupAssignment,
        String memberId
    ) {
        MemberAssignment newMemberAssignment = newGroupAssignment.members().get(memberId);
        if (newMemberAssignment != null) {
            return new org.apache.kafka.coordinator.group.streams.Assignment(
                newMemberAssignment.activeTasks(),
                newMemberAssignment.standbyTasks(),
                newMemberAssignment.warmupTasks()
            );
        } else {
            return org.apache.kafka.coordinator.group.streams.Assignment.EMPTY;
        }
    }

    static AssignmentMemberSpec createAssignmentMemberSpec(
        StreamsGroupMember member,
        Assignment targetAssignment
    ) {
        return new AssignmentMemberSpec(
            Optional.ofNullable(member.instanceId()),
            Optional.ofNullable(member.rackId()),
            targetAssignment.activeTasks(),
            targetAssignment.standbyTasks(),
            targetAssignment.warmupTasks(),
            member.processId(),
            member.clientTags(),
            Collections.emptyMap() // TODO: TaskOffsets is missing
        );
    }
}
