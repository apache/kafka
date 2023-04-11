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
package org.apache.kafka.coordinator.group.consumer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.assignor.AssignmentMemberSpec;
import org.apache.kafka.coordinator.group.assignor.AssignmentSpec;
import org.apache.kafka.coordinator.group.assignor.AssignmentTopicMetadata;
import org.apache.kafka.coordinator.group.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignorException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.coordinator.group.RecordHelpers.newTargetAssignmentEpochRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newTargetAssignmentRecord;

/**
 * Build a new Target Assignment based on the provided parameters. As a result,
 * it yields the records that must be persisted to the log and the new member
 * assignments as a map.
 *
 * Records are only created for members which have a new target assignment. If
 * their assignment did not change, no new record is needed.
 *
 * When a member is deleted, it is assumed that its target assignment record
 * is deleted as part of the member deletion process. In other words, this class
 * does not yield a tombstone for remove members.
 */
public class TargetAssignmentBuilder {
    public static class TargetAssignmentResult {
        private final List<org.apache.kafka.coordinator.group.Record> records;
        private final Map<String, MemberAssignment> assignments;

        TargetAssignmentResult(
            List<org.apache.kafka.coordinator.group.Record> records,
            Map<String, MemberAssignment> assignments
        ) {
            Objects.requireNonNull(records);
            Objects.requireNonNull(assignments);
            this.records = records;
            this.assignments = assignments;
        }

        public List<org.apache.kafka.coordinator.group.Record> records() {
            return records;
        }

        public Map<String, MemberAssignment> assignments() {
            return assignments;
        }
    }

    private final String groupId;
    private final int groupEpoch;
    private final PartitionAssignor assignor;
    private Map<String, ConsumerGroupMember> members = Collections.emptyMap();
    private Map<String, TopicMetadata> subscriptionMetadata = Collections.emptyMap();
    private Map<String, MemberAssignment> assignments = Collections.emptyMap();
    private Map<String, ConsumerGroupMember> updatedMembers = new HashMap<>();

    /**
     * Instanciates the object.
     *
     * @param groupId       The group id.
     * @param groupEpoch    The group epoch to compute a target assignment for.
     * @param assignor      The assignor to use to compute the target assignment.
     */
    public TargetAssignmentBuilder(
        String groupId,
        int groupEpoch,
        PartitionAssignor assignor
    ) {
        this.groupId = Objects.requireNonNull(groupId);
        this.groupEpoch = groupEpoch;
        this.assignor = Objects.requireNonNull(assignor);
    }

    /**
     * Adds all the current members.
     *
     * @param members   The current members in the consumer groups.
     *
     * @return This object.
     */
    public TargetAssignmentBuilder withMembers(
        Map<String, ConsumerGroupMember> members
    ) {
        this.members = members;
        return this;
    }

    /**
     * Adds the subscription metadata to use.
     *
     * @param subscriptionMetadata  The subscription metadata.
     *
     * @return This object.
     */
    public TargetAssignmentBuilder withSubscriptionMetadata(
        Map<String, TopicMetadata> subscriptionMetadata
    ) {
        this.subscriptionMetadata = subscriptionMetadata;
        return this;
    }

    /**
     * Adds the current target assignments.
     *
     * @param assignments   The current assignments.
     *
     * @return This object.
     */
    public TargetAssignmentBuilder withTargetAssignments(
        Map<String, MemberAssignment> assignments
    ) {
        this.assignments = assignments;
        return this;
    }

    /**
     * Updates a member. This is useful when the updated member is
     * not yet materialized in memory.
     *
     * @param memberId      The member id.
     * @param updatedMember The updated member.
     *
     * @return This object.
     */
    public TargetAssignmentBuilder withUpdatedMember(
        String memberId,
        ConsumerGroupMember updatedMember
    ) {
        this.updatedMembers.put(memberId, updatedMember);
        return this;
    }

    /**
     * Removes a member. This is useful when the removed member
     * is not yet materialized in memory.
     *
     * @param memberId The member id.
     *
     * @return This object.
     */
    public TargetAssignmentBuilder withRemoveMembers(
        String memberId
    ) {
        return withUpdatedMember(memberId, null);
    }

    /**
     * Builds the new target assignment.
     *
     * @return A TargetAssignmentResult which contains the records to update
     *         the current target assignment.
     * @throws PartitionAssignorException
     */
    public TargetAssignmentResult build() throws PartitionAssignorException {
        Map<String, AssignmentMemberSpec> memberSpecs = new HashMap<>();
        members.forEach((memberId, member) -> addMemberSpec(
            memberSpecs,
            member,
            assignments.getOrDefault(memberId, MemberAssignment.EMPTY)
        ));

        updatedMembers.forEach((memberId, updatedMemberOrNull) -> {
            if (updatedMemberOrNull == null) {
                memberSpecs.remove(memberId);
            } else {
                addMemberSpec(
                    memberSpecs,
                    updatedMemberOrNull,
                    assignments.getOrDefault(memberId, MemberAssignment.EMPTY)
                );
            }
        });

        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        subscriptionMetadata.forEach((topicName, topicMetadata) ->
            topics.put(topicMetadata.id(), new AssignmentTopicMetadata(topicMetadata.numPartitions()))
        );

        // Compute the assignment.
        GroupAssignment newGroupAssignment = assignor.assign(new AssignmentSpec(
            Collections.unmodifiableMap(memberSpecs),
            Collections.unmodifiableMap(topics)
        ));

        List<org.apache.kafka.coordinator.group.Record> records = new ArrayList<>();
        Map<String, MemberAssignment> newTargetAssignment = new HashMap<>();

        // Compute delta from previous to new assignment and create the
        // relevant records.
        memberSpecs.keySet().forEach(memberId -> {
            MemberAssignment oldMemberAssignment = assignments.get(memberId);
            MemberAssignment newMemberAssignment = newMemberAssignment(newGroupAssignment, memberId);

            newTargetAssignment.put(memberId, newMemberAssignment);

            if (oldMemberAssignment == null) {
                // If the member had no assignment, we always create a record for him.
                records.add(newTargetAssignmentRecord(
                    groupId,
                    memberId,
                    newMemberAssignment.partitions()
                ));
            } else {
                // If the member had an assignment, we only create a record if the
                // new assignment is different.
                if (!newMemberAssignment.equals(oldMemberAssignment)) {
                    records.add(newTargetAssignmentRecord(
                        groupId,
                        memberId,
                        newMemberAssignment.partitions()
                    ));
                }
            }
        });

        // Bump the assignment epoch.
        records.add(newTargetAssignmentEpochRecord(groupId, groupEpoch));

        return new TargetAssignmentResult(records, newTargetAssignment);
    }

    private MemberAssignment newMemberAssignment(
        GroupAssignment newGroupAssignment,
        String memberId
    ) {
        org.apache.kafka.coordinator.group.assignor.MemberAssignment newMemberAssignment = newGroupAssignment.members().get(memberId);
        if (newMemberAssignment != null) {
            return new MemberAssignment(newMemberAssignment.targetPartitions());
        } else {
            return MemberAssignment.EMPTY;
        }
    }

    private void addMemberSpec(
        Map<String, AssignmentMemberSpec> members,
        ConsumerGroupMember member,
        MemberAssignment targetAssignment
    ) {
        Set<Uuid> subscribedTopics = new HashSet<>();
        member.subscribedTopicNames().forEach(topicName -> {
            TopicMetadata topicMetadata = subscriptionMetadata.get(topicName);
            if (topicMetadata != null) {
                subscribedTopics.add(topicMetadata.id());
            }
        });

        members.put(member.memberId(), new AssignmentMemberSpec(
            Optional.ofNullable(member.instanceId()),
            Optional.ofNullable(member.rackId()),
            subscribedTopics,
            targetAssignment.partitions()
        ));
    }
}
