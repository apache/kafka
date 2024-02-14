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
import org.apache.kafka.coordinator.group.Record;
import org.apache.kafka.coordinator.group.assignor.AssignmentMemberSpec;
import org.apache.kafka.coordinator.group.assignor.AssignmentSpec;
import org.apache.kafka.coordinator.group.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.assignor.MemberAssignment;
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
 * does not yield a tombstone for removed members.
 */
public class TargetAssignmentBuilder {
    /**
     * The assignment result returned by {{@link TargetAssignmentBuilder#build()}}.
     */
    public static class TargetAssignmentResult {
        /**
         * The records that must be applied to the __consumer_offsets
         * topics to persist the new target assignment.
         */
        private final List<Record> records;

        /**
         * The new target assignment for the group.
         */
        private final Map<String, Assignment> targetAssignment;

        TargetAssignmentResult(
            List<org.apache.kafka.coordinator.group.Record> records,
            Map<String, Assignment> targetAssignment
        ) {
            Objects.requireNonNull(records);
            Objects.requireNonNull(targetAssignment);
            this.records = records;
            this.targetAssignment = targetAssignment;
        }

        /**
         * @return The records.
         */
        public List<Record> records() {
            return records;
        }

        /**
         * @return The target assignment.
         */
        public Map<String, Assignment> targetAssignment() {
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
    private final PartitionAssignor assignor;

    /**
     * The members in the group.
     */
    private Map<String, ConsumerGroupMember> members = Collections.emptyMap();

    /**
     * The subscription metadata.
     */
    private Map<String, TopicMetadata> subscriptionMetadata = Collections.emptyMap();

    /**
     * The existing target assignment.
     */
    private Map<String, Assignment> targetAssignment = Collections.emptyMap();

    /**
     * The members which have been updated or deleted. Deleted members
     * are signaled by a null value.
     */
    private final Map<String, ConsumerGroupMember> updatedMembers = new HashMap<>();

    /**
     * The static members in the group.
     */
    private Map<String, String> staticMembers = new HashMap<>();

    /**
     * Constructs the object.
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
     * Adds all the existing members.
     *
     * @param members   The existing members in the consumer group.
     * @return This object.
     */
    public TargetAssignmentBuilder withMembers(
        Map<String, ConsumerGroupMember> members
    ) {
        this.members = members;
        return this;
    }

    /**
     * Adds all the existing static members.
     *
     * @param staticMembers   The existing static members in the consumer group.
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
     * @param subscriptionMetadata  The subscription metadata.
     * @return This object.
     */
    public TargetAssignmentBuilder withSubscriptionMetadata(
        Map<String, TopicMetadata> subscriptionMetadata
    ) {
        this.subscriptionMetadata = subscriptionMetadata;
        return this;
    }

    /**
     * Adds the existing target assignment.
     *
     * @param targetAssignment   The existing target assignment.
     * @return This object.
     */
    public TargetAssignmentBuilder withTargetAssignment(
        Map<String, Assignment> targetAssignment
    ) {
        this.targetAssignment = targetAssignment;
        return this;
    }

    /**
     * Adds or updates a member. This is useful when the updated member is
     * not yet materialized in memory.
     *
     * @param memberId  The member id.
     * @param member    The member to add or update.
     * @return This object.
     */
    public TargetAssignmentBuilder addOrUpdateMember(
        String memberId,
        ConsumerGroupMember member
    ) {
        this.updatedMembers.put(memberId, member);
        return this;
    }

    /**
     * Removes a member. This is useful when the removed member
     * is not yet materialized in memory.
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
     * @return A TargetAssignmentResult which contains the records to update
     *         the existing target assignment.
     * @throws PartitionAssignorException if the target assignment cannot be computed.
     */
    public TargetAssignmentResult build() throws PartitionAssignorException {
        Map<String, AssignmentMemberSpec> memberSpecs = new HashMap<>();

        // Prepare the member spec for all members.
        members.forEach((memberId, member) -> memberSpecs.put(memberId, createAssignmentMemberSpec(
            member,
            targetAssignment.getOrDefault(memberId, Assignment.EMPTY),
            subscriptionMetadata
        )));

        // Update the member spec if updated or deleted members.
        updatedMembers.forEach((memberId, updatedMemberOrNull) -> {
            if (updatedMemberOrNull == null) {
                memberSpecs.remove(memberId);
            } else {
                ConsumerGroupMember member = members.get(memberId);
                Assignment assignment;
                // A new static member joins and needs to replace an existing departed one.
                if (member == null && staticMembers.containsKey(updatedMemberOrNull.instanceId())) {
                    assignment = targetAssignment.getOrDefault(staticMembers.get(updatedMemberOrNull.instanceId()), Assignment.EMPTY);
                } else {
                    assignment = targetAssignment.getOrDefault(memberId, Assignment.EMPTY);
                }
                memberSpecs.put(memberId, createAssignmentMemberSpec(
                    updatedMemberOrNull,
                    assignment,
                    subscriptionMetadata
                ));
            }
        });

        // Prepare the topic metadata.
        Map<Uuid, TopicMetadata> topicMetadataMap = new HashMap<>();
        subscriptionMetadata.forEach((topicName, topicMetadata) ->
            topicMetadataMap.put(
                topicMetadata.id(),
                topicMetadata
            )
        );

        // Compute the assignment.
        GroupAssignment newGroupAssignment = assignor.assign(
            new AssignmentSpec(Collections.unmodifiableMap(memberSpecs)),
            new SubscribedTopicMetadata(topicMetadataMap)
        );

        // Compute delta from previous to new target assignment and create the
        // relevant records.
        List<Record> records = new ArrayList<>();
        Map<String, Assignment> newTargetAssignment = new HashMap<>();

        memberSpecs.keySet().forEach(memberId -> {
            Assignment oldMemberAssignment = targetAssignment.get(memberId);
            Assignment newMemberAssignment = newMemberAssignment(newGroupAssignment, memberId);

            newTargetAssignment.put(memberId, newMemberAssignment);

            if (oldMemberAssignment == null) {
                // If the member had no assignment, we always create a record for it.
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

        // Bump the target assignment epoch.
        records.add(newTargetAssignmentEpochRecord(groupId, groupEpoch));

        return new TargetAssignmentResult(records, newTargetAssignment);
    }

    private Assignment newMemberAssignment(
        GroupAssignment newGroupAssignment,
        String memberId
    ) {
        MemberAssignment newMemberAssignment = newGroupAssignment.members().get(memberId);
        if (newMemberAssignment != null) {
            return new Assignment(newMemberAssignment.targetPartitions());
        } else {
            return Assignment.EMPTY;
        }
    }

    public static AssignmentMemberSpec createAssignmentMemberSpec(
        ConsumerGroupMember member,
        Assignment targetAssignment,
        Map<String, TopicMetadata> subscriptionMetadata
    ) {
        Set<Uuid> subscribedTopics = new HashSet<>();
        member.subscribedTopicNames().forEach(topicName -> {
            TopicMetadata topicMetadata = subscriptionMetadata.get(topicName);
            if (topicMetadata != null) {
                subscribedTopics.add(topicMetadata.id());
            }
        });

        return new AssignmentMemberSpec(
            Optional.ofNullable(member.instanceId()),
            Optional.ofNullable(member.rackId()),
            subscribedTopics,
            targetAssignment.partitions()
        );
    }
}
