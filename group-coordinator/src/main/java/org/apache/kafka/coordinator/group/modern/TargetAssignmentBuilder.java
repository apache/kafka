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
package org.apache.kafka.coordinator.group.modern;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.image.TopicsImage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

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
public class TargetAssignmentBuilder<T extends ModernGroupMember> {

    /**
     * The assignment result returned by {{@link TargetAssignmentBuilder#build()}}.
     */
    public static class TargetAssignmentResult {
        /**
         * The records that must be applied to the __consumer_offsets
         * topics to persist the new target assignment.
         */
        private final List<CoordinatorRecord> records;

        /**
         * The new target assignment for the group.
         */
        private final Map<String, MemberAssignment> targetAssignment;

        TargetAssignmentResult(
            List<CoordinatorRecord> records,
            Map<String, MemberAssignment> targetAssignment
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
        public Map<String, MemberAssignment> targetAssignment() {
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
    private Map<String, T> members = Collections.emptyMap();

    /**
     * The subscription metadata.
     */
    private Map<String, TopicMetadata> subscriptionMetadata = Collections.emptyMap();

    /**
     * The subscription type of the consumer group.
     */
    private SubscriptionType subscriptionType;

    /**
     * The existing target assignment.
     */
    private Map<String, Assignment> targetAssignment = Collections.emptyMap();

    /**
     * Reverse lookup map representing topic partitions with
     * their current member assignments.
     */
    private Map<Uuid, Map<Integer, String>> invertedTargetAssignment = Collections.emptyMap();

    /**
     * The topics image.
     */
    private TopicsImage topicsImage = TopicsImage.EMPTY;

    /**
     * The members which have been updated or deleted. Deleted members
     * are signaled by a null value.
     */
    private final Map<String, T> updatedMembers = new HashMap<>();

    /**
     * The static members in the group.
     */
    private Map<String, String> staticMembers = new HashMap<>();

    public interface TargetAssignmentRecordBuilder {
        CoordinatorRecord build(
            final String groupId,
            final String memberId,
            final Map<Uuid, Set<Integer>> partitions
       );
    }

    public interface TargetAssignmentEpochRecordBuilder {
        CoordinatorRecord build(
            final String groupId,
            final int assignmentEpoch
        );
    }

    private TargetAssignmentRecordBuilder targetAssignmentRecordBuilder =
        GroupCoordinatorRecordHelpers::newConsumerGroupTargetAssignmentRecord;

    private TargetAssignmentEpochRecordBuilder targetAssignmentEpochRecordBuilder =
        GroupCoordinatorRecordHelpers::newConsumerGroupTargetAssignmentEpochRecord;

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
    public TargetAssignmentBuilder<T> withMembers(
        Map<String, T> members
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
    public TargetAssignmentBuilder<T> withStaticMembers(
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
    public TargetAssignmentBuilder<T> withSubscriptionMetadata(
        Map<String, TopicMetadata> subscriptionMetadata
    ) {
        this.subscriptionMetadata = subscriptionMetadata;
        return this;
    }

    /**
     * Adds the subscription type in use.
     *
     * @param subscriptionType  Subscription type of the group.
     * @return This object.
     */
    public TargetAssignmentBuilder<T> withSubscriptionType(
        SubscriptionType subscriptionType
    ) {
        this.subscriptionType = subscriptionType;
        return this;
    }

    /**
     * Adds the existing target assignment.
     *
     * @param targetAssignment   The existing target assignment.
     * @return This object.
     */
    public TargetAssignmentBuilder<T> withTargetAssignment(
        Map<String, Assignment> targetAssignment
    ) {
        this.targetAssignment = targetAssignment;
        return this;
    }

    /**
     * Adds the existing topic partition assignments.
     *
     * @param invertedTargetAssignment   The reverse lookup map of the current target assignment.
     * @return This object.
     */
    public TargetAssignmentBuilder<T> withInvertedTargetAssignment(
        Map<Uuid, Map<Integer, String>> invertedTargetAssignment
    ) {
        this.invertedTargetAssignment = invertedTargetAssignment;
        return this;
    }

    /**
     * Adds the topics image.
     *
     * @param topicsImage    The topics image.
     * @return This object.
     */
    public TargetAssignmentBuilder<T> withTopicsImage(
        TopicsImage topicsImage
    ) {
        this.topicsImage = topicsImage;
        return this;
    }

    public TargetAssignmentBuilder<T> withTargetAssignmentRecordBuilder(
        TargetAssignmentRecordBuilder targetAssignmentRecordBuilder
    ) {
        this.targetAssignmentRecordBuilder = targetAssignmentRecordBuilder;
        return this;
    }

    public TargetAssignmentBuilder<T> withTargetAssignmentEpochRecordBuilder(
        TargetAssignmentEpochRecordBuilder targetAssignmentEpochRecordBuilder
    ) {
        this.targetAssignmentEpochRecordBuilder = targetAssignmentEpochRecordBuilder;
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
    public TargetAssignmentBuilder<T> addOrUpdateMember(
        String memberId,
        T member
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
    public TargetAssignmentBuilder<T> removeMember(
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
        Map<String, MemberSubscriptionAndAssignmentImpl> memberSpecs = new HashMap<>();
        TopicIds.TopicResolver topicResolver = new TopicIds.CachedTopicResolver(topicsImage);

        // Prepare the member spec for all members.
        members.forEach((memberId, member) ->
            memberSpecs.put(memberId, createMemberSubscriptionAndAssignment(
                member,
                targetAssignment.getOrDefault(memberId, Assignment.EMPTY),
                topicResolver
            ))
        );

        // Update the member spec if updated or deleted members.
        updatedMembers.forEach((memberId, updatedMemberOrNull) -> {
            if (updatedMemberOrNull == null) {
                memberSpecs.remove(memberId);
            } else {
                Assignment assignment = targetAssignment.getOrDefault(memberId, Assignment.EMPTY);

                // A new static member joins and needs to replace an existing departed one.
                if (updatedMemberOrNull.instanceId() != null) {
                    String previousMemberId = staticMembers.get(updatedMemberOrNull.instanceId());
                    if (previousMemberId != null && !previousMemberId.equals(memberId)) {
                        assignment = targetAssignment.getOrDefault(previousMemberId, Assignment.EMPTY);
                    }
                }

                memberSpecs.put(memberId, createMemberSubscriptionAndAssignment(
                    updatedMemberOrNull,
                    assignment,
                    topicResolver
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
            new GroupSpecImpl(
                Collections.unmodifiableMap(memberSpecs),
                subscriptionType,
                invertedTargetAssignment
            ),
            new SubscribedTopicDescriberImpl(topicMetadataMap)
        );

        // Compute delta from previous to new target assignment and create the
        // relevant records.
        List<CoordinatorRecord> records = new ArrayList<>();

        for (String memberId : memberSpecs.keySet()) {
            Assignment oldMemberAssignment = targetAssignment.get(memberId);
            Assignment newMemberAssignment = newMemberAssignment(newGroupAssignment, memberId);

            if (!newMemberAssignment.equals(oldMemberAssignment)) {
                // If the member had no assignment or had a different assignment, we
                // create a record for the new assignment.
                records.add(targetAssignmentRecordBuilder.build(
                    groupId,
                    memberId,
                    newMemberAssignment.partitions()
                ));
            }
        }

        // Bump the target assignment epoch.
        records.add(targetAssignmentEpochRecordBuilder.build(groupId, groupEpoch));

        return new TargetAssignmentResult(records, newGroupAssignment.members());
    }

    private Assignment newMemberAssignment(
        GroupAssignment newGroupAssignment,
        String memberId
    ) {
        MemberAssignment newMemberAssignment = newGroupAssignment.members().get(memberId);
        if (newMemberAssignment != null) {
            return new Assignment(newMemberAssignment.partitions());
        } else {
            return Assignment.EMPTY;
        }
    }

    // private for testing
    static <T extends ModernGroupMember> MemberSubscriptionAndAssignmentImpl createMemberSubscriptionAndAssignment(
        T member,
        Assignment memberAssignment,
        TopicIds.TopicResolver topicResolver
    ) {
        return new MemberSubscriptionAndAssignmentImpl(
            Optional.ofNullable(member.rackId()),
            Optional.ofNullable(member.instanceId()),
            new TopicIds(member.subscribedTopicNames(), topicResolver),
            memberAssignment
        );
    }
}
