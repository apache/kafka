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
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.modern.consumer.ResolvedRegularExpression;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupMember;
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
public abstract class TargetAssignmentBuilder<T extends ModernGroupMember, U extends TargetAssignmentBuilder<T, U>> {

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

    public static class ConsumerTargetAssignmentBuilder extends TargetAssignmentBuilder<ConsumerGroupMember, ConsumerTargetAssignmentBuilder> {

        /**
         * The resolved regular expressions.
         */
        private Map<String, ResolvedRegularExpression> resolvedRegularExpressions = Collections.emptyMap();

        public ConsumerTargetAssignmentBuilder(
            String groupId,
            int groupEpoch,
            PartitionAssignor assignor
        ) {
            super(groupId, groupEpoch, assignor);
        }

        /**
         * Adds all the existing resolved regular expressions.
         *
         * @param resolvedRegularExpressions The resolved regular expressions.
         * @return This object.
         */
        public ConsumerTargetAssignmentBuilder withResolvedRegularExpressions(
            Map<String, ResolvedRegularExpression> resolvedRegularExpressions
        ) {
            this.resolvedRegularExpressions = resolvedRegularExpressions;
            return self();
        }

        @Override
        protected ConsumerTargetAssignmentBuilder self() {
            return this;
        }

        @Override
        protected CoordinatorRecord newTargetAssignmentRecord(
            String groupId,
            String memberId,
            Map<Uuid, Set<Integer>> partitions
        ) {
            return GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(
                groupId,
                memberId,
                partitions
            );
        }

        @Override
        protected CoordinatorRecord newTargetAssignmentEpochRecord(String groupId, int assignmentEpoch) {
            return GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(
                groupId,
                assignmentEpoch
            );
        }

        @Override
        protected MemberSubscriptionAndAssignmentImpl newMemberSubscriptionAndAssignment(
            ConsumerGroupMember member,
            Assignment memberAssignment,
            TopicIds.TopicResolver topicResolver
        ) {
            Set<String> subscriptions = member.subscribedTopicNames();

            // Check whether the member is also subscribed to a regular expression. If it is,
            // create the union of the two subscriptions.
            String subscribedTopicRegex = member.subscribedTopicRegex();
            if (subscribedTopicRegex != null && !subscribedTopicRegex.isEmpty()) {
                ResolvedRegularExpression resolvedRegularExpression = resolvedRegularExpressions.get(subscribedTopicRegex);
                if (resolvedRegularExpression != null) {
                    if (subscriptions.isEmpty()) {
                        subscriptions = resolvedRegularExpression.topics;
                    } else if (!resolvedRegularExpression.topics.isEmpty()) {
                        // We only use a UnionSet when the member uses both type of subscriptions. The
                        // protocol allows it. However, the Apache Kafka Consumer does not support it.
                        // Other clients such as librdkafka may support it.
                        subscriptions = new UnionSet<>(subscriptions, resolvedRegularExpression.topics);
                    }
                }
            }

            return new MemberSubscriptionAndAssignmentImpl(
                Optional.ofNullable(member.rackId()),
                Optional.ofNullable(member.instanceId()),
                new TopicIds(subscriptions, topicResolver),
                memberAssignment
            );
        }
    }

    public static class ShareTargetAssignmentBuilder extends TargetAssignmentBuilder<ShareGroupMember, ShareTargetAssignmentBuilder> {
        public ShareTargetAssignmentBuilder(
            String groupId,
            int groupEpoch,
            PartitionAssignor assignor
        ) {
            super(groupId, groupEpoch, assignor);
        }

        @Override
        protected ShareTargetAssignmentBuilder self() {
            return this;
        }

        @Override
        protected CoordinatorRecord newTargetAssignmentRecord(
            String groupId,
            String memberId,
            Map<Uuid, Set<Integer>> partitions
        ) {
            return GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentRecord(
                groupId,
                memberId,
                partitions
            );
        }

        @Override
        protected CoordinatorRecord newTargetAssignmentEpochRecord(String groupId, int assignmentEpoch) {
            return GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentEpochRecord(
                groupId,
                assignmentEpoch
            );
        }

        @Override
        protected MemberSubscriptionAndAssignmentImpl newMemberSubscriptionAndAssignment(
            ShareGroupMember member,
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
    public U withMembers(
        Map<String, T> members
    ) {
        this.members = members;
        return self();
    }

    /**
     * Adds all the existing static members.
     *
     * @param staticMembers   The existing static members in the consumer group.
     * @return This object.
     */
    public U withStaticMembers(
        Map<String, String> staticMembers
    ) {
        this.staticMembers = staticMembers;
        return self();
    }

    /**
     * Adds the subscription metadata to use.
     *
     * @param subscriptionMetadata  The subscription metadata.
     * @return This object.
     */
    public U withSubscriptionMetadata(
        Map<String, TopicMetadata> subscriptionMetadata
    ) {
        this.subscriptionMetadata = subscriptionMetadata;
        return self();
    }

    /**
     * Adds the subscription type in use.
     *
     * @param subscriptionType  Subscription type of the group.
     * @return This object.
     */
    public U withSubscriptionType(
        SubscriptionType subscriptionType
    ) {
        this.subscriptionType = subscriptionType;
        return self();
    }

    /**
     * Adds the existing target assignment.
     *
     * @param targetAssignment   The existing target assignment.
     * @return This object.
     */
    public U withTargetAssignment(
        Map<String, Assignment> targetAssignment
    ) {
        this.targetAssignment = targetAssignment;
        return self();
    }

    /**
     * Adds the existing topic partition assignments.
     *
     * @param invertedTargetAssignment   The reverse lookup map of the current target assignment.
     * @return This object.
     */
    public U withInvertedTargetAssignment(
        Map<Uuid, Map<Integer, String>> invertedTargetAssignment
    ) {
        this.invertedTargetAssignment = invertedTargetAssignment;
        return self();
    }

    /**
     * Adds the topics image.
     *
     * @param topicsImage    The topics image.
     * @return This object.
     */
    public U withTopicsImage(
        TopicsImage topicsImage
    ) {
        this.topicsImage = topicsImage;
        return self();
    }

    /**
     * Adds or updates a member. This is useful when the updated member is
     * not yet materialized in memory.
     *
     * @param memberId  The member id.
     * @param member    The member to add or update.
     * @return This object.
     */
    public U addOrUpdateMember(
        String memberId,
        T member
    ) {
        this.updatedMembers.put(memberId, member);
        return self();
    }

    /**
     * Removes a member. This is useful when the removed member
     * is not yet materialized in memory.
     *
     * @param memberId The member id.
     * @return This object.
     */
    public U removeMember(
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
            memberSpecs.put(memberId, newMemberSubscriptionAndAssignment(
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

                memberSpecs.put(memberId, newMemberSubscriptionAndAssignment(
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
                records.add(newTargetAssignmentRecord(
                    groupId,
                    memberId,
                    newMemberAssignment.partitions()
                ));
            }
        }

        // Bump the target assignment epoch.
        records.add(newTargetAssignmentEpochRecord(groupId, groupEpoch));

        return new TargetAssignmentResult(records, newGroupAssignment.members());
    }

    protected abstract U self();

    protected abstract CoordinatorRecord newTargetAssignmentRecord(
        String groupId,
        String memberId,
        Map<Uuid, Set<Integer>> partitions
    );

    protected abstract CoordinatorRecord newTargetAssignmentEpochRecord(
        String groupId,
        int assignmentEpoch
    );

    protected abstract MemberSubscriptionAndAssignmentImpl newMemberSubscriptionAndAssignment(
        T member,
        Assignment memberAssignment,
        TopicIds.TopicResolver topicResolver
    );

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
}
