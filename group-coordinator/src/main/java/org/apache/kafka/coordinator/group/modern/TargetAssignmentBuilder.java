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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.group.TargetAssignmentMetadata;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.modern.consumer.ResolvedRegularExpression;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupMember;
import org.apache.kafka.coordinator.group.util.UnionSet;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Build a new Target Assignment based on the provided parameters.
 */
public abstract class TargetAssignmentBuilder<T extends ModernGroupMember, U extends TargetAssignmentBuilder<T, U>> {

    /**
     * The assignment result returned by {@link TargetAssignmentBuilder#build()}.
     *
     * @param targetAssignment         The new target assignment for the group.
     * @param targetAssignmentMetadata The new target assignment metadata.
     */
    public record TargetAssignmentResult(
        Map<String, Assignment> targetAssignment,
        TargetAssignmentMetadata targetAssignmentMetadata
    ) {
        public TargetAssignmentResult {
            Objects.requireNonNull(targetAssignment);
            Objects.requireNonNull(targetAssignmentMetadata);
        }
    }

    public static class ConsumerTargetAssignmentBuilder extends TargetAssignmentBuilder<ConsumerGroupMember, ConsumerTargetAssignmentBuilder> {

        /**
         * The resolved regular expressions.
         */
        private Map<String, ResolvedRegularExpression> resolvedRegularExpressions = Map.of();

        public ConsumerTargetAssignmentBuilder(
            int groupEpoch,
            PartitionAssignor assignor
        ) {
            super(groupEpoch, assignor);
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
                        subscriptions = resolvedRegularExpression.topics();
                    } else if (!resolvedRegularExpression.topics().isEmpty()) {
                        // We only use a UnionSet when the member uses both type of subscriptions. The
                        // protocol allows it. However, the Apache Kafka Consumer does not support it.
                        // Other clients such as librdkafka may support it.
                        subscriptions = new UnionSet<>(subscriptions, resolvedRegularExpression.topics());
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
            int groupEpoch,
            PartitionAssignor assignor
        ) {
            super(groupEpoch, assignor);
        }

        @Override
        protected ShareTargetAssignmentBuilder self() {
            return this;
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
    private final PartitionAssignor assignor;

    /**
     * The members in the group.
     */
    private Map<String, T> members = Map.of();

    /**
     * The subscription type of the consumer group.
     */
    private SubscriptionType subscriptionType;

    /**
     * The existing target assignment.
     */
    private Map<String, Assignment> targetAssignment = Map.of();

    /**
     * Reverse lookup map representing topic partitions with
     * their current member assignments.
     */
    private Map<Uuid, Map<Integer, String>> invertedTargetAssignment = Map.of();

    /**
     * The metadata image.
     */
    private CoordinatorMetadataImage metadataImage = CoordinatorMetadataImage.EMPTY;

    /**
     * Topic partition assignable map.
     */
    private Optional<Map<Uuid, Set<Integer>>> topicAssignablePartitionsMap = Optional.empty();

    /**
     * Constructs the object.
     *
     * @param groupEpoch    The group epoch to compute a target assignment for.
     * @param assignor      The assignor to use to compute the target assignment.
     */
    public TargetAssignmentBuilder(
        int groupEpoch,
        PartitionAssignor assignor
    ) {
        this.groupEpoch = groupEpoch;
        this.assignor = Objects.requireNonNull(assignor);
    }

    /**
     * Sets the time.
     *
     * @param time The time.
     * @return This object.
     */
    public U withTime(Time time) {
        this.time = time;
        return self();
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
     * Adds the metadata image.
     *
     * @param metadataImage    The metadata image.
     * @return This object.
     */
    public U withMetadataImage(
        CoordinatorMetadataImage metadataImage
    ) {
        this.metadataImage = metadataImage;
        return self();
    }

    public U withTopicAssignablePartitionsMap(
        Map<Uuid, Set<Integer>> topicAssignablePartitionsMap
    ) {
        this.topicAssignablePartitionsMap = Optional.of(topicAssignablePartitionsMap);
        return self();
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
        TopicIds.TopicResolver topicResolver = new TopicIds.CachedTopicResolver(metadataImage);

        // Prepare the member spec for all members.
        members.forEach((memberId, member) ->
            memberSpecs.put(memberId, newMemberSubscriptionAndAssignment(
                member,
                targetAssignment.getOrDefault(memberId, Assignment.EMPTY),
                topicResolver
            ))
        );

        // Compute the assignment.
        GroupAssignment newGroupAssignment = assignor.assign(
            new GroupSpecImpl(
                Collections.unmodifiableMap(memberSpecs),
                subscriptionType,
                invertedTargetAssignment,
                topicAssignablePartitionsMap
            ),
            new SubscribedTopicDescriberImpl(metadataImage)
        );

        Map<String, Assignment> newTargetAssignment = new HashMap<>();
        for (String memberId : memberSpecs.keySet()) {
            newTargetAssignment.put(memberId, newMemberAssignment(newGroupAssignment, memberId));
        }

        return new TargetAssignmentResult(
            newTargetAssignment,
            new TargetAssignmentMetadata(groupEpoch, time.milliseconds())
        );
    }

    protected abstract U self();

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
