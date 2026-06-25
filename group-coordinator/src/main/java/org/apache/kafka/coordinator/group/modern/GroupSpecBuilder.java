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
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.modern.consumer.ResolvedRegularExpression;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupMember;
import org.apache.kafka.coordinator.group.util.UnionSet;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Builds the {@link GroupSpec} describing the members of a modern group and their existing assignments.
 */
public abstract class GroupSpecBuilder<T extends ModernGroupMember, U extends GroupSpecBuilder<T, U>> {

    public static class ConsumerGroupSpecBuilder extends GroupSpecBuilder<ConsumerGroupMember, ConsumerGroupSpecBuilder> {

        /**
         * The resolved regular expressions.
         */
        private Map<String, ResolvedRegularExpression> resolvedRegularExpressions = Map.of();

        /**
         * Adds all the existing resolved regular expressions.
         *
         * @param resolvedRegularExpressions The resolved regular expressions.
         * @return This object.
         */
        public ConsumerGroupSpecBuilder withResolvedRegularExpressions(
            Map<String, ResolvedRegularExpression> resolvedRegularExpressions
        ) {
            this.resolvedRegularExpressions = resolvedRegularExpressions;
            return self();
        }

        @Override
        protected ConsumerGroupSpecBuilder self() {
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

    public static class ShareGroupSpecBuilder extends GroupSpecBuilder<ShareGroupMember, ShareGroupSpecBuilder> {
        @Override
        protected ShareGroupSpecBuilder self() {
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
     * Whether the {@link GroupSpec} produced by {@link GroupSpecBuilder#build()} will be used on a
     * background thread. When {@code true}, {@link GroupSpecBuilder#build()} takes copies of any
     * mutable collections, so that subsequent changes are not visible to the assignor.
     */
    private boolean assignorOffload;

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
     * Sets whether the {@link GroupSpec} produced by {@link GroupSpecBuilder#build()} will be used
     * on a background thread. When {@code true}, {@link GroupSpecBuilder#build()} takes copies of
     * any mutable collections, so that subsequent changes are not visible to the assignor.
     *
     * @param assignorOffload Whether the produced {@link GroupSpec} will be consumed on a
     *                        background thread.
     * @return This object.
     */
    public U withAssignorOffload(boolean assignorOffload) {
        this.assignorOffload = assignorOffload;
        return self();
    }

    /**
     * Builds the {@link GroupSpec} to be passed to the assignor.
     *
     * @return The {@link GroupSpec} describing the members and their existing assignments.
     */
    public GroupSpec build() {
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

        Map<Uuid, Map<Integer, String>> invertedTargetAssignment = this.invertedTargetAssignment;
        Optional<Map<Uuid, Set<Integer>>> topicAssignablePartitionsMap = this.topicAssignablePartitionsMap;
        if (assignorOffload) {
            Map<Uuid, Map<Integer, String>> invertedTargetAssignmentCopy = new HashMap<>(invertedTargetAssignment.size());
            for (Map.Entry<Uuid, Map<Integer, String>> entry : invertedTargetAssignment.entrySet()) {
                invertedTargetAssignmentCopy.put(entry.getKey(), Map.copyOf(entry.getValue()));
            }
            invertedTargetAssignment = invertedTargetAssignmentCopy;

            if (topicAssignablePartitionsMap.isPresent()) {
                Map<Uuid, Set<Integer>> topicAssignablePartitionsMapCopy = new HashMap<>(topicAssignablePartitionsMap.get().size());
                for (Map.Entry<Uuid, Set<Integer>> entry : topicAssignablePartitionsMap.get().entrySet()) {
                    topicAssignablePartitionsMapCopy.put(entry.getKey(), Set.copyOf(entry.getValue()));
                }
                topicAssignablePartitionsMap = Optional.of(topicAssignablePartitionsMapCopy);
            }
        }

        return new GroupSpecImpl(
            Collections.unmodifiableMap(memberSpecs),
            subscriptionType,
            invertedTargetAssignment,
            topicAssignablePartitionsMap
        );
    }

    protected abstract U self();

    protected abstract MemberSubscriptionAndAssignmentImpl newMemberSubscriptionAndAssignment(
        T member,
        Assignment memberAssignment,
        TopicIds.TopicResolver topicResolver
    );
}
