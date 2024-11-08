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
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.coordinator.group.Group;
import org.apache.kafka.coordinator.group.Utils;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineInteger;
import org.apache.kafka.timeline.TimelineObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HETEROGENEOUS;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;

/**
 * The abstract group provides definitions for the consumer and share group.
 */
public abstract class ModernGroup<T extends ModernGroupMember> implements Group {

    public static class DeadlineAndEpoch {
        static final DeadlineAndEpoch EMPTY = new DeadlineAndEpoch(0L, 0);

        public final long deadlineMs;
        public final int epoch;

        DeadlineAndEpoch(long deadlineMs, int epoch) {
            this.deadlineMs = deadlineMs;
            this.epoch = epoch;
        }
    }

    /**
     * The snapshot registry.
     */
    protected final SnapshotRegistry snapshotRegistry;

    /**
     * The group id.
     */
    protected final String groupId;

    /**
     * The group epoch. The epoch is incremented whenever the subscriptions
     * are updated and it will trigger the computation of a new assignment
     * for the group.
     */
    protected final TimelineInteger groupEpoch;

    /**
     * The group members.
     */
    protected final TimelineHashMap<String, T> members;

    /**
     * The number of subscribers or regular expressions per topic.
     */
    protected final TimelineHashMap<String, Integer> subscribedTopicNames;

    /**
     * The metadata associated with each subscribed topic name.
     */
    protected final TimelineHashMap<String, TopicMetadata> subscribedTopicMetadata;

    /**
     * The group's subscription type.
     * This value is set to Homogeneous by default.
     */
    protected final TimelineObject<SubscriptionType> subscriptionType;

    /**
     * The target assignment epoch. An assignment epoch smaller than the group epoch
     * means that a new assignment is required. The assignment epoch is updated when
     * a new assignment is installed.
     */
    protected final TimelineInteger targetAssignmentEpoch;

    /**
     * The target assignment per member id.
     */
    protected final TimelineHashMap<String, Assignment> targetAssignment;

    /**
     * Reverse lookup map representing topic partitions with
     * their current member assignments.
     */
    private final TimelineHashMap<Uuid, TimelineHashMap<Integer, String>> invertedTargetAssignment;

    /**
     * The metadata refresh deadline. It consists of a timestamp in milliseconds together with
     * the group epoch at the time of setting it. The metadata refresh time is considered as a
     * soft state (read that it is not stored in a timeline data structure). It is like this
     * because it is not persisted to the log. The group epoch is here to ensure that the
     * metadata refresh deadline is invalidated if the group epoch does not correspond to
     * the current group epoch. This can happen if the metadata refresh deadline is updated
     * after having refreshed the metadata but the write operation failed. In this case, the
     * time is not automatically rolled back.
     */
    protected DeadlineAndEpoch metadataRefreshDeadline = DeadlineAndEpoch.EMPTY;

    protected ModernGroup(
        SnapshotRegistry snapshotRegistry,
        String groupId
    ) {
        this.snapshotRegistry = Objects.requireNonNull(snapshotRegistry);
        this.groupId = Objects.requireNonNull(groupId);
        this.groupEpoch = new TimelineInteger(snapshotRegistry);
        this.members = new TimelineHashMap<>(snapshotRegistry, 0);
        this.subscribedTopicNames = new TimelineHashMap<>(snapshotRegistry, 0);
        this.subscribedTopicMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
        this.subscriptionType = new TimelineObject<>(snapshotRegistry, HOMOGENEOUS);
        this.targetAssignmentEpoch = new TimelineInteger(snapshotRegistry);
        this.targetAssignment = new TimelineHashMap<>(snapshotRegistry, 0);
        this.invertedTargetAssignment = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    /**
     * @return the group formatted as a list group response based on the committed offset.
     */
    public ListGroupsResponseData.ListedGroup asListedGroup(long committedOffset) {
        return new ListGroupsResponseData.ListedGroup()
            .setGroupId(groupId)
            .setProtocolType(protocolType())
            .setGroupState(stateAsString(committedOffset))
            .setGroupType(type().toString());
    }

    /**
     * @return The group id.
     */
    @Override
    public String groupId() {
        return groupId;
    }

    /**
     * @return The group epoch.
     */
    public int groupEpoch() {
        return groupEpoch.get();
    }

    /**
     * Sets the group epoch.
     *
     * @param groupEpoch The new group epoch.
     */
    public void setGroupEpoch(int groupEpoch) {
        this.groupEpoch.set(groupEpoch);
        maybeUpdateGroupState();
    }

    /**
     * @return The target assignment epoch.
     */
    public int assignmentEpoch() {
        return targetAssignmentEpoch.get();
    }

    /**
     * Sets the assignment epoch.
     *
     * @param targetAssignmentEpoch The new assignment epoch.
     */
    public void setTargetAssignmentEpoch(int targetAssignmentEpoch) {
        this.targetAssignmentEpoch.set(targetAssignmentEpoch);
        maybeUpdateGroupState();
    }

    /**
     * @return A boolean indicating whether the member exists or not.
     */
    @Override
    public boolean hasMember(String memberId) {
        return members.containsKey(memberId);
    }

    /**
     * @return The number of members.
     */
    @Override
    public int numMembers() {
        return members.size();
    }

    /**
     * @return An immutable Map containing all the members keyed by their id.
     */
    public Map<String, T> members() {
        return Collections.unmodifiableMap(members);
    }

    /**
     * @return An immutable map containing all the subscribed topic names
     *         with the subscribers counts per topic.
     */
    public Map<String, Integer> subscribedTopicNames() {
        return Collections.unmodifiableMap(subscribedTopicNames);
    }

    /**
     * Returns true if the group is actively subscribed to the topic.
     *
     * @param topic  The topic name.
     *
     * @return Whether the group is subscribed to the topic.
     */
    @Override
    public boolean isSubscribedToTopic(String topic) {
        return subscribedTopicNames.containsKey(topic);
    }

    /**
     * @return The group's subscription type.
     */
    public SubscriptionType subscriptionType() {
        return subscriptionType.get();
    }

    /**
     * Returns the target assignment of the member.
     *
     * @return The ConsumerGroupMemberAssignment or an EMPTY one if it does not
     *         exist.
     */
    public Assignment targetAssignment(String memberId) {
        return targetAssignment.getOrDefault(memberId, Assignment.EMPTY);
    }

    /**
     * @return An immutable map containing all the topic partitions
     *         with their current member assignments.
     */
    public Map<Uuid, Map<Integer, String>> invertedTargetAssignment() {
        return Collections.unmodifiableMap(invertedTargetAssignment);
    }

    /**
     * Updates target assignment of a member.
     *
     * @param memberId              The member id.
     * @param newTargetAssignment   The new target assignment.
     */
    public void updateTargetAssignment(String memberId, Assignment newTargetAssignment) {
        updateInvertedTargetAssignment(
            memberId,
            targetAssignment.getOrDefault(memberId, new Assignment(Collections.emptyMap())),
            newTargetAssignment
        );
        targetAssignment.put(memberId, newTargetAssignment);
    }

    /**
     * Updates the reverse lookup map of the target assignment.
     *
     * @param memberId              The member Id.
     * @param oldTargetAssignment   The old target assignment.
     * @param newTargetAssignment   The new target assignment.
     */
    private void updateInvertedTargetAssignment(
        String memberId,
        Assignment oldTargetAssignment,
        Assignment newTargetAssignment
    ) {
        // Combine keys from both old and new assignments.
        Set<Uuid> allTopicIds = new HashSet<>();
        allTopicIds.addAll(oldTargetAssignment.partitions().keySet());
        allTopicIds.addAll(newTargetAssignment.partitions().keySet());

        for (Uuid topicId : allTopicIds) {
            Set<Integer> oldPartitions = oldTargetAssignment.partitions().getOrDefault(topicId, Collections.emptySet());
            Set<Integer> newPartitions = newTargetAssignment.partitions().getOrDefault(topicId, Collections.emptySet());

            TimelineHashMap<Integer, String> topicPartitionAssignment = invertedTargetAssignment.computeIfAbsent(
                topicId, k -> new TimelineHashMap<>(snapshotRegistry, Math.max(oldPartitions.size(), newPartitions.size()))
            );

            // Remove partitions that aren't present in the new assignment only if the partition is currently
            // still assigned to the member in question.
            // If p0 was moved from A to B, and the target assignment map was updated for B first, we don't want to
            // remove the key p0 from the inverted map and undo the action when A eventually tries to update its assignment.
            for (Integer partition : oldPartitions) {
                if (!newPartitions.contains(partition) && memberId.equals(topicPartitionAssignment.get(partition))) {
                    topicPartitionAssignment.remove(partition);
                }
            }

            // Add partitions that are in the new assignment but not in the old assignment.
            for (Integer partition : newPartitions) {
                if (!oldPartitions.contains(partition)) {
                    topicPartitionAssignment.put(partition, memberId);
                }
            }

            if (topicPartitionAssignment.isEmpty()) {
                invertedTargetAssignment.remove(topicId);
            } else {
                invertedTargetAssignment.put(topicId, topicPartitionAssignment);
            }
        }
    }

    /**
     * Removes the target assignment of a member.
     *
     * @param memberId The member id.
     */
    public void removeTargetAssignment(String memberId) {
        updateInvertedTargetAssignment(
            memberId,
            targetAssignment.getOrDefault(memberId, Assignment.EMPTY),
            Assignment.EMPTY
        );
        targetAssignment.remove(memberId);
    }

    /**
     * @return An immutable Map containing all the target assignment keyed by member id.
     */
    public Map<String, Assignment> targetAssignment() {
        return Collections.unmodifiableMap(targetAssignment);
    }

    /**
     * @return An immutable Map of subscription metadata for
     *         each topic that the consumer group is subscribed to.
     */
    public Map<String, TopicMetadata> subscriptionMetadata() {
        return Collections.unmodifiableMap(subscribedTopicMetadata);
    }

    /**
     * Updates the subscription metadata. This replaces the previous one.
     *
     * @param subscriptionMetadata The new subscription metadata.
     */
    public void setSubscriptionMetadata(
        Map<String, TopicMetadata> subscriptionMetadata
    ) {
        this.subscribedTopicMetadata.clear();
        this.subscribedTopicMetadata.putAll(subscriptionMetadata);
    }

    /**
     * Computes the subscription metadata based on the current subscription info.
     *
     * @param subscribedTopicNames      Map of topic names to the number of subscribers.
     * @param topicsImage               The current metadata for all available topics.
     * @param clusterImage              The current metadata for the Kafka cluster.
     *
     * @return An immutable map of subscription metadata for each topic that the consumer group is subscribed to.
     */
    public Map<String, TopicMetadata> computeSubscriptionMetadata(
        Map<String, Integer> subscribedTopicNames,
        TopicsImage topicsImage,
        ClusterImage clusterImage
    ) {
        // Create the topic metadata for each subscribed topic.
        Map<String, TopicMetadata> newSubscriptionMetadata = new HashMap<>(subscribedTopicNames.size());

        subscribedTopicNames.forEach((topicName, count) -> {
            TopicImage topicImage = topicsImage.getTopic(topicName);
            if (topicImage != null) {
                newSubscriptionMetadata.put(topicName, new TopicMetadata(
                    topicImage.id(),
                    topicImage.name(),
                    topicImage.partitions().size()
                ));
            }
        });

        return Collections.unmodifiableMap(newSubscriptionMetadata);
    }

    /**
     * Updates the metadata refresh deadline.
     *
     * @param deadlineMs The deadline in milliseconds.
     * @param groupEpoch The associated group epoch.
     */
    public void setMetadataRefreshDeadline(
        long deadlineMs,
        int groupEpoch
    ) {
        this.metadataRefreshDeadline = new DeadlineAndEpoch(deadlineMs, groupEpoch);
    }

    /**
     * Requests a metadata refresh.
     */
    public void requestMetadataRefresh() {
        this.metadataRefreshDeadline = DeadlineAndEpoch.EMPTY;
    }

    /**
     * Checks if a metadata refresh is required. A refresh is required in two cases:
     * 1) The deadline is smaller or equal to the current time;
     * 2) The group epoch associated with the deadline is larger than
     *    the current group epoch. This means that the operations which updated
     *    the deadline failed.
     *
     * @param currentTimeMs The current time in milliseconds.
     * @return A boolean indicating whether a refresh is required or not.
     */
    public boolean hasMetadataExpired(long currentTimeMs) {
        return currentTimeMs >= metadataRefreshDeadline.deadlineMs || groupEpoch() < metadataRefreshDeadline.epoch;
    }

    /**
     * @return The metadata refresh deadline.
     */
    public DeadlineAndEpoch metadataRefreshDeadline() {
        return metadataRefreshDeadline;
    }

    /**
     * Updates the subscribed topic names count.
     * The subscription type is updated as a consequence.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    protected void maybeUpdateSubscribedTopicNamesAndGroupSubscriptionType(
        ModernGroupMember oldMember,
        ModernGroupMember newMember
    ) {
        maybeUpdateSubscribedTopicNames(subscribedTopicNames, oldMember, newMember);
        subscriptionType.set(subscriptionType(subscribedTopicNames, members.size()));
    }

    /**
     * Updates the subscription count.
     *
     * @param subscribedTopicCount  The map to update.
     * @param oldMember             The old member.
     * @param newMember             The new member.
     */
    private static void maybeUpdateSubscribedTopicNames(
        Map<String, Integer> subscribedTopicCount,
        ModernGroupMember oldMember,
        ModernGroupMember newMember
    ) {
        if (oldMember != null) {
            oldMember.subscribedTopicNames().forEach(topicName ->
                subscribedTopicCount.compute(topicName, Utils::decValue)
            );
        }

        if (newMember != null) {
            newMember.subscribedTopicNames().forEach(topicName ->
                subscribedTopicCount.compute(topicName, Utils::incValue)
            );
        }
    }

    /**
     * Updates the subscription count.
     *
     * @param oldMember             The old member.
     * @param newMember             The new member.
     *
     * @return Copy of the map of topics to the count of number of subscribers.
     */
    public Map<String, Integer> computeSubscribedTopicNames(
        ModernGroupMember oldMember,
        ModernGroupMember newMember
    ) {
        Map<String, Integer> subscribedTopicNames = new HashMap<>(this.subscribedTopicNames);
        maybeUpdateSubscribedTopicNames(
            subscribedTopicNames,
            oldMember,
            newMember
        );
        return subscribedTopicNames;
    }

    /**
     * Updates the subscription count with a set of members removed.
     *
     * @param removedMembers        The set of removed members.
     *
     * @return Copy of the map of topics to the count of number of subscribers.
     */
    public Map<String, Integer> computeSubscribedTopicNames(
        Set<? extends ModernGroupMember> removedMembers
    ) {
        Map<String, Integer> subscribedTopicNames = new HashMap<>(this.subscribedTopicNames);
        if (removedMembers != null) {
            removedMembers.forEach(removedMember ->
                maybeUpdateSubscribedTopicNames(
                    subscribedTopicNames,
                    removedMember,
                    null
                )
            );
        }
        return subscribedTopicNames;
    }

    /**
     * Compute the subscription type of the group.
     *
     * @param subscribedTopicNames      A map of topic names to the count of members subscribed to each topic.
     *
     * @return {@link SubscriptionType#HOMOGENEOUS} if all members are subscribed to exactly the same topics;
     *         otherwise, {@link SubscriptionType#HETEROGENEOUS}.
     */
    public static SubscriptionType subscriptionType(
        Map<String, Integer> subscribedTopicNames,
        int numberOfMembers
    ) {
        if (subscribedTopicNames.isEmpty()) {
            return HOMOGENEOUS;
        }

        for (int subscriberCount : subscribedTopicNames.values()) {
            if (subscriberCount != numberOfMembers) {
                return HETEROGENEOUS;
            }
        }
        return HOMOGENEOUS;
    }

    /**
     * Gets the protocol type for the group.
     *
     * @return The group protocol type.
     */
    public abstract String protocolType();

    /**
     * Gets or creates a member.
     *
     * @param memberId          The member id.
     * @param createIfNotExists Booleans indicating whether the member must be
     *                          created if it does not exist.
     *
     * @return A ConsumerGroupMember.
     * @throws UnknownMemberIdException when the member does not exist and createIfNotExists is false.
     */
    public abstract T getOrMaybeCreateMember(String memberId, boolean createIfNotExists) throws UnknownMemberIdException;

    /**
     * Adds or updates the member.
     *
     * @param newMember The new member state.
     */
    public abstract void updateMember(T newMember);

    /**
     * Remove the member from the group.
     *
     * @param memberId The member id to remove.
     */
    public abstract void removeMember(String memberId);

    /**
     * Updates the current state of the group.
     */
    protected abstract void maybeUpdateGroupState();
}
