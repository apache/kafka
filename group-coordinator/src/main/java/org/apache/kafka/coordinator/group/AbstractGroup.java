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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.StaleMemberEpochException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.coordinator.group.common.Assignment;
import org.apache.kafka.coordinator.group.common.TopicMetadata;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineInteger;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public abstract class AbstractGroup implements Group {

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
    protected final TimelineHashMap<String, GroupMember> members;

    /**
     * The number of subscribers per topic.
     */
    protected final TimelineHashMap<String, Integer> subscribedTopicNames;

    /**
     * The metadata associated with each subscribed topic name.
     */
    protected final TimelineHashMap<String, TopicMetadata> subscribedTopicMetadata;

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
     * The current partition epoch maps each topic-partitions to their current epoch where
     * the epoch is the epoch of their owners. When a member revokes a partition, it removes
     * its epochs from this map. When a member gets a partition, it adds its epochs to this map.
     */
    protected final TimelineHashMap<Uuid, TimelineHashMap<Integer, Integer>> currentPartitionEpoch;

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

    protected AbstractGroup(
        SnapshotRegistry snapshotRegistry,
        String groupId
    ) {
        this.snapshotRegistry = Objects.requireNonNull(snapshotRegistry);
        this.groupId = Objects.requireNonNull(groupId);
        this.groupEpoch = new TimelineInteger(snapshotRegistry);
        this.members = new TimelineHashMap<>(snapshotRegistry, 0);
        this.subscribedTopicNames = new TimelineHashMap<>(snapshotRegistry, 0);
        this.subscribedTopicMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
        this.targetAssignmentEpoch = new TimelineInteger(snapshotRegistry);
        this.targetAssignment = new TimelineHashMap<>(snapshotRegistry, 0);
        this.currentPartitionEpoch = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    /**
     * @return the group formatted as a list group response based on the committed offset.
     */
    public ListGroupsResponseData.ListedGroup asListedGroup(long committedOffset) {
        return new ListGroupsResponseData.ListedGroup()
            .setGroupId(groupId)
            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
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
    public Map<String, GroupMember> members() {
        return Collections.unmodifiableMap(members);
    }

    /**
     * @return An immutable Set containing all the subscribed topic names.
     */
    public Set<String> subscribedTopicNames() {
        return Collections.unmodifiableSet(subscribedTopicNames.keySet());
    }

    /**
     * Returns true if the consumer group is actively subscribed to the topic.
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
     * Returns the target assignment of the member.
     *
     * @return The ConsumerGroupMemberAssignment or an EMPTY one if it does not
     *         exist.
     */
    public Assignment targetAssignment(String memberId) {
        return targetAssignment.getOrDefault(memberId, Assignment.EMPTY);
    }

    /**
     * Updates target assignment of a member.
     *
     * @param memberId              The member id.
     * @param newTargetAssignment   The new target assignment.
     */
    public void updateTargetAssignment(String memberId, Assignment newTargetAssignment) {
        targetAssignment.put(memberId, newTargetAssignment);
    }

    /**
     * Removes the target assignment of a member.
     *
     * @param memberId The member id.
     */
    public void removeTargetAssignment(String memberId) {
        targetAssignment.remove(memberId);
    }

    /**
     * @return An immutable Map containing all the target assignment keyed by member id.
     */
    public Map<String, Assignment> targetAssignment() {
        return Collections.unmodifiableMap(targetAssignment);
    }

    /**
     * Returns the current epoch of a partition or -1 if the partition
     * does not have one.
     *
     * @param topicId       The topic id.
     * @param partitionId   The partition id.
     *
     * @return The epoch or -1.
     */
    public int currentPartitionEpoch(
        Uuid topicId, int partitionId
    ) {
        Map<Integer, Integer> partitions = currentPartitionEpoch.get(topicId);
        if (partitions == null) {
            return -1;
        } else {
            return partitions.getOrDefault(partitionId, -1);
        }
    }

    /**
     * @return An immutable Map of subscription metadata for
     *         each topic that the consumer group is subscribed to.
     */
    public Map<String, TopicMetadata> subscriptionMetadata() {
        return Collections.unmodifiableMap(subscribedTopicMetadata);
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
     * Validates the OffsetCommit request.
     *
     * @param memberId          The member id.
     * @param groupInstanceId   The group instance id.
     * @param memberEpoch       The member epoch.
     * @param isTransactional   Whether the offset commit is transactional or not. It has no
     *                          impact when a consumer group is used.
     */
    @Override
    public void validateOffsetCommit(
        String memberId,
        String groupInstanceId,
        int memberEpoch,
        boolean isTransactional
    ) throws UnknownMemberIdException, StaleMemberEpochException {
        // When the member epoch is -1, the request comes from either the admin client
        // or a consumer which does not use the group management facility. In this case,
        // the request can commit offsets if the group is empty.
        if (memberEpoch < 0 && members().isEmpty()) return;

        final GroupMember member = getOrMaybeCreateMember(memberId, false);
        validateMemberEpoch(memberEpoch, member.memberEpoch());
    }

    /**
     * Validates the OffsetFetch request.
     *
     * @param memberId              The member id for consumer groups.
     * @param memberEpoch           The member epoch for consumer groups.
     * @param lastCommittedOffset   The last committed offsets in the timeline.
     */
    @Override
    public void validateOffsetFetch(
        String memberId,
        int memberEpoch,
        long lastCommittedOffset
    ) throws UnknownMemberIdException, StaleMemberEpochException {
        // When the member id is null and the member epoch is -1, the request either comes
        // from the admin client or from a client which does not provide them. In this case,
        // the fetch request is accepted.
        if (memberId == null && memberEpoch < 0) return;

        final GroupMember member = members.get(memberId, lastCommittedOffset);
        if (member == null) {
            throw new UnknownMemberIdException(String.format("Member %s is not a member of group %s.",
                memberId, groupId));
        }
        validateMemberEpoch(memberEpoch, member.memberEpoch());
    }

    /**
     * Validates the OffsetDelete request.
     */
    @Override
    public void validateOffsetDelete() {
        // Do nothing.
    }

    /**
     * See {@link org.apache.kafka.coordinator.group.OffsetExpirationCondition}
     *
     * @return The offset expiration condition for the group or Empty if no such condition exists.
     */
    @Override
    public Optional<OffsetExpirationCondition> offsetExpirationCondition() {
        return Optional.of(new OffsetExpirationConditionImpl(offsetAndMetadata -> offsetAndMetadata.commitTimestampMs));
    }

    /**
     * Computes the subscription metadata based on the current subscription and
     * an updated member.
     *
     * @param oldMember     The old member of the consumer group.
     * @param newMember     The updated member of the consumer group.
     * @param topicsImage   The current metadata for all available topics.
     * @param clusterImage  The current metadata for the Kafka cluster.
     *
     * @return An immutable map of subscription metadata for each topic that the consumer group is subscribed to.
     */
    public Map<String, TopicMetadata> computeSubscriptionMetadata(
        GroupMember oldMember,
        GroupMember newMember,
        TopicsImage topicsImage,
        ClusterImage clusterImage
    ) {
        // Copy and update the current subscriptions.
        Map<String, Integer> subscribedTopicNames = new HashMap<>(this.subscribedTopicNames);
        maybeUpdateSubscribedTopicNames(subscribedTopicNames, oldMember, newMember);

        // Create the topic metadata for each subscribed topic.
        Map<String, TopicMetadata> newSubscriptionMetadata = new HashMap<>(subscribedTopicNames.size());

        subscribedTopicNames.forEach((topicName, count) -> {
            TopicImage topicImage = topicsImage.getTopic(topicName);
            if (topicImage != null) {
                Map<Integer, Set<String>> partitionRacks = new HashMap<>();
                topicImage.partitions().forEach((partition, partitionRegistration) -> {
                    Set<String> racks = new HashSet<>();
                    for (int replica : partitionRegistration.replicas) {
                        Optional<String> rackOptional = clusterImage.broker(replica).rack();
                        // Only add the rack if it is available for the broker/replica.
                        rackOptional.ifPresent(racks::add);
                    }
                    // If rack information is unavailable for all replicas of this partition,
                    // no corresponding entry will be stored for it in the map.
                    if (!racks.isEmpty())
                        partitionRacks.put(partition, racks);
                });

                newSubscriptionMetadata.put(topicName, new TopicMetadata(
                    topicImage.id(),
                    topicImage.name(),
                    topicImage.partitions().size(),
                    partitionRacks)
                );
            }
        });

        return Collections.unmodifiableMap(newSubscriptionMetadata);
    }

    /**
     * Throws a StaleMemberEpochException if the received member epoch does not match
     * the expected member epoch.
     */
    private void validateMemberEpoch(
        int receivedMemberEpoch,
        int expectedMemberEpoch
    ) throws StaleMemberEpochException {
        if (receivedMemberEpoch != expectedMemberEpoch) {
            throw new StaleMemberEpochException(String.format("The received member epoch %d does not match "
                + "the expected member epoch %d.", receivedMemberEpoch, expectedMemberEpoch));
        }
    }

    /**
     * Updates the subscribed topic names count.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    protected void maybeUpdateSubscribedTopicNames(
        GroupMember oldMember,
        GroupMember newMember
    ) {
        maybeUpdateSubscribedTopicNames(subscribedTopicNames, oldMember, newMember);
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
        GroupMember oldMember,
        GroupMember newMember
    ) {
        if (oldMember != null) {
            oldMember.subscribedTopicNames().forEach(topicName ->
                subscribedTopicCount.compute(topicName, AbstractGroup::decValue)
            );
        }

        if (newMember != null) {
            newMember.subscribedTopicNames().forEach(topicName ->
                subscribedTopicCount.compute(topicName, AbstractGroup::incValue)
            );
        }
    }

    /**
     * Decrements value by 1; returns null when reaching zero. This helper is
     * meant to be used with Map#compute.
     */
    protected static Integer decValue(String key, Integer value) {
        if (value == null) return null;
        return value == 1 ? null : value - 1;
    }

    /**
     * Increments value by 1; This helper is meant to be used with Map#compute.
     */
    protected static Integer incValue(String key, Integer value) {
        return value == null ? 1 : value + 1;
    }

    /**
     * Gets or creates a member.
     *
     * @param memberId          The member id.
     * @param createIfNotExists Booleans indicating whether the member must be
     *                          created if it does not exist.
     *
     * @return A ConsumerGroupMember.
     */
    public abstract GroupMember getOrMaybeCreateMember(String memberId, boolean createIfNotExists);

    public abstract void updateMember(GroupMember newMember);

    public abstract void removeMember(String memberId);

    protected abstract void maybeUpdateGroupState();
}
