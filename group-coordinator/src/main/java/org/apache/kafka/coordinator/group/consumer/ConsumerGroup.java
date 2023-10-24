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

import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.StaleMemberEpochException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.coordinator.group.Group;
import org.apache.kafka.coordinator.group.OffsetExpirationCondition;
import org.apache.kafka.coordinator.group.OffsetExpirationConditionImpl;
import org.apache.kafka.coordinator.group.Record;
import org.apache.kafka.coordinator.group.RecordHelpers;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A Consumer Group. All the metadata in this class are backed by
 * records in the __consumer_offsets partitions.
 */
public class ConsumerGroup implements Group {

    public enum ConsumerGroupState {
        EMPTY("empty"),
        ASSIGNING("assigning"),
        RECONCILING("reconciling"),
        STABLE("stable"),
        DEAD("dead");

        private final String name;

        ConsumerGroupState(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

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
    private final SnapshotRegistry snapshotRegistry;

    /**
     * The group id.
     */
    private final String groupId;

    /**
     * The group state.
     */
    private final TimelineObject<ConsumerGroupState> state;

    /**
     * The group epoch. The epoch is incremented whenever the subscriptions
     * are updated and it will trigger the computation of a new assignment
     * for the group.
     */
    private final TimelineInteger groupEpoch;

    /**
     * The group members.
     */
    private final TimelineHashMap<String, ConsumerGroupMember> members;

    /**
     * The number of members supporting each server assignor name.
     */
    private final TimelineHashMap<String, Integer> serverAssignors;

    /**
     * The number of subscribers per topic.
     */
    private final TimelineHashMap<String, Integer> subscribedTopicNames;

    /**
     * The metadata associated with each subscribed topic name.
     */
    private final TimelineHashMap<String, TopicMetadata> subscribedTopicMetadata;

    /**
     * The target assignment epoch. An assignment epoch smaller than the group epoch
     * means that a new assignment is required. The assignment epoch is updated when
     * a new assignment is installed.
     */
    private final TimelineInteger targetAssignmentEpoch;

    /**
     * The target assignment per member id.
     */
    private final TimelineHashMap<String, Assignment> targetAssignment;

    /**
     * The current partition epoch maps each topic-partitions to their current epoch where
     * the epoch is the epoch of their owners. When a member revokes a partition, it removes
     * its epochs from this map. When a member gets a partition, it adds its epochs to this map.
     */
    private final TimelineHashMap<Uuid, TimelineHashMap<Integer, Integer>> currentPartitionEpoch;

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
    private DeadlineAndEpoch metadataRefreshDeadline = DeadlineAndEpoch.EMPTY;

    public ConsumerGroup(
        SnapshotRegistry snapshotRegistry,
        String groupId
    ) {
        this.snapshotRegistry = Objects.requireNonNull(snapshotRegistry);
        this.groupId = Objects.requireNonNull(groupId);
        this.state = new TimelineObject<>(snapshotRegistry, ConsumerGroupState.EMPTY);
        this.groupEpoch = new TimelineInteger(snapshotRegistry);
        this.members = new TimelineHashMap<>(snapshotRegistry, 0);
        this.serverAssignors = new TimelineHashMap<>(snapshotRegistry, 0);
        this.subscribedTopicNames = new TimelineHashMap<>(snapshotRegistry, 0);
        this.subscribedTopicMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
        this.targetAssignmentEpoch = new TimelineInteger(snapshotRegistry);
        this.targetAssignment = new TimelineHashMap<>(snapshotRegistry, 0);
        this.currentPartitionEpoch = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    /**
     * @return The group type (Consumer).
     */
    @Override
    public GroupType type() {
        return GroupType.CONSUMER;
    }

    /**
     * @return The current state as a String.
     */
    @Override
    public String stateAsString() {
        return state.get().toString();
    }

    /**
     * @return The current state as a String with given committedOffset.
     */
    public String stateAsString(long committedOffset) {
        return state.get(committedOffset).toString();
    }

    /**
     * @return the group formatted as a list group response based on the committed offset.
     */
    public ListGroupsResponseData.ListedGroup asListedGroup(long committedOffset) {
        return new ListGroupsResponseData.ListedGroup()
            .setGroupId(groupId)
            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
            .setGroupState(state.get(committedOffset).toString());
    }

    /**
     * @return The group id.
     */
    @Override
    public String groupId() {
        return groupId;
    }

    /**
     * @return The current state.
     */
    public ConsumerGroupState state() {
        return state.get();
    }

    /**
     * @return The current state based on committed offset.
     */
    public ConsumerGroupState state(long committedOffset) {
        return state.get(committedOffset);
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
     * Gets or creates a member.
     *
     * @param memberId          The member id.
     * @param createIfNotExists Booleans indicating whether the member must be
     *                          created if it does not exist.
     *
     * @return A ConsumerGroupMember.
     */
    public ConsumerGroupMember getOrMaybeCreateMember(
        String memberId,
        boolean createIfNotExists
    ) {
        ConsumerGroupMember member = members.get(memberId);
        if (member == null) {
            if (!createIfNotExists) {
                throw new UnknownMemberIdException(String.format("Member %s is not a member of group %s.",
                    memberId, groupId));
            }
            member = new ConsumerGroupMember.Builder(memberId).build();
            members.put(memberId, member);
        }

        return member;
    }

    /**
     * Updates the member.
     *
     * @param newMember The new member state.
     */
    public void updateMember(ConsumerGroupMember newMember) {
        if (newMember == null) {
            throw new IllegalArgumentException("newMember cannot be null.");
        }
        ConsumerGroupMember oldMember = members.put(newMember.memberId(), newMember);
        maybeUpdateSubscribedTopicNames(oldMember, newMember);
        maybeUpdateServerAssignors(oldMember, newMember);
        maybeUpdatePartitionEpoch(oldMember, newMember);
        maybeUpdateGroupState();
    }

    /**
     * Remove the member from the group.
     *
     * @param memberId The member id to remove.
     */
    public void removeMember(String memberId) {
        ConsumerGroupMember oldMember = members.remove(memberId);
        maybeUpdateSubscribedTopicNames(oldMember, null);
        maybeUpdateServerAssignors(oldMember, null);
        maybeRemovePartitionEpoch(oldMember);
        maybeUpdateGroupState();
    }

    /**
     * Returns true if the member exists.
     *
     * @param memberId The member id.
     *
     * @return A boolean indicating whether the member exists or not.
     */
    public boolean hasMember(String memberId) {
        return members.containsKey(memberId);
    }

    /**
     * @return The number of members.
     */
    public int numMembers() {
        return members.size();
    }

    /**
     * @return An immutable Map containing all the members keyed by their id.
     */
    public Map<String, ConsumerGroupMember> members() {
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
     * Compute the preferred (server side) assignor for the group while
     * taking into account the updated member. The computation relies
     * on {{@link ConsumerGroup#serverAssignors}} persisted structure
     * but it does not update it.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     *
     * @return An Optional containing the preferred assignor.
     */
    public Optional<String> computePreferredServerAssignor(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        // Copy the current count and update it.
        Map<String, Integer> counts = new HashMap<>(this.serverAssignors);
        maybeUpdateServerAssignors(counts, oldMember, newMember);

        return counts.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey);
    }

    /**
     * @return The preferred assignor for the group.
     */
    public Optional<String> preferredServerAssignor() {
        return serverAssignors.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey);
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
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember,
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
     */
    @Override
    public void validateOffsetCommit(
        String memberId,
        String groupInstanceId,
        int memberEpoch
    ) throws UnknownMemberIdException, StaleMemberEpochException {
        // When the member epoch is -1, the request comes from either the admin client
        // or a consumer which does not use the group management facility. In this case,
        // the request can commit offsets if the group is empty.
        if (memberEpoch < 0 && members().isEmpty()) return;

        final ConsumerGroupMember member = getOrMaybeCreateMember(memberId, false);
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

        final ConsumerGroupMember member = members.get(memberId, lastCommittedOffset);
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
    public void validateOffsetDelete() {}

    /**
     * Validates the DeleteGroups request.
     */
    @Override
    public void validateDeleteGroup() throws ApiException {
        if (state() != ConsumerGroupState.EMPTY) {
            throw Errors.NON_EMPTY_GROUP.exception();
        }
    }

    /**
     * Populates the list of records with tombstone(s) for deleting the group.
     *
     * @param records The list of records.
     */
    @Override
    public void createGroupTombstoneRecords(List<Record> records) {
        records.add(RecordHelpers.newTargetAssignmentEpochTombstoneRecord(groupId()));
        records.add(RecordHelpers.newGroupSubscriptionMetadataTombstoneRecord(groupId()));
        records.add(RecordHelpers.newGroupEpochTombstoneRecord(groupId()));
    }

    @Override
    public boolean isEmpty() {
        return state() == ConsumerGroupState.EMPTY;
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
     * Updates the current state of the group.
     */
    private void maybeUpdateGroupState() {
        if (members.isEmpty()) {
            state.set(ConsumerGroupState.EMPTY);
        } else if (groupEpoch.get() > targetAssignmentEpoch.get()) {
            state.set(ConsumerGroupState.ASSIGNING);
        } else {
            for (ConsumerGroupMember member : members.values()) {
                if (member.targetMemberEpoch() != targetAssignmentEpoch.get() || member.state() != ConsumerGroupMember.MemberState.STABLE) {
                    state.set(ConsumerGroupState.RECONCILING);
                    return;
                }
            }

            state.set(ConsumerGroupState.STABLE);
        }
    }

    /**
     * Updates the server assignors count.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdateServerAssignors(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        maybeUpdateServerAssignors(serverAssignors, oldMember, newMember);
    }

    /**
     * Updates the server assignors count.
     *
     * @param serverAssignorCount   The count to update.
     * @param oldMember             The old member.
     * @param newMember             The new member.
     */
    private static void maybeUpdateServerAssignors(
        Map<String, Integer> serverAssignorCount,
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        if (oldMember != null) {
            oldMember.serverAssignorName().ifPresent(name ->
                serverAssignorCount.compute(name, ConsumerGroup::decValue)
            );
        }
        if (newMember != null) {
            newMember.serverAssignorName().ifPresent(name ->
                serverAssignorCount.compute(name, ConsumerGroup::incValue)
            );
        }
    }

    /**
     * Updates the subscribed topic names count.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdateSubscribedTopicNames(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
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
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        if (oldMember != null) {
            oldMember.subscribedTopicNames().forEach(topicName ->
                subscribedTopicCount.compute(topicName, ConsumerGroup::decValue)
            );
        }

        if (newMember != null) {
            newMember.subscribedTopicNames().forEach(topicName ->
                subscribedTopicCount.compute(topicName, ConsumerGroup::incValue)
            );
        }
    }

    /**
     * Updates the partition epochs based on the old and the new member.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdatePartitionEpoch(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        if (oldMember == null) {
            addPartitionEpochs(newMember.assignedPartitions(), newMember.memberEpoch());
            addPartitionEpochs(newMember.partitionsPendingRevocation(), newMember.memberEpoch());
        } else {
            if (!oldMember.assignedPartitions().equals(newMember.assignedPartitions())) {
                removePartitionEpochs(oldMember.assignedPartitions());
                addPartitionEpochs(newMember.assignedPartitions(), newMember.memberEpoch());
            }
            if (!oldMember.partitionsPendingRevocation().equals(newMember.partitionsPendingRevocation())) {
                removePartitionEpochs(oldMember.partitionsPendingRevocation());
                addPartitionEpochs(newMember.partitionsPendingRevocation(), newMember.memberEpoch());
            }
        }
    }

    /**
     * Removes the partition epochs for the provided member.
     *
     * @param oldMember The old member.
     */
    private void maybeRemovePartitionEpoch(
        ConsumerGroupMember oldMember
    ) {
        if (oldMember != null) {
            removePartitionEpochs(oldMember.assignedPartitions());
            removePartitionEpochs(oldMember.partitionsPendingRevocation());
        }
    }

    /**
     * Removes the partition epochs based on the provided assignment.
     *
     * @param assignment    The assignment.
     */
    private void removePartitionEpochs(
        Map<Uuid, Set<Integer>> assignment
    ) {
        assignment.forEach((topicId, assignedPartitions) -> {
            currentPartitionEpoch.compute(topicId, (__, partitionsOrNull) -> {
                if (partitionsOrNull != null) {
                    assignedPartitions.forEach(partitionsOrNull::remove);
                    if (partitionsOrNull.isEmpty()) {
                        return null;
                    } else {
                        return partitionsOrNull;
                    }
                } else {
                    return null;
                }
            });
        });
    }

    /**
     * Adds the partitions epoch based on the provided assignment.
     *
     * @param assignment    The assignment.
     * @param epoch         The new epoch.
     */
    private void addPartitionEpochs(
        Map<Uuid, Set<Integer>> assignment,
        int epoch
    ) {
        assignment.forEach((topicId, assignedPartitions) -> {
            currentPartitionEpoch.compute(topicId, (__, partitionsOrNull) -> {
                if (partitionsOrNull == null) {
                    partitionsOrNull = new TimelineHashMap<>(snapshotRegistry, assignedPartitions.size());
                }
                for (Integer partitionId : assignedPartitions) {
                    partitionsOrNull.put(partitionId, epoch);
                }
                return partitionsOrNull;
            });
        });
    }

    /**
     * Decrements value by 1; returns null when reaching zero. This helper is
     * meant to be used with Map#compute.
     */
    private static Integer decValue(String key, Integer value) {
        if (value == null) return null;
        return value == 1 ? null : value - 1;
    }

    /**
     * Increments value by 1; This helper is meant to be used with Map#compute.
     */
    private static Integer incValue(String key, Integer value) {
        return value == null ? 1 : value + 1;
    }
}
