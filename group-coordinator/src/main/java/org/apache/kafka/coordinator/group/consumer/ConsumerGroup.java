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

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.StaleMemberEpochException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.coordinator.group.Group;
import org.apache.kafka.coordinator.group.OffsetExpirationCondition;
import org.apache.kafka.coordinator.group.OffsetExpirationConditionImpl;
import org.apache.kafka.coordinator.group.CoordinatorRecord;
import org.apache.kafka.coordinator.group.CoordinatorRecordHelpers;
import org.apache.kafka.coordinator.group.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineInteger;
import org.apache.kafka.timeline.TimelineObject;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.coordinator.group.assignor.SubscriptionType.HETEROGENEOUS;
import static org.apache.kafka.coordinator.group.assignor.SubscriptionType.HOMOGENEOUS;
import static org.apache.kafka.coordinator.group.consumer.ConsumerGroup.ConsumerGroupState.ASSIGNING;
import static org.apache.kafka.coordinator.group.consumer.ConsumerGroup.ConsumerGroupState.EMPTY;
import static org.apache.kafka.coordinator.group.consumer.ConsumerGroup.ConsumerGroupState.RECONCILING;
import static org.apache.kafka.coordinator.group.consumer.ConsumerGroup.ConsumerGroupState.STABLE;

/**
 * A Consumer Group. All the metadata in this class are backed by
 * records in the __consumer_offsets partitions.
 */
public class ConsumerGroup implements Group {

    public enum ConsumerGroupState {
        EMPTY("Empty"),
        ASSIGNING("Assigning"),
        RECONCILING("Reconciling"),
        STABLE("Stable"),
        DEAD("Dead");

        private final String name;

        private final String lowerCaseName;

        ConsumerGroupState(String name) {
            this.name = name;
            this.lowerCaseName = name.toLowerCase(Locale.ROOT);
        }

        @Override
        public String toString() {
            return name;
        }

        public String toLowerCaseString() {
            return lowerCaseName;
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
     * The static group members.
     */
    private final TimelineHashMap<String, String> staticMembers;

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
     * The consumer group's subscription type.
     * This value is set to Homogeneous by default.
     */
    private final TimelineObject<SubscriptionType> subscriptionType;

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
     * The coordinator metrics.
     */
    private final GroupCoordinatorMetricsShard metrics;

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

    /**
     * The number of members that use the classic protocol.
     */
    private final TimelineInteger numClassicProtocolMembers;

    /**
     * Map of protocol names to the number of members that use classic protocol and support them.
     */
    private final TimelineHashMap<String, Integer> classicProtocolMembersSupportedProtocols;

    public ConsumerGroup(
        SnapshotRegistry snapshotRegistry,
        String groupId,
        GroupCoordinatorMetricsShard metrics
    ) {
        this.snapshotRegistry = Objects.requireNonNull(snapshotRegistry);
        this.groupId = Objects.requireNonNull(groupId);
        this.state = new TimelineObject<>(snapshotRegistry, EMPTY);
        this.groupEpoch = new TimelineInteger(snapshotRegistry);
        this.members = new TimelineHashMap<>(snapshotRegistry, 0);
        this.staticMembers = new TimelineHashMap<>(snapshotRegistry, 0);
        this.serverAssignors = new TimelineHashMap<>(snapshotRegistry, 0);
        this.subscribedTopicNames = new TimelineHashMap<>(snapshotRegistry, 0);
        this.subscribedTopicMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
        this.subscriptionType = new TimelineObject<>(snapshotRegistry, HOMOGENEOUS);
        this.targetAssignmentEpoch = new TimelineInteger(snapshotRegistry);
        this.targetAssignment = new TimelineHashMap<>(snapshotRegistry, 0);
        this.currentPartitionEpoch = new TimelineHashMap<>(snapshotRegistry, 0);
        this.metrics = Objects.requireNonNull(metrics);
        this.numClassicProtocolMembers = new TimelineInteger(snapshotRegistry);
        this.classicProtocolMembersSupportedProtocols = new TimelineHashMap<>(snapshotRegistry, 0);
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
            .setGroupState(state.get(committedOffset).toString())
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
     * Sets the number of members using the classic protocol.
     *
     * @param numClassicProtocolMembers The new NumClassicProtocolMembers.
     */
    public void setNumClassicProtocolMembers(int numClassicProtocolMembers) {
        this.numClassicProtocolMembers.set(numClassicProtocolMembers);
    }

    /**
     * Get member id of a static member that matches the given group
     * instance id.
     *
     * @param groupInstanceId The group instance id.
     *
     * @return The member id corresponding to the given instance id or null if it does not exist
     */
    public String staticMemberId(String groupInstanceId) {
        return staticMembers.get(groupInstanceId);
    }

    /**
     * Gets or creates a new member but without adding it to the group. Adding a member
     * is done via the {@link ConsumerGroup#updateMember(ConsumerGroupMember)} method.
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
        if (member != null) return member;

        if (!createIfNotExists) {
            throw new UnknownMemberIdException(
                String.format("Member %s is not a member of group %s.", memberId, groupId)
            );
        }

        return new ConsumerGroupMember.Builder(memberId).build();
    }

    /**
     * Gets a static member.
     *
     * @param instanceId The group instance id.
     *
     * @return The member corresponding to the given instance id or null if it does not exist
     */
    public ConsumerGroupMember staticMember(String instanceId) {
        String existingMemberId = staticMemberId(instanceId);
        return existingMemberId == null ? null : getOrMaybeCreateMember(existingMemberId, false);
    }

    /**
     * Adds or updates the member.
     *
     * @param newMember The new member state.
     */
    public void updateMember(ConsumerGroupMember newMember) {
        if (newMember == null) {
            throw new IllegalArgumentException("newMember cannot be null.");
        }
        ConsumerGroupMember oldMember = members.put(newMember.memberId(), newMember);
        maybeUpdateSubscribedTopicNamesAndGroupSubscriptionType(oldMember, newMember);
        maybeUpdateServerAssignors(oldMember, newMember);
        maybeUpdatePartitionEpoch(oldMember, newMember);
        updateStaticMember(newMember);
        maybeUpdateGroupState();
        maybeUpdateNumClassicProtocolMembers(oldMember, newMember);
        maybeUpdateClassicProtocolMembersSupportedProtocols(oldMember, newMember);
    }

    /**
     * Updates the member id stored against the instance id if the member is a static member.
     *
     * @param newMember The new member state.
     */
    private void updateStaticMember(ConsumerGroupMember newMember) {
        if (newMember.instanceId() != null) {
            staticMembers.put(newMember.instanceId(), newMember.memberId());
        }
    }

    /**
     * Remove the member from the group.
     *
     * @param memberId The member id to remove.
     */
    public void removeMember(String memberId) {
        ConsumerGroupMember oldMember = members.remove(memberId);
        maybeUpdateSubscribedTopicNamesAndGroupSubscriptionType(oldMember, null);
        maybeUpdateServerAssignors(oldMember, null);
        maybeRemovePartitionEpoch(oldMember);
        removeStaticMember(oldMember);
        maybeUpdateGroupState();
        maybeUpdateNumClassicProtocolMembers(oldMember, null);
        maybeUpdateClassicProtocolMembersSupportedProtocols(oldMember, null);
    }

    /**
     * Remove the static member mapping if the removed member is static.
     *
     * @param oldMember The member to remove.
     */
    private void removeStaticMember(ConsumerGroupMember oldMember) {
        if (oldMember.instanceId() != null) {
            staticMembers.remove(oldMember.instanceId());
        }
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
     * @return The number of members that use the classic protocol.
     */
    public int numClassicProtocolMembers() {
        return numClassicProtocolMembers.get();
    }

    /**
     * @return The map of the protocol name and the number of members using the classic protocol that support it.
     */
    public Map<String, Integer> classicMembersSupportedProtocols() {
        return Collections.unmodifiableMap(classicProtocolMembersSupportedProtocols);
    }

    /**
     * @return An immutable Map containing all the members keyed by their id.
     */
    public Map<String, ConsumerGroupMember> members() {
        return Collections.unmodifiableMap(members);
    }

    /**
     * @return An immutable Map containing all the static members keyed by instance id.
     */
    public Map<String, String> staticMembers() {
        return Collections.unmodifiableMap(staticMembers);
    }

    /**
     * @return An immutable map containing all the subscribed topic names
     *         with the subscribers counts per topic.
     */
    public Map<String, Integer> subscribedTopicNames() {
        return Collections.unmodifiableMap(subscribedTopicNames);
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
        return preferredServerAssignor(Long.MAX_VALUE);
    }

    /**
     * @return The preferred assignor for the group with given offset.
     */
    public Optional<String> preferredServerAssignor(long committedOffset) {
        return serverAssignors.entrySet(committedOffset).stream()
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
    public void createGroupTombstoneRecords(List<CoordinatorRecord> records) {
        members().forEach((memberId, member) ->
            records.add(CoordinatorRecordHelpers.newCurrentAssignmentTombstoneRecord(groupId(), memberId))
        );

        members().forEach((memberId, member) ->
            records.add(CoordinatorRecordHelpers.newTargetAssignmentTombstoneRecord(groupId(), memberId))
        );
        records.add(CoordinatorRecordHelpers.newTargetAssignmentEpochTombstoneRecord(groupId()));

        members().forEach((memberId, member) ->
            records.add(CoordinatorRecordHelpers.newMemberSubscriptionTombstoneRecord(groupId(), memberId))
        );

        records.add(CoordinatorRecordHelpers.newGroupSubscriptionMetadataTombstoneRecord(groupId()));
        records.add(CoordinatorRecordHelpers.newGroupEpochTombstoneRecord(groupId()));
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

    @Override
    public boolean isInStates(Set<String> statesFilter, long committedOffset) {
        return statesFilter.contains(state.get(committedOffset).toLowerCaseString());
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
        ConsumerGroupState previousState = state.get();
        ConsumerGroupState newState = STABLE;
        if (members.isEmpty()) {
            newState = EMPTY;
        } else if (groupEpoch.get() > targetAssignmentEpoch.get()) {
            newState = ASSIGNING;
        } else {
            for (ConsumerGroupMember member : members.values()) {
                if (!member.isReconciledTo(targetAssignmentEpoch.get())) {
                    newState = RECONCILING;
                    break;
                }
            }
        }

        state.set(newState);
        metrics.onConsumerGroupStateTransition(previousState, newState);
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
     * Updates the number of the members that use the classic protocol.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdateNumClassicProtocolMembers(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        int delta = 0;
        if (oldMember != null && oldMember.useClassicProtocol()) {
            delta--;
        }
        if (newMember != null && newMember.useClassicProtocol()) {
            delta++;
        }
        setNumClassicProtocolMembers(numClassicProtocolMembers() + delta);
    }

    /**
     * Updates the supported protocol count of the members that use the classic protocol.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdateClassicProtocolMembersSupportedProtocols(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        if (oldMember != null) {
            oldMember.supportedClassicProtocols().ifPresent(protocols ->
                protocols.forEach(protocol ->
                    classicProtocolMembersSupportedProtocols.compute(protocol.name(), ConsumerGroup::decValue)
                )
            );
        }
        if (newMember != null) {
            newMember.supportedClassicProtocols().ifPresent(protocols ->
                protocols.forEach(protocol ->
                    classicProtocolMembersSupportedProtocols.compute(protocol.name(), ConsumerGroup::incValue)
                )
            );
        }
    }

    /**
     * Updates the subscribed topic names count.
     * The subscription type is updated as a consequence.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdateSubscribedTopicNamesAndGroupSubscriptionType(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
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
     * Updates the subscription count.
     *
     * @param oldMember             The old member.
     * @param newMember             The new member.
     *
     * @return Copy of the map of topics to the count of number of subscribers.
     */
    public Map<String, Integer> computeSubscribedTopicNames(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
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
     * Compute the subscription type of the consumer group.
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
     * Updates the partition epochs based on the old and the new member.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdatePartitionEpoch(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        maybeRemovePartitionEpoch(oldMember);
        addPartitionEpochs(newMember.assignedPartitions(), newMember.memberEpoch());
        addPartitionEpochs(newMember.partitionsPendingRevocation(), newMember.memberEpoch());
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
            removePartitionEpochs(oldMember.assignedPartitions(), oldMember.memberEpoch());
            removePartitionEpochs(oldMember.partitionsPendingRevocation(), oldMember.memberEpoch());
        }
    }

    /**
     * Removes the partition epochs based on the provided assignment.
     *
     * @param assignment    The assignment.
     * @param expectedEpoch The expected epoch.
     * @throws IllegalStateException if the epoch does not match the expected one.
     * package-private for testing.
     */
    void removePartitionEpochs(
        Map<Uuid, Set<Integer>> assignment,
        int expectedEpoch
    ) {
        assignment.forEach((topicId, assignedPartitions) -> {
            currentPartitionEpoch.compute(topicId, (__, partitionsOrNull) -> {
                if (partitionsOrNull != null) {
                    assignedPartitions.forEach(partitionId -> {
                        Integer prevValue = partitionsOrNull.remove(partitionId);
                        if (prevValue != expectedEpoch) {
                            throw new IllegalStateException(
                                String.format("Cannot remove the epoch %d from %s-%s because the partition is " +
                                    "still owned at a different epoch %d", expectedEpoch, topicId, partitionId, prevValue));
                        }
                    });
                    if (partitionsOrNull.isEmpty()) {
                        return null;
                    } else {
                        return partitionsOrNull;
                    }
                } else {
                    throw new IllegalStateException(
                        String.format("Cannot remove the epoch %d from %s because it does not have any epoch",
                            expectedEpoch, topicId));
                }
            });
        });
    }

    /**
     * Adds the partitions epoch based on the provided assignment.
     *
     * @param assignment    The assignment.
     * @param epoch         The new epoch.
     * @throws IllegalStateException if the partition already has an epoch assigned.
     * package-private for testing.
     */
    void addPartitionEpochs(
        Map<Uuid, Set<Integer>> assignment,
        int epoch
    ) {
        assignment.forEach((topicId, assignedPartitions) -> {
            currentPartitionEpoch.compute(topicId, (__, partitionsOrNull) -> {
                if (partitionsOrNull == null) {
                    partitionsOrNull = new TimelineHashMap<>(snapshotRegistry, assignedPartitions.size());
                }
                for (Integer partitionId : assignedPartitions) {
                    Integer prevValue = partitionsOrNull.put(partitionId, epoch);
                    if (prevValue != null) {
                        throw new IllegalStateException(
                            String.format("Cannot set the epoch of %s-%s to %d because the partition is " +
                                "still owned at epoch %d", topicId, partitionId, epoch, prevValue));
                    }
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

    public ConsumerGroupDescribeResponseData.DescribedGroup asDescribedGroup(
        long committedOffset,
        String defaultAssignor,
        TopicsImage topicsImage
    ) {
        ConsumerGroupDescribeResponseData.DescribedGroup describedGroup = new ConsumerGroupDescribeResponseData.DescribedGroup()
            .setGroupId(groupId)
            .setAssignorName(preferredServerAssignor(committedOffset).orElse(defaultAssignor))
            .setGroupEpoch(groupEpoch.get(committedOffset))
            .setGroupState(state.get(committedOffset).toString())
            .setAssignmentEpoch(targetAssignmentEpoch.get(committedOffset));
        members.entrySet(committedOffset).forEach(
            entry -> describedGroup.members().add(
                entry.getValue().asConsumerGroupDescribeMember(
                    targetAssignment.get(entry.getValue().memberId(), committedOffset),
                    topicsImage
                )
            )
        );
        return describedGroup;
    }

    /**
     * Create a new consumer group according to the given classic group.
     *
     * @param snapshotRegistry  The SnapshotRegistry.
     * @param metrics           The GroupCoordinatorMetricsShard.
     * @param classicGroup      The converted classic group.
     * @param topicsImage       The TopicsImage for topic id and topic name conversion.
     * @return  The created ConsumerGruop.
     */
    public static ConsumerGroup fromClassicGroup(
        SnapshotRegistry snapshotRegistry,
        GroupCoordinatorMetricsShard metrics,
        ClassicGroup classicGroup,
        TopicsImage topicsImage
    ) {
        String groupId = classicGroup.groupId();
        ConsumerGroup consumerGroup = new ConsumerGroup(snapshotRegistry, groupId, metrics);
        consumerGroup.setGroupEpoch(classicGroup.generationId());
        consumerGroup.setTargetAssignmentEpoch(classicGroup.generationId());

        classicGroup.allMembers().forEach(classicGroupMember -> {
            ConsumerPartitionAssignor.Assignment assignment = ConsumerProtocol.deserializeAssignment(
                ByteBuffer.wrap(classicGroupMember.assignment())
            );
            Map<Uuid, Set<Integer>> partitions = topicPartitionMapFromList(assignment.partitions(), topicsImage);

            ConsumerPartitionAssignor.Subscription subscription = ConsumerProtocol.deserializeSubscription(
                ByteBuffer.wrap(classicGroupMember.metadata(classicGroup.protocolName().get()))
            );

            // The target assignment and the assigned partitions of each member are set based on the last
            // assignment of the classic group. All the members are put in the Stable state. If the classic
            // group was in Preparing Rebalance or Completing Rebalance states, the classic members are
            // asked to rejoin the group to re-trigger a rebalance or collect their assignments.
            ConsumerGroupMember newMember = new ConsumerGroupMember.Builder(classicGroupMember.memberId())
                .setMemberEpoch(classicGroup.generationId())
                .setState(MemberState.STABLE)
                .setPreviousMemberEpoch(classicGroup.generationId())
                .setInstanceId(classicGroupMember.groupInstanceId().orElse(null))
                .setRackId(subscription.rackId().orElse(null))
                .setRebalanceTimeoutMs(classicGroupMember.rebalanceTimeoutMs())
                .setClientId(classicGroupMember.clientId())
                .setClientHost(classicGroupMember.clientHost())
                .setSubscribedTopicNames(subscription.topics())
                .setAssignedPartitions(partitions)
                .setClassicMemberMetadata(
                    new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                        .setSessionTimeoutMs(classicGroupMember.sessionTimeoutMs())
                        .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(
                            classicGroupMember.supportedProtocols()
                        ))
                )
                .build();
            consumerGroup.updateTargetAssignment(newMember.memberId(), new Assignment(partitions));
            consumerGroup.updateMember(newMember);
        });

        return consumerGroup;
    }

    /**
     * Populate the record list with the records needed to create the given consumer group.
     *
     * @param records The list to which the new records are added.
     */
    public void createConsumerGroupRecords(
        List<CoordinatorRecord> records
    ) {
        members().forEach((__, consumerGroupMember) ->
            records.add(CoordinatorRecordHelpers.newMemberSubscriptionRecord(groupId(), consumerGroupMember))
        );

        records.add(CoordinatorRecordHelpers.newGroupEpochRecord(groupId(), groupEpoch()));

        members().forEach((consumerGroupMemberId, consumerGroupMember) ->
            records.add(CoordinatorRecordHelpers.newTargetAssignmentRecord(
                groupId(),
                consumerGroupMemberId,
                targetAssignment(consumerGroupMemberId).partitions()
            ))
        );

        records.add(CoordinatorRecordHelpers.newTargetAssignmentEpochRecord(groupId(), groupEpoch()));

        members().forEach((__, consumerGroupMember) ->
            records.add(CoordinatorRecordHelpers.newCurrentAssignmentRecord(groupId(), consumerGroupMember))
        );
    }

    /**
     * @return The map of topic id and partition set converted from the list of TopicPartition.
     */
    private static Map<Uuid, Set<Integer>> topicPartitionMapFromList(
        List<TopicPartition> partitions,
        TopicsImage topicsImage
    ) {
        Map<Uuid, Set<Integer>> topicPartitionMap = new HashMap<>();
        partitions.forEach(topicPartition -> {
            TopicImage topicImage = topicsImage.getTopic(topicPartition.topic());
            if (topicImage != null) {
                topicPartitionMap
                    .computeIfAbsent(topicImage.id(), __ -> new HashSet<>())
                    .add(topicPartition.partition());
            }
        });
        return topicPartitionMap;
    }

    /**
     * Checks whether at least one of the given protocols can be supported. A
     * protocol can be supported if it is supported by all members that use the
     * classic protocol.
     *
     * @param memberProtocolType  The member protocol type.
     * @param memberProtocols     The set of protocol names.
     *
     * @return A boolean based on the condition mentioned above.
     */
    public boolean supportsClassicProtocols(String memberProtocolType, Set<String> memberProtocols) {
        if (ConsumerProtocol.PROTOCOL_TYPE.equals(memberProtocolType)) {
            if (isEmpty()) {
                return !memberProtocols.isEmpty();
            } else {
                return memberProtocols.stream().anyMatch(
                    name -> classicProtocolMembersSupportedProtocols.getOrDefault(name, 0) == numClassicProtocolMembers()
                );
            }
        }
        return false;
    }

    /**
     * Checks whether all the members use the classic protocol except the given member.
     *
     * @param memberId The member to remove.
     * @return A boolean indicating whether all the members use the classic protocol.
     */
    public boolean allMembersUseClassicProtocolExcept(String memberId) {
        return numClassicProtocolMembers() == members().size() - 1 &&
            !getOrMaybeCreateMember(memberId, false).useClassicProtocol();
    }
}
