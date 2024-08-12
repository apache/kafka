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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.StaleMemberEpochException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.coordinator.group.CoordinatorRecord;
import org.apache.kafka.coordinator.group.Group;
import org.apache.kafka.coordinator.group.OffsetExpirationCondition;
import org.apache.kafka.coordinator.group.OffsetExpirationConditionImpl;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.Subtopology;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.apache.kafka.coordinator.group.streams.StreamsGroup.StreamsGroupState.ASSIGNING;
import static org.apache.kafka.coordinator.group.streams.StreamsGroup.StreamsGroupState.EMPTY;
import static org.apache.kafka.coordinator.group.streams.StreamsGroup.StreamsGroupState.RECONCILING;
import static org.apache.kafka.coordinator.group.streams.StreamsGroup.StreamsGroupState.STABLE;

/**
 * A Streams Group. All the metadata in this class are backed by records in the __consumer_offsets partitions.
 */
public class StreamsGroup implements Group {

    public enum StreamsGroupState {
        EMPTY("Empty"),
        ASSIGNING("Assigning"),
        RECONCILING("Reconciling"),
        STABLE("Stable"),
        DEAD("Dead");

        private final String name;

        private final String lowerCaseName;

        StreamsGroupState(String name) {
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
    private final TimelineObject<StreamsGroupState> state;

    /**
     * The group epoch. The epoch is incremented whenever the subscriptions are updated and it will trigger the computation of a new
     * assignment for the group.
     */
    private final TimelineInteger groupEpoch;

    /**
     * The group members.
     */
    private final TimelineHashMap<String, StreamsGroupMember> members;

    /**
     * The static group members.
     */
    private final TimelineHashMap<String, String> staticMembers;

    /**
     * The number of members supporting each assignor name.
     */
    private final TimelineHashMap<String, Integer> assignors;

    /**
     * The metadata associated with each subscribed topic name.
     */
    private final TimelineHashMap<String, TopicMetadata> subscribedTopicMetadata;

    /**
     * The target assignment epoch. An assignment epoch smaller than the group epoch means that a new assignment is required. The assignment
     * epoch is updated when a new assignment is installed.
     */
    private final TimelineInteger targetAssignmentEpoch;

    /**
     * The target assignment per member id.
     */
    private final TimelineHashMap<String, Assignment> targetAssignment;

    /**
     * Reverse lookup map representing tasks with their current member assignments.
     */
    private final TimelineHashMap<String, TimelineHashMap<Integer, String>> invertedTargetActiveTasksAssignment;
    private final TimelineHashMap<String, TimelineHashMap<Integer, String>> invertedTargetStandbyTasksAssignment;
    private final TimelineHashMap<String, TimelineHashMap<Integer, String>> invertedTargetWarmupTasksAssignment;

    /**
     * The current partition epoch maps each topic-partitions to their current epoch where the epoch is the epoch of their owners. When a
     * member revokes a partition, it removes its epochs from this map. When a member gets a partition, it adds its epochs to this map.
     */
    private final TimelineHashMap<String, TimelineHashMap<Integer, Integer>> currentActiveTasksEpoch;
    private final TimelineHashMap<String, TimelineHashMap<Integer, Integer>> currentStandbyTasksEpoch;
    private final TimelineHashMap<String, TimelineHashMap<Integer, Integer>> currentWarmupTasksEpoch;

    /**
     * The coordinator metrics.
     */
    private final GroupCoordinatorMetricsShard metrics;

    /**
     * The Streams topology.
     */
    private TimelineObject<Optional<StreamsTopology>> topology;

    /**
     * The metadata refresh deadline. It consists of a timestamp in milliseconds together with the group epoch at the time of setting it.
     * The metadata refresh time is considered as a soft state (read that it is not stored in a timeline data structure). It is like this
     * because it is not persisted to the log. The group epoch is here to ensure that the metadata refresh deadline is invalidated if the
     * group epoch does not correspond to the current group epoch. This can happen if the metadata refresh deadline is updated after having
     * refreshed the metadata but the write operation failed. In this case, the time is not automatically rolled back.
     */
    private DeadlineAndEpoch metadataRefreshDeadline = DeadlineAndEpoch.EMPTY;

    public StreamsGroup(
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
        this.assignors = new TimelineHashMap<>(snapshotRegistry, 0);
        this.subscribedTopicMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
        this.targetAssignmentEpoch = new TimelineInteger(snapshotRegistry);
        this.targetAssignment = new TimelineHashMap<>(snapshotRegistry, 0);
        this.invertedTargetActiveTasksAssignment = new TimelineHashMap<>(snapshotRegistry, 0);
        this.invertedTargetStandbyTasksAssignment = new TimelineHashMap<>(snapshotRegistry, 0);
        this.invertedTargetWarmupTasksAssignment = new TimelineHashMap<>(snapshotRegistry, 0);
        this.currentActiveTasksEpoch = new TimelineHashMap<>(snapshotRegistry, 0);
        this.currentStandbyTasksEpoch = new TimelineHashMap<>(snapshotRegistry, 0);
        this.currentWarmupTasksEpoch = new TimelineHashMap<>(snapshotRegistry, 0);
        this.metrics = Objects.requireNonNull(metrics);
        this.topology = new TimelineObject<>(snapshotRegistry, Optional.empty());
    }

    /**
     * @return The group type (Streams).
     */
    @Override
    public GroupType type() {
        return GroupType.STREAMS;
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

    public StreamsTopology topology() {
        return topology.get().orElse(null);
    }

    public void setTopology(StreamsTopology topology) {
        this.topology.set(Optional.of(topology));
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
    public StreamsGroupState state() {
        return state.get();
    }

    /**
     * @return The current state based on committed offset.
     */
    public StreamsGroupState state(long committedOffset) {
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
     * Get member id of a static member that matches the given group instance id.
     *
     * @param groupInstanceId The group instance id.
     * @return The member id corresponding to the given instance id or null if it does not exist
     */
    public String staticMemberId(String groupInstanceId) {
        return staticMembers.get(groupInstanceId);
    }

    /**
     * Gets or creates a new member but without adding it to the group. Adding a member is done via the
     * {@link StreamsGroup#updateMember(StreamsGroupMember)} method.
     *
     * @param memberId          The member id.
     * @param createIfNotExists Booleans indicating whether the member must be created if it does not exist.
     * @return A StreamsGroupMember.
     */
    public StreamsGroupMember getOrMaybeCreateMember(
        String memberId,
        boolean createIfNotExists
    ) {
        StreamsGroupMember member = members.get(memberId);
        if (member != null) {
            return member;
        }

        if (!createIfNotExists) {
            throw new UnknownMemberIdException(
                String.format("Member %s is not a member of group %s.", memberId, groupId)
            );
        }

        return new StreamsGroupMember.Builder(memberId).build();
    }

    /**
     * Gets a static member.
     *
     * @param instanceId The group instance id.
     * @return The member corresponding to the given instance id or null if it does not exist
     */
    public StreamsGroupMember staticMember(String instanceId) {
        String existingMemberId = staticMemberId(instanceId);
        return existingMemberId == null ? null : getOrMaybeCreateMember(existingMemberId, false);
    }

    /**
     * Adds or updates the member.
     *
     * @param newMember The new member state.
     */
    public void updateMember(StreamsGroupMember newMember) {
        if (newMember == null) {
            throw new IllegalArgumentException("newMember cannot be null.");
        }
        StreamsGroupMember oldMember = members.put(newMember.memberId(), newMember);
        maybeUpdateTaskEpoch(oldMember, newMember);
        updateStaticMember(newMember);
        maybeUpdateGroupState();
        maybeUpdateAssignors(oldMember, newMember);
    }

    /**
     * Updates the member id stored against the instance id if the member is a static member.
     *
     * @param newMember The new member state.
     */
    private void updateStaticMember(StreamsGroupMember newMember) {
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
        StreamsGroupMember oldMember = members.remove(memberId);
        maybeRemoveTaskEpoch(oldMember);
        removeStaticMember(oldMember);
        maybeUpdateGroupState();
    }

    /**
     * Remove the static member mapping if the removed member is static.
     *
     * @param oldMember The member to remove.
     */
    private void removeStaticMember(StreamsGroupMember oldMember) {
        if (oldMember.instanceId() != null) {
            staticMembers.remove(oldMember.instanceId());
        }
    }

    /**
     * Returns true if the member exists.
     *
     * @param memberId The member id.
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
    public Map<String, StreamsGroupMember> members() {
        return Collections.unmodifiableMap(members);
    }

    /**
     * @return An immutable Map containing all the static members keyed by instance id.
     */
    public Map<String, String> staticMembers() {
        return Collections.unmodifiableMap(staticMembers);
    }

    /**
     * Returns the target assignment of the member.
     *
     * @return The StreamsGroupMemberAssignment or an EMPTY one if it does not exist.
     */
    public Assignment targetAssignment(String memberId) {
        return targetAssignment.getOrDefault(memberId, Assignment.EMPTY);
    }

    /**
     * @return An immutable map containing all the topic partitions with their current member assignments.
     */
    public Map<String, Map<Integer, String>> invertedTargetActiveTasksAssignment() {
        return Collections.unmodifiableMap(invertedTargetActiveTasksAssignment);
    }

    public Map<String, Map<Integer, String>> invertedTargetStandbyTasksAssignment() {
        return Collections.unmodifiableMap(invertedTargetStandbyTasksAssignment);
    }

    public Map<String, Map<Integer, String>> invertedTargetWarmupTasksAssignment() {
        return Collections.unmodifiableMap(invertedTargetWarmupTasksAssignment);
    }

    /**
     * Updates the server assignors count.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdateAssignors(
        StreamsGroupMember oldMember,
        StreamsGroupMember newMember
    ) {
        maybeUpdateAssignors(assignors, oldMember, newMember);
    }

    private static void maybeUpdateAssignors(
        Map<String, Integer> serverAssignorCount,
        StreamsGroupMember oldMember,
        StreamsGroupMember newMember
    ) {
        if (oldMember != null) {
            oldMember.assignor().ifPresent(name ->
                serverAssignorCount.compute(name, StreamsGroup::decValue)
            );
        }
        if (newMember != null) {
            newMember.assignor().ifPresent(name ->
                serverAssignorCount.compute(name, StreamsGroup::incValue)
            );
        }
    }

    /**
     * Updates the target assignment of a member.
     *
     * @param memberId            The member id.
     * @param newTargetAssignment The new target assignment.
     */
    public void updateTargetAssignment(String memberId, Assignment newTargetAssignment) {
        updateInvertedTargetActiveTasksAssignment(
            memberId,
            targetAssignment.getOrDefault(memberId, new Assignment(emptyMap(), emptyMap(), emptyMap())),
            newTargetAssignment
        );
        updateInvertedTargetStandbyTasksAssignment(
            memberId,
            targetAssignment.getOrDefault(memberId, new Assignment(emptyMap(), emptyMap(), emptyMap())),
            newTargetAssignment
        );
        updateInvertedTargetWarmupTasksAssignment(
            memberId,
            targetAssignment.getOrDefault(memberId, new Assignment(emptyMap(), emptyMap(), emptyMap())),
            newTargetAssignment
        );
        targetAssignment.put(memberId, newTargetAssignment);
    }


    private void updateInvertedTargetActiveTasksAssignment(
        String memberId,
        Assignment oldTargetAssignment,
        Assignment newTargetAssignment
    ) {
        updateInvertedTargetAssignment(
            memberId,
            oldTargetAssignment,
            newTargetAssignment,
            invertedTargetActiveTasksAssignment
        );
    }

    private void updateInvertedTargetStandbyTasksAssignment(
        String memberId,
        Assignment oldTargetAssignment,
        Assignment newTargetAssignment
    ) {
        updateInvertedTargetAssignment(
            memberId,
            oldTargetAssignment,
            newTargetAssignment,
            invertedTargetStandbyTasksAssignment
        );
    }

    private void updateInvertedTargetWarmupTasksAssignment(
        String memberId,
        Assignment oldTargetAssignment,
        Assignment newTargetAssignment
    ) {
        updateInvertedTargetAssignment(
            memberId,
            oldTargetAssignment,
            newTargetAssignment,
            invertedTargetWarmupTasksAssignment
        );
    }

    /**
     * Updates the reverse lookup map of the target assignment.
     *
     * @param memberId            The member Id.
     * @param oldTargetAssignment The old target assignment.
     * @param newTargetAssignment The new target assignment.
     */
    private void updateInvertedTargetAssignment(
        String memberId,
        Assignment oldTargetAssignment,
        Assignment newTargetAssignment,
        TimelineHashMap<String, TimelineHashMap<Integer, String>> invertedTargetAssignment
    ) {
        // Combine keys from both old and new assignments.
        Set<String> allSubtopologyIds = new HashSet<>();
        allSubtopologyIds.addAll(oldTargetAssignment.activeTasks().keySet());
        allSubtopologyIds.addAll(newTargetAssignment.activeTasks().keySet());

        for (String subtopologyId : allSubtopologyIds) {
            Set<Integer> oldPartitions = oldTargetAssignment.activeTasks().getOrDefault(subtopologyId, Collections.emptySet());
            Set<Integer> newPartitions = newTargetAssignment.activeTasks().getOrDefault(subtopologyId, Collections.emptySet());

            TimelineHashMap<Integer, String> taskPartitionAssignment = invertedTargetAssignment.computeIfAbsent(
                subtopologyId, k -> new TimelineHashMap<>(snapshotRegistry, Math.max(oldPartitions.size(), newPartitions.size()))
            );

            // Remove partitions that aren't present in the new assignment only if the partition is currently
            // still assigned to the member in question.
            // If p0 was moved from A to B, and the target assignment map was updated for B first, we don't want to
            // remove the key p0 from the inverted map and undo the action when A eventually tries to update its assignment.
            for (Integer partition : oldPartitions) {
                if (!newPartitions.contains(partition) && memberId.equals(taskPartitionAssignment.get(partition))) {
                    taskPartitionAssignment.remove(partition);
                }
            }

            // Add partitions that are in the new assignment but not in the old assignment.
            for (Integer partition : newPartitions) {
                if (!oldPartitions.contains(partition)) {
                    taskPartitionAssignment.put(partition, memberId);
                }
            }

            if (taskPartitionAssignment.isEmpty()) {
                invertedTargetAssignment.remove(subtopologyId);
            } else {
                invertedTargetAssignment.put(subtopologyId, taskPartitionAssignment);
            }
        }
    }

    /**
     * Removes the target assignment of a member.
     *
     * @param memberId The member id.
     */
    public void removeTargetAssignment(String memberId) {
        updateInvertedTargetActiveTasksAssignment(
            memberId,
            targetAssignment.getOrDefault(memberId, Assignment.EMPTY),
            Assignment.EMPTY
        );
        updateInvertedTargetStandbyTasksAssignment(
            memberId,
            targetAssignment.getOrDefault(memberId, Assignment.EMPTY),
            Assignment.EMPTY
        );
        updateInvertedTargetWarmupTasksAssignment(
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
     * Returns the current epoch of a partition or -1 if the partition does not have one.
     *
     * @param topicId     The topic id.
     * @param partitionId The partition id.
     * @return The epoch or -1.
     */
    public int currentActiveTaskEpoch(
        String topicId, int partitionId
    ) {
        Map<Integer, Integer> partitions = currentActiveTasksEpoch.get(topicId);
        if (partitions == null) {
            return -1;
        } else {
            return partitions.getOrDefault(partitionId, -1);
        }
    }

    /**
     * Compute the preferred (server side) assignor for the group while taking into account the updated member. The computation relies on
     * {{@link StreamsGroup#assignors}} persisted structure but it does not update it.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     * @return An Optional containing the preferred assignor.
     */
    public Optional<String> computePreferredServerAssignor(
        StreamsGroupMember oldMember,
        StreamsGroupMember newMember
    ) {
        // Copy the current count and update it.
        Map<String, Integer> counts = new HashMap<>(this.assignors);
        maybeUpdateAssignors(counts, oldMember, newMember);

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
        return assignors.entrySet(committedOffset).stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey);
    }

    /**
     * @return An immutable Map of subscription metadata for each topic that the consumer group is subscribed to.
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
     * @param topicsImage          The current metadata for all available topics.
     * @param clusterImage         The current metadata for the Kafka cluster.
     * @return An immutable map of subscription metadata for each topic that the consumer group is subscribed to.
     */
    public Map<String, TopicMetadata> computeSubscriptionMetadata(
        TopicsImage topicsImage,
        ClusterImage clusterImage
    ) {
        Set<String> subscribedTopicNames = topology.get().map(StreamsTopology::topicSubscription).orElse(Collections.emptySet());

        // Create the topic metadata for each subscribed topic.
        Map<String, TopicMetadata> newSubscriptionMetadata = new HashMap<>(subscribedTopicNames.size());

        subscribedTopicNames.forEach(topicName -> {
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
                    if (!racks.isEmpty()) {
                        partitionRacks.put(partition, racks);
                    }
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
     * Checks if a metadata refresh is required. A refresh is required in two cases: 1) The deadline is smaller or equal to the current
     * time; 2) The group epoch associated with the deadline is larger than the current group epoch. This means that the operations which
     * updated the deadline failed.
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
     * @param memberId        The member id.
     * @param groupInstanceId The group instance id.
     * @param memberEpoch     The member epoch.
     * @param isTransactional Whether the offset commit is transactional or not. It has no impact when a consumer group is used.
     */
    @Override
    public void validateOffsetCommit(
        String memberId,
        String groupInstanceId,
        int memberEpoch,
        boolean isTransactional,
        short apiVersion
    ) throws UnknownMemberIdException, StaleMemberEpochException {
        // When the member epoch is -1, the request comes from either the admin client
        // or a consumer which does not use the group management facility. In this case,
        // the request can commit offsets if the group is empty.
        if (memberEpoch < 0 && members().isEmpty()) {
            return;
        }

        final StreamsGroupMember member = getOrMaybeCreateMember(memberId, false);
        validateMemberEpoch(memberEpoch, member.memberEpoch());
    }

    /**
     * Validates the OffsetFetch request.
     *
     * @param memberId            The member id for consumer groups.
     * @param memberEpoch         The member epoch for consumer groups.
     * @param lastCommittedOffset The last committed offsets in the timeline.
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
        if (memberId == null && memberEpoch < 0) {
            return;
        }

        final StreamsGroupMember member = members.get(memberId, lastCommittedOffset);
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
    }

    /**
     * Validates the DeleteGroups request.
     */
    @Override
    public void validateDeleteGroup() throws ApiException {
        if (state() != StreamsGroupState.EMPTY) {
            throw Errors.NON_EMPTY_GROUP.exception();
        }
    }

    @Override
    public boolean isSubscribedToTopic(String topic) {
        return false;
    }

    /**
     * Populates the list of records with tombstone(s) for deleting the group.
     *
     * @param records The list of records.
     */
    @Override
    public void createGroupTombstoneRecords(List<CoordinatorRecord> records) {
        members().forEach((memberId, member) ->
            records.add(CoordinatorStreamsRecordHelpers.newStreamsCurrentAssignmentTombstoneRecord(groupId(), memberId))
        );

        members().forEach((memberId, member) ->
            records.add(CoordinatorStreamsRecordHelpers.newStreamsTargetAssignmentTombstoneRecord(groupId(), memberId))
        );
        records.add(CoordinatorStreamsRecordHelpers.newStreamsTargetAssignmentEpochTombstoneRecord(groupId()));

        members().forEach((memberId, member) ->
            records.add(CoordinatorStreamsRecordHelpers.newStreamsGroupMemberTombstoneRecord(groupId(), memberId))
        );

        records.add(CoordinatorStreamsRecordHelpers.newStreamsGroupPartitionMetadataTombstoneRecord(groupId()));
        records.add(CoordinatorStreamsRecordHelpers.newStreamsGroupEpochTombstoneRecord(groupId()));
        records.add(CoordinatorStreamsRecordHelpers.newStreamsGroupTopologyRecordTombstone(groupId()));
    }

    @Override
    public boolean isEmpty() {
        return state() == StreamsGroupState.EMPTY;
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
     * Throws a StaleMemberEpochException if the received member epoch does not match the expected member epoch.
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
        StreamsGroupState previousState = state.get();
        StreamsGroupState newState = STABLE;
        if (members.isEmpty()) {
            newState = EMPTY;
        } else if (groupEpoch.get() > targetAssignmentEpoch.get()) {
            newState = ASSIGNING;
        } else {
            for (StreamsGroupMember member : members.values()) {
                if (!member.isReconciledTo(targetAssignmentEpoch.get())) {
                    newState = RECONCILING;
                    break;
                }
            }
        }

        state.set(newState);
        metrics.onStreamsGroupStateTransition(previousState, newState);
    }

    /**
     * Updates the tasks epochs based on the old and the new member.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdateTaskEpoch(
        StreamsGroupMember oldMember,
        StreamsGroupMember newMember
    ) {
        maybeRemoveTaskEpoch(oldMember);
        addTaskEpochs(
            newMember.assignedActiveTasks(),
            newMember.assignedStandbyTasks(),
            newMember.assignedWarmupTasks(),
            newMember.memberEpoch()
        );
        addTaskEpochs(
            newMember.activeTasksPendingRevocation(),
            emptyMap(),
            emptyMap(),
            newMember.memberEpoch()
        );
    }

    /**
     * Removes the task epochs for the provided member.
     *
     * @param oldMember The old member.
     */
    private void maybeRemoveTaskEpoch(
        StreamsGroupMember oldMember
    ) {
        if (oldMember != null) {
            removeActiveTaskEpochs(oldMember.assignedActiveTasks(), oldMember.memberEpoch());
            removeStandbyTaskEpochs(oldMember.assignedStandbyTasks(), oldMember.memberEpoch());
            removeWarmupTaskEpochs(oldMember.assignedWarmupTasks(), oldMember.memberEpoch());
            removeActiveTaskEpochs(oldMember.activeTasksPendingRevocation(), oldMember.memberEpoch());
        }
    }

    void removeActiveTaskEpochs(
        Map<String, Set<Integer>> assignment,
        int expectedEpoch
    ) {
        removeTaskEpochs(assignment, currentActiveTasksEpoch, expectedEpoch);
    }

    void removeStandbyTaskEpochs(
        Map<String, Set<Integer>> assignment,
        int expectedEpoch
    ) {
        removeTaskEpochs(assignment, currentStandbyTasksEpoch, expectedEpoch);
    }

    void removeWarmupTaskEpochs(
        Map<String, Set<Integer>> assignment,
        int expectedEpoch
    ) {
        removeTaskEpochs(assignment, currentWarmupTasksEpoch, expectedEpoch);
    }

    /**
     * Removes the task epochs based on the provided assignment.
     *
     * @param assignment    The assignment.
     * @param expectedEpoch The expected epoch.
     * @throws IllegalStateException if the epoch does not match the expected one. package-private for testing.
     */
    private void removeTaskEpochs(
        Map<String, Set<Integer>> assignment,
        TimelineHashMap<String, TimelineHashMap<Integer, Integer>> currentTasksEpoch,
        int expectedEpoch
    ) {
        assignment.forEach((subtopologyId, assignedPartitions) -> {
            currentTasksEpoch.compute(subtopologyId, (__, partitionsOrNull) -> {
                if (partitionsOrNull != null) {
                    assignedPartitions.forEach(partitionId -> {
                        Integer prevValue = partitionsOrNull.remove(partitionId);
                        if (prevValue != expectedEpoch) {
                            throw new IllegalStateException(
                                String.format("Cannot remove the epoch %d from task %s_%s because the partition is " +
                                    "still owned at a different epoch %d", expectedEpoch, subtopologyId, partitionId, prevValue));
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
                            expectedEpoch, subtopologyId));
                }
            });
        });
    }

    /**
     * Adds the partitions epoch based on the provided assignment.
     *
     * @param activeTasks  The assigned active tasks.
     * @param standbyTasks The assigned standby tasks.
     * @param warmupTasks  The assigned warmup tasks.
     * @param epoch        The new epoch.
     * @throws IllegalStateException if the partition already has an epoch assigned. package-private for testing.
     */
    void addTaskEpochs(
        Map<String, Set<Integer>> activeTasks,
        Map<String, Set<Integer>> standbyTasks,
        Map<String, Set<Integer>> warmupTasks,
        int epoch
    ) {
        addTaskEpochs(activeTasks, epoch, currentActiveTasksEpoch);
        addTaskEpochs(standbyTasks, epoch, currentStandbyTasksEpoch);
        addTaskEpochs(warmupTasks, epoch, currentWarmupTasksEpoch);
    }

    void addTaskEpochs(
        Map<String, Set<Integer>> tasks,
        int epoch,
        TimelineHashMap<String, TimelineHashMap<Integer, Integer>> currentTasksEpoch
    ) {
        tasks.forEach((subtopologyId, assignedTaskPartitions) -> {
            currentTasksEpoch.compute(subtopologyId, (__, partitionsOrNull) -> {
                if (partitionsOrNull == null) {
                    partitionsOrNull = new TimelineHashMap<>(snapshotRegistry, assignedTaskPartitions.size());
                }
                for (Integer partitionId : assignedTaskPartitions) {
                    Integer prevValue = partitionsOrNull.put(partitionId, epoch);
                    if (prevValue != null) {
                        throw new IllegalStateException(
                            String.format("Cannot set the epoch of %s-%s to %d because the partition is " +
                                "still owned at epoch %d", subtopologyId, partitionId, epoch, prevValue));
                    }
                }
                return partitionsOrNull;
            });
        });
    }

    /**
     * Decrements value by 1; returns null when reaching zero. This helper is meant to be used with Map#compute.
     */
    private static Integer decValue(String key, Integer value) {
        if (value == null) {
            return null;
        }
        return value == 1 ? null : value - 1;
    }

    /**
     * Increments value by 1; This helper is meant to be used with Map#compute.
     */
    private static Integer incValue(String key, Integer value) {
        return value == null ? 1 : value + 1;
    }

    /**
     * Populate the record list with the records needed to create the given Streams group.
     *
     * @param records The list to which the new records are added.
     */
    public void createStreamsGroupRecords(
        List<CoordinatorRecord> records
    ) {
        members().forEach((__, streamsGroupMember) ->
            records.add(CoordinatorStreamsRecordHelpers.newStreamsGroupMemberRecord(groupId(), streamsGroupMember))
        );

        records.add(CoordinatorStreamsRecordHelpers.newStreamsGroupEpochRecord(groupId(), groupEpoch()));

        members().forEach((streamsGroupMemberId, streamsGroupMember) ->
            records.add(CoordinatorStreamsRecordHelpers.newStreamsTargetAssignmentRecord(
                groupId(),
                streamsGroupMemberId,
                targetAssignment(streamsGroupMemberId).activeTasks(),
                targetAssignment(streamsGroupMemberId).standbyTasks(),
                targetAssignment(streamsGroupMemberId).warmupTasks()
            ))
        );

        records.add(CoordinatorStreamsRecordHelpers.newStreamsTargetAssignmentEpochRecord(groupId(), groupEpoch()));
        records.add(CoordinatorStreamsRecordHelpers.newStreamsGroupPartitionMetadataRecord(groupId(), subscriptionMetadata()));

        members().forEach((__, streamsGroupMember) ->
            records.add(CoordinatorStreamsRecordHelpers.newStreamsCurrentAssignmentRecord(groupId(), streamsGroupMember))
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
     * Checks whether the member has any unreleased tasks.
     *
     * @param member The member to check.
     * @return A boolean indicating whether the member has partitions in the target assignment that hasn't been revoked by other members.
     */
    public boolean waitingOnUnreleasedActiveTasks(StreamsGroupMember member) {
        if (member.state() == MemberState.UNRELEASED_TASKS) {
            for (Map.Entry<String, Set<Integer>> entry : targetAssignment().get(member.memberId()).activeTasks().entrySet()) {
                String subtopologyId = entry.getKey();
                Set<Integer> assignedActiveTasks = member.assignedActiveTasks().getOrDefault(subtopologyId, Collections.emptySet());

                for (int partition : entry.getValue()) {
                    if (!assignedActiveTasks.contains(partition) && currentActiveTaskEpoch(subtopologyId, partition) != -1) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public void setTopology(final StreamsGroupTopologyValue topology) {
        this.topology.set(Optional.of(new StreamsTopology(topology.topologyId(), topology.topology().stream().collect(Collectors.toMap(
            Subtopology::subtopology, x -> x)))));
    }
}
