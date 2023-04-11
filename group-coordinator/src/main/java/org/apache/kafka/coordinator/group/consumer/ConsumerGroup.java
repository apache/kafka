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
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.coordinator.group.Group;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineInteger;
import org.apache.kafka.timeline.TimelineObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

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
     * The metadata of the subscribed topics.
     */
    private final TimelineHashMap<String, TopicMetadata> subscribedTopicMetadata;

    /**
     * The assignment epoch. An assignment epoch smaller than the group epoch means
     * that a new assignment is required. The assignment epoch is updated when a new
     * assignment is installed.
     */
    private final TimelineInteger assignmentEpoch;

    /**
     * The target assignment.
     */
    private final TimelineHashMap<String, Assignment> assignments;

    /**
     * The current partition epoch maps each topic-partitions to their current epoch where
     * the epoch is the epoch of their owners. When a member revokes a partition, it removes
     * itself from this map. When a member gets a partition, it adds itself to this map.
     */
    private final TimelineHashMap<Uuid, TimelineHashMap<Integer, Integer>> currentPartitionEpoch;

    public ConsumerGroup(
        SnapshotRegistry snapshotRegistry,
        String groupId
    ) {
        this.snapshotRegistry = Objects.requireNonNull(snapshotRegistry);
        this.groupId = Objects.requireNonNull(groupId);
        this.state = new TimelineObject<>(snapshotRegistry, ConsumerGroupState.EMPTY);
        this.groupEpoch = new TimelineInteger(snapshotRegistry);
        this.members = new TimelineHashMap<>(snapshotRegistry, 0);
        this.subscribedTopicMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
        this.assignmentEpoch = new TimelineInteger(snapshotRegistry);
        this.assignments = new TimelineHashMap<>(snapshotRegistry, 0);
        this.currentPartitionEpoch = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    /**
     * The type of this group.
     *
     * @return The group type (Consumer).
     */
    @Override
    public GroupType type() {
        return GroupType.CONSUMER;
    }

    /**
     * The state of this group.
     *
     * @return The current state as a String.
     */
    @Override
    public String stateAsString() {
        return state.get().toString();
    }

    /**
     * The group id.
     *
     * @return The group id.
     */
    @Override
    public String groupId() {
        return groupId;
    }

    /**
     * The state of this group.
     *
     * @return The current state.
     */
    public ConsumerGroupState state() {
        return state.get();
    }

    /**
     * Returns the current group epoch.
     *
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
     * Returns the current assignment epoch.
     *
     * @return The current assignment epoch.
     */
    public int assignmentEpoch() {
        return assignmentEpoch.get();
    }

    /**
     * Sets the assignment epoch.
     *
     * @param assignmentEpoch The new assignment epoch.
     */
    public void setAssignmentEpoch(int assignmentEpoch) {
        this.assignmentEpoch.set(assignmentEpoch);
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
        ConsumerGroupMember oldMember = members.put(newMember.memberId(), newMember);
        maybeUpdatePartitionEpoch(oldMember, newMember);
        maybeUpdateGroupState();
    }

    /**
     * Remove the member from the group.
     *
     * @param memberId The member id to remove.
     */
    public void removeMember(String memberId) {
        ConsumerGroupMember member = members.remove(memberId);
        maybeRemovePartitionEpoch(member);
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
     * Returns the number of members in the group.
     *
     * @return The number of members.
     */
    public int numMembers() {
        return members.size();
    }

    /**
     * Returns the members keyed by their id.
     *
     * @return A immutable Map containing all the members.
     */
    public Map<String, ConsumerGroupMember> members() {
        return Collections.unmodifiableMap(members);
    }

    /**
     * Returns the current target assignment of the member.
     *
     * @return The ConsumerGroupMemberAssignment or an EMPTY one if it does not
     *         exist.
     */
    public Assignment targetAssignment(String memberId) {
        return assignments.getOrDefault(memberId, Assignment.EMPTY);
    }

    /**
     * Updates target assignment of a member.
     *
     * @param memberId              The member id.
     * @param newTargetAssignment   The new target assignment.
     */
    public void updateTargetAssignment(String memberId, Assignment newTargetAssignment) {
        assignments.put(memberId, newTargetAssignment);
    }

    /**
     * Removes the target assignment of a member.
     *
     * @param memberId The member id.
     */
    public void removeTargetAssignment(String memberId) {
        assignments.remove(memberId);
    }

    /**
     * Returns the target assignments for the entire group.
     *
     * @return A immutable Map containing all the target assignments.
     */
    public Map<String, Assignment> targetAssignments() {
        return Collections.unmodifiableMap(assignments);
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
     * using the provided assignor for the member.
     *
     * @param updatedMemberId       The member id.
     * @param serverAssignorNameOpt The assignor name.
     *
     * @return An Optional containing the preferred assignor.
     */
    public Optional<String> preferredServerAssignor(
        String updatedMemberId,
        Optional<String> serverAssignorNameOpt
    ) {
        Map<String, Integer> counts = new HashMap<>();

        serverAssignorNameOpt.ifPresent(serverAssignorName ->
            counts.put(serverAssignorName, 1)
        );

        members.forEach((memberId, member) -> {
            if (!memberId.equals(updatedMemberId) && member.serverAssignorName().isPresent()) {
                counts.compute(member.serverAssignorName().get(), (k, v) -> v == null ? 1 : v + 1);
            }
        });

        return counts.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey);
    }

    /**
     * Returns the subscription metadata for all the topics whose
     * members are subscribed to.
     *
     * @return An immutable Map containing the subscription metadata.
     */
    public Map<String, TopicMetadata> subscriptionMetadata() {
        return Collections.unmodifiableMap(subscribedTopicMetadata);
    }

    /**
     * Updates the subscription metadata. This replace the previous one.
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
     * Computes new subscription metadata but with specific information for
     * a member.
     *
     * @param memberId                      The member id.
     * @param updatedMemberSubscriptions    The member's updated topic subscriptions.
     * @param topicsImage                   The topic metadata.
     *
     * @return The new subscription metadata as an immutable Map.
     */
    public Map<String, TopicMetadata> computeSubscriptionMetadata(
        String memberId,
        List<String> updatedMemberSubscriptions,
        TopicsImage topicsImage
    ) {
        Map<String, TopicMetadata> newSubscriptionMetadata = new HashMap<>(subscriptionMetadata().size());

        Consumer<List<String>> updateSubscription = subscribedTopicNames -> {
            subscribedTopicNames.forEach(topicName ->
                newSubscriptionMetadata.computeIfAbsent(topicName, __ -> {
                    TopicImage topicImage = topicsImage.getTopic(topicName);
                    if (topicImage == null) {
                        return null;
                    } else {
                        return new TopicMetadata(
                            topicImage.id(),
                            topicImage.name(),
                            topicImage.partitions().size()
                        );
                    }
                })
            );
        };

        if (updatedMemberSubscriptions != null) {
            updateSubscription.accept(updatedMemberSubscriptions);
        }

        members.forEach((mid, member) -> {
            if (!mid.equals(memberId)) {
                updateSubscription.accept(member.subscribedTopicNames());
            }
        });

        return Collections.unmodifiableMap(newSubscriptionMetadata);
    }

    /**
     * Updates the current state of the group.
     */
    private void maybeUpdateGroupState() {
        if (members.isEmpty()) {
            state.set(ConsumerGroupState.EMPTY);
        } else if (groupEpoch.get() > assignmentEpoch.get()) {
            state.set(ConsumerGroupState.ASSIGNING);
        } else {
            for (Map.Entry<String, ConsumerGroupMember> keyValue : members.entrySet()) {
                ConsumerGroupMember member = keyValue.getValue();
                if (member.nextMemberEpoch() != assignmentEpoch.get() || member.state() != ConsumerGroupMember.MemberState.STABLE) {
                    state.set(ConsumerGroupState.RECONCILING);
                    return;
                }
            }

            state.set(ConsumerGroupState.STABLE);
        }
    }

    /**
     * Updates the partition epochs based on the old and the new member.
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
     */
    private void addPartitionEpochs(
        Map<Uuid, Set<Integer>> assignment,
        int epoch
    ) {
        assignment.forEach((topicId, assignedPartitions) -> {
            currentPartitionEpoch.compute(topicId, (__, partitionsOrNull) -> {
                if (partitionsOrNull == null) partitionsOrNull = new TimelineHashMap<>(snapshotRegistry, 1);
                for (Integer partitionId : assignedPartitions) {
                    partitionsOrNull.put(partitionId, epoch);
                }
                return partitionsOrNull;
            });
        });
    }

}
