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

import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
import org.apache.kafka.coordinator.group.classic.ClassicGroupMember;
import org.apache.kafka.coordinator.group.classic.ClassicGroupState;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupRegularExpressionKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupRegularExpressionValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.generated.GroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.TopicMetadata;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.modern.consumer.ResolvedRegularExpression;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineHashSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
import static org.apache.kafka.coordinator.group.Group.GroupType.CLASSIC;
import static org.apache.kafka.coordinator.group.Group.GroupType.CONSUMER;
import static org.apache.kafka.coordinator.group.Group.GroupType.SHARE;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.EMPTY;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.STABLE;

public class GroupMetadataStore {

    private SnapshotRegistry snapshotRegistry;
    private final GroupCoordinatorMetricsShard metrics;
    private Time time;
    private final LogContext logContext = new LogContext();

    /**
     * The groups keyed by their name.
     */
    private final TimelineHashMap<String, Group> groups;

    /**
     * The group ids keyed by topic names.
     */
    private final TimelineHashMap<String, TimelineHashSet<String>> groupsByTopics;

    public GroupMetadataStore(SnapshotRegistry snapshotRegistry, GroupCoordinatorMetricsShard metrics, Time time) {
        this.snapshotRegistry = snapshotRegistry;
        if (this.snapshotRegistry == null)
            this.snapshotRegistry = new SnapshotRegistry(logContext);
        this.metrics = Objects.requireNonNull(metrics, "GroupCoordinatorMetricsShard must be set.");
        this.time = time;
        if (this.time == null)
            this.time = Time.SYSTEM;

        this.groups = new TimelineHashMap<>(snapshotRegistry, 0);
        this.groupsByTopics = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    /**
     * Returns the snapshot registry.
     *
     * @return The snapshot registry.
     */
    public SnapshotRegistry snapshotRegistry() {
        return snapshotRegistry;
    }


    /**
     * @return The group corresponding to the group id or throw GroupIdNotFoundException.
     */
    public Group group(String groupId) throws GroupIdNotFoundException {
        Group group = groups.get(groupId, Long.MAX_VALUE);
        if (group == null) {
            throw new GroupIdNotFoundException(String.format("Group %s not found.", groupId));
        }
        return group;
    }

    /**
     * @return The group corresponding to the group id at the given committed offset
     *         or throw GroupIdNotFoundException.
     */
    public Group group(String groupId, long committedOffset) throws GroupIdNotFoundException {
        Group group = groups.get(groupId, committedOffset);
        if (group == null) {
            throw new GroupIdNotFoundException(String.format("Group %s not found.", groupId));
        }
        return group;
    }

    /**
     * @return The set of groups subscribed to the topic.
     */
    public Set<String> groupsSubscribedToTopic(String topicName) {
        Set<String> groups = groupsByTopics.get(topicName);
        return groups != null ? groups : Set.of();
    }

    /**
     * @return The set of all groups' ids.
     */
    public Set<String> groupIds() {
        return Collections.unmodifiableSet(this.groups.keySet());
    }

    /**
     * Subscribes a group to a topic.
     *
     * @param groupId   The group id.
     * @param topicName The topic name.
     */
    public void subscribeGroupToTopic(
        String groupId,
        String topicName
    ) {
        groupsByTopics
            .computeIfAbsent(topicName, __ -> new TimelineHashSet<>(snapshotRegistry, 1))
            .add(groupId);
    }

    /**
     * Unsubscribes a group from a topic.
     *
     * @param groupId   The group id.
     * @param topicName The topic name.
     */
    public void unsubscribeGroupFromTopic(
        String groupId,
        String topicName
    ) {
        groupsByTopics.computeIfPresent(topicName, (__, groupIds) -> {
            groupIds.remove(groupId);
            return groupIds.isEmpty() ? null : groupIds;
        });
    }

    /**
     * Removes the group.
     *
     * @param groupId The group id.
     *
     * @return The type of the removed group.
     */
    public Group removeGroup(
        String groupId
    ) {
        return groups.remove(groupId);
    }

    /**
     * Replays ConsumerGroupMemberMetadataKey/Value to update the hard state of
     * the consumer group. It updates the subscription part of the member or
     * delete the member.
     *
     * @param key   A ConsumerGroupMemberMetadataKey key.
     * @param value A ConsumerGroupMemberMetadataValue record.
     */
    public void replay(
        ConsumerGroupMemberMetadataKey key,
        ConsumerGroupMemberMetadataValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();

        ConsumerGroup consumerGroup;
        try {
            consumerGroup = getOrMaybeCreatePersistedConsumerGroup(groupId, value != null);
        } catch (GroupIdNotFoundException ex) {
            // If the group does not exist and a tombstone is replayed, we can ignore it.
            return;
        }

        Set<String> oldSubscribedTopicNames = new HashSet<>(consumerGroup.subscribedTopicNames().keySet());

        if (value != null) {
            ConsumerGroupMember oldMember = consumerGroup.getOrMaybeCreateMember(memberId, true);
            consumerGroup.updateMember(new ConsumerGroupMember.Builder(oldMember)
                .updateWith(value)
                .build());
        } else {
            ConsumerGroupMember oldMember;
            try {
                oldMember = consumerGroup.getOrMaybeCreateMember(memberId, false);
            } catch (UnknownMemberIdException ex) {
                // If the member does not exist, we can ignore it.
                return;
            }

            if (oldMember.memberEpoch() != LEAVE_GROUP_MEMBER_EPOCH) {
                throw new IllegalStateException("Received a tombstone record to delete member " + memberId
                    + " but did not receive ConsumerGroupCurrentMemberAssignmentValue tombstone.");
            }
            if (consumerGroup.targetAssignment().containsKey(memberId)) {
                throw new IllegalStateException("Received a tombstone record to delete member " + memberId
                    + " but did not receive ConsumerGroupTargetAssignmentMetadataValue tombstone.");
            }
            consumerGroup.removeMember(memberId);
        }

        updateGroupsByTopics(groupId, oldSubscribedTopicNames, consumerGroup.subscribedTopicNames().keySet());
    }

    /**
     * Replays ConsumerGroupMetadataKey/Value to update the hard state of
     * the consumer group. It updates the group epoch of the consumer
     * group or deletes the consumer group.
     *
     * @param key   A ConsumerGroupMetadataKey key.
     * @param value A ConsumerGroupMetadataValue record.
     */
    public void replay(
        ConsumerGroupMetadataKey key,
        ConsumerGroupMetadataValue value
    ) {
        String groupId = key.groupId();

        if (value != null) {
            ConsumerGroup consumerGroup = getOrMaybeCreatePersistedConsumerGroup(groupId, true);
            consumerGroup.setGroupEpoch(value.epoch());
        } else {
            ConsumerGroup consumerGroup;
            try {
                consumerGroup = getOrMaybeCreatePersistedConsumerGroup(groupId, false);
            } catch (GroupIdNotFoundException ex) {
                // If the group does not exist, we can ignore the tombstone.
                return;
            }

            if (!consumerGroup.members().isEmpty()) {
                throw new IllegalStateException("Received a tombstone record to delete group " + groupId
                    + " but the group still has " + consumerGroup.members().size() + " members.");
            }
            if (!consumerGroup.targetAssignment().isEmpty()) {
                throw new IllegalStateException("Received a tombstone record to delete group " + groupId
                    + " but the target assignment still has " + consumerGroup.targetAssignment().size()
                    + " members.");
            }
            if (consumerGroup.assignmentEpoch() != -1) {
                throw new IllegalStateException("Received a tombstone record to delete group " + groupId
                    + " but did not receive ConsumerGroupTargetAssignmentMetadataValue tombstone.");
            }
            removeGroup(groupId);
        }

    }

    /**
     * Replays ConsumerGroupPartitionMetadataKey/Value to update the hard state of
     * the consumer group. It updates the subscription metadata of the consumer
     * group.
     *
     * @param key   A ConsumerGroupPartitionMetadataKey key.
     * @param value A ConsumerGroupPartitionMetadataValue record.
     */
    public void replay(
        ConsumerGroupPartitionMetadataKey key,
        ConsumerGroupPartitionMetadataValue value
    ) {
        String groupId = key.groupId();

        ConsumerGroup group;
        try {
            group = getOrMaybeCreatePersistedConsumerGroup(groupId, value != null);
        } catch (GroupIdNotFoundException ex) {
            // If the group does not exist, we can ignore the tombstone.
            return;
        }

        if (value != null) {
            Map<String, TopicMetadata> subscriptionMetadata = new HashMap<>();
            value.topics().forEach(topicMetadata -> {
                subscriptionMetadata.put(topicMetadata.topicName(), TopicMetadata.fromRecord(topicMetadata));
            });
            group.setSubscriptionMetadata(subscriptionMetadata);
        } else {
            group.setSubscriptionMetadata(Map.of());
        }
    }

    /**
     * Replays ConsumerGroupTargetAssignmentMemberKey/Value to update the hard state of
     * the consumer group. It updates the target assignment of the member or deletes it.
     *
     * @param key   A ConsumerGroupTargetAssignmentMemberKey key.
     * @param value A ConsumerGroupTargetAssignmentMemberValue record.
     */
    public void replay(
        ConsumerGroupTargetAssignmentMemberKey key,
        ConsumerGroupTargetAssignmentMemberValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();

        if (value != null) {
            ConsumerGroup group = getOrMaybeCreatePersistedConsumerGroup(groupId, true);
            group.updateTargetAssignment(memberId, Assignment.fromRecord(value));
        } else {
            ConsumerGroup group;
            try {
                group = getOrMaybeCreatePersistedConsumerGroup(groupId, false);
            } catch (GroupIdNotFoundException ex) {
                // If the group does not exist, we can ignore the tombstone.
                return;
            }
            group.removeTargetAssignment(memberId);
        }
    }

    /**
     * Replays ConsumerGroupTargetAssignmentMetadataKey/Value to update the hard state of
     * the consumer group. It updates the target assignment epoch or set it to -1 to signal
     * that it has been deleted.
     *
     * @param key   A ConsumerGroupTargetAssignmentMetadataKey key.
     * @param value A ConsumerGroupTargetAssignmentMetadataValue record.
     */
    public void replay(
        ConsumerGroupTargetAssignmentMetadataKey key,
        ConsumerGroupTargetAssignmentMetadataValue value
    ) {
        String groupId = key.groupId();

        if (value != null) {
            ConsumerGroup group = getOrMaybeCreatePersistedConsumerGroup(groupId, true);
            group.setTargetAssignmentEpoch(value.assignmentEpoch());
        } else {
            ConsumerGroup group;
            try {
                group = getOrMaybeCreatePersistedConsumerGroup(groupId, false);
            } catch (GroupIdNotFoundException ex) {
                // If the group does not exist, we can ignore the tombstone.
                return;
            }
            if (!group.targetAssignment().isEmpty()) {
                throw new IllegalStateException("Received a tombstone record to delete target assignment of " + groupId
                    + " but the assignment still has " + group.targetAssignment().size() + " members.");
            }
            group.setTargetAssignmentEpoch(-1);
        }
    }

    /**
     * Replays ConsumerGroupCurrentMemberAssignmentKey/Value to update the hard state of
     * the consumer group. It updates the assignment of a member or deletes it.
     *
     * @param key   A ConsumerGroupCurrentMemberAssignmentKey key.
     * @param value A ConsumerGroupCurrentMemberAssignmentValue record.
     */
    public void replay(
        ConsumerGroupCurrentMemberAssignmentKey key,
        ConsumerGroupCurrentMemberAssignmentValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();

        if (value != null) {
            ConsumerGroup group = getOrMaybeCreatePersistedConsumerGroup(groupId, true);
            ConsumerGroupMember oldMember = group.getOrMaybeCreateMember(memberId, true);
            ConsumerGroupMember newMember = new ConsumerGroupMember.Builder(oldMember)
                .updateWith(value)
                .build();
            group.updateMember(newMember);
        } else {
            ConsumerGroup group;
            try {
                group = getOrMaybeCreatePersistedConsumerGroup(groupId, false);
            } catch (GroupIdNotFoundException ex) {
                // If the group does not exist, we can ignore the tombstone.
                return;
            }

            ConsumerGroupMember oldMember;
            try {
                oldMember = group.getOrMaybeCreateMember(memberId, false);
            } catch (UnknownMemberIdException ex) {
                // If the member does not exist, we can ignore the tombstone.
                return;
            }

            ConsumerGroupMember newMember = new ConsumerGroupMember.Builder(oldMember)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setPreviousMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setAssignedPartitions(Map.of())
                .setPartitionsPendingRevocation(Map.of())
                .build();
            group.updateMember(newMember);
        }
    }

    /**
     * Replays ConsumerGroupRegularExpressionKey/Value to update the hard state of
     * the consumer group.
     *
     * @param key   A ConsumerGroupRegularExpressionKey key.
     * @param value A ConsumerGroupRegularExpressionValue record.
     */
    public void replay(
        ConsumerGroupRegularExpressionKey key,
        ConsumerGroupRegularExpressionValue value
    ) {
        String groupId = key.groupId();
        String regex = key.regularExpression();

        ConsumerGroup consumerGroup;
        try {
            consumerGroup = getOrMaybeCreatePersistedConsumerGroup(groupId, value != null);
        } catch (GroupIdNotFoundException ex) {
            // If the group does not exist and a tombstone is replayed, we can ignore it.
            return;
        }

        Set<String> oldSubscribedTopicNames = new HashSet<>(consumerGroup.subscribedTopicNames().keySet());

        if (value != null) {
            consumerGroup.updateResolvedRegularExpression(
                regex,
                new ResolvedRegularExpression(
                    new HashSet<>(value.topics()),
                    value.version(),
                    value.timestamp()
                )
            );
        } else {
            consumerGroup.removeResolvedRegularExpression(regex);
        }

        updateGroupsByTopics(groupId, oldSubscribedTopicNames, consumerGroup.subscribedTopicNames().keySet());
    }

    /**
     * Replays GroupMetadataKey/Value to update the soft state of
     * the classic group.
     *
     * @param key   A GroupMetadataKey key.
     * @param value A GroupMetadataValue record.
     */
    public void replay(
        GroupMetadataKey key,
        GroupMetadataValue value
    ) {
        String groupId = key.group();

        if (value == null)  {
            // Tombstone. Group should be removed.
            removeGroup(groupId);
        } else {
            List<ClassicGroupMember> loadedMembers = new ArrayList<>();
            for (GroupMetadataValue.MemberMetadata member : value.members()) {
                int rebalanceTimeout = member.rebalanceTimeout() == -1 ?
                    member.sessionTimeout() : member.rebalanceTimeout();

                JoinGroupRequestData.JoinGroupRequestProtocolCollection supportedProtocols = new JoinGroupRequestData.JoinGroupRequestProtocolCollection();
                supportedProtocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
                    .setName(value.protocol())
                    .setMetadata(member.subscription()));

                ClassicGroupMember loadedMember = new ClassicGroupMember(
                    member.memberId(),
                    Optional.ofNullable(member.groupInstanceId()),
                    member.clientId(),
                    member.clientHost(),
                    rebalanceTimeout,
                    member.sessionTimeout(),
                    value.protocolType(),
                    supportedProtocols,
                    member.assignment()
                );

                loadedMembers.add(loadedMember);
            }

            String protocolType = value.protocolType();

            ClassicGroup classicGroup = new ClassicGroup(
                this.logContext,
                groupId,
                loadedMembers.isEmpty() ? EMPTY : STABLE,
                time,
                value.generation(),
                protocolType == null || protocolType.isEmpty() ? Optional.empty() : Optional.of(protocolType),
                Optional.ofNullable(value.protocol()),
                Optional.ofNullable(value.leader()),
                value.currentStateTimestamp() == -1 ? Optional.empty() : Optional.of(value.currentStateTimestamp())
            );

            loadedMembers.forEach(member -> classicGroup.add(member, null));
            groups.put(groupId, classicGroup);

            classicGroup.setSubscribedTopics(
                classicGroup.computeSubscribedTopics()
            );
        }
    }

    /**
     * The method should be called on the replay path.
     * Gets or maybe creates a consumer group and updates the groups map if a new group is created.
     *
     * @param groupId           The group id.
     * @param createIfNotExists A boolean indicating whether the group should be
     *                          created if it does not exist.
     *
     * @return A ConsumerGroup.
     * @throws GroupIdNotFoundException if the group does not exist and createIfNotExists is false or
     *                                  if the group is not a consumer group.
     * @throws IllegalStateException    if the group does not have the expected type.
     * Package private for testing.
     */
    ConsumerGroup getOrMaybeCreatePersistedConsumerGroup(
        String groupId,
        boolean createIfNotExists
    ) throws GroupIdNotFoundException, IllegalStateException {
        Group group = groups.get(groupId);

        if (group == null && !createIfNotExists) {
            throw new GroupIdNotFoundException(String.format("Consumer group %s not found", groupId));
        }

        if (group == null) {
            ConsumerGroup consumerGroup = new ConsumerGroup(snapshotRegistry, groupId, metrics);
            groups.put(groupId, consumerGroup);
            return consumerGroup;
        } else if (group.type() == CONSUMER) {
            return (ConsumerGroup) group;
        } else if (group.type() == CLASSIC && ((ClassicGroup) group).isSimpleGroup()) {
            // If the group is a simple classic group, it was automatically created to hold committed
            // offsets if no group existed. Simple classic groups are not backed by any records
            // in the __consumer_offsets topic hence we can safely replace it here. Without this,
            // replaying consumer group records after offset commit records would not work.
            ConsumerGroup consumerGroup = new ConsumerGroup(snapshotRegistry, groupId, metrics);
            groups.put(groupId, consumerGroup);
            return consumerGroup;
        } else {
            throw new IllegalStateException(String.format("Group %s is not a consumer group", groupId));
        }
    }

    /**
     * Gets or maybe creates a share group.
     *
     * @param groupId           The group id.
     * @param createIfNotExists A boolean indicating whether the group should be
     *                          created if it does not exist.
     *
     * @return A ShareGroup.
     * @throws GroupIdNotFoundException if the group does not exist and createIfNotExists is false or
     *                                  if the group is not a consumer group.
     *
     * Package private for testing.
     */
    ShareGroup getOrMaybeCreatePersistedShareGroup(
        String groupId,
        boolean createIfNotExists
    ) throws GroupIdNotFoundException {
        Group group = groups.get(groupId);

        if (group == null && !createIfNotExists) {
            throw new IllegalStateException(String.format("Share group %s not found.", groupId));
        }

        if (group == null) {
            ShareGroup shareGroup = new ShareGroup(snapshotRegistry, groupId);
            groups.put(groupId, shareGroup);
            return shareGroup;
        }

        if (group.type() != SHARE) {
            // We don't support upgrading/downgrading between protocols at the moment so
            // we throw an exception if a group exists with the wrong type.
            throw new GroupIdNotFoundException(String.format("Group %s is not a share group.", groupId));
        }

        return (ShareGroup) group;
    }

    /**
     * Gets or maybe creates a classic group.
     *
     * @param groupId           The group id.
     * @param createIfNotExists A boolean indicating whether the group should be
     *                          created if it does not exist.
     *
     * @return A ClassicGroup.
     * @throws GroupIdNotFoundException if the group does not exist and createIfNotExists is false or
     *                                  if the group is not a classic group.
     *
     * Package private for testing.
     */
    ClassicGroup getOrMaybeCreateClassicGroup(
        String groupId,
        boolean createIfNotExists
    ) throws GroupIdNotFoundException {
        Group group = groups.get(groupId);

        if (group == null && !createIfNotExists) {
            throw new GroupIdNotFoundException(String.format("Classic group %s not found.", groupId));
        }

        if (group == null) {
            ClassicGroup classicGroup = new ClassicGroup(logContext, groupId, ClassicGroupState.EMPTY, time);
            groups.put(groupId, classicGroup);
            return classicGroup;
        } else {
            if (group.type() == CLASSIC) {
                return (ClassicGroup) group;
            } else {
                throw new GroupIdNotFoundException(String.format("Group %s is not a classic group.", groupId));
            }
        }
    }

    /**
     * Updates the group by topics mapping.
     *
     * @param groupId               The group id.
     * @param oldSubscribedTopics   The old group subscriptions.
     * @param newSubscribedTopics   The new group subscriptions.
     */
    private void updateGroupsByTopics(
        String groupId,
        Set<String> oldSubscribedTopics,
        Set<String> newSubscribedTopics
    ) {
        if (oldSubscribedTopics.isEmpty()) {
            newSubscribedTopics.forEach(topicName ->
                subscribeGroupToTopic(groupId, topicName)
            );
        } else if (newSubscribedTopics.isEmpty()) {
            oldSubscribedTopics.forEach(topicName ->
                unsubscribeGroupFromTopic(groupId, topicName)
            );
        } else {
            oldSubscribedTopics.forEach(topicName -> {
                if (!newSubscribedTopics.contains(topicName)) {
                    unsubscribeGroupFromTopic(groupId, topicName);
                }
            });
            newSubscribedTopics.forEach(topicName -> {
                if (!oldSubscribedTopics.contains(topicName)) {
                    subscribeGroupToTopic(groupId, topicName);
                }
            });
        }
    }
}
