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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
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
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.modern.TopicMetadata;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.modern.consumer.ResolvedRegularExpression;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupMember;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class contains helper methods to create records stored in
 * the __consumer_offsets topic.
 */
public class GroupCoordinatorRecordHelpers {

    private static final short GROUP_METADATA_VALUE_VERSION = 3;

    private GroupCoordinatorRecordHelpers() {}

    /**
     * Creates a ConsumerGroupMemberMetadata record.
     *
     * @param groupId   The consumer group id.
     * @param member    The consumer group member.
     * @return The record.
     */
    public static CoordinatorRecord newConsumerGroupMemberSubscriptionRecord(
        String groupId,
        ConsumerGroupMember member
    ) {
        List<String> topicNames = new ArrayList<>(member.subscribedTopicNames());
        Collections.sort(topicNames);
        return CoordinatorRecord.record(
            new ConsumerGroupMemberMetadataKey()
                .setGroupId(groupId)
                .setMemberId(member.memberId()),
            new ApiMessageAndVersion(
                new ConsumerGroupMemberMetadataValue()
                    .setRackId(member.rackId())
                    .setInstanceId(member.instanceId())
                    .setClientId(member.clientId())
                    .setClientHost(member.clientHost())
                    .setSubscribedTopicNames(topicNames)
                    .setSubscribedTopicRegex(member.subscribedTopicRegex())
                    .setServerAssignor(member.serverAssignorName().orElse(null))
                    .setRebalanceTimeoutMs(member.rebalanceTimeoutMs())
                    .setClassicMemberMetadata(member.classicMemberMetadata().orElse(null)),
                (short) 0
            )
        );
    }

    /**
     * Creates a ConsumerGroupMemberMetadata tombstone.
     *
     * @param groupId   The consumer group id.
     * @param memberId  The consumer group member id.
     * @return The record.
     */
    public static CoordinatorRecord newConsumerGroupMemberSubscriptionTombstoneRecord(
        String groupId,
        String memberId
    ) {
        return CoordinatorRecord.tombstone(
            new ConsumerGroupMemberMetadataKey()
                .setGroupId(groupId)
                .setMemberId(memberId)
        );
    }

    /**
     * Creates a ConsumerGroupPartitionMetadata record.
     *
     * @param groupId                   The consumer group id.
     * @param newSubscriptionMetadata   The subscription metadata.
     * @return The record.
     */
    public static CoordinatorRecord newConsumerGroupSubscriptionMetadataRecord(
        String groupId,
        Map<String, TopicMetadata> newSubscriptionMetadata
    ) {
        ConsumerGroupPartitionMetadataValue value = new ConsumerGroupPartitionMetadataValue();
        newSubscriptionMetadata.forEach((topicName, topicMetadata) ->
            value.topics().add(new ConsumerGroupPartitionMetadataValue.TopicMetadata()
                .setTopicId(topicMetadata.id())
                .setTopicName(topicMetadata.name())
                .setNumPartitions(topicMetadata.numPartitions())
            )
        );

        return CoordinatorRecord.record(
            new ConsumerGroupPartitionMetadataKey()
                .setGroupId(groupId),
            new ApiMessageAndVersion(
                value,
                (short) 0
            )
        );
    }

    /**
     * Creates a ConsumerGroupPartitionMetadata tombstone.
     *
     * @param groupId   The consumer group id.
     * @return The record.
     */
    public static CoordinatorRecord newConsumerGroupSubscriptionMetadataTombstoneRecord(
        String groupId
    ) {
        return CoordinatorRecord.tombstone(
            new ConsumerGroupPartitionMetadataKey()
                .setGroupId(groupId)
        );
    }

    /**
     * Creates a ConsumerGroupMetadata record.
     *
     * @param groupId       The consumer group id.
     * @param newGroupEpoch The consumer group epoch.
     * @return The record.
     */
    public static CoordinatorRecord newConsumerGroupEpochRecord(
        String groupId,
        int newGroupEpoch
    ) {
        return CoordinatorRecord.record(
            new ConsumerGroupMetadataKey()
                .setGroupId(groupId),
            new ApiMessageAndVersion(
                new ConsumerGroupMetadataValue()
                    .setEpoch(newGroupEpoch),
                (short) 0
            )
        );
    }

    /**
     * Creates a ConsumerGroupMetadata tombstone.
     *
     * @param groupId   The consumer group id.
     * @return The record.
     */
    public static CoordinatorRecord newConsumerGroupEpochTombstoneRecord(
        String groupId
    ) {
        return CoordinatorRecord.tombstone(
            new ConsumerGroupMetadataKey()
                .setGroupId(groupId)
        );
    }

    /**
     * Creates a ConsumerGroupTargetAssignmentMember record.
     *
     * @param groupId       The consumer group id.
     * @param memberId      The consumer group member id.
     * @param partitions    The target partitions of the member.
     * @return The record.
     */
    public static CoordinatorRecord newConsumerGroupTargetAssignmentRecord(
        String groupId,
        String memberId,
        Map<Uuid, Set<Integer>> partitions
    ) {
        List<ConsumerGroupTargetAssignmentMemberValue.TopicPartition> topicPartitions =
            new ArrayList<>(partitions.size());

        for (Map.Entry<Uuid, Set<Integer>> entry : partitions.entrySet()) {
            topicPartitions.add(
                new ConsumerGroupTargetAssignmentMemberValue.TopicPartition()
                    .setTopicId(entry.getKey())
                    .setPartitions(new ArrayList<>(entry.getValue()))
            );
        }

        return CoordinatorRecord.record(
            new ConsumerGroupTargetAssignmentMemberKey()
                .setGroupId(groupId)
                .setMemberId(memberId),
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMemberValue()
                    .setTopicPartitions(topicPartitions),
                (short) 0
            )
        );
    }

    /**
     * Creates a ConsumerGroupTargetAssignmentMember tombstone.
     *
     * @param groupId       The consumer group id.
     * @param memberId      The consumer group member id.
     * @return The record.
     */
    public static CoordinatorRecord newConsumerGroupTargetAssignmentTombstoneRecord(
        String groupId,
        String memberId
    ) {
        return CoordinatorRecord.tombstone(
            new ConsumerGroupTargetAssignmentMemberKey()
                .setGroupId(groupId)
                .setMemberId(memberId)
        );
    }

    /**
     * Creates a ConsumerGroupTargetAssignmentMetadata record.
     *
     * @param groupId           The consumer group id.
     * @param assignmentEpoch   The consumer group epoch.
     * @return The record.
     */
    public static CoordinatorRecord newConsumerGroupTargetAssignmentEpochRecord(
        String groupId,
        int assignmentEpoch
    ) {
        return CoordinatorRecord.record(
            new ConsumerGroupTargetAssignmentMetadataKey()
                .setGroupId(groupId),
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMetadataValue()
                    .setAssignmentEpoch(assignmentEpoch),
                (short) 0
            )
        );
    }

    /**
     * Creates a ConsumerGroupTargetAssignmentMetadata tombstone.
     *
     * @param groupId   The consumer group id.
     * @return The record.
     */
    public static CoordinatorRecord newConsumerGroupTargetAssignmentEpochTombstoneRecord(
        String groupId
    ) {
        return CoordinatorRecord.tombstone(
            new ConsumerGroupTargetAssignmentMetadataKey()
                .setGroupId(groupId)
        );
    }

    /**
     * Creates a ConsumerGroupCurrentMemberAssignment record.
     *
     * @param groupId   The consumer group id.
     * @param member    The consumer group member.
     * @return The record.
     */
    public static CoordinatorRecord newConsumerGroupCurrentAssignmentRecord(
        String groupId,
        ConsumerGroupMember member
    ) {
        return CoordinatorRecord.record(
            new ConsumerGroupCurrentMemberAssignmentKey()
                .setGroupId(groupId)
                .setMemberId(member.memberId()),
            new ApiMessageAndVersion(
                new ConsumerGroupCurrentMemberAssignmentValue()
                    .setMemberEpoch(member.memberEpoch())
                    .setPreviousMemberEpoch(member.previousMemberEpoch())
                    .setState(member.state().value())
                    .setAssignedPartitions(toTopicPartitions(member.assignedPartitions()))
                    .setPartitionsPendingRevocation(toTopicPartitions(member.partitionsPendingRevocation())),
                (short) 0
            )
        );
    }

    /**
     * Creates a ConsumerGroupCurrentMemberAssignment tombstone.
     *
     * @param groupId   The consumer group id.
     * @param memberId  The consumer group member id.
     * @return The record.
     */
    public static CoordinatorRecord newConsumerGroupCurrentAssignmentTombstoneRecord(
        String groupId,
        String memberId
    ) {
        return CoordinatorRecord.tombstone(
            new ConsumerGroupCurrentMemberAssignmentKey()
                .setGroupId(groupId)
                .setMemberId(memberId)
        );
    }

    /**
     * Creates a ConsumerGroupRegularExpression record.
     *
     * @param groupId                       The consumer group id.
     * @param regex                         The regular expression.
     * @param resolvedRegularExpression     The metadata associated with the regular expression.
     * @return The record.
     */
    public static CoordinatorRecord newConsumerGroupRegularExpressionRecord(
        String groupId,
        String regex,
        ResolvedRegularExpression resolvedRegularExpression
    ) {
        List<String> topics = new ArrayList<>(resolvedRegularExpression.topics);
        Collections.sort(topics);

        return CoordinatorRecord.record(
            new ConsumerGroupRegularExpressionKey()
                .setGroupId(groupId)
                .setRegularExpression(regex),
            new ApiMessageAndVersion(
                new ConsumerGroupRegularExpressionValue()
                    .setTopics(topics)
                    .setVersion(resolvedRegularExpression.version)
                    .setTimestamp(resolvedRegularExpression.timestamp),
                (short) 0
            )
        );
    }

    /**
     * Creates a ConsumerGroupRegularExpression tombstone.
     *
     * @param groupId   The consumer group id.
     * @param regex     The regular expression.
     * @return The record.
     */
    public static CoordinatorRecord newConsumerGroupRegularExpressionTombstone(
        String groupId,
        String regex
    ) {
        return CoordinatorRecord.tombstone(
            new ConsumerGroupRegularExpressionKey()
                .setGroupId(groupId)
                .setRegularExpression(regex)
        );
    }

    /**
     * Creates a GroupMetadata record.
     *
     * @param group              The classic group.
     * @param assignment         The classic group assignment.
     * @return The record.
     */
    public static CoordinatorRecord newGroupMetadataRecord(
        ClassicGroup group,
        Map<String, byte[]> assignment
    ) {
        List<GroupMetadataValue.MemberMetadata> members = new ArrayList<>(group.allMembers().size());
        group.allMembers().forEach(member -> {
            byte[] subscription = group.protocolName().map(member::metadata).orElse(null);
            if (subscription == null) {
                throw new IllegalStateException("Attempted to write non-empty group metadata with no defined protocol.");
            }

            byte[] memberAssignment = assignment.get(member.memberId());
            if (memberAssignment == null) {
                throw new IllegalStateException("Attempted to write member " + member.memberId() +
                    " of group " + group.groupId() + " with no assignment.");
            }

            members.add(
                new GroupMetadataValue.MemberMetadata()
                    .setMemberId(member.memberId())
                    .setClientId(member.clientId())
                    .setClientHost(member.clientHost())
                    .setRebalanceTimeout(member.rebalanceTimeoutMs())
                    .setSessionTimeout(member.sessionTimeoutMs())
                    .setGroupInstanceId(member.groupInstanceId().orElse(null))
                    .setSubscription(subscription)
                    .setAssignment(memberAssignment)
            );
        });

        return CoordinatorRecord.record(
            new GroupMetadataKey()
                .setGroup(group.groupId()),
            new ApiMessageAndVersion(
                new GroupMetadataValue()
                    .setProtocol(group.protocolName().orElse(null))
                    .setProtocolType(group.protocolType().orElse(""))
                    .setGeneration(group.generationId())
                    .setLeader(group.leaderOrNull())
                    .setCurrentStateTimestamp(group.currentStateTimestampOrDefault())
                    .setMembers(members),
                GROUP_METADATA_VALUE_VERSION
            )
        );
    }

    /**
     * Creates a GroupMetadata tombstone.
     *
     * @param groupId  The group id.
     * @return The record.
     */
    public static CoordinatorRecord newGroupMetadataTombstoneRecord(
        String groupId
    ) {
        return CoordinatorRecord.tombstone(
            new GroupMetadataKey()
                .setGroup(groupId)
        );
    }

    /**
     * Creates an empty GroupMetadata record.
     *
     * @param group              The classic group.
     * @return The record.
     */
    public static CoordinatorRecord newEmptyGroupMetadataRecord(
        ClassicGroup group
    ) {
        return CoordinatorRecord.record(
            new GroupMetadataKey()
                .setGroup(group.groupId()),
            new ApiMessageAndVersion(
                new GroupMetadataValue()
                    .setProtocol(null)
                    .setProtocolType("")
                    .setGeneration(0)
                    .setLeader(null)
                    .setCurrentStateTimestamp(group.currentStateTimestampOrDefault())
                    .setMembers(Collections.emptyList()),
                GROUP_METADATA_VALUE_VERSION
            )
        );
    }

    /**
     * Creates an OffsetCommit record.
     *
     * @param groupId           The group id.
     * @param topic             The topic name.
     * @param partitionId       The partition id.
     * @param offsetAndMetadata The offset and metadata.
     * @return The record.
     */
    public static CoordinatorRecord newOffsetCommitRecord(
        String groupId,
        String topic,
        int partitionId,
        OffsetAndMetadata offsetAndMetadata
    ) {
        short version = offsetCommitValueVersion(offsetAndMetadata.expireTimestampMs.isPresent());

        return CoordinatorRecord.record(
            new OffsetCommitKey()
                .setGroup(groupId)
                .setTopic(topic)
                .setPartition(partitionId),
            new ApiMessageAndVersion(
                new OffsetCommitValue()
                    .setOffset(offsetAndMetadata.committedOffset)
                    .setLeaderEpoch(offsetAndMetadata.leaderEpoch.orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
                    .setMetadata(offsetAndMetadata.metadata)
                    .setCommitTimestamp(offsetAndMetadata.commitTimestampMs)
                    // Version 1 has a non-empty expireTimestamp field
                    .setExpireTimestamp(offsetAndMetadata.expireTimestampMs.orElse(OffsetCommitRequest.DEFAULT_TIMESTAMP)),
                version
            )
        );
    }

    static short offsetCommitValueVersion(boolean expireTimestampMs) {
        if (expireTimestampMs) {
            return 1;
        } else {
            // Serialize with the highest supported non-flexible version
            // until a tagged field is introduced or the version is bumped.
            return  3;
        }
    }

    /**
     * Creates an OffsetCommit tombstone record.
     *
     * @param groupId           The group id.
     * @param topic             The topic name.
     * @param partitionId       The partition id.
     * @return The record.
     */
    public static CoordinatorRecord newOffsetCommitTombstoneRecord(
        String groupId,
        String topic,
        int partitionId
    ) {
        return CoordinatorRecord.tombstone(
            new OffsetCommitKey()
                .setGroup(groupId)
                .setTopic(topic)
                .setPartition(partitionId)
        );
    }

    /**
     * Creates a ShareGroupMemberMetadata record.
     *
     * @param groupId   The consumer group id.
     * @param member    The consumer group member.
     * @return The record.
     */
    public static CoordinatorRecord newShareGroupMemberSubscriptionRecord(
        String groupId,
        ShareGroupMember member
    ) {
        List<String> topicNames = new ArrayList<>(member.subscribedTopicNames());
        Collections.sort(topicNames);
        return CoordinatorRecord.record(
            new ShareGroupMemberMetadataKey()
                .setGroupId(groupId)
                .setMemberId(member.memberId()),
            new ApiMessageAndVersion(
                new ShareGroupMemberMetadataValue()
                    .setRackId(member.rackId())
                    .setClientId(member.clientId())
                    .setClientHost(member.clientHost())
                    .setSubscribedTopicNames(topicNames),
                (short) 0
            )
        );
    }

    /**
     * Creates a ShareGroupMemberMetadata tombstone.
     *
     * @param groupId   The share group id.
     * @param memberId  The share group member id.
     * @return The record.
     */
    public static CoordinatorRecord newShareGroupMemberSubscriptionTombstoneRecord(
        String groupId,
        String memberId
    ) {
        return CoordinatorRecord.tombstone(
            new ShareGroupMemberMetadataKey()
                .setGroupId(groupId)
                .setMemberId(memberId)
        );
    }

    /**
     * Creates a ShareGroupPartitionMetadata record.
     *
     * @param groupId                   The group id.
     * @param newSubscriptionMetadata   The subscription metadata.
     * @return The record.
     */
    public static CoordinatorRecord newShareGroupSubscriptionMetadataRecord(
        String groupId,
        Map<String, TopicMetadata> newSubscriptionMetadata
    ) {
        ShareGroupPartitionMetadataValue value = new ShareGroupPartitionMetadataValue();
        newSubscriptionMetadata.forEach((topicName, topicMetadata) ->
            value.topics().add(new ShareGroupPartitionMetadataValue.TopicMetadata()
                .setTopicId(topicMetadata.id())
                .setTopicName(topicMetadata.name())
                .setNumPartitions(topicMetadata.numPartitions())
            )
        );

        return CoordinatorRecord.record(
            new ShareGroupPartitionMetadataKey()
                .setGroupId(groupId),
            new ApiMessageAndVersion(
                value,
                (short) 0
            )
        );
    }

    /**
     * Creates a ShareGroupPartitionMetadata tombstone.
     *
     * @param groupId   The group id.
     * @return The record.
     */
    public static CoordinatorRecord newShareGroupSubscriptionMetadataTombstoneRecord(
        String groupId
    ) {
        return CoordinatorRecord.tombstone(
            new ShareGroupPartitionMetadataKey()
                .setGroupId(groupId)
        );
    }

    /**
     * Creates a ShareGroupMetadata record.
     *
     * @param groupId       The group id.
     * @param newGroupEpoch The group epoch.
     * @return The record.
     */
    public static CoordinatorRecord newShareGroupEpochRecord(
        String groupId,
        int newGroupEpoch
    ) {
        return CoordinatorRecord.record(
            new ShareGroupMetadataKey()
                .setGroupId(groupId),
            new ApiMessageAndVersion(
                new ShareGroupMetadataValue()
                    .setEpoch(newGroupEpoch),
                (short) 0
            )
        );
    }

    /**
     * Creates a ShareGroupMetadata tombstone.
     *
     * @param groupId   The group id.
     * @return The record.
     */
    public static CoordinatorRecord newShareGroupEpochTombstoneRecord(
        String groupId
    ) {
        return CoordinatorRecord.tombstone(
            new ShareGroupMetadataKey()
                .setGroupId(groupId)
        );
    }

    /**
     * Creates a ShareGroupTargetAssignmentMember record.
     *
     * @param groupId       The group id.
     * @param memberId      The group member id.
     * @param partitions    The target partitions of the member.
     * @return The record.
     */
    public static CoordinatorRecord newShareGroupTargetAssignmentRecord(
        String groupId,
        String memberId,
        Map<Uuid, Set<Integer>> partitions
    ) {
        List<ShareGroupTargetAssignmentMemberValue.TopicPartition> topicPartitions =
            new ArrayList<>(partitions.size());

        for (Map.Entry<Uuid, Set<Integer>> entry : partitions.entrySet()) {
            topicPartitions.add(
                new ShareGroupTargetAssignmentMemberValue.TopicPartition()
                    .setTopicId(entry.getKey())
                    .setPartitions(new ArrayList<>(entry.getValue()))
            );
        }

        return CoordinatorRecord.record(
            new ShareGroupTargetAssignmentMemberKey()
                .setGroupId(groupId)
                .setMemberId(memberId),
            new ApiMessageAndVersion(
                new ShareGroupTargetAssignmentMemberValue()
                    .setTopicPartitions(topicPartitions),
                (short) 0
            )
        );
    }

    /**
     * Creates a ShareGroupTargetAssignmentMember tombstone.
     *
     * @param groupId       The group id.
     * @param memberId      The group member id.
     * @return The record.
     */
    public static CoordinatorRecord newShareGroupTargetAssignmentTombstoneRecord(
        String groupId,
        String memberId
    ) {
        return CoordinatorRecord.tombstone(
            new ShareGroupTargetAssignmentMemberKey()
                .setGroupId(groupId)
                .setMemberId(memberId)
        );
    }

    /**
     * Creates a ShareGroupTargetAssignmentMetadata record.
     *
     * @param groupId           The group id.
     * @param assignmentEpoch   The group epoch.
     * @return The record.
     */
    public static CoordinatorRecord newShareGroupTargetAssignmentEpochRecord(
        String groupId,
        int assignmentEpoch
    ) {
        return CoordinatorRecord.record(
            new ShareGroupTargetAssignmentMetadataKey()
                .setGroupId(groupId),
            new ApiMessageAndVersion(
                new ShareGroupTargetAssignmentMetadataValue()
                    .setAssignmentEpoch(assignmentEpoch),
                (short) 0
            )
        );
    }

    /**
     * Creates a ShareGroupTargetAssignmentMetadata tombstone.
     *
     * @param groupId   The group id.
     * @return The record.
     */
    public static CoordinatorRecord newShareGroupTargetAssignmentEpochTombstoneRecord(
        String groupId
    ) {
        return CoordinatorRecord.tombstone(
            new ShareGroupTargetAssignmentMetadataKey()
                .setGroupId(groupId)
        );
    }

    /**
     * Creates a ShareGroupCurrentMemberAssignment record.
     *
     * @param groupId   The group id.
     * @param member    The group member.
     * @return The record.
     */
    public static CoordinatorRecord newShareGroupCurrentAssignmentRecord(
        String groupId,
        ShareGroupMember member
    ) {
        return CoordinatorRecord.record(
            new ShareGroupCurrentMemberAssignmentKey()
                .setGroupId(groupId)
                .setMemberId(member.memberId()),
            new ApiMessageAndVersion(
                new ShareGroupCurrentMemberAssignmentValue()
                    .setMemberEpoch(member.memberEpoch())
                    .setPreviousMemberEpoch(member.previousMemberEpoch())
                    .setState(member.state().value())
                    .setAssignedPartitions(toShareGroupTopicPartitions(member.assignedPartitions())),
                (short) 0
            )
        );
    }

    /**
     * Creates a ConsumerGroupCurrentMemberAssignment tombstone.
     *
     * @param groupId   The consumer group id.
     * @param memberId  The consumer group member id.
     * @return The record.
     */
    public static CoordinatorRecord newShareGroupCurrentAssignmentTombstoneRecord(
        String groupId,
        String memberId
    ) {
        return CoordinatorRecord.tombstone(
            new ShareGroupCurrentMemberAssignmentKey()
                .setGroupId(groupId)
                .setMemberId(memberId)
        );
    }

    private static List<ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions> toTopicPartitions(
        Map<Uuid, Set<Integer>> topicPartitions
    ) {
        List<ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions> topics = new ArrayList<>(topicPartitions.size());
        topicPartitions.forEach((topicId, partitions) ->
            topics.add(new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                .setTopicId(topicId)
                .setPartitions(new ArrayList<>(partitions)))
        );
        return topics;
    }

    private static List<ShareGroupCurrentMemberAssignmentValue.TopicPartitions> toShareGroupTopicPartitions(
        Map<Uuid, Set<Integer>> topicPartitions
    ) {
        List<ShareGroupCurrentMemberAssignmentValue.TopicPartitions> topics = new ArrayList<>(topicPartitions.size());
        topicPartitions.forEach((topicId, partitions) ->
            topics.add(new ShareGroupCurrentMemberAssignmentValue.TopicPartitions()
                .setTopicId(topicId)
                .setPartitions(new ArrayList<>(partitions)))
        );
        return topics;
    }
}
