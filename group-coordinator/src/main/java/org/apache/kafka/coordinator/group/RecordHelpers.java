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
import org.apache.kafka.coordinator.group.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.consumer.TopicMetadata;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataValue;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RecordHelpers {
    private RecordHelpers() {}

    public static Record newMemberSubscriptionRecord(
        String groupId,
        ConsumerGroupMember member
    ) {
        return new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupMemberMetadataKey()
                    .setGroupId(groupId)
                    .setMemberId(member.memberId()),
                (short) 5
            ),
            new ApiMessageAndVersion(
                new ConsumerGroupMemberMetadataValue()
                    .setRackId(member.rackId())
                    .setInstanceId(member.instanceId())
                    .setClientId(member.clientId())
                    .setClientHost(member.clientHost())
                    .setSubscribedTopicNames(member.subscribedTopicNames())
                    .setSubscribedTopicRegex(member.subscribedTopicRegex())
                    .setServerAssignor(member.serverAssignorName().orElse(null))
                    .setRebalanceTimeoutMs(member.rebalanceTimeoutMs())
                    .setAssignors(member.clientAssignors().stream().map(assignorState ->
                        new ConsumerGroupMemberMetadataValue.Assignor()
                            .setName(assignorState.name())
                            .setReason(assignorState.reason())
                            .setMinimumVersion(assignorState.minimumVersion())
                            .setMaximumVersion(assignorState.maximumVersion())
                            .setVersion(assignorState.metadataVersion())
                            .setMetadata(assignorState.metadataBytes().array())
                    ).collect(Collectors.toList())),
                (short) 0
            )
        );
    }

    public static Record newMemberSubscriptionTombstoneRecord(
        String groupId,
        String memberId
    ) {
        return new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupMemberMetadataKey()
                    .setGroupId(groupId)
                    .setMemberId(memberId),
                (short) 5
            ),
            null // Tombstone.
        );
    }

    public static Record newGroupSubscriptionMetadataRecord(
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

        return new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupPartitionMetadataKey()
                    .setGroupId(groupId),
                (short) 4
            ),
            new ApiMessageAndVersion(
                value,
                (short) 0
            )
        );
    }

    public static Record newGroupSubscriptionMetadataTombstoneRecord(
        String groupId
    ) {
        return new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupPartitionMetadataKey()
                    .setGroupId(groupId),
                (short) 4
            ),
            null // Tombstone.
        );
    }

    public static Record newGroupEpochRecord(
        String groupId,
        int newGroupEpoch
    ) {
        return new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupMetadataKey()
                    .setGroupId(groupId),
                (short) 3
            ),
            new ApiMessageAndVersion(
                new ConsumerGroupMetadataValue()
                    .setEpoch(newGroupEpoch),
                (short) 0
            )
        );
    }

    public static Record newGroupEpochTombstoneRecord(
        String groupId
    ) {
        return new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupMetadataKey()
                    .setGroupId(groupId),
                (short) 3
            ),
            null // Tombstone.
        );
    }

    public static Record newTargetAssignmentRecord(
        String groupId,
        String memberId,
        Map<Uuid, Set<Integer>> partitions
    ) {
        return new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMemberKey()
                    .setGroupId(groupId)
                    .setMemberId(memberId),
                (short) 7
            ),
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMemberValue()
                    .setTopicPartitions(partitions.entrySet().stream()
                        .map(keyValue -> new ConsumerGroupTargetAssignmentMemberValue.TopicPartition()
                            .setTopicId(keyValue.getKey())
                            .setPartitions(new ArrayList<>(keyValue.getValue())))
                        .collect(Collectors.toList())),
                (short) 0
            )
        );
    }

    public static Record newTargetAssignmentTombstoneRecord(
        String groupId,
        String memberId
    ) {
        return new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMemberKey()
                    .setGroupId(groupId)
                    .setMemberId(memberId),
                (short) 7
            ),
            null // Tombstone.
        );
    }

    public static Record newTargetAssignmentEpochRecord(
        String groupId,
        int groupEpoch
    ) {
        return new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMetadataKey()
                    .setGroupId(groupId),
                (short) 6
            ),
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMetadataValue()
                    .setAssignmentEpoch(groupEpoch),
                (short) 0
            )
        );
    }

    public static Record newTargetAssignmentEpochTombstoneRecord(
        String groupId
    ) {
        return new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMetadataKey()
                    .setGroupId(groupId),
                (short) 6
            ),
            null // Tombstone.
        );
    }

    public static Record newCurrentAssignmentRecord(
        String groupId,
        ConsumerGroupMember member
    ) {
        return new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupCurrentMemberAssignmentKey()
                    .setGroupId(groupId)
                    .setMemberId(member.memberId()),
                (short) 8
            ),
            new ApiMessageAndVersion(
                new ConsumerGroupCurrentMemberAssignmentValue()
                    .setMemberEpoch(member.memberEpoch())
                    .setPreviousMemberEpoch(member.previousMemberEpoch())
                    .setTargetMemberEpoch(member.nextMemberEpoch())
                    .setAssigned(topicPartitions(member.assigned()))
                    .setRevoking(topicPartitions(member.revoking()))
                    .setAssigning(topicPartitions(member.assigning())),
                (short) 0
            )
        );
    }

    private static List<ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions> topicPartitions(
        Map<Uuid, Set<Integer>> topicPartitions
    ) {
        return topicPartitions.entrySet().stream()
            .map(keyValue -> new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                .setTopicId(keyValue.getKey())
                .setPartitions(new ArrayList<>(keyValue.getValue())))
            .collect(Collectors.toList());
    }

    public static Record newCurrentAssignmentTombstoneRecord(
        String groupId,
        String memberId
    ) {
        return new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupCurrentMemberAssignmentKey()
                    .setGroupId(groupId)
                    .setMemberId(memberId),
                (short) 8
            ),
            null // Tombstone
        );
    }
}
