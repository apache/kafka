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

import org.apache.kafka.common.message.StreamsGroupInitializeRequestData;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.generated.StreamsGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class contains helper methods to create records stored in the __consumer_offsets topic.
 */
@SuppressWarnings("ClassDataAbstractionCoupling")
public class CoordinatorStreamsRecordHelpers {

    private CoordinatorStreamsRecordHelpers() {
    }

    public static CoordinatorRecord newStreamsGroupMemberRecord(
        String groupId,
        StreamsGroupMember member
    ) {
        return new CoordinatorRecord(
            new ApiMessageAndVersion(
                new StreamsGroupMemberMetadataKey()
                    .setGroupId(groupId)
                    .setMemberId(member.memberId()),
                (short) 17
            ),
            new ApiMessageAndVersion(
                new StreamsGroupMemberMetadataValue()
                    .setRackId(member.rackId())
                    .setInstanceId(member.instanceId())
                    .setClientId(member.clientId())
                    .setClientHost(member.clientHost())
                    .setRebalanceTimeoutMs(member.rebalanceTimeoutMs())
                    .setTopologyId(member.topologyId())
                    .setProcessId(member.processId())
                    .setUserEndpoint(member.userEndpoint())
                    .setClientTags(member.clientTags().entrySet().stream().map(e ->
                        new StreamsGroupMemberMetadataValue.KeyValue()
                            .setKey(e.getKey())
                            .setValue(e.getValue())
                    ).collect(Collectors.toList())),
                (short) 0
            )
        );
    }

    /**
     * Creates a StreamsGroupMemberMetadata tombstone.
     *
     * @param groupId  The streams group id.
     * @param memberId The streams group member id.
     * @return The record.
     */
    public static CoordinatorRecord newStreamsGroupMemberTombstoneRecord(
        String groupId,
        String memberId
    ) {
        return new CoordinatorRecord(
            new ApiMessageAndVersion(
                new StreamsGroupMemberMetadataKey()
                    .setGroupId(groupId)
                    .setMemberId(memberId),
                (short) 17
            ),
            null // Tombstone.
        );
    }

    /**
     * Creates a StreamsGroupPartitionMetadata record.
     *
     * @param groupId                 The streams group id.
     * @param newSubscriptionMetadata The subscription metadata.
     * @return The record.
     */
    public static CoordinatorRecord newStreamsGroupPartitionMetadataRecord(
        String groupId,
        Map<String, org.apache.kafka.coordinator.group.streams.TopicMetadata> newSubscriptionMetadata
    ) {
        StreamsGroupPartitionMetadataValue value = new StreamsGroupPartitionMetadataValue();
        newSubscriptionMetadata.forEach((topicName, topicMetadata) -> {
            List<StreamsGroupPartitionMetadataValue.PartitionMetadata> partitionMetadata = new ArrayList<>();
            // If the partition rack information map is empty, store an empty list in the record.
            if (!topicMetadata.partitionRacks().isEmpty()) {
                topicMetadata.partitionRacks().forEach((partition, racks) ->
                    partitionMetadata.add(new StreamsGroupPartitionMetadataValue.PartitionMetadata()
                        .setPartition(partition)
                        .setRacks(new ArrayList<>(racks))
                    )
                );
            }
            value.topics().add(new StreamsGroupPartitionMetadataValue.TopicMetadata()
                .setTopicId(topicMetadata.id())
                .setTopicName(topicMetadata.name())
                .setNumPartitions(topicMetadata.numPartitions())
                .setPartitionMetadata(partitionMetadata)
            );
        });

        return new CoordinatorRecord(
            new ApiMessageAndVersion(
                new StreamsGroupPartitionMetadataKey()
                    .setGroupId(groupId),
                (short) 16
            ),
            new ApiMessageAndVersion(
                value,
                (short) 0
            )
        );
    }

    /**
     * Creates a StreamsGroupPartitionMetadata tombstone.
     *
     * @param groupId The streams group id.
     * @return The record.
     */
    public static CoordinatorRecord newStreamsGroupPartitionMetadataTombstoneRecord(
        String groupId
    ) {
        return new CoordinatorRecord(
            new ApiMessageAndVersion(
                new StreamsGroupPartitionMetadataKey()
                    .setGroupId(groupId),
                (short) 16
            ),
            null // Tombstone.
        );
    }

    public static CoordinatorRecord newStreamsGroupEpochRecord(
        String groupId,
        int newGroupEpoch
    ) {
        return new CoordinatorRecord(
            new ApiMessageAndVersion(
                new StreamsGroupMetadataKey()
                    .setGroupId(groupId),
                (short) 15
            ),
            new ApiMessageAndVersion(
                new StreamsGroupMetadataValue()
                    .setEpoch(newGroupEpoch),
                (short) 0
            )
        );
    }

    /**
     * Creates a StreamsGroupMetadata tombstone.
     *
     * @param groupId The streams group id.
     * @return The record.
     */
    public static CoordinatorRecord newStreamsGroupEpochTombstoneRecord(
        String groupId
    ) {
        return new CoordinatorRecord(
            new ApiMessageAndVersion(
                new StreamsGroupMetadataKey()
                    .setGroupId(groupId),
                (short) 15
            ),
            null // Tombstone.
        );
    }

    public static CoordinatorRecord newStreamsGroupTargetAssignmentRecord(
        String groupId,
        String memberId,
        Map<String, Set<Integer>> activeTasks,
        Map<String, Set<Integer>> standbyTasks,
        Map<String, Set<Integer>> warmupTasks
    ) {
        List<StreamsGroupTargetAssignmentMemberValue.TaskIds> activeTaskIds = new ArrayList<>(activeTasks.size());
        for (Map.Entry<String, Set<Integer>> entry : activeTasks.entrySet()) {
            activeTaskIds.add(
                new StreamsGroupTargetAssignmentMemberValue.TaskIds()
                    .setSubtopologyId(entry.getKey())
                    .setPartitions(new ArrayList<>(entry.getValue()))
            );
        }
        List<StreamsGroupTargetAssignmentMemberValue.TaskIds> standbyTaskIds = new ArrayList<>(standbyTasks.size());
        for (Map.Entry<String, Set<Integer>> entry : standbyTasks.entrySet()) {
            standbyTaskIds.add(
                new StreamsGroupTargetAssignmentMemberValue.TaskIds()
                    .setSubtopologyId(entry.getKey())
                    .setPartitions(new ArrayList<>(entry.getValue()))
            );
        }
        List<StreamsGroupTargetAssignmentMemberValue.TaskIds> warmupTaskIds = new ArrayList<>(warmupTasks.size());
        for (Map.Entry<String, Set<Integer>> entry : warmupTasks.entrySet()) {
            warmupTaskIds.add(
                new StreamsGroupTargetAssignmentMemberValue.TaskIds()
                    .setSubtopologyId(entry.getKey())
                    .setPartitions(new ArrayList<>(entry.getValue()))
            );
        }

        return new CoordinatorRecord(
            new ApiMessageAndVersion(
                new StreamsGroupTargetAssignmentMemberKey()
                    .setGroupId(groupId)
                    .setMemberId(memberId),
                (short) 19
            ),
            new ApiMessageAndVersion(
                new StreamsGroupTargetAssignmentMemberValue()
                    .setActiveTasks(activeTaskIds)
                    .setStandbyTasks(standbyTaskIds)
                    .setWarmupTasks(warmupTaskIds),
                (short) 0
            )
        );
    }

    /**
     * Creates a StreamsGroupTargetAssignmentMember tombstone.
     *
     * @param groupId  The streams group id.
     * @param memberId The streams group member id.
     * @return The record.
     */
    public static CoordinatorRecord newStreamsGroupTargetAssignmentTombstoneRecord(
        String groupId,
        String memberId
    ) {
        return new CoordinatorRecord(
            new ApiMessageAndVersion(
                new StreamsGroupTargetAssignmentMemberKey()
                    .setGroupId(groupId)
                    .setMemberId(memberId),
                (short) 19
            ),
            null // Tombstone.
        );
    }


    public static CoordinatorRecord newStreamsGroupTargetAssignmentEpochRecord(
        String groupId,
        int assignmentEpoch
    ) {
        return new CoordinatorRecord(
            new ApiMessageAndVersion(
                new StreamsGroupTargetAssignmentMetadataKey()
                    .setGroupId(groupId),
                (short) 18
            ),
            new ApiMessageAndVersion(
                new StreamsGroupTargetAssignmentMetadataValue()
                    .setAssignmentEpoch(assignmentEpoch),
                (short) 0
            )
        );
    }

    /**
     * Creates a StreamsGroupTargetAssignmentMetadata tombstone.
     *
     * @param groupId The streams group id.
     * @return The record.
     */
    public static CoordinatorRecord newStreamsGroupTargetAssignmentEpochTombstoneRecord(
        String groupId
    ) {
        return new CoordinatorRecord(
            new ApiMessageAndVersion(
                new StreamsGroupTargetAssignmentMetadataKey()
                    .setGroupId(groupId),
                (short) 18
            ),
            null // Tombstone.
        );
    }

    public static CoordinatorRecord newStreamsGroupCurrentAssignmentRecord(
        String groupId,
        StreamsGroupMember member
    ) {
        return new CoordinatorRecord(
            new ApiMessageAndVersion(
                new StreamsGroupCurrentMemberAssignmentKey()
                    .setGroupId(groupId)
                    .setMemberId(member.memberId()),
                (short) 20
            ),
            new ApiMessageAndVersion(
                new StreamsGroupCurrentMemberAssignmentValue()
                    .setMemberEpoch(member.memberEpoch())
                    .setPreviousMemberEpoch(member.previousMemberEpoch())
                    .setState(member.state().value())
                    .setActiveTasks(toTaskIds(member.assignedActiveTasks()))
                    .setStandbyTasks(toTaskIds(member.assignedStandbyTasks()))
                    .setWarmupTasks(toTaskIds(member.assignedWarmupTasks()))
                    .setActiveTasksPendingRevocation(toTaskIds(member.activeTasksPendingRevocation()))
                    .setStandbyTasksPendingRevocation(toTaskIds(member.standbyTasksPendingRevocation()))
                    .setWarmupTasksPendingRevocation(toTaskIds(member.warmupTasksPendingRevocation())),
                (short) 0
            )
        );
    }

    /**
     * Creates a StreamsGroupCurrentMemberAssignment tombstone.
     *
     * @param groupId  The streams group id.
     * @param memberId The streams group member id.
     * @return The record.
     */
    public static CoordinatorRecord newStreamsGroupCurrentAssignmentTombstoneRecord(
        String groupId,
        String memberId
    ) {
        return new CoordinatorRecord(
            new ApiMessageAndVersion(
                new StreamsGroupCurrentMemberAssignmentKey()
                    .setGroupId(groupId)
                    .setMemberId(memberId),
                (short) 20
            ),
            null // Tombstone
        );
    }

    private static List<StreamsGroupCurrentMemberAssignmentValue.TaskIds> toTaskIds(
        Map<String, Set<Integer>> tasks
    ) {
        List<StreamsGroupCurrentMemberAssignmentValue.TaskIds> taskIds = new ArrayList<>(tasks.size());
        tasks.forEach((subtopologyId, partitions) ->
            taskIds.add(new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
                .setSubtopologyId(subtopologyId)
                .setPartitions(new ArrayList<>(partitions)))
        );
        return taskIds;
    }

    /**
     * Creates a StreamsTopology record.
     *
     * @param groupId       The consumer group id.
     * @param subtopologies The subtopologies in the new topology.
     * @return The record.
     */
    public static CoordinatorRecord newStreamsGroupTopologyRecord(String groupId,
                                                                  List<StreamsGroupInitializeRequestData.Subtopology> subtopologies) {
        StreamsGroupTopologyValue value = new StreamsGroupTopologyValue();
        subtopologies.forEach(subtopology -> {
            List<StreamsGroupTopologyValue.TopicInfo> repartitionSourceTopics = subtopology.repartitionSourceTopics().stream()
                .map(topicInfo -> {
                    List<StreamsGroupTopologyValue.TopicConfig> topicConfigs =  topicInfo.topicConfigs() != null ? topicInfo.topicConfigs().stream()
                        .map(config -> new StreamsGroupTopologyValue.TopicConfig().setKey(config.key()).setValue(config.value()))
                        .collect(Collectors.toList()) : null;
                    return new StreamsGroupTopologyValue.TopicInfo().setName(topicInfo.name()).setTopicConfigs(topicConfigs)
                        .setPartitions(topicInfo.partitions());
                }).collect(Collectors.toList());

            List<StreamsGroupTopologyValue.TopicInfo> stateChangelogTopics = subtopology.stateChangelogTopics().stream().map(topicInfo -> {
                List<StreamsGroupTopologyValue.TopicConfig> topicConfigs = topicInfo.topicConfigs() != null ? topicInfo.topicConfigs().stream()
                    .map(config -> new StreamsGroupTopologyValue.TopicConfig().setKey(config.key()).setValue(config.value()))
                    .collect(Collectors.toList()) : null;
                return new StreamsGroupTopologyValue.TopicInfo().setName(topicInfo.name()).setTopicConfigs(topicConfigs);
            }).collect(Collectors.toList());

            value.topology().add(new StreamsGroupTopologyValue.Subtopology().setSubtopologyId(subtopology.subtopologyId())
                .setSourceTopics(subtopology.sourceTopics()).setRepartitionSinkTopics(subtopology.repartitionSinkTopics())
                .setRepartitionSourceTopics(repartitionSourceTopics).setStateChangelogTopics(stateChangelogTopics));
        });

        return new CoordinatorRecord(new ApiMessageAndVersion(
            new StreamsGroupTopologyKey()
                .setGroupId(groupId),
            (short) 21),
            new ApiMessageAndVersion(value, (short) 0));
    }

    /**
     * Creates a StreamsGroupTopology tombstone.
     *
     * @param groupId The streams group id.
     * @return The record.
     */
    public static CoordinatorRecord newStreamsGroupTopologyRecordTombstone(
        String groupId
    ) {
        return new CoordinatorRecord(
            new ApiMessageAndVersion(
                new StreamsGroupTopologyKey()
                    .setGroupId(groupId),
                (short) 21
            ),
            null // Tombstone
        );
    }
}
