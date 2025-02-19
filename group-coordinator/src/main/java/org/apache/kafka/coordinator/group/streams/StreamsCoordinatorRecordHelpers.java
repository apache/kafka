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

import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class contains helper methods to create records stored in the __consumer_offsets topic.
 */
public class StreamsCoordinatorRecordHelpers {

    public static CoordinatorRecord newStreamsGroupMemberRecord(
        String groupId,
        StreamsGroupMember member
    ) {
        Objects.requireNonNull(groupId, "groupId should not be null here");
        Objects.requireNonNull(member, "member should not be null here");

        return CoordinatorRecord.record(
            new StreamsGroupMemberMetadataKey()
                .setGroupId(groupId)
                .setMemberId(member.memberId()),
            new ApiMessageAndVersion(
                new StreamsGroupMemberMetadataValue()
                    .setRackId(member.rackId().orElse(null))
                    .setInstanceId(member.instanceId().orElse(null))
                    .setClientId(member.clientId())
                    .setClientHost(member.clientHost())
                    .setRebalanceTimeoutMs(member.rebalanceTimeoutMs())
                    .setTopologyEpoch(member.topologyEpoch())
                    .setProcessId(member.processId())
                    .setUserEndpoint(member.userEndpoint().orElse(null))
                    .setClientTags(member.clientTags().entrySet().stream().map(e ->
                        new StreamsGroupMemberMetadataValue.KeyValue()
                            .setKey(e.getKey())
                            .setValue(e.getValue())
                    ).sorted(Comparator.comparing(StreamsGroupMemberMetadataValue.KeyValue::key)).collect(Collectors.toList())),
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
        Objects.requireNonNull(groupId, "groupId should not be null here");
        Objects.requireNonNull(memberId, "memberId should not be null here");

        return CoordinatorRecord.tombstone(
            new StreamsGroupMemberMetadataKey()
                .setGroupId(groupId)
                .setMemberId(memberId)
        );
    }

    /**
     * Creates a StreamsGroupPartitionMetadata record.
     *
     * @param groupId              The streams group id.
     * @param newPartitionMetadata The partition metadata.
     * @return The record.
     */
    public static CoordinatorRecord newStreamsGroupPartitionMetadataRecord(
        String groupId,
        Map<String, org.apache.kafka.coordinator.group.streams.TopicMetadata> newPartitionMetadata
    ) {
        Objects.requireNonNull(groupId, "groupId should not be null here");
        Objects.requireNonNull(newPartitionMetadata, "newPartitionMetadata should not be null here");

        StreamsGroupPartitionMetadataValue value = new StreamsGroupPartitionMetadataValue();
        newPartitionMetadata.forEach((topicName, topicMetadata) -> {
            value.topics().add(new StreamsGroupPartitionMetadataValue.TopicMetadata()
                .setTopicId(topicMetadata.id())
                .setTopicName(topicMetadata.name())
                .setNumPartitions(topicMetadata.numPartitions())
            );
        });

        value.topics().sort(Comparator.comparing(StreamsGroupPartitionMetadataValue.TopicMetadata::topicName));

        return CoordinatorRecord.record(
            new StreamsGroupPartitionMetadataKey()
                .setGroupId(groupId),
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
        Objects.requireNonNull(groupId, "groupId should not be null here");

        return CoordinatorRecord.tombstone(
            new StreamsGroupPartitionMetadataKey()
                .setGroupId(groupId)
        );
    }

    public static CoordinatorRecord newStreamsGroupEpochRecord(
        String groupId,
        int newGroupEpoch
    ) {
        Objects.requireNonNull(groupId, "groupId should not be null here");

        return CoordinatorRecord.record(
            new StreamsGroupMetadataKey()
                .setGroupId(groupId),
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
        Objects.requireNonNull(groupId, "groupId should not be null here");

        return CoordinatorRecord.tombstone(
            new StreamsGroupMetadataKey()
                .setGroupId(groupId)
        );
    }

    public static CoordinatorRecord newStreamsGroupTargetAssignmentRecord(
        String groupId,
        String memberId,
        TasksTuple assignment
    ) {
        Objects.requireNonNull(groupId, "groupId should not be null here");
        Objects.requireNonNull(memberId, "memberId should not be null here");
        Objects.requireNonNull(assignment, "assignment should not be null here");

        List<StreamsGroupTargetAssignmentMemberValue.TaskIds> activeTaskIds = new ArrayList<>(assignment.activeTasks().size());
        for (Map.Entry<String, Set<Integer>> entry : assignment.activeTasks().entrySet()) {
            activeTaskIds.add(
                new StreamsGroupTargetAssignmentMemberValue.TaskIds()
                    .setSubtopologyId(entry.getKey())
                    .setPartitions(entry.getValue().stream().sorted().toList())
            );
        }
        activeTaskIds.sort(Comparator.comparing(StreamsGroupTargetAssignmentMemberValue.TaskIds::subtopologyId));
        List<StreamsGroupTargetAssignmentMemberValue.TaskIds> standbyTaskIds = new ArrayList<>(assignment.standbyTasks().size());
        for (Map.Entry<String, Set<Integer>> entry : assignment.standbyTasks().entrySet()) {
            standbyTaskIds.add(
                new StreamsGroupTargetAssignmentMemberValue.TaskIds()
                    .setSubtopologyId(entry.getKey())
                    .setPartitions(entry.getValue().stream().sorted().toList())
            );
        }
        standbyTaskIds.sort(Comparator.comparing(StreamsGroupTargetAssignmentMemberValue.TaskIds::subtopologyId));
        List<StreamsGroupTargetAssignmentMemberValue.TaskIds> warmupTaskIds = new ArrayList<>(assignment.warmupTasks().size());
        for (Map.Entry<String, Set<Integer>> entry : assignment.warmupTasks().entrySet()) {
            warmupTaskIds.add(
                new StreamsGroupTargetAssignmentMemberValue.TaskIds()
                    .setSubtopologyId(entry.getKey())
                    .setPartitions(entry.getValue().stream().sorted().toList())
            );
        }
        warmupTaskIds.sort(Comparator.comparing(StreamsGroupTargetAssignmentMemberValue.TaskIds::subtopologyId));

        return CoordinatorRecord.record(
            new StreamsGroupTargetAssignmentMemberKey()
                .setGroupId(groupId)
                .setMemberId(memberId),
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
        Objects.requireNonNull(groupId, "groupId should not be null here");
        Objects.requireNonNull(memberId, "memberId should not be null here");

        return CoordinatorRecord.tombstone(
            new StreamsGroupTargetAssignmentMemberKey()
                .setGroupId(groupId)
                .setMemberId(memberId)
        );
    }


    public static CoordinatorRecord newStreamsGroupTargetAssignmentEpochRecord(
        String groupId,
        int assignmentEpoch
    ) {
        Objects.requireNonNull(groupId, "groupId should not be null here");

        return CoordinatorRecord.record(
            new StreamsGroupTargetAssignmentMetadataKey()
                .setGroupId(groupId),
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
        Objects.requireNonNull(groupId, "groupId should not be null here");

        return CoordinatorRecord.tombstone(
            new StreamsGroupTargetAssignmentMetadataKey()
                .setGroupId(groupId)
        );
    }

    public static CoordinatorRecord newStreamsGroupCurrentAssignmentRecord(
        String groupId,
        StreamsGroupMember member
    ) {
        Objects.requireNonNull(groupId, "groupId should not be null here");
        Objects.requireNonNull(member, "member should not be null here");

        return CoordinatorRecord.record(
            new StreamsGroupCurrentMemberAssignmentKey()
                .setGroupId(groupId)
                .setMemberId(member.memberId()),
            new ApiMessageAndVersion(
                new StreamsGroupCurrentMemberAssignmentValue()
                    .setMemberEpoch(member.memberEpoch())
                    .setPreviousMemberEpoch(member.previousMemberEpoch())
                    .setState(member.state().value())
                    .setActiveTasks(toTaskIds(member.assignedTasks().activeTasks()))
                    .setStandbyTasks(toTaskIds(member.assignedTasks().standbyTasks()))
                    .setWarmupTasks(toTaskIds(member.assignedTasks().warmupTasks()))
                    .setActiveTasksPendingRevocation(toTaskIds(member.tasksPendingRevocation().activeTasks()))
                    .setStandbyTasksPendingRevocation(toTaskIds(member.tasksPendingRevocation().standbyTasks()))
                    .setWarmupTasksPendingRevocation(toTaskIds(member.tasksPendingRevocation().warmupTasks())),
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
        Objects.requireNonNull(groupId, "groupId should not be null here");
        Objects.requireNonNull(memberId, "memberId should not be null here");

        return CoordinatorRecord.tombstone(
            new StreamsGroupCurrentMemberAssignmentKey()
                .setGroupId(groupId)
                .setMemberId(memberId)
        );
    }

    private static List<StreamsGroupCurrentMemberAssignmentValue.TaskIds> toTaskIds(
        Map<String, Set<Integer>> tasks
    ) {
        List<StreamsGroupCurrentMemberAssignmentValue.TaskIds> taskIds = new ArrayList<>(tasks.size());
        tasks.forEach((subtopologyId, partitions) ->
            taskIds.add(new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
                .setSubtopologyId(subtopologyId)
                .setPartitions(partitions.stream().sorted().toList()))
        );
        taskIds.sort(Comparator.comparing(StreamsGroupCurrentMemberAssignmentValue.TaskIds::subtopologyId));
        return taskIds;
    }

    /**
     * Creates a StreamsTopology record.
     *
     * @param groupId  The consumer group id.
     * @param topology The new topology.
     * @return The record.
     */
    public static CoordinatorRecord newStreamsGroupTopologyRecord(String groupId,
                                                                  StreamsGroupHeartbeatRequestData.Topology topology) {
        Objects.requireNonNull(groupId, "groupId should not be null here");
        Objects.requireNonNull(topology, "topology should not be null here");

        return newStreamsGroupTopologyRecord(groupId, convertToStreamsGroupTopologyRecord(topology));
    }

    /**
     * Creates a StreamsTopology record.
     *
     * @param groupId The consumer group id.
     * @param value   The encoded topology record value.
     * @return The record.
     */
    public static CoordinatorRecord newStreamsGroupTopologyRecord(String groupId, StreamsGroupTopologyValue value) {
        Objects.requireNonNull(groupId, "groupId should not be null here");
        Objects.requireNonNull(value, "value should not be null here");

        return CoordinatorRecord.record(
            new StreamsGroupTopologyKey()
                .setGroupId(groupId),
            new ApiMessageAndVersion(value, (short) 0)
        );
    }

    /**
     * Encodes subtopologies from the Heartbeat RPC to a StreamsTopology record value.
     *
     * @param topology The new topology
     * @return The record value.
     */
    public static StreamsGroupTopologyValue convertToStreamsGroupTopologyRecord(StreamsGroupHeartbeatRequestData.Topology topology) {
        Objects.requireNonNull(topology, "topology should not be null here");

        StreamsGroupTopologyValue value = new StreamsGroupTopologyValue();
        value.setEpoch(topology.epoch());
        topology.subtopologies().forEach(subtopology -> {
            List<StreamsGroupTopologyValue.TopicInfo> repartitionSourceTopics =
                subtopology.repartitionSourceTopics().stream()
                    .map(StreamsCoordinatorRecordHelpers::convertToTopicInfo)
                    .collect(Collectors.toList());

            List<StreamsGroupTopologyValue.TopicInfo> stateChangelogTopics =
                subtopology.stateChangelogTopics().stream()
                    .map(StreamsCoordinatorRecordHelpers::convertToTopicInfo)
                    .collect(Collectors.toList());

            List<StreamsGroupTopologyValue.CopartitionGroup> copartitionGroups =
                subtopology.copartitionGroups().stream()
                    .map(copartitionGroup -> new StreamsGroupTopologyValue.CopartitionGroup()
                        .setSourceTopics(copartitionGroup.sourceTopics())
                        .setSourceTopicRegex(copartitionGroup.sourceTopicRegex())
                        .setRepartitionSourceTopics(copartitionGroup.repartitionSourceTopics())
                    )
                    .collect(Collectors.toList());

            value.subtopologies().add(
                new StreamsGroupTopologyValue.Subtopology()
                    .setSubtopologyId(subtopology.subtopologyId())
                    .setSourceTopics(subtopology.sourceTopics())
                    .setSourceTopicRegex(subtopology.sourceTopicRegex())
                    .setRepartitionSinkTopics(subtopology.repartitionSinkTopics())
                    .setRepartitionSourceTopics(repartitionSourceTopics)
                    .setStateChangelogTopics(stateChangelogTopics)
                    .setCopartitionGroups(copartitionGroups)
            );
        });
        return value;
    }

    private static StreamsGroupTopologyValue.TopicInfo convertToTopicInfo(StreamsGroupHeartbeatRequestData.TopicInfo topicInfo) {
        List<StreamsGroupTopologyValue.TopicConfig> topicConfigs = topicInfo.topicConfigs() != null ? topicInfo.topicConfigs().stream()
            .map(config -> new StreamsGroupTopologyValue.TopicConfig().setKey(config.key()).setValue(config.value()))
            .collect(Collectors.toList()) : null;
        return new StreamsGroupTopologyValue.TopicInfo()
            .setName(topicInfo.name())
            .setTopicConfigs(topicConfigs)
            .setPartitions(topicInfo.partitions())
            .setReplicationFactor(topicInfo.replicationFactor());
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
        Objects.requireNonNull(groupId, "groupId should not be null here");

        return CoordinatorRecord.tombstone(
            new StreamsGroupTopologyKey()
                .setGroupId(groupId)
        );
    }
}
