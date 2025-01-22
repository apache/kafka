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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.generated.StreamsGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue.Endpoint;
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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StreamsCoordinatorRecordHelpersTest {

    @Test
    public void testNewStreamsGroupMemberRecord() {
        StreamsGroupMember member = new StreamsGroupMember.Builder("member-id")
            .setRackId("rack-id")
            .setInstanceId("instance-id")
            .setClientId("client-id")
            .setClientHost("client-host")
            .setRebalanceTimeoutMs(1000)
            .setTopologyEpoch(1)
            .setProcessId("process-id")
            .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("user-endpoint").setPort(40))
            .setClientTags(Map.of("tag1", "value1", "tag2", "value2"))
            .build();

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new StreamsGroupMemberMetadataKey()
                .setGroupId("group-id")
                .setMemberId("member-id"),
            new ApiMessageAndVersion(
                new StreamsGroupMemberMetadataValue()
                    .setRackId("rack-id")
                    .setInstanceId("instance-id")
                    .setClientId("client-id")
                    .setClientHost("client-host")
                    .setRebalanceTimeoutMs(1000)
                    .setTopologyEpoch(1)
                    .setProcessId("process-id")
                    .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("user-endpoint").setPort(40))
                    .setClientTags(List.of(
                        new StreamsGroupMemberMetadataValue.KeyValue().setKey("tag1").setValue("value1"),
                        new StreamsGroupMemberMetadataValue.KeyValue().setKey("tag2").setValue("value2")
                    )),
                (short) 0
            )
        );

        assertEquals(expectedRecord, StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord("group-id", member));
    }

    @Test
    public void testNewStreamsGroupMemberTombstoneRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new StreamsGroupMemberMetadataKey()
                .setGroupId("group-id")
                .setMemberId("member-id")
        );

        assertEquals(expectedRecord, StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord("group-id", "member-id"));
    }

    @Test
    public void testNewStreamsGroupPartitionMetadataRecord() {
        Uuid uuid1 = Uuid.randomUuid();
        Uuid uuid2 = Uuid.randomUuid();
        Map<String, TopicMetadata> newPartitionMetadata = Map.of(
            "topic1", new TopicMetadata(uuid1, "topic1", 1, Map.of(0, Set.of("rack1", "rack2"))),
            "topic2", new TopicMetadata(uuid2, "topic2", 2, Map.of(1, Set.of("rack3")))
        );

        StreamsGroupPartitionMetadataValue value = new StreamsGroupPartitionMetadataValue();
        value.topics().add(new StreamsGroupPartitionMetadataValue.TopicMetadata()
            .setTopicId(uuid1)
            .setTopicName("topic1")
            .setNumPartitions(1)
            .setPartitionMetadata(List.of(
                new StreamsGroupPartitionMetadataValue.PartitionMetadata()
                    .setPartition(0)
                    .setRacks(List.of("rack1", "rack2"))
            ))
        );
        value.topics().add(new StreamsGroupPartitionMetadataValue.TopicMetadata()
            .setTopicId(uuid2)
            .setTopicName("topic2")
            .setNumPartitions(2)
            .setPartitionMetadata(List.of(
                new StreamsGroupPartitionMetadataValue.PartitionMetadata()
                    .setPartition(1)
                    .setRacks(List.of("rack3"))
            ))
        );

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new StreamsGroupPartitionMetadataKey()
                .setGroupId("group-id"),
            new ApiMessageAndVersion(value, (short) 0)
        );

        assertEquals(expectedRecord,
            StreamsCoordinatorRecordHelpers.newStreamsGroupPartitionMetadataRecord("group-id", newPartitionMetadata));
    }

    @Test
    public void testNewStreamsGroupPartitionMetadataTombstoneRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new StreamsGroupPartitionMetadataKey()
                .setGroupId("group-id")
        );

        assertEquals(expectedRecord, StreamsCoordinatorRecordHelpers.newStreamsGroupPartitionMetadataTombstoneRecord("group-id"));
    }

    @Test
    public void testNewStreamsGroupEpochRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new StreamsGroupMetadataKey()
                .setGroupId("group-id"),
            new ApiMessageAndVersion(
                new StreamsGroupMetadataValue()
                    .setEpoch(42),
                (short) 0
            )
        );

        assertEquals(expectedRecord, StreamsCoordinatorRecordHelpers.newStreamsGroupEpochRecord("group-id", 42));
    }

    @Test
    public void testNewStreamsGroupEpochTombstoneRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new StreamsGroupMetadataKey()
                .setGroupId("group-id")
        );

        assertEquals(expectedRecord, StreamsCoordinatorRecordHelpers.newStreamsGroupEpochTombstoneRecord("group-id"));
    }

    @Test
    public void testNewStreamsGroupTargetAssignmentRecord() {
        Map<String, Set<Integer>> activeTasks = Map.of("subtopology1", Set.of(1, 2, 3));
        Map<String, Set<Integer>> standbyTasks = Map.of("subtopology2", Set.of(4, 5, 6));
        Map<String, Set<Integer>> warmupTasks = Map.of("subtopology3", Set.of(7, 8, 9));

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new StreamsGroupTargetAssignmentMemberKey()
                .setGroupId("group-id")
                .setMemberId("member-id"),
            new ApiMessageAndVersion(
                new StreamsGroupTargetAssignmentMemberValue()
                    .setActiveTasks(List.of(
                        new StreamsGroupTargetAssignmentMemberValue.TaskIds()
                            .setSubtopologyId("subtopology1")
                            .setPartitions(List.of(1, 2, 3))
                    ))
                    .setStandbyTasks(List.of(
                        new StreamsGroupTargetAssignmentMemberValue.TaskIds()
                            .setSubtopologyId("subtopology2")
                            .setPartitions(List.of(4, 5, 6))
                    ))
                    .setWarmupTasks(List.of(
                        new StreamsGroupTargetAssignmentMemberValue.TaskIds()
                            .setSubtopologyId("subtopology3")
                            .setPartitions(List.of(7, 8, 9))
                    )),
                (short) 0
            )
        );

        assertEquals(expectedRecord,
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord("group-id", "member-id",
                new TasksTuple(activeTasks, standbyTasks, warmupTasks)));
    }

    @Test
    public void testNewStreamsGroupTargetAssignmentTombstoneRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new StreamsGroupTargetAssignmentMemberKey()
                .setGroupId("group-id")
                .setMemberId("member-id")
        );

        assertEquals(expectedRecord,
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord("group-id", "member-id"));
    }

    @Test
    public void testNewStreamsGroupTargetAssignmentEpochRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new StreamsGroupTargetAssignmentMetadataKey()
                .setGroupId("group-id"),
            new ApiMessageAndVersion(
                new StreamsGroupTargetAssignmentMetadataValue()
                    .setAssignmentEpoch(42),
                (short) 0
            )
        );

        assertEquals(expectedRecord, StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentEpochRecord("group-id", 42));
    }

    @Test
    public void testNewStreamsGroupTargetAssignmentEpochTombstoneRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new StreamsGroupTargetAssignmentMetadataKey()
                .setGroupId("group-id")
        );

        assertEquals(expectedRecord, StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentEpochTombstoneRecord("group-id"));
    }

    @Test
    public void testNewStreamsGroupCurrentAssignmentRecord() {
        StreamsGroupMember member = new StreamsGroupMember.Builder("member-id")
            .setRackId("rack-id")
            .setInstanceId("instance-id")
            .setClientId("client-id")
            .setClientHost("client-host")
            .setRebalanceTimeoutMs(1000)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setState(MemberState.STABLE)
            .setTopologyEpoch(1)
            .setProcessId("process-id")
            .setUserEndpoint(new Endpoint().setHost("user-endpoint").setPort(40))
            .setClientTags(Map.of("tag1", "value1", "tag2", "value2"))
            .setAssignedTasks(new TasksTuple(
                Map.of(
                    "subtopology1", Set.of(1, 2, 3)
                ),
                Map.of(
                    "subtopology2", Set.of(4, 5, 6)
                ),
                Map.of(
                    "subtopology3", Set.of(7, 8, 9)
                )
            ))
            .setTasksPendingRevocation(new TasksTuple(
                Map.of(
                    "subtopology1", Set.of(1, 2, 3)
                ),
                Map.of(
                    "subtopology2", Set.of(4, 5, 6)
                ),
                Map.of(
                    "subtopology3", Set.of(7, 8, 9)
                )
            ))
            .build();

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new StreamsGroupCurrentMemberAssignmentKey()
                .setGroupId("group-id")
                .setMemberId("member-id"),
            new ApiMessageAndVersion(
                new StreamsGroupCurrentMemberAssignmentValue()
                    .setMemberEpoch(1)
                    .setPreviousMemberEpoch(0)
                    .setState(MemberState.STABLE.value())
                    .setActiveTasks(List.of(
                        new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
                            .setSubtopologyId("subtopology1")
                            .setPartitions(List.of(1, 2, 3))
                    ))
                    .setStandbyTasks(List.of(
                        new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
                            .setSubtopologyId("subtopology2")
                            .setPartitions(List.of(4, 5, 6))
                    ))
                    .setWarmupTasks(List.of(
                        new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
                            .setSubtopologyId("subtopology3")
                            .setPartitions(List.of(7, 8, 9))
                    ))
                    .setActiveTasksPendingRevocation(List.of(
                        new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
                            .setSubtopologyId("subtopology1")
                            .setPartitions(List.of(1, 2, 3))
                    ))
                    .setStandbyTasksPendingRevocation(List.of(
                        new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
                            .setSubtopologyId("subtopology2")
                            .setPartitions(List.of(4, 5, 6))
                    ))
                    .setWarmupTasksPendingRevocation(List.of(
                        new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
                            .setSubtopologyId("subtopology3")
                            .setPartitions(List.of(7, 8, 9))
                    )),
                (short) 0
            )
        );

        assertEquals(expectedRecord, StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord("group-id", member));
    }

    @Test
    public void testNewStreamsGroupCurrentAssignmentTombstoneRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new StreamsGroupCurrentMemberAssignmentKey()
                .setGroupId("group-id")
                .setMemberId("member-id")
        );

        assertEquals(expectedRecord,
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord("group-id", "member-id"));
    }

    @Test
    public void testNewStreamsGroupTopologyRecord() {
        StreamsGroupHeartbeatRequestData.Topology topology =
            new StreamsGroupHeartbeatRequestData.Topology()
                .setEpoch(42)
                .setSubtopologies(
                    List.of(new StreamsGroupHeartbeatRequestData.Subtopology()
                        .setSubtopologyId("subtopology-id")
                        .setRepartitionSinkTopics(List.of("foo"))
                        .setSourceTopics(List.of("bar"))
                        .setSourceTopicRegex(List.of("regex"))
                        .setRepartitionSourceTopics(
                            List.of(
                                new StreamsGroupHeartbeatRequestData.TopicInfo()
                                    .setName("repartition")
                                    .setPartitions(4)
                                    .setReplicationFactor((short) 3)
                                    .setTopicConfigs(List.of(
                                        new StreamsGroupHeartbeatRequestData.KeyValue()
                                            .setKey("config-name1")
                                            .setValue("config-value1")
                                    ))
                            )
                        )
                        .setStateChangelogTopics(
                            List.of(
                                new StreamsGroupHeartbeatRequestData.TopicInfo()
                                    .setName("changelog")
                                    .setReplicationFactor((short) 2)
                                    .setTopicConfigs(List.of(
                                        new StreamsGroupHeartbeatRequestData.KeyValue()
                                            .setKey("config-name2")
                                            .setValue("config-value2")
                                    ))
                            )
                        )
                        .setCopartitionGroups(List.of(
                            new StreamsGroupHeartbeatRequestData.CopartitionGroup()
                                .setSourceTopics(List.of((short) 0))
                                .setRepartitionSourceTopics(List.of((short) 0)),
                            new StreamsGroupHeartbeatRequestData.CopartitionGroup()
                                .setSourceTopicRegex(List.of((short) 0))
                        ))
                    )
                );

        StreamsGroupTopologyValue expectedTopology =
            new StreamsGroupTopologyValue()
                .setEpoch(42)
                .setSubtopologies(
                    List.of(new StreamsGroupTopologyValue.Subtopology()
                        .setSubtopologyId("subtopology-id")
                        .setRepartitionSinkTopics(List.of("foo"))
                        .setSourceTopics(List.of("bar"))
                        .setSourceTopicRegex(List.of("regex"))
                        .setRepartitionSourceTopics(
                            List.of(
                                new StreamsGroupTopologyValue.TopicInfo()
                                    .setName("repartition")
                                    .setPartitions(4)
                                    .setReplicationFactor((short) 3)
                                    .setTopicConfigs(List.of(
                                        new StreamsGroupTopologyValue.TopicConfig()
                                            .setKey("config-name1")
                                            .setValue("config-value1")
                                    ))
                            )
                        )
                        .setStateChangelogTopics(
                            List.of(
                                new StreamsGroupTopologyValue.TopicInfo()
                                    .setName("changelog")
                                    .setReplicationFactor((short) 2)
                                    .setTopicConfigs(List.of(
                                        new StreamsGroupTopologyValue.TopicConfig()
                                            .setKey("config-name2")
                                            .setValue("config-value2")
                                    ))
                            )
                        )
                        .setCopartitionGroups(List.of(
                            new StreamsGroupTopologyValue.CopartitionGroup()
                                .setSourceTopics(List.of((short) 0))
                                .setRepartitionSourceTopics(List.of((short) 0)),
                            new StreamsGroupTopologyValue.CopartitionGroup()
                                .setSourceTopicRegex(List.of((short) 0))
                        ))
                    )
                );

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new StreamsGroupTopologyKey()
                .setGroupId("group-id"),
            new ApiMessageAndVersion(
                expectedTopology,
                (short) 0));

        assertEquals(expectedRecord, StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord("group-id", topology));
    }

    @Test
    public void testNewStreamsGroupTopologyRecordTombstone() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new StreamsGroupTopologyKey()
                .setGroupId("group-id")
        );

        assertEquals(expectedRecord, StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecordTombstone("group-id"));
    }
}