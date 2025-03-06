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
import org.apache.kafka.coordinator.group.generated.StreamsGroupTargetAssignmentMemberValue.TaskIds;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.TaskRole;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasks;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksTuple;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class StreamsCoordinatorRecordHelpersTest {

    public static final String CLIENT_HOST = "client-host";
    public static final String CLIENT_ID = "client-id";
    public static final String CONFIG_NAME_1 = "config-name1";
    public static final String CONFIG_NAME_2 = "config-name2";
    public static final String CONFIG_VALUE_1 = "config-value1";
    public static final String CONFIG_VALUE_2 = "config-value2";
    public static final String GROUP_ID = "group-id";
    public static final String INSTANCE_ID = "instance-id";
    public static final String MEMBER_ID = "member-id";
    public static final String PROCESS_ID = "process-id";
    public static final String RACK_1 = "rack1";
    public static final String RACK_2 = "rack2";
    public static final String RACK_3 = "rack3";
    public static final String SUBTOPOLOGY_1 = "subtopology1";
    public static final String SUBTOPOLOGY_2 = "subtopology2";
    public static final String SUBTOPOLOGY_3 = "subtopology3";
    public static final String TAG_1 = "tag1";
    public static final String TAG_2 = "tag2";
    public static final String TOPIC_1 = "topic1";
    public static final String TOPIC_2 = "topic2";
    public static final String TOPIC_BAR = "bar";
    public static final String TOPIC_CHANGELOG = "changelog";
    public static final String TOPIC_FOO = "foo";
    public static final String TOPIC_REGEX = "regex";
    public static final String TOPIC_REPARTITION = "repartition";
    public static final String USER_ENDPOINT = "user-endpoint";
    public static final String VALUE_1 = "value1";
    public static final String VALUE_2 = "value2";
    public static final int REBALANCE_TIMEOUT_MS = 1000;
    public static final int USER_ENDPOINT_PORT = 40;

    @Test
    public void testNewStreamsGroupMemberRecord() {
        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_ID)
            .setRackId(RACK_1)
            .setInstanceId(INSTANCE_ID)
            .setClientId(CLIENT_ID)
            .setClientHost(CLIENT_HOST)
            .setRebalanceTimeoutMs(REBALANCE_TIMEOUT_MS)
            .setTopologyEpoch(1)
            .setProcessId(PROCESS_ID)
            .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost(USER_ENDPOINT).setPort(USER_ENDPOINT_PORT))
            .setClientTags(Map.of(TAG_1, VALUE_1, TAG_2, VALUE_2))
            .build();

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new StreamsGroupMemberMetadataKey()
                .setGroupId(GROUP_ID)
                .setMemberId(MEMBER_ID),
            new ApiMessageAndVersion(
                new StreamsGroupMemberMetadataValue()
                    .setRackId(RACK_1)
                    .setInstanceId(INSTANCE_ID)
                    .setClientId(CLIENT_ID)
                    .setClientHost(CLIENT_HOST)
                    .setRebalanceTimeoutMs(REBALANCE_TIMEOUT_MS)
                    .setTopologyEpoch(1)
                    .setProcessId(PROCESS_ID)
                    .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost(USER_ENDPOINT).setPort(USER_ENDPOINT_PORT))
                    .setClientTags(List.of(
                        new StreamsGroupMemberMetadataValue.KeyValue().setKey(TAG_1).setValue(VALUE_1),
                        new StreamsGroupMemberMetadataValue.KeyValue().setKey(TAG_2).setValue(VALUE_2)
                    )),
                (short) 0
            )
        );

        assertEquals(expectedRecord, StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(GROUP_ID, member));
    }

    @Test
    public void testNewStreamsGroupMemberRecordWithNullRackId() {
        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_ID)
            .setRackId(null)
            .setInstanceId(INSTANCE_ID)
            .setClientId(CLIENT_ID)
            .setClientHost(CLIENT_HOST)
            .setRebalanceTimeoutMs(REBALANCE_TIMEOUT_MS)
            .setTopologyEpoch(1)
            .setProcessId(PROCESS_ID)
            .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost(USER_ENDPOINT).setPort(USER_ENDPOINT_PORT))
            .setClientTags(Map.of(TAG_1, VALUE_1, TAG_2, VALUE_2))
            .build();

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new StreamsGroupMemberMetadataKey()
                .setGroupId(GROUP_ID)
                .setMemberId(MEMBER_ID),
            new ApiMessageAndVersion(
                new StreamsGroupMemberMetadataValue()
                    .setRackId(null)
                    .setInstanceId(INSTANCE_ID)
                    .setClientId(CLIENT_ID)
                    .setClientHost(CLIENT_HOST)
                    .setRebalanceTimeoutMs(REBALANCE_TIMEOUT_MS)
                    .setTopologyEpoch(1)
                    .setProcessId(PROCESS_ID)
                    .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost(USER_ENDPOINT).setPort(USER_ENDPOINT_PORT))
                    .setClientTags(List.of(
                        new StreamsGroupMemberMetadataValue.KeyValue().setKey(TAG_1).setValue(VALUE_1),
                        new StreamsGroupMemberMetadataValue.KeyValue().setKey(TAG_2).setValue(VALUE_2)
                    )),
                (short) 0
            )
        );

        assertEquals(expectedRecord, StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(GROUP_ID, member));
    }

    @Test
    public void testNewStreamsGroupMemberRecordWithNullInstanceId() {
        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_ID)
            .setRackId(RACK_1)
            .setInstanceId(null)
            .setClientId(CLIENT_ID)
            .setClientHost(CLIENT_HOST)
            .setRebalanceTimeoutMs(REBALANCE_TIMEOUT_MS)
            .setTopologyEpoch(1)
            .setProcessId(PROCESS_ID)
            .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost(USER_ENDPOINT).setPort(USER_ENDPOINT_PORT))
            .setClientTags(Map.of(TAG_1, VALUE_1, TAG_2, VALUE_2))
            .build();

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new StreamsGroupMemberMetadataKey()
                .setGroupId(GROUP_ID)
                .setMemberId(MEMBER_ID),
            new ApiMessageAndVersion(
                new StreamsGroupMemberMetadataValue()
                    .setRackId(RACK_1)
                    .setInstanceId(null)
                    .setClientId(CLIENT_ID)
                    .setClientHost(CLIENT_HOST)
                    .setRebalanceTimeoutMs(REBALANCE_TIMEOUT_MS)
                    .setTopologyEpoch(1)
                    .setProcessId(PROCESS_ID)
                    .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost(USER_ENDPOINT).setPort(USER_ENDPOINT_PORT))
                    .setClientTags(List.of(
                        new StreamsGroupMemberMetadataValue.KeyValue().setKey(TAG_1).setValue(VALUE_1),
                        new StreamsGroupMemberMetadataValue.KeyValue().setKey(TAG_2).setValue(VALUE_2)
                    )),
                (short) 0
            )
        );

        assertEquals(expectedRecord, StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(GROUP_ID, member));
    }

    @Test
    public void testNewStreamsGroupMemberRecordWithNullUserEndpoint() {
        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_ID)
            .setRackId(RACK_1)
            .setInstanceId(INSTANCE_ID)
            .setClientId(CLIENT_ID)
            .setClientHost(CLIENT_HOST)
            .setRebalanceTimeoutMs(REBALANCE_TIMEOUT_MS)
            .setTopologyEpoch(1)
            .setProcessId(PROCESS_ID)
            .setUserEndpoint(null)
            .setClientTags(Map.of(TAG_1, VALUE_1, TAG_2, VALUE_2))
            .build();

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new StreamsGroupMemberMetadataKey()
                .setGroupId(GROUP_ID)
                .setMemberId(MEMBER_ID),
            new ApiMessageAndVersion(
                new StreamsGroupMemberMetadataValue()
                    .setRackId(RACK_1)
                    .setInstanceId(INSTANCE_ID)
                    .setClientId(CLIENT_ID)
                    .setClientHost(CLIENT_HOST)
                    .setRebalanceTimeoutMs(REBALANCE_TIMEOUT_MS)
                    .setTopologyEpoch(1)
                    .setProcessId(PROCESS_ID)
                    .setUserEndpoint(null)
                    .setClientTags(List.of(
                        new StreamsGroupMemberMetadataValue.KeyValue().setKey(TAG_1).setValue(VALUE_1),
                        new StreamsGroupMemberMetadataValue.KeyValue().setKey(TAG_2).setValue(VALUE_2)
                    )),
                (short) 0
            )
        );

        assertEquals(expectedRecord, StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(GROUP_ID, member));
    }

    @Test
    public void testNewStreamsGroupMemberTombstoneRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new StreamsGroupMemberMetadataKey()
                .setGroupId(GROUP_ID)
                .setMemberId(MEMBER_ID)
        );

        assertEquals(expectedRecord, StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord(GROUP_ID, MEMBER_ID));
    }

    @Test
    public void testNewStreamsGroupPartitionMetadataRecord() {
        Uuid uuid1 = Uuid.randomUuid();
        Uuid uuid2 = Uuid.randomUuid();
        Map<String, TopicMetadata> newPartitionMetadata = Map.of(
            TOPIC_1, new TopicMetadata(uuid1, TOPIC_1, 1),
            TOPIC_2, new TopicMetadata(uuid2, TOPIC_2, 2)
        );

        StreamsGroupPartitionMetadataValue value = new StreamsGroupPartitionMetadataValue();
        value.topics().add(new StreamsGroupPartitionMetadataValue.TopicMetadata()
            .setTopicId(uuid1)
            .setTopicName(TOPIC_1)
            .setNumPartitions(1)
        );
        value.topics().add(new StreamsGroupPartitionMetadataValue.TopicMetadata()
            .setTopicId(uuid2)
            .setTopicName(TOPIC_2)
            .setNumPartitions(2)
        );

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new StreamsGroupPartitionMetadataKey()
                .setGroupId(GROUP_ID),
            new ApiMessageAndVersion(value, (short) 0)
        );

        assertEquals(expectedRecord,
            StreamsCoordinatorRecordHelpers.newStreamsGroupPartitionMetadataRecord(GROUP_ID, newPartitionMetadata));
    }

    @Test
    public void testNewStreamsGroupPartitionMetadataTombstoneRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new StreamsGroupPartitionMetadataKey()
                .setGroupId(GROUP_ID)
        );

        assertEquals(expectedRecord, StreamsCoordinatorRecordHelpers.newStreamsGroupPartitionMetadataTombstoneRecord(GROUP_ID));
    }

    @Test
    public void testNewStreamsGroupEpochRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new StreamsGroupMetadataKey()
                .setGroupId(GROUP_ID),
            new ApiMessageAndVersion(
                new StreamsGroupMetadataValue()
                    .setEpoch(42),
                (short) 0
            )
        );

        assertEquals(expectedRecord, StreamsCoordinatorRecordHelpers.newStreamsGroupEpochRecord(GROUP_ID, 42));
    }

    @Test
    public void testNewStreamsGroupEpochTombstoneRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new StreamsGroupMetadataKey()
                .setGroupId(GROUP_ID)
        );

        assertEquals(expectedRecord, StreamsCoordinatorRecordHelpers.newStreamsGroupEpochTombstoneRecord(GROUP_ID));
    }

    @Test
    public void testNewStreamsGroupTargetAssignmentRecord() {
        Map<String, Set<Integer>> activeTasks = Map.of(SUBTOPOLOGY_1, Set.of(1, 2, 3));
        Map<String, Set<Integer>> standbyTasks = Map.of(SUBTOPOLOGY_2, Set.of(4, 5, 6));
        Map<String, Set<Integer>> warmupTasks = Map.of(SUBTOPOLOGY_3, Set.of(7, 8, 9));

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new StreamsGroupTargetAssignmentMemberKey()
                .setGroupId(GROUP_ID)
                .setMemberId(MEMBER_ID),
            new ApiMessageAndVersion(
                new StreamsGroupTargetAssignmentMemberValue()
                    .setActiveTasks(List.of(
                        new StreamsGroupTargetAssignmentMemberValue.TaskIds()
                            .setSubtopologyId(SUBTOPOLOGY_1)
                            .setPartitions(List.of(1, 2, 3))
                    ))
                    .setStandbyTasks(List.of(
                        new StreamsGroupTargetAssignmentMemberValue.TaskIds()
                            .setSubtopologyId(SUBTOPOLOGY_2)
                            .setPartitions(List.of(4, 5, 6))
                    ))
                    .setWarmupTasks(List.of(
                        new StreamsGroupTargetAssignmentMemberValue.TaskIds()
                            .setSubtopologyId(SUBTOPOLOGY_3)
                            .setPartitions(List.of(7, 8, 9))
                    )),
                (short) 0
            )
        );

        assertEquals(expectedRecord,
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(GROUP_ID, MEMBER_ID,
                new TasksTuple(activeTasks, standbyTasks, warmupTasks)));
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testNewStreamsGroupTargetAssignmentRecordWithEmptyTaskIds(TaskRole taskRole) {
        final StreamsGroupTargetAssignmentMemberValue targetAssignmentMemberValue = new StreamsGroupTargetAssignmentMemberValue();
        final List<TaskIds> taskIds = List.of(new TaskIds().setSubtopologyId(SUBTOPOLOGY_1).setPartitions(List.of(1, 2, 3)));

        switch (taskRole) {
            case ACTIVE:
                targetAssignmentMemberValue.setActiveTasks(taskIds);
                break;
            case STANDBY:
                targetAssignmentMemberValue.setStandbyTasks(taskIds);
                break;
            case WARMUP:
                targetAssignmentMemberValue.setWarmupTasks(taskIds);
                break;
        }

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new StreamsGroupTargetAssignmentMemberKey()
                .setGroupId(GROUP_ID)
                .setMemberId(MEMBER_ID),
            new ApiMessageAndVersion(
                targetAssignmentMemberValue,
                (short) 0
            )
        );

        assertEquals(expectedRecord,
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(GROUP_ID, MEMBER_ID,
                mkTasksTuple(taskRole, mkTasks(SUBTOPOLOGY_1, 1, 2, 3))));
    }

    @Test
    public void testNewStreamsGroupTargetAssignmentTombstoneRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new StreamsGroupTargetAssignmentMemberKey()
                .setGroupId(GROUP_ID)
                .setMemberId(MEMBER_ID)
        );

        assertEquals(expectedRecord,
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord(GROUP_ID, MEMBER_ID));
    }

    @Test
    public void testNewStreamsGroupTargetAssignmentEpochRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new StreamsGroupTargetAssignmentMetadataKey()
                .setGroupId(GROUP_ID),
            new ApiMessageAndVersion(
                new StreamsGroupTargetAssignmentMetadataValue()
                    .setAssignmentEpoch(42),
                (short) 0
            )
        );

        assertEquals(expectedRecord, StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentEpochRecord(GROUP_ID, 42));
    }

    @Test
    public void testNewStreamsGroupTargetAssignmentEpochTombstoneRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new StreamsGroupTargetAssignmentMetadataKey()
                .setGroupId(GROUP_ID)
        );

        assertEquals(expectedRecord, StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentEpochTombstoneRecord(GROUP_ID));
    }

    @Test
    public void testNewStreamsGroupCurrentAssignmentRecord() {
        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_ID)
            .setRackId(RACK_1)
            .setInstanceId(INSTANCE_ID)
            .setClientId(CLIENT_ID)
            .setClientHost(CLIENT_HOST)
            .setRebalanceTimeoutMs(REBALANCE_TIMEOUT_MS)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setState(MemberState.STABLE)
            .setTopologyEpoch(1)
            .setProcessId(PROCESS_ID)
            .setUserEndpoint(new Endpoint().setHost(USER_ENDPOINT).setPort(USER_ENDPOINT_PORT))
            .setClientTags(Map.of(TAG_1, VALUE_1, TAG_2, VALUE_2))
            .setAssignedTasks(new TasksTuple(
                Map.of(
                    SUBTOPOLOGY_1, Set.of(1, 2, 3)
                ),
                Map.of(
                    SUBTOPOLOGY_2, Set.of(4, 5, 6)
                ),
                Map.of(
                    SUBTOPOLOGY_3, Set.of(7, 8, 9)
                )
            ))
            .setTasksPendingRevocation(new TasksTuple(
                Map.of(
                    SUBTOPOLOGY_1, Set.of(1, 2, 3)
                ),
                Map.of(
                    SUBTOPOLOGY_2, Set.of(4, 5, 6)
                ),
                Map.of(
                    SUBTOPOLOGY_3, Set.of(7, 8, 9)
                )
            ))
            .build();

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new StreamsGroupCurrentMemberAssignmentKey()
                .setGroupId(GROUP_ID)
                .setMemberId(MEMBER_ID),
            new ApiMessageAndVersion(
                new StreamsGroupCurrentMemberAssignmentValue()
                    .setMemberEpoch(1)
                    .setPreviousMemberEpoch(0)
                    .setState(MemberState.STABLE.value())
                    .setActiveTasks(List.of(
                        new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
                            .setSubtopologyId(SUBTOPOLOGY_1)
                            .setPartitions(List.of(1, 2, 3))
                    ))
                    .setStandbyTasks(List.of(
                        new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
                            .setSubtopologyId(SUBTOPOLOGY_2)
                            .setPartitions(List.of(4, 5, 6))
                    ))
                    .setWarmupTasks(List.of(
                        new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
                            .setSubtopologyId(SUBTOPOLOGY_3)
                            .setPartitions(List.of(7, 8, 9))
                    ))
                    .setActiveTasksPendingRevocation(List.of(
                        new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
                            .setSubtopologyId(SUBTOPOLOGY_1)
                            .setPartitions(List.of(1, 2, 3))
                    ))
                    .setStandbyTasksPendingRevocation(List.of(
                        new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
                            .setSubtopologyId(SUBTOPOLOGY_2)
                            .setPartitions(List.of(4, 5, 6))
                    ))
                    .setWarmupTasksPendingRevocation(List.of(
                        new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
                            .setSubtopologyId(SUBTOPOLOGY_3)
                            .setPartitions(List.of(7, 8, 9))
                    )),
                (short) 0
            )
        );

        assertEquals(expectedRecord, StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(GROUP_ID, member));
    }

    @Test
    public void testNewStreamsGroupCurrentAssignmentRecordWithEmptyAssignment() {
        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_ID)
            .setRackId(RACK_1)
            .setInstanceId(INSTANCE_ID)
            .setClientId(CLIENT_ID)
            .setClientHost(CLIENT_HOST)
            .setRebalanceTimeoutMs(REBALANCE_TIMEOUT_MS)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setState(MemberState.STABLE)
            .setTopologyEpoch(1)
            .setProcessId(PROCESS_ID)
            .setUserEndpoint(new Endpoint().setHost(USER_ENDPOINT).setPort(USER_ENDPOINT_PORT))
            .setClientTags(Map.of(TAG_1, VALUE_1, TAG_2, VALUE_2))
            .setAssignedTasks(new TasksTuple(Map.of(), Map.of(), Map.of()))
            .setTasksPendingRevocation(new TasksTuple(Map.of(), Map.of(), Map.of()))
            .build();

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new StreamsGroupCurrentMemberAssignmentKey()
                .setGroupId(GROUP_ID)
                .setMemberId(MEMBER_ID),
            new ApiMessageAndVersion(
                new StreamsGroupCurrentMemberAssignmentValue()
                    .setMemberEpoch(1)
                    .setPreviousMemberEpoch(0)
                    .setState(MemberState.STABLE.value())
                    .setActiveTasks(List.of())
                    .setStandbyTasks(List.of())
                    .setWarmupTasks(List.of())
                    .setActiveTasksPendingRevocation(List.of())
                    .setStandbyTasksPendingRevocation(List.of())
                    .setWarmupTasksPendingRevocation(List.of()),
                (short) 0
            )
        );

        assertEquals(expectedRecord, StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(GROUP_ID, member));
    }

    @Test
    public void testNewStreamsGroupCurrentAssignmentTombstoneRecord() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new StreamsGroupCurrentMemberAssignmentKey()
                .setGroupId(GROUP_ID)
                .setMemberId(MEMBER_ID)
        );

        assertEquals(expectedRecord,
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord(GROUP_ID, MEMBER_ID));
    }

    @Test
    public void testNewStreamsGroupTopologyRecord() {
        StreamsGroupHeartbeatRequestData.Topology topology =
            new StreamsGroupHeartbeatRequestData.Topology()
                .setEpoch(42)
                .setSubtopologies(
                    List.of(new StreamsGroupHeartbeatRequestData.Subtopology()
                            .setSubtopologyId(SUBTOPOLOGY_1)
                            .setRepartitionSinkTopics(List.of(TOPIC_FOO))
                            .setSourceTopics(List.of(TOPIC_BAR))
                            .setSourceTopicRegex(List.of(TOPIC_REGEX))
                            .setRepartitionSourceTopics(
                                List.of(
                                    new StreamsGroupHeartbeatRequestData.TopicInfo()
                                        .setName(TOPIC_REPARTITION)
                                        .setPartitions(4)
                                        .setReplicationFactor((short) 3)
                                        .setTopicConfigs(List.of(
                                            new StreamsGroupHeartbeatRequestData.KeyValue()
                                                .setKey(CONFIG_NAME_1)
                                                .setValue(CONFIG_VALUE_1)
                                        ))
                                )
                            )
                            .setStateChangelogTopics(
                                List.of(
                                    new StreamsGroupHeartbeatRequestData.TopicInfo()
                                        .setName(TOPIC_CHANGELOG)
                                        .setReplicationFactor((short) 2)
                                        .setTopicConfigs(List.of(
                                            new StreamsGroupHeartbeatRequestData.KeyValue()
                                                .setKey(CONFIG_NAME_2)
                                                .setValue(CONFIG_VALUE_2)
                                        ))
                                )
                            )
                            .setCopartitionGroups(List.of(
                                new StreamsGroupHeartbeatRequestData.CopartitionGroup()
                                    .setSourceTopics(List.of((short) 0))
                                    .setRepartitionSourceTopics(List.of((short) 0)),
                                new StreamsGroupHeartbeatRequestData.CopartitionGroup()
                                    .setSourceTopicRegex(List.of((short) 0))
                            )),
                        new StreamsGroupHeartbeatRequestData.Subtopology()
                            .setSubtopologyId(SUBTOPOLOGY_1)
                            .setRepartitionSinkTopics(List.of())
                            .setSourceTopics(List.of(TOPIC_BAR))
                            .setSourceTopicRegex(List.of())
                            .setRepartitionSourceTopics(List.of())
                            .setStateChangelogTopics(List.of())
                            .setCopartitionGroups(List.of())
                    )
                );

        StreamsGroupTopologyValue expectedTopology =
            new StreamsGroupTopologyValue()
                .setEpoch(42)
                .setSubtopologies(
                    List.of(new StreamsGroupTopologyValue.Subtopology()
                            .setSubtopologyId(SUBTOPOLOGY_1)
                            .setRepartitionSinkTopics(List.of(TOPIC_FOO))
                            .setSourceTopics(List.of(TOPIC_BAR))
                            .setSourceTopicRegex(List.of(TOPIC_REGEX))
                            .setRepartitionSourceTopics(
                                List.of(
                                    new StreamsGroupTopologyValue.TopicInfo()
                                        .setName(TOPIC_REPARTITION)
                                        .setPartitions(4)
                                        .setReplicationFactor((short) 3)
                                        .setTopicConfigs(List.of(
                                            new StreamsGroupTopologyValue.TopicConfig()
                                                .setKey(CONFIG_NAME_1)
                                                .setValue(CONFIG_VALUE_1)
                                        ))
                                )
                            )
                            .setStateChangelogTopics(
                                List.of(
                                    new StreamsGroupTopologyValue.TopicInfo()
                                        .setName(TOPIC_CHANGELOG)
                                        .setReplicationFactor((short) 2)
                                        .setTopicConfigs(List.of(
                                            new StreamsGroupTopologyValue.TopicConfig()
                                                .setKey(CONFIG_NAME_2)
                                                .setValue(CONFIG_VALUE_2)
                                        ))
                                )
                            )
                            .setCopartitionGroups(List.of(
                                new StreamsGroupTopologyValue.CopartitionGroup()
                                    .setSourceTopics(List.of((short) 0))
                                    .setRepartitionSourceTopics(List.of((short) 0)),
                                new StreamsGroupTopologyValue.CopartitionGroup()
                                    .setSourceTopicRegex(List.of((short) 0))
                            )),
                        new StreamsGroupTopologyValue.Subtopology()
                            .setSubtopologyId(SUBTOPOLOGY_1)
                            .setRepartitionSinkTopics(List.of())
                            .setSourceTopics(List.of(TOPIC_BAR))
                            .setSourceTopicRegex(List.of())
                            .setRepartitionSourceTopics(List.of())
                            .setStateChangelogTopics(List.of())
                            .setCopartitionGroups(List.of())
                    )
                );

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new StreamsGroupTopologyKey()
                .setGroupId(GROUP_ID),
            new ApiMessageAndVersion(
                expectedTopology,
                (short) 0));

        assertEquals(expectedRecord, StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord(GROUP_ID, topology));
    }

    @Test
    public void testNewStreamsGroupTopologyRecordTombstone() {
        CoordinatorRecord expectedRecord = CoordinatorRecord.tombstone(
            new StreamsGroupTopologyKey()
                .setGroupId(GROUP_ID)
        );

        assertEquals(expectedRecord, StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecordTombstone(GROUP_ID));
    }

    @Test
    public void testNewStreamsGroupMemberRecordNullGroupId() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(null, mock(StreamsGroupMember.class)));
        assertEquals("groupId should not be null here", exception.getMessage());
    }

    @Test
    public void testNewStreamsGroupMemberRecordNullMember() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord("groupId", null));
        assertEquals("member should not be null here", exception.getMessage());
    }

    @Test
    public void testNewStreamsGroupMemberTombstoneRecordNullGroupId() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord(null, "memberId"));
        assertEquals("groupId should not be null here", exception.getMessage());
    }

    @Test
    public void testNewStreamsGroupMemberTombstoneRecordNullMemberId() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord("groupId", null));
        assertEquals("memberId should not be null here", exception.getMessage());
    }

    @Test
    public void testNewStreamsGroupPartitionMetadataRecordNullGroupId() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupPartitionMetadataRecord(null, Map.of()));
        assertEquals("groupId should not be null here", exception.getMessage());
    }

    @Test
    public void testNewStreamsGroupPartitionMetadataRecordNullNewPartitionMetadata() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupPartitionMetadataRecord("groupId", null));
        assertEquals("newPartitionMetadata should not be null here", exception.getMessage());
    }

    @Test
    public void testNewStreamsGroupPartitionMetadataTombstoneRecordNullGroupId() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupPartitionMetadataTombstoneRecord(null));
        assertEquals("groupId should not be null here", exception.getMessage());
    }

    @Test
    public void testNewStreamsGroupEpochRecordNullGroupId() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupEpochRecord(null, 1));
        assertEquals("groupId should not be null here", exception.getMessage());
    }

    @Test
    public void testNewStreamsGroupEpochTombstoneRecordNullGroupId() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupEpochTombstoneRecord(null));
        assertEquals("groupId should not be null here", exception.getMessage());
    }

    @Test
    public void testNewStreamsGroupTargetAssignmentRecordNullGroupId() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(null, "memberId", mock(TasksTuple.class)));
        assertEquals("groupId should not be null here", exception.getMessage());
    }

    @Test
    public void testNewStreamsGroupTargetAssignmentRecordNullMemberId() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord("groupId", null, mock(TasksTuple.class)));
        assertEquals("memberId should not be null here", exception.getMessage());
    }

    @Test
    public void testNewStreamsGroupTargetAssignmentRecordNullAssignment() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord("groupId", "memberId", null));
        assertEquals("assignment should not be null here", exception.getMessage());
    }

    @Test
    public void testNewStreamsGroupTargetAssignmentTombstoneRecordNullGroupId() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord(null, "memberId"));
        assertEquals("groupId should not be null here", exception.getMessage());
    }

    @Test
    public void testNewStreamsGroupTargetAssignmentTombstoneRecordNullMemberId() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord("groupId", null));
        assertEquals("memberId should not be null here", exception.getMessage());
    }

    @Test
    public void testNewStreamsGroupTargetAssignmentEpochRecordNullGroupId() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentEpochRecord(null, 1));
        assertEquals("groupId should not be null here", exception.getMessage());
    }

    @Test
    public void testNewStreamsGroupTargetAssignmentEpochTombstoneRecordNullGroupId() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentEpochTombstoneRecord(null));
        assertEquals("groupId should not be null here", exception.getMessage());
    }

    @Test
    public void testNewStreamsGroupCurrentAssignmentRecordNullGroupId() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(null, mock(StreamsGroupMember.class)));
        assertEquals("groupId should not be null here", exception.getMessage());
    }

    @Test
    public void testNewStreamsGroupCurrentAssignmentRecordNullMember() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord("groupId", null));
        assertEquals("member should not be null here", exception.getMessage());
    }

    @Test
    public void testNewStreamsGroupCurrentAssignmentTombstoneRecordNullGroupId() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord(null, "memberId"));
        assertEquals("groupId should not be null here", exception.getMessage());
    }

    @Test
    public void testNewStreamsGroupCurrentAssignmentTombstoneRecordNullMemberId() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord("groupId", null));
        assertEquals("memberId should not be null here", exception.getMessage());
    }

    @Test
    public void testNewStreamsGroupTopologyRecordWithValueNullGroupId() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord(null, mock(StreamsGroupTopologyValue.class)));
        assertEquals("groupId should not be null here", exception.getMessage());
    }

    @Test
    public void testNewStreamsGroupTopologyRecordWithTopologyNullGroupId() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord(null, mock(StreamsGroupHeartbeatRequestData.Topology.class)));
        assertEquals("groupId should not be null here", exception.getMessage());
    }

    @Test
    public void testNewStreamsGroupTopologyRecordNullTopology() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord("groupId", (StreamsGroupHeartbeatRequestData.Topology) null));
        assertEquals("topology should not be null here", exception.getMessage());
    }

    @Test
    public void testNewStreamsGroupTopologyRecordNullValue() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord("groupId", (StreamsGroupTopologyValue) null));
        assertEquals("value should not be null here", exception.getMessage());
    }

    @Test
    public void testNewStreamsGroupTopologyRecordTombstoneNullGroupId() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecordTombstone(null));
        assertEquals("groupId should not be null here", exception.getMessage());
    }

    @Test
    public void testConvertToStreamsGroupTopologyRecordNullTopology() {
        NullPointerException exception = assertThrows(NullPointerException.class, () ->
            StreamsCoordinatorRecordHelpers.convertToStreamsGroupTopologyRecord(null));
        assertEquals("topology should not be null here", exception.getMessage());
    }
}